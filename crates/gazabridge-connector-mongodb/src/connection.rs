//! The MongoDB `Connection` implementation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use futures::{TryStreamExt, stream};
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::connector::{CountResult, HealthReport, InspectOptions, Transaction};
use gazabridge_plugin_sdk::ddl::{DdlPlan, SchemaOperation};
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::profile::Version;
use gazabridge_plugin_sdk::query::{ExplainPlan, QueryRequest, QueryResult};
use gazabridge_plugin_sdk::read::{
    Page, PageRequest, ReadPosition, ReadRequest, RecordBatch, RecordStream,
};
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, IndexInfo, NamespaceEntry, NamespacePath, ObjectKind, ObjectRef, ObjectSchema,
    SamplingMethod,
};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, RecordFailure, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{
    Capability, CapabilityManifest, Connection, ConnectorError, ConnectorResult, ErrorKind,
    ServerInfo,
};
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::IndexOptions;
use mongodb::results::CollectionType;
use mongodb::{Client, ClientSession, Collection, IndexModel};
use tokio_util::sync::CancellationToken;

use crate::CONNECTOR_ID;
use crate::convert::{document_to_record, from_bson, record_to_document, to_bson};
use crate::error::map_mongo;
use crate::filter::{and, to_query, to_sort};
use crate::infer::Inferrer;

#[derive(Debug)]
pub struct Inner {
    pub client: Client,
    pub manifest: CapabilityManifest,
    pub read_only: bool,
    pub default_db: Option<String>,
    pub transactions: bool,
}

impl Inner {
    fn ensure_writable(&self) -> ConnectorResult<()> {
        if self.read_only {
            Err(ConnectorError::ReadOnly)
        } else {
            Ok(())
        }
    }

    fn database_name(&self, ns: &NamespacePath) -> ConnectorResult<String> {
        ns.database
            .clone()
            .or_else(|| ns.schema.clone())
            .or_else(|| self.default_db.clone())
            .ok_or_else(|| {
                ConnectorError::invalid_input(
                    "no database given and the connection has no default database",
                )
            })
    }

    fn collection(&self, object: &ObjectRef) -> ConnectorResult<Collection<Document>> {
        if object.name.is_empty() || object.name.contains('\0') || object.name.contains('$') {
            return Err(ConnectorError::invalid_input("invalid collection name"));
        }
        Ok(self
            .client
            .database(&self.database_name(&object.namespace)?)
            .collection(&object.name))
    }
}

#[derive(Debug, Clone)]
pub struct MongoConnection {
    inner: Arc<Inner>,
}

impl MongoConnection {
    pub(crate) fn new(inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

/// Projection leaving out `fields` (used when nothing is included).
fn exclusion(fields: &[String]) -> Option<Document> {
    (!fields.is_empty()).then(|| fields.iter().map(|f| (f.clone(), Bson::Int32(0))).collect())
}

fn projection(fields: &[String], keep: &[String]) -> Option<Document> {
    if fields.is_empty() {
        return None;
    }
    let mut p = Document::new();
    for f in fields.iter().chain(keep) {
        p.insert(f.clone(), 1);
    }
    if !fields.iter().any(|f| f == "_id") && !keep.iter().any(|f| f == "_id") {
        p.insert("_id", 0);
    }
    Some(p)
}

/// Aggregation expression for a sort key; missing becomes null so it
/// compares the way `sort` orders it.
fn key_expr(field: &str) -> Bson {
    if field == "_id" {
        Bson::String("$_id".into())
    } else {
        Bson::Document(doc! { "$ifNull": [format!("${field}"), Bson::Null] })
    }
}

/// `$expr` keyset condition. Aggregation comparisons follow BSON sort order
/// across types, unlike query operators (whose type bracketing would skip
/// documents whose key has a different type from the last one seen).
fn after_expr(keys: &[(String, SortDirection)], values: &[Bson]) -> Document {
    let mut branches = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        let mut parts: Vec<Bson> = (0..i)
            .map(|j| Bson::Document(doc! { "$eq": [key_expr(&keys[j].0), values[j].clone()] }))
            .collect();
        let op = if keys[i].1 == SortDirection::Asc {
            "$gt"
        } else {
            "$lt"
        };
        parts.push(Bson::Document(
            doc! { op: [key_expr(&keys[i].0), values[i].clone()] },
        ));
        branches.push(Bson::Document(doc! { "$and": parts }));
    }
    doc! { "$expr": { "$or": branches } }
}

fn key_value(doc: &Document, path: &str) -> Bson {
    let mut cur: Option<&Bson> = None;
    let mut d = Some(doc);
    for seg in path.split('.') {
        cur = d.and_then(|x| x.get(seg));
        d = match cur {
            Some(Bson::Document(inner)) => Some(inner),
            _ => None,
        };
    }
    cur.cloned().unwrap_or(Bson::Null)
}

fn position(values: &[Bson]) -> ConnectorResult<ReadPosition> {
    let canonical: Vec<Value> = values.iter().map(from_bson).collect();
    Ok(ReadPosition {
        connector_id: CONNECTOR_ID.into(),
        token: serde_json::json!({ "after": serde_json::to_value(canonical).map_err(|e| ConnectorError::internal(e.to_string()))? }),
    })
}

fn parse_position(p: &ReadPosition, n: usize) -> ConnectorResult<Vec<Bson>> {
    if p.connector_id != CONNECTOR_ID {
        return Err(ConnectorError::invalid_input(
            "position belongs to a different connector",
        ));
    }
    let values: Vec<Value> = serde_json::from_value(
        p.token
            .get("after")
            .cloned()
            .ok_or_else(|| ConnectorError::invalid_input("position has no `after` key"))?,
    )
    .map_err(|e| ConnectorError::invalid_input(format!("invalid position: {e}")))?;
    if values.len() != n {
        return Err(ConnectorError::invalid_input(
            "position does not match the read's ordering keys",
        ));
    }
    values
        .iter()
        .map(|v| to_bson(v, None).map(|b| b.unwrap_or(Bson::Null)))
        .collect()
}

struct ReadState {
    coll: Collection<Document>,
    filter: Document,
    sort: Document,
    keys: Vec<(String, SortDirection)>,
    hidden: Vec<String>,
    projection: Option<Document>,
    after: Option<Vec<Bson>>,
    batch: i64,
    remaining: Option<u64>,
    cancel: CancellationToken,
    done: bool,
}

async fn next_batch(mut st: ReadState) -> Option<(ConnectorResult<RecordBatch>, ReadState)> {
    if st.done {
        return None;
    }
    if st.cancel.is_cancelled() {
        st.done = true;
        return Some((Err(ConnectorError::Cancelled), st));
    }
    let limit = match st.remaining {
        Some(0) => return None,
        Some(r) => r.min(st.batch as u64) as i64,
        None => st.batch,
    };
    let filter = match &st.after {
        Some(values) => and(vec![st.filter.clone(), after_expr(&st.keys, values)]),
        None => st.filter.clone(),
    };
    let fetch = async {
        let mut find = st.coll.find(filter).sort(st.sort.clone()).limit(limit);
        if let Some(p) = &st.projection {
            find = find.projection(p.clone());
        }
        find.await
            .map_err(map_mongo)?
            .try_collect::<Vec<Document>>()
            .await
            .map_err(map_mongo)
    };
    let docs = tokio::select! {
        r = fetch => r,
        _ = st.cancel.cancelled() => Err(ConnectorError::Cancelled),
    };
    match docs {
        Err(e) => {
            st.done = true;
            Some((Err(e), st))
        }
        Ok(docs) if docs.is_empty() => None,
        Ok(docs) => {
            let n = docs.len() as u64;
            if (n as i64) < limit {
                st.done = true;
            }
            if let Some(r) = &mut st.remaining {
                *r -= n;
            }
            let last_keys = docs.last().map(|d| {
                st.keys
                    .iter()
                    .map(|(k, _)| key_value(d, k))
                    .collect::<Vec<_>>()
            });
            let records = docs
                .iter()
                .map(|d| {
                    let mut r = document_to_record(d);
                    for h in &st.hidden {
                        r.fields.shift_remove(h);
                    }
                    r
                })
                .collect();
            let pos = match last_keys {
                Some(k) => {
                    let p = position(&k);
                    st.after = Some(k);
                    match p {
                        Ok(p) => Some(p),
                        Err(e) => return Some((Err(e), st)),
                    }
                }
                None => None,
            };
            Some((Ok(RecordBatch::new(records, pos)), st))
        }
    }
}

fn columns_from(records: &[Record]) -> Vec<ColumnInfo> {
    let mut seen: Vec<(String, CanonicalType)> = Vec::new();
    for r in records {
        for (k, v) in &r.fields {
            let t = v.value.canonical_type();
            match seen.iter_mut().find(|(n, _)| n == k) {
                Some((_, existing)) => *existing = existing.clone().unify(t),
                None => seen.push((k.clone(), t)),
            }
        }
    }
    seen.into_iter()
        .enumerate()
        .map(|(i, (name, t))| ColumnInfo {
            name,
            native_type: "mixed".into(),
            canonical_type: t,
            nullable: true,
            default_expression: None,
            is_generated: false,
            is_identity: false,
            comment: None,
            ordinal: i as u32 + 1,
        })
        .collect()
}

fn parse_json(text: &str) -> ConnectorResult<Bson> {
    let json: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| ConnectorError::invalid_input(format!("query is not valid JSON: {e}")))?;
    Bson::try_from(json)
        .map_err(|e| ConnectorError::invalid_input(format!("invalid Extended JSON: {e}")))
}

fn pipeline_of(text: &str, read_only: bool) -> ConnectorResult<Vec<Document>> {
    let Bson::Array(stages) = parse_json(text)? else {
        return Err(ConnectorError::invalid_input(
            "an aggregation pipeline must be a JSON array of stages",
        ));
    };
    let mut out = Vec::with_capacity(stages.len());
    for s in stages {
        let Bson::Document(d) = s else {
            return Err(ConnectorError::invalid_input(
                "every pipeline stage must be an object",
            ));
        };
        if read_only && d.keys().any(|k| k == "$out" || k == "$merge") {
            return Err(ConnectorError::ReadOnly);
        }
        out.push(d);
    }
    Ok(out)
}

fn filter_of(text: &str) -> ConnectorResult<Document> {
    if text.trim().is_empty() {
        return Ok(Document::new());
    }
    match parse_json(text)? {
        Bson::Document(d) => Ok(d),
        _ => Err(ConnectorError::invalid_input(
            "a filter must be a JSON object",
        )),
    }
}

fn key_filter(record: &Record, keys: &[String]) -> ConnectorResult<Document> {
    let mut f = Document::new();
    for k in keys {
        let v = record.get(k);
        if v.is_absent() {
            return Err(ConnectorError::new(
                ErrorKind::Constraint,
                format!("key field `{k}` is missing or null"),
            ));
        }
        f.insert(k.clone(), to_bson(v, None)?.unwrap_or(Bson::Null));
    }
    Ok(f)
}

fn failure(index: usize, e: &ConnectorError) -> RecordFailure {
    RecordFailure {
        index,
        kind: e.kind(),
        message: e.to_string(),
        code: e.code().map(str::to_owned),
    }
}

const DIRECT_MODES: [WriteMode; 5] = [
    WriteMode::Insert,
    WriteMode::Append,
    WriteMode::Upsert,
    WriteMode::SkipExisting,
    WriteMode::FailOnConflict,
];

pub fn direct_modes() -> [WriteMode; 5] {
    DIRECT_MODES
}

/// Writes a batch. With a session the batch runs inside its transaction.
async fn write(
    coll: &Collection<Document>,
    batch: &WriteBatch,
    mut session: Option<&mut ClientSession>,
) -> ConnectorResult<WriteOutcome> {
    if !DIRECT_MODES.contains(&batch.mode) {
        return Err(ConnectorError::invalid_input(format!(
            "write mode {:?} is orchestrated by the migration engine, not applied by the connector",
            batch.mode
        )));
    }
    if batch.mode.requires_key() && batch.key_fields.is_empty() {
        return Err(ConnectorError::invalid_input(
            "this write mode needs key fields",
        ));
    }
    let mut outcome = WriteOutcome::default();
    let mut docs = Vec::with_capacity(batch.records.len());
    let mut indexes = Vec::with_capacity(batch.records.len());
    for (i, r) in batch.records.iter().enumerate() {
        match record_to_document(r) {
            Ok(d) => {
                docs.push(d);
                indexes.push(i);
            }
            Err(e) if batch.atomic => return Err(e),
            Err(e) => outcome.failures.push(failure(i, &e)),
        }
    }
    if docs.is_empty() {
        return Ok(outcome);
    }
    match batch.mode {
        WriteMode::Insert | WriteMode::Append | WriteMode::FailOnConflict => {
            let n = docs.len() as u64;
            let ordered = batch.atomic || batch.mode == WriteMode::FailOnConflict;
            let result = match session.as_deref_mut() {
                Some(s) => coll.insert_many(docs).ordered(ordered).session(s).await,
                None => coll.insert_many(docs).ordered(ordered).await,
            };
            match result {
                Ok(r) => outcome.inserted += r.inserted_ids.len() as u64,
                Err(e) => {
                    let write_errors = match e.kind.as_ref() {
                        mongodb::error::ErrorKind::InsertMany(m) if !batch.atomic => {
                            m.write_errors.clone()
                        }
                        _ => None,
                    };
                    let Some(errors) = write_errors else {
                        return Err(map_mongo(e));
                    };
                    for w in &errors {
                        let err = ConnectorError::from_driver(
                            crate::error::kind_for_code(w.code),
                            &w.message,
                            Some(w.code.to_string()),
                        );
                        outcome.failures.push(failure(
                            indexes.get(w.index).copied().unwrap_or(w.index),
                            &err,
                        ));
                    }
                    // Ordered inserts stop at the first error; unordered ones continue.
                    let attempted = if ordered {
                        errors.first().map_or(0, |w| w.index as u64)
                    } else {
                        n - errors.len() as u64
                    };
                    outcome.inserted += attempted;
                }
            }
        }
        WriteMode::Upsert | WriteMode::SkipExisting => {
            for (doc, &i) in docs.into_iter().zip(&indexes) {
                let filter = match key_filter(&batch.records[i], &batch.key_fields) {
                    Ok(f) => f,
                    Err(e) if batch.atomic => return Err(e),
                    Err(e) => {
                        outcome.failures.push(failure(i, &e));
                        continue;
                    }
                };
                let result = if batch.mode == WriteMode::Upsert {
                    let op = coll.replace_one(filter, doc).upsert(true);
                    match session.as_deref_mut() {
                        Some(s) => op.session(s).await,
                        None => op.await,
                    }
                } else {
                    let op = coll
                        .update_one(filter, doc! { "$setOnInsert": doc })
                        .upsert(true);
                    match session.as_deref_mut() {
                        Some(s) => op.session(s).await,
                        None => op.await,
                    }
                };
                match result {
                    Ok(r) if r.upserted_id.is_some() => outcome.inserted += 1,
                    Ok(_) if batch.mode == WriteMode::Upsert => outcome.updated += 1,
                    Ok(_) => outcome.skipped += 1,
                    Err(e) => {
                        let err = map_mongo(e);
                        if batch.atomic || !err.kind().is_record_level() {
                            return Err(err);
                        }
                        outcome.failures.push(failure(i, &err));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(outcome)
}

fn check_expected(matched: u64, expected: Option<u64>) -> ConnectorResult<()> {
    match expected {
        Some(e) if e != matched => Err(ConnectorError::new(
            ErrorKind::Conflict,
            format!(
                "expected {e} matching document(s) but found {matched}; the data changed or the filter is wrong"
            ),
        )),
        _ => Ok(()),
    }
}

fn set_document(set: &Record) -> ConnectorResult<Document> {
    if set.is_empty() {
        return Err(ConnectorError::invalid_input(
            "an update needs at least one field",
        ));
    }
    if set.fields.contains_key("_id") {
        return Err(ConnectorError::invalid_input(
            "`_id` is immutable and cannot be updated",
        ));
    }
    let mut setd = Document::new();
    let mut unset = Document::new();
    for (k, v) in &set.fields {
        match to_bson(
            &v.value,
            v.meta.as_ref().and_then(|m| m.original_type.as_deref()),
        )? {
            Some(b) => {
                setd.insert(k.clone(), b);
            }
            // Setting a field to "missing" removes it.
            None => {
                unset.insert(k.clone(), "");
            }
        }
    }
    let mut u = Document::new();
    if !setd.is_empty() {
        u.insert("$set", setd);
    }
    if !unset.is_empty() {
        u.insert("$unset", unset);
    }
    Ok(u)
}

fn native_name(t: &CanonicalType) -> ConnectorResult<String> {
    use CanonicalType as T;
    Ok(match t {
        T::Boolean => "bool",
        T::Int { bits } | T::UInt { bits } if *bits <= 31 => "int",
        T::Int { .. } => "long",
        T::UInt { .. } | T::BigInt | T::Decimal { .. } => "decimal",
        T::Float { .. } => "double",
        T::String { .. } | T::Enum { .. } => "string",
        T::Date
        | T::Time { .. }
        | T::LocalDateTime { .. }
        | T::ZonedDateTime
        | T::Duration
        | T::Interval => "string",
        T::UtcDateTime { .. } => "date",
        T::Binary { .. } => "binData",
        T::Uuid => "binData(uuid)",
        T::ObjectId => "objectId",
        T::Array(_) => "array",
        T::Object | T::Map { .. } | T::Json => "object",
        T::Null => "null",
        T::Mixed(_) => "mixed",
        T::Geometry | T::Extension { .. } | T::Unknown => {
            return Err(ConnectorError::unsupported(Capability::CreateCollection));
        }
    }
    .into())
}

fn ddl_text(op: &SchemaOperation, inner: &Inner) -> ConnectorResult<String> {
    let db = |ns: &NamespacePath| inner.database_name(ns);
    Ok(match op {
        SchemaOperation::CreateNamespace { namespace, .. } => {
            format!(
                "// database \"{}\" is created implicitly by its first write",
                db(namespace)?
            )
        }
        SchemaOperation::CreateCollection { object, .. } => {
            format!(
                "db.getSiblingDB({:?}).createCollection({:?})",
                db(&object.namespace)?,
                object.name
            )
        }
        SchemaOperation::CreateIndex { object, index, .. } => format!(
            "db.getSiblingDB({:?}).getCollection({:?}).createIndex({}, {{ name: {:?}, unique: {} }})",
            db(&object.namespace)?,
            object.name,
            serde_json::to_string(
                &index
                    .columns
                    .iter()
                    .map(|c| (c.clone(), 1))
                    .collect::<BTreeMap<_, _>>()
            )
            .unwrap_or_default(),
            index.name,
            index.unique
        ),
        SchemaOperation::DropObject { object, .. } => match object.kind {
            ObjectKind::Database | ObjectKind::Schema => {
                format!("db.getSiblingDB({:?}).dropDatabase()", object.name)
            }
            _ => format!(
                "db.getSiblingDB({:?}).getCollection({:?}).drop()",
                db(&object.namespace)?,
                object.name
            ),
        },
        SchemaOperation::TruncateObject { object } => {
            format!(
                "db.getSiblingDB({:?}).getCollection({:?}).deleteMany({{}})",
                db(&object.namespace)?,
                object.name
            )
        }
        SchemaOperation::CreateTable { .. } => {
            return Err(ConnectorError::unsupported(Capability::CreateTable));
        }
    })
}

#[async_trait]
impl Connection for MongoConnection {
    fn manifest(&self) -> &CapabilityManifest {
        &self.inner.manifest
    }

    fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        let started = Instant::now();
        self.inner
            .client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(map_mongo)?;
        Ok(HealthReport {
            latency: started.elapsed(),
            checked_at: Utc::now(),
            notes: Vec::new(),
        })
    }

    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        let admin = self.inner.client.database("admin");
        let build = admin
            .run_command(doc! { "buildInfo": 1 })
            .await
            .map_err(map_mongo)?;
        let hello = admin
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(map_mongo)?;
        let version = build.get_str("version").unwrap_or("0").to_owned();
        let mut properties = BTreeMap::new();
        if let Ok(set) = hello.get_str("setName") {
            properties.insert("replica_set".into(), set.to_owned());
        }
        let topology = if hello.get_str("msg") == Ok("isdbgrid") {
            "sharded"
        } else if hello.contains_key("setName") {
            "replica_set"
        } else {
            "standalone"
        };
        properties.insert("topology".into(), topology.into());
        properties.insert("transactions".into(), self.inner.transactions.to_string());
        Ok(ServerInfo {
            family: EngineFamily::MongoDb,
            product: "MongoDB".into(),
            version_string: format!("MongoDB {version}"),
            version: Version(version),
            properties,
        })
    }

    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        let db = parent.database.clone().or_else(|| parent.schema.clone());
        let Some(db) = db else {
            let names = self
                .inner
                .client
                .list_database_names()
                .await
                .map_err(map_mongo)?;
            return Ok(names
                .into_iter()
                .map(|name| NamespaceEntry {
                    name,
                    kind: ObjectKind::Database,
                    comment: None,
                    estimated_count: None,
                })
                .collect());
        };
        let specs: Vec<_> = self
            .inner
            .client
            .database(&db)
            .list_collections()
            .await
            .map_err(map_mongo)?
            .try_collect()
            .await
            .map_err(map_mongo)?;
        let mut out: Vec<NamespaceEntry> = specs
            .into_iter()
            .filter(|s| !s.name.starts_with("system."))
            .map(|s| NamespaceEntry {
                kind: if matches!(s.collection_type, CollectionType::View) {
                    ObjectKind::View
                } else {
                    ObjectKind::Collection
                },
                comment: matches!(s.collection_type, CollectionType::Timeseries)
                    .then(|| "time series".into()),
                name: s.name,
                estimated_count: None,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        if ident.is_empty() {
            return Err(ConnectorError::invalid_input(
                "identifier must not be empty",
            ));
        }
        if ident.contains('\0') {
            return Err(ConnectorError::invalid_input(
                "identifier must not contain NUL",
            ));
        }
        // MongoDB names need no quoting; JSON-escape for display in commands.
        serde_json::to_string(ident).map_err(|e| ConnectorError::internal(e.to_string()))
    }

    async fn close(&self) {
        self.inner.client.clone().shutdown().await;
    }

    async fn inspect(
        &self,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        let coll = self.inner.collection(object)?;
        let size = options.sample_size.max(1);
        let docs: Vec<Document> = match options.sampling {
            SamplingMethod::Random => {
                match coll
                    .aggregate(vec![doc! { "$sample": { "size": size as i64 } }])
                    .await
                {
                    Ok(c) => c.try_collect().await.map_err(map_mongo)?,
                    // Some compatible servers lack $sample; fall back to the first documents.
                    Err(_) => coll
                        .find(doc! {})
                        .limit(size as i64)
                        .await
                        .map_err(map_mongo)?
                        .try_collect()
                        .await
                        .map_err(map_mongo)?,
                }
            }
            SamplingMethod::First => coll
                .find(doc! {})
                .limit(size as i64)
                .await
                .map_err(map_mongo)?
                .try_collect()
                .await
                .map_err(map_mongo)?,
            SamplingMethod::FullScan => coll
                .find(doc! {})
                .await
                .map_err(map_mongo)?
                .try_collect()
                .await
                .map_err(map_mongo)?,
        };
        let mut inf = Inferrer::new();
        for d in &docs {
            inf.add(d);
        }
        let inferred = inf.finish(options.sampling);
        let sampled = inferred.sampled_documents;
        let columns = inferred
            .fields
            .iter()
            .filter(|f| !f.path.contains('.') && !f.path.contains("[]"))
            .enumerate()
            .map(|(i, f)| ColumnInfo {
                name: f.path.clone(),
                native_type: f
                    .type_counts
                    .keys()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join("|"),
                canonical_type: f.canonical_type.clone(),
                nullable: f.null_count > 0 || f.present_count < sampled,
                default_expression: None,
                is_generated: false,
                is_identity: f.path == "_id",
                comment: Some(format!(
                    "present in {:.1}% of sampled documents",
                    f.occurrence_ratio(sampled) * 100.0
                )),
                ordinal: i as u32 + 1,
            })
            .collect();
        let indexes = if options.include_indexes {
            let models: Vec<IndexModel> = coll
                .list_indexes()
                .await
                .map_err(map_mongo)?
                .try_collect()
                .await
                .map_err(map_mongo)?;
            models
                .into_iter()
                .map(|m| {
                    let name = m
                        .options
                        .as_ref()
                        .and_then(|o| o.name.clone())
                        .unwrap_or_default();
                    IndexInfo {
                        primary: name == "_id_",
                        unique: name == "_id_"
                            || m.options.as_ref().and_then(|o| o.unique).unwrap_or(false),
                        columns: m.keys.keys().cloned().collect(),
                        method: Some("btree".into()),
                        predicate: m
                            .options
                            .as_ref()
                            .and_then(|o| o.partial_filter_expression.as_ref())
                            .map(|p| p.to_string()),
                        definition: Some(m.keys.to_string()),
                        name,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        let estimated = coll.estimated_document_count().await.ok();
        Ok(ObjectSchema {
            object: object.clone(),
            columns,
            primary_key: vec!["_id".into()],
            indexes,
            constraints: Vec::new(),
            inferred: Some(inferred),
            estimated_count: estimated,
            comment: None,
            definition: None,
        })
    }

    async fn read(
        &self,
        request: ReadRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        if request.batch_size == 0 {
            return Err(ConnectorError::invalid_input(
                "batch size must be at least 1",
            ));
        }
        let coll = self.inner.collection(&request.object)?;
        let mut keys: Vec<(String, SortDirection)> = request
            .order_by
            .iter()
            .map(|k| (k.field.0.clone(), k.direction))
            .collect();
        // `_id` breaks ties so the keyset is unique.
        if !keys.iter().any(|(k, _)| k == "_id") {
            let dir = keys.last().map_or(SortDirection::Asc, |(_, d)| *d);
            keys.push(("_id".into(), dir));
        }
        let sort_keys: Vec<SortKey> = keys
            .iter()
            .map(|(k, d)| SortKey {
                field: gazabridge_plugin_sdk::filter::FieldPath::new(k.clone()),
                direction: *d,
            })
            .collect();
        let key_names: Vec<String> = keys.iter().map(|(k, _)| k.clone()).collect();
        // Excluded fields stay out, except sort keys the resumable read needs
        // (those are read and then hidden).
        let excluded: Vec<String> = request
            .exclude
            .iter()
            .filter(|f| !key_names.contains(f))
            .cloned()
            .collect();
        let hidden: Vec<String> = if request.projection.is_empty() {
            request
                .exclude
                .iter()
                .filter(|f| key_names.contains(f))
                .cloned()
                .collect()
        } else {
            key_names
                .iter()
                .filter(|k| !request.projection.contains(k))
                .map(|k| k.split('.').next().unwrap_or(k).to_owned())
                .collect()
        };
        let after = request
            .resume_after
            .as_ref()
            .map(|p| parse_position(p, keys.len()))
            .transpose()?;
        let state = ReadState {
            coll,
            filter: request
                .filter
                .as_ref()
                .map(to_query)
                .transpose()?
                .unwrap_or_default(),
            sort: to_sort(&sort_keys)?,
            projection: projection(&request.projection, &key_names)
                .or_else(|| exclusion(&excluded)),
            hidden,
            keys,
            after,
            batch: request.batch_size.min(i64::MAX as usize) as i64,
            remaining: request.limit,
            cancel,
            done: false,
        };
        Ok(Box::pin(stream::unfold(state, next_batch)))
    }

    async fn read_page(
        &self,
        request: PageRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<Page> {
        let coll = self.inner.collection(&request.object)?;
        let limit = request.effective_limit();
        let sort = if request.order_by.is_empty() {
            doc! { "_id": 1 }
        } else {
            to_sort(&request.order_by)?
        };
        let filter = request
            .filter
            .as_ref()
            .map(to_query)
            .transpose()?
            .unwrap_or_default();
        let mut find = coll
            .find(filter)
            .sort(sort)
            .skip(request.offset)
            .limit(i64::from(limit) + 1);
        if let Some(p) =
            projection(&request.projection, &[]).or_else(|| exclusion(&request.exclude))
        {
            find = find.projection(p);
        }
        let fetch = async {
            find.await
                .map_err(map_mongo)?
                .try_collect::<Vec<Document>>()
                .await
                .map_err(map_mongo)
        };
        let docs = tokio::select! {
            r = fetch => r?,
            _ = cancel.cancelled() => return Err(ConnectorError::Cancelled),
        };
        let has_more = docs.len() > limit as usize;
        let records: Vec<Record> = docs
            .iter()
            .take(limit as usize)
            .map(document_to_record)
            .collect();
        Ok(Page {
            columns: columns_from(&records),
            records,
            offset: request.offset,
            has_more,
        })
    }

    async fn count(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        exact: bool,
    ) -> ConnectorResult<CountResult> {
        let coll = self.inner.collection(object)?;
        if exact {
            let f = filter.map(to_query).transpose()?.unwrap_or_default();
            Ok(CountResult {
                count: coll.count_documents(f).await.map_err(map_mongo)?,
                exact: true,
            })
        } else if filter.is_some() {
            Err(ConnectorError::invalid_input(
                "estimated counts cannot be filtered",
            ))
        } else {
            Ok(CountResult {
                count: coll.estimated_document_count().await.map_err(map_mongo)?,
                exact: false,
            })
        }
    }

    async fn count_within(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        max_time: std::time::Duration,
    ) -> ConnectorResult<CountResult> {
        let coll = self.inner.collection(object)?;
        let f = filter.map(to_query).transpose()?.unwrap_or_default();
        // maxTimeMS makes the server itself stop counting.
        let count = coll
            .count_documents(f)
            .max_time(max_time)
            .await
            .map_err(map_mongo)?;
        Ok(CountResult { count, exact: true })
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        self.inner.ensure_writable()?;
        let coll = self.inner.collection(&batch.object)?;
        let work = async {
            if batch.atomic {
                if !self.inner.transactions {
                    return Err(ConnectorError::new(
                        ErrorKind::Unsupported,
                        "atomic batches need transactions, which require a replica set or sharded cluster",
                    ));
                }
                let mut s = self.inner.client.start_session().await.map_err(map_mongo)?;
                s.start_transaction().await.map_err(map_mongo)?;
                match write(&coll, &batch, Some(&mut s)).await {
                    Ok(o) => {
                        s.commit_transaction().await.map_err(map_mongo)?;
                        Ok(o)
                    }
                    Err(e) => {
                        let _ = s.abort_transaction().await;
                        Err(e)
                    }
                }
            } else {
                write(&coll, &batch, None).await
            }
        };
        tokio::select! {
            r = work => r,
            _ = cancel.cancelled() => Err(ConnectorError::Cancelled),
        }
    }

    async fn update(&self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        if !request.filter.is_restrictive() {
            return Err(ConnectorError::invalid_input(
                "an update without a restrictive filter is not allowed",
            ));
        }
        let coll = self.inner.collection(&request.object)?;
        let filter = to_query(&request.filter)?;
        let update = set_document(&request.set)?;
        if let Some(expected) = request.expect_matched {
            if self.inner.transactions {
                let mut s = self.inner.client.start_session().await.map_err(map_mongo)?;
                s.start_transaction().await.map_err(map_mongo)?;
                let r = coll
                    .update_many(filter, update)
                    .session(&mut s)
                    .await
                    .map_err(map_mongo);
                return match r
                    .and_then(|r| check_expected(r.matched_count, Some(expected)).map(|_| r))
                {
                    Ok(r) => {
                        s.commit_transaction().await.map_err(map_mongo)?;
                        Ok(MutationOutcome {
                            matched: r.matched_count,
                            modified: r.modified_count,
                        })
                    }
                    Err(e) => {
                        let _ = s.abort_transaction().await;
                        Err(e)
                    }
                };
            }
            // Without transactions: check first (a concurrent change can still slip in).
            check_expected(
                coll.count_documents(filter.clone())
                    .await
                    .map_err(map_mongo)?,
                Some(expected),
            )?;
        }
        let r = coll.update_many(filter, update).await.map_err(map_mongo)?;
        Ok(MutationOutcome {
            matched: r.matched_count,
            modified: r.modified_count,
        })
    }

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        if !request.filter.is_restrictive() {
            return Err(ConnectorError::invalid_input(
                "a delete without a restrictive filter is not allowed",
            ));
        }
        let coll = self.inner.collection(&request.object)?;
        let filter = to_query(&request.filter)?;
        if let Some(expected) = request.expect_matched {
            if self.inner.transactions {
                let mut s = self.inner.client.start_session().await.map_err(map_mongo)?;
                s.start_transaction().await.map_err(map_mongo)?;
                let r = coll
                    .delete_many(filter)
                    .session(&mut s)
                    .await
                    .map_err(map_mongo);
                return match r
                    .and_then(|r| check_expected(r.deleted_count, Some(expected)).map(|_| r))
                {
                    Ok(r) => {
                        s.commit_transaction().await.map_err(map_mongo)?;
                        Ok(MutationOutcome {
                            matched: r.deleted_count,
                            modified: r.deleted_count,
                        })
                    }
                    Err(e) => {
                        let _ = s.abort_transaction().await;
                        Err(e)
                    }
                };
            }
            check_expected(
                coll.count_documents(filter.clone())
                    .await
                    .map_err(map_mongo)?,
                Some(expected),
            )?;
        }
        let r = coll.delete_many(filter).await.map_err(map_mongo)?;
        Ok(MutationOutcome {
            matched: r.deleted_count,
            modified: r.deleted_count,
        })
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        self.inner.ensure_writable()?;
        if !self.inner.transactions {
            return Err(ConnectorError::unsupported(Capability::Transactions));
        }
        let mut session = self.inner.client.start_session().await.map_err(map_mongo)?;
        session.start_transaction().await.map_err(map_mongo)?;
        Ok(Box::new(MongoTransaction {
            inner: self.inner.clone(),
            session: Some(session),
        }))
    }

    fn plan_ddl(&self, ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
        let statements = ops
            .iter()
            .map(|op| ddl_text(op, &self.inner))
            .collect::<ConnectorResult<Vec<_>>>()?;
        Ok(DdlPlan {
            statements,
            destructive: ops.iter().any(SchemaOperation::is_destructive),
            warnings: Vec::new(),
        })
    }

    async fn apply_ddl(
        &self,
        ops: &[SchemaOperation],
        cancel: CancellationToken,
    ) -> ConnectorResult<()> {
        self.inner.ensure_writable()?;
        self.plan_ddl(ops)?;
        for op in ops {
            if cancel.is_cancelled() {
                return Err(ConnectorError::Cancelled);
            }
            match op {
                SchemaOperation::CreateNamespace { .. } => {}
                SchemaOperation::CreateCollection {
                    object,
                    if_not_exists,
                } => {
                    let db = self
                        .inner
                        .client
                        .database(&self.inner.database_name(&object.namespace)?);
                    if let Err(e) = db.create_collection(&object.name).await {
                        // 48 = NamespaceExists
                        let exists = matches!(e.kind.as_ref(), mongodb::error::ErrorKind::Command(c) if c.code == 48);
                        if !(exists && *if_not_exists) {
                            return Err(map_mongo(e));
                        }
                    }
                }
                SchemaOperation::CreateIndex { object, index, .. } => {
                    let mut keys = Document::new();
                    for c in &index.columns {
                        keys.insert(c.clone(), 1);
                    }
                    let model = IndexModel::builder()
                        .keys(keys)
                        .options(
                            IndexOptions::builder()
                                .name(index.name.clone())
                                .unique(index.unique)
                                .build(),
                        )
                        .build();
                    self.inner
                        .collection(object)?
                        .create_index(model)
                        .await
                        .map_err(map_mongo)?;
                }
                SchemaOperation::DropObject { object, .. } => match object.kind {
                    ObjectKind::Database | ObjectKind::Schema => self
                        .inner
                        .client
                        .database(&object.name)
                        .drop()
                        .await
                        .map_err(map_mongo)?,
                    _ => self
                        .inner
                        .collection(object)?
                        .drop()
                        .await
                        .map_err(map_mongo)?,
                },
                SchemaOperation::TruncateObject { object } => {
                    self.inner
                        .collection(object)?
                        .delete_many(doc! {})
                        .await
                        .map_err(map_mongo)?;
                }
                SchemaOperation::CreateTable { .. } => {
                    return Err(ConnectorError::unsupported(Capability::CreateTable));
                }
            }
        }
        Ok(())
    }

    fn native_type_for(&self, canonical: &CanonicalType) -> ConnectorResult<String> {
        native_name(canonical)
    }

    async fn execute(
        &self,
        request: QueryRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<QueryResult> {
        run_query(&self.inner, &request, &cancel, None).await
    }

    async fn explain(
        &self,
        request: QueryRequest,
        analyze: bool,
        cancel: CancellationToken,
    ) -> ConnectorResult<ExplainPlan> {
        let collection = request
            .collection
            .clone()
            .ok_or_else(|| ConnectorError::invalid_input("a collection is required"))?;
        let db = self
            .inner
            .client
            .database(&self.inner.database_name(&NamespacePath {
                database: request.database.clone(),
                schema: None,
            })?);
        let inner_cmd = match request.language {
            QueryLanguage::MongoFilter => {
                doc! { "find": &collection, "filter": filter_of(&request.text)? }
            }
            QueryLanguage::MongoAggregation => {
                doc! { "aggregate": &collection, "pipeline": pipeline_of(&request.text, true)?, "cursor": {} }
            }
            _ => {
                return Err(ConnectorError::invalid_input(
                    "MongoDB connections run MongoDB filters and pipelines only",
                ));
            }
        };
        let verbosity = if analyze {
            "executionStats"
        } else {
            "queryPlanner"
        };
        let fut = db.run_command(doc! { "explain": inner_cmd, "verbosity": verbosity });
        let plan = tokio::select! {
            r = fut => r.map_err(map_mongo)?,
            _ = cancel.cancelled() => return Err(ConnectorError::Cancelled),
        };
        let json: serde_json::Value = Bson::Document(plan).into_relaxed_extjson();
        let winning = json
            .pointer("/queryPlanner/winningPlan")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let text = serde_json::to_string_pretty(&winning).unwrap_or_default();
        Ok(ExplainPlan {
            native: json,
            text,
            analyzed: analyze,
        })
    }
}

async fn run_query(
    inner: &Inner,
    request: &QueryRequest,
    cancel: &CancellationToken,
    session: Option<&mut ClientSession>,
) -> ConnectorResult<QueryResult> {
    if !request.params.is_empty() {
        return Err(ConnectorError::invalid_input(
            "MongoDB queries take values inline as Extended JSON, not parameters",
        ));
    }
    let started = Instant::now();
    let collection = request
        .collection
        .clone()
        .ok_or_else(|| ConnectorError::invalid_input("a collection is required"))?;
    let coll = inner.collection(&ObjectRef::collection(
        inner.database_name(&NamespacePath {
            database: request.database.clone(),
            schema: None,
        })?,
        collection,
    ))?;
    let limit = i64::from(request.max_rows).saturating_add(1);
    let max_time = request.timeout;
    let work = async {
        let cursor_docs: Vec<Document> = match request.language {
            QueryLanguage::MongoFilter => {
                let mut find = coll.find(filter_of(&request.text)?).limit(limit);
                if let Some(t) = max_time {
                    find = find.max_time(t);
                }
                match session {
                    Some(s) => {
                        let mut c = find.session(&mut *s).await.map_err(map_mongo)?;
                        let mut out = Vec::new();
                        while let Some(d) = c.next(&mut *s).await {
                            out.push(d.map_err(map_mongo)?);
                        }
                        out
                    }
                    None => find
                        .await
                        .map_err(map_mongo)?
                        .try_collect()
                        .await
                        .map_err(map_mongo)?,
                }
            }
            QueryLanguage::MongoAggregation => {
                let mut pipeline = pipeline_of(&request.text, inner.read_only)?;
                let writes = pipeline
                    .iter()
                    .any(|d| d.keys().any(|k| k == "$out" || k == "$merge"));
                if !writes {
                    pipeline.push(doc! { "$limit": limit });
                }
                let mut agg = coll.aggregate(pipeline);
                if let Some(t) = max_time {
                    agg = agg.max_time(t);
                }
                match session {
                    Some(s) => {
                        let mut c = agg.session(&mut *s).await.map_err(map_mongo)?;
                        let mut out = Vec::new();
                        while let Some(d) = c.next(&mut *s).await {
                            out.push(d.map_err(map_mongo)?);
                        }
                        out
                    }
                    None => agg
                        .await
                        .map_err(map_mongo)?
                        .try_collect()
                        .await
                        .map_err(map_mongo)?,
                }
            }
            _ => {
                return Err(ConnectorError::invalid_input(
                    "MongoDB connections run MongoDB filters and pipelines only",
                ));
            }
        };
        Ok::<_, ConnectorError>(cursor_docs)
    };
    let timeout = max_time.map(|t| t + Duration::from_secs(1));
    let docs = tokio::select! {
        r = async {
            match timeout {
                Some(t) => tokio::time::timeout(t, work).await.map_err(|_| ConnectorError::new(ErrorKind::Timeout, "query timed out"))?,
                None => work.await,
            }
        } => r?,
        _ = cancel.cancelled() => return Err(ConnectorError::Cancelled),
    };
    let truncated = docs.len() > request.max_rows as usize;
    let rows: Vec<Record> = docs
        .iter()
        .take(request.max_rows as usize)
        .map(document_to_record)
        .collect();
    Ok(QueryResult {
        columns: columns_from(&rows),
        rows,
        affected: None,
        truncated,
        elapsed: started.elapsed(),
        notices: Vec::new(),
    })
}

/// A multi-document transaction (replica sets and sharded clusters only).
struct MongoTransaction {
    inner: Arc<Inner>,
    session: Option<ClientSession>,
}

impl MongoTransaction {
    fn session(&mut self) -> ConnectorResult<&mut ClientSession> {
        self.session
            .as_mut()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))
    }
}

#[async_trait]
impl Transaction for MongoTransaction {
    async fn write_batch(&mut self, batch: WriteBatch) -> ConnectorResult<WriteOutcome> {
        let coll = self.inner.collection(&batch.object)?;
        let atomic = WriteBatch {
            atomic: true,
            ..batch
        };
        write(&coll, &atomic, Some(self.session()?)).await
    }

    async fn update(&mut self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        if !request.filter.is_restrictive() {
            return Err(ConnectorError::invalid_input(
                "an update without a restrictive filter is not allowed",
            ));
        }
        let coll = self.inner.collection(&request.object)?;
        let r = coll
            .update_many(to_query(&request.filter)?, set_document(&request.set)?)
            .session(self.session()?)
            .await
            .map_err(map_mongo)?;
        check_expected(r.matched_count, request.expect_matched)?;
        Ok(MutationOutcome {
            matched: r.matched_count,
            modified: r.modified_count,
        })
    }

    async fn delete(&mut self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        if !request.filter.is_restrictive() {
            return Err(ConnectorError::invalid_input(
                "a delete without a restrictive filter is not allowed",
            ));
        }
        let coll = self.inner.collection(&request.object)?;
        let r = coll
            .delete_many(to_query(&request.filter)?)
            .session(self.session()?)
            .await
            .map_err(map_mongo)?;
        check_expected(r.deleted_count, request.expect_matched)?;
        Ok(MutationOutcome {
            matched: r.deleted_count,
            modified: r.deleted_count,
        })
    }

    async fn execute(&mut self, request: QueryRequest) -> ConnectorResult<QueryResult> {
        let inner = self.inner.clone();
        run_query(
            &inner,
            &request,
            &CancellationToken::new(),
            Some(self.session()?),
        )
        .await
    }

    async fn commit(mut self: Box<Self>) -> ConnectorResult<()> {
        let mut s = self
            .session
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        s.commit_transaction().await.map_err(map_mongo)
    }

    async fn rollback(mut self: Box<Self>) -> ConnectorResult<()> {
        let mut s = self
            .session
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        s.abort_transaction().await.map_err(map_mongo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyset_uses_expressions_with_null_normalisation() {
        let d = after_expr(
            &[
                ("id".into(), SortDirection::Asc),
                ("_id".into(), SortDirection::Asc),
            ],
            &[Bson::Int64(5), Bson::Int32(1)],
        );
        let expected = doc! { "$expr": { "$or": [
            { "$and": [ { "$gt": [ { "$ifNull": ["$id", Bson::Null] }, 5i64 ] } ] },
            { "$and": [ { "$eq": [ { "$ifNull": ["$id", Bson::Null] }, 5i64 ] }, { "$gt": ["$_id", 1i32] } ] },
        ] } };
        assert_eq!(d, expected);
    }

    #[test]
    fn read_only_pipelines_cannot_write() {
        assert!(matches!(
            pipeline_of(r#"[{"$match": {}}, {"$out": "x"}]"#, true),
            Err(ConnectorError::ReadOnly)
        ));
        assert!(pipeline_of(r#"[{"$match": {"a": {"$gt": 1}}}]"#, true).is_ok());
        assert!(pipeline_of(r#"{"$match": {}}"#, false).is_err());
    }

    #[test]
    fn updates_cannot_touch_id_and_missing_unsets() {
        let mut set = Record::new();
        set.insert("a", Value::Int(1));
        set.insert("b", Value::Missing);
        assert_eq!(
            set_document(&set).unwrap(),
            doc! { "$set": { "a": 1i64 }, "$unset": { "b": "" } }
        );
        let mut bad = Record::new();
        bad.insert("_id", Value::Int(1));
        assert!(set_document(&bad).is_err());
    }

    #[test]
    fn nested_key_values_are_extracted() {
        let d = doc! { "a": { "b": 3 } };
        assert_eq!(key_value(&d, "a.b"), Bson::Int32(3));
        assert_eq!(key_value(&d, "a.c"), Bson::Null);
    }
}
