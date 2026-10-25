//! The PostgreSQL `Connection` implementation.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use deadpool_postgres::{Object, Pool};
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::connector::{CountResult, HealthReport, InspectOptions, Transaction};
use gazabridge_plugin_sdk::ddl::{DdlPlan, SchemaOperation};
use gazabridge_plugin_sdk::filter::Filter;
use gazabridge_plugin_sdk::profile::Version;
use gazabridge_plugin_sdk::query::{ExplainPlan, QueryRequest, QueryResult};
use gazabridge_plugin_sdk::read::{Page, PageRequest, ReadRequest, RecordStream};
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, NamespaceEntry, NamespacePath, ObjectKind, ObjectRef, ObjectSchema,
};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{
    CapabilityManifest, Connection, ConnectorError, ConnectorResult, ErrorKind, ServerInfo,
};
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{Client, Row, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;

use crate::error::{map_pg, map_pool};
use crate::meta::{self, TableMeta};
use crate::read::{self, decode_column};
use crate::sql::{self, quote_ident};
use crate::types::{
    ColumnShape, PgType, TextParam, as_params, canonical_for, encode, native_type_for,
};
use crate::write;

type MetaKey = (String, String);

pub struct Inner {
    pub pool: Pool,
    pub tls: MakeRustlsConnect,
    pub manifest: CapabilityManifest,
    pub read_only: bool,
    meta: Mutex<HashMap<MetaKey, Arc<TableMeta>>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

impl Inner {
    pub async fn client(&self) -> ConnectorResult<Object> {
        self.pool.get().await.map_err(map_pool)
    }

    fn ensure_writable(&self) -> ConnectorResult<()> {
        if self.read_only {
            Err(ConnectorError::ReadOnly)
        } else {
            Ok(())
        }
    }

    /// Runs a query, asking the server to cancel it if `cancel` fires.
    pub async fn query_cancellable(
        &self,
        sql: &str,
        params: &[TextParam],
        cancel: &CancellationToken,
    ) -> ConnectorResult<Vec<Row>> {
        let client = self.client().await?;
        let token = client.cancel_token();
        let refs = as_params(params);
        tokio::select! {
            r = client.query(sql, &refs) => r.map_err(map_pg),
            _ = cancel.cancelled() => {
                let _ = token.cancel_query(self.tls.clone()).await;
                Err(ConnectorError::Cancelled)
            }
        }
    }

    async fn table(&self, object: &ObjectRef) -> ConnectorResult<Arc<TableMeta>> {
        let key = (
            object.namespace.schema.clone().unwrap_or_default(),
            object.name.clone(),
        );
        if let Some(m) = self.meta.lock().ok().and_then(|c| c.get(&key).cloned()) {
            return Ok(m);
        }
        let client = self.client().await?;
        let m = Arc::new(meta::load(&client, object).await?);
        if let Ok(mut c) = self.meta.lock() {
            c.insert(key, m.clone());
        }
        Ok(m)
    }

    fn invalidate(&self) {
        if let Ok(mut c) = self.meta.lock() {
            c.clear();
        }
    }
}

/// Ends a transaction on a pooled client. If ROLLBACK itself fails the
/// client is removed from the pool so an open transaction is never reused.
async fn finish<T>(client: Object, result: ConnectorResult<T>) -> ConnectorResult<T> {
    match result {
        Ok(v) => match client.batch_execute("COMMIT").await {
            Ok(()) => Ok(v),
            Err(e) => {
                let _ = Object::take(client);
                Err(map_pg(e))
            }
        },
        Err(e) => {
            if client.batch_execute("ROLLBACK").await.is_err() {
                let _ = Object::take(client);
            }
            Err(e)
        }
    }
}

/// An open PostgreSQL connection pool.
#[derive(Debug, Clone)]
pub struct PostgresConnection {
    inner: Arc<Inner>,
}

impl PostgresConnection {
    pub(crate) fn new(
        pool: Pool,
        tls: MakeRustlsConnect,
        manifest: CapabilityManifest,
        read_only: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                tls,
                manifest,
                read_only,
                meta: Mutex::new(HashMap::new()),
            }),
        }
    }
}

/// Canonical type of a result column type reported by the driver.
pub fn canonical_for_type(ty: &Type) -> CanonicalType {
    match ty.kind() {
        Kind::Array(elem) => match canonical_for_type(elem) {
            CanonicalType::Extension { .. } => CanonicalType::Extension {
                name: ty.name().to_owned(),
            },
            inner => CanonicalType::Array(Box::new(inner)),
        },
        Kind::Domain(base) => canonical_for_type(base),
        Kind::Enum(labels) => CanonicalType::Enum {
            name: Some(ty.name().to_owned()),
            labels: labels.clone(),
        },
        Kind::Simple => canonical_for(&PgType {
            name: ty.name().to_owned(),
            typtype: "b".into(),
            typmod: -1,
            ..Default::default()
        }),
        _ => CanonicalType::Extension {
            name: ty.name().to_owned(),
        },
    }
}

fn require_language(req: &QueryRequest) -> ConnectorResult<()> {
    if req.language != QueryLanguage::PostgresSql {
        return Err(ConnectorError::invalid_input(
            "PostgreSQL connections execute PostgreSQL SQL only",
        ));
    }
    Ok(())
}

fn params_of(req: &QueryRequest) -> ConnectorResult<Vec<TextParam>> {
    req.params
        .iter()
        .map(|v| encode(v, ColumnShape::Unknown))
        .collect()
}

enum Outcome {
    Done(QueryResult),
    /// Multi-statement scripts and statements that cannot run in a
    /// transaction block go through the simple-query protocol.
    UseSimpleProtocol,
}

fn needs_simple(e: &tokio_postgres::Error) -> bool {
    e.as_db_error().is_some_and(|db| {
        (db.code().code() == "42601" && db.message().contains("multiple commands"))
            || db.code().code() == "25001"
    })
}

async fn run_extended(
    client: &mut Object,
    req: &QueryRequest,
    read_only: bool,
) -> ConnectorResult<Outcome> {
    let started = Instant::now();
    let params = params_of(req)?;
    let simple_allowed = !read_only && params.is_empty();
    let tx = client
        .build_transaction()
        .read_only(read_only)
        .start()
        .await
        .map_err(map_pg)?;
    let stmt = match tx.prepare(&req.text).await {
        Ok(s) => s,
        Err(e) if simple_allowed && needs_simple(&e) => return Ok(Outcome::UseSimpleProtocol),
        Err(e) if read_only && needs_simple(&e) => {
            return Err(ConnectorError::invalid_input(
                "read-only connections run one statement at a time",
            ));
        }
        Err(e) => return Err(map_pg(e)),
    };
    if stmt.params().len() != params.len() {
        return Err(ConnectorError::invalid_input(format!(
            "the statement expects {} parameter(s) but {} were given",
            stmt.params().len(),
            params.len()
        )));
    }
    let columns: Vec<ColumnInfo> = stmt
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo {
            name: c.name().to_owned(),
            native_type: c.type_().name().to_owned(),
            canonical_type: canonical_for_type(c.type_()),
            nullable: true,
            default_expression: None,
            is_generated: false,
            is_identity: false,
            comment: None,
            ordinal: i as u32 + 1,
        })
        .collect();
    let refs = as_params(&params);
    let (rows, affected, truncated) = if columns.is_empty() {
        match tx.execute(&stmt, &refs).await {
            Ok(n) => (Vec::new(), Some(n), false),
            Err(e) if simple_allowed && needs_simple(&e) => return Ok(Outcome::UseSimpleProtocol),
            Err(e) => return Err(map_pg(e)),
        }
    } else {
        let portal = tx.bind(&stmt, &refs).await.map_err(map_pg)?;
        let limit = i32::try_from(req.max_rows.saturating_add(1)).unwrap_or(i32::MAX);
        let fetched = tx.query_portal(&portal, limit).await.map_err(map_pg)?;
        let truncated = fetched.len() > req.max_rows as usize;
        let mut rows = Vec::with_capacity(fetched.len().min(req.max_rows as usize));
        for row in fetched.iter().take(req.max_rows as usize) {
            let mut record = Record::with_capacity(columns.len());
            for (i, c) in columns.iter().enumerate() {
                record.insert(c.name.clone(), decode_column(row, i, None)?);
            }
            rows.push(record);
        }
        (rows, None, truncated)
    };
    tx.commit().await.map_err(map_pg)?;
    Ok(Outcome::Done(QueryResult {
        columns,
        rows,
        affected,
        truncated,
        elapsed: started.elapsed(),
        notices: Vec::new(),
    }))
}

async fn run_simple(client: &Client, req: &QueryRequest) -> ConnectorResult<QueryResult> {
    let started = Instant::now();
    let messages = client.simple_query(&req.text).await.map_err(map_pg)?;
    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut rows = Vec::new();
    let mut affected = 0u64;
    let mut saw_command = false;
    let mut rows_in_statement = 0u64;
    let mut truncated = false;
    for m in messages {
        match m {
            SimpleQueryMessage::Row(r) => {
                rows_in_statement += 1;
                if columns.is_empty() {
                    columns = r
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(i, c)| ColumnInfo {
                            name: c.name().to_owned(),
                            native_type: "text".into(),
                            canonical_type: CanonicalType::TEXT,
                            nullable: true,
                            default_expression: None,
                            is_generated: false,
                            is_identity: false,
                            comment: None,
                            ordinal: i as u32 + 1,
                        })
                        .collect();
                }
                if rows.len() >= req.max_rows as usize {
                    truncated = true;
                    continue;
                }
                let mut record = Record::with_capacity(columns.len());
                for (i, c) in columns.iter().enumerate() {
                    let v = r
                        .get(i)
                        .map_or(Value::Null, |s| Value::String(s.to_owned()));
                    record.insert(c.name.clone(), v);
                }
                rows.push(record);
            }
            SimpleQueryMessage::CommandComplete(n) => {
                saw_command = true;
                // Row counts of SELECT-like statements are not "affected" rows.
                if rows_in_statement == 0 {
                    affected += n;
                }
                rows_in_statement = 0;
            }
            _ => {}
        }
    }
    Ok(QueryResult {
        affected: (saw_command && rows.is_empty()).then_some(affected),
        columns,
        rows,
        truncated,
        elapsed: started.elapsed(),
        notices: vec![
            "executed with the simple-query protocol; values are returned as text".into(),
        ],
    })
}

fn render_plan(node: &serde_json::Value, depth: usize, out: &mut String) {
    let get = |k: &str| {
        node.get(k)
            .map(|v| v.to_string().trim_matches('"').to_owned())
    };
    // PostgreSQL's text format names ModifyTable nodes by their operation.
    let name = match (get("Node Type").as_deref(), get("Operation")) {
        (Some("ModifyTable"), Some(op)) => op,
        (Some(t), _) => t.to_owned(),
        (None, _) => "?".into(),
    };
    let mut line = format!("{}{name}", "  ".repeat(depth));
    if let Some(rel) = get("Relation Name") {
        line.push_str(&format!(" on {rel}"));
    }
    if let (Some(cost), Some(rows)) = (get("Total Cost"), get("Plan Rows")) {
        line.push_str(&format!("  (cost={cost} rows={rows})"));
    }
    if let (Some(rows), Some(time)) = (get("Actual Rows"), get("Actual Total Time")) {
        line.push_str(&format!("  (actual rows={rows} time={time} ms)"));
    }
    out.push_str(&line);
    out.push('\n');
    if let Some(children) = node.get("Plans").and_then(|p| p.as_array()) {
        for c in children {
            render_plan(c, depth + 1, out);
        }
    }
}

fn entry(name: String, kind: ObjectKind, estimated_count: Option<u64>) -> NamespaceEntry {
    NamespaceEntry {
        name,
        kind,
        comment: None,
        estimated_count,
    }
}

#[async_trait]
impl Connection for PostgresConnection {
    fn manifest(&self) -> &CapabilityManifest {
        &self.inner.manifest
    }

    fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        let client = self.inner.client().await?;
        let started = Instant::now();
        client.simple_query("SELECT 1").await.map_err(map_pg)?;
        Ok(HealthReport {
            latency: started.elapsed(),
            checked_at: Utc::now(),
            notes: Vec::new(),
        })
    }

    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        let client = self.inner.client().await?;
        let row = client
            .query_one(
                "SELECT version(), current_setting('server_version'), current_database()::text",
                &[],
            )
            .await
            .map_err(map_pg)?;
        let banner: String = row.get(0);
        let server_version: String = row.get(1);
        let (product, version) = if let Some(rest) = banner.split("CockroachDB").nth(1) {
            let v = rest
                .split_whitespace()
                .find(|w| w.starts_with('v'))
                .map(|w| w.trim_start_matches('v').to_owned())
                .unwrap_or(server_version.clone());
            ("CockroachDB".to_owned(), v)
        } else if banner.contains("EnterpriseDB") {
            ("EDB Postgres".to_owned(), server_version.clone())
        } else {
            ("PostgreSQL".to_owned(), server_version.clone())
        };
        let mut properties = BTreeMap::new();
        properties.insert("database".to_owned(), row.get::<_, String>(2));
        for setting in [
            "wal_level",
            "server_encoding",
            "max_replication_slots",
            "TimeZone",
            "transaction_read_only",
        ] {
            if let Ok(Some(r)) = client
                .query_opt(
                    "SELECT current_setting($1::text, true)",
                    &as_params(&[TextParam(Some(setting.into()))]),
                )
                .await
                && let Some(v) = r.get::<_, Option<String>>(0)
            {
                properties.insert(setting.to_owned(), v);
            }
        }
        if let Ok(r) = client.query_one("SELECT pg_is_in_recovery()", &[]).await {
            properties.insert("in_recovery".into(), r.get::<_, bool>(0).to_string());
        }
        Ok(ServerInfo {
            family: EngineFamily::Postgres,
            product,
            version: Version(version),
            version_string: banner,
            properties,
        })
    }

    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        let client = self.inner.client().await?;
        match (&parent.database, &parent.schema) {
            (None, None) => {
                let rows = client
                    .query(
                        "SELECT datname::text FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname",
                        &[],
                    )
                    .await
                    .map_err(map_pg)?;
                Ok(rows
                    .iter()
                    .map(|r| entry(r.get(0), ObjectKind::Database, None))
                    .collect())
            }
            (Some(db), None) => {
                let current: String = client
                    .query_one("SELECT current_database()::text", &[])
                    .await
                    .map_err(map_pg)?
                    .get(0);
                if *db != current {
                    return Err(ConnectorError::new(
                        ErrorKind::InvalidInput,
                        format!("browsing `{db}` needs a connection to that database"),
                    ));
                }
                let rows = client
                    .query(
                        r"SELECT nspname::text FROM pg_namespace
                          WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                            AND nspname NOT LIKE 'pg\_toast%' AND nspname NOT LIKE 'pg\_temp\_%'
                          ORDER BY nspname",
                        &[],
                    )
                    .await
                    .map_err(map_pg)?;
                Ok(rows
                    .iter()
                    .map(|r| entry(r.get(0), ObjectKind::Schema, None))
                    .collect())
            }
            (_, Some(schema)) => {
                let schema_param = [TextParam(Some(schema.clone()))];
                let p = as_params(&schema_param);
                let mut out = Vec::new();
                for r in client
                    .query(
                        r"SELECT c.relname::text, c.relkind::text, c.relispartition, c.reltuples::float8
                          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = $1::text AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
                          ORDER BY c.relname",
                        &p,
                    )
                    .await
                    .map_err(map_pg)?
                {
                    let relkind: String = r.get(1);
                    let kind = match relkind.as_str() {
                        "v" => ObjectKind::View,
                        "m" => ObjectKind::MaterializedView,
                        "S" => ObjectKind::Sequence,
                        _ if r.get::<_, bool>(2) => ObjectKind::Partition,
                        _ => ObjectKind::Table,
                    };
                    let tuples: f64 = r.get(3);
                    out.push(entry(r.get(0), kind, (tuples >= 0.0 && kind != ObjectKind::Sequence).then_some(tuples as u64)));
                }
                for r in client
                    .query(
                        r"SELECT p.proname::text, p.prokind::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                          WHERE n.nspname = $1::text AND p.prokind IN ('f', 'p') ORDER BY p.proname",
                        &p,
                    )
                    .await
                    .map_err(map_pg)?
                {
                    let kind = if r.get::<_, String>(1) == "p" { ObjectKind::Procedure } else { ObjectKind::Function };
                    out.push(entry(r.get(0), kind, None));
                }
                for r in client
                    .query(
                        r"SELECT t.typname::text FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                          WHERE n.nspname = $1::text AND t.typtype = 'e' ORDER BY t.typname",
                        &p,
                    )
                    .await
                    .map_err(map_pg)?
                {
                    out.push(entry(r.get(0), ObjectKind::Enum, None));
                }
                for r in client
                    .query(
                        r"SELECT DISTINCT tg.tgname::text FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid
                          JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = $1::text AND NOT tg.tgisinternal ORDER BY 1",
                        &p,
                    )
                    .await
                    .map_err(map_pg)?
                {
                    out.push(entry(r.get(0), ObjectKind::Trigger, None));
                }
                Ok(out)
            }
        }
    }

    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        quote_ident(ident)
    }

    async fn close(&self) {
        self.inner.pool.close();
    }

    async fn inspect(
        &self,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        let m = self.inner.table(object).await?;
        let client = self.inner.client().await?;
        let indexes = if options.include_indexes {
            meta::indexes(&client, object).await?
        } else {
            Vec::new()
        };
        let constraints = if options.include_constraints {
            meta::constraints(&client, object, &m.schema).await?
        } else {
            Vec::new()
        };
        Ok(ObjectSchema {
            object: ObjectRef {
                kind: m.kind,
                namespace: NamespacePath::schema(
                    object.namespace.database.clone(),
                    m.schema.clone(),
                ),
                name: object.name.clone(),
            },
            columns: m.columns.clone(),
            primary_key: m.primary_key.clone(),
            indexes,
            constraints,
            inferred: None,
            estimated_count: m.estimated_rows,
            comment: m.comment.clone(),
            definition: None,
        })
    }

    async fn read(
        &self,
        request: ReadRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        let m = self.inner.table(&request.object).await?;
        read::open(self.inner.clone(), m, &request, cancel)
    }

    async fn read_page(
        &self,
        request: PageRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<Page> {
        let m = self.inner.table(&request.object).await?;
        read::page(&self.inner, &m, &request, &cancel).await
    }

    async fn count_within(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        max_time: std::time::Duration,
    ) -> ConnectorResult<CountResult> {
        let m = self.inner.table(object).await?;
        Ok(CountResult {
            count: read::count_within(&self.inner, &m, object, filter, max_time).await?,
            exact: true,
        })
    }

    async fn count(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        exact: bool,
    ) -> ConnectorResult<CountResult> {
        let m = self.inner.table(object).await?;
        if exact {
            Ok(CountResult {
                count: read::count(&self.inner, &m, object, filter).await?,
                exact: true,
            })
        } else if filter.is_some() {
            Err(ConnectorError::invalid_input(
                "estimated counts cannot be filtered",
            ))
        } else {
            m.estimated_rows
                .map(|count| CountResult {
                    count,
                    exact: false,
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ErrorKind::NotFound,
                        "no statistics yet; run ANALYZE or request an exact count",
                    )
                })
        }
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&batch.object).await?;
        let client = self.inner.client().await?;
        let token = client.cancel_token();
        client.batch_execute("BEGIN").await.map_err(map_pg)?;
        let result = tokio::select! {
            r = write::write_records(&client, &m, &batch) => r,
            _ = cancel.cancelled() => {
                let _ = token.cancel_query(self.inner.tls.clone()).await;
                Err(ConnectorError::Cancelled)
            }
        };
        finish(client, result).await
    }

    async fn update(&self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let client = self.inner.client().await?;
        client.batch_execute("BEGIN").await.map_err(map_pg)?;
        let result = write::update(&client, &m, &request).await;
        finish(client, result).await
    }

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let client = self.inner.client().await?;
        client.batch_execute("BEGIN").await.map_err(map_pg)?;
        let result = write::delete(&client, &m, &request).await;
        finish(client, result).await
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        self.inner.ensure_writable()?;
        let client = self.inner.client().await?;
        client.batch_execute("BEGIN").await.map_err(map_pg)?;
        Ok(Box::new(PgTransaction {
            inner: self.inner.clone(),
            client: Some(client),
        }))
    }

    fn plan_ddl(&self, ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
        sql::plan(ops)
    }

    async fn apply_ddl(
        &self,
        ops: &[SchemaOperation],
        cancel: CancellationToken,
    ) -> ConnectorResult<()> {
        self.inner.ensure_writable()?;
        let plan = sql::plan(ops)?;
        let client = self.inner.client().await?;
        let token = client.cancel_token();
        client.batch_execute("BEGIN").await.map_err(map_pg)?;
        let result = tokio::select! {
            r = async {
                for stmt in &plan.statements {
                    client.batch_execute(stmt).await.map_err(map_pg)?;
                }
                Ok(())
            } => r,
            _ = cancel.cancelled() => {
                let _ = token.cancel_query(self.inner.tls.clone()).await;
                Err(ConnectorError::Cancelled)
            }
        };
        let r = finish(client, result).await;
        self.inner.invalidate();
        r
    }

    fn native_type_for(&self, canonical: &CanonicalType) -> ConnectorResult<String> {
        native_type_for(canonical)
    }

    async fn execute(
        &self,
        request: QueryRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<QueryResult> {
        require_language(&request)?;
        let mut client = self.inner.client().await?;
        let token = client.cancel_token();
        let read_only = self.inner.read_only;
        let work = async {
            let fut = run_extended(&mut client, &request, read_only);
            let outcome = match request.timeout {
                Some(t) => tokio::time::timeout(t, fut)
                    .await
                    .map_err(|_| ConnectorError::new(ErrorKind::Timeout, "query timed out"))??,
                None => fut.await?,
            };
            match outcome {
                Outcome::Done(r) => Ok(r),
                Outcome::UseSimpleProtocol => run_simple(&client, &request).await,
            }
        };
        let result = tokio::select! {
            r = work => r,
            _ = cancel.cancelled() => Err(ConnectorError::Cancelled),
        };
        if matches!(result, Err(ref e) if matches!(e.kind(), ErrorKind::Cancelled | ErrorKind::Timeout))
        {
            let _ = token.cancel_query(self.inner.tls.clone()).await;
        }
        self.inner.invalidate();
        result
    }

    async fn explain(
        &self,
        request: QueryRequest,
        analyze: bool,
        cancel: CancellationToken,
    ) -> ConnectorResult<ExplainPlan> {
        require_language(&request)?;
        let params = params_of(&request)?;
        let mut client = self.inner.client().await?;
        let token = client.cancel_token();
        let read_only = self.inner.read_only;
        let sql = if analyze {
            format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}", request.text)
        } else {
            format!("EXPLAIN (FORMAT JSON) {}", request.text)
        };
        let work = async {
            // EXPLAIN ANALYZE executes the statement; always roll back.
            let tx = client
                .build_transaction()
                .read_only(read_only)
                .start()
                .await
                .map_err(map_pg)?;
            let row = tx
                .query_one(&sql, &as_params(&params))
                .await
                .map_err(map_pg)?;
            // The plan column is `json` or `text` depending on the protocol path.
            let plan = match decode_column(&row, 0, None)? {
                Value::Json(j) => j,
                Value::String(raw) => serde_json::from_str(&raw).map_err(|e| {
                    ConnectorError::new(
                        ErrorKind::Protocol,
                        format!("unexpected EXPLAIN output: {e}"),
                    )
                })?,
                other => {
                    return Err(ConnectorError::new(
                        ErrorKind::Protocol,
                        format!("unexpected EXPLAIN output of kind {}", other.kind()),
                    ));
                }
            };
            tx.rollback().await.map_err(map_pg)?;
            Ok::<_, ConnectorError>(plan)
        };
        let plan = tokio::select! {
            r = work => r,
            _ = cancel.cancelled() => {
                let _ = token.cancel_query(self.inner.tls.clone()).await;
                Err(ConnectorError::Cancelled)
            }
        }?;
        let mut text = String::new();
        if let Some(root) = plan.get(0).and_then(|p| p.get("Plan")) {
            render_plan(root, 0, &mut text);
        }
        Ok(ExplainPlan {
            native: plan,
            text,
            analyzed: analyze,
        })
    }
}

/// A transaction on a dedicated pooled connection. Dropped without commit
/// or rollback, the connection is closed so the server rolls it back.
struct PgTransaction {
    inner: Arc<Inner>,
    client: Option<Object>,
}

impl PgTransaction {
    fn client(&self) -> ConnectorResult<&Object> {
        self.client
            .as_ref()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        if let Some(c) = self.client.take() {
            drop(Object::take(c));
        }
    }
}

#[async_trait]
impl Transaction for PgTransaction {
    async fn write_batch(&mut self, batch: WriteBatch) -> ConnectorResult<WriteOutcome> {
        let m = self.inner.table(&batch.object).await?;
        write::write_records(self.client()?, &m, &batch).await
    }

    async fn update(&mut self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::update(self.client()?, &m, &request).await
    }

    async fn delete(&mut self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::delete(self.client()?, &m, &request).await
    }

    async fn execute(&mut self, request: QueryRequest) -> ConnectorResult<QueryResult> {
        require_language(&request)?;
        let params = params_of(&request)?;
        let started = Instant::now();
        let client = self.client()?;
        let stmt = client.prepare(&request.text).await.map_err(map_pg)?;
        let refs = as_params(&params);
        if stmt.columns().is_empty() {
            let n = client.execute(&stmt, &refs).await.map_err(map_pg)?;
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected: Some(n),
                truncated: false,
                elapsed: started.elapsed(),
                notices: Vec::new(),
            });
        }
        let fetched = client.query(&stmt, &refs).await.map_err(map_pg)?;
        let columns: Vec<ColumnInfo> = stmt
            .columns()
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnInfo {
                name: c.name().to_owned(),
                native_type: c.type_().name().to_owned(),
                canonical_type: canonical_for_type(c.type_()),
                nullable: true,
                default_expression: None,
                is_generated: false,
                is_identity: false,
                comment: None,
                ordinal: i as u32 + 1,
            })
            .collect();
        let truncated = fetched.len() > request.max_rows as usize;
        let mut rows = Vec::new();
        for row in fetched.iter().take(request.max_rows as usize) {
            let mut record = Record::with_capacity(columns.len());
            for (i, c) in columns.iter().enumerate() {
                record.insert(c.name.clone(), decode_column(row, i, None)?);
            }
            rows.push(record);
        }
        Ok(QueryResult {
            columns,
            rows,
            affected: None,
            truncated,
            elapsed: started.elapsed(),
            notices: Vec::new(),
        })
    }

    async fn commit(mut self: Box<Self>) -> ConnectorResult<()> {
        let client = self
            .client
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        finish(client, Ok(())).await
    }

    async fn rollback(mut self: Box<Self>) -> ConnectorResult<()> {
        let client = self
            .client
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        match client.batch_execute("ROLLBACK").await {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = Object::take(client);
                Err(map_pg(e))
            }
        }
    }
}

/// Default schema for scratch objects.
pub fn public_schema() -> NamespacePath {
    NamespacePath::schema(None, "public")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_render_as_an_indented_tree() {
        let plan = serde_json::json!({
            "Node Type": "Hash Join", "Total Cost": 10.5, "Plan Rows": 3,
            "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders", "Total Cost": 1.0, "Plan Rows": 3,
                 "Actual Rows": 3, "Actual Total Time": 0.01},
                {"Node Type": "Hash", "Total Cost": 2.0, "Plan Rows": 1}
            ]
        });
        let mut text = String::new();
        render_plan(&plan, 0, &mut text);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "Hash Join  (cost=10.5 rows=3)");
        assert_eq!(
            lines[1],
            "  Seq Scan on orders  (cost=1.0 rows=3)  (actual rows=3 time=0.01 ms)"
        );
        assert_eq!(lines[2], "  Hash  (cost=2.0 rows=1)");
    }

    #[test]
    fn driver_types_map_to_canonical_types() {
        assert_eq!(
            canonical_for_type(&Type::INT4_ARRAY),
            CanonicalType::Array(Box::new(CanonicalType::Int { bits: 32 }))
        );
        assert_eq!(
            canonical_for_type(&Type::TIMESTAMPTZ),
            CanonicalType::UtcDateTime {
                fractional_digits: 6
            }
        );
        assert!(matches!(
            canonical_for_type(&Type::INET),
            CanonicalType::Extension { .. }
        ));
    }
}
