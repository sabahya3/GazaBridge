//! The connector contract suite.
//!
//! The suite exercises a live connection through the public
//! [`Connection`] trait only. It checks positive behaviour for every
//! capability a connector declares, and that undeclared capabilities are
//! rejected with [`ConnectorError::Unsupported`] rather than half-working.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::conversion::{ConversionPolicy, convert};
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::capability::{Capability, QueryLanguage};
use gazabridge_plugin_sdk::cdc::ChangeFeedRequest;
use gazabridge_plugin_sdk::connector::Connection;
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, SchemaOperation, TableDefinition};
use gazabridge_plugin_sdk::error::{ConnectorError, ErrorKind};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::read::{PageRequest, ReadRequest};
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::write::{DeleteRequest, UpdateRequest, WriteBatch};
use gazabridge_plugin_sdk::{API_VERSION, ConnectorResult};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::fixtures::{round_trip_columns, round_trip_records};

const ROWS: usize = 30;

/// Version of the contract suite, recorded in profile contract evidence.
pub const SUITE_VERSION: &str = "1.0";

/// Supplies connections to a disposable target for the suite.
#[async_trait]
pub trait ContractHarness: Send + Sync {
    fn family(&self) -> EngineFamily;
    /// Namespace in which the suite may create and drop objects.
    fn scratch_namespace(&self) -> NamespacePath;
    async fn connect(&self) -> ConnectorResult<Arc<dyn Connection>>;
    async fn connect_read_only(&self) -> ConnectorResult<Arc<dyn Connection>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "detail", rename_all = "snake_case")]
pub enum CaseOutcome {
    Passed,
    Failed(String),
    NotApplicable(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CaseResult {
    pub name: &'static str,
    pub outcome: CaseOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContractReport {
    pub connector_id: String,
    pub server: Option<String>,
    pub cases: Vec<CaseResult>,
}

impl ContractReport {
    pub fn failures(&self) -> Vec<&CaseResult> {
        self.cases
            .iter()
            .filter(|c| matches!(c.outcome, CaseOutcome::Failed(_)))
            .collect()
    }

    pub fn passed(&self) -> bool {
        self.failures().is_empty()
    }

    /// Panics with a readable summary if any case failed.
    pub fn assert_passed(&self) {
        assert!(self.passed(), "contract suite failed:\n{self}");
    }
}

impl fmt::Display for ContractReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "connector {} ({})",
            self.connector_id,
            self.server.as_deref().unwrap_or("unknown server")
        )?;
        for c in &self.cases {
            match &c.outcome {
                CaseOutcome::Passed => writeln!(f, "  PASS {}", c.name)?,
                CaseOutcome::Failed(m) => writeln!(f, "  FAIL {}: {m}", c.name)?,
                CaseOutcome::NotApplicable(m) => writeln!(f, "  N/A  {}: {m}", c.name)?,
            }
        }
        Ok(())
    }
}

type CaseResultInner = Result<CaseOutcome, String>;

fn ensure(cond: bool, msg: impl Into<String>) -> Result<(), String> {
    if cond { Ok(()) } else { Err(msg.into()) }
}

fn na(reason: &str) -> CaseResultInner {
    Ok(CaseOutcome::NotApplicable(reason.to_owned()))
}

fn err<E: fmt::Display>(context: &str) -> impl FnOnce(E) -> String + '_ {
    move |e| format!("{context}: {e}")
}

fn is_unsupported<T>(r: &ConnectorResult<T>) -> bool {
    matches!(r, Err(ConnectorError::Unsupported { .. }))
}

fn rejects_mutation<T>(r: &ConnectorResult<T>) -> bool {
    matches!(r, Err(e) if matches!(e.kind(), ErrorKind::ReadOnly | ErrorKind::Unsupported | ErrorKind::Permission))
}

struct Suite {
    conn: Arc<dyn Connection>,
    family: EngineFamily,
    object: ObjectRef,
    namespace: NamespacePath,
    cases: Vec<CaseResult>,
}

impl Suite {
    fn has(&self, c: Capability) -> bool {
        self.conn.manifest().supports(c)
    }

    fn is_document(&self) -> bool {
        self.family == EngineFamily::MongoDb
    }

    fn record(&mut self, name: &'static str, result: CaseResultInner) {
        let outcome = match result {
            Ok(o) => o,
            Err(m) => CaseOutcome::Failed(m),
        };
        self.cases.push(CaseResult { name, outcome });
    }

    fn order_by_id() -> Vec<SortKey> {
        vec![SortKey {
            field: FieldPath::new("id"),
            direction: SortDirection::Asc,
        }]
    }

    async fn read_all(&self, request: ReadRequest) -> Result<Vec<Record>, String> {
        let mut stream = self
            .conn
            .read(request, CancellationToken::new())
            .await
            .map_err(err("read"))?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(err("read batch"))?;
            out.extend(batch.records);
        }
        Ok(out)
    }

    async fn count(&self) -> Result<u64, String> {
        if self.has(Capability::ExactCount) {
            return Ok(self
                .conn
                .count(&self.object, None, true)
                .await
                .map_err(err("count"))?
                .count);
        }
        let mut req = ReadRequest::new(self.object.clone(), 500);
        req.order_by = Self::order_by_id();
        Ok(self.read_all(req).await?.len() as u64)
    }

    async fn object_listed(&self) -> Result<bool, String> {
        let entries = self.conn.list(&self.namespace).await.map_err(err("list"))?;
        Ok(entries.iter().any(|e| e.name == self.object.name))
    }

    fn table_definition(&self) -> Result<TableDefinition, String> {
        let mut columns = Vec::new();
        for c in round_trip_columns() {
            columns.push(ColumnDefinition {
                name: c.name.to_owned(),
                native_type: None,
                canonical_type: c.canonical_type.clone(),
                nullable: c.name != "id",
                default_expression: None,
            });
        }
        Ok(TableDefinition {
            object: self.object.clone(),
            columns,
            primary_key: vec!["id".into()],
            foreign_keys: Vec::new(),
        })
    }

    fn create_op(&self) -> Result<SchemaOperation, String> {
        Ok(if self.is_document() {
            SchemaOperation::CreateCollection {
                object: self.object.clone(),
                if_not_exists: false,
                options: None,
            }
        } else {
            SchemaOperation::CreateTable {
                table: self.table_definition()?,
                if_not_exists: false,
            }
        })
    }
}

/// Runs the full suite. Objects are created in the harness's scratch
/// namespace with a unique name and dropped at the end.
pub async fn run_contract_suite(harness: &dyn ContractHarness) -> ContractReport {
    let conn = match harness.connect().await {
        Ok(c) => c,
        Err(e) => {
            return ContractReport {
                connector_id: "unknown".into(),
                server: None,
                cases: vec![CaseResult {
                    name: "connect",
                    outcome: CaseOutcome::Failed(e.to_string()),
                }],
            };
        }
    };
    let family = harness.family();
    let namespace = harness.scratch_namespace();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("gb_contract_{}", &suffix[..12]);
    let object = ObjectRef {
        namespace: namespace.clone(),
        name,
        kind: if family == EngineFamily::MongoDb {
            ObjectKind::Collection
        } else {
            ObjectKind::Table
        },
    };
    let mut s = Suite {
        conn: conn.clone(),
        family,
        object,
        namespace,
        cases: Vec::new(),
    };
    let connector_id = conn.manifest().connector_id.clone();

    // ---- connection basics ------------------------------------------------
    let manifest = conn.manifest().clone();
    s.record(
        "api_version_compatible",
        ensure(
            manifest.api_version.is_compatible_with_host(API_VERSION),
            format!(
                "connector API {} vs host {}",
                manifest.api_version, API_VERSION
            ),
        )
        .map(|_| CaseOutcome::Passed),
    );
    s.record(
        "manifest_family_matches",
        ensure(
            manifest.family == family,
            "manifest family differs from harness",
        )
        .map(|_| CaseOutcome::Passed),
    );
    let health = conn.health_check().await;
    s.record(
        "health_check",
        health
            .map(|_| CaseOutcome::Passed)
            .map_err(|e| e.to_string()),
    );

    let server = conn.server_info().await;
    let server_desc = server
        .as_ref()
        .ok()
        .map(|i| format!("{} {}", i.product, i.version));
    s.record(
        "server_info",
        match server {
            Ok(info) => ensure(info.family == family, "server family mismatch")
                .and_then(|_| {
                    ensure(
                        !info.version.parts().is_empty(),
                        "server version has no numeric part",
                    )
                })
                .map(|_| CaseOutcome::Passed),
            Err(e) => Err(e.to_string()),
        },
    );

    let root = conn.list(&NamespacePath::root()).await;
    s.record(
        "list_root",
        root.map(|_| CaseOutcome::Passed).map_err(|e| e.to_string()),
    );

    s.record("quote_identifier", {
        (|| -> Result<CaseOutcome, String> {
            let plain = conn.quote_identifier("customers").map_err(err("plain"))?;
            ensure(!plain.is_empty(), "empty quoted identifier")?;
            let weird = conn
                .quote_identifier("we\"ird`na'me ;--")
                .map_err(err("special characters"))?;
            ensure(weird.contains("ird"), "quoted identifier lost content")?;
            conn.quote_identifier("عملاء_الفواتير")
                .map_err(err("arabic"))?;
            ensure(
                conn.quote_identifier("bad\0name").is_err(),
                "NUL in identifier must be rejected",
            )?;
            ensure(
                conn.quote_identifier("").is_err(),
                "empty identifier must be rejected",
            )?;
            Ok(CaseOutcome::Passed)
        })()
    });

    // ---- undeclared capabilities are rejected ---------------------------
    s.record("undeclared_capabilities_rejected", {
        let mut problems = Vec::new();
        if !s.has(Capability::ChangeDataCapture) {
            let r = conn
                .change_feed(
                    ChangeFeedRequest {
                        objects: vec![],
                        method: gazabridge_domain::compatibility::SyncMethod::NativeCdc,
                        resume_after: None,
                        cursor_field: None,
                        poll_interval_ms: 1000,
                    },
                    CancellationToken::new(),
                )
                .await;
            if !is_unsupported(&r) {
                problems.push("change_feed");
            }
        }
        if !s.has(Capability::Transactions) && !is_unsupported(&conn.begin().await) {
            problems.push("begin");
        }
        if !s.has(Capability::ResumeTokens) && !is_unsupported(&conn.current_position(&[]).await) {
            problems.push("current_position");
        }
        if !s.has(Capability::ExecuteQuery) {
            let r = conn
                .execute(
                    QueryRequest {
                        language: QueryLanguage::SqliteSql,
                        text: "SELECT 1".into(),
                        params: vec![],
                        max_rows: 1,
                        timeout: None,
                        database: None,
                        collection: None,
                    },
                    CancellationToken::new(),
                )
                .await;
            if !is_unsupported(&r) {
                problems.push("execute");
            }
        }
        if problems.is_empty() {
            Ok(CaseOutcome::Passed)
        } else {
            Err(format!(
                "undeclared operations did not return Unsupported: {}",
                problems.join(", ")
            ))
        }
    });

    // ---- DDL -------------------------------------------------------------
    let can_create = if s.is_document() {
        s.has(Capability::CreateCollection)
    } else {
        s.has(Capability::CreateTable)
    };
    if !can_create {
        s.record("create_object", na("connector cannot create objects"));
        return finish(s, connector_id, server_desc, harness).await;
    }

    let create = match s.create_op() {
        Ok(op) => op,
        Err(e) => {
            s.record("create_object", Err(e));
            return finish(s, connector_id, server_desc, harness).await;
        }
    };
    let plan_result = async {
        let plan = conn
            .plan_ddl(std::slice::from_ref(&create))
            .map_err(err("plan_ddl"))?;
        ensure(
            !plan.statements.is_empty() || s.is_document(),
            "plan produced no statements",
        )?;
        ensure(
            !plan.destructive,
            "creating an object must not be planned as destructive",
        )?;
        ensure(
            !s.object_listed().await?,
            "plan_ddl created the object; planning must have no side effects",
        )?;
        Ok(CaseOutcome::Passed)
    }
    .await;
    s.record("plan_ddl_has_no_side_effects", plan_result);

    let created = conn
        .apply_ddl(std::slice::from_ref(&create), CancellationToken::new())
        .await;
    let created_ok = created.is_ok();
    s.record(
        "create_object",
        created
            .map(|_| CaseOutcome::Passed)
            .map_err(|e| e.to_string()),
    );
    if !created_ok {
        return finish(s, connector_id, server_desc, harness).await;
    }
    let listed = s.object_listed().await;
    s.record(
        "created_object_is_listed",
        listed
            .and_then(|l| ensure(l, "object missing from list()"))
            .map(|_| CaseOutcome::Passed),
    );

    // Document stores get the equivalent of the relational primary key.
    if s.is_document() && s.has(Capability::CreateIndex) {
        let result = conn
            .apply_ddl(
                &[SchemaOperation::CreateIndex {
                    object: s.object.clone(),
                    index: gazabridge_plugin_sdk::ddl::IndexDefinition {
                        name: "gb_contract_id".into(),
                        columns: vec!["id".into()],
                        unique: true,
                        native: None,
                    },
                    if_not_exists: true,
                }],
                CancellationToken::new(),
            )
            .await;
        s.record(
            "unique_key_index",
            result
                .map(|_| CaseOutcome::Passed)
                .map_err(|e| e.to_string()),
        );
    }

    if s.has(Capability::InspectSchema) && !s.is_document() {
        let result = async {
            let schema = conn
                .inspect(&s.object, &Default::default())
                .await
                .map_err(err("inspect"))?;
            for c in round_trip_columns() {
                ensure(
                    schema.column(c.name).is_some(),
                    format!("column `{}` missing from inspection", c.name),
                )?;
            }
            ensure(
                schema.primary_key == vec!["id".to_string()],
                format!("primary key {:?}", schema.primary_key),
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("inspect_object", result);
    } else {
        s.record(
            "inspect_object",
            na("schema inspection not declared or schemaless"),
        );
    }

    // ---- writes and reads ------------------------------------------------
    let records = round_trip_records(ROWS);
    let insert = conn
        .write_batch(
            WriteBatch {
                object: s.object.clone(),
                mode: WriteMode::Insert,
                records: records.clone(),
                key_fields: vec!["id".into()],
                atomic: false,
            },
            CancellationToken::new(),
        )
        .await;
    let inserted_ok =
        matches!(&insert, Ok(o) if o.failures.is_empty() && o.inserted == ROWS as u64);
    s.record(
        "write_batch_insert",
        match insert {
            Ok(o) => ensure(
                o.failures.is_empty(),
                format!("record failures: {:?}", o.failures),
            )
            .and_then(|_| {
                ensure(
                    o.inserted == ROWS as u64,
                    format!("inserted {} of {ROWS}", o.inserted),
                )
            })
            .map(|_| CaseOutcome::Passed),
            Err(e) => Err(e.to_string()),
        },
    );
    if !inserted_ok {
        return finish(s, connector_id, server_desc, harness).await;
    }

    if s.is_document() && s.has(Capability::InferDocumentStructure) {
        let result = async {
            let options = gazabridge_plugin_sdk::connector::InspectOptions {
                sampling: gazabridge_plugin_sdk::schema::SamplingMethod::FullScan,
                ..Default::default()
            };
            let schema = conn
                .inspect(&s.object, &options)
                .await
                .map_err(err("inspect"))?;
            let inferred = schema
                .inferred
                .ok_or("no inferred structure for a document store")?;
            ensure(
                inferred.sampled_documents == ROWS as u64,
                format!("sampled {}", inferred.sampled_documents),
            )?;
            let label = inferred
                .fields
                .iter()
                .find(|f| f.path == "label")
                .ok_or("field `label` not inferred")?;
            ensure(
                label.present_count == ROWS as u64,
                "wrong presence count for `label`",
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("document_structure_inference", result);
    }

    if s.has(Capability::ExactCount) {
        let c = conn.count(&s.object, None, true).await;
        s.record(
            "exact_count",
            match c {
                Ok(c) => ensure(c.exact && c.count == ROWS as u64, format!("count {c:?}"))
                    .map(|_| CaseOutcome::Passed),
                Err(e) => Err(e.to_string()),
            },
        );
    }

    let round_trip = async {
        let mut req = ReadRequest::new(s.object.clone(), 7);
        req.order_by = Suite::order_by_id();
        let got = s.read_all(req).await?;
        ensure(
            got.len() == ROWS,
            format!("read {} of {ROWS} records", got.len()),
        )?;
        let strict = ConversionPolicy::strict();
        for (i, (want, have)) in records.iter().zip(&got).enumerate() {
            for col in round_trip_columns() {
                let expected = want.get(col.name);
                let actual = have.get(col.name);
                if expected.is_absent() {
                    ensure(
                        actual.is_absent(),
                        format!(
                            "row {i} `{}`: expected null, got {}",
                            col.name,
                            actual.kind()
                        ),
                    )?;
                    continue;
                }
                let converted = convert(actual, &col.canonical_type, &strict)
                    .map_err(|e| format!("row {i} `{}`: {e}", col.name))?;
                ensure(
                    &converted.value == expected,
                    format!(
                        "row {i} `{}`: value changed in round trip ({} → {})",
                        col.name,
                        expected.kind(),
                        actual.kind()
                    ),
                )?;
            }
        }
        Ok(CaseOutcome::Passed)
    }
    .await;
    s.record("stream_read_round_trip", round_trip);

    if s.has(Capability::PaginatedRead) {
        let result = async {
            let mut seen = Vec::new();
            let mut offset = 0;
            loop {
                let page = conn
                    .read_page(
                        PageRequest {
                            object: s.object.clone(),
                            projection: vec!["id".into()],
                            exclude: Vec::new(),
                            filter: None,
                            order_by: Suite::order_by_id(),
                            offset,
                            limit: 8,
                        },
                        CancellationToken::new(),
                    )
                    .await
                    .map_err(err("read_page"))?;
                ensure(page.records.len() <= 8, "page exceeded its limit")?;
                for r in &page.records {
                    if let &Value::Int(id) = r.get("id") {
                        seen.push(id);
                    } else {
                        return Err(format!("id read back as {}", r.get("id").kind()));
                    }
                }
                offset += page.records.len() as u64;
                if !page.has_more {
                    break;
                }
                ensure(offset < 1000, "pagination did not terminate")?;
            }
            let expected: Vec<i64> = (0..ROWS as i64).collect();
            ensure(seen == expected, format!("pages returned {seen:?}"))?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("paginated_read", result);
    }

    if s.has(Capability::FilteredRead) {
        let result = async {
            let mut req = ReadRequest::new(s.object.clone(), 100);
            req.order_by = Suite::order_by_id();
            req.filter = Some(Filter::Ge {
                field: FieldPath::new("id"),
                value: Value::Int(25),
            });
            let got = s.read_all(req).await?;
            ensure(
                got.len() == 5,
                format!("filter returned {} records", got.len()),
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("filtered_read", result);
    }

    if s.has(Capability::Checkpoints) {
        let result = async {
            let mut req = ReadRequest::new(s.object.clone(), 10);
            req.order_by = Suite::order_by_id();
            let mut stream = conn
                .read(req.clone(), CancellationToken::new())
                .await
                .map_err(err("read"))?;
            let first = stream
                .next()
                .await
                .ok_or("stream ended early")?
                .map_err(err("first batch"))?;
            drop(stream);
            let position = first
                .position
                .ok_or("ordered read did not report a resumable position")?;
            req.resume_after = Some(position);
            let rest = s.read_all(req).await?;
            let ids: Vec<_> = rest.iter().map(|r| r.get("id").clone()).collect();
            let expected: Vec<_> = (10..ROWS as i64).map(Value::Int).collect();
            ensure(ids == expected, "resume returned the wrong records")?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("resume_from_position", result);
    }

    let dup = async {
        let again = conn
            .write_batch(
                WriteBatch {
                    object: s.object.clone(),
                    mode: WriteMode::Insert,
                    records: records[..1].to_vec(),
                    key_fields: vec!["id".into()],
                    atomic: false,
                },
                CancellationToken::new(),
            )
            .await;
        match again {
            Ok(o) => ensure(
                o.inserted == 0 && o.failures.iter().any(|f| f.kind == ErrorKind::Conflict),
                "duplicate key was not reported as a conflict",
            )?,
            Err(e) => ensure(
                e.kind() == ErrorKind::Conflict,
                format!("expected conflict, got {e}"),
            )?,
        }
        ensure(
            s.count().await? == ROWS as u64,
            "duplicate insert changed the record count",
        )?;
        Ok(CaseOutcome::Passed)
    }
    .await;
    s.record("duplicate_key_reported", dup);

    if s.has(Capability::WriteUpsert) {
        let result = async {
            let mut changed = records[..5].to_vec();
            for r in &mut changed {
                r.insert("label", Value::String("upserted".into()));
            }
            for _ in 0..2 {
                conn.write_batch(
                    WriteBatch {
                        object: s.object.clone(),
                        mode: WriteMode::Upsert,
                        records: changed.clone(),
                        key_fields: vec!["id".into()],
                        atomic: false,
                    },
                    CancellationToken::new(),
                )
                .await
                .map_err(err("upsert"))?;
            }
            ensure(
                s.count().await? == ROWS as u64,
                "upsert changed the record count",
            )?;
            let mut req = ReadRequest::new(s.object.clone(), 100);
            req.order_by = Suite::order_by_id();
            req.filter = Some(Filter::eq("id", Value::Int(3)));
            let got = s.read_all(req).await?;
            ensure(
                got.first().map(|r| r.get("label")) == Some(&Value::String("upserted".into())),
                "upsert did not update the record",
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("upsert_is_idempotent", result);
    }

    if s.has(Capability::WriteUpdate) {
        let result = async {
            let mut set = Record::new();
            set.insert("label", Value::String("updated".into()));
            let unrestricted = conn
                .update(UpdateRequest {
                    object: s.object.clone(),
                    filter: Filter::and(vec![]),
                    set: set.clone(),
                    expect_matched: None,
                })
                .await;
            ensure(
                unrestricted.is_err(),
                "unrestricted UPDATE must be rejected",
            )?;
            let out = conn
                .update(UpdateRequest {
                    object: s.object.clone(),
                    filter: Filter::eq("id", Value::Int(1)),
                    set: set.clone(),
                    expect_matched: Some(1),
                })
                .await
                .map_err(err("update"))?;
            ensure(out.matched == 1, format!("update matched {}", out.matched))?;
            let wrong = conn
                .update(UpdateRequest {
                    object: s.object.clone(),
                    filter: Filter::eq("id", Value::Int(1)),
                    set,
                    expect_matched: Some(2),
                })
                .await;
            ensure(wrong.is_err(), "optimistic-concurrency mismatch must fail")?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("update_guards", result);
    }

    if s.has(Capability::WriteDelete) {
        let result = async {
            let unrestricted = conn
                .delete(DeleteRequest {
                    object: s.object.clone(),
                    filter: Filter::and(vec![]),
                    expect_matched: None,
                })
                .await;
            ensure(
                unrestricted.is_err(),
                "unrestricted DELETE must be rejected",
            )?;
            ensure(
                s.count().await? == ROWS as u64,
                "rejected DELETE removed records",
            )?;
            let out = conn
                .delete(DeleteRequest {
                    object: s.object.clone(),
                    filter: Filter::eq("id", Value::Int(2)),
                    expect_matched: Some(1),
                })
                .await
                .map_err(err("delete"))?;
            ensure(
                out.matched == 1,
                "delete matched the wrong number of records",
            )?;
            ensure(
                s.count().await? == ROWS as u64 - 1,
                "delete did not remove the record",
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("delete_guards", result);
    }

    if s.has(Capability::Transactions) {
        let result = async {
            let before = s.count().await?;
            let mut extra = round_trip_records(1);
            extra[0].insert("id", Value::Int(10_000));
            let mut tx = conn.begin().await.map_err(err("begin"))?;
            tx.write_batch(WriteBatch {
                object: s.object.clone(),
                mode: WriteMode::Insert,
                records: extra.clone(),
                key_fields: vec!["id".into()],
                atomic: true,
            })
            .await
            .map_err(err("tx write"))?;
            tx.rollback().await.map_err(err("rollback"))?;
            ensure(s.count().await? == before, "rolled-back write is visible")?;

            let mut tx = conn.begin().await.map_err(err("begin"))?;
            tx.write_batch(WriteBatch {
                object: s.object.clone(),
                mode: WriteMode::Insert,
                records: extra,
                key_fields: vec!["id".into()],
                atomic: true,
            })
            .await
            .map_err(err("tx write"))?;
            tx.commit().await.map_err(err("commit"))?;
            ensure(
                s.count().await? == before + 1,
                "committed write is not visible",
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("transaction_commit_and_rollback", result);
    }

    if s.is_document() {
        let result = async {
            let mut with_null = Record::new();
            with_null.insert("id", Value::Int(20_000));
            with_null.insert("opt", Value::Null);
            let mut without = Record::new();
            without.insert("id", Value::Int(20_001));
            conn.write_batch(
                WriteBatch {
                    object: s.object.clone(),
                    mode: WriteMode::Insert,
                    records: vec![with_null, without],
                    key_fields: vec!["id".into()],
                    atomic: false,
                },
                CancellationToken::new(),
            )
            .await
            .map_err(err("write"))?;
            let mut req = ReadRequest::new(s.object.clone(), 10);
            req.order_by = Suite::order_by_id();
            req.filter = Some(Filter::Ge {
                field: FieldPath::new("id"),
                value: Value::Int(20_000),
            });
            let got = s.read_all(req).await?;
            ensure(got.len() == 2, "expected two documents")?;
            ensure(
                got[0].get("opt") == &Value::Null,
                "explicit null was not preserved",
            )?;
            ensure(
                got[1].get("opt") == &Value::Missing,
                "missing field became present",
            )?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("null_vs_missing", result);
    }

    let cancel = async {
        let token = CancellationToken::new();
        let mut req = ReadRequest::new(s.object.clone(), 1);
        req.order_by = Suite::order_by_id();
        let mut stream = conn.read(req, token.clone()).await.map_err(err("read"))?;
        token.cancel();
        let drained = tokio::time::timeout(Duration::from_secs(10), async {
            let mut n = 0usize;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => n += b.len(),
                    Err(e) if e.kind() == ErrorKind::Cancelled => break,
                    Err(e) => return Err(format!("unexpected error after cancel: {e}")),
                }
            }
            Ok(n)
        })
        .await
        .map_err(|_| "read did not stop within 10s of cancellation".to_string())??;
        ensure(drained < ROWS, "cancelled read still returned every record")?;
        Ok(CaseOutcome::Passed)
    }
    .await;
    s.record("read_cancellation", cancel);

    finish(s, connector_id, server_desc, harness).await
}

async fn finish(
    mut s: Suite,
    connector_id: String,
    server: Option<String>,
    harness: &dyn ContractHarness,
) -> ContractReport {
    // Read-only enforcement uses a separate connection.
    let ro = async {
        let ro = harness
            .connect_read_only()
            .await
            .map_err(err("connect read-only"))?;
        ensure(ro.is_read_only(), "read-only connection reports writable")?;
        let m = ro.manifest();
        if let Some(c) = m.capabilities.iter().find(|c| c.is_mutating()) {
            return Err(format!("read-only manifest still declares {c}"));
        }
        let write = ro
            .write_batch(
                WriteBatch {
                    object: s.object.clone(),
                    mode: WriteMode::Insert,
                    records: round_trip_records(1),
                    key_fields: vec![],
                    atomic: false,
                },
                CancellationToken::new(),
            )
            .await;
        ensure(
            rejects_mutation(&write),
            "read-only connection accepted a write",
        )?;
        let drop = ro
            .apply_ddl(
                &[SchemaOperation::DropObject {
                    object: s.object.clone(),
                    if_exists: true,
                }],
                CancellationToken::new(),
            )
            .await;
        ensure(rejects_mutation(&drop), "read-only connection accepted DDL")?;
        ro.close().await;
        Ok(CaseOutcome::Passed)
    }
    .await;
    s.record("read_only_enforced", ro);

    if s.has(Capability::DropObject) {
        let drop = async {
            let op = SchemaOperation::DropObject {
                object: s.object.clone(),
                if_exists: true,
            };
            let plan = s
                .conn
                .plan_ddl(std::slice::from_ref(&op))
                .map_err(err("plan drop"))?;
            ensure(plan.destructive, "DROP must be planned as destructive")?;
            s.conn
                .apply_ddl(std::slice::from_ref(&op), CancellationToken::new())
                .await
                .map_err(err("drop"))?;
            ensure(!s.object_listed().await?, "dropped object is still listed")?;
            Ok(CaseOutcome::Passed)
        }
        .await;
        s.record("drop_object", drop);
    }
    s.conn.close().await;
    ContractReport {
        connector_id,
        server,
        cases: s.cases,
    }
}

/// Converts `value` to `t` strictly; helper for connector unit tests.
pub fn strict_convert(value: &Value, t: &CanonicalType) -> Option<Value> {
    convert(value, t, &ConversionPolicy::strict())
        .ok()
        .map(|c| c.value)
}
