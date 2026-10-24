//! The SQLite `Connection` implementation.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use gazabridge_domain::CanonicalType;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::connector::{CountResult, HealthReport, InspectOptions, Transaction};
use gazabridge_plugin_sdk::ddl::{DdlPlan, SchemaOperation};
use gazabridge_plugin_sdk::filter::Filter;
use gazabridge_plugin_sdk::profile::Version;
use gazabridge_plugin_sdk::query::{ExplainPlan, QueryRequest, QueryResult};
use gazabridge_plugin_sdk::read::{Page, PageRequest, ReadRequest, RecordStream};
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, ConstraintInfo, ConstraintKind, NamespaceEntry, NamespacePath, ObjectKind,
    ObjectRef, ObjectSchema,
};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{
    Capability, CapabilityManifest, Connection, ConnectorError, ConnectorResult, ErrorKind,
    ServerInfo,
};
use sqlx::{Column, Either, Executor, Row, SqlitePool, TypeInfo};
use tokio_util::sync::CancellationToken;

use crate::error::map_sqlx;
use crate::meta::{self, TableMeta};
use crate::sql::{self, DEFAULT_SCHEMA, quote_ident, schema_of};
use crate::types::{bind_all, canonical_for_declared, decode, encode, native_type_for, raw_value};
use crate::{read, write};

type MetaKey = (String, String);

#[derive(Debug)]
struct Inner {
    pool: SqlitePool,
    manifest: CapabilityManifest,
    read_only: bool,
    meta: Mutex<HashMap<MetaKey, Arc<TableMeta>>>,
}

impl Inner {
    fn ensure_writable(&self) -> ConnectorResult<()> {
        if self.read_only {
            Err(ConnectorError::ReadOnly)
        } else {
            Ok(())
        }
    }

    async fn table(&self, object: &ObjectRef) -> ConnectorResult<Arc<TableMeta>> {
        let key = (schema_of(object).to_owned(), object.name.clone());
        if let Some(m) = self.meta.lock().ok().and_then(|c| c.get(&key).cloned()) {
            return Ok(m);
        }
        let m = Arc::new(meta::load(&self.pool, object).await?);
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

/// An open SQLite database.
#[derive(Debug, Clone)]
pub struct SqliteConnection {
    inner: Arc<Inner>,
}

impl SqliteConnection {
    pub(crate) fn new(pool: SqlitePool, manifest: CapabilityManifest, read_only: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                manifest,
                read_only,
                meta: Mutex::new(HashMap::new()),
            }),
        }
    }
}

fn require_language(req: &QueryRequest) -> ConnectorResult<()> {
    if req.language != QueryLanguage::SqliteSql {
        return Err(ConnectorError::invalid_input(
            "SQLite connections execute SQLite SQL only",
        ));
    }
    Ok(())
}

async fn with_cancel<T>(
    cancel: &CancellationToken,
    timeout: Option<std::time::Duration>,
    fut: impl std::future::Future<Output = ConnectorResult<T>>,
) -> ConnectorResult<T> {
    let guarded = async {
        match timeout {
            Some(t) => tokio::time::timeout(t, fut)
                .await
                .map_err(|_| ConnectorError::new(ErrorKind::Timeout, "query timed out"))?,
            None => fut.await,
        }
    };
    tokio::select! {
        r = guarded => r,
        _ = cancel.cancelled() => Err(ConnectorError::Cancelled),
    }
}

fn result_columns(row: &sqlx::sqlite::SqliteRow) -> Vec<ColumnInfo> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let decl = c.type_info().name().to_owned();
            ColumnInfo {
                name: c.name().to_owned(),
                canonical_type: canonical_for_declared(if decl == "NULL" { "" } else { &decl }),
                native_type: decl,
                nullable: true,
                default_expression: None,
                is_generated: false,
                is_identity: false,
                comment: None,
                ordinal: i as u32 + 1,
            }
        })
        .collect()
}

async fn total_changes(conn: &mut sqlx::SqliteConnection) -> ConnectorResult<u64> {
    let n: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    Ok(n.max(0) as u64)
}

async fn run_query(
    conn: &mut sqlx::SqliteConnection,
    req: &QueryRequest,
) -> ConnectorResult<QueryResult> {
    let started = Instant::now();
    let binds = req
        .params
        .iter()
        .map(|v| encode(v, None))
        .collect::<ConnectorResult<Vec<_>>>()?;
    // sqlite3_changes() is not reset by DDL, so per-statement counts can
    // double-count; the total_changes() delta is exact.
    let changes_before = total_changes(conn).await?;
    // Parameterless text may be a multi-statement script; parameterized
    // text is a single prepared statement.
    let mut stream = if binds.is_empty() {
        conn.fetch_many(sqlx::raw_sql(&req.text))
    } else {
        conn.fetch_many(bind_all(sqlx::query(&req.text), &binds))
    };
    let mut rows = Vec::new();
    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut truncated = false;
    while let Some(item) = stream.next().await {
        match item.map_err(map_sqlx)? {
            Either::Left(_) => {}
            Either::Right(row) => {
                if columns.is_empty() {
                    columns = result_columns(&row);
                }
                if rows.len() >= req.max_rows as usize {
                    truncated = true;
                    break;
                }
                let mut record = gazabridge_domain::Record::with_capacity(columns.len());
                for (i, c) in columns.iter().enumerate() {
                    record.insert(
                        c.name.clone(),
                        decode(raw_value(&row, i)?, &c.canonical_type),
                    );
                }
                rows.push(record);
            }
        }
    }
    drop(stream);
    let affected = if truncated {
        None
    } else {
        Some(total_changes(conn).await?.saturating_sub(changes_before))
    };
    Ok(QueryResult {
        columns,
        affected: if rows.is_empty() { affected } else { None },
        rows,
        truncated,
        elapsed: started.elapsed(),
        notices: Vec::new(),
    })
}

#[async_trait]
impl Connection for SqliteConnection {
    fn manifest(&self) -> &CapabilityManifest {
        &self.inner.manifest
    }

    fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        let started = Instant::now();
        sqlx::query("SELECT 1")
            .execute(&self.inner.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(HealthReport {
            latency: started.elapsed(),
            checked_at: Utc::now(),
            notes: Vec::new(),
        })
    }

    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        let pool = &self.inner.pool;
        let version: String = sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(pool)
            .await
            .map_err(map_sqlx)?;
        let mut properties = BTreeMap::new();
        for pragma in [
            "journal_mode",
            "foreign_keys",
            "page_size",
            "encoding",
            "query_only",
        ] {
            let row = sqlx::query(&format!("PRAGMA {pragma}"))
                .fetch_optional(pool)
                .await
                .map_err(map_sqlx)?;
            if let Some(row) = row {
                let v = raw_value(&row, 0)?;
                if let Some(text) = gazabridge_domain::conversion::scalar_to_text(&v) {
                    properties.insert(pragma.to_owned(), text);
                }
            }
        }
        Ok(ServerInfo {
            family: EngineFamily::Sqlite,
            product: "SQLite".into(),
            version_string: format!("SQLite {version}"),
            version: Version(version),
            properties,
        })
    }

    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        let pool = &self.inner.pool;
        match &parent.schema {
            None => {
                let rows = sqlx::query("SELECT name FROM pragma_database_list ORDER BY seq")
                    .fetch_all(pool)
                    .await
                    .map_err(map_sqlx)?;
                rows.iter()
                    .map(|r| {
                        Ok(NamespaceEntry {
                            name: r.try_get("name").map_err(map_sqlx)?,
                            kind: ObjectKind::Schema,
                            comment: None,
                            estimated_count: None,
                        })
                    })
                    .collect()
            }
            Some(schema) => {
                let sql = format!(
                    "SELECT type, name FROM {}.sqlite_master
                     WHERE type IN ('table', 'view', 'trigger', 'index') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
                     ORDER BY type, name",
                    quote_ident(schema)?
                );
                let rows = sqlx::query(&sql).fetch_all(pool).await.map_err(map_sqlx)?;
                rows.iter()
                    .map(|r| {
                        let kind = match r.try_get::<String, _>("type").map_err(map_sqlx)?.as_str()
                        {
                            "view" => ObjectKind::View,
                            "trigger" => ObjectKind::Trigger,
                            "index" => ObjectKind::Index,
                            _ => ObjectKind::Table,
                        };
                        Ok(NamespaceEntry {
                            name: r.try_get("name").map_err(map_sqlx)?,
                            kind,
                            comment: None,
                            estimated_count: None,
                        })
                    })
                    .collect()
            }
        }
    }

    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        quote_ident(ident)
    }

    async fn close(&self) {
        self.inner.pool.close().await;
    }

    async fn inspect(
        &self,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        let m = self.inner.table(object).await?;
        let indexes = if options.include_indexes {
            meta::indexes(&self.inner.pool, object).await?
        } else {
            Vec::new()
        };
        let mut constraints: Vec<ConstraintInfo> = Vec::new();
        if options.include_constraints {
            if !m.primary_key.is_empty() {
                constraints.push(ConstraintInfo {
                    name: None,
                    columns: m.primary_key.clone(),
                    kind: ConstraintKind::PrimaryKey,
                });
            }
            for idx in indexes.iter().filter(|i| i.unique && !i.primary) {
                constraints.push(ConstraintInfo {
                    name: Some(idx.name.clone()),
                    columns: idx.columns.clone(),
                    kind: ConstraintKind::Unique,
                });
            }
            constraints.extend(meta::foreign_keys(&self.inner.pool, object).await?);
        }
        Ok(ObjectSchema {
            object: ObjectRef {
                kind: m.kind,
                ..object.clone()
            },
            columns: m.columns.clone(),
            primary_key: m.primary_key.clone(),
            indexes,
            constraints,
            inferred: None,
            estimated_count: None,
            comment: None,
            definition: m.definition.clone(),
        })
    }

    async fn read(
        &self,
        request: ReadRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        let m = self.inner.table(&request.object).await?;
        read::open(self.inner.pool.clone(), &m, &request, cancel)
    }

    async fn read_page(
        &self,
        request: PageRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<Page> {
        let m = self.inner.table(&request.object).await?;
        with_cancel(&cancel, None, read::page(&self.inner.pool, &m, &request)).await
    }

    async fn count(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        exact: bool,
    ) -> ConnectorResult<CountResult> {
        if !exact {
            return Err(ConnectorError::unsupported(Capability::CountEstimate));
        }
        let m = self.inner.table(object).await?;
        Ok(CountResult {
            count: read::count(&self.inner.pool, &m, object, filter).await?,
            exact: true,
        })
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&batch.object).await?;
        with_cancel(&cancel, None, async {
            let mut tx = self.inner.pool.begin().await.map_err(map_sqlx)?;
            let outcome = write::write_records(&mut tx, &m, &batch).await?;
            tx.commit().await.map_err(map_sqlx)?;
            Ok(outcome)
        })
        .await
    }

    async fn update(&self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let mut tx = self.inner.pool.begin().await.map_err(map_sqlx)?;
        let out = write::update(&mut tx, &m, &request).await?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(out)
    }

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let mut tx = self.inner.pool.begin().await.map_err(map_sqlx)?;
        let out = write::delete(&mut tx, &m, &request).await?;
        tx.commit().await.map_err(map_sqlx)?;
        Ok(out)
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        self.inner.ensure_writable()?;
        let tx = self.inner.pool.begin().await.map_err(map_sqlx)?;
        Ok(Box::new(SqliteTransaction {
            inner: self.inner.clone(),
            tx,
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
        let result = with_cancel(&cancel, None, async {
            let mut tx = self.inner.pool.begin().await.map_err(map_sqlx)?;
            for stmt in &plan.statements {
                sqlx::query(stmt)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx)?;
            }
            tx.commit().await.map_err(map_sqlx)
        })
        .await;
        self.inner.invalidate();
        result
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
        let result = with_cancel(&cancel, request.timeout, async {
            let mut conn = self.inner.pool.acquire().await.map_err(map_sqlx)?;
            run_query(&mut conn, &request).await
        })
        .await;
        // Statements may have changed the schema.
        self.inner.invalidate();
        result
    }

    async fn explain(
        &self,
        request: QueryRequest,
        analyze: bool,
        cancel: CancellationToken,
    ) -> ConnectorResult<ExplainPlan> {
        if analyze {
            return Err(ConnectorError::unsupported(Capability::ExplainAnalyze));
        }
        require_language(&request)?;
        let binds = request
            .params
            .iter()
            .map(|v| encode(v, None))
            .collect::<ConnectorResult<Vec<_>>>()?;
        let sql = format!("EXPLAIN QUERY PLAN {}", request.text);
        let rows = with_cancel(&cancel, request.timeout, async {
            bind_all(sqlx::query(&sql), &binds)
                .fetch_all(&self.inner.pool)
                .await
                .map_err(map_sqlx)
        })
        .await?;
        let mut nodes = Vec::with_capacity(rows.len());
        let mut depth: HashMap<i64, usize> = HashMap::new();
        let mut text = String::new();
        for r in &rows {
            let id: i64 = r.try_get("id").map_err(map_sqlx)?;
            let parent: i64 = r.try_get("parent").map_err(map_sqlx)?;
            let detail: String = r.try_get("detail").map_err(map_sqlx)?;
            let d = depth.get(&parent).map_or(0, |d| d + 1);
            depth.insert(id, d);
            text.push_str(&format!("{}{}\n", "  ".repeat(d), detail));
            nodes.push(serde_json::json!({ "id": id, "parent": parent, "detail": detail }));
        }
        Ok(ExplainPlan {
            native: serde_json::Value::Array(nodes),
            text,
            analyzed: false,
        })
    }
}

/// A SQLite transaction. Dropped without commit, sqlx rolls it back.
struct SqliteTransaction {
    inner: Arc<Inner>,
    tx: sqlx::Transaction<'static, sqlx::Sqlite>,
}

#[async_trait]
impl Transaction for SqliteTransaction {
    async fn write_batch(&mut self, batch: WriteBatch) -> ConnectorResult<WriteOutcome> {
        let m = self.inner.table(&batch.object).await?;
        write::write_records(&mut self.tx, &m, &batch).await
    }

    async fn update(&mut self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::update(&mut self.tx, &m, &request).await
    }

    async fn delete(&mut self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::delete(&mut self.tx, &m, &request).await
    }

    async fn execute(&mut self, request: QueryRequest) -> ConnectorResult<QueryResult> {
        require_language(&request)?;
        run_query(&mut self.tx, &request).await
    }

    async fn commit(self: Box<Self>) -> ConnectorResult<()> {
        self.tx.commit().await.map_err(map_sqlx)
    }

    async fn rollback(self: Box<Self>) -> ConnectorResult<()> {
        self.tx.rollback().await.map_err(map_sqlx)
    }
}

/// The schema SQLite objects live in unless attached databases are used.
pub fn main_schema() -> NamespacePath {
    NamespacePath::schema(None, DEFAULT_SCHEMA)
}
