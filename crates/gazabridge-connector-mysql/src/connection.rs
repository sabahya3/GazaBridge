//! The MySQL `Connection` implementation.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::{CanonicalType, Record};
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
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Pool, Row, Value as My};
use tokio_util::sync::CancellationToken;

use crate::error::map_mysql;
use crate::meta::{self, TableMeta, cell_str, cell_u64};
use crate::read;
use crate::sql::{self, database_of, is_read_only_statement, quote_ident};
use crate::types::{canonical_for_column, encode, native_type_for, raw_value};
use crate::write;

type MetaKey = (String, String);

/// Server flavour; a few statements differ between MySQL and MariaDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    MySql,
    MariaDb,
}

pub struct Inner {
    pub pool: Pool,
    pub manifest: CapabilityManifest,
    pub read_only: bool,
    pub flavor: Flavor,
    pub query_timeout: Option<Duration>,
    meta: Mutex<HashMap<MetaKey, Arc<TableMeta>>>,
    /// Kept open as long as the connection (pool connections dial it).
    _tunnel: Option<gazabridge_ssh::Tunnel>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlConnection")
            .field("read_only", &self.read_only)
            .field("flavor", &self.flavor)
            .finish_non_exhaustive()
    }
}

impl Inner {
    pub async fn conn(&self) -> ConnectorResult<Conn> {
        self.pool.get_conn().await.map_err(map_mysql)
    }

    fn ensure_writable(&self) -> ConnectorResult<()> {
        if self.read_only {
            Err(ConnectorError::ReadOnly)
        } else {
            Ok(())
        }
    }

    /// Asks the server to stop whatever connection `id` is running.
    pub async fn kill_query(&self, id: u32) {
        if let Ok(mut c) = self.pool.get_conn().await {
            let _ = c.query_drop(format!("KILL QUERY {id}")).await;
        }
    }

    /// Runs `work` on `conn`, killing the server-side statement when
    /// `cancel` fires or `timeout` elapses.
    pub async fn guarded<T, F>(
        &self,
        id: u32,
        cancel: &CancellationToken,
        timeout: Option<Duration>,
        work: F,
    ) -> ConnectorResult<T>
    where
        F: std::future::Future<Output = ConnectorResult<T>>,
    {
        let sleep = async {
            match timeout {
                Some(t) => tokio::time::sleep(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            r = work => r,
            _ = cancel.cancelled() => {
                self.kill_query(id).await;
                Err(ConnectorError::Cancelled)
            }
            _ = sleep => {
                self.kill_query(id).await;
                Err(ConnectorError::new(ErrorKind::Timeout, "query timed out"))
            }
        }
    }

    /// Runs a prepared query with cancellation.
    pub async fn rows_cancellable(
        &self,
        sql: &str,
        params: Vec<My>,
        cancel: &CancellationToken,
    ) -> ConnectorResult<Vec<Row>> {
        let mut c = self.conn().await?;
        let id = c.id();
        let r = self
            .guarded(id, cancel, None, async {
                c.exec::<Row, _, _>(sql, Params::Positional(params))
                    .await
                    .map_err(map_mysql)
            })
            .await;
        if r.is_err() {
            // The connection may be mid-result; never return it to the pool.
            drop(c);
        }
        r
    }

    pub async fn table(&self, object: &ObjectRef) -> ConnectorResult<Arc<TableMeta>> {
        let key = (
            database_of(&object.namespace)
                .unwrap_or_default()
                .to_owned(),
            object.name.clone(),
        );
        if let Some(m) = self.meta.lock().ok().and_then(|c| c.get(&key).cloned()) {
            return Ok(m);
        }
        let mut c = self.conn().await?;
        let m = Arc::new(meta::load(&mut c, object).await?);
        if let Ok(mut cache) = self.meta.lock() {
            cache.insert(key, m.clone());
        }
        Ok(m)
    }

    fn invalidate(&self) {
        if let Ok(mut c) = self.meta.lock() {
            c.clear();
        }
    }
}

/// Commits or rolls back a transaction on `conn`.
async fn finish<T>(mut conn: Conn, result: ConnectorResult<T>) -> ConnectorResult<T> {
    match result {
        Ok(v) => {
            conn.query_drop("COMMIT").await.map_err(map_mysql)?;
            Ok(v)
        }
        Err(e) => {
            // A failed ROLLBACK drops the connection, which the server rolls back.
            let _ = conn.query_drop("ROLLBACK").await;
            Err(e)
        }
    }
}

/// An open MySQL connection pool.
#[derive(Debug, Clone)]
pub struct MySqlConnection {
    inner: Arc<Inner>,
}

impl MySqlConnection {
    pub(crate) async fn new(
        pool: Pool,
        manifest: CapabilityManifest,
        read_only: bool,
        query_timeout: Option<Duration>,
        tunnel: Option<gazabridge_ssh::Tunnel>,
    ) -> ConnectorResult<Self> {
        let mut c = pool.get_conn().await.map_err(map_mysql)?;
        let version: Option<String> = c.query_first("SELECT VERSION()").await.map_err(map_mysql)?;
        drop(c);
        let flavor = if version
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("mariadb")
        {
            Flavor::MariaDb
        } else {
            Flavor::MySql
        };
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                manifest,
                read_only,
                flavor,
                query_timeout,
                meta: Mutex::new(HashMap::new()),
                _tunnel: tunnel,
            }),
        })
    }
}

fn require_language(req: &QueryRequest) -> ConnectorResult<()> {
    if req.language != QueryLanguage::MySql {
        return Err(ConnectorError::invalid_input(
            "MySQL connections execute MySQL SQL only",
        ));
    }
    Ok(())
}

fn params_of(req: &QueryRequest) -> ConnectorResult<Vec<My>> {
    req.params.iter().map(|v| encode(v, None)).collect()
}

fn entry(name: String, kind: ObjectKind, estimated_count: Option<u64>) -> NamespaceEntry {
    NamespaceEntry {
        name,
        kind,
        comment: None,
        estimated_count,
    }
}

const SYSTEM_DATABASES: [&str; 4] = ["information_schema", "performance_schema", "mysql", "sys"];

fn result_columns(cols: &[mysql_async::Column]) -> Vec<ColumnInfo> {
    cols.iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo {
            name: c.name_str().into_owned(),
            native_type: format!("{:?}", c.column_type())
                .trim_start_matches("MYSQL_TYPE_")
                .to_ascii_lowercase(),
            canonical_type: canonical_for_column(c),
            nullable: true,
            default_expression: None,
            is_generated: false,
            is_identity: false,
            comment: None,
            ordinal: i as u32 + 1,
        })
        .collect()
}

/// Decodes a result row of an ad-hoc query using the result metadata.
fn decode_result_row(row: Row, columns: &[ColumnInfo]) -> Record {
    let binary_flags: Vec<bool> = row
        .columns_ref()
        .iter()
        .map(|c| c.character_set() == 63)
        .collect();
    let values = row.unwrap();
    let mut record = Record::with_capacity(columns.len());
    for ((v, c), binary) in values.into_iter().zip(columns).zip(binary_flags) {
        let is_json = c.canonical_type == CanonicalType::Json;
        let binary = binary
            && !is_json
            && matches!(
                c.canonical_type,
                CanonicalType::Binary { .. } | CanonicalType::Geometry
            );
        let raw = raw_value(v, binary, c.canonical_type == CanonicalType::Date);
        record.insert(c.name.clone(), read::decode_typed(raw, &c.canonical_type));
    }
    record
}

#[derive(Default)]
struct Collected {
    columns: Vec<ColumnInfo>,
    rows: Vec<Record>,
    truncated: bool,
    affected: u64,
    saw_rows: bool,
}

async fn drain<P: mysql_async::prelude::Protocol>(
    mut result: mysql_async::QueryResult<'_, 'static, P>,
    max: usize,
) -> ConnectorResult<Collected> {
    let mut out = Collected::default();
    loop {
        let cols = result.columns().map(|c| c.to_vec()).unwrap_or_default();
        let has_columns = !cols.is_empty();
        if has_columns && !out.saw_rows {
            out.columns = result_columns(&cols);
            out.saw_rows = true;
        }
        // `collect` finishes this set and may already load the next one; a
        // column-less last set is then never seen by the loop, so its
        // affected rows are read right after.
        let set_affected = result.affected_rows();
        let more_before = !result.is_empty();
        let set_rows: Vec<Row> = result.collect().await.map_err(map_mysql)?;
        if has_columns {
            if out.columns.len() == cols.len() {
                for row in set_rows {
                    if out.rows.len() >= max {
                        out.truncated = true;
                        break;
                    }
                    out.rows.push(decode_result_row(row, &out.columns));
                }
            }
        } else {
            out.affected += set_affected;
        }
        if result.is_empty() {
            let next_loaded = if has_columns { true } else { more_before };
            if next_loaded && result.columns_ref().is_empty() {
                // Stale values are 0 after a row set (its EOF packet).
                out.affected += result.affected_rows();
            }
            break;
        }
    }
    Ok(out)
}

/// Runs one request and collects the first result set that has columns.
async fn run_statement(
    conn: &mut Conn,
    req: &QueryRequest,
    params: Vec<My>,
) -> ConnectorResult<QueryResult> {
    let started = Instant::now();
    let max = req.max_rows as usize;
    let mut notices = Vec::new();
    // Prepared statements give typed values; scripts and statements the
    // binary protocol cannot prepare go through the text protocol.
    let collected = match conn.prep(req.text.as_str()).await {
        Ok(stmt) => {
            let r = conn
                .exec_iter(stmt, Params::Positional(params))
                .await
                .map_err(map_mysql)?;
            drain(r, max).await?
        }
        Err(mysql_async::Error::Server(e))
            if params.is_empty() && matches!(e.code, 1295 | 1064) =>
        {
            notices.push("executed with the text protocol; values are returned as text".to_owned());
            let r = conn
                .query_iter(req.text.as_str())
                .await
                .map_err(map_mysql)?;
            drain(r, max).await?
        }
        Err(e) => return Err(map_mysql(e)),
    };
    Ok(QueryResult {
        affected: (!collected.saw_rows).then_some(collected.affected),
        columns: collected.columns,
        rows: collected.rows,
        truncated: collected.truncated,
        elapsed: started.elapsed(),
        notices,
    })
}

fn version_number(banner: &str) -> String {
    banner
        .split(['-', ' '])
        .next()
        .unwrap_or(banner)
        .trim()
        .to_owned()
}

#[async_trait]
impl Connection for MySqlConnection {
    fn manifest(&self) -> &CapabilityManifest {
        &self.inner.manifest
    }

    fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        let mut c = self.inner.conn().await?;
        let started = Instant::now();
        c.query_drop("SELECT 1").await.map_err(map_mysql)?;
        Ok(HealthReport {
            latency: started.elapsed(),
            checked_at: Utc::now(),
            notes: Vec::new(),
        })
    }

    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        let mut c = self.inner.conn().await?;
        let row: Option<Row> = c
            .query_first("SELECT VERSION(), @@version_comment, DATABASE()")
            .await
            .map_err(map_mysql)?;
        let row = row.ok_or_else(|| ConnectorError::internal("no version row"))?;
        let banner = cell_str(&row, 0).unwrap_or_default();
        let comment = cell_str(&row, 1).unwrap_or_default();
        let lower = format!("{banner} {comment}").to_ascii_lowercase();
        let product = if lower.contains("tidb") {
            "TiDB"
        } else if lower.contains("mariadb") {
            "MariaDB"
        } else if lower.contains("percona") {
            "Percona Server"
        } else {
            "MySQL"
        };
        let version = if product == "TiDB" {
            banner
                .split("TiDB-v")
                .nth(1)
                .map(version_number)
                .unwrap_or_else(|| version_number(&banner))
        } else {
            version_number(&banner)
        };
        let mut properties = BTreeMap::new();
        if let Some(db) = cell_str(&row, 2) {
            properties.insert("database".to_owned(), db);
        }
        properties.insert("version_comment".to_owned(), comment);
        for var in [
            "sql_mode",
            "character_set_server",
            "collation_server",
            "log_bin",
            "binlog_format",
            "gtid_mode",
            "read_only",
            "max_allowed_packet",
            "lower_case_table_names",
        ] {
            let r: Result<Option<Row>, _> =
                c.exec_first("SELECT @@GLOBAL.".to_owned() + var, ()).await;
            if let Ok(Some(r)) = r
                && let Some(v) = cell_str(&r, 0)
            {
                properties.insert(var.to_owned(), v);
            }
        }
        Ok(ServerInfo {
            family: EngineFamily::MySql,
            product: product.to_owned(),
            version: Version(version),
            version_string: banner,
            properties,
        })
    }

    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        let mut c = self.inner.conn().await?;
        let Some(db) = database_of(parent) else {
            let rows: Vec<Row> = c
                .query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME")
                .await
                .map_err(map_mysql)?;
            return Ok(rows
                .iter()
                .filter_map(|r| cell_str(r, 0))
                .filter(|n| !SYSTEM_DATABASES.contains(&n.to_ascii_lowercase().as_str()))
                .map(|n| entry(n, ObjectKind::Database, None))
                .collect());
        };
        let p = || Params::Positional(vec![My::Bytes(db.as_bytes().to_vec())]);
        let mut out = Vec::new();
        let tables: Vec<Row> = c
            .exec(
                "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
                p(),
            )
            .await
            .map_err(map_mysql)?;
        for r in &tables {
            let kind = match cell_str(r, 1).as_deref() {
                Some("VIEW" | "SYSTEM VIEW") => ObjectKind::View,
                Some("SEQUENCE") => ObjectKind::Sequence,
                _ => ObjectKind::Table,
            };
            let estimate = (kind == ObjectKind::Table)
                .then(|| cell_u64(r, 2))
                .flatten();
            out.push(entry(cell_str(r, 0).unwrap_or_default(), kind, estimate));
        }
        let routines: Vec<Row> = c
            .exec(
                "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME",
                p(),
            )
            .await
            .map_err(map_mysql)?;
        for r in &routines {
            let kind = if cell_str(r, 1).as_deref() == Some("PROCEDURE") {
                ObjectKind::Procedure
            } else {
                ObjectKind::Function
            };
            out.push(entry(cell_str(r, 0).unwrap_or_default(), kind, None));
        }
        let triggers: Vec<Row> = c
            .exec(
                "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
                 WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME",
                p(),
            )
            .await
            .map_err(map_mysql)?;
        for r in &triggers {
            out.push(entry(
                cell_str(r, 0).unwrap_or_default(),
                ObjectKind::Trigger,
                None,
            ));
        }
        Ok(out)
    }

    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        quote_ident(ident)
    }

    async fn close(&self) {
        let _ = self.inner.pool.clone().disconnect().await;
    }

    async fn inspect(
        &self,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        let m = self.inner.table(object).await?;
        let mut c = self.inner.conn().await?;
        let indexes = if options.include_indexes {
            meta::indexes(&mut c, &m.database, &object.name).await?
        } else {
            Vec::new()
        };
        let constraints = if options.include_constraints {
            meta::constraints(&mut c, &m.database, &object.name).await?
        } else {
            Vec::new()
        };
        let definition = if m.kind == ObjectKind::View {
            let r: Option<Row> = c
                .exec_first(
                    "SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    (m.database.clone(), object.name.clone()),
                )
                .await
                .map_err(map_mysql)?;
            r.and_then(|r| cell_str(&r, 0))
        } else {
            None
        };
        Ok(ObjectSchema {
            object: ObjectRef {
                kind: m.kind,
                namespace: NamespacePath::database(m.database.clone()),
                name: object.name.clone(),
            },
            columns: m.columns.clone(),
            primary_key: m.primary_key.clone(),
            indexes,
            constraints,
            inferred: None,
            estimated_count: m.estimated_rows,
            comment: m.comment.clone(),
            definition,
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
        max_time: Duration,
    ) -> ConnectorResult<CountResult> {
        let m = self.inner.table(object).await?;
        Ok(CountResult {
            count: read::count(&self.inner, &m, object, filter, Some(max_time)).await?,
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
                count: read::count(&self.inner, &m, object, filter, None).await?,
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
                        "no statistics for this object; request an exact count",
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
        let mut c = self.inner.conn().await?;
        let id = c.id();
        c.query_drop("START TRANSACTION").await.map_err(map_mysql)?;
        let result = self
            .inner
            .guarded(id, &cancel, None, write::write_records(&mut c, &m, &batch))
            .await;
        finish(c, result).await
    }

    async fn update(&self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let mut c = self.inner.conn().await?;
        c.query_drop("START TRANSACTION").await.map_err(map_mysql)?;
        let result = write::update(&mut c, &m, &request).await;
        finish(c, result).await
    }

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        self.inner.ensure_writable()?;
        let m = self.inner.table(&request.object).await?;
        let mut c = self.inner.conn().await?;
        c.query_drop("START TRANSACTION").await.map_err(map_mysql)?;
        let result = write::delete(&mut c, &m, &request).await;
        finish(c, result).await
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        self.inner.ensure_writable()?;
        let mut c = self.inner.conn().await?;
        c.query_drop("START TRANSACTION").await.map_err(map_mysql)?;
        Ok(Box::new(MyTransaction {
            inner: self.inner.clone(),
            conn: Some(c),
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
        // Validate everything before running anything.
        sql::plan(ops)?;
        let mut c = self.inner.conn().await?;
        let id = c.id();
        let result = self
            .inner
            .guarded(id, &cancel, None, async {
                for op in ops {
                    // MySQL has no CREATE INDEX IF NOT EXISTS.
                    if let SchemaOperation::CreateIndex {
                        object,
                        index,
                        if_not_exists: true,
                    } = op
                    {
                        let db = match database_of(&object.namespace) {
                            Some(d) => d.to_owned(),
                            None => meta::current_database(&mut c).await?.unwrap_or_default(),
                        };
                        let found: Option<Row> = c
                            .exec_first(
                                "SELECT 1 FROM information_schema.STATISTICS
                                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1",
                                (db, object.name.clone(), index.name.clone()),
                            )
                            .await
                            .map_err(map_mysql)?;
                        if found.is_some() {
                            continue;
                        }
                    }
                    for stmt in sql::plan(std::slice::from_ref(op))?.statements {
                        c.query_drop(stmt.as_str()).await.map_err(map_mysql)?;
                    }
                }
                Ok(())
            })
            .await;
        self.inner.invalidate();
        result
    }

    fn native_type_for(&self, canonical: &CanonicalType) -> ConnectorResult<String> {
        native_type_for(canonical).map(|(t, _)| t)
    }

    async fn execute(
        &self,
        request: QueryRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<QueryResult> {
        require_language(&request)?;
        if self.inner.read_only && !is_read_only_statement(&request.text) {
            return Err(ConnectorError::ReadOnly);
        }
        let params = params_of(&request)?;
        let mut c = self.inner.conn().await?;
        let id = c.id();
        let timeout = request.timeout.or(self.inner.query_timeout);
        let result = self
            .inner
            .guarded(
                id,
                &cancel,
                timeout,
                run_statement(&mut c, &request, params),
            )
            .await;
        if result.is_err() {
            drop(c);
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
        let read_only_stmt = is_read_only_statement(&request.text);
        let first = request
            .text
            .trim_start()
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        let dml = matches!(first.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE");
        if analyze && !(read_only_stmt || (dml && !self.inner.read_only)) {
            return Err(ConnectorError::invalid_input(
                "EXPLAIN ANALYZE runs the statement; only queries and INSERT/UPDATE/DELETE (rolled back) are allowed",
            ));
        }
        let sql = match (analyze, self.inner.flavor) {
            (false, _) => format!("EXPLAIN FORMAT=JSON {}", request.text),
            (true, Flavor::MariaDb) => format!("ANALYZE FORMAT=JSON {}", request.text),
            (true, Flavor::MySql) => format!("EXPLAIN ANALYZE {}", request.text),
        };
        let mut c = self.inner.conn().await?;
        let id = c.id();
        let work = async {
            // ANALYZE executes the statement; it always runs in a transaction
            // that is rolled back.
            c.query_drop("START TRANSACTION").await.map_err(map_mysql)?;
            let r: ConnectorResult<Option<Row>> = c
                .exec_first(sql.as_str(), Params::Positional(params))
                .await
                .map_err(map_mysql);
            let _ = c.query_drop("ROLLBACK").await;
            r
        };
        let row = self
            .inner
            .guarded(id, &cancel, request.timeout, work)
            .await?;
        let text = row.and_then(|r| cell_str(&r, 0)).unwrap_or_default();
        let native = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text.clone()));
        let rendered = if native.is_string() {
            text
        } else {
            serde_json::to_string_pretty(&native).unwrap_or(text)
        };
        Ok(ExplainPlan {
            native,
            text: rendered,
            analyzed: analyze,
        })
    }
}

/// A transaction on a dedicated pooled connection. Dropped without commit
/// or rollback, the pool resets the connection, which rolls it back.
struct MyTransaction {
    inner: Arc<Inner>,
    conn: Option<Conn>,
}

impl MyTransaction {
    fn conn(&mut self) -> ConnectorResult<&mut Conn> {
        self.conn
            .as_mut()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))
    }
}

#[async_trait]
impl Transaction for MyTransaction {
    async fn write_batch(&mut self, batch: WriteBatch) -> ConnectorResult<WriteOutcome> {
        let m = self.inner.table(&batch.object).await?;
        write::write_records(self.conn()?, &m, &batch).await
    }

    async fn update(&mut self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::update(self.conn()?, &m, &request).await
    }

    async fn delete(&mut self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        let m = self.inner.table(&request.object).await?;
        write::delete(self.conn()?, &m, &request).await
    }

    async fn execute(&mut self, request: QueryRequest) -> ConnectorResult<QueryResult> {
        require_language(&request)?;
        let params = params_of(&request)?;
        run_statement(self.conn()?, &request, params).await
    }

    async fn commit(mut self: Box<Self>) -> ConnectorResult<()> {
        let c = self
            .conn
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        finish(c, Ok(())).await
    }

    async fn rollback(mut self: Box<Self>) -> ConnectorResult<()> {
        let mut c = self
            .conn
            .take()
            .ok_or_else(|| ConnectorError::internal("transaction already finished"))?;
        c.query_drop("ROLLBACK").await.map_err(map_mysql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_trimmed() {
        assert_eq!(version_number("8.0.36"), "8.0.36");
        assert_eq!(version_number("11.4.2-MariaDB-ubu2404"), "11.4.2");
        assert_eq!(version_number("8.0.11-TiDB-v7.5.1"), "8.0.11");
    }

    #[test]
    fn decoded_values_follow_the_column_type() {
        use gazabridge_domain::Value;
        let t = CanonicalType::Int { bits: 32 };
        assert_eq!(
            read::decode_typed(Value::String("42".into()), &t),
            Value::Int(42)
        );
        assert_eq!(
            read::decode_typed(Value::Binary(br#"{"a":1}"#.to_vec()), &CanonicalType::Json),
            Value::Json(serde_json::json!({"a": 1}))
        );
    }
}
