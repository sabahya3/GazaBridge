//! Table copies between relational engines against real servers:
//! PostgreSQL → MySQL, MySQL → PostgreSQL, PostgreSQL → PostgreSQL and
//! MySQL → MySQL,
//! with deep validation, indexes, reruns and the plan's blockers.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::report::ReportFormat;
use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, RunView, ValidationReport,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, MySqlSettings, MySqlTlsMode, PgSslMode,
    PostgresSettings, SecretRef,
};
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::JobStatus;
use gazabridge_domain::{Record, Value};
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

async fn sql(conn: &Arc<dyn Connection>, language: QueryLanguage, text: &str) -> Vec<Record> {
    conn.execute(
        QueryRequest {
            language,
            text: text.into(),
            params: vec![],
            max_rows: 10_000,
            timeout: None,
            database: None,
            collection: None,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|e| panic!("{text}: {e}"))
    .rows
}

async fn my(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    sql(conn, QueryLanguage::MySql, text).await
}

async fn pg(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    sql(conn, QueryLanguage::PostgresSql, text).await
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        Value::Decimal(d) => d.to_plain_string().parse().unwrap(),
        other => panic!("not a count: {other:?}"),
    }
}

async fn finish(m: &MigrationService, run: RunId) -> RunView {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let v = m.view(run).await.unwrap();
        if v.status.is_terminal() || v.status == JobStatus::Paused {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {v:#?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn deep(m: &MigrationService, run: RunId) -> ValidationReport {
    m.validate(run, Level::Deep).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(v) = m.view(run).await.unwrap().validation
            && !v.running
        {
            return v;
        }
        assert!(Instant::now() < deadline, "validation timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn request(source: ConnectionId, target: ConnectionId, items: Vec<CopyItem>) -> MigrationRequest {
    MigrationRequest {
        name: None,
        source,
        target,
        items,
        existing: ExistingData::Skip,
        copy_indexes: true,
        batch_size: 50,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 2,
        doc_mode: DocMode::Flatten,
        table_mode: TableMode::Reference,
        backup_before_replace: false,
        sync: None,
    }
}

fn pg_table(schema: &str, name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::schema(Some("postgres".into()), schema), name)
}

fn my_table(db: &str, name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::database(db), name)
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn tables_copy_between_postgresql_and_mysql() {
    let (pgc, myc) = tokio::join!(
        async {
            Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .expect("postgres")
        },
        async {
            Mysql::default()
                .with_tag("8.4")
                .start()
                .await
                .expect("mysql")
        }
    );

    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    registry
        .register(Arc::new(gazabridge_connector_mysql::MySqlConnectorFactory))
        .unwrap();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );
    let quarantine = tempfile::tempdir().unwrap();
    let m = MigrationService::new(
        store.clone(),
        ExplorerService::new(store, svc.clone()),
        svc.redactor().clone(),
        quarantine.path().to_path_buf(),
    );
    let pg_id = svc
        .create(
            ConnectionProfile::new(
                "pg",
                "postgres.postgresql",
                ConnectionSettings::Postgres(PostgresSettings {
                    host: pgc.get_host().await.unwrap().to_string(),
                    port: pgc.get_host_port_ipv4(5432).await.unwrap(),
                    database: "postgres".into(),
                    username: "postgres".into(),
                    password: SecretRef::None,
                    ssl_mode: PgSslMode::Disable,
                    search_path: None,
                    application_name: "GazaBridge".into(),
                    connect_timeout_secs: 10,
                    statement_timeout_ms: 0,
                    pool_size: 4,
                }),
            ),
            // The image's documented default password.
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap()
        .id;
    let my_id = svc
        .create(
            ConnectionProfile::new(
                "mysql",
                "mysql.mysql",
                ConnectionSettings::MySql(MySqlSettings {
                    host: myc.get_host().await.unwrap().to_string(),
                    port: myc.get_host_port_ipv4(3306).await.unwrap(),
                    database: Some("test".into()),
                    username: "root".into(),
                    password: SecretRef::None,
                    tls_mode: MySqlTlsMode::Disabled,
                    charset: "utf8mb4".into(),
                    connect_timeout_secs: 20,
                    query_timeout_ms: 0,
                    pool_size: 4,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id;
    let pgx = svc.open(pg_id).await.unwrap();
    let myx = svc.open(my_id).await.unwrap();

    for stmt in [
        "CREATE TABLE items (
            id bigint PRIMARY KEY,
            name text NOT NULL,
            price numeric(12,2),
            tags jsonb,
            created timestamptz,
            uid uuid,
            active boolean,
            note varchar(50),
            data bytea,
            day date,
            score double precision
        )",
        "CREATE UNIQUE INDEX items_name ON items (name)",
        "CREATE INDEX items_active_created ON items (active, created)",
        "CREATE INDEX items_lower ON items (lower(note))",
        "INSERT INTO items
            SELECT i, 'عنصر ' || i, (i * 1.25)::numeric(12,2),
                   jsonb_build_object('n', i, 'tags', jsonb_build_array('a', i)),
                   timestamptz '2026-01-01 00:00:00+00' + i * interval '1 hour',
                   md5(i::text)::uuid, i % 2 = 0,
                   CASE WHEN i % 5 = 0 THEN NULL ELSE 'note 🚀 ' || i END,
                   decode(md5(i::text), 'hex'), date '2026-01-01' + i, i / 3.0
            FROM generate_series(1, 300) i",
        "CREATE TABLE lines (item_id bigint REFERENCES items(id), n int, qty int, PRIMARY KEY (item_id, n))",
        "INSERT INTO lines SELECT i, k, i * k FROM generate_series(1, 300) i, generate_series(1, 2) k",
        "CREATE TABLE nokey (a int)",
    ] {
        pg(&pgx, stmt).await;
    }

    // ---- a table without a primary key is a blocker -----------------------
    let blocked = m
        .plan(&request(
            pg_id,
            my_id,
            vec![CopyItem {
                source: pg_table("public", "nokey"),
                target: my_table("copy", "nokey"),
            }],
        ))
        .await
        .unwrap();
    assert!(
        blocked.blockers.iter().any(|b| b.contains("primary key")),
        "{:?}",
        blocked.blockers
    );

    // ---- PostgreSQL → MySQL into a new database ----------------------------
    let req = request(
        pg_id,
        my_id,
        vec![
            CopyItem {
                source: pg_table("public", "items"),
                target: my_table("copy", "items"),
            },
            CopyItem {
                source: pg_table("public", "lines"),
                target: my_table("copy", "lines"),
            },
        ],
    );
    let plan = m.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let items = &plan.items[0];
    assert_eq!(items.kind, "table_copy");
    assert!(
        items.statements[0].starts_with("CREATE DATABASE IF NOT EXISTS `copy`"),
        "{:?}",
        items.statements
    );
    let types = &items.tables[0].types;
    let cols = &items.tables[0].columns;
    let ty = |c: &str| types[cols.iter().position(|x| x == c).unwrap()].clone();
    assert_eq!(ty("tags"), "json");
    assert_eq!(ty("price"), "decimal(12,2)");
    assert!(ty("name").starts_with("varchar("), "{types:?}");
    assert_eq!(items.tables[0].rows.len(), 3);
    assert!(
        items.warnings.iter().any(|w| w.contains("items_lower")),
        "{:?}",
        items.warnings
    );
    assert!(
        plan.items[1]
            .warnings
            .iter()
            .any(|w| w.contains("foreign keys")),
        "{:?}",
        plan.items[1].warnings
    );
    assert!(my(&myx, "SHOW DATABASES LIKE 'copy'").await.is_empty());

    let run = m.start(req.clone(), false).await.unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 300);
    assert_eq!(v.streams[1].stats.inserted, 600);
    let report = deep(&m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");
    let indexes = my(
        &myx,
        "SELECT DISTINCT INDEX_NAME AS n FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = 'copy' AND TABLE_NAME = 'items' ORDER BY 1",
    )
    .await;
    let names: Vec<String> = indexes
        .iter()
        .map(|r| match r.get("n") {
            Value::String(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, ["items_active_created", "items_name", "PRIMARY"]);
    let row = &my(
        &myx,
        "SELECT name, price, JSON_EXTRACT(tags, '$.tags[1]') AS t, note, HEX(data) AS d, uid
         FROM copy.items WHERE id = 7",
    )
    .await[0];
    assert_eq!(row.get("name"), &Value::String("عنصر 7".into()));
    assert_eq!(row.get("note"), &Value::String("note 🚀 7".into()));
    assert_eq!(
        row.get("uid"),
        &Value::String("8f14e45f-ceea-167a-5a36-dedd4bea2543".into())
    );

    // A rerun copies nothing new; a change in the target fails validation.
    let again = finish(&m, m.start(req.clone(), false).await.unwrap()).await;
    assert_eq!(again.status, JobStatus::Completed, "{again:#?}");
    assert_eq!(again.streams[0].stats.inserted, 0);
    my(&myx, "UPDATE copy.items SET price = 0 WHERE id = 9").await;
    let changed = deep(&m, run).await;
    assert_eq!(changed.passed, Some(false), "{changed:#?}");

    // Reports in every format; files are never overwritten.
    let reports = tempfile::tempdir().unwrap();
    for format in [
        ReportFormat::Json,
        ReportFormat::Html,
        ReportFormat::Markdown,
        ReportFormat::Csv,
    ] {
        let path = reports.path().join(format!("run.{}", format.extension()));
        m.export_report(run, &path, format).await.unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("copy.items"), "{format:?}: {text}");
        match format {
            ReportFormat::Json => {
                let j: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(j["source_name"], "pg");
                assert_eq!(j["target_name"], "mysql");
                assert_eq!(j["validation"]["passed"], false);
                assert_eq!(j["streams"][0]["stats"]["inserted"], 300);
            }
            ReportFormat::Html => {
                assert!(text.starts_with("<!doctype html>"));
                assert!(
                    text.contains("<span class=\"bad\">differences</span>"),
                    "{text}"
                );
                assert!(!text.contains("<script"));
            }
            ReportFormat::Markdown => {
                assert!(
                    text.contains("public.items | copy.items | done | 300 | 300 |"),
                    "{text}"
                );
                assert!(text.contains("## Differences"), "{text}");
            }
            ReportFormat::Csv => {
                let lines: Vec<&str> = text.lines().collect();
                assert_eq!(lines.len(), 3, "{text}");
                assert!(
                    lines[1].contains("public.items,copy.items,done,300,300,0,0,0,"),
                    "{text}"
                );
                assert!(lines[1].contains(",differences,0,0,1,"), "{text}");
            }
        }
        let again = m.export_report(run, &path, format).await.unwrap_err();
        assert!(again.to_string().contains("already exists"), "{again}");
    }
    my(&myx, "UPDATE copy.items SET price = 11.25 WHERE id = 9").await;

    // ---- MySQL → PostgreSQL: back into another schema ----------------------
    let back = request(
        my_id,
        pg_id,
        vec![
            CopyItem {
                source: my_table("copy", "items"),
                target: pg_table("back", "items"),
            },
            CopyItem {
                source: my_table("copy", "lines"),
                target: pg_table("back", "lines"),
            },
        ],
    );
    let plan = m.plan(&back).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let run = m.start(back, false).await.unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    let report = deep(&m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");
    // Every value survived the round trip.
    let differ = pg(
        &pgx,
        "SELECT count(*) AS n FROM items a LEFT JOIN back.items b ON b.id = a.id
           AND b.name = a.name AND b.price IS NOT DISTINCT FROM a.price
           AND b.tags::jsonb = a.tags AND b.created = a.created
           AND b.uid::text = a.uid::text AND b.active = a.active
           AND b.note IS NOT DISTINCT FROM a.note AND b.data = a.data
           AND b.day = a.day AND b.score = a.score
         WHERE b.id IS NULL",
    )
    .await;
    assert_eq!(int(differ[0].get("n")), 0);
    assert_eq!(
        int(pg(&pgx, "SELECT count(*) AS n FROM back.lines").await[0].get("n")),
        600
    );

    // ---- PostgreSQL → PostgreSQL, replacing the target ---------------------
    let mut same = request(
        pg_id,
        pg_id,
        vec![CopyItem {
            source: pg_table("public", "items"),
            target: pg_table("back", "items"),
        }],
    );
    same.existing = ExistingData::Replace;
    let plan = m.plan(&same).await.unwrap();
    assert!(plan.destructive);
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let run = m.start(same, true).await.unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 300);
    let report = deep(&m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");
    let ty = pg(
        &pgx,
        "SELECT data_type AS t FROM information_schema.columns
         WHERE table_schema = 'back' AND table_name = 'items' AND column_name = 'uid'",
    )
    .await;
    assert_eq!(ty[0].get("t"), &Value::String("uuid".into()));

    // ---- MySQL → MySQL, into another database ------------------------------
    let run = m
        .start(
            request(
                my_id,
                my_id,
                vec![CopyItem {
                    source: my_table("copy", "items"),
                    target: my_table("copy2", "items"),
                }],
            ),
            false,
        )
        .await
        .unwrap();
    let v = finish(&m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 300);
    let report = deep(&m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");
}
