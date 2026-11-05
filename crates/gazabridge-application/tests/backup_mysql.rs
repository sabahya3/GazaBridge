//! MySQL backups in the GazaBridge format against a real server: tables,
//! keys, defaults and rows restored into another database, and the backup a
//! table copy takes before it replaces its MySQL target.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::backup::{BackupMethod, BackupRequest, BackupService, RestoreRequest};
use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, TransferService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Record;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, MySqlSettings, MySqlTlsMode, SecretRef,
};
use gazabridge_domain::job::JobStatus;
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;

async fn sql(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    conn.execute(
        QueryRequest {
            language: QueryLanguage::MySql,
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

fn table(db: &str, name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::database(db), name)
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mysql_backups_restore_tables_keys_and_rows_into_another_database() {
    let my = Mysql::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("mysql");
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(gazabridge_connector_mysql::MySqlConnectorFactory))
        .unwrap();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );
    let explorer = ExplorerService::new(store.clone(), svc.clone());
    let transfers = TransferService::new(store.clone(), explorer.clone(), svc.redactor().clone());
    let dir = tempfile::tempdir().unwrap();
    let backups = BackupService::new(
        store.clone(),
        svc.clone(),
        explorer.clone(),
        transfers,
        dir.path().join("backups"),
    );
    let id = svc
        .create(
            ConnectionProfile::new(
                "mysql",
                "mysql.mysql",
                ConnectionSettings::MySql(MySqlSettings {
                    host: my.get_host().await.unwrap().to_string(),
                    port: my.get_host_port_ipv4(3306).await.unwrap(),
                    database: Some("test".into()),
                    username: "root".into(),
                    password: SecretRef::None,
                    tls_mode: MySqlTlsMode::Disabled,
                    charset: "utf8mb4".into(),
                    connect_timeout_secs: 20,
                    query_timeout_ms: 0,
                    pool_size: 2,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id;
    let conn = svc.open(id).await.unwrap();
    for s in [
        "CREATE TABLE test.parents (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(40) NOT NULL DEFAULT 'it''s new')",
        "CREATE TABLE test.children (
            id BIGINT PRIMARY KEY,
            parent_id INT NOT NULL,
            amount DECIMAL(12,2),
            at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
            doc JSON,
            note TEXT,
            big BIGINT UNSIGNED,
            flag TINYINT(1) DEFAULT 0,
            CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES test.parents (id),
            KEY children_by_parent (parent_id)
        )",
        "INSERT INTO test.parents (name) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 50) SELECT CONCAT('p', n) FROM g",
        "INSERT INTO test.children WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 700)
            SELECT n, 1 + n % 50, n * 1.25, TIMESTAMP '2026-01-01 00:00:00' + INTERVAL n MINUTE,
                   JSON_OBJECT('g', n, 'ar', 'نص'), IF(n % 3 = 0, NULL, CONCAT('n', n)),
                   18446744073709551615 - n, n % 2 FROM g",
    ] {
        sql(&conn, s).await;
    }

    // Children first on purpose: the restore orders tables by their keys.
    let path = backups
        .backup(
            &BackupRequest {
                connection: id,
                objects: vec![table("test", "children"), table("test", "parents")],
                method: BackupMethod::GazaBridge,
                label: None,
            },
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap();
    let m = backups.manifest(&path).await.unwrap();
    assert!(
        m.warnings.iter().any(|w| w.contains("auto-increment")),
        "{:?}",
        m.warnings
    );
    assert_eq!(m.objects.iter().map(|o| o.records).sum::<u64>(), 750);

    // mysqldump is not offered; the message says so.
    let err = backups
        .backup(
            &BackupRequest {
                connection: id,
                objects: vec![table("test", "parents")],
                method: BackupMethod::Native,
                label: None,
            },
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("mysqldump"), "{err}");

    let req = RestoreRequest {
        backup: path.clone(),
        connection: id,
        rename_to: Some("restored".into()),
        only: vec![],
        existing: ExistingData::Fail,
    };
    let (plan, _) = backups.plan_restore(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.statements[0].starts_with("CREATE DATABASE IF NOT EXISTS `restored`"),
        "{:?}",
        plan.statements
    );
    backups
        .restore(&req, false, CancellationToken::new(), &|_| {})
        .await
        .unwrap();
    for t in ["parents", "children"] {
        let a = sql(&conn, &format!("SELECT * FROM test.{t} ORDER BY id")).await;
        let b = sql(&conn, &format!("SELECT * FROM restored.{t} ORDER BY id")).await;
        assert_eq!(a, b, "{t}");
    }
    let types = sql(
        &conn,
        "SELECT GROUP_CONCAT(COLUMN_TYPE ORDER BY ORDINAL_POSITION) AS t FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = 'restored' AND TABLE_NAME = 'children'",
    )
    .await;
    assert_eq!(
        types[0].get("t"),
        &gazabridge_domain::Value::String(
            "bigint,int,decimal(12,2),datetime(3),json,text,bigint unsigned,tinyint(1)".into()
        )
    );
    // Defaults came back as defaults, foreign key and index too.
    sql(&conn, "INSERT INTO restored.parents (id) VALUES (999)").await;
    assert_eq!(
        sql(&conn, "SELECT name FROM restored.parents WHERE id = 999").await[0].get("name"),
        &gazabridge_domain::Value::String("it's new".into())
    );
    sql(
        &conn,
        "INSERT INTO restored.children (id, parent_id) VALUES (9999, 999)",
    )
    .await;
    let row = &sql(
        &conn,
        "SELECT at IS NOT NULL AS has_at, flag FROM restored.children WHERE id = 9999",
    )
    .await[0];
    assert_eq!(row.get("has_at"), &gazabridge_domain::Value::Int(1));
    assert_eq!(
        sql(
            &conn,
            "SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = 'restored'
             AND CONSTRAINT_TYPE = 'FOREIGN KEY'"
        )
        .await
        .len(),
        1
    );
    assert_eq!(
        sql(
            &conn,
            "SELECT DISTINCT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'restored'
             AND INDEX_NAME = 'children_by_parent'"
        )
        .await
        .len(),
        1
    );

    // A table copy that replaces its MySQL target backs the target up first.
    let migrations = MigrationService::new(
        store,
        explorer,
        svc.redactor().clone(),
        dir.path().join("q"),
    )
    .with_backups(backups.clone());
    let req = MigrationRequest {
        name: None,
        source: id,
        target: id,
        items: vec![CopyItem {
            source: table("test", "parents"),
            target: table("restored", "parents"),
        }],
        existing: ExistingData::Replace,
        copy_indexes: true,
        batch_size: 100,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 1,
        doc_mode: Default::default(),
        table_mode: Default::default(),
        backup_before_replace: true,
        sync: None,
    };
    // The restored children reference these parents.
    sql(&conn, "DROP TABLE restored.children").await;
    let plan = migrations.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.warnings.iter().any(|w| w.contains("backed up")),
        "{:?}",
        plan.warnings
    );
    let run = migrations.start(req, true).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let v = loop {
        let v = migrations.view(run).await.unwrap();
        if v.status.is_terminal() {
            break v;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    let backup = v.streams[0]
        .backup
        .clone()
        .expect("a backup before the drop");
    let m = backups.manifest(&backup).await.unwrap();
    assert_eq!(
        (
            m.objects[0].object.to_string().as_str(),
            m.objects[0].records
        ),
        ("restored.parents", 51)
    );
    assert_eq!(
        sql(&conn, "SELECT COUNT(*) AS n FROM restored.parents").await[0].get("n"),
        &gazabridge_domain::Value::Int(50)
    );
}
