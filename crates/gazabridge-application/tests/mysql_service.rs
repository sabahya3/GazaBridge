//! The application services on a real MySQL server: connection test and
//! detection, Explorer browsing and editing, Query Studio, and export and
//! import through every row-friendly file format.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use gazabridge_application::{
    BrowseRequest, ConnectionService, ExplorerService, ExportRequest, ImportRequest, PasswordInput,
    QueryService, TransferService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::compatibility::ConnectionState;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, MySqlSettings, MySqlTlsMode, SecretRef,
};
use gazabridge_domain::ejson;
use gazabridge_domain::job::WriteMode;
use gazabridge_files::{
    Compression, FileFormat, JsonFlavor, MalformedPolicy, ReadOptions, WriteOptions,
};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::filter::{FieldPath, Filter};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

fn sql(text: &str) -> QueryRequest {
    QueryRequest {
        language: QueryLanguage::MySql,
        text: text.into(),
        params: vec![],
        max_rows: 100,
        timeout: None,
        database: None,
        collection: None,
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mysql_through_the_application_services() {
    let c = Mysql::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(3306).await.unwrap();

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
    let queries = QueryService::new(store.clone(), explorer.clone(), svc.redactor().clone());

    // ---- connection: a wrong password fails cleanly, then a fix works ----
    let profile = ConnectionProfile::new(
        "mysql",
        "mysql.mysql",
        ConnectionSettings::MySql(MySqlSettings {
            host,
            port,
            database: Some("test".into()),
            username: "root".into(),
            password: SecretRef::None,
            tls_mode: MySqlTlsMode::Disabled,
            charset: "utf8mb4".into(),
            connect_timeout_secs: 20,
            query_timeout_ms: 0,
            pool_size: 2,
        }),
    );
    let bad = svc
        .create(
            profile.clone(),
            PasswordInput::SessionOnly(Zeroizing::new("wrong".into())),
        )
        .await
        .unwrap();
    let outcome = svc.test_saved(bad.id).await.unwrap();
    assert_eq!(outcome.state, ConnectionState::ConnectionFailed);
    assert_eq!(
        outcome.error_code.as_deref(),
        Some("authentication"),
        "{outcome:?}"
    );

    // The image's root user has no password.
    let id = svc
        .update(bad.clone(), PasswordInput::None)
        .await
        .unwrap()
        .id;
    let outcome = svc.test_saved(id).await.unwrap();
    assert_eq!(
        outcome.server.as_ref().unwrap().product,
        "MySQL",
        "{outcome:?}"
    );
    assert_eq!(outcome.detected_profile.as_deref(), Some("mysql.mysql"));

    // ---- data ------------------------------------------------------------
    let conn = svc.open(id).await.unwrap();
    for stmt in [
        "CREATE TABLE src (id INT PRIMARY KEY, name VARCHAR(40) NULL, price DECIMAL(10,2), doc JSON, at DATETIME(3), big BIGINT UNSIGNED)",
        "CREATE TABLE dst LIKE src",
        "CREATE TABLE dst_csv LIKE src",
        "INSERT INTO src VALUES
            (1, 'غزة', 1.25, '{\"g\": 1}', '2026-10-25 01:00:00.123', 18446744073709551615),
            (2, 'two', 2.50, '[1, \"x\"]', '2026-10-25 02:00:00', 0),
            (3, NULL, 3.75, '\"text\"', '2026-10-25 03:00:00', 3),
            (4, 'emoji 🚀', 5.00, '{}', '2026-10-25 04:00:00', 4)",
    ] {
        conn.execute(sql(stmt), CancellationToken::new())
            .await
            .unwrap();
    }

    // ---- explorer --------------------------------------------------------
    let dbs = explorer.list(id, &NamespacePath::root()).await.unwrap();
    assert!(
        dbs.iter()
            .any(|d| d.name == "test" && d.kind == ObjectKind::Database)
    );
    let objects = explorer
        .list(id, &NamespacePath::database("test"))
        .await
        .unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o.name == "src" && o.kind == ObjectKind::Table)
    );
    let src = ObjectRef::table(NamespacePath::database("test"), "src");
    let page = explorer
        .browse(
            id,
            BrowseRequest {
                object: src.clone(),
                filter: Some(Filter::Ge {
                    field: FieldPath::new("id"),
                    value: Value::Int(2),
                }),
                projection: vec![],
                exclude: vec![],
                order_by: vec![],
                offset: 0,
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert!(page.has_more);
    assert_eq!(page.key_fields, vec!["id".to_string()]);
    assert!(page.edit_blocker.is_none(), "{:?}", page.edit_blocker);

    explorer
        .insert(id, &src, r#"{"id": 9, "name": "nine", "price": "9.99"}"#)
        .await
        .unwrap();
    let nine = explorer
        .browse(
            id,
            BrowseRequest {
                object: src.clone(),
                filter: Some(Filter::eq("id", Value::Int(9))),
                projection: vec![],
                exclude: vec![],
                order_by: vec![],
                offset: 0,
                limit: 1,
            },
        )
        .await
        .unwrap();
    let original = ejson::record_to_ejson_string(&nine.records[0]);
    let changed = original.replace("\"nine\"", "\"NINE\"");
    assert_ne!(original, changed);
    let outcome = explorer
        .apply_update(id, &src, &original, &changed)
        .await
        .unwrap();
    assert_eq!(outcome.matched, 1);
    explorer.delete(id, &src, &changed).await.unwrap();

    // ---- query studio ------------------------------------------------------
    let ctx = queries.context(id).await.unwrap();
    assert!(ctx.languages.contains(&QueryLanguage::MySql), "{ctx:?}");

    // ---- export / import: JSONL (typed) and CSV ------------------------------
    let dir = tempfile::tempdir().unwrap();
    for (format, flavor, target) in [
        (FileFormat::Jsonl, JsonFlavor::Typed, "dst"),
        (FileFormat::Csv, JsonFlavor::Typed, "dst_csv"),
    ] {
        let path = dir.path().join(format!("src.{target}"));
        let out = transfers
            .export(
                id,
                ExportRequest {
                    object: src.clone(),
                    filter: None,
                    projection: vec![],
                    exclude: vec![],
                    order_by: vec![],
                    skip: 0,
                    limit: None,
                    path: path.clone(),
                    options: WriteOptions::new(format),
                },
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(out.records_read, 4);
        if format == FileFormat::Csv {
            // CSV import is not offered yet; the file must still hold every row.
            let text = std::fs::read_to_string(&path).unwrap();
            assert_eq!(text.lines().count(), 5, "{text}");
            assert!(text.contains("غزة") && text.contains("18446744073709551615"));
            continue;
        }
        let r = transfers
            .import(
                id,
                ImportRequest {
                    object: ObjectRef::table(NamespacePath::database("test"), target),
                    path: path.clone(),
                    options: ReadOptions {
                        format,
                        json_flavor: flavor,
                        compression: Compression::None,
                        malformed: MalformedPolicy::Fail,
                    },
                    mode: WriteMode::Insert,
                    key_fields: vec![],
                    batch_size: 3,
                    stop_on_error: false,
                },
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!((r.inserted, r.failed), (4, 0), "{r:?}");
        let same = conn
            .execute(
                sql(&format!(
                    "SELECT COUNT(*) AS n FROM src s LEFT JOIN {target} d ON d.id = s.id
                     AND d.name <=> s.name AND d.price = s.price AND d.doc = s.doc
                     AND d.at = s.at AND d.big = s.big WHERE d.id IS NULL"
                )),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            same.rows[0].get("n"),
            &Value::Int(0),
            "rows changed via {format:?}"
        );
    }
}
