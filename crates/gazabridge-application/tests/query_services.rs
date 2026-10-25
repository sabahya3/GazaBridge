//! Query Studio service against real PostgreSQL and MongoDB servers.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;
use std::time::Duration;

use gazabridge_application::{
    ConnectionService, ExplorerService, PasswordInput, QueryInput, QueryService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::queries::HistoryStatus;
use mongodb::bson::doc;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use zeroize::Zeroizing;

async fn services() -> (MetadataStore, ConnectionService, QueryService) {
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    registry
        .register(Arc::new(
            gazabridge_connector_mongodb::MongoConnectorFactory,
        ))
        .unwrap();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );
    let explorer = ExplorerService::new(store.clone(), svc.clone());
    let queries = QueryService::new(store.clone(), explorer, svc.redactor().clone());
    (store, svc, queries)
}

fn q(language: QueryLanguage, text: &str) -> QueryInput {
    QueryInput {
        language,
        text: text.into(),
        params: vec![],
        database: None,
        collection: None,
        max_rows: 100,
        timeout: None,
        confirmed: false,
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn query_studio_on_postgres() {
    let c = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let (store, svc, queries) = services().await;
    let id = svc
        .create(
            ConnectionProfile::new(
                "pg",
                "postgres.postgresql",
                ConnectionSettings::Postgres(PostgresSettings {
                    host,
                    port,
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
    let sql = |text: &str| q(QueryLanguage::PostgresSql, text);

    let ctx = queries.context(id).await.unwrap();
    assert!(ctx.languages.contains(&QueryLanguage::PostgresSql));
    assert!(ctx.explain && ctx.explain_analyze);

    queries
        .run(
            id,
            "a",
            sql("CREATE TABLE t (id serial PRIMARY KEY, name text)"),
        )
        .await
        .unwrap();
    let mut insert = sql("INSERT INTO t (name) SELECT 'n' || g FROM generate_series(1, $1::int) g");
    insert.params = vec![Value::Int(5)];
    assert_eq!(
        queries.run(id, "b", insert).await.unwrap().affected,
        Some(5)
    );

    let mut select = sql("SELECT id, name FROM t WHERE id > $1 ORDER BY id");
    select.params = vec![Value::Int(1)];
    select.max_rows = 3;
    let r = queries.run(id, "c", select).await.unwrap();
    assert_eq!(r.rows.len(), 3);
    assert!(r.truncated);
    assert_eq!(*r.rows[0].get("name"), Value::String("n2".into()));

    // Destructive statements need confirmation.
    let err = queries
        .run(id, "d", sql("DELETE FROM t"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), "confirmation_required");

    // EXPLAIN, and EXPLAIN ANALYZE of an INSERT leaves no row behind.
    let plan = queries
        .explain(id, "e", sql("SELECT * FROM t WHERE id = 1"), false)
        .await
        .unwrap();
    assert!(!plan.analyzed);
    assert!(!plan.text.is_empty());
    let plan = queries
        .explain(id, "f", sql("INSERT INTO t (name) VALUES ('ghost')"), true)
        .await
        .unwrap();
    assert!(plan.analyzed);
    let n = queries
        .run(id, "g", sql("SELECT count(*) AS n FROM t"))
        .await
        .unwrap();
    assert_eq!(*n.rows[0].get("n"), Value::Int(5));

    // Cancel a long query from another task.
    let runner = queries.clone();
    let long = tokio::spawn(async move {
        runner
            .run(
                id,
                "sleep",
                q(QueryLanguage::PostgresSql, "SELECT pg_sleep(30)"),
            )
            .await
    });
    let mut cancelled = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if queries.cancel("sleep").await {
            cancelled = true;
            break;
        }
    }
    assert!(cancelled, "the query never became cancellable");
    let err = tokio::time::timeout(Duration::from_secs(10), long)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), "cancelled");

    // Timeouts.
    let mut slow = sql("SELECT pg_sleep(5)");
    slow.timeout = Some(Duration::from_millis(300));
    assert_eq!(
        queries.run(id, "h", slow).await.unwrap_err().code(),
        "connection"
    );

    // The server still answers after cancel and timeout.
    assert_eq!(
        queries
            .run(id, "i", sql("SELECT 1 AS one"))
            .await
            .unwrap()
            .rows
            .len(),
        1
    );

    let history = store.queries().history(Some(id), 50).await.unwrap();
    assert!(history.iter().any(|h| h.status == HistoryStatus::Cancelled));
    assert!(history.iter().any(|h| h.status == HistoryStatus::Error));
    assert!(
        !history.iter().any(|h| h.text == "DELETE FROM t"),
        "refused queries never ran"
    );
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn query_studio_on_mongodb() {
    let rs = common::mongo::start("8.0").await;
    let raw = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    raw.database("shop")
        .collection("orders")
        .insert_many([
            doc! { "status": "open", "total": 10 },
            doc! { "status": "open", "total": 5 },
            doc! { "status": "closed", "total": 7 },
        ])
        .await
        .unwrap();
    let (_store, svc, queries) = services().await;
    let id = svc
        .create(
            ConnectionProfile::new(
                "mongo",
                "mongodb.community",
                ConnectionSettings::MongoDb(MongoSettings {
                    uri: None,
                    hosts: vec![HostPort {
                        host: rs.host.clone(),
                        port: rs.port,
                    }],
                    replica_set: None,
                    auth_source: None,
                    username: None,
                    password: SecretRef::None,
                    default_database: Some("shop".into()),
                    tls: false,
                    read_preference: ReadPreference::Primary,
                    direct_connection: true,
                    server_selection_timeout_ms: 10_000,
                    connect_timeout_ms: 10_000,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id;
    let in_orders = |language, text: &str| {
        let mut i = q(language, text);
        i.database = Some("shop".into());
        i.collection = Some("orders".into());
        i
    };

    let ctx = queries.context(id).await.unwrap();
    assert!(ctx.languages.contains(&QueryLanguage::MongoFilter));
    assert!(ctx.languages.contains(&QueryLanguage::MongoAggregation));

    let r = queries
        .run(
            id,
            "a",
            in_orders(QueryLanguage::MongoFilter, r#"{"status": "open"}"#),
        )
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);

    let r = queries
        .run(
            id,
            "b",
            in_orders(
                QueryLanguage::MongoAggregation,
                r#"[{"$group": {"_id": "$status", "sum": {"$sum": "$total"}}}, {"$sort": {"_id": 1}}]"#,
            ),
        )
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(*r.rows[1].get("sum"), Value::Int(15));

    // $out needs confirmation, then writes the collection.
    let out = in_orders(
        QueryLanguage::MongoAggregation,
        r#"[{"$match": {"status": "closed"}}, {"$out": "closed_orders"}]"#,
    );
    assert_eq!(
        queries.run(id, "c", out.clone()).await.unwrap_err().code(),
        "confirmation_required"
    );
    let mut confirmed = out;
    confirmed.confirmed = true;
    queries.run(id, "d", confirmed).await.unwrap();
    let copied = raw
        .database("shop")
        .collection::<mongodb::bson::Document>("closed_orders")
        .count_documents(doc! {})
        .await
        .unwrap();
    assert_eq!(copied, 1);

    let plan = queries
        .explain(
            id,
            "e",
            in_orders(QueryLanguage::MongoFilter, r#"{"total": {"$gt": 6}}"#),
            false,
        )
        .await
        .unwrap();
    assert!(!plan.text.is_empty());

    let err = queries
        .run(id, "f", in_orders(QueryLanguage::MongoFilter, "{not json"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), "invalid_input", "{err}");
}
