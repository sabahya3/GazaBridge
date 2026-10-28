//! Export and import against real MongoDB and PostgreSQL servers.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;

use gazabridge_application::{
    ConnectionService, ExplorerService, ExportRequest, ImportRequest, PasswordInput,
    TransferService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::Value;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
};
use gazabridge_domain::job::WriteMode;
use gazabridge_files::{
    Compression, FileFormat, JsonFlavor, MalformedPolicy, ReadOptions, WriteOptions,
};
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::filter::{FieldPath, Filter};
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{Bson, Decimal128, Document, Regex, doc, oid::ObjectId};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

async fn services() -> (MetadataStore, ConnectionService, TransferService) {
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
    let transfers = TransferService::new(store.clone(), explorer, svc.redactor().clone());
    (store, svc, transfers)
}

fn read_opts(
    format: FileFormat,
    json_flavor: JsonFlavor,
    malformed: MalformedPolicy,
) -> ReadOptions {
    ReadOptions {
        format,
        json_flavor,
        compression: Compression::None,
        malformed,
    }
}

fn import(object: &ObjectRef, path: std::path::PathBuf, options: ReadOptions) -> ImportRequest {
    ImportRequest {
        object: object.clone(),
        path,
        options,
        mode: WriteMode::Insert,
        key_fields: vec![],
        batch_size: 2,
        stop_on_error: false,
    }
}

/// Documents without `_id`, sorted by `n`, for comparing copies.
async fn contents(coll: &mongodb::Collection<Document>) -> Vec<Document> {
    use futures::TryStreamExt;
    coll.find(doc! {})
        .sort(doc! { "n": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_collections_round_trip_through_every_file_format() {
    let rs = common::mongo::start("8.0").await;
    let raw = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let db = raw.database("shop");
    let source = db.collection::<Document>("orders");
    let docs: Vec<Document> = (1..=5)
        .map(|n| {
            doc! {
                "_id": ObjectId::new(), "n": Bson::Int32(n), "big": Bson::Int64(i64::from(n)),
                "price": Decimal128::from_bytes([0x31, 0x30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x3c, 0x30]),
                "at": mongodb::bson::DateTime::from_millis(1_700_000_000_000 + i64::from(n)),
                "re": Regex { pattern: "^a".into(), options: "i".into() },
                "nested": { "k": Bson::Int32(n), "list": [1, "two"] },
                "status": if n % 2 == 0 { "open" } else { "closed" },
            }
        })
        .collect();
    source.insert_many(docs.clone()).await.unwrap();

    let (store, svc, transfers) = services().await;
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
    let dir = tempfile::tempdir().unwrap();
    let orders = ObjectRef::collection("shop", "orders");

    // BSON and canonical Extended JSON copies are exact, gzip or not.
    for (i, (format, flavor, compression)) in [
        (FileFormat::Bson, JsonFlavor::Typed, Compression::None),
        (
            FileFormat::Jsonl,
            JsonFlavor::MongoCanonical,
            Compression::Gzip,
        ),
        (
            FileFormat::Json,
            JsonFlavor::MongoCanonical,
            Compression::None,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let path = dir.path().join(format!("orders{i}.{}", format.extension()));
        let mut options = WriteOptions::new(format);
        options.json_flavor = flavor;
        options.compression = compression;
        let out = transfers
            .export(
                id,
                ExportRequest {
                    object: orders.clone(),
                    filter: None,
                    projection: vec![],
                    exclude: vec![],
                    order_by: vec![],
                    skip: 0,
                    limit: None,
                    path: path.clone(),
                    options,
                },
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(out.file.as_ref().unwrap().records, 5);

        let target = ObjectRef::collection("shop", format!("copy{i}"));
        let mut opts = read_opts(format, flavor, MalformedPolicy::Fail);
        opts.compression = compression;
        let r = transfers
            .import(
                id,
                import(&target, path, opts),
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(r.inserted, 5, "{format:?}");
        assert_eq!(
            contents(&db.collection(&format!("copy{i}"))).await,
            contents(&source).await,
            "{format:?}"
        );
    }

    // Filtered export to CSV with sampled columns.
    let csv = dir.path().join("open.csv");
    let mut options = WriteOptions::new(FileFormat::Csv);
    options.csv.null_text = "NULL".into();
    transfers
        .export(
            id,
            ExportRequest {
                object: orders.clone(),
                filter: Some(Filter::eq("status", Value::String("open".into()))),
                projection: vec![],
                exclude: vec![],
                order_by: vec![],
                skip: 0,
                limit: None,
                path: csv.clone(),
                options,
            },
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    let text = std::fs::read_to_string(&csv).unwrap();
    assert_eq!(text.lines().count(), 3, "{text}");
    assert!(text.lines().next().unwrap().starts_with("_id,"), "{text}");

    // Malformed lines are quarantined with line numbers; good ones land.
    let bad = dir.path().join("mixed.jsonl");
    std::fs::write(&bad, "{\"n\": 100}\n{oops\n{\"n\": 101}\n").unwrap();
    let q = dir.path().join("rejected.jsonl");
    let target = ObjectRef::collection("shop", "mixed");
    let r = transfers
        .import(
            id,
            import(
                &target,
                bad,
                read_opts(
                    FileFormat::Jsonl,
                    JsonFlavor::MongoRelaxed,
                    MalformedPolicy::Quarantine { path: q.clone() },
                ),
            ),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!((r.inserted, r.malformed), (2, 1));
    assert!(r.messages[0].starts_with("line 2"), "{:?}", r.messages);
    assert_eq!(std::fs::read_to_string(q).unwrap(), "{oops\n");

    // Duplicate _id: rejected records are reported, the rest written.
    let path = dir.path().join("orders0.bson");
    let r = transfers
        .import(
            id,
            import(
                &ObjectRef::collection("shop", "copy0"),
                path.clone(),
                read_opts(FileFormat::Bson, JsonFlavor::Typed, MalformedPolicy::Fail),
            ),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!((r.inserted, r.failed), (0, 5));

    // Upsert by _id updates instead.
    let mut up = import(
        &ObjectRef::collection("shop", "copy0"),
        path,
        read_opts(FileFormat::Bson, JsonFlavor::Typed, MalformedPolicy::Fail),
    );
    up.mode = WriteMode::Upsert;
    up.key_fields = vec!["_id".into()];
    let r = transfers
        .import(id, up, CancellationToken::new(), |_| {})
        .await
        .unwrap();
    assert_eq!(r.failed, 0);
    assert_eq!(r.inserted + r.updated + r.skipped, 5, "{r:?}");

    // A cancelled export leaves no file.
    let cancelled = dir.path().join("cancelled.jsonl");
    let token = CancellationToken::new();
    token.cancel();
    let err = transfers
        .export(
            id,
            ExportRequest {
                object: orders,
                filter: None,
                projection: vec![],
                exclude: vec![],
                order_by: vec![],
                skip: 0,
                limit: None,
                path: cancelled.clone(),
                options: WriteOptions::new(FileFormat::Jsonl),
            },
            token,
            |_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "cancelled");
    assert!(!cancelled.exists());

    let audit = store.audit().list(Some("object"), None, 50).await.unwrap();
    assert!(audit.iter().filter(|e| e.action == "data.export").count() >= 4);
    assert!(audit.iter().any(|e| e.action == "data.import"));
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn postgres_tables_export_and_import() {
    let c = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("container");
    let host = c.get_host().await.unwrap().to_string();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let (_store, svc, transfers) = services().await;
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
                    pool_size: 2,
                }),
            ),
            // The image's documented default password.
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap()
        .id;
    let conn = svc.open(id).await.unwrap();
    for sql in [
        "CREATE TABLE src (id int PRIMARY KEY, name text, price numeric(10,2), doc jsonb, at timestamptz, tags text[])",
        "CREATE TABLE dst (LIKE src INCLUDING ALL)",
        "INSERT INTO src SELECT g, 'n' || g, g * 1.25, jsonb_build_object('g', g), '2026-10-25T00:00:00Z'::timestamptz + g * interval '1 hour', ARRAY['a', 'b'] FROM generate_series(1, 7) g",
        "UPDATE src SET name = NULL WHERE id = 3",
    ] {
        conn.execute(
            QueryRequest {
                language: QueryLanguage::PostgresSql,
                text: sql.into(),
                params: vec![],
                max_rows: 10,
                timeout: None,
                database: None,
                collection: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    }
    let public = NamespacePath::schema(Some("postgres".into()), "public");
    let src = ObjectRef::table(public.clone(), "src");
    let dst = ObjectRef::table(public, "dst");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("src.jsonl");

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
                options: WriteOptions::new(FileFormat::Jsonl),
            },
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(out.records_read, 7);
    let r = transfers
        .import(
            id,
            import(
                &dst,
                path.clone(),
                read_opts(FileFormat::Jsonl, JsonFlavor::Typed, MalformedPolicy::Fail),
            ),
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!((r.inserted, r.failed), (7, 0), "{r:?}");

    let same = conn
        .execute(
            QueryRequest {
                language: QueryLanguage::PostgresSql,
                text: "SELECT count(*) AS n FROM (SELECT * FROM src EXCEPT SELECT * FROM dst) d"
                    .into(),
                params: vec![],
                max_rows: 1,
                timeout: None,
                database: None,
                collection: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        *same.rows[0].get("n"),
        Value::Int(0),
        "every row, NULLs included, arrived unchanged"
    );

    // Stop at the first rejection (primary key).
    let mut stop = import(
        &dst,
        path.clone(),
        read_opts(FileFormat::Jsonl, JsonFlavor::Typed, MalformedPolicy::Fail),
    );
    stop.stop_on_error = true;
    let err = transfers
        .import(id, stop, CancellationToken::new(), |_| {})
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("stopped at the first rejected record"),
        "{err}"
    );

    // Upsert by id.
    let mut up = import(
        &dst,
        path,
        read_opts(FileFormat::Jsonl, JsonFlavor::Typed, MalformedPolicy::Fail),
    );
    up.mode = WriteMode::Upsert;
    up.key_fields = vec!["id".into()];
    let r = transfers
        .import(id, up, CancellationToken::new(), |_| {})
        .await
        .unwrap();
    assert_eq!(r.failed, 0, "{r:?}");

    // CSV with a filter and table columns.
    let csv = dir.path().join("cheap.csv");
    let mut options = WriteOptions::new(FileFormat::Csv);
    options.csv.null_text = "NULL".into();
    transfers
        .export(
            id,
            ExportRequest {
                object: src,
                projection: vec![],
                exclude: vec![],
                order_by: vec![],
                skip: 0,
                limit: None,
                filter: Some(Filter::Le {
                    field: FieldPath::new("price"),
                    value: Value::Int(5),
                }),
                path: csv.clone(),
                options,
            },
            CancellationToken::new(),
            |_| {},
        )
        .await
        .unwrap();
    let text = std::fs::read_to_string(&csv).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "id,name,price,doc,at,tags");
    assert_eq!(lines.len(), 5, "{text}");
    assert!(lines[3].starts_with("3,NULL,3.75,"), "{text}");
    conn.close().await;
}
