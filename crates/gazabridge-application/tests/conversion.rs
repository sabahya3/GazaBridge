//! MongoDB ⇄ PostgreSQL conversions against real servers: normalized
//! tables, JSONB round trips, embedded child rows and identity mapping.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, RunView,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
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
use mongodb::bson::{Bson, DateTime, Decimal128, Document, doc, oid::ObjectId};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

async fn sql(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    conn.execute(
        QueryRequest {
            language: QueryLanguage::PostgresSql,
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

async fn scalar(conn: &Arc<dyn Connection>, text: &str) -> Value {
    sql(conn, text).await[0].fields[0].value.clone()
}

async fn all(c: &mongodb::Client, db: &str, coll: &str) -> Vec<Document> {
    use futures::TryStreamExt;
    c.database(db)
        .collection::<Document>(coll)
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn customer(i: i32) -> Document {
    let mut d = doc! {
        "_id": ObjectId::new(),
        "name": format!("عميل {i}"),
        "email": format!("c{i}@example.test"),
        "age": 20 + i % 50,
        "address": { "city": if i % 2 == 0 { "Gaza" } else { "Rafah" }, "zip": format!("{:05}", i) },
        "tags": (0..i % 3).map(|t| Bson::String(format!("t{t}"))).collect::<Vec<_>>(),
        "orders": (0..i % 4).map(|k| Bson::Document(doc! {
            "sku": format!("S{k}"),
            "qty": k + 1,
            "price": Decimal128::from_bytes(gazabridge_bson_decimal(&format!("{}.50", k + 1))),
        })).collect::<Vec<_>>(),
        "joined": DateTime::from_millis(1_767_225_600_000 + i64::from(i) * 86_400_000),
    };
    if i == 7 {
        // Outside what the columns hold: kept in `_extra`, not lost.
        d.insert("age", "unknown");
    }
    d
}

fn gazabridge_bson_decimal(text: &str) -> [u8; 16] {
    text.parse::<Decimal128>().unwrap().bytes()
}

struct Env {
    svc: ConnectionService,
    migrations: MigrationService,
    _quarantine: tempfile::TempDir,
}

async fn env() -> Env {
    let store = MetadataStore::open_in_memory().await.unwrap();
    let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
    registry
        .register(Arc::new(
            gazabridge_connector_mongodb::MongoConnectorFactory,
        ))
        .unwrap();
    registry
        .register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))
        .unwrap();
    let svc = ConnectionService::new(
        store.clone(),
        registry,
        SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
    );
    let quarantine = tempfile::tempdir().unwrap();
    let migrations = MigrationService::new(
        store.clone(),
        ExplorerService::new(store, svc.clone()),
        svc.redactor().clone(),
        quarantine.path().to_path_buf(),
    );
    Env {
        svc,
        migrations,
        _quarantine: quarantine,
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
        batch_size: 64,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 2,
        doc_mode: DocMode::Flatten,
        table_mode: TableMode::Reference,
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

async fn validated(m: &MigrationService, run: RunId) -> gazabridge_application::ValidationReport {
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

fn public(name: &str) -> ObjectRef {
    ObjectRef::table(
        NamespacePath::schema(Some("postgres".into()), "public"),
        name,
    )
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_and_postgresql_convert_both_ways() {
    let (rs, pg) = tokio::join!(common::mongo::start("8.0"), async {
        Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .expect("postgres")
    });
    let mongo = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let shop = mongo.database("shop");
    let customers: Vec<Document> = (0..200).map(customer).collect();
    shop.collection::<Document>("customers")
        .insert_many(customers)
        .await
        .unwrap();
    shop.run_command(doc! { "createIndexes": "customers", "indexes": [
        { "key": { "email": 1 }, "name": "email_unique", "unique": true },
        { "key": { "address.city": 1, "joined": -1 }, "name": "by_city" },
    ]})
    .await
    .unwrap();

    let e = env().await;
    let m = &e.migrations;
    let mongo_id = e
        .svc
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
                    default_database: None,
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
    let pg_id = e
        .svc
        .create(
            ConnectionProfile::new(
                "pg",
                "postgres.postgresql",
                ConnectionSettings::Postgres(PostgresSettings {
                    host: pg.get_host().await.unwrap().to_string(),
                    port: pg.get_host_port_ipv4(5432).await.unwrap(),
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
    let pgc = e.svc.open(pg_id).await.unwrap();

    // ---- MongoDB → PostgreSQL, normalized -------------------------------
    let mut req = request(
        mongo_id,
        pg_id,
        vec![CopyItem {
            source: ObjectRef::collection("shop", "customers"),
            target: public("customers"),
        }],
    );
    req.doc_mode = DocMode::Normalize;
    let plan = m.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let item = &plan.items[0];
    let tables: Vec<&str> = item.tables.iter().map(|t| t.table.name.as_str()).collect();
    assert_eq!(tables, ["customers", "customers_tags", "customers_orders"]);
    assert_eq!(
        item.tables[0].columns,
        [
            "_id",
            "name",
            "email",
            "age",
            "address_city",
            "address_zip",
            "joined",
            "_extra"
        ]
    );
    assert_eq!(item.tables[0].types[0], "character(24)");
    assert_eq!(item.tables[0].types[3], "jsonb");
    assert!(
        plan.warnings
            .iter()
            .chain(&item.warnings)
            .any(|w| w.contains("`age`")),
        "{:?}",
        item.warnings
    );
    assert!(!item.tables[0].rows.is_empty() && !item.documents.is_empty());
    assert!(
        item.statements.iter().any(|s| s.contains("FOREIGN KEY")),
        "{:?}",
        item.statements
    );
    assert_eq!(
        item.indexes,
        ["customers_email_unique", "customers_by_city"]
    );
    assert_eq!(
        sql(
            &pgc,
            "SELECT 1 FROM pg_tables WHERE tablename = 'customers'"
        )
        .await
        .len(),
        0,
        "planning wrote nothing"
    );

    let run = m.start(req.clone(), false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.read, 200);
    assert_eq!(
        scalar(&pgc, "SELECT count(*) FROM customers").await,
        Value::Int(200)
    );
    let orders: i64 = (0..200).map(|i| i64::from(i % 4)).sum();
    let tags: i64 = (0..200).map(|i| i64::from(i % 3)).sum();
    assert_eq!(
        scalar(&pgc, "SELECT count(*) FROM customers_orders").await,
        Value::Int(orders)
    );
    assert_eq!(
        scalar(&pgc, "SELECT count(*) FROM customers_tags").await,
        Value::Int(tags)
    );
    // Types survive: decimals, dates, Arabic text, ObjectId as character(24).
    let r = &sql(&pgc, "SELECT o.price::text, c.joined::text, c.name, length(c._id) FROM customers c JOIN customers_orders o ON o._parent_id = c._id WHERE c.email = 'c5@example.test' AND o._index = 0").await[0];
    assert_eq!(r.fields[0].value, Value::String("1.50".into()));
    assert!(
        matches!(&r.fields[1].value, Value::String(s) if s.starts_with("2026-01-06")),
        "{r:?}"
    );
    assert_eq!(r.fields[2].value, Value::String("عميل 5".into()));
    assert_eq!(r.fields[3].value, Value::Int(24));
    // `age` holds numbers and text in the sample: one JSONB column keeps both.
    let r = &sql(
        &pgc,
        "SELECT age, (SELECT age FROM customers WHERE email = 'c8@example.test') AS other FROM customers WHERE email = 'c7@example.test'",
    )
    .await[0];
    assert_eq!(
        (r.fields[0].value.clone(), r.fields[1].value.clone()),
        (
            Value::Json(serde_json::json!("unknown")),
            Value::Json(serde_json::json!(28))
        )
    );
    assert_eq!(scalar(&pgc, "SELECT count(*) FROM pg_indexes WHERE indexname IN ('customers_email_unique', 'customers_by_city')").await, Value::Int(2));

    // Deep validation across engines: every row of every table, by key.
    let first = run;
    m.validate(first, Level::Deep).await.unwrap();
    let deep = validated(m, first).await;
    let s0 = &deep.streams[0];
    assert!(s0.passed, "{s0:#?}");
    assert_eq!(s0.targets.len(), 3);
    assert_eq!(s0.targets[0].matched, 200);
    assert_eq!(s0.targets[2].matched, orders as u64);
    // A child row changed in PostgreSQL is found.
    sql(&pgc, "UPDATE customers_orders SET qty = 99 WHERE _index = 0 AND _parent_id = (SELECT _id FROM customers WHERE email = 'c5@example.test')").await;
    sql(&pgc, "INSERT INTO customers_tags (_parent_id, _index, value) SELECT _id, 50, 'stale' FROM customers WHERE email = 'c5@example.test'").await;
    m.validate(first, Level::Deep).await.unwrap();
    let deep = validated(m, first).await;
    let s0 = &deep.streams[0];
    assert!(!s0.passed);
    assert_eq!(
        (s0.targets[2].different, s0.targets[1].extra),
        (1, 1),
        "{s0:#?}"
    );
    assert!(
        s0.differences
            .iter()
            .any(|d| d.detail.as_deref().is_some_and(|x| x.contains("`qty`"))),
        "{:?}",
        s0.differences
    );
    sql(&pgc, "DELETE FROM customers_tags WHERE _index = 50").await;

    // Running again keeps everything as is (skip existing, by key).
    let run = m.start(req.clone(), false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 0);
    assert_eq!(
        scalar(&pgc, "SELECT count(*) FROM customers_orders").await,
        Value::Int(orders)
    );

    // An existing table without the planned columns blocks the copy.
    sql(&pgc, "CREATE TABLE other (_id text PRIMARY KEY)").await;
    let mut clash = req.clone();
    clash.items[0].target = public("other");
    clash.doc_mode = DocMode::Flatten;
    let plan = m.plan(&clash).await.unwrap();
    assert!(
        plan.blockers
            .iter()
            .any(|b| b.contains("without the column")),
        "{:?}",
        plan.blockers
    );

    // ---- MongoDB → PostgreSQL (JSONB, new schema) → MongoDB -------------
    let mut to_jsonb = request(
        mongo_id,
        pg_id,
        vec![CopyItem {
            source: ObjectRef::collection("shop", "customers"),
            target: ObjectRef::table(
                NamespacePath::schema(Some("postgres".into()), "raw"),
                "customers",
            ),
        }],
    );
    to_jsonb.doc_mode = DocMode::Jsonb;
    // A dry run into a new schema creates nothing.
    let tables_before = sql(&pgc, "SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')").await;
    let dry = finish(m, m.dry_run(to_jsonb.clone()).await.unwrap()).await;
    assert_eq!(dry.status, JobStatus::Completed, "{dry:#?}");
    assert_eq!(dry.streams[0].stats.inserted, 200);
    assert_eq!(sql(&pgc, "SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')").await, tables_before);
    assert!(
        sql(&pgc, "SELECT 1 FROM pg_namespace WHERE nspname = 'raw'")
            .await
            .is_empty(),
        "no schema created"
    );
    let v = finish(m, m.start(to_jsonb, false).await.unwrap()).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(scalar(&pgc, "SELECT doc->'address'->>'city' FROM raw.customers WHERE doc->>'email' = 'c2@example.test'").await, Value::String("Gaza".into()));
    let back = request(
        pg_id,
        mongo_id,
        vec![CopyItem {
            source: ObjectRef::table(
                NamespacePath::schema(Some("postgres".into()), "raw"),
                "customers",
            ),
            target: ObjectRef::collection("back", "customers"),
        }],
    );
    let plan = m.plan(&back).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.items[0]
            .warnings
            .iter()
            .any(|w| w.contains("restored as stored"))
    );
    let back_run = m.start(back, false).await.unwrap();
    let v = finish(m, back_run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    // Same documents and types (ObjectId, Int32, Decimal128, dates); JSONB
    // does not keep key order, so documents compare field by field.
    assert_eq!(
        all(&mongo, "shop", "customers").await,
        all(&mongo, "back", "customers").await
    );

    // ---- PostgreSQL → MongoDB, embedding child rows ----------------------
    for s in [
        "CREATE TABLE buyers (id integer PRIMARY KEY, name text NOT NULL, visits integer, joined timestamptz)",
        "CREATE UNIQUE INDEX buyers_name ON buyers (name)",
        "CREATE TABLE purchases (id bigint PRIMARY KEY, buyer_id integer NOT NULL REFERENCES buyers (id), qty integer, price numeric(10,2))",
        "CREATE TABLE notes (body text)",
        "INSERT INTO buyers SELECT g, 'b' || g, g % 5, '2026-01-01T00:00:00Z'::timestamptz + g * interval '1 day' FROM generate_series(1, 150) g",
        "INSERT INTO purchases SELECT g, 1 + g % 150, g % 7, g * 0.25 FROM generate_series(1, 400) g",
    ] {
        sql(&pgc, s).await;
    }
    let mut embed = request(
        pg_id,
        mongo_id,
        vec![
            CopyItem {
                source: public("buyers"),
                target: ObjectRef::collection("fromsql", "buyers"),
            },
            CopyItem {
                source: public("purchases"),
                target: ObjectRef::collection("fromsql", "purchases"),
            },
        ],
    );
    embed.table_mode = TableMode::Embed;
    let plan = m.plan(&embed).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert_eq!(plan.items[1].kind, "embedded");
    assert!(
        plan.items[0].documents[0].contains("purchases"),
        "{}",
        plan.items[0].documents[0]
    );
    let embed_run = m.start(embed, false).await.unwrap();
    let v = finish(m, embed_run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    let buyers = all(&mongo, "fromsql", "buyers").await;
    assert_eq!(buyers.len(), 150);
    assert!(
        all(&mongo, "fromsql", "purchases").await.is_empty(),
        "embedded, not copied on its own"
    );
    let b1 = buyers.iter().find(|d| d.get_i32("_id") == Ok(1)).unwrap();
    assert!(matches!(b1.get("visits"), Some(Bson::Int32(1))));
    assert!(b1.get_datetime("joined").is_ok());
    let items = b1.get_array("purchases").unwrap();
    let embedded: usize = buyers
        .iter()
        .map(|d| d.get_array("purchases").unwrap().len())
        .sum();
    assert_eq!(embedded, 400);
    let first = items[0].as_document().unwrap();
    assert!(
        first.get("buyer_id").is_none(),
        "the reference is implied by nesting"
    );
    assert!(matches!(first.get("price"), Some(Bson::Decimal128(_))));
    assert!(matches!(first.get("qty"), Some(Bson::Int32(_))));
    let ids: Vec<i64> = items
        .iter()
        .map(|i| i.as_document().unwrap().get_i64("id").unwrap())
        .collect();
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "children keep their key order"
    );
    let idx: Vec<String> = mongo
        .database("fromsql")
        .collection::<Document>("buyers")
        .list_index_names()
        .await
        .unwrap();
    assert!(idx.contains(&"buyers_name".to_owned()), "{idx:?}");

    // Rows → documents are validated too, embedded children included.
    for run in [back_run, embed_run] {
        m.validate(run, Level::Deep).await.unwrap();
        let deep = validated(m, run).await;
        assert_eq!(deep.passed, Some(true), "{deep:#?}");
    }

    // Tables without a primary key are refused (no safe resume).
    let plan = m
        .plan(&request(
            pg_id,
            mongo_id,
            vec![CopyItem {
                source: public("notes"),
                target: ObjectRef::collection("fromsql", "notes"),
            }],
        ))
        .await
        .unwrap();
    assert!(
        plan.blockers.iter().any(|b| b.contains("no primary key")),
        "{:?}",
        plan.blockers
    );
}
