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
        backup_before_replace: true,
        sync: None,
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

/// Array items whose own `_id`s repeat or are missing (as in data written
/// by Mongoose), and a unique field that many documents lack, survive
/// MongoDB → PostgreSQL (normalized) → MongoDB with child tables copied as
/// their own collections. Found on the owner's data: rows were lost and the
/// unique index failed.
#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn array_items_with_repeated_or_missing_ids_survive_the_way_back() {
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
    let app = mongo.database("app");
    let shared = ObjectId::new();
    let tasks: Vec<Document> = (0..40)
        .map(|i| {
            let mut d = doc! {
                "_id": ObjectId::new(),
                "title": format!("task {i}"),
                "tours": (0..3).map(|k| {
                    let mut t = doc! { "title": format!("tour {i}.{k}") };
                    match (i, k) {
                        // The same item copied into two tasks.
                        (0 | 1, 0) => { t.insert("_id", shared); }
                        // Items without an id.
                        (_, 2) => {}
                        _ => { t.insert("_id", ObjectId::new()); }
                    }
                    Bson::Document(t)
                }).collect::<Vec<_>>(),
            };
            if i % 4 == 0 {
                d.insert("serial", format!("S-{i}"));
            }
            d
        })
        .collect();
    app.collection::<Document>("tasks")
        .insert_many(tasks)
        .await
        .unwrap();
    app.run_command(doc! { "createIndexes": "tasks", "indexes": [
        { "key": { "serial": 1 }, "name": "serial_1", "unique": true, "sparse": true },
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

    let mut to_pg = request(
        mongo_id,
        pg_id,
        vec![CopyItem {
            source: ObjectRef::collection("app", "tasks"),
            target: public("tasks"),
        }],
    );
    to_pg.doc_mode = DocMode::Normalize;
    let v = finish(m, m.start(to_pg, false).await.unwrap()).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");

    // Tables made before shapes were saved come back flat: drop the saved
    // shape to copy them back that way.
    let pgc = e.svc.open(pg_id).await.unwrap();
    sql(&pgc, "DROP TABLE _gazabridge_shapes").await;

    // Back, each table to its own collection, as the owner did.
    let back = request(
        pg_id,
        mongo_id,
        vec![
            CopyItem {
                source: public("tasks"),
                target: ObjectRef::collection("back", "tasks"),
            },
            CopyItem {
                source: public("tasks_tours"),
                target: ObjectRef::collection("back", "tasks_tours"),
            },
        ],
    );
    let plan = m.plan(&back).await.unwrap();
    assert!(
        plan.items[1]
            .warnings
            .iter()
            .any(|w| w.contains("_id_column")),
        "{:?}",
        plan.items[1].warnings
    );
    let run = m.start(back, false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert!(
        !v.events.iter().any(|ev| ev.level != "info"),
        "{:#?}",
        v.events
    );
    assert_eq!(v.streams[0].stats.inserted, 40);
    assert_eq!(v.streams[1].stats.inserted, 120, "no item is lost");
    m.validate(run, Level::Deep).await.unwrap();
    let report = validated(m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");

    let tours = all(&mongo, "back", "tasks_tours").await;
    assert_eq!(tours.len(), 120);
    let with_shared = tours
        .iter()
        .filter(|t| t.get_object_id("_id_column").ok() == Some(shared))
        .count();
    assert_eq!(with_shared, 2);
    assert_eq!(
        tours
            .iter()
            .filter(|t| matches!(t.get("_id_column"), Some(Bson::Null) | None))
            .count(),
        40
    );
    let key = tours[0].get_document("_id").unwrap();
    assert!(key.get_object_id("_parent_id").is_ok() && key.get("_index").is_some());

    // The unique index is back and still lets documents go without a serial.
    let tasks = mongo.database("back").collection::<Document>("tasks");
    let names: Vec<String> = tasks.list_index_names().await.unwrap();
    assert!(names.iter().any(|n| n.contains("serial")), "{names:?}");
    tasks
        .insert_many([
            doc! { "title": "no serial" },
            doc! { "title": "none either" },
        ])
        .await
        .unwrap();
    let dup = tasks
        .insert_one(doc! { "title": "dup", "serial": "S-0" })
        .await;
    assert!(dup.is_err(), "the index is still unique");
}

/// A document with most of what MongoDB documents hold: 32- and 64-bit
/// integers, nested objects, arrays of objects and of strings, a mixed
/// array, a field of the wrong type and a field only some documents have.
fn shaped(i: i32) -> Document {
    let mut d = doc! {
        "_id": ObjectId::new(),
        "title": format!("عنصر {i} 🚀"),
        "count": i,
        "big": 5_000_000_000_i64 + i64::from(i),
        "small_long": i64::from(i),
        "price": 1.5 * f64::from(i),
        "done": i % 2 == 0,
        "note": Bson::Null,
        "when": DateTime::from_millis(1_767_225_600_000 + i64::from(i) * 1_000),
        "address": { "city": "Gaza", "geo": { "lat": 31.5, "lng": 34.25 } },
        "tags": (0..i % 3).map(|t| Bson::String(format!("t{t}"))).collect::<Vec<_>>(),
        "tours": (0..i % 4).map(|k| {
            let mut t = doc! { "_id": ObjectId::new(), "title": format!("tour {i}.{k}"), "n": k };
            if k == 1 {
                t.insert("rare", "x");
            }
            Bson::Document(t)
        }).collect::<Vec<_>>(),
        "mixed": if i % 2 == 0 { vec![Bson::Int32(1), Bson::String("two".into())] } else { vec![] },
        "scores": (0..i % 3).map(|k| Bson::Int32(k * 10)).collect::<Vec<_>>(),
    };
    if i == 3 {
        d.insert("only_here", doc! { "deep": { "x": 1 } });
    }
    if i == 5 {
        d.insert("count", "five");
    }
    d
}

/// Keys sorted at every level: SQL keeps no key order.
fn sorted(b: &Bson) -> Bson {
    match b {
        Bson::Document(d) => {
            let mut keys: Vec<&String> = d.keys().collect();
            keys.sort();
            Bson::Document(
                keys.into_iter()
                    .map(|k| (k.clone(), sorted(&d[k])))
                    .collect(),
            )
        }
        Bson::Array(a) => Bson::Array(a.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

/// Collections normalized or flattened into tables come back as the same
/// documents, rows added in SQL included, using the mapping saved with the
/// tables.
#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn flattened_and_normalized_tables_come_back_as_the_original_documents() {
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
    let originals: Vec<Document> = (0..12).map(shaped).collect();
    mongo
        .database("app")
        .collection::<Document>("items")
        .insert_many(originals.clone())
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

    for (mode, table) in [(DocMode::Normalize, "items"), (DocMode::Flatten, "flat")] {
        let mut to_pg = request(
            mongo_id,
            pg_id,
            vec![CopyItem {
                source: ObjectRef::collection("app", "items"),
                target: public(table),
            }],
        );
        to_pg.doc_mode = mode;
        let v = finish(m, m.start(to_pg, false).await.unwrap()).await;
        assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
        assert!(
            v.events
                .iter()
                .any(|ev| ev.message.contains("_gazabridge_shapes")),
            "{:#?}",
            v.events
        );
    }

    // Rows written in SQL after the copy.
    let pgc = e.svc.open(pg_id).await.unwrap();
    let new_id = "aaaaaaaaaaaaaaaaaaaaaaaa";
    let tour_id = "bbbbbbbbbbbbbbbbbbbbbbbb";
    sql(&pgc, &format!("INSERT INTO items (_id, title, count, address_city) VALUES ('{new_id}', 'from sql', '7', 'Rafah')")).await;
    sql(&pgc, &format!("INSERT INTO items_tours (_parent_id, _index, _id, title, n) VALUES ('{new_id}', 0, '{tour_id}', 'sql tour', 9)")).await;
    sql(&pgc, &format!("INSERT INTO items_tags (_parent_id, _index, value) VALUES ('{new_id}', 1, 'second'), ('{new_id}', 0, 'first')")).await;
    sql(&pgc, &format!("INSERT INTO flat (_id, title, tours) VALUES ('{new_id}', 'from sql', '[{{\"title\": \"j\", \"n\": 1}}]')")).await;

    let back = request(
        pg_id,
        mongo_id,
        vec![
            CopyItem {
                source: public("items"),
                target: ObjectRef::collection("back", "items"),
            },
            CopyItem {
                source: public("items_tags"),
                target: ObjectRef::collection("back", "items_tags"),
            },
            CopyItem {
                source: public("items_tours"),
                target: ObjectRef::collection("back", "items_tours"),
            },
            // The child tables are read even when not selected.
            CopyItem {
                source: public("flat"),
                target: ObjectRef::collection("back", "flat"),
            },
        ],
    );
    let plan = m.plan(&back).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    for i in [0, 3] {
        assert!(
            plan.items[i]
                .warnings
                .iter()
                .any(|w| w.contains("original shape")),
            "{:?}",
            plan.items[i].warnings
        );
    }
    assert_eq!(plan.items[1].kind, "embedded");
    assert_eq!(plan.items[2].kind, "embedded");
    let run = m.start(back, false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    m.validate(run, Level::Deep).await.unwrap();
    let report = validated(m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");

    let names = mongo
        .database("back")
        .list_collection_names()
        .await
        .unwrap();
    assert!(
        !names.iter().any(|n| n.starts_with("items_")),
        "child tables are arrays again: {names:?}"
    );
    let new_oid = ObjectId::parse_str(new_id).unwrap();
    for coll in ["items", "flat"] {
        let docs = all(&mongo, "back", coll).await;
        assert_eq!(docs.len(), originals.len() + 1, "{coll}");
        for o in &originals {
            let id = o.get_object_id("_id").unwrap();
            let got = docs
                .iter()
                .find(|d| d.get_object_id("_id").ok() == Some(id))
                .unwrap_or_else(|| panic!("{coll}: {id} is missing"));
            assert_eq!(
                sorted(&Bson::Document(got.clone())),
                sorted(&Bson::Document(o.clone())),
                "{coll}: {id} changed\noriginal: {o}\nback:     {got}"
            );
        }
        let added = docs
            .iter()
            .find(|d| d.get_object_id("_id").ok() == Some(new_oid))
            .unwrap();
        assert_eq!(added.get_str("title").unwrap(), "from sql", "{added}");
        assert!(!added.contains_key("_parent_id") && !added.contains_key("_index"));
        let tours = added.get_array("tours").unwrap();
        if coll == "items" {
            assert_eq!(added.get_i32("count").unwrap(), 7, "{added}");
            assert_eq!(
                added
                    .get_document("address")
                    .unwrap()
                    .get_str("city")
                    .unwrap(),
                "Rafah"
            );
            assert_eq!(
                tours,
                &vec![Bson::Document(doc! {
                    "_id": ObjectId::parse_str(tour_id).unwrap(),
                    "title": "sql tour",
                    "n": 9,
                })],
                "{added}"
            );
            assert_eq!(
                added.get_array("tags").unwrap(),
                &vec![Bson::String("first".into()), Bson::String("second".into())],
                "in `_index` order"
            );
        } else {
            assert_eq!(
                tours,
                &vec![Bson::Document(doc! { "title": "j", "n": 1 })],
                "{added}"
            );
        }
    }
}
