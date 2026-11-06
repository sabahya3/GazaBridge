//! MongoDB ⇄ MySQL conversions against real servers: normalized tables,
//! JSON round trips, embedded child rows and identity mapping.

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
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, MySqlSettings, MySqlTlsMode,
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
        .register(Arc::new(gazabridge_connector_mysql::MySqlConnectorFactory))
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

fn shop(name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::database("shop"), name)
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_and_mysql_convert_both_ways() {
    let (rs, my) = tokio::join!(common::mongo::start("8.0"), async {
        Mysql::default()
            .with_tag("8.4")
            .start()
            .await
            .expect("mysql")
    });
    let mongo = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let source = mongo.database("shop");
    let customers: Vec<Document> = (0..200).map(customer).collect();
    source
        .collection::<Document>("customers")
        .insert_many(customers)
        .await
        .unwrap();
    source
        .run_command(doc! { "createIndexes": "customers", "indexes": [
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
    let my_id = e
        .svc
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
                    pool_size: 4,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id;
    let myc = e.svc.open(my_id).await.unwrap();

    // ---- MongoDB → MySQL, normalized, into a new database ----------------
    let mut req = request(
        mongo_id,
        my_id,
        vec![CopyItem {
            source: ObjectRef::collection("shop", "customers"),
            target: shop("customers"),
        }],
    );
    req.doc_mode = DocMode::Normalize;
    let plan = m.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let item = &plan.items[0];
    let tables: Vec<&str> = item.tables.iter().map(|t| t.table.name.as_str()).collect();
    assert_eq!(tables, ["customers", "customers_tags", "customers_orders"]);
    assert_eq!(item.tables[0].types[0], "char(24)");
    assert!(
        item.statements[0].starts_with("CREATE DATABASE IF NOT EXISTS `shop`"),
        "{:?}",
        item.statements
    );
    assert!(
        item.warnings.iter().any(|w| w.contains("varchar(")),
        "{:?}",
        item.warnings
    );
    assert_eq!(
        sql(
            &myc,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'shop'"
        )
        .await
        .len(),
        0,
        "planning wrote nothing"
    );
    let dry = finish(m, m.dry_run(req.clone()).await.unwrap()).await;
    assert_eq!(dry.status, JobStatus::Completed, "{dry:#?}");
    assert_eq!(
        sql(
            &myc,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'shop'"
        )
        .await
        .len(),
        0,
        "a dry run creates nothing"
    );

    let run = m.start(req.clone(), false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.read, 200);
    let orders: i64 = (0..200).map(|i| i64::from(i % 4)).sum();
    let tags: i64 = (0..200).map(|i| i64::from(i % 3)).sum();
    assert_eq!(
        scalar(&myc, "SELECT COUNT(*) FROM shop.customers").await,
        Value::Int(200)
    );
    assert_eq!(
        scalar(&myc, "SELECT COUNT(*) FROM shop.customers_orders").await,
        Value::Int(orders)
    );
    assert_eq!(
        scalar(&myc, "SELECT COUNT(*) FROM shop.customers_tags").await,
        Value::Int(tags)
    );
    let r = &sql(&myc, "SELECT o.price = 1.5, CAST(c.joined AS CHAR), c.name, LENGTH(c._id) FROM shop.customers c JOIN shop.customers_orders o ON o._parent_id = c._id WHERE c.email = 'c5@example.test' AND o._index = 0").await[0];
    // Unconstrained decimals are decimal(65,30) in MySQL: equal in value.
    assert_eq!(r.fields[0].value, Value::Int(1));
    assert!(
        matches!(&r.fields[1].value, Value::String(s) if s.starts_with("2026-01-06")),
        "{r:?}"
    );
    assert_eq!(r.fields[2].value, Value::String("عميل 5".into()));
    assert_eq!(r.fields[3].value, Value::Int(24));
    assert_eq!(
        scalar(&myc, "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'shop' AND INDEX_NAME IN ('customers_email_unique', 'customers_by_city')").await,
        Value::Int(2)
    );

    m.validate(run, Level::Deep).await.unwrap();
    let deep = validated(m, run).await;
    let s0 = &deep.streams[0];
    assert!(s0.passed, "{s0:#?}");
    assert_eq!(s0.targets[0].matched, 200);
    assert_eq!(s0.targets[2].matched, orders as u64);
    sql(&myc, "UPDATE shop.customers_orders SET qty = 99 WHERE _index = 0 AND _parent_id = (SELECT _id FROM shop.customers WHERE email = 'c5@example.test')").await;
    m.validate(run, Level::Deep).await.unwrap();
    let deep = validated(m, run).await;
    assert!(!deep.streams[0].passed);
    assert_eq!(
        deep.streams[0].targets[2].different, 1,
        "{:#?}",
        deep.streams[0]
    );

    // Running again keeps everything as is (skip existing, by key).
    let v = finish(m, m.start(req.clone(), false).await.unwrap()).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.inserted, 0);

    // ---- MongoDB → MySQL (JSON) → MongoDB: identical documents -------------
    let mut to_json = request(
        mongo_id,
        my_id,
        vec![CopyItem {
            source: ObjectRef::collection("shop", "customers"),
            target: ObjectRef::table(NamespacePath::database("raw"), "customers"),
        }],
    );
    to_json.doc_mode = DocMode::Jsonb;
    let v = finish(m, m.start(to_json, false).await.unwrap()).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    let back = request(
        my_id,
        mongo_id,
        vec![CopyItem {
            source: ObjectRef::table(NamespacePath::database("raw"), "customers"),
            target: ObjectRef::collection("back", "customers"),
        }],
    );
    let plan = m.plan(&back).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let back_run = m.start(back, false).await.unwrap();
    let v = finish(m, back_run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(
        all(&mongo, "shop", "customers").await,
        all(&mongo, "back", "customers").await
    );

    // ---- MySQL → MongoDB, embedding child rows -----------------------------
    for s in [
        "CREATE TABLE test.buyers (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL, visits INT, joined DATETIME)",
        "CREATE UNIQUE INDEX buyers_name ON test.buyers (name)",
        "CREATE TABLE test.purchases (id BIGINT PRIMARY KEY, buyer_id INT NOT NULL, qty INT, price DECIMAL(10,2), FOREIGN KEY (buyer_id) REFERENCES test.buyers (id))",
        "INSERT INTO test.buyers WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 150) SELECT n, CONCAT('b', n), n % 5, TIMESTAMP('2026-01-01') + INTERVAL n DAY FROM g",
        "INSERT INTO test.purchases WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 400) SELECT n, 1 + n % 150, n % 7, n * 0.25 FROM g",
    ] {
        sql(&myc, s).await;
    }
    let mut embed = request(
        my_id,
        mongo_id,
        vec![
            CopyItem {
                source: ObjectRef::table(NamespacePath::database("test"), "buyers"),
                target: ObjectRef::collection("fromsql", "buyers"),
            },
            CopyItem {
                source: ObjectRef::table(NamespacePath::database("test"), "purchases"),
                target: ObjectRef::collection("fromsql", "purchases"),
            },
        ],
    );
    embed.table_mode = TableMode::Embed;
    let plan = m.plan(&embed).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert_eq!(plan.items[1].kind, "embedded");
    let embed_run = m.start(embed, false).await.unwrap();
    let v = finish(m, embed_run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    let buyers = all(&mongo, "fromsql", "buyers").await;
    assert_eq!(buyers.len(), 150);
    let embedded: usize = buyers
        .iter()
        .map(|d| d.get_array("purchases").unwrap().len())
        .sum();
    assert_eq!(embedded, 400);
    let b1 = buyers.iter().find(|d| d.get_i32("_id") == Ok(1)).unwrap();
    assert!(matches!(b1.get("visits"), Some(Bson::Int32(1))));
    assert!(
        b1.get_datetime("joined").is_ok(),
        "DATETIME becomes a date: {b1:?}"
    );
    let first = b1.get_array("purchases").unwrap()[0].as_document().unwrap();
    assert!(
        matches!(first.get("price"), Some(Bson::Decimal128(_))),
        "{first:?}"
    );
    for run in [back_run, embed_run] {
        m.validate(run, Level::Deep).await.unwrap();
        let deep = validated(m, run).await;
        assert_eq!(deep.passed, Some(true), "{deep:#?}");
    }

    // ---- an account limited to one database is told before copying -------
    for stmt in [
        // Test-only account in a throwaway container.
        "CREATE USER 'limited'@'%' IDENTIFIED BY 'limited-test-pw'",
        "GRANT ALL ON test.* TO 'limited'@'%'",
    ] {
        sql(&myc, stmt).await;
    }
    let limited = e
        .svc
        .create(
            ConnectionProfile::new(
                "limited",
                "mysql.mysql",
                ConnectionSettings::MySql(MySqlSettings {
                    host: my.get_host().await.unwrap().to_string(),
                    port: my.get_host_port_ipv4(3306).await.unwrap(),
                    database: Some("test".into()),
                    username: "limited".into(),
                    password: SecretRef::None,
                    tls_mode: MySqlTlsMode::Disabled,
                    charset: "utf8mb4".into(),
                    connect_timeout_secs: 20,
                    query_timeout_ms: 0,
                    pool_size: 2,
                }),
            ),
            PasswordInput::SessionOnly(zeroize::Zeroizing::new("limited-test-pw".into())),
        )
        .await
        .unwrap()
        .id;
    let into = |db: &str| {
        request(
            mongo_id,
            limited,
            vec![CopyItem {
                source: ObjectRef::collection("shop", "customers"),
                target: ObjectRef::table(NamespacePath::database(db), "customers_again"),
            }],
        )
    };
    let refused = m.plan(&into("elsewhere")).await.unwrap();
    assert!(
        refused.blockers.iter().any(|b| b.contains("`elsewhere`")
            && b.contains("can write to: test")
            && b.contains("GRANT ALL ON `elsewhere`.* TO 'limited'@'%'")),
        "{:?}",
        refused.blockers
    );
    let allowed = m.plan(&into("test")).await.unwrap();
    assert!(allowed.blockers.is_empty(), "{:?}", allowed.blockers);
    // Root may create anything.
    let mut as_root = into("elsewhere");
    as_root.target = my_id;
    assert!(m.plan(&as_root).await.unwrap().blockers.is_empty());
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

/// The MySQL side of the shape round trip: only the root tables are
/// selected on the way back; their child tables are read anyway.
#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mysql_tables_made_from_documents_come_back_as_the_original_documents() {
    let (rs, my) = tokio::join!(common::mongo::start("8.0"), async {
        Mysql::default()
            .with_tag("8.4")
            .start()
            .await
            .expect("mysql")
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
    let my_id = e
        .svc
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
                    pool_size: 4,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id;

    for (mode, table) in [(DocMode::Normalize, "items"), (DocMode::Flatten, "flat")] {
        let mut to_my = request(
            mongo_id,
            my_id,
            vec![CopyItem {
                source: ObjectRef::collection("app", "items"),
                target: shop(table),
            }],
        );
        to_my.doc_mode = mode;
        let v = finish(m, m.start(to_my, false).await.unwrap()).await;
        assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
        assert!(
            v.events
                .iter()
                .any(|ev| ev.message.contains("_gazabridge_shapes")),
            "{:#?}",
            v.events
        );
    }

    let myc = e.svc.open(my_id).await.unwrap();
    let new_id = "aaaaaaaaaaaaaaaaaaaaaaaa";
    sql(
        &myc,
        &format!("INSERT INTO shop.items (_id, title, count) VALUES ('{new_id}', 'from sql', '7')"),
    )
    .await;
    sql(&myc, &format!("INSERT INTO shop.items_tags (_parent_id, _index, value) VALUES ('{new_id}', 1, 'second'), ('{new_id}', 0, 'first')")).await;

    let back = request(
        my_id,
        mongo_id,
        vec![
            CopyItem {
                source: shop("items"),
                target: ObjectRef::collection("back", "items"),
            },
            CopyItem {
                source: shop("flat"),
                target: ObjectRef::collection("back", "flat"),
            },
        ],
    );
    let run = m.start(back, false).await.unwrap();
    let v = finish(m, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    m.validate(run, Level::Deep).await.unwrap();
    let report = validated(m, run).await;
    assert_eq!(report.passed, Some(true), "{report:#?}");

    let new_oid = ObjectId::parse_str(new_id).unwrap();
    for coll in ["items", "flat"] {
        let docs = all(&mongo, "back", coll).await;
        let expected = originals.len() + usize::from(coll == "items");
        assert_eq!(docs.len(), expected, "{coll}");
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
    }
    let docs = all(&mongo, "back", "items").await;
    let added = docs
        .iter()
        .find(|d| d.get_object_id("_id").ok() == Some(new_oid))
        .unwrap();
    assert_eq!(added.get_i32("count").unwrap(), 7, "{added}");
    assert_eq!(
        added.get_array("tags").unwrap(),
        &vec![Bson::String("first".into()), Bson::String("second".into())],
        "in `_index` order"
    );
}
