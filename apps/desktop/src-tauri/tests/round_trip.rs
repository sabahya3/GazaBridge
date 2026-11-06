//! Data that travels through every engine, end to end through the app's own
//! wiring (`AppState::initialize`) against real servers: MongoDB 8.0,
//! PostgreSQL 16, MySQL 8.4 and MariaDB 11.4.
//!
//! - Documents with every common BSON type become normalized PostgreSQL
//!   tables, are copied PostgreSQL → MySQL → MariaDB → PostgreSQL, and must
//!   come back equal to the first tables, value by value.
//! - The same documents stored whole (JSON mode) travel MongoDB → MySQL →
//!   PostgreSQL → MongoDB and must come back as the same documents.
//! - Each step is validated deeply; files round-trip MongoDB and MySQL data.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

#[path = "../../../../crates/gazabridge-application/tests/common/mongo.rs"]
mod mongo;

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use gazabridge_application::{
    CopyItem, ExistingData, ExportRequest, ImportRequest, MigrationRequest, OnRecordError,
    PasswordInput, RunView,
};
use gazabridge_desktop::AppState;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, MySqlSettings, MySqlTlsMode,
    PgSslMode, PostgresSettings, ReadPreference, SecretRef,
};
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::{JobStatus, WriteMode};
use gazabridge_domain::{Record, Value};
use gazabridge_files::{
    Compression, FileFormat, JsonFlavor, MalformedPolicy, ReadOptions, WriteOptions,
};
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_mapping::table_to_doc::TableMode;
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use mongodb::bson::{
    Binary, Bson, DateTime, Decimal128, Document, doc, oid::ObjectId, spec::BinarySubtype,
};
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

const PEOPLE: i32 = 300;

fn person(i: i32) -> Document {
    let mut d = doc! {
        "_id": ObjectId::from_bytes([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, (i / 256) as u8, (i % 256) as u8]),
        "name": match i % 4 {
            0 => format!("أحمد {i}"),
            1 => format!("emoji 🚀 {i}"),
            2 => format!("quote ' \" \\ {i}"),
            _ => format!("plain {i}"),
        },
        "score": if i % 7 == 0 { -1.5e10 } else { f64::from(i) / 3.0 },
        "balance": format!("{}.{:04}", i * 1_000_003, i % 10_000).parse::<Decimal128>().unwrap(),
        "born": DateTime::from_millis(-315_619_200_000 + i64::from(i) * 86_400_123),
        "active": i % 2 == 0,
        "big": i64::from(i) * 30_000_000_000,
        "tags": (0..i % 3).map(|t| Bson::String(format!("t{t}"))).collect::<Vec<_>>(),
        "address": { "city": if i % 2 == 0 { "غزة" } else { "Rafah" }, "geo": { "lat": 31.5 + f64::from(i) / 1000.0, "lng": 34.4 } },
        "orders": (0..i % 4).map(|k| Bson::Document(doc! {
            "sku": format!("S{k}"),
            "qty": k + 1,
            "price": format!("{}.25", k + 1).parse::<Decimal128>().unwrap(),
        })).collect::<Vec<_>>(),
        "blob": Binary { subtype: BinarySubtype::Generic, bytes: vec![(i % 256) as u8, 0, 255, 10] },
    };
    if i % 3 != 0 {
        d.insert("age", 18 + i % 60);
    }
    if i % 5 == 0 {
        d.insert("note", Bson::Null);
    } else if i % 5 == 1 {
        d.insert("note", format!("ملاحظة {i}"));
    }
    d
}

async fn mongo_conn(state: &AppState, rs: &mongo::ReplicaSet) -> ConnectionId {
    state
        .connections
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
        .id
}

async fn mysql_conn(
    state: &AppState,
    name: &str,
    profile: &str,
    host: String,
    port: u16,
) -> ConnectionId {
    state
        .connections
        .create(
            ConnectionProfile::new(
                name,
                profile,
                ConnectionSettings::MySql(MySqlSettings {
                    host,
                    port,
                    database: None,
                    username: "root".into(),
                    password: SecretRef::None,
                    tls_mode: MySqlTlsMode::Disabled,
                    charset: "utf8mb4".into(),
                    connect_timeout_secs: 30,
                    query_timeout_ms: 0,
                    pool_size: 4,
                }),
            ),
            PasswordInput::None,
        )
        .await
        .unwrap()
        .id
}

async fn pg_sql(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    sql(conn, QueryLanguage::PostgresSql, text).await
}

async fn sql(conn: &Arc<dyn Connection>, language: QueryLanguage, text: &str) -> Vec<Record> {
    conn.execute(
        QueryRequest {
            language,
            text: text.into(),
            params: vec![],
            max_rows: 100_000,
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

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => panic!("not text: {other:?}"),
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int(n) => *n,
        Value::UInt(n) => i64::try_from(*n).unwrap(),
        Value::Decimal(d) => d.to_plain_string().parse().unwrap(),
        other => panic!("not a count: {other:?}"),
    }
}

fn request(
    source: ConnectionId,
    target: ConnectionId,
    items: Vec<CopyItem>,
    doc_mode: DocMode,
) -> MigrationRequest {
    MigrationRequest {
        name: None,
        source,
        target,
        items,
        existing: ExistingData::Skip,
        copy_indexes: true,
        batch_size: 97,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 3,
        doc_mode,
        table_mode: TableMode::Reference,
        backup_before_replace: true,
        sync: None,
    }
}

async fn settle(state: &AppState, run: RunId) -> RunView {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let v = state.migrations.view(run).await.unwrap();
        let checking = v.validation.as_ref().is_some_and(|x| x.running)
            || (v.validation.is_none() && !v.dry_run && v.status == JobStatus::Completed);
        if (v.status.is_terminal() || v.status == JobStatus::Paused) && !checking {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {v:#?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Plans (no blockers), copies, and validates deeply; returns the run.
async fn copy(state: &AppState, req: MigrationRequest, what: &str) -> RunView {
    let plan = state.migrations.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{what}: {:?}", plan.blockers);
    let run = state.migrations.start(req, false).await.unwrap();
    let v = settle(state, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{what}: {v:#?}");
    state.migrations.validate(run, Level::Deep).await.unwrap();
    let v = settle(state, run).await;
    let report = v.validation.clone().unwrap();
    assert_eq!(report.passed, Some(true), "{what}: {report:#?}");
    v
}

fn pg_table(schema: &str, name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::schema(Some("postgres".into()), schema), name)
}

fn my_table(db: &str, name: &str) -> ObjectRef {
    ObjectRef::table(NamespacePath::database(db), name)
}

/// Relaxed Extended JSON with keys sorted: the same document whatever order
/// an engine kept its keys in.
fn canonical(d: &Document) -> serde_json::Value {
    fn sort(v: serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let mut pairs: Vec<_> = m.into_iter().map(|(k, v)| (k, sort(v))).collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                serde_json::Value::Object(pairs.into_iter().collect())
            }
            serde_json::Value::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(sort).collect())
            }
            other => other,
        }
    }
    sort(Bson::Document(d.clone()).into_canonical_extjson())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn data_travels_through_every_engine_and_comes_back_the_same() {
    let (rs, pg, my, maria) = tokio::join!(
        mongo::start("8.0"),
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
        },
        async {
            GenericImage::new("mariadb", "11.4")
                .with_wait_for(
                    testcontainers_modules::testcontainers::core::WaitFor::message_on_stderr(
                        "ready for connections",
                    ),
                )
                .with_env_var("MARIADB_ALLOW_EMPTY_ROOT_PASSWORD", "1")
                .start()
                .await
                .expect("mariadb")
        }
    );
    let mongo = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let originals: Vec<Document> = (0..PEOPLE).map(person).collect();
    mongo
        .database("lab")
        .collection::<Document>("people")
        .insert_many(originals.clone())
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
        .await
        .unwrap();
    let mongo_id = mongo_conn(&state, &rs).await;
    let pg_id = state
        .connections
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
                    pool_size: 4,
                }),
            ),
            // The image's documented default password.
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap()
        .id;
    let my_id = mysql_conn(
        &state,
        "mysql",
        "mysql.mysql",
        my.get_host().await.unwrap().to_string(),
        my.get_host_port_ipv4(3306).await.unwrap(),
    )
    .await;
    let maria_id = mysql_conn(
        &state,
        "mariadb",
        "mysql.mariadb",
        maria.get_host().await.unwrap().to_string(),
        maria.get_host_port_ipv4(3306).await.unwrap(),
    )
    .await;
    for (id, product) in [(my_id, "MySQL"), (maria_id, "MariaDB")] {
        let t = state.connections.test_saved(id).await.unwrap();
        assert_eq!(t.server.as_ref().unwrap().product, product, "{t:?}");
    }
    let pgc = state.connections.open(pg_id).await.unwrap();
    let myc = state.connections.open(my_id).await.unwrap();

    // ---- 1. MongoDB → PostgreSQL, normalized into child tables ------------
    let v = copy(
        &state,
        request(
            mongo_id,
            pg_id,
            vec![CopyItem {
                source: ObjectRef::collection("lab", "people"),
                target: pg_table("lab", "people"),
            }],
            DocMode::Normalize,
        ),
        "MongoDB → PostgreSQL",
    )
    .await;
    assert_eq!(v.streams[0].stats.read, PEOPLE as u64);
    let tables: Vec<String> = sql(
        &pgc,
        QueryLanguage::PostgresSql,
        "SELECT table_name AS t FROM information_schema.tables WHERE table_schema = 'lab' ORDER BY 1",
    )
    .await
    .iter()
    .map(|r| text(r.get("t")))
    .collect();
    // With the mapping saved to rebuild the documents later.
    assert_eq!(
        tables,
        [
            "_gazabridge_shapes",
            "people",
            "people_orders",
            "people_tags"
        ]
    );

    // ---- 2. PostgreSQL → MySQL → MariaDB → PostgreSQL ---------------------
    let hop = |s: ConnectionId,
               t: ConnectionId,
               from: &dyn Fn(&str) -> ObjectRef,
               to: &dyn Fn(&str) -> ObjectRef| {
        request(
            s,
            t,
            tables
                .iter()
                .map(|n| CopyItem {
                    source: from(n),
                    target: to(n),
                })
                .collect(),
            DocMode::Flatten,
        )
    };
    // A dry run writes nothing, not even the database.
    let dry = hop(pg_id, my_id, &|n| pg_table("lab", n), &|n| {
        my_table("lab", n)
    });
    let run = state.migrations.dry_run(dry).await.unwrap();
    let v = settle(&state, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert!(
        sql(&myc, QueryLanguage::MySql, "SHOW DATABASES LIKE 'lab'")
            .await
            .is_empty()
    );
    copy(
        &state,
        hop(pg_id, my_id, &|n| pg_table("lab", n), &|n| {
            my_table("lab", n)
        }),
        "PostgreSQL → MySQL",
    )
    .await;
    copy(
        &state,
        hop(my_id, maria_id, &|n| my_table("lab", n), &|n| {
            my_table("lab", n)
        }),
        "MySQL → MariaDB",
    )
    .await;
    copy(
        &state,
        hop(maria_id, pg_id, &|n| my_table("lab", n), &|n| {
            pg_table("back", n)
        }),
        "MariaDB → PostgreSQL",
    )
    .await;
    // Every value came back: compare each column in the first table's type.
    for t in &tables {
        let cols = sql(
            &pgc,
            QueryLanguage::PostgresSql,
            &format!(
                "SELECT a.attname AS c, format_type(a.atttypid, a.atttypmod) AS t
                 FROM pg_attribute a WHERE a.attrelid = 'lab.{t}'::regclass AND a.attnum > 0 AND NOT a.attisdropped"
            ),
        )
        .await;
        let mut on = Vec::new();
        for c in &cols {
            let (name, ty) = (text(c.get("c")), text(c.get("t")));
            on.push(format!(
                "a.\"{name}\" IS NOT DISTINCT FROM b.\"{name}\"::{}",
                if ty == "json" { "jsonb".into() } else { ty }
            ));
        }
        let key = if t == "people" { "_id" } else { "_parent_id" };
        let differ = sql(
            &pgc,
            QueryLanguage::PostgresSql,
            &format!(
                "SELECT count(*) AS n FROM lab.{t} a WHERE NOT EXISTS (SELECT 1 FROM back.{t} b WHERE {})",
                on.join(" AND ")
            ),
        )
        .await;
        assert_eq!(
            int(differ[0].get("n")),
            0,
            "{t}: rows changed on the way ({key})"
        );
        let counts = sql(
            &pgc,
            QueryLanguage::PostgresSql,
            &format!(
                "SELECT (SELECT count(*) FROM lab.{t}) AS a, (SELECT count(*) FROM back.{t}) AS b"
            ),
        )
        .await;
        assert_eq!(counts[0].get("a"), counts[0].get("b"), "{t}");
    }

    // ---- 3. Whole documents: MongoDB → MySQL → PostgreSQL → MongoDB --------
    copy(
        &state,
        request(
            mongo_id,
            my_id,
            vec![CopyItem {
                source: ObjectRef::collection("lab", "people"),
                target: my_table("docs", "people"),
            }],
            DocMode::Jsonb,
        ),
        "MongoDB → MySQL (JSON)",
    )
    .await;
    copy(
        &state,
        request(
            my_id,
            pg_id,
            vec![CopyItem {
                source: my_table("docs", "people"),
                target: pg_table("docs", "people"),
            }],
            DocMode::Flatten,
        ),
        "MySQL → PostgreSQL (JSON table)",
    )
    .await;
    copy(
        &state,
        request(
            pg_id,
            mongo_id,
            vec![CopyItem {
                source: pg_table("docs", "people"),
                target: ObjectRef::collection("lab_back", "people"),
            }],
            DocMode::Flatten,
        ),
        "PostgreSQL → MongoDB",
    )
    .await;
    let back: Vec<Document> = mongo
        .database("lab_back")
        .collection::<Document>("people")
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(back.len(), originals.len());
    for (a, b) in originals.iter().zip(&back) {
        assert_eq!(
            canonical(a),
            canonical(b),
            "document {} changed",
            a.get_object_id("_id").unwrap()
        );
    }

    // ---- 4. Files: MongoDB BSON round trip, MySQL rows through JSONL ------
    let files = dir.path().join("files");
    std::fs::create_dir_all(&files).unwrap();
    for (conn, object, format, into) in [
        (
            mongo_id,
            ObjectRef::collection("lab", "people"),
            FileFormat::Bson,
            ObjectRef::collection("lab_file", "people"),
        ),
        (
            my_id,
            my_table("lab", "people"),
            FileFormat::Jsonl,
            my_table("lab_file", "people"),
        ),
    ] {
        if format == FileFormat::Jsonl {
            sql(&myc, QueryLanguage::MySql, "CREATE DATABASE lab_file").await;
            sql(
                &myc,
                QueryLanguage::MySql,
                "CREATE TABLE lab_file.people LIKE lab.people",
            )
            .await;
        }
        let path = files.join(format!("people.{format:?}.gz"));
        let mut options = WriteOptions::new(format);
        options.compression = Compression::Gzip;
        let out = state
            .transfers
            .export(
                conn,
                ExportRequest {
                    object: object.clone(),
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
        assert_eq!(out.records_read, PEOPLE as u64);
        let r = state
            .transfers
            .import(
                conn,
                ImportRequest {
                    object: into.clone(),
                    path,
                    options: ReadOptions {
                        format,
                        json_flavor: JsonFlavor::Typed,
                        compression: Compression::Gzip,
                        malformed: MalformedPolicy::Fail,
                    },
                    mode: WriteMode::Insert,
                    key_fields: vec![],
                    batch_size: 50,
                    stop_on_error: true,
                },
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
        assert_eq!(
            (r.inserted, r.failed),
            (PEOPLE as u64, 0),
            "{format:?}: {r:?}"
        );
    }
    let file_copy: Vec<Document> = mongo
        .database("lab_file")
        .collection::<Document>("people")
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(file_copy, originals, "BSON files keep documents exactly");
    let differ = sql(
        &myc,
        QueryLanguage::MySql,
        "SELECT COUNT(*) AS n FROM lab.people a LEFT JOIN lab_file.people b ON b._id = a._id
         AND CAST(a.balance AS CHAR) = CAST(b.balance AS CHAR) AND a.born <=> b.born
         AND a.name = b.name AND a.blob <=> b.blob AND a.address_geo_lat <=> b.address_geo_lat
         WHERE b._id IS NULL",
    )
    .await;
    assert_eq!(
        int(differ[0].get("n")),
        0,
        "MySQL rows changed through JSONL"
    );

    // ---- 5. CSV: a PostgreSQL table out and back in; a sheet into MongoDB --
    pg_sql(&pgc, "CREATE TABLE back.people_csv (LIKE lab.people)").await;
    let path = files.join("people.csv");
    let mut options = WriteOptions::new(FileFormat::Csv);
    options.csv.columns = Vec::new();
    state
        .transfers
        .export(
            pg_id,
            ExportRequest {
                object: pg_table("lab", "people"),
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
    let csv_in = |conn: ConnectionId, object: ObjectRef, path: std::path::PathBuf| {
        let transfers = state.transfers.clone();
        async move {
            transfers
                .import(
                    conn,
                    ImportRequest {
                        object,
                        path,
                        options: ReadOptions {
                            format: FileFormat::Csv,
                            json_flavor: JsonFlavor::Typed,
                            compression: Compression::None,
                            malformed: MalformedPolicy::Fail,
                        },
                        mode: WriteMode::Insert,
                        key_fields: vec![],
                        batch_size: 64,
                        stop_on_error: true,
                    },
                    CancellationToken::new(),
                    |_| {},
                )
                .await
                .unwrap()
        }
    };
    let r = csv_in(pg_id, pg_table("back", "people_csv"), path).await;
    assert_eq!((r.inserted, r.failed), (PEOPLE as u64, 0), "{r:?}");
    let differ = pg_sql(
        &pgc,
        "SELECT count(*) AS n FROM lab.people a WHERE NOT EXISTS (
            SELECT 1 FROM back.people_csv b WHERE (to_jsonb(b) - '_extra') = (to_jsonb(a) - '_extra')
              AND b._extra IS NOT DISTINCT FROM a._extra)",
    )
    .await;
    let cols = pg_sql(
        &pgc,
        "SELECT x.key AS k, x.value::text AS a, (to_jsonb(b) -> x.key)::text AS b FROM lab.people a
         JOIN back.people_csv b ON b._id = a._id, jsonb_each(to_jsonb(a)) x
         WHERE (to_jsonb(b) -> x.key) IS DISTINCT FROM x.value LIMIT 12",
    )
    .await;
    assert_eq!(
        int(differ[0].get("n")),
        0,
        "PostgreSQL rows changed through CSV: {cols:?}"
    );

    let sheet = files.join("sheet.csv");
    std::fs::write(
        &sheet,
        "\u{feff}code;qty;price;ok;when;note\r\n007;3;1.5;true;2026-01-01;غزة\r\nA1;-2;1.50;false;;\"x;y\"\r\n",
    )
    .unwrap();
    let r = csv_in(mongo_id, ObjectRef::collection("lab", "sheet"), sheet).await;
    assert_eq!(r.inserted, 2, "{r:?}");
    let docs: Vec<Document> = mongo
        .database("lab")
        .collection::<Document>("sheet")
        .find(doc! {})
        .projection(doc! { "_id": 0 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        docs,
        [
            doc! { "code": "007", "qty": 3_i64, "price": 1.5, "ok": true, "when": "2026-01-01", "note": "غزة" },
            doc! { "code": "A1", "qty": -2_i64, "price": "1.50", "ok": false, "note": "x;y" },
        ]
    );
}
