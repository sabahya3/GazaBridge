//! Continuous sync against real servers: MongoDB change streams into
//! MongoDB and PostgreSQL, PostgreSQL polling into MongoDB, pause and
//! resume from the stored position, and standalone servers refused clearly.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, RunView, StreamPhase, SyncKind, SyncSettings,
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
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{DateTime, Document, doc, oid::ObjectId};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

struct Env {
    svc: ConnectionService,
    m: MigrationService,
    _dir: tempfile::TempDir,
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
    let dir = tempfile::tempdir().unwrap();
    let m = MigrationService::new(
        store.clone(),
        ExplorerService::new(store, svc.clone()),
        svc.redactor().clone(),
        dir.path().join("q"),
    );
    Env { svc, m, _dir: dir }
}

async fn mongo(e: &Env, name: &str, host: &str, port: u16) -> ConnectionId {
    e.svc
        .create(
            ConnectionProfile::new(
                name,
                "mongodb.community",
                ConnectionSettings::MongoDb(MongoSettings {
                    uri: None,
                    hosts: vec![HostPort {
                        host: host.into(),
                        port,
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

async fn client(host: &str, port: u16) -> mongodb::Client {
    mongodb::Client::with_uri_str(format!("mongodb://{host}:{port}/?directConnection=true"))
        .await
        .unwrap()
}

fn request(
    source: ConnectionId,
    target: ConnectionId,
    items: Vec<CopyItem>,
    sync: SyncSettings,
) -> MigrationRequest {
    MigrationRequest {
        name: None,
        source,
        target,
        items,
        existing: ExistingData::Skip,
        copy_indexes: true,
        batch_size: 100,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 1,
        doc_mode: DocMode::Normalize,
        table_mode: Default::default(),
        backup_before_replace: true,
        sync: Some(sync),
    }
}

fn streams() -> SyncSettings {
    SyncSettings {
        method: SyncKind::ChangeStream,
        cursor_field: None,
        interval_secs: 1,
        initial_copy: true,
    }
}

/// Waits until `check` holds (polling every 100 ms, up to 60 s).
async fn eventually<F, Fut>(what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn view(m: &MigrationService, run: RunId) -> RunView {
    m.view(run).await.unwrap()
}

async fn following(m: &MigrationService, run: RunId) {
    eventually("the copy to finish and following to start", || async {
        let v = view(m, run).await;
        assert!(v.status != JobStatus::Failed, "{v:#?}");
        v.streams.iter().all(|s| s.phase == StreamPhase::Following)
    })
    .await;
}

async fn settled(m: &MigrationService, run: RunId) -> RunView {
    eventually("the run to stop", || async {
        let v = view(m, run).await;
        v.status.is_terminal() || v.status == JobStatus::Paused
    })
    .await;
    view(m, run).await
}

async fn count(c: &mongodb::Client, db: &str, coll: &str, filter: Document) -> u64 {
    c.database(db)
        .collection::<Document>(coll)
        .count_documents(filter)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_changes_flow_into_mongodb_and_postgresql_and_resume_after_a_pause() {
    let (src_rs, dst_rs, pg) = tokio::join!(
        common::mongo::start("8.0"),
        common::mongo::start("8.0"),
        async {
            Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .expect("postgres")
        }
    );
    let src = client(&src_rs.host, src_rs.port).await;
    let dst = client(&dst_rs.host, dst_rs.port).await;
    let orders = src.database("shop").collection::<Document>("orders");
    orders
        .insert_many(
            (0..2_000)
                .map(|i| doc! { "_id": i, "n": i, "items": [ { "sku": "A" }, { "sku": "B" } ] }),
        )
        .await
        .unwrap();

    let e = env().await;
    let s = mongo(&e, "source", &src_rs.host, src_rs.port).await;
    let t = mongo(&e, "target", &dst_rs.host, dst_rs.port).await;
    let item = CopyItem {
        source: ObjectRef::collection("shop", "orders"),
        target: ObjectRef::collection("copy", "orders"),
    };

    // MongoDB → MongoDB. Changes made while the (throttled) copy runs are
    // applied after it: the starting point is taken before the copy.
    let mut req = request(s, t, vec![item.clone()], streams());
    req.max_records_per_sec = Some(2_000);
    let plan = e.m.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.warnings.iter().any(|w| w.contains("pause or stop")),
        "{:?}",
        plan.warnings
    );
    let run = e.m.start(req.clone(), false).await.unwrap();
    orders
        .insert_one(doc! { "_id": 5_000, "n": 5_000, "items": [] })
        .await
        .unwrap();
    orders
        .update_one(doc! { "_id": 1_999 }, doc! { "$set": { "n": -1 } })
        .await
        .unwrap();
    following(&e.m, run).await;
    orders.delete_one(doc! { "_id": 7 }).await.unwrap();
    orders
        .update_one(doc! { "_id": 3 }, doc! { "$set": { "late": true } })
        .await
        .unwrap();
    eventually("changes during and after the copy", || async {
        count(&dst, "copy", "orders", doc! { "_id": 5_000 }).await == 1
            && count(&dst, "copy", "orders", doc! { "_id": 1_999, "n": -1 }).await == 1
            && count(&dst, "copy", "orders", doc! { "_id": 7 }).await == 0
            && count(&dst, "copy", "orders", doc! { "_id": 3, "late": true }).await == 1
    })
    .await;
    let v = view(&e.m, run).await;
    assert!(
        v.streams[0].changes >= 2 && v.streams[0].deletes >= 1,
        "{:?}",
        v.streams[0]
    );
    assert!(v.streams[0].last_change.is_some());

    // Pause; changes made while paused arrive after resuming.
    e.m.pause(run).await.unwrap();
    assert_eq!(settled(&e.m, run).await.status, JobStatus::Paused);
    orders
        .insert_one(doc! { "_id": 6_000, "n": 6_000, "items": [] })
        .await
        .unwrap();
    orders.delete_one(doc! { "_id": 8 }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        count(&dst, "copy", "orders", doc! { "_id": 6_000 }).await,
        0,
        "nothing applied while paused"
    );
    e.m.resume(run).await.unwrap();
    eventually("changes made while paused", || async {
        count(&dst, "copy", "orders", doc! { "_id": 6_000 }).await == 1
            && count(&dst, "copy", "orders", doc! { "_id": 8 }).await == 0
    })
    .await;
    assert_eq!(
        view(&e.m, run).await.streams[0].phase,
        StreamPhase::Following
    );

    // Stop, then a deep check: the target equals the source.
    e.m.cancel(run).await.unwrap();
    assert_eq!(settled(&e.m, run).await.status, JobStatus::Cancelled);
    e.m.validate(run, Level::Deep).await.unwrap();
    eventually("validation", || async {
        view(&e.m, run).await.validation.is_some_and(|v| !v.running)
    })
    .await;
    let v = view(&e.m, run).await.validation.unwrap();
    assert_eq!(v.passed, Some(true), "{v:#?}");

    // MongoDB → PostgreSQL: rows, child rows and deletes follow too.
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
    let table = ObjectRef::table(
        NamespacePath::schema(Some("postgres".into()), "public"),
        "orders",
    );
    let run =
        e.m.start(
            request(
                s,
                pg_id,
                vec![CopyItem {
                    source: item.source.clone(),
                    target: table,
                }],
                streams(),
            ),
            false,
        )
        .await
        .unwrap();
    following(&e.m, run).await;
    orders.insert_one(doc! { "_id": 9_000, "n": 9_000, "items": [ { "sku": "X" }, { "sku": "Y" }, { "sku": "Z" } ] }).await.unwrap();
    orders
        .update_one(
            doc! { "_id": 10 },
            doc! { "$set": { "items": [ { "sku": "only" } ] } },
        )
        .await
        .unwrap();
    orders.delete_one(doc! { "_id": 11 }).await.unwrap();
    eventually("rows and child rows in PostgreSQL", || async {
        scalar(
            &pgc,
            "SELECT count(*) FROM orders_items WHERE _parent_id = 9000",
        )
        .await
            == Value::Int(3)
            && scalar(
                &pgc,
                "SELECT count(*) FROM orders_items WHERE _parent_id = 10",
            )
            .await
                == Value::Int(1)
            && scalar(&pgc, "SELECT count(*) FROM orders WHERE _id = 11").await == Value::Int(0)
            && scalar(
                &pgc,
                "SELECT count(*) FROM orders_items WHERE _parent_id = 11",
            )
            .await
                == Value::Int(0)
    })
    .await;
    e.m.cancel(run).await.unwrap();
    settled(&e.m, run).await;

    // PostgreSQL → MongoDB by polling `updated_at`.
    for s in [
        "CREATE TABLE people (id integer PRIMARY KEY, name text, updated_at timestamptz NOT NULL DEFAULT now())",
        "INSERT INTO people (id, name) SELECT g, 'p' || g FROM generate_series(1, 50) g",
    ] {
        sql(&pgc, s).await;
    }
    let people = CopyItem {
        source: ObjectRef::table(
            NamespacePath::schema(Some("postgres".into()), "public"),
            "people",
        ),
        target: ObjectRef::collection("frompg", "people"),
    };
    let mut polling = request(
        pg_id,
        t,
        vec![people.clone()],
        SyncSettings {
            method: SyncKind::Polling,
            cursor_field: Some("updated_at".into()),
            interval_secs: 1,
            initial_copy: true,
        },
    );
    polling.doc_mode = DocMode::Flatten;
    let plan = e.m.plan(&polling).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.warnings
            .iter()
            .any(|w| w.contains("deleted records are not removed")),
        "{:?}",
        plan.warnings
    );
    let mut missing = polling.clone();
    missing.sync.as_mut().unwrap().cursor_field = Some("changed".into());
    assert!(
        e.m.plan(&missing)
            .await
            .unwrap()
            .blockers
            .iter()
            .any(|b| b.contains("no column `changed`"))
    );
    let run = e.m.start(polling, false).await.unwrap();
    following(&e.m, run).await;
    assert_eq!(count(&dst, "frompg", "people", doc! {}).await, 50);
    sql(&pgc, "INSERT INTO people (id, name) VALUES (51, 'new')").await;
    sql(
        &pgc,
        "UPDATE people SET name = 'renamed', updated_at = now() WHERE id = 2",
    )
    .await;
    eventually("polled inserts and updates", || async {
        count(&dst, "frompg", "people", doc! { "_id": 51 }).await == 1
            && count(
                &dst,
                "frompg",
                "people",
                doc! { "_id": 2, "name": "renamed" },
            )
            .await
                == 1
    })
    .await;
    e.m.pause(run).await.unwrap();
    settled(&e.m, run).await;
    sql(
        &pgc,
        "UPDATE people SET name = 'while paused', updated_at = now() WHERE id = 3",
    )
    .await;
    e.m.resume(run).await.unwrap();
    eventually("a change made while paused", || async {
        count(
            &dst,
            "frompg",
            "people",
            doc! { "_id": 3, "name": "while paused" },
        )
        .await
            == 1
    })
    .await;
    let v = view(&e.m, run).await;
    assert!(v.streams[0].changes >= 3, "{:?}", v.streams[0]);
    e.m.cancel(run).await.unwrap();
    settled(&e.m, run).await;
}

async fn sql(conn: &Arc<dyn Connection>, text: &str) -> Vec<Record> {
    conn.execute(
        QueryRequest {
            language: QueryLanguage::PostgresSql,
            text: text.into(),
            params: vec![],
            max_rows: 1_000,
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

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn standalone_servers_are_told_to_poll_instead() {
    let standalone = GenericImage::new("mongo", "8.0")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .start()
        .await
        .unwrap();
    let host = standalone.get_host().await.unwrap().to_string();
    let port = standalone.get_host_port_ipv4(27017).await.unwrap();
    let c = client(&host, port).await;
    c.database("shop")
        .collection::<Document>("orders")
        .insert_many((0..20).map(|i| doc! { "_id": ObjectId::new(), "n": i, "updatedAt": DateTime::from_millis(1_767_225_600_000 + i) }))
        .await
        .unwrap();
    let e = env().await;
    let id = mongo(&e, "standalone", &host, port).await;
    let item = CopyItem {
        source: ObjectRef::collection("shop", "orders"),
        target: ObjectRef::collection("copy", "orders"),
    };
    let plan =
        e.m.plan(&request(id, id, vec![item.clone()], streams()))
            .await
            .unwrap();
    assert!(
        plan.blockers
            .iter()
            .any(|b| b.contains("replica set") && b.contains("polling")),
        "{:?}",
        plan.blockers
    );

    // Polling works on a standalone server.
    let run =
        e.m.start(
            request(
                id,
                id,
                vec![item],
                SyncSettings {
                    method: SyncKind::Polling,
                    cursor_field: Some("updatedAt".into()),
                    interval_secs: 1,
                    initial_copy: true,
                },
            ),
            false,
        )
        .await
        .unwrap();
    following(&e.m, run).await;
    c.database("shop")
        .collection::<Document>("orders")
        .insert_one(doc! { "_id": ObjectId::new(), "n": 99, "updatedAt": DateTime::now() })
        .await
        .unwrap();
    eventually("a polled insert", || async {
        count(&c, "copy", "orders", doc! { "n": 99 }).await == 1
    })
    .await;
    e.m.cancel(run).await.unwrap();
    settled(&e.m, run).await;
}
