//! End to end through the app's own wiring (`AppState::initialize`, the
//! same connectors and services the window uses) against real MongoDB and
//! PostgreSQL servers: a whole-database copy with every kind of collection,
//! a dry run that changes nothing, deep validation, a conversion to
//! PostgreSQL, and a backup restored elsewhere.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

#[path = "../../../../crates/gazabridge-application/tests/common/mongo.rs"]
mod mongo;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use gazabridge_application::backup::{BackupMethod, BackupRequest, RestoreRequest};
use gazabridge_application::tools::{self, ToolKind};
use gazabridge_application::{
    CopyItem, ExistingData, MigrationRequest, OnRecordError, PasswordInput, RunView,
};
use gazabridge_desktop::AppState;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
};
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::JobStatus;
use gazabridge_mapping::doc_to_table::DocMode;
use gazabridge_migration::validate::Level;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use mongodb::bson::{
    Binary, Bson, DateTime, Decimal128, Document, doc, oid::ObjectId, spec::BinarySubtype,
};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

async fn client(rs: &mongo::ReplicaSet) -> mongodb::Client {
    mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap()
}

async fn mongo_conn(state: &AppState, name: &str, rs: &mongo::ReplicaSet) -> ConnectionId {
    state
        .connections
        .create(
            ConnectionProfile::new(
                name,
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

/// Every document of a collection as BSON bytes (order and types count).
async fn bytes(c: &mongodb::Client, db: &str, coll: &str, sort: Document) -> Vec<Vec<u8>> {
    use futures::TryStreamExt;
    let docs: Vec<Document> = c
        .database(db)
        .collection::<Document>(coll)
        .find(doc! {})
        .sort(sort)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    docs.iter()
        .map(|d| mongodb::bson::to_vec(d).unwrap())
        .collect()
}

/// `type` and `options` per collection (the UUID differs by design).
async fn specs(c: &mongodb::Client, db: &str) -> BTreeMap<String, Document> {
    use futures::TryStreamExt;
    let entries: Vec<Document> = c
        .database(db)
        .run_cursor_command(doc! { "listCollections": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    entries
        .into_iter()
        .filter(|e| !e.get_str("name").unwrap().starts_with("system."))
        .map(|e| {
            let mut spec = doc! { "type": e.get_str("type").unwrap(), "options": e.get_document("options").unwrap().clone() };
            let options = spec.get_document_mut("options").unwrap();
            options.remove("uuid");
            // `strict` is the default; collMod writes it out, create does not.
            if options.get_str("validationLevel") == Ok("strict") {
                options.remove("validationLevel");
            }
            (e.get_str("name").unwrap().to_owned(), spec)
        })
        .collect()
}

async fn indexes(c: &mongodb::Client, db: &str, coll: &str) -> Vec<Document> {
    use futures::TryStreamExt;
    let mut v: Vec<Document> = match c
        .database(db)
        .run_cursor_command(doc! { "listIndexes": coll })
        .await
    {
        Ok(cur) => cur.try_collect().await.unwrap(),
        Err(_) => return Vec::new(), // views have no indexes
    };
    for d in &mut v {
        d.remove("v");
        d.remove("ns");
    }
    v.sort_by_key(|d| d.get_str("name").unwrap_or_default().to_owned());
    v
}

fn failures(v: &RunView) -> String {
    let Some(r) = &v.validation else {
        return "no validation".into();
    };
    r.streams
        .iter()
        .filter(|s| !s.passed)
        .map(|s| {
            let t: Vec<String> = s
                .targets
                .iter()
                .map(|t| {
                    format!(
                        "{} m{} miss{} x{} d{}",
                        t.target, t.matched, t.missing, t.extra, t.different
                    )
                })
                .collect();
            format!(
                "{}: {:?} {:?} {:?}",
                s.source,
                t,
                s.notes,
                s.differences.iter().take(3).collect::<Vec<_>>()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
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

async fn seed(c: &mongodb::Client) {
    let shop = c.database("shop");
    shop.collection::<Document>("orders")
        .insert_many((0..3_000).map(|i: i32| {
            doc! {
                "_id": ObjectId::new(),
                "n": i,
                "big": i64::from(i) * 7_000_000_000,
                "price": format!("{}.{:02}", i % 500, i % 100).parse::<Decimal128>().unwrap(),
                "at": DateTime::from_millis(1_767_225_600_000 + i64::from(i) * 60_000),
                "customer": format!("c{}", i % 200),
                "items": (0..i % 4).map(|k| Bson::Document(doc! { "sku": format!("S{k}"), "qty": k + 1 })).collect::<Vec<_>>(),
                "note": if i % 5 == 0 { Bson::Null } else { Bson::String(format!("طلب رقم {i}")) },
                "raw": Binary { subtype: BinarySubtype::Generic, bytes: vec![u8::try_from(i % 256).unwrap(); 4] },
            }
        }))
        .await
        .unwrap();
    shop.run_command(doc! {
        "create": "customers",
        "validator": { "$jsonSchema": { "bsonType": "object", "required": ["email"], "properties": { "email": { "bsonType": "string" } } } },
        "validationAction": "error",
    })
    .await
    .unwrap();
    shop.collection::<Document>("customers")
        .insert_many((0..200).map(|i| doc! { "_id": format!("c{i}"), "email": format!("c{i}@example.test"), "name": format!("عميل {i}") }))
        .await
        .unwrap();
    shop.run_command(doc! { "create": "events", "capped": true, "size": 1_048_576, "max": 5_000 })
        .await
        .unwrap();
    shop.collection::<Document>("events")
        .insert_many((0..500).map(|i| doc! { "_id": ObjectId::new(), "kind": "click", "i": i }))
        .await
        .unwrap();
    shop.run_command(doc! { "create": "names", "collation": { "locale": "ar", "strength": 2 } })
        .await
        .unwrap();
    shop.collection::<Document>("names")
        .insert_many(["أحمد", "سارة", "محمد"].map(|n| doc! { "_id": ObjectId::new(), "name": n }))
        .await
        .unwrap();
    shop.run_command(doc! { "create": "metrics", "timeseries": { "timeField": "ts", "metaField": "sensor", "granularity": "minutes" } })
        .await
        .unwrap();
    shop.collection::<Document>("metrics")
        .insert_many((0..300).map(|i| doc! { "_id": ObjectId::new(), "ts": DateTime::from_millis(1_767_225_600_000 + i * 60_000), "sensor": { "id": i % 3 }, "value": f64::from(i32::try_from(i).unwrap()) / 2.0 }))
        .await
        .unwrap();
    shop.run_command(doc! { "create": "big_orders", "viewOn": "orders", "pipeline": [ { "$match": { "n": { "$gte": 2_900 } } }, { "$project": { "n": 1, "price": 1 } } ] })
        .await
        .unwrap();
    shop.run_command(doc! { "createIndexes": "orders", "indexes": [
        { "key": { "at": 1 }, "name": "ttl", "expireAfterSeconds": 31_536_000 },
        { "key": { "customer": 1, "at": -1 }, "name": "by_customer" },
        { "key": { "note": "text" }, "name": "note_text" },
    ]})
    .await
    .unwrap();
    shop.run_command(doc! { "createIndexes": "customers", "indexes": [
        { "key": { "email": 1 }, "name": "email_unique", "unique": true, "partialFilterExpression": { "email": { "$type": "string" } } },
    ]})
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn the_app_copies_converts_checks_and_restores_a_whole_database() {
    let (src_rs, dst_rs, pg) = tokio::join!(mongo::start("8.0"), mongo::start("8.0"), async {
        Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .expect("postgres")
    });
    let (src, dst) = (client(&src_rs).await, client(&dst_rs).await);
    seed(&src).await;

    let dir = tempfile::tempdir().unwrap();
    let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
        .await
        .unwrap();
    let s = mongo_conn(&state, "source", &src_rs).await;
    let t = mongo_conn(&state, "target", &dst_rs).await;

    // The whole database, as the page lists it (collections, views, time series).
    let listed = state
        .explorer
        .list(s, &NamespacePath::database("shop"))
        .await
        .unwrap();
    let items: Vec<CopyItem> = listed
        .iter()
        .filter(|e| {
            matches!(e.kind, ObjectKind::Collection | ObjectKind::View)
                && !e.name.starts_with("system.")
        })
        .map(|e| CopyItem {
            source: ObjectRef {
                namespace: NamespacePath::database("shop"),
                name: e.name.clone(),
                kind: e.kind,
            },
            target: ObjectRef::collection("copy", e.name.clone()),
        })
        .collect();
    assert_eq!(items.len(), 6, "{listed:?}");
    let req = MigrationRequest {
        name: None,
        source: s,
        target: t,
        items,
        existing: ExistingData::Skip,
        copy_indexes: true,
        batch_size: 400,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 3,
        doc_mode: DocMode::Flatten,
        table_mode: Default::default(),
        backup_before_replace: true,
        sync: None,
    };
    let plan = state.migrations.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    let view = plan
        .items
        .iter()
        .find(|i| i.source.name == "big_orders")
        .unwrap();
    assert_eq!(view.kind, "view");
    let customers = plan
        .items
        .iter()
        .find(|i| i.source.name == "customers")
        .unwrap();
    assert!(
        customers
            .statements
            .iter()
            .any(|s| s.contains("collMod") && s.contains("$jsonSchema")),
        "{:?}",
        customers.statements
    );

    // Dry run: everything read, nothing written.
    let v = settle(&state, state.migrations.dry_run(req.clone()).await.unwrap()).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert!(
        dst.list_database_names()
            .await
            .unwrap()
            .iter()
            .all(|d| d != "copy"),
        "a dry run creates nothing"
    );

    // The copy, its automatic count check, then a deep check.
    let run = state.migrations.start(req.clone(), false).await.unwrap();
    let v = settle(&state, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(
        v.validation.as_ref().unwrap().passed,
        Some(true),
        "{}",
        failures(&v)
    );
    state.migrations.validate(run, Level::Deep).await.unwrap();
    let v = settle(&state, run).await;
    assert_eq!(v.validation.as_ref().unwrap().level, Level::Deep);
    assert_eq!(
        v.validation.as_ref().unwrap().passed,
        Some(true),
        "{}",
        failures(&v)
    );

    // Same collections with the same options (capped, collation, time
    // series, validator, view definition), data and indexes.
    assert_eq!(specs(&src, "shop").await, specs(&dst, "copy").await);
    for (coll, sort) in [
        ("orders", doc! { "_id": 1 }),
        ("customers", doc! { "_id": 1 }),
        ("events", doc! { "$natural": 1 }),
        ("names", doc! { "_id": 1 }),
        ("big_orders", doc! { "_id": 1 }),
    ] {
        assert_eq!(
            bytes(&src, "shop", coll, sort.clone()).await,
            bytes(&dst, "copy", coll, sort).await,
            "{coll}"
        );
        assert_eq!(
            indexes(&src, "shop", coll).await,
            indexes(&dst, "copy", coll).await,
            "{coll}"
        );
    }
    // Time series documents are rebuilt from buckets on read: same values
    // and types, field order not guaranteed.
    let sorted = |v: Vec<Vec<u8>>| -> Vec<Document> {
        v.iter()
            .map(|b| {
                let d: Document = mongodb::bson::from_slice(b).unwrap();
                let mut pairs: Vec<(String, Bson)> = d.into_iter().collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs.into_iter().collect()
            })
            .collect()
    };
    let (a, b) = (
        sorted(bytes(&src, "shop", "metrics", doc! { "_id": 1 }).await),
        sorted(bytes(&dst, "copy", "metrics", doc! { "_id": 1 }).await),
    );
    assert_eq!(a.len(), 300);
    assert!(
        a.iter()
            .zip(&b)
            .all(|(x, y)| mongodb::bson::to_vec(x).unwrap() == mongodb::bson::to_vec(y).unwrap()),
        "metrics values and types"
    );
    assert_eq!(
        indexes(&src, "shop", "metrics").await,
        indexes(&dst, "copy", "metrics").await
    );
    let rejected = dst
        .database("copy")
        .collection::<Document>("customers")
        .insert_one(doc! { "_id": "x", "email": 5 })
        .await;
    assert!(rejected.is_err(), "the validator is active on the copy");

    // MongoDB → PostgreSQL (normalized), checked deeply across engines.
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
                    pool_size: 2,
                }),
            ),
            // The image's documented default password, for this session only.
            PasswordInput::SessionOnly(Zeroizing::new("postgres".into())),
        )
        .await
        .unwrap()
        .id;
    let to_pg = MigrationRequest {
        target: pg_id,
        items: ["orders", "customers"]
            .map(|c| CopyItem {
                source: ObjectRef::collection("shop", c),
                target: ObjectRef::table(NamespacePath::schema(Some("postgres".into()), "shop"), c),
            })
            .to_vec(),
        doc_mode: DocMode::Normalize,
        ..req.clone()
    };
    let run = state.migrations.start(to_pg, false).await.unwrap();
    let v = settle(&state, run).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    state.migrations.validate(run, Level::Deep).await.unwrap();
    let v = settle(&state, run).await;
    assert_eq!(
        v.validation.as_ref().unwrap().passed,
        Some(true),
        "{}",
        failures(&v)
    );
    let orders_check = &v.validation.as_ref().unwrap().streams[0];
    assert_eq!(orders_check.targets.len(), 2, "orders and orders_items");

    // A backup of the whole database, restored on the other server.
    let objects: Vec<ObjectRef> = ["orders", "customers", "events", "names", "metrics"]
        .map(|c| ObjectRef::collection("shop", c))
        .to_vec();
    let backup = state
        .backups
        .backup(
            &BackupRequest {
                connection: s,
                objects: objects.clone(),
                method: BackupMethod::GazaBridge,
                label: Some("shop".into()),
            },
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap();
    let restore = RestoreRequest {
        backup: backup.clone(),
        connection: t,
        rename_to: Some("restored".into()),
        only: vec![],
        existing: ExistingData::Fail,
    };
    let (plan, _) = state.backups.plan_restore(&restore).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    state
        .backups
        .restore(&restore, false, CancellationToken::new(), &|_| {})
        .await
        .unwrap();
    for coll in ["orders", "customers", "names"] {
        assert_eq!(
            bytes(&src, "shop", coll, doc! { "_id": 1 }).await,
            bytes(&dst, "restored", coll, doc! { "_id": 1 }).await,
            "{coll}"
        );
    }
    if tools::detect(ToolKind::MongoDump).await.path.is_some()
        && tools::detect(ToolKind::MongoRestore).await.path.is_some()
    {
        let native = state
            .backups
            .backup(
                &BackupRequest {
                    connection: s,
                    objects,
                    method: BackupMethod::Native,
                    label: None,
                },
                CancellationToken::new(),
                &|_| {},
            )
            .await
            .unwrap();
        let req = RestoreRequest {
            backup: native,
            connection: t,
            rename_to: Some("native".into()),
            only: vec![],
            existing: ExistingData::Fail,
        };
        state
            .backups
            .restore(&req, false, CancellationToken::new(), &|_| {})
            .await
            .unwrap();
        assert_eq!(
            specs(&src, "shop").await.get("events"),
            specs(&dst, "native").await.get("events"),
            "mongodump keeps the capped options"
        );
        assert_eq!(
            bytes(&src, "shop", "orders", doc! { "_id": 1 }).await,
            bytes(&dst, "native", "orders", doc! { "_id": 1 }).await
        );
    }
}
