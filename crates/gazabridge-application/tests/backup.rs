//! Backups and restores against real servers: GazaBridge files on MongoDB
//! and PostgreSQL, `mongodump`/`mongorestore` when installed, checksums,
//! and backups taken before a copy drops its target.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::backup::{BackupMethod, BackupRequest, BackupService, RestoreRequest};
use gazabridge_application::tools::{self, ToolKind};
use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, TransferService,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, PgSslMode, PostgresSettings,
    ReadPreference, SecretRef,
};
use gazabridge_domain::ids::ConnectionId;
use gazabridge_domain::job::JobStatus;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::Connection;
use gazabridge_plugin_sdk::capability::QueryLanguage;
use gazabridge_plugin_sdk::query::QueryRequest;
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{
    Binary, DateTime, Decimal128, Document, doc, oid::ObjectId, spec::BinarySubtype,
};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

struct Env {
    store: MetadataStore,
    svc: ConnectionService,
    backups: BackupService,
    explorer: ExplorerService,
    dir: tempfile::TempDir,
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
    Env {
        store,
        svc,
        backups,
        explorer,
        dir,
    }
}

async fn mongo_conn(e: &Env, rs: &common::mongo::ReplicaSet) -> ConnectionId {
    e.svc
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

async fn all(c: &mongodb::Client, db: &str, coll: &str) -> Vec<Vec<u8>> {
    use futures::TryStreamExt;
    let docs: Vec<Document> = c
        .database(db)
        .collection::<Document>(coll)
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    // Byte-exact: field order and BSON types count.
    docs.iter()
        .map(|d| mongodb::bson::to_vec(d).unwrap())
        .collect()
}

async fn index_specs(c: &mongodb::Client, db: &str, coll: &str) -> Vec<Document> {
    use futures::TryStreamExt;
    let mut v: Vec<Document> = c
        .database(db)
        .run_cursor_command(doc! { "listIndexes": coll })
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap()
        .into_iter()
        .map(|mut d| {
            d.remove("v");
            d.remove("ns");
            d
        })
        .collect();
    v.sort_by_key(|d| d.get_str("name").unwrap_or_default().to_owned());
    v
}

fn restore(
    backup: &std::path::Path,
    to: ConnectionId,
    rename: &str,
    existing: ExistingData,
) -> RestoreRequest {
    RestoreRequest {
        backup: backup.to_path_buf(),
        connection: to,
        rename_to: Some(rename.into()),
        only: vec![],
        existing,
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_backups_restore_exactly_and_guard_their_files() {
    let rs = common::mongo::start("8.0").await;
    let c = mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap();
    let shop = c.database("shop");
    shop.collection::<Document>("orders")
        .insert_many((0..2_000).map(|i| {
            doc! {
                "_id": ObjectId::new(), "n": i, "big": i64::from(i) * 9_000_000_000,
                "price": "12.50".parse::<Decimal128>().unwrap(),
                "at": DateTime::from_millis(1_767_225_600_000 + i64::from(i)),
                "raw": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] },
                "note": format!("طلب {i}"),
            }
        }))
        .await
        .unwrap();
    shop.collection::<Document>("customers")
        .insert_many(
            (0..300)
                .map(|i| doc! { "_id": format!("c{i}"), "email": format!("c{i}@example.test") }),
        )
        .await
        .unwrap();
    shop.collection::<Document>("logs")
        .insert_one(doc! { "x": 1 })
        .await
        .unwrap();
    shop.run_command(doc! { "createIndexes": "customers", "indexes": [
        { "key": { "email": 1 }, "name": "email_unique", "unique": true, "partialFilterExpression": { "email": { "$type": "string" } } },
    ]}).await.unwrap();
    shop.run_command(doc! { "createIndexes": "orders", "indexes": [
        { "key": { "at": 1 }, "name": "ttl", "expireAfterSeconds": 31_536_000 },
    ]})
    .await
    .unwrap();

    let e = env().await;
    let id = mongo_conn(&e, &rs).await;
    let objects = vec![
        ObjectRef::collection("shop", "orders"),
        ObjectRef::collection("shop", "customers"),
    ];

    // GazaBridge backup: files, checksums, index specifications.
    let dir = e
        .backups
        .backup(
            &BackupRequest {
                connection: id,
                objects: objects.clone(),
                method: BackupMethod::GazaBridge,
                label: Some("shop".into()),
            },
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap();
    let m = e.backups.manifest(&dir).await.unwrap();
    assert_eq!(m.objects.len(), 2);
    assert_eq!((m.objects[0].records, m.objects[1].records), (2_000, 300));
    assert!(
        m.objects
            .iter()
            .all(|o| o.sha256.as_ref().is_some_and(|s| s.len() == 64))
    );
    assert_eq!(m.objects[1].indexes[0].name, "email_unique");
    assert_eq!(e.backups.list().await.unwrap().len(), 1);

    // Plan, then restore into another database: identical documents and indexes.
    let req = restore(&dir, id, "restored", ExistingData::Fail);
    let (plan, _) = e.backups.plan_restore(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(plan.items.iter().all(|i| i.verified && !i.target_exists));
    assert_eq!(
        plan.items[0].to,
        ObjectRef::collection("restored", "orders")
    );
    assert_eq!(
        c.database("restored")
            .list_collection_names()
            .await
            .unwrap()
            .len(),
        0,
        "planning wrote nothing"
    );
    let n = e
        .backups
        .restore(&req, false, CancellationToken::new(), &|_| {})
        .await
        .unwrap();
    assert_eq!(n, 2_300);
    for coll in ["orders", "customers"] {
        assert_eq!(
            all(&c, "shop", coll).await,
            all(&c, "restored", coll).await,
            "{coll}"
        );
        assert_eq!(
            index_specs(&c, "shop", coll).await,
            index_specs(&c, "restored", coll).await,
            "{coll}"
        );
    }

    // Again: keep what exists; replacing needs confirmation.
    let n = e
        .backups
        .restore(
            &restore(&dir, id, "restored", ExistingData::Skip),
            false,
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap();
    assert_eq!(n, 2_300, "all kept");
    let err = e
        .backups
        .restore(
            &restore(&dir, id, "restored", ExistingData::Replace),
            false,
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "confirmation_required");

    // A changed file is refused before anything is written.
    let file = dir.join(m.objects[1].file.as_ref().unwrap());
    let mut bytes = std::fs::read(&file).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&file, bytes).unwrap();
    let (plan, _) = e
        .backups
        .plan_restore(&restore(&dir, id, "other", ExistingData::Fail))
        .await
        .unwrap();
    assert!(
        plan.blockers.iter().any(|b| b.contains("checksum")),
        "{:?}",
        plan.blockers
    );
    assert!(
        e.backups
            .restore(
                &restore(&dir, id, "other", ExistingData::Fail),
                false,
                CancellationToken::new(),
                &|_| {}
            )
            .await
            .is_err()
    );
    assert!(
        c.database("other")
            .list_collection_names()
            .await
            .unwrap()
            .is_empty()
    );

    // Deleting: only backups under the backup folder.
    assert!(e.backups.delete(e.dir.path()).await.is_err());
    e.backups.delete(&dir).await.unwrap();
    assert!(!dir.exists() && e.backups.list().await.unwrap().is_empty());

    // mongodump / mongorestore, when installed.
    let info = tools::detect(ToolKind::MongoDump).await;
    if info.path.is_none() || tools::detect(ToolKind::MongoRestore).await.path.is_none() {
        eprintln!("mongodump/mongorestore not installed: native backups not exercised");
    } else {
        let dir = e
            .backups
            .backup(
                &BackupRequest {
                    connection: id,
                    objects: objects.clone(),
                    method: BackupMethod::Native,
                    label: None,
                },
                CancellationToken::new(),
                &|_| {},
            )
            .await
            .unwrap();
        let m = e.backups.manifest(&dir).await.unwrap();
        let archive = m.archive.clone().unwrap();
        assert!(
            archive.command.contains("--config=<temporary file>")
                && archive.command.contains("--excludeCollection=logs"),
            "{}",
            archive.command
        );
        assert_eq!(archive.sha256.len(), 64);
        let req = restore(&dir, id, "native", ExistingData::Fail);
        let (plan, _) = e.backups.plan_restore(&req).await.unwrap();
        assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
        assert!(plan.command.as_deref().unwrap().contains("--nsTo=native.*"));
        e.backups
            .restore(&req, false, CancellationToken::new(), &|_| {})
            .await
            .unwrap();
        for coll in ["orders", "customers"] {
            assert_eq!(
                all(&c, "shop", coll).await,
                all(&c, "native", coll).await,
                "{coll}"
            );
        }
        assert!(
            !c.database("native")
                .list_collection_names()
                .await
                .unwrap()
                .contains(&"logs".to_owned())
        );
    }

    // A copy that replaces its target backs the target up first.
    let migrations = MigrationService::new(
        e.store.clone(),
        e.explorer.clone(),
        e.svc.redactor().clone(),
        e.dir.path().join("q"),
    )
    .with_backups(e.backups.clone());
    let item = CopyItem {
        source: ObjectRef::collection("shop", "customers"),
        target: ObjectRef::collection("restored", "customers"),
    };
    let req = MigrationRequest {
        name: None,
        source: id,
        target: id,
        items: vec![item],
        existing: ExistingData::Replace,
        copy_indexes: true,
        batch_size: 500,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 1,
        doc_mode: Default::default(),
        table_mode: Default::default(),
        backup_before_replace: true,
    };
    assert!(
        migrations
            .plan(&req)
            .await
            .unwrap()
            .warnings
            .iter()
            .any(|w| w.contains("backed up"))
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
    let m = e.backups.manifest(&backup).await.unwrap();
    assert_eq!(
        (
            m.objects[0].object.to_string().as_str(),
            m.objects[0].records
        ),
        ("restored.customers", 300)
    );
}

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

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn postgresql_backups_restore_tables_keys_and_rows_into_another_schema() {
    let pg = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres");
    let e = env().await;
    let id = e
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
    let conn = e.svc.open(id).await.unwrap();
    for s in [
        "CREATE TABLE parents (id serial PRIMARY KEY, name text NOT NULL)",
        "CREATE TABLE children (id bigint PRIMARY KEY, parent_id integer NOT NULL REFERENCES parents (id), amount numeric(12,2), at timestamptz, doc jsonb, tags text[], note text)",
        "CREATE INDEX children_by_parent ON children (parent_id)",
        "INSERT INTO parents (name) SELECT 'p' || g FROM generate_series(1, 50) g",
        "INSERT INTO children SELECT g, 1 + g % 50, g * 1.25, '2026-01-01T00:00:00Z'::timestamptz + g * interval '1 minute', jsonb_build_object('g', g, 'ar', 'نص'), ARRAY['a', 'b'], CASE WHEN g % 3 = 0 THEN NULL ELSE 'n' || g END FROM generate_series(1, 700) g",
    ] {
        sql(&conn, s).await;
    }
    let public =
        |n: &str| ObjectRef::table(NamespacePath::schema(Some("postgres".into()), "public"), n);
    // Children first on purpose: the restore orders tables by their keys.
    let dir = e
        .backups
        .backup(
            &BackupRequest {
                connection: id,
                objects: vec![public("children"), public("parents")],
                method: BackupMethod::GazaBridge,
                label: None,
            },
            CancellationToken::new(),
            &|_| {},
        )
        .await
        .unwrap();
    let m = e.backups.manifest(&dir).await.unwrap();
    assert!(
        m.warnings.iter().any(|w| w.contains("sequence")),
        "{:?}",
        m.warnings
    );
    assert_eq!(
        m.objects[0].table.as_ref().unwrap().foreign_keys[0]
            .referenced
            .name,
        "parents"
    );

    let req = restore(&dir, id, "restored", ExistingData::Fail);
    let (plan, _) = e.backups.plan_restore(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(
        plan.statements
            .iter()
            .any(|s| s.contains("REFERENCES \"restored\".\"parents\"")),
        "{:?}",
        plan.statements
    );
    e.backups
        .restore(&req, false, CancellationToken::new(), &|_| {})
        .await
        .unwrap();
    for t in ["parents", "children"] {
        let a = sql(&conn, &format!("SELECT * FROM public.{t} ORDER BY id")).await;
        let b = sql(&conn, &format!("SELECT * FROM restored.{t} ORDER BY id")).await;
        assert_eq!(a, b, "{t}");
    }
    let types = sql(&conn, "SELECT string_agg(data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'restored' AND table_name = 'children'").await;
    assert_eq!(
        types[0].fields[0].value,
        Value::String("bigint,integer,numeric,timestamp with time zone,jsonb,ARRAY,text".into())
    );
    assert_eq!(sql(&conn, "SELECT 1 FROM pg_indexes WHERE schemaname = 'restored' AND indexname = 'children_by_parent'").await.len(), 1);
    assert_eq!(sql(&conn, "SELECT 1 FROM information_schema.table_constraints WHERE table_schema = 'restored' AND constraint_type = 'FOREIGN KEY'").await.len(), 1);

    // pg_dump is optional; when absent the native method says how to get it.
    if tools::detect(ToolKind::PgDump).await.path.is_none() {
        let err = e
            .backups
            .backup(
                &BackupRequest {
                    connection: id,
                    objects: vec![public("parents")],
                    method: BackupMethod::Native,
                    label: None,
                },
                CancellationToken::new(),
                &|_| {},
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("brew install libpq"), "{err}");
        let left: Vec<_> = std::fs::read_dir(e.backups.root())
            .unwrap()
            .flatten()
            .map(|d| d.file_name())
            .collect();
        assert_eq!(left.len(), 1, "no partial folder left behind: {left:?}");
    }
}
