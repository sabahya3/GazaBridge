//! MongoDB → MongoDB copies between two real servers: exact documents and
//! indexes, conflict modes, pause/resume, recovery after a restart, and
//! quarantine of rejected documents.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test helpers outside #[test] functions

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use gazabridge_application::{
    ConnectionService, CopyItem, ExistingData, ExplorerService, MigrationRequest, MigrationService,
    OnRecordError, PasswordInput, RunView, StreamPhase,
};
use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, HostPort, MongoSettings, ReadPreference, SecretRef,
};
use gazabridge_domain::ids::{ConnectionId, RunId};
use gazabridge_domain::job::JobStatus;
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
use gazabridge_storage::MetadataStore;
use mongodb::bson::{
    Binary, Bson, DateTime, Decimal128, Document, doc, oid::ObjectId, spec::BinarySubtype,
};

struct Env {
    store: MetadataStore,
    svc: ConnectionService,
    quarantine: tempfile::TempDir,
}

impl Env {
    async fn new() -> Self {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
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
        Self {
            store,
            svc,
            quarantine: tempfile::tempdir().unwrap(),
        }
    }

    /// A fresh service over the same metadata, as after an app restart.
    fn migrations(&self) -> MigrationService {
        MigrationService::new(
            self.store.clone(),
            ExplorerService::new(self.store.clone(), self.svc.clone()),
            self.svc.redactor().clone(),
            self.quarantine.path().to_path_buf(),
        )
    }

    async fn connect(
        &self,
        name: &str,
        rs: &common::mongo::ReplicaSet,
        read_only: bool,
    ) -> ConnectionId {
        let mut p = ConnectionProfile::new(
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
        );
        p.read_only = read_only;
        self.svc.create(p, PasswordInput::None).await.unwrap().id
    }
}

async fn client(rs: &common::mongo::ReplicaSet) -> mongodb::Client {
    mongodb::Client::with_uri_str(format!(
        "mongodb://{}:{}/?directConnection=true",
        rs.host, rs.port
    ))
    .await
    .unwrap()
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

/// Equal document lists, reporting only the first difference.
/// Byte-exact: field order and BSON types count (`Document` equality
/// ignores order).
fn same(a: &[Document], b: &[Document]) {
    assert_eq!(a.len(), b.len(), "document counts differ");
    let bytes = |d: &Document| mongodb::bson::to_vec(d).unwrap();
    if let Some((x, y)) = a.iter().zip(b).find(|(x, y)| bytes(x) != bytes(y)) {
        panic!("first difference:\nsource: {x:?}\ntarget: {y:?}");
    }
}

/// Index specifications by name, without the server-specific version field.
async fn indexes(c: &mongodb::Client, db: &str, coll: &str) -> Vec<Document> {
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

fn item(src_db: &str, coll: &str, dst_db: &str) -> CopyItem {
    CopyItem {
        source: ObjectRef::collection(src_db, coll),
        target: ObjectRef::collection(dst_db, coll),
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
        batch_size: 700,
        on_record_error: OnRecordError::Stop,
        max_records_per_sec: None,
        parallel: 2,
    }
}

async fn wait(m: &MigrationService, run: RunId, until: impl Fn(&RunView) -> bool) -> RunView {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let v = m.view(run).await.unwrap();
        if until(&v) {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out: {v:#?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn settled(v: &RunView) -> bool {
    v.status.is_terminal() || v.status == JobStatus::Paused
}

fn order(i: i32) -> Document {
    doc! {
        "_id": ObjectId::new(),
        "n32": i,
        "n64": i64::from(i) * 10_000_000_000,
        "price": Decimal128::from_bytes([u8::try_from(i % 200).unwrap(); 16]),
        "at": DateTime::from_millis(1_767_225_600_000 + i64::from(i)),
        "customer": format!("c{}", i % 300),
        "lines": [ { "sku": "A", "qty": 1 }, { "sku": "B", "qty": i } ],
        "note": if i % 7 == 0 { Bson::Null } else { Bson::String(format!("نص {i}")) },
        "raw": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, u8::try_from(i % 250).unwrap()] },
    }
}

#[tokio::test]
#[ignore = "needs Docker; run with cargo test -- --ignored"]
async fn mongodb_collections_copy_between_servers_exactly_and_resumably() {
    let (src_rs, dst_rs) = tokio::join!(common::mongo::start("8.0"), common::mongo::start("8.0"));
    let (src, dst) = (client(&src_rs).await, client(&dst_rs).await);
    let shop = src.database("shop");
    let orders: Vec<Document> = (0..5_000).map(order).collect();
    shop.collection::<Document>("orders")
        .insert_many(orders)
        .await
        .unwrap();
    let customers: Vec<Document> = (0..300)
        .map(|i| doc! { "_id": format!("c{i}"), "email": format!("c{i}@example.test"), "notes": "regular customer" })
        .collect();
    shop.collection::<Document>("customers")
        .insert_many(customers)
        .await
        .unwrap();
    shop.run_command(doc! { "createIndexes": "orders", "indexes": [
        { "key": { "customer": 1, "at": -1 }, "name": "by_customer" },
        { "key": { "at": 1 }, "name": "ttl", "expireAfterSeconds": 31_536_000 },
    ]})
    .await
    .unwrap();
    shop.run_command(doc! { "createIndexes": "customers", "indexes": [
        { "key": { "email": 1 }, "name": "email_unique", "unique": true,
          "partialFilterExpression": { "email": { "$type": "string" } } },
        { "key": { "notes": "text" }, "name": "notes_text", "weights": { "notes": 3 } },
    ]})
    .await
    .unwrap();
    // The target already holds a different version of c1 and its own c999.
    dst.database("copy")
        .collection::<Document>("customers")
        .insert_many([
            doc! { "_id": "c1", "email": "kept@example.test", "notes": "target version" },
            doc! { "_id": "c999", "email": "extra@example.test" },
        ])
        .await
        .unwrap();

    let env = Env::new().await;
    let s = env.connect("source", &src_rs, true).await;
    let t = env.connect("target", &dst_rs, false).await;
    let m = env.migrations();

    // Plan: reads only, says what will happen.
    let req = request(
        s,
        t,
        vec![
            item("shop", "orders", "copy"),
            item("shop", "customers", "copy"),
        ],
    );
    let plan = m.plan(&req).await.unwrap();
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    assert!(!plan.destructive);
    assert_eq!(plan.items[0].source_count, Some(5_000));
    assert_eq!(plan.items[0].indexes, ["by_customer", "ttl"]);
    assert!(!plan.items[0].target_exists);
    assert_eq!(
        (plan.items[1].target_exists, plan.items[1].target_count),
        (true, Some(2))
    );
    assert!(
        plan.items[1].warnings.iter().any(|w| w.contains("kept")),
        "{:?}",
        plan.items[1].warnings
    );
    assert!(
        plan.items[1]
            .statements
            .iter()
            .any(|s| s.contains("partialFilterExpression")),
        "{:?}",
        plan.items[1].statements
    );
    assert!(
        all(&dst, "copy", "orders").await.is_empty(),
        "planning wrote nothing"
    );

    // Copy, keeping what the target already has.
    let run = m.start(req.clone(), false).await.unwrap();
    let v = wait(&m, run, settled).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert!(v.streams.iter().all(|s| s.phase == StreamPhase::Done));
    assert_eq!(v.streams[0].stats.inserted, 5_000);
    assert_eq!(
        (v.streams[1].stats.inserted, v.streams[1].stats.skipped),
        (299, 1)
    );
    assert_eq!(v.streams[0].indexes_created, 2);

    // Same documents, same BSON types (Int32 stays Int32, Decimal128, dates, binary).
    same(
        &all(&src, "shop", "orders").await,
        &all(&dst, "copy", "orders").await,
    );
    let copied = all(&dst, "copy", "customers").await;
    assert_eq!(copied.len(), 301);
    assert_eq!(
        copied
            .iter()
            .find(|d| d.get_str("_id") == Ok("c1"))
            .unwrap()
            .get_str("notes"),
        Ok("target version")
    );
    // Same index specifications: compound order, TTL, unique + partial, text weights.
    same(
        &indexes(&src, "shop", "orders").await,
        &indexes(&dst, "copy", "orders").await,
    );
    same(
        &indexes(&src, "shop", "customers").await,
        &indexes(&dst, "copy", "customers").await,
    );

    // Guards: read-only targets and copying a collection onto itself.
    let ro = env.connect("ro-target", &dst_rs, true).await;
    let plan = m
        .plan(&request(s, ro, vec![item("shop", "orders", "other")]))
        .await
        .unwrap();
    assert!(
        plan.blockers.iter().any(|b| b.contains("read-only")),
        "{:?}",
        plan.blockers
    );
    let plan = m
        .plan(&request(s, s, vec![item("shop", "orders", "shop")]))
        .await
        .unwrap();
    assert!(
        plan.blockers.iter().any(|b| b.contains("onto itself")),
        "{:?}",
        plan.blockers
    );

    // Replace drops the target first, and only with explicit confirmation.
    let mut replace = request(s, t, vec![item("shop", "customers", "copy")]);
    replace.existing = ExistingData::Replace;
    assert!(m.plan(&replace).await.unwrap().destructive);
    let err = m.start(replace.clone(), false).await.unwrap_err();
    assert_eq!(err.code(), "confirmation_required");
    assert_eq!(
        all(&dst, "copy", "customers").await.len(),
        301,
        "nothing dropped without confirmation"
    );
    let run = m.start(replace, true).await.unwrap();
    assert_eq!(wait(&m, run, settled).await.status, JobStatus::Completed);
    same(
        &all(&src, "shop", "customers").await,
        &all(&dst, "copy", "customers").await,
    );

    // Pause, then a crash while resuming, then a restart and a resume.
    let mut slow = request(s, t, vec![item("shop", "orders", "copy2")]);
    slow.batch_size = 100;
    slow.max_records_per_sec = Some(4_000);
    let run = m.start(slow, false).await.unwrap();
    wait(&m, run, |v| v.streams[0].stats.inserted >= 300).await;
    m.pause(run).await.unwrap();
    let v = wait(&m, run, settled).await;
    assert_eq!(v.status, JobStatus::Paused);
    let copied_before = v.streams[0].stats.processed();
    assert!(
        copied_before > 0 && copied_before < 5_000,
        "{copied_before}"
    );
    let jobs = env.store.jobs();
    jobs.transition_run(run, JobStatus::Resuming).await.unwrap();
    jobs.transition_run(run, JobStatus::Running).await.unwrap();
    let restarted = env.migrations();
    assert_eq!(restarted.recover_interrupted().await.unwrap(), 1);
    assert_eq!(restarted.view(run).await.unwrap().status, JobStatus::Paused);
    restarted.resume(run).await.unwrap();
    let v = wait(&restarted, run, settled).await;
    assert_eq!(v.status, JobStatus::Completed, "{v:#?}");
    assert_eq!(v.streams[0].stats.processed(), 5_000);
    same(
        &all(&src, "shop", "orders").await,
        &all(&dst, "copy2", "orders").await,
    );
    assert!(
        v.events.iter().any(|e| e.message.contains("resume")),
        "{:?}",
        v.events
    );

    // Rejected documents go to quarantine; the rest is copied.
    let strict = dst.database("strict");
    strict
        .run_command(doc! { "create": "customers", "validator": { "email": { "$regex": "^c1" } } })
        .await
        .unwrap();
    let mut q = request(s, t, vec![item("shop", "customers", "strict")]);
    q.on_record_error = OnRecordError::Quarantine;
    q.copy_indexes = false;
    let run = m.start(q, false).await.unwrap();
    let v = wait(&m, run, settled).await;
    assert_eq!(v.status, JobStatus::CompletedWithWarnings, "{v:#?}");
    let accepted = all(&dst, "strict", "customers").await.len() as u64;
    assert_eq!(v.streams[0].stats.inserted, accepted);
    assert_eq!(v.streams[0].stats.failed, 300 - accepted);
    let file = v.streams[0].quarantine.clone().unwrap();
    assert!(file.starts_with(env.quarantine.path()));
    assert_eq!(
        std::fs::read_to_string(file).unwrap().lines().count() as u64,
        300 - accepted
    );
    assert!(!v.streams[0].rejections.is_empty());

    // The history lists every run, newest first.
    let recent = m.recent(10).await.unwrap();
    assert_eq!(recent.len(), 4);
    assert_eq!(recent[0].run_id, run);
}
