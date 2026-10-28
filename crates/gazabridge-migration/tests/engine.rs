//! The engine against an in-memory connector that can drop connections,
//! lose acknowledgements and reject records on demand.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_migration::{
    Checkpoints, EngineError, EngineOptions, RecordErrors, Resume, RetryPolicy, StreamSpec,
    StreamStats, copy_stream,
};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits};
use gazabridge_plugin_sdk::connector::HealthReport;
use gazabridge_plugin_sdk::profile::Version;
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest, RecordBatch, RecordStream};
use gazabridge_plugin_sdk::schema::{NamespaceEntry, NamespacePath, ObjectRef};
use gazabridge_plugin_sdk::write::{RecordFailure, WriteBatch, WriteOutcome};
use gazabridge_plugin_sdk::{
    ApiVersion, Connection, ConnectorError, ConnectorResult, ErrorKind, ServerInfo,
};
use tokio_util::sync::CancellationToken;

const N: i64 = 2_500;

#[derive(Debug)]
struct Fake {
    manifest: CapabilityManifest,
    source: Vec<Record>,
    target: Mutex<BTreeMap<i64, Record>>,
    /// Read batches served before the next read drops the connection.
    drop_read_after: Mutex<Option<usize>>,
    /// Writes that fail with a dropped connection before reaching the server.
    fail_writes: AtomicU32,
    /// Writes applied whose acknowledgement is then lost.
    lose_acks: AtomicU32,
    reject: BTreeSet<i64>,
    /// Kind of error for every read, when set.
    broken: Option<ErrorKind>,
    reads_opened: AtomicU32,
}

/// Uses up one injected fault, if any are left.
fn take_one(n: &AtomicU32) -> bool {
    let mut cur = n.load(Ordering::Relaxed);
    while cur > 0 {
        match n.compare_exchange(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

fn id_of(r: &Record) -> i64 {
    match r.get("_id") {
        Value::Int(i) => *i,
        _ => panic!("no _id"),
    }
}

impl Fake {
    fn new() -> Self {
        Self {
            manifest: CapabilityManifest {
                connector_id: "fake".into(),
                family: EngineFamily::MongoDb,
                api_version: ApiVersion { major: 1, minor: 0 },
                capabilities: BTreeSet::new(),
                write_modes: BTreeSet::new(),
                sync_methods: BTreeSet::new(),
                query_languages: BTreeSet::new(),
                limits: ConnectorLimits::default(),
            },
            source: (0..N)
                .map(|i| {
                    Record::from_pairs([
                        ("_id", Value::Int(i)),
                        ("n", Value::String(format!("doc {i}"))),
                    ])
                })
                .collect(),
            target: Mutex::default(),
            drop_read_after: Mutex::new(None),
            fail_writes: AtomicU32::new(0),
            lose_acks: AtomicU32::new(0),
            reject: BTreeSet::new(),
            broken: None,
            reads_opened: AtomicU32::new(0),
        }
    }

    fn target_ids(&self) -> Vec<i64> {
        self.target.lock().unwrap().keys().copied().collect()
    }

    fn apply(&self, batch: &WriteBatch) -> WriteOutcome {
        let mut out = WriteOutcome::default();
        let mut t = self.target.lock().unwrap();
        for (index, r) in batch.records.iter().enumerate() {
            let id = id_of(r);
            if self.reject.contains(&id) {
                out.failures.push(RecordFailure {
                    index,
                    kind: ErrorKind::Constraint,
                    message: "document failed validation".into(),
                    code: Some("121".into()),
                });
                continue;
            }
            match (batch.mode, t.contains_key(&id)) {
                (WriteMode::SkipExisting, true) => out.skipped += 1,
                (WriteMode::Upsert, true) => {
                    t.insert(id, r.clone());
                    out.updated += 1;
                }
                (_, true) => out.failures.push(RecordFailure {
                    index,
                    kind: ErrorKind::Conflict,
                    message: "E11000 duplicate key".into(),
                    code: Some("11000".into()),
                }),
                (_, false) => {
                    t.insert(id, r.clone());
                    out.inserted += 1;
                }
            }
        }
        out
    }
}

#[async_trait]
impl Connection for Fake {
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }
    fn is_read_only(&self) -> bool {
        false
    }
    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        Err(ConnectorError::new(
            ErrorKind::Unsupported,
            "not needed here",
        ))
    }
    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        Ok(ServerInfo {
            family: EngineFamily::MongoDb,
            product: "fake".into(),
            version: Version("1".into()),
            version_string: "1".into(),
            properties: BTreeMap::new(),
        })
    }
    async fn list(&self, _parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        Ok(vec![])
    }
    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        Ok(ident.to_owned())
    }
    async fn close(&self) {}

    async fn read(
        &self,
        request: ReadRequest,
        _cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        self.reads_opened.fetch_add(1, Ordering::Relaxed);
        if let Some(kind) = self.broken {
            return Err(ConnectorError::new(kind, "refused"));
        }
        let start = request
            .resume_after
            .map_or(0, |p| p.token["after"].as_u64().unwrap() as usize + 1);
        let mut batches: Vec<ConnectorResult<RecordBatch>> = Vec::new();
        let mut drop_after = self.drop_read_after.lock().unwrap().take();
        for chunk_start in (start..self.source.len()).step_by(request.batch_size) {
            if drop_after == Some(0) {
                batches.push(Err(ConnectorError::new(
                    ErrorKind::Connection,
                    "connection reset",
                )));
                break;
            }
            drop_after = drop_after.map(|d| d - 1);
            let end = (chunk_start + request.batch_size).min(self.source.len());
            batches.push(Ok(RecordBatch::new(
                self.source[chunk_start..end].to_vec(),
                Some(ReadPosition {
                    connector_id: "fake".into(),
                    token: serde_json::json!({ "after": end - 1 }),
                }),
            )));
        }
        Ok(Box::pin(futures::stream::iter(batches)))
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        _cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        if take_one(&self.fail_writes) {
            return Err(ConnectorError::new(
                ErrorKind::Connection,
                "connection reset",
            ));
        }
        let out = self.apply(&batch);
        if take_one(&self.lose_acks) {
            return Err(ConnectorError::new(ErrorKind::Timeout, "no reply"));
        }
        Ok(out)
    }
}

#[derive(Default)]
struct Saved(Mutex<Vec<(Option<ReadPosition>, StreamStats)>>);

#[async_trait]
impl Checkpoints for Saved {
    async fn save(
        &self,
        _stream: &str,
        position: Option<&ReadPosition>,
        stats: &StreamStats,
    ) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .push((position.cloned(), stats.clone()));
        Ok(())
    }
}

impl Saved {
    fn last(&self) -> Resume {
        let (position, stats) = self.0.lock().unwrap().last().cloned().unwrap();
        Resume { position, stats }
    }
}

fn spec(mode: WriteMode) -> StreamSpec {
    StreamSpec {
        key: "shop.orders".into(),
        source: ObjectRef::collection("shop", "orders"),
        target: ObjectRef::collection("copy", "orders"),
        filter: None,
        mode,
        key_fields: vec!["_id".into()],
    }
}

fn options() -> EngineOptions {
    EngineOptions {
        batch_size: 1_000,
        queue_depth: 2,
        retry: RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        },
        on_record_error: RecordErrors::Fail,
        max_records_per_sec: None,
    }
}

fn all_ids() -> Vec<i64> {
    (0..N).collect()
}

#[tokio::test]
async fn copies_everything_in_bounded_batches_with_a_checkpoint_after_each() {
    let fake = Arc::new(Fake::new());
    let saved = Saved::default();
    let out = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        None,
        CancellationToken::new(),
        &saved,
        &|_| {},
    )
    .await
    .unwrap();
    assert_eq!(fake.target_ids(), all_ids());
    assert!(!out.stopped);
    assert_eq!(
        (out.stats.read, out.stats.inserted, out.stats.failed),
        (2_500, 2_500, 0)
    );
    let counts: Vec<u64> = saved
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|(_, s)| s.processed())
        .collect();
    assert_eq!(counts, [1_000, 2_000, 2_500]);
}

#[tokio::test]
async fn dropped_connections_and_lost_acks_are_retried_without_duplicates() {
    let fake = Arc::new(Fake::new());
    *fake.drop_read_after.lock().unwrap() = Some(1);
    fake.fail_writes.store(1, Ordering::Relaxed);
    // The second batch reaches the server but its reply is lost: the retry
    // must skip what is already there instead of failing on duplicates.
    fake.lose_acks.store(1, Ordering::Relaxed);
    let out = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
    )
    .await
    .unwrap();
    assert_eq!(fake.target_ids(), all_ids());
    assert_eq!(out.stats.failed, 0);
    assert_eq!(out.stats.inserted + out.stats.skipped, 2_500);
    assert!(out.stats.retries >= 3, "{:?}", out.stats);
    assert_eq!(
        fake.reads_opened.load(Ordering::Relaxed),
        2,
        "the read reopened once, from its position"
    );
}

#[tokio::test]
async fn a_stopped_copy_resumes_without_losing_or_duplicating_records() {
    let fake = Arc::new(Fake::new());
    let saved = Saved::default();
    let stop = CancellationToken::new();
    let s = stop.clone();
    let out = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        None,
        stop,
        &saved,
        &move |_| s.cancel(),
    )
    .await
    .unwrap();
    assert!(out.stopped);
    assert_eq!(fake.target_ids().len(), 1_000);

    // The process died after writing the next batch but before its
    // checkpoint: those records are in the target, the checkpoint says 1000.
    let next: Vec<Record> = fake.source[1_000..2_000].to_vec();
    fake.apply(&WriteBatch {
        object: ObjectRef::collection("copy", "orders"),
        mode: WriteMode::Insert,
        records: next,
        key_fields: vec![],
        atomic: false,
    });

    let out = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        Some(saved.last()),
        CancellationToken::new(),
        &saved,
        &|_| {},
    )
    .await
    .unwrap();
    assert!(!out.stopped);
    assert_eq!(fake.target_ids(), all_ids());
    assert_eq!(out.stats.failed, 0);
    // Totals carry over: 1000 before, then 1000 found already there, 500 new.
    assert_eq!(
        (out.stats.read, out.stats.inserted, out.stats.skipped),
        (2_500, 1_500, 1_000)
    );
}

#[tokio::test]
async fn rejected_records_stop_the_copy_or_go_to_quarantine() {
    let mut f = Fake::new();
    f.reject = BTreeSet::from([5, 1_234]);
    let fake = Arc::new(f);
    let err = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err.error, EngineError::Rejected(ref m) if m.contains("_id=5") && m.contains("validation")),
        "{}",
        err.error
    );

    let fake = {
        let mut f = Fake::new();
        f.reject = BTreeSet::from([5, 1_234]);
        Arc::new(f)
    };
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.on_record_error = RecordErrors::Quarantine {
        dir: dir.path().to_path_buf(),
    };
    let out = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &opts,
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
    )
    .await
    .unwrap();
    assert_eq!((out.stats.inserted, out.stats.failed), (2_498, 2));
    assert_eq!(out.rejections.len(), 2);
    let text = std::fs::read_to_string(out.quarantine.unwrap()).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(
        text.contains("doc 5") && text.contains("doc 1234"),
        "{text}"
    );
}

#[tokio::test]
async fn permanent_errors_fail_at_once() {
    let mut f = Fake::new();
    f.broken = Some(ErrorKind::Authentication);
    let fake = Arc::new(f);
    let err = copy_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &options(),
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(err.error, EngineError::Source(_)));
    assert_eq!(fake.reads_opened.load(Ordering::Relaxed), 1);
    assert!(fake.target_ids().is_empty());
}
