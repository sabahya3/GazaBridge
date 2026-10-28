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
use gazabridge_migration::validate::{Comparison, DiffKind, Level, validate_stream};
use gazabridge_migration::{
    Checkpoints, EngineError, EngineOptions, RecordErrors, Resume, RetryPolicy, StreamSpec,
    StreamStats, TargetSpec, Transform, Transformed, convert_stream, copy_stream,
};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits};
use gazabridge_plugin_sdk::connector::HealthReport;
use gazabridge_plugin_sdk::filter::Filter;
use gazabridge_plugin_sdk::profile::Version;
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest, RecordBatch, RecordStream};
use gazabridge_plugin_sdk::schema::{NamespaceEntry, NamespacePath, ObjectRef};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, RecordFailure, WriteBatch, WriteOutcome,
};
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
    /// Child rows of the `lines` target, by (`order`, `index`).
    lines: Mutex<BTreeMap<(i64, i64), Record>>,
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

fn id_of_field(r: &Record, field: &str) -> i64 {
    match r.get(field) {
        Value::Int(i) => *i,
        _ => panic!("no {field}"),
    }
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
            lines: Mutex::default(),
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

    fn apply_lines(&self, batch: &WriteBatch) -> WriteOutcome {
        let mut out = WriteOutcome::default();
        let mut t = self.lines.lock().unwrap();
        for (index, r) in batch.records.iter().enumerate() {
            let Value::Int(qty) = r.get("qty") else {
                panic!()
            };
            if *qty < 0 {
                out.failures.push(RecordFailure {
                    index,
                    kind: ErrorKind::Constraint,
                    message: "qty must not be negative".into(),
                    code: None,
                });
                continue;
            }
            let key = (id_of_field(r, "order"), id_of_field(r, "index"));
            if t.insert(key, r.clone()).is_some() && batch.mode == WriteMode::Upsert {
                out.updated += 1;
            } else {
                out.inserted += 1;
            }
        }
        out
    }

    fn apply(&self, batch: &WriteBatch) -> WriteOutcome {
        if batch.object.name == "lines" {
            return self.apply_lines(batch);
        }
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
        if request.object.namespace.database.as_deref() == Some("copy") {
            // Target lookups (dry runs, validation): `IN` on the key or owner.
            let Some(Filter::In { field, values }) = request.filter else {
                panic!("target reads are lookups")
            };
            let wanted: BTreeSet<i64> = values
                .iter()
                .map(|v| match v {
                    Value::Int(i) => *i,
                    _ => panic!(),
                })
                .collect();
            let rows: Vec<Record> = if request.object.name == "lines" {
                assert_eq!(field.0, "order");
                self.lines
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|((o, _), _)| wanted.contains(o))
                    .map(|(_, r)| r.clone())
                    .collect()
            } else {
                self.target
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(id, _)| wanted.contains(id))
                    .map(|(_, r)| r.clone())
                    .collect()
            };
            return Ok(Box::pin(futures::stream::iter([Ok(RecordBatch::new(
                rows, None,
            ))])));
        }
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

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        let Filter::In { values, .. } = request.filter else {
            panic!("unexpected filter")
        };
        let owners: BTreeSet<i64> = values
            .iter()
            .map(|v| match v {
                Value::Int(i) => *i,
                _ => panic!(),
            })
            .collect();
        let removed = if request.object.name == "lines" {
            let mut t = self.lines.lock().unwrap();
            let before = t.len();
            t.retain(|(o, _), _| !owners.contains(o));
            before - t.len()
        } else {
            let mut t = self.target.lock().unwrap();
            let before = t.len();
            t.retain(|id, _| !owners.contains(id));
            before - t.len()
        };
        Ok(MutationOutcome {
            matched: removed as u64,
            modified: removed as u64,
        })
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
        source_keys: vec![],
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
        dry_run: false,
        assume_empty_targets: false,
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

/// Each order becomes a row in `orders` plus `n % 3` rows in `lines`.
struct Split {
    replace: bool,
}

#[async_trait]
impl Transform for Split {
    fn targets(&self) -> Vec<TargetSpec> {
        vec![
            TargetSpec {
                object: ObjectRef::collection("copy", "orders"),
                key_fields: vec!["_id".into()],
                owner: Some("_id".into()),
                replace: false,
            },
            TargetSpec {
                object: ObjectRef::collection("copy", "lines"),
                key_fields: vec!["order".into(), "index".into()],
                owner: Some("order".into()),
                replace: self.replace,
            },
        ]
    }

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError> {
        let mut t = Transformed {
            rows: vec![Vec::new(), Vec::new()],
            ..Transformed::default()
        };
        for (i, r) in records.iter().enumerate() {
            let id = id_of(r);
            if id == 13 {
                t.rejected.push((i, "cannot convert".into()));
                continue;
            }
            t.owners.push((i, Value::Int(id)));
            t.rows[0].push((i, r.clone()));
            for k in 0..id % 3 {
                // Order 41's second line is invalid in the target.
                let qty = if id == 41 && k == 1 { -1 } else { 1 };
                t.rows[1].push((
                    i,
                    Record::from_pairs([
                        ("order", Value::Int(id)),
                        ("index", Value::Int(k)),
                        ("qty", Value::Int(qty)),
                    ]),
                ));
            }
        }
        Ok(t)
    }
}

#[tokio::test]
async fn conversions_write_parents_and_children_and_reject_whole_records() {
    let fake = Arc::new(Fake::new());
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.on_record_error = RecordErrors::Quarantine {
        dir: dir.path().to_path_buf(),
    };
    let out = convert_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Insert),
        &opts,
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
        Arc::new(Split { replace: false }),
    )
    .await
    .unwrap();
    // 13 failed to convert; 41 was rejected in a child table after its
    // parent row was written: both are quarantined as whole documents.
    assert_eq!(out.stats.read, 2_500);
    assert_eq!(out.stats.failed, 2);
    assert_eq!(out.rejections.len(), 2);
    assert!(
        out.rejections
            .iter()
            .any(|r| r.contains("_id=41") && r.contains("negative")),
        "{:?}",
        out.rejections
    );
    let text = std::fs::read_to_string(out.quarantine.unwrap()).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(!fake.target_ids().contains(&13));
    assert!(!fake.target_ids().contains(&41), "no half-written document");
    let lines = fake.lines.lock().unwrap().len() as i64;
    let expected: i64 = (0..N).filter(|i| *i != 13 && *i != 41).map(|i| i % 3).sum();
    assert_eq!(lines, expected, "order 41 left no lines behind");

    // Replacing parents first deletes their old child rows.
    fake.lines.lock().unwrap().insert(
        (2, 9),
        Record::from_pairs([
            ("order", Value::Int(2)),
            ("index", Value::Int(9)),
            ("qty", Value::Int(1)),
        ]),
    );
    convert_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::Upsert),
        &opts,
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
        Arc::new(Split { replace: true }),
    )
    .await
    .unwrap();
    assert!(
        !fake.lines.lock().unwrap().contains_key(&(2, 9)),
        "stale child row removed"
    );
}

#[tokio::test]
async fn dry_runs_predict_every_outcome_and_write_nothing() {
    let fake = Arc::new(Fake::new());
    // The target already holds orders 0..100.
    fake.apply(&WriteBatch {
        object: ObjectRef::collection("copy", "orders"),
        mode: WriteMode::Insert,
        records: fake.source[..100].to_vec(),
        key_fields: vec![],
        atomic: false,
    });
    let before = fake.target_ids();
    let mut opts = options();
    opts.dry_run = true;
    for (mode, expect) in [
        (WriteMode::SkipExisting, (2_400, 0, 100, 0)),
        (WriteMode::Upsert, (2_400, 100, 0, 0)),
        (WriteMode::Insert, (2_400, 0, 0, 100)),
    ] {
        let saved = Saved::default();
        let out = copy_stream(
            fake.clone(),
            fake.clone(),
            &spec(mode),
            &opts,
            None,
            CancellationToken::new(),
            &saved,
            &|_| {},
        )
        .await
        .unwrap();
        let s = &out.stats;
        assert_eq!(
            (s.inserted, s.updated, s.skipped, s.failed),
            expect,
            "{mode:?}"
        );
        assert_eq!(s.read, 2_500);
        assert!(saved.0.lock().unwrap().is_empty(), "no checkpoints");
        assert!(out.quarantine.is_none());
        if mode == WriteMode::Insert {
            assert!(
                out.rejections[0].contains("already holds this key"),
                "{:?}",
                out.rejections
            );
        }
    }
    assert_eq!(fake.target_ids(), before, "nothing written");

    // Records that cannot be converted are reported, children are probed.
    let out = convert_stream(
        fake.clone(),
        fake.clone(),
        &spec(WriteMode::SkipExisting),
        &opts,
        None,
        CancellationToken::new(),
        &Saved::default(),
        &|_| {},
        Arc::new(Split { replace: false }),
    )
    .await
    .unwrap();
    assert_eq!(out.stats.failed, 1, "order 13 cannot be converted");
    assert!(fake.lines.lock().unwrap().is_empty(), "nothing written");
}

#[tokio::test]
async fn validation_finds_missing_changed_and_extra_rows() {
    let fake = Arc::new(Fake::new());
    copy_stream(
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
    let identity = || {
        Arc::new(gazabridge_migration::Identity(TargetSpec {
            object: ObjectRef::collection("copy", "orders"),
            key_fields: vec!["_id".into()],
            owner: None,
            replace: false,
        })) as Arc<dyn Transform>
    };
    let check = |level| {
        let fake = fake.clone();
        let t = identity();
        async move {
            validate_stream(
                fake.clone(),
                fake,
                &ObjectRef::collection("shop", "orders"),
                t,
                level,
                Comparison::Exact,
                700,
                CancellationToken::new(),
                &|_| {},
            )
            .await
            .unwrap()
        }
    };
    let v = check(Level::Deep).await;
    assert!(v.passed, "{v:#?}");
    assert_eq!(
        (v.checked, v.targets[0].matched, v.targets[0].chunks),
        (2_500, 2_500, 4)
    );
    assert_eq!(v.targets[0].expected_checksum, v.targets[0].actual_checksum);

    {
        let mut t = fake.target.lock().unwrap();
        t.remove(&10);
        t.insert(
            20,
            Record::from_pairs([
                ("_id", Value::Int(20)),
                ("n", Value::String("changed".into())),
            ]),
        );
    }
    let v = check(Level::Deep).await;
    assert!(!v.passed);
    let c = &v.targets[0];
    assert_eq!(
        (c.missing, c.different, c.matched, c.mismatched_chunks),
        (1, 1, 2_498, 1)
    );
    assert_ne!(c.expected_checksum, c.actual_checksum);
    let changed = v
        .differences
        .iter()
        .find(|d| d.kind == DiffKind::Different)
        .unwrap();
    assert!(
        changed.detail.as_deref().unwrap().contains("`n`"),
        "{changed:?}"
    );
    // Keys only: the changed content is not looked at.
    let v = check(Level::Standard).await;
    assert_eq!((v.targets[0].missing, v.targets[0].different), (1, 0));
}
