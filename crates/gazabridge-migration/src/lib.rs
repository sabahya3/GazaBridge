//! The migration engine: copies one object (a *stream*) from a source
//! connection to a target connection.
//!
//! ```text
//! read ──▶ bounded queue ──▶ write ──▶ checkpoint
//! ```
//!
//! * The reader runs at most `queue_depth` batches ahead of the writer
//!   (backpressure), so memory stays bounded whatever the object's size.
//! * A checkpoint is saved only after the target acknowledged a batch, so a
//!   resumed copy never skips records.
//! * The batch after the last checkpoint may already be in the target (the
//!   process stopped between the write and the checkpoint). That batch, and
//!   every retried batch, is written idempotently (skip existing by key), so
//!   a resumed or retried copy never duplicates records either.
//! * Dropped connections, timeouts and transient server errors are retried
//!   with exponential backoff; reads continue from the last position.
//! * Records the target rejects stop the copy, or go to a quarantine file
//!   that can be imported again once fixed.

mod quarantine;
pub mod transform;
pub mod validate;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use gazabridge_domain::conversion::scalar_to_text;
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter};
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest, RecordBatch};
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_plugin_sdk::write::{DeleteRequest, WriteBatch};
use gazabridge_plugin_sdk::{Connection, ConnectorError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::quarantine::Quarantine;
pub use crate::transform::{Identity, TargetSpec, Transform, Transformed};

/// Rejection messages kept per stream; the rest are only counted.
const MAX_REJECTIONS: usize = 20;

/// One object to copy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpec {
    /// Stable name for checkpoints and reports, e.g. `shop.orders`.
    pub key: String,
    pub source: ObjectRef,
    pub target: ObjectRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    /// Insert, append, upsert, skip-existing or fail-on-conflict. Replace
    /// and truncate are prepared by the caller before the copy starts.
    pub mode: WriteMode,
    /// Identity of a record in the target (MongoDB: `_id`). Needed for
    /// idempotent retries and resumes, and for upserts.
    pub key_fields: Vec<String>,
    /// Identity of a record in the source, for messages about rejected
    /// records; defaults to `key_fields`.
    #[serde(default)]
    pub source_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Attempts per batch (and per read), the first one included.
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 6,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// Exponential backoff before retry number `attempt` (1-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

/// What happens to records the target rejects (duplicate keys, validation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordErrors {
    /// Stop the copy at the first rejected record.
    Fail,
    /// Write rejected records to a JSONL file in `dir` and continue.
    Quarantine { dir: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineOptions {
    pub batch_size: usize,
    /// Batches the reader may hold ahead of the writer.
    pub queue_depth: usize,
    pub retry: RetryPolicy,
    pub on_record_error: RecordErrors,
    /// Throttle, in records per second, to spare a busy server.
    pub max_records_per_sec: Option<u64>,
    /// Read and convert everything but write nothing: the stats say what
    /// would be inserted, kept, replaced or rejected. No checkpoints, no
    /// quarantine file, no deletes.
    #[serde(default)]
    pub dry_run: bool,
    /// Dry runs: the targets will be dropped first (replace), so nothing in
    /// them counts.
    #[serde(default)]
    pub assume_empty_targets: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            queue_depth: 4,
            retry: RetryPolicy::default(),
            on_record_error: RecordErrors::Fail,
            max_records_per_sec: None,
            dry_run: false,
            assume_empty_targets: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStats {
    pub read: u64,
    pub inserted: u64,
    pub updated: u64,
    /// Already present in the target (skip-existing, resumed batches).
    pub skipped: u64,
    /// Rejected by the target (quarantined or stopping the copy).
    pub failed: u64,
    pub bytes: u64,
    pub retries: u64,
}

impl StreamStats {
    /// Records the target has acknowledged one way or another.
    pub fn processed(&self) -> u64 {
        self.inserted + self.updated + self.skipped + self.failed
    }
}

/// Where a stopped copy continues.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resume {
    pub position: Option<ReadPosition>,
    pub stats: StreamStats,
}

/// Durable progress storage.
#[async_trait]
pub trait Checkpoints: Send + Sync {
    /// Called after the target acknowledged every record up to `position`.
    async fn save(
        &self,
        stream: &str,
        position: Option<&ReadPosition>,
        stats: &StreamStats,
    ) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Outcome {
    pub stats: StreamStats,
    pub position: Option<ReadPosition>,
    /// Stopped on request (pause or cancel) before the end of the source.
    pub stopped: bool,
    pub quarantine: Option<PathBuf>,
    /// First rejected records, with their key and the target's message.
    pub rejections: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("reading the source failed: {0}")]
    Source(ConnectorError),
    #[error("writing to the target failed: {0}")]
    Target(ConnectorError),
    #[error("converting records failed: {0}")]
    Transform(ConnectorError),
    #[error("the target rejected {0}")]
    Rejected(String),
    #[error("saving the checkpoint failed: {0}")]
    Checkpoint(String),
    #[error("writing the quarantine file failed: {0}")]
    Quarantine(#[from] gazabridge_files::FileError),
}

/// A failed copy with the progress made before the failure.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct Failure {
    pub error: EngineError,
    pub outcome: Outcome,
}

/// Insert-like modes turn into skip-existing so writing a batch twice is
/// harmless. Without key fields nothing can be matched, so nothing changes.
fn idempotent(mode: WriteMode, keys: &[String]) -> WriteMode {
    match mode {
        WriteMode::Insert | WriteMode::Append | WriteMode::FailOnConflict if !keys.is_empty() => {
            WriteMode::SkipExisting
        }
        other => other,
    }
}

fn describe(records: &[Record], keys: &[String], index: usize, message: &str) -> String {
    let id = records
        .get(index)
        .map(|r| {
            keys.iter()
                .map(|k| {
                    let v = r.get(k);
                    format!(
                        "{k}={}",
                        scalar_to_text(v).unwrap_or_else(|| format!("{v:?}"))
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("#{index}"));
    format!("record {id}: {message}")
}

type Item = Result<RecordBatch, ConnectorError>;

struct Reader {
    source: Arc<dyn Connection>,
    spec: StreamSpec,
    batch_size: usize,
    retry: RetryPolicy,
    position: Option<ReadPosition>,
    retries: Arc<AtomicU64>,
}

impl Reader {
    /// Streams batches into `tx`, reopening the read after transient
    /// failures from the last position received.
    async fn run(mut self, tx: mpsc::Sender<Item>, stop: CancellationToken) {
        let mut attempt = 0u32;
        let mut read_any = false;
        loop {
            let mut req = ReadRequest::new(self.spec.source.clone(), self.batch_size);
            req.filter = self.spec.filter.clone();
            req.resume_after = self.position.clone();
            let failure = match self.source.read(req, stop.clone()).await {
                Err(e) => e,
                Ok(mut stream) => {
                    let mut failed = None;
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(batch) => {
                                attempt = 0;
                                read_any = true;
                                self.position = batch.position.clone();
                                if tx.send(Ok(batch)).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                failed = Some(e);
                                break;
                            }
                        }
                    }
                    match failed {
                        None => return,
                        Some(e) => e,
                    }
                }
            };
            // Reopening is only safe from a known position.
            let resumable = !read_any || self.position.is_some();
            if stop.is_cancelled()
                || !failure.is_retryable()
                || !resumable
                || attempt + 1 >= self.retry.max_attempts
            {
                let _ = tx.send(Err(failure)).await;
                return;
            }
            attempt += 1;
            self.retries.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(stream = %self.spec.key, attempt, error = %failure, "read failed; retrying");
            tokio::select! {
                () = tokio::time::sleep(self.retry.delay(attempt)) => {}
                () = stop.cancelled() => return,
            }
        }
    }
}

struct Writer<'a> {
    target: Arc<dyn Connection>,
    spec: &'a StreamSpec,
    options: &'a EngineOptions,
    checkpoints: &'a dyn Checkpoints,
    on_progress: &'a (dyn Fn(&StreamStats) + Send + Sync),
    quarantine: Option<Quarantine>,
    transform: Arc<dyn Transform>,
    targets: Vec<TargetSpec>,
    stats: StreamStats,
    position: Option<ReadPosition>,
    rejections: Vec<String>,
    reader_retries: Arc<AtomicU64>,
    base_retries: u64,
    write_retries: u64,
}

impl Writer<'_> {
    /// Returns whether the copy was stopped before the end.
    async fn run(
        &mut self,
        rx: &mut mpsc::Receiver<Item>,
        stop: &CancellationToken,
        resumed: bool,
    ) -> Result<bool, EngineError> {
        // A non-resumable source restarts from the beginning: everything
        // up to the checkpoint is already in the target.
        let careful_all = resumed && self.position.is_none() && self.stats.read > 0;
        let mut careful = resumed;
        let started = Instant::now();
        let base = self.stats.read;
        loop {
            let item = tokio::select! {
                biased;
                () = stop.cancelled() => return Ok(true),
                item = rx.recv() => item,
            };
            let Some(item) = item else {
                return Ok(false);
            };
            let batch = item.map_err(EngineError::Source)?;
            if batch.records.is_empty() {
                continue;
            }
            let records = batch.records;
            let Some(out) = self.transform(&records, stop).await? else {
                return Ok(true);
            };
            let source_keys = if self.spec.source_keys.is_empty() {
                &self.spec.key_fields
            } else {
                &self.spec.source_keys
            };
            let mut rejected: BTreeMap<usize, String> = out
                .rejected
                .into_iter()
                .map(|(i, m)| (i, describe(&records, source_keys, i, &m)))
                .collect();
            let mut rows_by_target = out.rows;
            rows_by_target.resize_with(self.targets.len(), Vec::new);
            if self.options.dry_run {
                self.probe(&records, source_keys, rows_by_target, &mut rejected)
                    .await?;
                self.stats.failed += rejected.len() as u64;
                for m in rejected.values() {
                    if self.rejections.len() < MAX_REJECTIONS {
                        self.rejections.push(m.clone());
                    }
                }
                self.stats.read += records.len() as u64;
                self.stats.bytes += batch.approx_bytes;
                self.position = batch.position.clone();
                (self.on_progress)(&self.stats);
                continue;
            }
            // Source records with rows already written, per target.
            let mut written: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); self.targets.len()];
            for (t, rows) in rows_by_target.into_iter().enumerate() {
                let target = self.targets[t].clone();
                if let Some(column) = target.owner.as_ref().filter(|_| target.replace) {
                    let owners: Vec<Value> = out
                        .owners
                        .iter()
                        .filter(|(i, _)| !rejected.contains_key(i))
                        .map(|(_, v)| v.clone())
                        .collect();
                    if !owners.is_empty() && self.delete(&target, column, owners, stop).await? {
                        return Ok(true);
                    }
                }
                let rows: Vec<(usize, Record)> = rows
                    .into_iter()
                    .filter(|(i, _)| !rejected.contains_key(i))
                    .collect();
                if rows.is_empty() {
                    continue;
                }
                let origin: Vec<usize> = rows.iter().map(|(i, _)| *i).collect();
                let records_out: Vec<Record> = rows.into_iter().map(|(_, r)| r).collect();
                let keys = &target.key_fields;
                let mut mode = if careful {
                    idempotent(self.spec.mode, keys)
                } else {
                    self.spec.mode
                };
                let mut attempt = 0u32;
                let outcome = loop {
                    let write = WriteBatch {
                        object: target.object.clone(),
                        mode,
                        records: records_out.clone(),
                        key_fields: keys.clone(),
                        atomic: false,
                    };
                    // Never interrupt a write half-way: stopping happens between
                    // batches, so every checkpoint matches the target.
                    match self
                        .target
                        .write_batch(write, CancellationToken::new())
                        .await
                    {
                        Ok(o) => break o,
                        Err(e)
                            if e.is_retryable()
                                && attempt + 1 < self.options.retry.max_attempts
                                && !stop.is_cancelled() =>
                        {
                            attempt += 1;
                            self.write_retries += 1;
                            mode = idempotent(self.spec.mode, keys);
                            tracing::warn!(stream = %self.spec.key, attempt, error = %e, "write failed; retrying");
                            tokio::select! {
                                () = tokio::time::sleep(self.options.retry.delay(attempt)) => {}
                                () = stop.cancelled() => return Ok(true),
                            }
                        }
                        Err(e) => return Err(EngineError::Target(e)),
                    }
                };
                self.stats.inserted += outcome.inserted;
                self.stats.updated += outcome.updated;
                self.stats.skipped += outcome.skipped;
                let failed_rows: BTreeSet<usize> =
                    outcome.failures.iter().map(|f| f.index).collect();
                for f in &outcome.failures {
                    let src = origin.get(f.index).copied().unwrap_or(f.index);
                    rejected
                        .entry(src)
                        .or_insert_with(|| describe(&records, source_keys, src, &f.message));
                }
                written[t].extend(
                    origin
                        .iter()
                        .enumerate()
                        .filter(|(row, _)| !failed_rows.contains(row))
                        .map(|(_, src)| *src),
                );
            }
            // A record rejected by a later target keeps nothing in earlier
            // ones: its rows are removed, children first.
            let partial: BTreeSet<usize> = rejected
                .keys()
                .filter(|i| written.iter().any(|w| w.contains(i)))
                .copied()
                .collect();
            if !partial.is_empty() {
                let owners: Vec<Value> = out
                    .owners
                    .iter()
                    .filter(|(i, _)| partial.contains(i))
                    .map(|(_, v)| v.clone())
                    .collect();
                for t in (0..self.targets.len()).rev() {
                    let target = self.targets[t].clone();
                    let column = match (&target.owner, target.key_fields.as_slice()) {
                        (Some(c), _) => c.clone(),
                        (None, [k]) => k.clone(),
                        _ => continue,
                    };
                    if !owners.is_empty()
                        && written[t].iter().any(|i| partial.contains(i))
                        && self.delete(&target, &column, owners.clone(), stop).await?
                    {
                        return Ok(true);
                    }
                }
            }
            if let Some(first) = rejected.values().next().cloned() {
                self.stats.failed += rejected.len() as u64;
                for m in rejected.values() {
                    if self.rejections.len() < MAX_REJECTIONS {
                        self.rejections.push(m.clone());
                    }
                }
                match self.quarantine.take() {
                    None => return Err(EngineError::Rejected(first)),
                    Some(q) => {
                        let bad: Vec<Record> = rejected
                            .keys()
                            .filter_map(|i| records.get(*i).cloned())
                            .collect();
                        self.quarantine = Some(q.add(bad).await?);
                    }
                }
            }
            self.stats.read += records.len() as u64;
            self.stats.bytes += batch.approx_bytes;
            self.position = batch.position.clone();
            self.stats.retries = self.base_retries
                + self.write_retries
                + self.reader_retries.load(Ordering::Relaxed);
            self.checkpoints
                .save(&self.spec.key, self.position.as_ref(), &self.stats)
                .await
                .map_err(EngineError::Checkpoint)?;
            (self.on_progress)(&self.stats);
            careful = careful_all;
            if let Some(rate) = self.options.max_records_per_sec.filter(|r| *r > 0) {
                let due = Duration::from_secs_f64((self.stats.read - base) as f64 / rate as f64);
                let elapsed = started.elapsed();
                if due > elapsed {
                    tokio::select! {
                        () = tokio::time::sleep(due - elapsed) => {}
                        () = stop.cancelled() => return Ok(true),
                    }
                }
            }
        }
    }
}

impl Writer<'_> {
    /// Dry runs: classifies each row by whether its key already exists in
    /// the target, the way the write mode would treat it.
    async fn probe(
        &mut self,
        records: &[Record],
        source_keys: &[String],
        rows_by_target: Vec<Vec<(usize, Record)>>,
        rejected: &mut BTreeMap<usize, String>,
    ) -> Result<(), EngineError> {
        for (t, rows) in rows_by_target.into_iter().enumerate() {
            let target = self.targets[t].clone();
            let rows: Vec<(usize, Record)> = rows
                .into_iter()
                .filter(|(i, _)| !rejected.contains_key(i))
                .collect();
            if rows.is_empty() {
                continue;
            }
            let existing = if self.options.assume_empty_targets {
                std::collections::HashMap::new()
            } else {
                let refs: Vec<&Record> = rows.iter().map(|(_, r)| r).collect();
                validate::fetch_existing(self.target.as_ref(), &target, &refs)
                    .await
                    .map_err(EngineError::Target)?
            };
            for (src, r) in &rows {
                let exists = existing.contains_key(&validate::key_of(r, &target.key_fields));
                match (self.spec.mode, exists) {
                    (_, false) => self.stats.inserted += 1,
                    (WriteMode::SkipExisting, true) => self.stats.skipped += 1,
                    (WriteMode::Upsert, true) => self.stats.updated += 1,
                    (_, true) => {
                        rejected.entry(*src).or_insert_with(|| {
                            describe(
                                records,
                                source_keys,
                                *src,
                                &format!("`{}` already holds this key", target.object),
                            )
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Applies the transform, retrying transient failures. `None` when
    /// stopped while waiting to retry.
    async fn transform(
        &mut self,
        records: &[Record],
        stop: &CancellationToken,
    ) -> Result<Option<Transformed>, EngineError> {
        let mut attempt = 0u32;
        loop {
            match self.transform.apply(records).await {
                Ok(t) => return Ok(Some(t)),
                Err(e)
                    if e.is_retryable()
                        && attempt + 1 < self.options.retry.max_attempts
                        && !stop.is_cancelled() =>
                {
                    attempt += 1;
                    self.write_retries += 1;
                    tokio::select! {
                        () = tokio::time::sleep(self.options.retry.delay(attempt)) => {}
                        () = stop.cancelled() => return Ok(None),
                    }
                }
                Err(e) => return Err(EngineError::Transform(e)),
            }
        }
    }

    /// Deletes rows owned by the batch (see [`TargetSpec::owner`]).
    /// Returns whether the copy was stopped meanwhile.
    async fn delete(
        &mut self,
        target: &TargetSpec,
        column: &str,
        owners: Vec<Value>,
        stop: &CancellationToken,
    ) -> Result<bool, EngineError> {
        let mut attempt = 0u32;
        loop {
            let request = DeleteRequest {
                object: target.object.clone(),
                filter: Filter::In {
                    field: FieldPath::new(column),
                    values: owners.clone(),
                },
                expect_matched: None,
            };
            match self.target.delete(request).await {
                Ok(_) => return Ok(false),
                Err(e)
                    if e.is_retryable()
                        && attempt + 1 < self.options.retry.max_attempts
                        && !stop.is_cancelled() =>
                {
                    attempt += 1;
                    self.write_retries += 1;
                    tokio::select! {
                        () = tokio::time::sleep(self.options.retry.delay(attempt)) => {}
                        () = stop.cancelled() => return Ok(true),
                    }
                }
                Err(e) => return Err(EngineError::Target(e)),
            }
        }
    }
}

/// Copies `spec.source` into `spec.target`, from `resume` when given.
///
/// Cancelling `stop` ends the copy between batches with `stopped: true`;
/// the last checkpoint then matches the target exactly.
#[allow(clippy::too_many_arguments)] // the engine's whole contract, kept explicit
pub async fn copy_stream(
    source: Arc<dyn Connection>,
    target: Arc<dyn Connection>,
    spec: &StreamSpec,
    options: &EngineOptions,
    resume: Option<Resume>,
    stop: CancellationToken,
    checkpoints: &dyn Checkpoints,
    on_progress: &(dyn Fn(&StreamStats) + Send + Sync),
) -> Result<Outcome, Box<Failure>> {
    let identity = Arc::new(Identity(TargetSpec {
        object: spec.target.clone(),
        key_fields: spec.key_fields.clone(),
        owner: None,
        replace: false,
    }));
    convert_stream(
        source,
        target,
        spec,
        options,
        resume,
        stop,
        checkpoints,
        on_progress,
        identity,
    )
    .await
}

/// Like [`copy_stream`], converting records with `transform` on the way
/// (into one or several target objects).
#[allow(clippy::too_many_arguments)] // the engine's whole contract, kept explicit
pub async fn convert_stream(
    source: Arc<dyn Connection>,
    target: Arc<dyn Connection>,
    spec: &StreamSpec,
    options: &EngineOptions,
    resume: Option<Resume>,
    stop: CancellationToken,
    checkpoints: &dyn Checkpoints,
    on_progress: &(dyn Fn(&StreamStats) + Send + Sync),
    transform: Arc<dyn Transform>,
) -> Result<Outcome, Box<Failure>> {
    let resumed = resume.is_some();
    let (stats, position) =
        resume.map_or((StreamStats::default(), None), |r| (r.stats, r.position));
    let reader_retries = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel(options.queue_depth.max(1));
    let reader_stop = stop.child_token();
    let reader = tokio::spawn(
        Reader {
            source,
            spec: spec.clone(),
            batch_size: options.batch_size.max(1),
            retry: options.retry.clone(),
            position: position.clone(),
            retries: reader_retries.clone(),
        }
        .run(tx, reader_stop.clone()),
    );
    let mut writer = Writer {
        target,
        spec,
        options,
        checkpoints,
        on_progress,
        quarantine: match &options.on_record_error {
            RecordErrors::Fail => None,
            RecordErrors::Quarantine { dir } => Some(Quarantine::new(dir.clone(), &spec.key)),
        },
        targets: transform.targets(),
        transform,
        base_retries: stats.retries,
        stats,
        position,
        rejections: Vec::new(),
        reader_retries,
        write_retries: 0,
    };
    let result = writer.run(&mut rx, &stop, resumed).await;
    reader_stop.cancel();
    drop(rx);
    let _ = reader.await;
    let quarantine = match writer.quarantine.take() {
        Some(q) => q.finish().await,
        None => Ok(None),
    };
    let mut outcome = Outcome {
        stats: writer.stats,
        position: writer.position,
        stopped: false,
        quarantine: None,
        rejections: writer.rejections,
    };
    match (result, quarantine) {
        (Ok(stopped), Ok(path)) => {
            outcome.stopped = stopped;
            outcome.quarantine = path;
            Ok(outcome)
        }
        (Err(error), q) => {
            outcome.quarantine = q.ok().flatten();
            Err(Box::new(Failure { error, outcome }))
        }
        (Ok(_), Err(e)) => Err(Box::new(Failure {
            error: e.into(),
            outcome,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        let d: Vec<u128> = (1..=6).map(|a| p.delay(a).as_millis()).collect();
        assert_eq!(d, [100, 200, 400, 800, 1000, 1000]);
    }

    #[test]
    fn retried_inserts_skip_what_is_already_there() {
        let id = vec!["_id".to_owned()];
        assert_eq!(idempotent(WriteMode::Insert, &id), WriteMode::SkipExisting);
        assert_eq!(
            idempotent(WriteMode::FailOnConflict, &id),
            WriteMode::SkipExisting
        );
        assert_eq!(idempotent(WriteMode::Upsert, &id), WriteMode::Upsert);
        assert_eq!(idempotent(WriteMode::Insert, &[]), WriteMode::Insert);
    }
}
