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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use gazabridge_domain::Record;
use gazabridge_domain::conversion::scalar_to_text;
use gazabridge_domain::job::WriteMode;
use gazabridge_plugin_sdk::filter::Filter;
use gazabridge_plugin_sdk::read::{ReadPosition, ReadRequest, RecordBatch};
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_plugin_sdk::write::{RecordFailure, WriteBatch};
use gazabridge_plugin_sdk::{Connection, ConnectorError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::quarantine::Quarantine;

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
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            queue_depth: 4,
            retry: RetryPolicy::default(),
            on_record_error: RecordErrors::Fail,
            max_records_per_sec: None,
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

fn describe(records: &[Record], keys: &[String], f: &RecordFailure) -> String {
    let id = records
        .get(f.index)
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
        .unwrap_or_else(|| format!("#{}", f.index));
    format!("record {id}: {}", f.message)
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
        let careful_all = resumed && self.position.is_none() && self.stats.processed() > 0;
        let mut careful = resumed;
        let started = Instant::now();
        let base = self.stats.processed();
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
            self.stats.read += batch.records.len() as u64;
            self.stats.bytes += batch.approx_bytes;
            let keys = &self.spec.key_fields;
            let mut mode = if careful {
                idempotent(self.spec.mode, keys)
            } else {
                self.spec.mode
            };
            let mut attempt = 0u32;
            let outcome = loop {
                let write = WriteBatch {
                    object: self.spec.target.clone(),
                    mode,
                    records: batch.records.clone(),
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
            self.stats.failed += outcome.failures.len() as u64;
            for f in &outcome.failures {
                if self.rejections.len() < MAX_REJECTIONS {
                    self.rejections.push(describe(&batch.records, keys, f));
                }
            }
            if let Some(first) = outcome.failures.first() {
                match self.quarantine.take() {
                    None => {
                        return Err(EngineError::Rejected(describe(&batch.records, keys, first)));
                    }
                    Some(q) => {
                        let rejected: Vec<Record> = outcome
                            .failures
                            .iter()
                            .filter_map(|f| batch.records.get(f.index).cloned())
                            .collect();
                        self.quarantine = Some(q.add(rejected).await?);
                    }
                }
            }
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
                let due =
                    Duration::from_secs_f64((self.stats.processed() - base) as f64 / rate as f64);
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
