//! Exporting objects to files and importing files into objects.
//!
//! Both directions stream in batches, report progress, can be cancelled
//! and are audited (object, file, counts, checksum; never record values).
//! An export writes to a temporary file that only replaces the target when
//! complete, so a cancelled or failed export leaves nothing behind.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use gazabridge_domain::ids::ConnectionId;
use gazabridge_domain::job::WriteMode;
use gazabridge_files::{
    FileFormat, FileReader, FileWriter, ReadOptions, WriteOptions, WriteSummary,
};
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions, ensure_writable};
use gazabridge_plugin_sdk::filter::{Filter, SortKey};
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::{ObjectRef, SamplingMethod};
use gazabridge_plugin_sdk::write::WriteBatch;
use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::audit::NewAuditEvent;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::explorer::ExplorerService;
use crate::{AppError, AppResult};

/// Records read and written per batch.
pub const DEFAULT_BATCH: usize = 1_000;
/// Per-record problems kept in a summary; counters keep counting.
const MAX_MESSAGES: usize = 100;

#[derive(Debug, Clone)]
pub struct ExportRequest {
    pub object: ObjectRef,
    pub filter: Option<Filter>,
    /// Fields to export; empty means all.
    pub projection: Vec<String>,
    /// Fields to leave out (documents), when `projection` is empty.
    pub exclude: Vec<String>,
    /// The query's sort; empty means the connector's stable order.
    pub order_by: Vec<SortKey>,
    /// Records to pass over first, then at most `limit` are written: the
    /// export holds exactly what the query shows.
    pub skip: u64,
    pub limit: Option<u64>,
    pub path: PathBuf,
    /// For CSV, empty columns mean "the object's columns" (tables) or the
    /// top-level fields found by sampling (collections).
    pub options: WriteOptions,
}

#[derive(Debug, Clone)]
pub struct ImportRequest {
    pub object: ObjectRef,
    pub path: PathBuf,
    pub options: ReadOptions,
    pub mode: WriteMode,
    /// Required for upserts.
    pub key_fields: Vec<String>,
    pub batch_size: usize,
    /// Stop at the first record the target rejects.
    pub stop_on_error: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Running,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferSummary {
    pub records_read: u64,
    pub inserted: u64,
    pub updated: u64,
    pub skipped: u64,
    /// Records the target rejected.
    pub failed: u64,
    /// Malformed input records skipped or quarantined.
    pub malformed: u64,
    pub quarantine_path: Option<PathBuf>,
    /// First problems, with their line or record number.
    pub messages: Vec<String>,
    /// Export only.
    pub file: Option<WriteSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub phase: Phase,
    pub records: u64,
    pub summary: TransferSummary,
    pub error: Option<String>,
    pub error_code: Option<String>,
}

impl Progress {
    fn running() -> Self {
        Self {
            phase: Phase::Running,
            records: 0,
            summary: TransferSummary::default(),
            error: None,
            error_code: None,
        }
    }
}

#[derive(Debug)]
struct Operation {
    progress: Progress,
    cancel: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct TransferService {
    store: MetadataStore,
    explorer: ExplorerService,
    redactor: gazabridge_secrets::Redactor,
    ops: Arc<Mutex<HashMap<String, Operation>>>,
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> AppResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AppError::Invalid(format!("file task failed: {e}")))
}

fn cancelled() -> AppError {
    AppError::Connector(ConnectorError::Cancelled)
}

fn check_path(path: &std::path::Path) -> AppResult<()> {
    if !path.is_absolute() {
        return Err(AppError::Invalid("choose a full file path".into()));
    }
    Ok(())
}

/// Top-level fields of an inferred structure, `_id` first.
fn top_level_fields(paths: impl Iterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = paths
        .filter(|p| !p.contains('.') && !p.contains('['))
        .collect();
    out.dedup();
    if let Some(i) = out.iter().position(|f| f == "_id") {
        let id = out.remove(i);
        out.insert(0, id);
    }
    out
}

impl TransferService {
    pub fn new(
        store: MetadataStore,
        explorer: ExplorerService,
        redactor: gazabridge_secrets::Redactor,
    ) -> Self {
        Self {
            store,
            explorer,
            redactor,
            ops: Arc::default(),
        }
    }

    async fn csv_columns(
        &self,
        conn: &dyn Connection,
        object: &ObjectRef,
    ) -> AppResult<Vec<String>> {
        let options = InspectOptions {
            include_indexes: false,
            include_constraints: false,
            sample_size: 1_000,
            sampling: SamplingMethod::Random,
        };
        let schema = conn.inspect(object, &options).await?;
        let columns = match schema.inferred {
            Some(inferred) => top_level_fields(inferred.fields.into_iter().map(|f| f.path)),
            None => schema.columns.into_iter().map(|c| c.name).collect(),
        };
        if columns.is_empty() {
            return Err(AppError::Invalid(
                "no columns found for the CSV export".into(),
            ));
        }
        Ok(columns)
    }

    async fn audit(
        &self,
        action: &str,
        id: ConnectionId,
        object: &ObjectRef,
        summary: String,
        details: serde_json::Value,
    ) -> AppResult<()> {
        self.store
            .audit()
            .record(NewAuditEvent {
                action,
                target_kind: "object",
                target_id: Some(format!("{id}/{object}")),
                summary,
                details,
                correlation_id: None,
            })
            .await?;
        Ok(())
    }

    /// Exports an object (optionally filtered) to a file.
    pub async fn export(
        &self,
        id: ConnectionId,
        mut req: ExportRequest,
        cancel: CancellationToken,
        on_progress: impl Fn(&Progress) + Send,
    ) -> AppResult<TransferSummary> {
        check_path(&req.path)?;
        let conn = self.explorer.session(id).await?;
        let tabular = matches!(req.options.format, FileFormat::Csv | FileFormat::Xlsx);
        if tabular && req.options.csv.columns.is_empty() {
            // Columns follow the query's Project when it has one.
            req.options.csv.columns = if req.projection.is_empty() {
                let mut all = self.csv_columns(conn.as_ref(), &req.object).await?;
                all.retain(|c| !req.exclude.contains(c));
                all
            } else {
                // `address.city` comes back inside `address`.
                let mut cols: Vec<String> = Vec::new();
                for p in &req.projection {
                    let top = p.split('.').next().unwrap_or(p).to_owned();
                    if !cols.contains(&top) {
                        cols.push(top);
                    }
                }
                cols
            };
        }
        let (path, options) = (req.path.clone(), req.options.clone());
        let mut writer = blocking(move || FileWriter::create(&path, options)).await??;
        let mut read = ReadRequest::new(req.object.clone(), DEFAULT_BATCH);
        read.filter = req.filter.clone();
        read.projection = req.projection.clone();
        read.exclude = req.exclude.clone();
        read.order_by = req.order_by.clone();
        // Reads have no offset (they resume by key), so skipped records are
        // read and dropped; the query bar keeps skips page-sized.
        read.limit = req.limit.map(|l| l.saturating_add(req.skip));
        let mut to_skip = req.skip;
        let r = conn.read(read, cancel.clone()).await;
        let mut stream = self.explorer.checked(id, r).await?;
        let mut progress = Progress::running();
        while let Some(batch) = stream.next().await {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let batch = self.explorer.checked(id, batch).await?;
            let mut records = batch.records;
            if to_skip > 0 {
                let k = usize::try_from(to_skip)
                    .unwrap_or(usize::MAX)
                    .min(records.len());
                records.drain(..k);
                to_skip -= k as u64;
            }
            if records.is_empty() {
                continue;
            }
            let n = records.len() as u64;
            writer = blocking(move || writer.write(&records).map(|()| writer)).await??;
            progress.records += n;
            progress.summary.records_read += n;
            on_progress(&progress);
        }
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let file = blocking(move || writer.finish()).await??;
        progress.summary.file = Some(file.clone());
        self.audit(
            "data.export",
            id,
            &req.object,
            format!(
                "Exported {} record(s) from {} to {}",
                file.records,
                req.object,
                file.path.display()
            ),
            serde_json::json!({
                "file": file.path, "format": req.options.format, "records": file.records,
                "bytes": file.bytes, "sha256": file.sha256, "filtered": req.filter.is_some(),
                "skip": req.skip, "limit": req.limit,
            }),
        )
        .await?;
        Ok(progress.summary)
    }

    /// Imports a file into an object.
    pub async fn import(
        &self,
        id: ConnectionId,
        req: ImportRequest,
        cancel: CancellationToken,
        on_progress: impl Fn(&Progress) + Send,
    ) -> AppResult<TransferSummary> {
        check_path(&req.path)?;
        if !req.object.kind.is_writable() {
            return Err(AppError::Invalid(format!(
                "records cannot be written to `{}`",
                req.object
            )));
        }
        if matches!(req.mode, WriteMode::Upsert | WriteMode::SkipExisting)
            && req.key_fields.is_empty()
        {
            return Err(AppError::Invalid(
                "upserts and skipping existing records need key fields".into(),
            ));
        }
        // Nothing an import does removes data; dropping and truncating
        // belong to migrations, with a dry run and confirmation.
        if req.mode.is_destructive() {
            return Err(AppError::Invalid("imports insert, append, upsert or skip existing records; destructive modes belong to migrations with a dry run".into()));
        }
        let conn = self.explorer.session(id).await?;
        ensure_writable(conn.as_ref())?;
        let (path, options) = (req.path.clone(), req.options.clone());
        let mut reader = blocking(move || FileReader::open(&path, options)).await??;
        let batch_size = req.batch_size.clamp(1, 50_000);
        let mut progress = Progress::running();
        loop {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let (back, batch) = blocking(move || {
                let r = reader.next_batch(batch_size);
                (reader, r)
            })
            .await?;
            reader = back;
            let batch = batch?;
            if batch.is_empty() {
                break;
            }
            let locations: Vec<u64> = batch.iter().map(|s| s.location).collect();
            let write = WriteBatch {
                object: req.object.clone(),
                mode: req.mode,
                records: batch.into_iter().map(|s| s.record).collect(),
                key_fields: req.key_fields.clone(),
                atomic: false,
            };
            let n = write.records.len() as u64;
            let r = conn.write_batch(write, cancel.clone()).await;
            let outcome = self.explorer.checked(id, r).await?;
            let s = &mut progress.summary;
            s.records_read += n;
            s.inserted += outcome.inserted;
            s.updated += outcome.updated;
            s.skipped += outcome.skipped;
            s.failed += outcome.failures.len() as u64;
            for f in &outcome.failures {
                if s.messages.len() < MAX_MESSAGES {
                    let at = locations.get(f.index).copied().unwrap_or_default();
                    s.messages.push(format!(
                        "record at {at}: {}",
                        self.redactor.scrub(&f.message)
                    ));
                }
            }
            progress.records += n;
            on_progress(&progress);
            if req.stop_on_error && !outcome.failures.is_empty() {
                let first = progress
                    .summary
                    .messages
                    .first()
                    .cloned()
                    .unwrap_or_default();
                return Err(AppError::Connector(ConnectorError::new(
                    ErrorKind::Constraint,
                    format!(
                        "stopped at the first rejected record ({} written before it): {first}",
                        progress.summary.inserted + progress.summary.updated
                    ),
                )));
            }
        }
        let stats = blocking(move || reader.finish()).await??;
        let s = &mut progress.summary;
        s.malformed = stats.skipped + stats.quarantined;
        s.quarantine_path = stats.quarantine_path.filter(|_| stats.quarantined > 0);
        for issue in stats.issues {
            if s.messages.len() < MAX_MESSAGES {
                s.messages.push(format!(
                    "{} {}: {}",
                    issue.kind, issue.location, issue.message
                ));
            }
        }
        let summary = progress.summary.clone();
        self.audit(
            "data.import",
            id,
            &req.object,
            format!(
                "Imported {} record(s) into {} from {}",
                summary.inserted + summary.updated,
                req.object,
                req.path.display()
            ),
            serde_json::json!({
                "file": req.path, "format": req.options.format, "mode": req.mode,
                "read": summary.records_read, "inserted": summary.inserted, "updated": summary.updated,
                "skipped": summary.skipped, "failed": summary.failed, "malformed": summary.malformed,
            }),
        )
        .await?;
        Ok(summary)
    }

    fn set(ops: &Mutex<HashMap<String, Operation>>, op_id: &str, f: impl FnOnce(&mut Progress)) {
        if let Ok(mut ops) = ops.lock()
            && let Some(op) = ops.get_mut(op_id)
        {
            f(&mut op.progress);
        }
    }

    fn begin(&self, op_id: &str) -> AppResult<CancellationToken> {
        let mut ops = self
            .ops
            .lock()
            .map_err(|_| AppError::Invalid("operation table unavailable".into()))?;
        if ops
            .get(op_id)
            .is_some_and(|o| o.progress.phase == Phase::Running)
        {
            return Err(AppError::Invalid(format!(
                "operation `{op_id}` is already running"
            )));
        }
        let cancel = CancellationToken::new();
        ops.insert(
            op_id.to_owned(),
            Operation {
                progress: Progress::running(),
                cancel: cancel.clone(),
            },
        );
        Ok(cancel)
    }

    fn finish_op(
        ops: &Mutex<HashMap<String, Operation>>,
        redactor: &gazabridge_secrets::Redactor,
        op_id: &str,
        result: AppResult<TransferSummary>,
    ) {
        Self::set(ops, op_id, |p| match result {
            Ok(summary) => {
                p.phase = Phase::Done;
                p.records = summary.records_read;
                p.summary = summary;
            }
            Err(e) => {
                p.phase = if e.code() == "cancelled" {
                    Phase::Cancelled
                } else {
                    Phase::Failed
                };
                p.error_code = Some(e.code().to_owned());
                p.error = Some(redactor.scrub(&e.to_string()));
            }
        });
    }

    /// Starts an export in the background; follow it with [`Self::status`].
    pub async fn start_export(
        &self,
        id: ConnectionId,
        op_id: &str,
        req: ExportRequest,
    ) -> AppResult<()> {
        check_path(&req.path)?;
        if req.path.exists() && !req.options.overwrite {
            return Err(gazabridge_files::FileError::Exists(req.path.clone()).into());
        }
        let cancel = self.begin(op_id)?;
        let (svc, op) = (self.clone(), op_id.to_owned());
        tokio::spawn(async move {
            let ops = svc.ops.clone();
            let r = svc
                .export(id, req, cancel, |p| {
                    Self::set(&ops, &op, |live| *live = p.clone())
                })
                .await;
            Self::finish_op(&svc.ops, &svc.redactor, &op, r);
        });
        Ok(())
    }

    /// Starts an import in the background; follow it with [`Self::status`].
    pub async fn start_import(
        &self,
        id: ConnectionId,
        op_id: &str,
        req: ImportRequest,
    ) -> AppResult<()> {
        check_path(&req.path)?;
        let cancel = self.begin(op_id)?;
        let (svc, op) = (self.clone(), op_id.to_owned());
        tokio::spawn(async move {
            let ops = svc.ops.clone();
            let r = svc
                .import(id, req, cancel, |p| {
                    Self::set(&ops, &op, |live| *live = p.clone())
                })
                .await;
            Self::finish_op(&svc.ops, &svc.redactor, &op, r);
        });
        Ok(())
    }

    pub fn status(&self, op_id: &str) -> Option<Progress> {
        self.ops.lock().ok()?.get(op_id).map(|o| o.progress.clone())
    }

    pub fn cancel(&self, op_id: &str) -> bool {
        let Ok(ops) = self.ops.lock() else {
            return false;
        };
        match ops.get(op_id) {
            Some(op) if op.progress.phase == Phase::Running => {
                op.cancel.cancel();
                true
            }
            _ => false,
        }
    }

    /// Drops a finished operation's status.
    pub fn forget(&self, op_id: &str) {
        let Ok(mut ops) = self.ops.lock() else { return };
        if ops
            .get(op_id)
            .is_some_and(|o| o.progress.phase != Phase::Running)
        {
            ops.remove(op_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_columns_from_inference_are_top_level_with_id_first() {
        let paths = [
            "name",
            "_id",
            "items",
            "items[].sku",
            "address.city",
            "address",
        ]
        .map(String::from);
        assert_eq!(
            top_level_fields(paths.into_iter()),
            ["_id", "name", "items", "address"]
        );
    }
}
