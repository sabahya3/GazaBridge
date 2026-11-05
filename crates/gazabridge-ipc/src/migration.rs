//! Copying collections between connections.

use serde::{Deserialize, Serialize};

use crate::explorer::ObjectAddr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyItemDto {
    pub source: ObjectAddr,
    pub target: ObjectAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationArgs {
    pub source_connection_id: String,
    pub target_connection_id: String,
    pub items: Vec<CopyItemDto>,
    /// `skip` (default), `update`, `fail` or `replace`.
    #[serde(default)]
    pub existing: String,
    #[serde(default)]
    pub copy_indexes: bool,
    #[serde(default)]
    pub batch_size: u32,
    /// `stop` (default) or `quarantine`.
    #[serde(default)]
    pub on_record_error: String,
    #[serde(default)]
    pub max_records_per_sec: Option<u64>,
    #[serde(default)]
    pub parallel: u32,
    #[serde(default)]
    pub name: Option<String>,
    /// The user confirmed dropping existing target collections.
    #[serde(default)]
    pub confirm_destructive: bool,
    /// MongoDB → PostgreSQL: `flatten` (default), `normalize`, `jsonb`, `hybrid`.
    #[serde(default)]
    pub doc_mode: String,
    /// PostgreSQL → MongoDB: `reference` (default) or `embed`.
    #[serde(default)]
    pub table_mode: String,
    /// Back up targets before dropping them; default yes.
    #[serde(default)]
    pub backup_before_replace: Option<bool>,
    /// Keep following changes after the copy.
    #[serde(default)]
    pub sync: Option<SyncArgs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncArgs {
    /// `change_stream` or `polling`.
    pub method: String,
    #[serde(default)]
    pub cursor_field: Option<String>,
    #[serde(default)]
    pub interval_secs: u64,
    #[serde(default)]
    pub initial_copy: bool,
}

/// A planned table with the rows the first documents become.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePreviewDto {
    pub table: ObjectAddr,
    pub columns: Vec<String>,
    pub types: Vec<String>,
    /// Where each column's value comes from (`address.city`).
    pub sources: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanItemDto {
    pub source: ObjectAddr,
    pub target: ObjectAddr,
    /// `copy`, `doc_to_table`, `table_to_doc` or `embedded`.
    #[serde(default)]
    pub kind: String,
    pub source_count: Option<u64>,
    pub target_exists: bool,
    pub target_count: Option<u64>,
    pub indexes: Vec<String>,
    pub statements: Vec<String>,
    pub warnings: Vec<String>,
    #[serde(default)]
    pub tables: Vec<TablePreviewDto>,
    /// Pretty JSON documents: source samples (to tables) or results (from tables).
    #[serde(default)]
    pub documents: Vec<String>,
    #[serde(default)]
    pub embedded_into: Option<ObjectAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlanDto {
    pub items: Vec<PlanItemDto>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub destructive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunArgs {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDto {
    pub source: ObjectAddr,
    pub target: ObjectAddr,
    /// `pending`, `preparing`, `copying`, `indexing`, `done`, `stopped`, `failed`.
    pub phase: String,
    pub total: Option<u64>,
    pub read: u64,
    pub inserted: u64,
    pub updated: u64,
    pub skipped: u64,
    pub failed: u64,
    pub bytes: u64,
    pub retries: u64,
    pub indexes_created: u32,
    pub quarantine: Option<String>,
    pub rejections: Vec<String>,
    pub error: Option<String>,
    /// Folder of the backup taken before the target was dropped.
    #[serde(default)]
    pub backup: Option<String>,
    /// Sync: changes applied after the copy, deletes among them.
    #[serde(default)]
    pub changes: u64,
    #[serde(default)]
    pub deletes: u64,
    /// RFC 3339.
    #[serde(default)]
    pub last_change: Option<String>,
}

impl StreamDto {
    pub fn processed(&self) -> u64 {
        self.inserted + self.updated + self.skipped + self.failed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventDto {
    /// RFC 3339.
    pub at: String,
    /// `info`, `warning` or `error`.
    pub level: String,
    pub stream: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDto {
    pub run_id: String,
    pub name: String,
    /// Job status, e.g. `running`, `paused`, `completed_with_warnings`.
    pub status: String,
    pub source_connection_id: String,
    pub target_connection_id: String,
    pub existing: String,
    pub streams: Vec<StreamDto>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub error: Option<String>,
    pub events: Vec<RunEventDto>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub validation: Option<ValidationDto>,
    /// Set for runs that keep the target in sync after the copy.
    #[serde(default)]
    pub sync: Option<SyncArgs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportArgs {
    pub run_id: String,
    /// `json`, `html`, `markdown` or `csv`.
    pub format: String,
    /// Absolute path of a file that does not exist yet.
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidateArgs {
    pub run_id: String,
    /// `basic` (counts), `standard` (keys) or `deep` (content).
    pub level: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifferenceDto {
    pub target: String,
    pub key: String,
    /// `missing`, `extra` or `different`.
    pub kind: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetCheckDto {
    pub target: ObjectAddr,
    pub expected: u64,
    pub matched: u64,
    pub missing: u64,
    pub extra: u64,
    pub different: u64,
    pub chunks: u64,
    pub mismatched_chunks: u64,
    pub expected_checksum: Option<String>,
    pub actual_checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamValidationDto {
    pub source: ObjectAddr,
    pub source_count: Option<u64>,
    pub target_count: Option<u64>,
    /// Both counts are exact (not estimates).
    pub counts_exact: bool,
    pub checked: u64,
    pub passed: bool,
    pub notes: Vec<String>,
    pub targets: Vec<TargetCheckDto>,
    pub differences: Vec<DifferenceDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationDto {
    pub level: String,
    pub running: bool,
    pub passed: Option<bool>,
    pub error: Option<String>,
    pub streams: Vec<StreamValidationDto>,
}

impl RunDto {
    /// An engine task currently owns the run.
    pub fn is_active(&self) -> bool {
        matches!(
            self.status.as_str(),
            "running" | "pausing" | "resuming" | "cancelling" | "validating"
        )
    }

    pub fn can_resume(&self) -> bool {
        matches!(self.status.as_str(), "paused" | "failed")
    }
}

/// One stream of a running copy, for the Monitor page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamMonitorDto {
    pub source: ObjectAddr,
    pub target: ObjectAddr,
    pub phase: String,
    pub processed: u64,
    pub total: Option<u64>,
    pub failed: u64,
    pub retries: u64,
    pub bytes: u64,
    pub records_per_sec: Option<f64>,
    pub bytes_per_sec: Option<f64>,
    pub eta_secs: Option<u64>,
    #[serde(default)]
    pub queue_len: u32,
    #[serde(default)]
    pub queue_capacity: u32,
    #[serde(default)]
    pub read_ms: Option<f64>,
    #[serde(default)]
    pub write_ms: Option<f64>,
    /// `source` or `target`: the side holding the copy back.
    #[serde(default)]
    pub slow_side: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMonitorDto {
    pub run_id: String,
    pub name: String,
    pub status: String,
    pub dry_run: bool,
    /// RFC 3339.
    pub started_at: Option<String>,
    pub processed: u64,
    pub total: Option<u64>,
    pub records_per_sec: Option<f64>,
    pub bytes_per_sec: Option<f64>,
    pub eta_secs: Option<u64>,
    pub streams: Vec<StreamMonitorDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorDto {
    /// RFC 3339.
    pub at: String,
    pub memory_bytes: Option<u64>,
    pub cpu_percent: Option<f32>,
    pub runs: Vec<RunMonitorDto>,
}
