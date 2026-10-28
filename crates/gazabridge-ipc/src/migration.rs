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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanItemDto {
    pub source: ObjectAddr,
    pub target: ObjectAddr,
    pub source_count: Option<u64>,
    pub target_exists: bool,
    pub target_count: Option<u64>,
    pub indexes: Vec<String>,
    pub statements: Vec<String>,
    pub warnings: Vec<String>,
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
