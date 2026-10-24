//! Batched writes.

use gazabridge_domain::Record;
use gazabridge_domain::job::WriteMode;
use serde::{Deserialize, Serialize};

use crate::error::ErrorKind;
use crate::filter::Filter;
use crate::schema::ObjectRef;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteBatch {
    pub object: ObjectRef,
    pub mode: WriteMode,
    pub records: Vec<Record>,
    /// Key fields used by upsert/skip-existing; required for those modes.
    #[serde(default)]
    pub key_fields: Vec<String>,
    /// Whether the batch must be applied atomically. Connectors without
    /// transactions reject atomic batches.
    #[serde(default)]
    pub atomic: bool,
}

/// Why a single record failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordFailure {
    /// Index of the record within the batch.
    pub index: usize,
    pub kind: ErrorKind,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOutcome {
    pub inserted: u64,
    pub updated: u64,
    pub skipped: u64,
    pub failures: Vec<RecordFailure>,
}

impl WriteOutcome {
    pub fn written(&self) -> u64 {
        self.inserted + self.updated
    }

    pub fn merge(&mut self, other: WriteOutcome) {
        self.inserted += other.inserted;
        self.updated += other.updated;
        self.skipped += other.skipped;
        self.failures.extend(other.failures);
    }
}

/// Targeted update used by the Data Editor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateRequest {
    pub object: ObjectRef,
    /// Must be restrictive (see [`Filter::is_restrictive`]); connectors
    /// reject unrestricted updates.
    pub filter: Filter,
    pub set: Record,
    /// Fail unless exactly this many records match (optimistic concurrency).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_matched: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub object: ObjectRef,
    pub filter: Filter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_matched: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationOutcome {
    pub matched: u64,
    pub modified: u64,
}
