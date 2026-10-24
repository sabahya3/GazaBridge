//! Change data capture.

use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;
use gazabridge_domain::Record;
use gazabridge_domain::compatibility::SyncMethod;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::read::ReadPosition;
use crate::schema::ObjectRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOperation {
    Insert,
    Update,
    Replace,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub object: ObjectRef,
    pub operation: ChangeOperation,
    /// Primary key / `_id` of the changed record.
    pub key: Record,
    /// Full record after the change (absent for deletes, and for updates
    /// when the source only emits deltas).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Record>,
    /// Previous image when the source provides it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Record>,
    /// Position to resume after this event.
    pub position: ReadPosition,
    /// Commit/cluster time reported by the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_time: Option<DateTime<Utc>>,
    /// Source transaction identifier when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeFeedRequest {
    pub objects: Vec<ObjectRef>,
    pub method: SyncMethod,
    /// Resume after this position; `None` starts at the current position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_after: Option<ReadPosition>,
    /// Column used by timestamp-incremental polling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<String>,
    /// Polling interval for polling-based methods.
    #[serde(default = "default_poll_ms")]
    pub poll_interval_ms: u64,
}

fn default_poll_ms() -> u64 {
    5_000
}

pub type ChangeStream = Pin<Box<dyn Stream<Item = Result<ChangeEvent, ConnectorError>> + Send>>;
