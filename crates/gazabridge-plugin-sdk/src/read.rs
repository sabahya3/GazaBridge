//! Streaming and paginated reads.

use std::pin::Pin;

use futures::Stream;
use gazabridge_domain::Record;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::filter::{Filter, SortKey};
use crate::schema::{ColumnInfo, ObjectRef};

/// Opaque, connector-defined position used to resume a read or change feed.
///
/// Connectors encode whatever they need (last primary key, resume token,
/// WAL LSN, binlog file+offset) as JSON. The engine persists it verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadPosition {
    pub connector_id: String,
    pub token: serde_json::Value,
}

/// Request to stream every matching record of an object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadRequest {
    pub object: ObjectRef,
    /// Fields to return; empty means all.
    #[serde(default)]
    pub projection: Vec<String>,
    /// Fields to leave out (document stores); used when `projection` is empty.
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    /// Ordering. When resuming, connectors require a stable unique order
    /// (normally the primary key) and reject requests without one.
    #[serde(default)]
    pub order_by: Vec<SortKey>,
    /// Maximum number of records per emitted batch.
    pub batch_size: usize,
    /// Stop after this many records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Continue after this position (from a previous batch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_after: Option<ReadPosition>,
}

impl ReadRequest {
    pub fn new(object: ObjectRef, batch_size: usize) -> Self {
        Self {
            object,
            projection: Vec::new(),
            exclude: Vec::new(),
            filter: None,
            order_by: Vec::new(),
            batch_size,
            limit: None,
            resume_after: None,
        }
    }
}

/// A batch of records read from a source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordBatch {
    pub records: Vec<Record>,
    /// Position after the last record of the batch, when the read is resumable.
    pub position: Option<ReadPosition>,
    /// Approximate payload size in bytes.
    pub approx_bytes: u64,
}

impl RecordBatch {
    pub fn new(records: Vec<Record>, position: Option<ReadPosition>) -> Self {
        let approx_bytes = records.iter().map(|r| r.approx_size() as u64).sum();
        Self {
            records,
            position,
            approx_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A bounded stream of record batches. Dropping it stops the read.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, ConnectorError>> + Send>>;

/// Request for one page of records for interactive viewing.
///
/// Pages are always bounded: `limit` is mandatory and connectors clamp it to
/// [`PageRequest::MAX_LIMIT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageRequest {
    pub object: ObjectRef,
    #[serde(default)]
    pub projection: Vec<String>,
    /// Fields to leave out (document stores); used when `projection` is empty.
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub order_by: Vec<SortKey>,
    pub offset: u64,
    pub limit: u32,
}

impl PageRequest {
    pub const MAX_LIMIT: u32 = 10_000;

    pub fn effective_limit(&self) -> u32 {
        self.limit.clamp(1, Self::MAX_LIMIT)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page {
    pub columns: Vec<ColumnInfo>,
    pub records: Vec<Record>,
    pub offset: u64,
    /// True when more records exist after this page.
    pub has_more: bool,
}
