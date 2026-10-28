//! The transform stage between reading and writing.
//!
//! A transform turns a batch of source records into rows for one or more
//! target objects (a document and its normalized child tables, say). Every
//! row is tagged with the position of the source record it came from, so a
//! source record rejected anywhere is quarantined once, as a whole, and its
//! rows for later targets are not written.

use async_trait::async_trait;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::ConnectorError;
use gazabridge_plugin_sdk::schema::ObjectRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetSpec {
    pub object: ObjectRef,
    /// Identity of a row in this target; makes retries and resumes idempotent.
    pub key_fields: Vec<String>,
    /// Column holding the key of the source record a row belongs to (child
    /// tables: the parent key). Rows of a rejected record are removed by it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Delete the batch's rows by `owner` before writing (child tables when
    /// parents are replaced, so a shorter array leaves no stale rows).
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Transformed {
    /// Rows per target, in [`Transform::targets`] order, tagged with the
    /// index of their source record in the batch.
    pub rows: Vec<Vec<(usize, Record)>>,
    /// Source records that could not be converted, with the reason.
    pub rejected: Vec<(usize, String)>,
    /// Key of each converted source record, matched against `owner`.
    pub owners: Vec<(usize, Value)>,
}

#[async_trait]
pub trait Transform: Send + Sync {
    /// Targets in write order: parents before children.
    fn targets(&self) -> Vec<TargetSpec>;

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError>;
}

/// Writes records unchanged to one target.
#[derive(Debug, Clone)]
pub struct Identity(pub TargetSpec);

#[async_trait]
impl Transform for Identity {
    fn targets(&self) -> Vec<TargetSpec> {
        vec![self.0.clone()]
    }

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError> {
        Ok(Transformed {
            rows: vec![records.iter().cloned().enumerate().collect()],
            rejected: Vec::new(),
            owners: Vec::new(),
        })
    }
}
