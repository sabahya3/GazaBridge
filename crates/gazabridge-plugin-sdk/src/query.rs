//! Ad-hoc query execution.

use std::time::Duration;

use gazabridge_domain::{Record, Value};
use serde::{Deserialize, Serialize};

use crate::capability::QueryLanguage;
use crate::schema::ColumnInfo;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub language: QueryLanguage,
    pub text: String,
    /// Positional parameters, bound by the driver.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Hard cap on returned rows; results beyond it are not fetched.
    pub max_rows: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
    /// Target database for engines where it is per-request (MongoDB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Collection for MongoDB filters and pipelines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Record>,
    /// Rows affected by DML, when the server reports it.
    pub affected: Option<u64>,
    /// True when `max_rows` cut the result short.
    pub truncated: bool,
    pub elapsed: Duration,
    #[serde(default)]
    pub notices: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainPlan {
    /// Plan in the server's native format (JSON when available).
    pub native: serde_json::Value,
    /// Plain-text rendering.
    pub text: String,
    /// True when the query was actually executed (EXPLAIN ANALYZE).
    pub analyzed: bool,
}
