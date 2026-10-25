//! Query Studio exchange types.

use serde::{Deserialize, Serialize};

use crate::explorer::{ColumnDto, RowDto};

/// Error code returned when a query needs explicit confirmation. The
/// message lists the risks after the last `:` as comma-separated names
/// (`drop`, `truncate`, `delete_without_where`, `update_without_where`,
/// `overwrites_collection`).
pub const CONFIRMATION_REQUIRED: &str = "confirmation_required";

/// Risk names from a `confirmation_required` error message.
pub fn risks_from_message(message: &str) -> Vec<String> {
    message
        .rsplit_once(':')
        .map(|(_, list)| {
            list.split(',')
                .map(|r| r.trim().to_owned())
                .filter(|r| !r.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryContextArgs {
    pub connection_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryContextDto {
    /// `postgres_sql`, `sqlite_sql`, `mongo_filter`, `mongo_aggregation`, …
    pub languages: Vec<String>,
    pub explain: bool,
    pub explain_analyze: bool,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunQueryArgs {
    pub connection_id: String,
    /// Client-chosen id used to cancel this run.
    pub run_id: String,
    pub language: String,
    pub text: String,
    /// Positional parameters as typed JSON (`$1`, `$2`, … in SQL).
    #[serde(default)]
    pub params: Vec<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub collection: Option<String>,
    pub max_rows: u32,
    #[serde(default)]
    pub timeout_secs: Option<u32>,
    /// The user confirmed the risks of a destructive query.
    #[serde(default)]
    pub confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResultDto {
    pub columns: Vec<ColumnDto>,
    pub rows: Vec<RowDto>,
    pub affected: Option<u64>,
    /// More rows existed than `max_rows`.
    pub truncated: bool,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub notices: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainArgs {
    pub query: RunQueryArgs,
    pub analyze: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainDto {
    pub text: String,
    /// Pretty-printed native plan (JSON).
    pub native: String,
    pub analyzed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelArgs {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryArgs {
    #[serde(default)]
    pub connection_id: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryDto {
    pub id: i64,
    pub connection_id: Option<String>,
    pub language: String,
    pub text: String,
    /// RFC 3339.
    pub started_at: String,
    pub duration_ms: Option<i64>,
    pub row_count: Option<i64>,
    pub affected: Option<i64>,
    /// `ok`, `error` or `cancelled`.
    pub status: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQueryDto {
    pub id: String,
    pub name: String,
    pub connection_id: Option<String>,
    pub language: String,
    pub text: String,
    pub favorite: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListSavedArgs {
    #[serde(default)]
    pub connection_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveQueryArgs {
    /// Existing saved query to update; `None` creates one.
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub connection_id: Option<String>,
    pub language: String,
    pub text: String,
    #[serde(default)]
    pub favorite: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSavedArgs {
    pub id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risks_parse_from_the_error_message() {
        assert_eq!(
            risks_from_message("confirmation required: drop,truncate"),
            ["drop", "truncate"]
        );
        assert!(risks_from_message("nothing here").is_empty());
    }
}
