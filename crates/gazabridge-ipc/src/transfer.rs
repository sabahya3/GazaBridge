//! Export and import exchange types.

use serde::{Deserialize, Serialize};

use crate::explorer::{FilterDto, ObjectAddr};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportArgs {
    pub connection_id: String,
    /// Client-chosen id to follow and cancel the export.
    pub op_id: String,
    pub object: ObjectAddr,
    #[serde(default)]
    pub filter: Option<FilterDto>,
    pub path: String,
    /// `json`, `jsonl`, `bson` or `csv`.
    pub format: String,
    /// `typed`, `mongo_relaxed` or `mongo_canonical`.
    #[serde(default)]
    pub json_flavor: String,
    #[serde(default)]
    pub gzip: bool,
    #[serde(default)]
    pub pretty: bool,
    /// The user confirmed replacing an existing file.
    #[serde(default)]
    pub overwrite: bool,
    /// Empty: the object's columns or sampled top-level fields.
    #[serde(default)]
    pub csv_columns: Vec<String>,
    #[serde(default)]
    pub csv_delimiter: String,
    #[serde(default)]
    pub csv_null_text: String,
    /// Leave out fields that are not CSV columns instead of failing.
    #[serde(default)]
    pub csv_ignore_extra: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportArgs {
    pub connection_id: String,
    pub op_id: String,
    pub object: ObjectAddr,
    pub path: String,
    pub format: String,
    #[serde(default)]
    pub json_flavor: String,
    #[serde(default)]
    pub gzip: bool,
    /// `fail`, `skip` or `quarantine`.
    pub malformed: String,
    /// Required for `quarantine`; must not exist.
    #[serde(default)]
    pub quarantine_path: Option<String>,
    /// `insert`, `append` or `upsert`.
    pub mode: String,
    #[serde(default)]
    pub key_fields: Vec<String>,
    pub batch_size: u32,
    #[serde(default)]
    pub stop_on_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpArgs {
    pub op_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDto {
    pub path: String,
    pub records: u64,
    pub bytes: u64,
    pub sha256: String,
    pub ignored_fields: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferSummaryDto {
    pub records_read: u64,
    pub inserted: u64,
    pub updated: u64,
    pub skipped: u64,
    pub failed: u64,
    pub malformed: u64,
    pub quarantine_path: Option<String>,
    pub messages: Vec<String>,
    pub file: Option<FileDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferStatusDto {
    /// `running`, `done`, `failed` or `cancelled`.
    pub phase: String,
    pub records: u64,
    pub summary: TransferSummaryDto,
    pub error: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PickPathArgs {
    /// Save dialog (export) or open dialog (import).
    pub save: bool,
    #[serde(default)]
    pub default_name: Option<String>,
    /// Extensions offered in the filter, without dots.
    #[serde(default)]
    pub extensions: Vec<String>,
}

/// Format and compression implied by a file name, e.g. `orders.jsonl.gz`.
pub fn detect_format(path: &str) -> Option<(&'static str, bool)> {
    let name = path.rsplit(['/', '\\']).next()?.to_ascii_lowercase();
    let (name, gzip) = match name.strip_suffix(".gz") {
        Some(rest) => (rest.to_owned(), true),
        None => (name, false),
    };
    let format = match name.rsplit_once('.')?.1 {
        "json" => "json",
        "jsonl" | "ndjson" => "jsonl",
        "bson" => "bson",
        "csv" => "csv",
        _ => return None,
    };
    Some((format, gzip))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_formats_from_paths() {
        assert_eq!(
            detect_format("/tmp/a/orders.JSONL.gz"),
            Some(("jsonl", true))
        );
        assert_eq!(detect_format("C:\\dump\\x.bson"), Some(("bson", false)));
        assert_eq!(detect_format("/tmp/x.txt"), None);
    }
}
