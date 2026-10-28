//! Streaming file formats.
//!
//! * JSON (one array of records, or a single object), JSONL/NDJSON and BSON
//!   (concatenated documents, the `mongodump` format) can be read and
//!   written; CSV can be written.
//! * JSON text is either GazaBridge typed JSON (exact for every canonical
//!   value) or MongoDB Extended JSON v2, relaxed or canonical (what
//!   `mongoexport` / `mongoimport` use).
//! * gzip compression on both sides.
//! * Readers never load a whole file; malformed records stop the read, are
//!   skipped, or are copied to a quarantine file, as the caller chooses,
//!   and every problem is reported with its line or record number.
//! * Writers write to a temporary file next to the target and move it into
//!   place only when finished, refusing to replace an existing file unless
//!   asked to. An unfinished or failed export leaves nothing behind.

mod read;
mod scan;
mod write;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub use read::{FileReader, ReadIssue, ReadOptions, ReadStats, SourceRecord};
pub use write::{CsvOptions, ExtraFields, FileWriter, WriteOptions, WriteSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Json,
    Jsonl,
    Bson,
    Csv,
    /// Excel workbook; one sheet, same column options as CSV. Write-only.
    Xlsx,
}

impl FileFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Bson => "bson",
            Self::Csv => "csv",
            Self::Xlsx => "xlsx",
        }
    }

    pub fn readable(self) -> bool {
        !matches!(self, Self::Csv | Self::Xlsx)
    }
}

/// How values JSON cannot represent natively are written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonFlavor {
    /// GazaBridge typed JSON: exact for every canonical value.
    #[default]
    Typed,
    /// MongoDB Extended JSON v2, relaxed (`mongoexport` default).
    MongoRelaxed,
    /// MongoDB Extended JSON v2, canonical (every number typed).
    MongoCanonical,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
}

/// What to do with a record that cannot be parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum MalformedPolicy {
    /// Stop at the first malformed record.
    #[default]
    Fail,
    /// Skip it and report it.
    Skip,
    /// Copy its raw bytes to this file (created new) and report it.
    Quarantine { path: PathBuf },
}

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0} already exists; choose another name or allow replacing it")]
    Exists(PathBuf),
    /// A record could not be parsed. `location` is a 1-based line number
    /// (JSONL) or record number (JSON, BSON).
    #[error("{kind} {location}: {message}")]
    Malformed {
        kind: &'static str,
        location: u64,
        message: String,
    },
    #[error("{0}")]
    Conversion(String),
    #[error(
        "field `{0}` is not one of the CSV columns; add it to the columns or choose to ignore extra fields"
    )]
    ExtraField(String),
    #[error("{0}")]
    Invalid(String),
}

impl FileError {
    pub fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

pub type FileResult<T> = Result<T, FileError>;

/// Format and compression implied by a file name (`orders.jsonl.gz`).
pub fn detect(path: &Path) -> Option<(FileFormat, Compression)> {
    let name = path.file_name()?.to_str()?.to_ascii_lowercase();
    let (name, compression) = match name.strip_suffix(".gz") {
        Some(rest) => (rest.to_owned(), Compression::Gzip),
        None => (name, Compression::None),
    };
    let format = match name.rsplit_once('.')?.1 {
        "json" => FileFormat::Json,
        "jsonl" | "ndjson" => FileFormat::Jsonl,
        "bson" => FileFormat::Bson,
        "csv" => FileFormat::Csv,
        "xlsx" => FileFormat::Xlsx,
        _ => return None,
    };
    Some((format, compression))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_format_and_compression_from_names() {
        assert_eq!(
            detect(Path::new("a/orders.JSONL.gz")),
            Some((FileFormat::Jsonl, Compression::Gzip))
        );
        assert_eq!(
            detect(Path::new("dump/shop/orders.bson")),
            Some((FileFormat::Bson, Compression::None))
        );
        assert_eq!(
            detect(Path::new("x.ndjson")),
            Some((FileFormat::Jsonl, Compression::None))
        );
        assert_eq!(detect(Path::new("x.txt")), None);
        assert_eq!(detect(Path::new("noext")), None);
    }
}
