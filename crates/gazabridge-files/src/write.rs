//! Streaming writers.

use std::collections::BTreeSet;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flate2::write::GzEncoder;
use gazabridge_bson::bson::Bson;
use gazabridge_domain::conversion::scalar_to_text;
use gazabridge_domain::{Record, Value, ejson};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::{Compression, FileError, FileFormat, FileResult, JsonFlavor};

/// What to do with a record field that is not a CSV column.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtraFields {
    /// Stop the export: nothing is dropped silently.
    #[default]
    Fail,
    /// Leave the field out and list it in the summary.
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvOptions {
    pub delimiter: u8,
    pub header: bool,
    /// Column order; required.
    pub columns: Vec<String>,
    /// Text written for null. Missing fields are always empty.
    pub null_text: String,
    pub extra_fields: ExtraFields,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            header: true,
            columns: Vec::new(),
            null_text: String::new(),
            extra_fields: ExtraFields::Fail,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteOptions {
    pub format: FileFormat,
    #[serde(default)]
    pub json_flavor: JsonFlavor,
    #[serde(default)]
    pub compression: Compression,
    /// Indented JSON (JSON arrays only).
    #[serde(default)]
    pub pretty: bool,
    /// Replace an existing file at the target path.
    #[serde(default)]
    pub overwrite: bool,
    #[serde(default)]
    pub csv: CsvOptions,
}

impl WriteOptions {
    pub fn new(format: FileFormat) -> Self {
        Self {
            format,
            json_flavor: JsonFlavor::Typed,
            compression: Compression::None,
            pretty: false,
            overwrite: false,
            csv: CsvOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteSummary {
    pub path: PathBuf,
    pub records: u64,
    /// Bytes on disk (after compression).
    pub bytes: u64,
    /// SHA-256 of the file on disk, hex.
    pub sha256: String,
    /// CSV fields left out under [`ExtraFields::Ignore`].
    pub ignored_fields: BTreeSet<String>,
}

/// Counts and hashes what reaches the temporary file.
#[derive(Debug)]
struct Hashing {
    inner: BufWriter<NamedTempFile>,
    hasher: Sha256,
    bytes: u64,
}

impl Write for Hashing {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug)]
enum Sink {
    Plain(Hashing),
    Gzip(GzEncoder<Hashing>),
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(w) => w.write(buf),
            Self::Gzip(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(w) => w.flush(),
            Self::Gzip(w) => w.flush(),
        }
    }
}

impl Sink {
    fn finish(self) -> std::io::Result<Hashing> {
        match self {
            Self::Plain(w) => Ok(w),
            Self::Gzip(w) => w.finish(),
        }
    }
}

enum Encoder {
    Json { sink: Sink, first: bool },
    Jsonl(Sink),
    Bson(Sink),
    Csv(Box<csv::Writer<Sink>>),
}

impl std::fmt::Debug for Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Json { .. } => "Json",
            Self::Jsonl(_) => "Jsonl",
            Self::Bson(_) => "Bson",
            Self::Csv(_) => "Csv",
        })
    }
}

/// Writes records to a file. Dropping it without [`FileWriter::finish`]
/// discards the partial file.
#[derive(Debug)]
pub struct FileWriter {
    path: PathBuf,
    options: WriteOptions,
    encoder: Option<Encoder>,
    records: u64,
    ignored: BTreeSet<String>,
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> FileError + '_ {
    move |e| FileError::io(path, e)
}

/// Mongo Extended JSON for a record.
fn mongo_json(record: &Record, canonical: bool) -> FileResult<serde_json::Value> {
    let doc = gazabridge_bson::record_to_document(record)
        .map_err(|e| FileError::Conversion(e.to_string()))?;
    let b = Bson::Document(doc);
    Ok(if canonical {
        b.into_canonical_extjson()
    } else {
        b.into_relaxed_extjson()
    })
}

fn json_of(record: &Record, flavor: JsonFlavor) -> FileResult<serde_json::Value> {
    match flavor {
        JsonFlavor::Typed => Ok(ejson::record_to_ejson(record)),
        JsonFlavor::MongoRelaxed => mongo_json(record, false),
        JsonFlavor::MongoCanonical => mongo_json(record, true),
    }
}

fn csv_cell(v: &Value, null_text: &str) -> String {
    match v {
        Value::Null => null_text.to_owned(),
        Value::Missing => String::new(),
        Value::Binary(b) => STANDARD.encode(b),
        Value::Array(_)
        | Value::Object(_)
        | Value::Map(_)
        | Value::Geometry { .. }
        | Value::Invalid { .. } => ejson::to_ejson(v).to_string(),
        Value::Extension { text: None, .. } => ejson::to_ejson(v).to_string(),
        other => scalar_to_text(other).unwrap_or_else(|| ejson::to_ejson(other).to_string()),
    }
}

impl FileWriter {
    /// Starts writing `path`. Fails with [`FileError::Exists`] when the
    /// file exists and `overwrite` is false.
    pub fn create(path: &Path, options: WriteOptions) -> FileResult<Self> {
        if path.exists() && !options.overwrite {
            return Err(FileError::Exists(path.to_path_buf()));
        }
        if options.format == FileFormat::Csv && options.csv.columns.is_empty() {
            return Err(FileError::Invalid(
                "a CSV export needs a list of columns".into(),
            ));
        }
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let tmp = tempfile::Builder::new()
            .prefix(".gazabridge-")
            .suffix(".partial")
            .tempfile_in(&dir)
            .map_err(io(path))?;
        let hashing = Hashing {
            inner: BufWriter::with_capacity(256 * 1024, tmp),
            hasher: Sha256::new(),
            bytes: 0,
        };
        let mut sink = match options.compression {
            Compression::None => Sink::Plain(hashing),
            Compression::Gzip => {
                Sink::Gzip(GzEncoder::new(hashing, flate2::Compression::default()))
            }
        };
        let encoder = match options.format {
            FileFormat::Json => {
                sink.write_all(b"[").map_err(io(path))?;
                Encoder::Json { sink, first: true }
            }
            FileFormat::Jsonl => Encoder::Jsonl(sink),
            FileFormat::Bson => Encoder::Bson(sink),
            FileFormat::Csv => {
                let mut w = csv::WriterBuilder::new()
                    .delimiter(options.csv.delimiter)
                    .from_writer(sink);
                if options.csv.header {
                    w.write_record(&options.csv.columns)
                        .map_err(|e| FileError::Conversion(e.to_string()))?;
                }
                Encoder::Csv(Box::new(w))
            }
        };
        Ok(Self {
            path: path.to_path_buf(),
            options,
            encoder: Some(encoder),
            records: 0,
            ignored: BTreeSet::new(),
        })
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn write(&mut self, records: &[Record]) -> FileResult<()> {
        let path = self.path.clone();
        let flavor = self.options.json_flavor;
        let pretty = self.options.pretty;
        let Some(encoder) = self.encoder.as_mut() else {
            return Err(FileError::Invalid("the writer is finished".into()));
        };
        for record in records {
            match encoder {
                Encoder::Json { sink, first } => {
                    let j = json_of(record, flavor)?;
                    let text = if pretty {
                        serde_json::to_string_pretty(&j).map(|t| t.replace('\n', "\n  "))
                    } else {
                        serde_json::to_string(&j)
                    }
                    .map_err(|e| FileError::Conversion(e.to_string()))?;
                    sink.write_all(if *first { b"\n  " } else { b",\n  " })
                        .map_err(io(&path))?;
                    sink.write_all(text.as_bytes()).map_err(io(&path))?;
                    *first = false;
                }
                Encoder::Jsonl(sink) => {
                    let j = json_of(record, flavor)?;
                    serde_json::to_writer(&mut *sink, &j)
                        .map_err(|e| FileError::Conversion(e.to_string()))?;
                    sink.write_all(b"\n").map_err(io(&path))?;
                }
                Encoder::Bson(sink) => {
                    let doc = gazabridge_bson::record_to_document(record)
                        .map_err(|e| FileError::Conversion(e.to_string()))?;
                    doc.to_writer(&mut *sink)
                        .map_err(|e| FileError::Conversion(e.to_string()))?;
                }
                Encoder::Csv(w) => {
                    let csv = &self.options.csv;
                    for k in record.fields.keys() {
                        if !csv.columns.iter().any(|c| c == k) {
                            match csv.extra_fields {
                                ExtraFields::Fail => return Err(FileError::ExtraField(k.clone())),
                                ExtraFields::Ignore => {
                                    self.ignored.insert(k.clone());
                                }
                            }
                        }
                    }
                    let row: Vec<String> = csv
                        .columns
                        .iter()
                        .map(|c| csv_cell(record.get(c), &csv.null_text))
                        .collect();
                    w.write_record(&row)
                        .map_err(|e| FileError::Conversion(e.to_string()))?;
                }
            }
            self.records += 1;
        }
        Ok(())
    }

    /// Completes the file and moves it into place.
    pub fn finish(mut self) -> FileResult<WriteSummary> {
        let path = self.path.clone();
        let Some(encoder) = self.encoder.take() else {
            return Err(FileError::Invalid("the writer is finished".into()));
        };
        let sink = match encoder {
            Encoder::Json { mut sink, first } => {
                sink.write_all(if first { b"]\n" } else { b"\n]\n" })
                    .map_err(io(&path))?;
                sink
            }
            Encoder::Jsonl(sink) | Encoder::Bson(sink) => sink,
            Encoder::Csv(w) => w
                .into_inner()
                .map_err(|e| FileError::io(&path, e.into_error()))?,
        };
        let hashing = sink.finish().map_err(io(&path))?;
        let Hashing {
            inner,
            hasher,
            bytes,
        } = hashing;
        let tmp = inner
            .into_inner()
            .map_err(|e| FileError::io(&path, e.into_error()))?;
        tmp.as_file().sync_all().map_err(io(&path))?;
        if self.options.overwrite {
            tmp.persist(&path)
                .map_err(|e| FileError::io(&path, e.error))?;
        } else {
            // Atomic: fails instead of replacing a file created meanwhile.
            tmp.persist_noclobber(&path).map_err(|e| {
                if e.error.kind() == std::io::ErrorKind::AlreadyExists {
                    FileError::Exists(path.clone())
                } else {
                    FileError::io(&path, e.error)
                }
            })?;
        }
        Ok(WriteSummary {
            path,
            records: self.records,
            bytes,
            sha256: hex::encode(hasher.finalize()),
            ignored_fields: std::mem::take(&mut self.ignored),
        })
    }
}
