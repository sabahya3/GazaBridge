//! Streaming readers.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::MultiGzDecoder;
use gazabridge_bson::bson::{Bson, Document};
use gazabridge_domain::{Record, ejson};
use serde::{Deserialize, Serialize};

use crate::scan::{ArrayScanner, MAX_ELEMENT_BYTES, Next};
use crate::{Compression, FileError, FileFormat, FileResult, JsonFlavor, MalformedPolicy};

/// Largest BSON document accepted (MongoDB's limit is 16 MiB; dumps of
/// oplog entries can be slightly larger).
const MAX_BSON_BYTES: usize = 48 * 1024 * 1024;
/// Problems kept in [`ReadStats::issues`]; the counters keep counting.
const MAX_ISSUES: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadOptions {
    pub format: FileFormat,
    /// How to read `$`-tagged values in JSON. `Typed` also accepts the
    /// common Extended JSON forms; `MongoRelaxed`/`MongoCanonical` read
    /// every BSON type (regex, timestamp, …) exactly.
    #[serde(default)]
    pub json_flavor: JsonFlavor,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub malformed: MalformedPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadIssue {
    /// `line` or `record`.
    pub kind: String,
    pub location: u64,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStats {
    pub records: u64,
    pub skipped: u64,
    pub quarantined: u64,
    pub issues: Vec<ReadIssue>,
    pub quarantine_path: Option<PathBuf>,
}

/// A parsed record and where it came from (1-based line or record number).
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord {
    pub record: Record,
    pub location: u64,
}

enum Source {
    Json(ArrayScanner<Box<dyn BufRead + Send>>),
    Jsonl(Box<dyn BufRead + Send>),
    Bson(Box<dyn BufRead + Send>),
    /// Rows under a header row of field names.
    Csv {
        reader: Box<csv::Reader<Box<dyn BufRead + Send>>>,
        headers: Vec<String>,
    },
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Json(_) => "Json",
            Self::Jsonl(_) => "Jsonl",
            Self::Bson(_) => "Bson",
            Self::Csv { .. } => "Csv",
        })
    }
}

#[derive(Debug)]
pub struct FileReader {
    path: PathBuf,
    options: ReadOptions,
    source: Source,
    /// Line (JSONL) or record (JSON, BSON) counter.
    position: u64,
    stats: ReadStats,
    quarantine: Option<BufWriter<File>>,
    done: bool,
}

enum Raw {
    Json(Vec<u8>),
    Bson(Vec<u8>),
    Csv(Vec<String>),
}

/// The delimiter used most in the header line (`,` when none is).
fn sniff_delimiter(reader: &mut Box<dyn BufRead + Send>) -> std::io::Result<u8> {
    let buf = reader.fill_buf()?;
    let line = buf.split(|b| *b == b'\n').next().unwrap_or_default();
    let mut best = (b',', 0);
    let mut quoted = false;
    let mut counts = [0usize; 4];
    const CANDIDATES: [u8; 4] = *b",;\t|";
    for b in line {
        if *b == b'"' {
            quoted = !quoted;
        } else if !quoted && let Some(i) = CANDIDATES.iter().position(|c| c == b) {
            counts[i] += 1;
        }
    }
    for (i, n) in counts.iter().enumerate() {
        if *n > best.1 {
            best = (CANDIDATES[i], *n);
        }
    }
    Ok(best.0)
}

impl FileReader {
    pub fn open(path: &Path, options: ReadOptions) -> FileResult<Self> {
        if !options.format.readable() {
            return Err(FileError::Invalid(format!(
                "{} files can only be written",
                options.format.extension()
            )));
        }
        let file = File::open(path).map_err(|e| FileError::io(path, e))?;
        let reader: Box<dyn BufRead + Send> = match options.compression {
            Compression::None => Box::new(BufReader::with_capacity(256 * 1024, file)),
            Compression::Gzip => Box::new(BufReader::with_capacity(
                256 * 1024,
                MultiGzDecoder::new(file),
            )),
        };
        let source = match options.format {
            FileFormat::Json => Source::Json(ArrayScanner::new(reader)),
            FileFormat::Jsonl => Source::Jsonl(reader),
            FileFormat::Bson => Source::Bson(reader),
            FileFormat::Csv => {
                let mut reader = reader;
                let delimiter = sniff_delimiter(&mut reader).map_err(|e| FileError::io(path, e))?;
                let mut reader = csv::ReaderBuilder::new()
                    .delimiter(delimiter)
                    .has_headers(true)
                    .flexible(true)
                    .from_reader(reader);
                let headers: Vec<String> = reader
                    .headers()
                    .map_err(|e| FileError::Malformed {
                        kind: "line",
                        location: 1,
                        message: format!("unreadable header row: {e}"),
                    })?
                    .iter()
                    .map(|h| h.trim().to_owned())
                    .collect();
                let mut seen = std::collections::BTreeSet::new();
                for h in &headers {
                    if h.is_empty() {
                        return Err(FileError::Invalid(
                            "the header row has an empty column name".into(),
                        ));
                    }
                    if !seen.insert(h) {
                        return Err(FileError::Invalid(format!(
                            "the header row names `{h}` twice"
                        )));
                    }
                }
                Source::Csv {
                    reader: Box::new(reader),
                    headers,
                }
            }
            FileFormat::Xlsx => unreachable!("checked above"),
        };
        let quarantine_path = match &options.malformed {
            MalformedPolicy::Quarantine { path } => Some(path.clone()),
            _ => None,
        };
        Ok(Self {
            path: path.to_path_buf(),
            options,
            source,
            position: 0,
            stats: ReadStats {
                quarantine_path,
                ..ReadStats::default()
            },
            quarantine: None,
            done: false,
        })
    }

    pub fn stats(&self) -> &ReadStats {
        &self.stats
    }

    fn kind(&self) -> &'static str {
        match self.source {
            Source::Jsonl(_) | Source::Csv { .. } => "line",
            _ => "record",
        }
    }

    fn io_err(&self, e: std::io::Error) -> FileError {
        FileError::io(&self.path, e)
    }

    /// Next raw record, or `None` at the end.
    fn raw(&mut self) -> FileResult<Option<Raw>> {
        let path = self.path.clone();
        match &mut self.source {
            Source::Json(scanner) => match scanner.next() {
                Ok(Next::Element(bytes)) => {
                    self.position += 1;
                    Ok(Some(Raw::Json(bytes)))
                }
                Ok(Next::End) => Ok(None),
                Err(e) => Err(FileError::Malformed {
                    kind: "record",
                    location: self.position + 1,
                    message: format!("{e} (byte {})", scanner.offset()),
                }),
            },
            Source::Jsonl(reader) => loop {
                let mut line = Vec::new();
                let n = reader
                    .by_ref()
                    .take(MAX_ELEMENT_BYTES as u64 + 1)
                    .read_until(b'\n', &mut line)
                    .map_err(|e| FileError::io(&path, e))?;
                if n == 0 {
                    return Ok(None);
                }
                self.position += 1;
                if line.len() > MAX_ELEMENT_BYTES {
                    return Err(FileError::Malformed {
                        kind: "line",
                        location: self.position,
                        message: "line is longer than 64 MiB".into(),
                    });
                }
                if self.position == 1 && line.starts_with(&[0xEF, 0xBB, 0xBF]) {
                    line.drain(..3);
                }
                while matches!(line.last(), Some(b'\n' | b'\r')) {
                    line.pop();
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(Raw::Json(line)));
            },
            Source::Csv { reader, .. } => {
                let mut row = csv::StringRecord::new();
                match reader.read_record(&mut row) {
                    Ok(false) => Ok(None),
                    Ok(true) => {
                        // The csv crate's line numbers are one lower than the
                        // file's (whose line 1 is the header).
                        self.position = row.position().map_or(self.position + 1, |p| p.line() + 1);
                        Ok(Some(Raw::Csv(row.iter().map(str::to_owned).collect())))
                    }
                    Err(e) => Err(FileError::Malformed {
                        kind: "line",
                        location: e.position().map_or(self.position + 1, |p| p.line() + 1),
                        message: e.to_string(),
                    }),
                }
            }
            Source::Bson(reader) => {
                let mut len = [0u8; 4];
                let mut got = 0;
                while got < 4 {
                    let n = reader
                        .read(&mut len[got..])
                        .map_err(|e| FileError::io(&path, e))?;
                    if n == 0 {
                        break;
                    }
                    got += n;
                }
                if got == 0 {
                    return Ok(None);
                }
                self.position += 1;
                let size = i32::from_le_bytes(len);
                if got < 4 || size < 5 || size as usize > MAX_BSON_BYTES {
                    return Err(FileError::Malformed {
                        kind: "record",
                        location: self.position,
                        message: format!(
                            "invalid BSON document length {size}; the file is truncated or not BSON"
                        ),
                    });
                }
                let mut buf = vec![0u8; size as usize];
                buf[..4].copy_from_slice(&len);
                reader
                    .read_exact(&mut buf[4..])
                    .map_err(|_| FileError::Malformed {
                        kind: "record",
                        location: self.position,
                        message: "the file ends inside a BSON document".into(),
                    })?;
                Ok(Some(Raw::Bson(buf)))
            }
        }
    }

    fn parse(&self, raw: &Raw) -> Result<Record, String> {
        match raw {
            Raw::Json(bytes) => {
                let j: serde_json::Value =
                    serde_json::from_slice(bytes).map_err(|e| format!("invalid JSON: {e}"))?;
                match self.options.json_flavor {
                    JsonFlavor::Typed => ejson::record_from_ejson(&j).map_err(|e| e.to_string()),
                    JsonFlavor::MongoRelaxed | JsonFlavor::MongoCanonical => {
                        match Bson::try_from(j) {
                            Ok(Bson::Document(doc)) => {
                                Ok(gazabridge_bson::document_to_record(&doc))
                            }
                            Ok(_) => Err("a record must be a JSON object".into()),
                            Err(e) => Err(format!("invalid Extended JSON: {e}")),
                        }
                    }
                }
            }
            Raw::Csv(cells) => {
                let Source::Csv { headers, .. } = &self.source else {
                    return Err("not a CSV file".into());
                };
                if cells.len() != headers.len() {
                    return Err(format!(
                        "{} values for {} columns",
                        cells.len(),
                        headers.len()
                    ));
                }
                let mut r = Record::with_capacity(cells.len());
                for (h, c) in headers.iter().zip(cells) {
                    // An empty cell is an absent field (NULL or the column
                    // default in a table).
                    if !c.is_empty() {
                        r.insert(h.clone(), gazabridge_domain::Value::String(c.clone()));
                    }
                }
                Ok(r)
            }
            Raw::Bson(bytes) => Document::from_reader(&bytes[..])
                .map(|d| gazabridge_bson::document_to_record(&d))
                .map_err(|e| format!("invalid BSON document: {e}")),
        }
    }

    fn malformed(&mut self, raw: &Raw, message: String) -> FileResult<()> {
        let kind = self.kind();
        let issue = ReadIssue {
            kind: kind.to_owned(),
            location: self.position,
            message: message.clone(),
        };
        match &self.options.malformed {
            MalformedPolicy::Fail => {
                return Err(FileError::Malformed {
                    kind,
                    location: self.position,
                    message,
                });
            }
            MalformedPolicy::Skip => self.stats.skipped += 1,
            MalformedPolicy::Quarantine { path } => {
                if self.quarantine.is_none() {
                    let file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path)
                        .map_err(|e| match e.kind() {
                            std::io::ErrorKind::AlreadyExists => FileError::Exists(path.clone()),
                            _ => FileError::io(path, e),
                        })?;
                    self.quarantine = Some(BufWriter::new(file));
                }
                let q = self
                    .quarantine
                    .as_mut()
                    .ok_or_else(|| FileError::Invalid("quarantine unavailable".into()))?;
                let r = match raw {
                    Raw::Json(bytes) => q.write_all(bytes).and_then(|()| q.write_all(b"\n")),
                    Raw::Bson(bytes) => q.write_all(bytes),
                    Raw::Csv(cells) => {
                        let mut w = csv::Writer::from_writer(Vec::new());
                        let line = w
                            .write_record(cells)
                            .map_err(std::io::Error::other)
                            .and_then(|()| {
                                w.into_inner()
                                    .map_err(|e| std::io::Error::other(e.to_string()))
                            });
                        line.and_then(|l| q.write_all(&l))
                    }
                };
                r.map_err(|e| self.io_err(e))?;
                self.stats.quarantined += 1;
            }
        }
        if self.stats.issues.len() < MAX_ISSUES {
            self.stats.issues.push(issue);
        }
        Ok(())
    }

    /// Up to `max` records; empty at the end of the file.
    pub fn next_batch(&mut self, max: usize) -> FileResult<Vec<SourceRecord>> {
        let mut out = Vec::with_capacity(max.min(4096));
        while !self.done && out.len() < max {
            let Some(raw) = self.raw()? else {
                self.done = true;
                break;
            };
            match self.parse(&raw) {
                Ok(record) => {
                    self.stats.records += 1;
                    out.push(SourceRecord {
                        record,
                        location: self.position,
                    });
                }
                Err(message) => self.malformed(&raw, message)?,
            }
        }
        Ok(out)
    }

    /// Flushes the quarantine file and returns the statistics.
    pub fn finish(mut self) -> FileResult<ReadStats> {
        if let Some(mut q) = self.quarantine.take() {
            q.flush().map_err(|e| self.io_err(e))?;
            q.get_ref().sync_all().map_err(|e| self.io_err(e))?;
        }
        Ok(self.stats)
    }
}
/// A CSV cell for a column whose values GazaBridge writes in a text form:
/// JSON text for JSON columns, base64 for binary ones. `None` leaves the
/// text as it is (the database then reports what does not fit).
pub fn decode_cell(
    column: &gazabridge_domain::CanonicalType,
    text: &str,
) -> Option<gazabridge_domain::Value> {
    use base64::Engine;
    use gazabridge_domain::{CanonicalType as T, Value};
    match column {
        T::Json | T::Object | T::Array(_) | T::Map { .. } => {
            serde_json::from_str(text).ok().map(Value::Json)
        }
        T::Binary { .. } => base64::engine::general_purpose::STANDARD
            .decode(text)
            .ok()
            .map(Value::Binary),
        _ => None,
    }
}

/// Text cells read as the values they plainly are, for targets without
/// column types (MongoDB): `true`/`false`, integers without leading zeros,
/// and decimals whose shortest form is the text itself. Anything else (ZIP
/// codes like `007`, `1.50`, dates) stays text.
pub fn infer_cell_types(record: &mut Record) {
    use gazabridge_domain::Value;
    for v in record.fields.values_mut() {
        let Value::String(s) = &v.value else { continue };
        let typed = match s.as_str() {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            t => {
                let digits = t.strip_prefix('-').unwrap_or(t);
                let plain_int = !digits.is_empty()
                    && digits.bytes().all(|b| b.is_ascii_digit())
                    && (digits == "0" || !digits.starts_with('0'))
                    && t != "-0";
                if plain_int {
                    t.parse::<i64>().ok().map(Value::Int)
                } else if t.contains('.') && !t.contains(['e', 'E']) {
                    t.parse::<f64>()
                        .ok()
                        .filter(|f| f.is_finite() && format!("{f:?}") == t)
                        .map(Value::Float)
                } else {
                    None
                }
            }
        };
        if let Some(t) = typed {
            v.value = t;
        }
    }
}
