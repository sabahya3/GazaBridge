//! Reading and filtering log files for the log viewer and diagnostics.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{LOG_FILE_NAME, LogLevel};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: Option<DateTime<Utc>>,
    pub level: LogLevel,
    pub target: String,
    pub message: String,
    pub job_id: Option<String>,
    pub run_id: Option<String>,
    pub correlation_id: Option<String>,
    /// The original JSON line.
    pub raw: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogQuery {
    /// Least severe level to include (e.g. `Warn` includes errors and warnings).
    pub max_verbosity: Option<LogLevel>,
    /// Case-insensitive substring searched in the whole line.
    pub search: Option<String>,
    pub job_id: Option<String>,
    pub run_id: Option<String>,
    pub correlation_id: Option<String>,
    /// Newest entries to return.
    pub limit: usize,
}

/// Log files in the directory, oldest first (`name.N` … `name.1`, `name`).
pub fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut rotated: Vec<(usize, PathBuf)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let n = name
                .strip_prefix(&format!("{LOG_FILE_NAME}."))?
                .parse()
                .ok()?;
            Some((n, e.path()))
        })
        .collect();
    rotated.sort_by_key(|a| std::cmp::Reverse(a.0));
    let mut out: Vec<PathBuf> = rotated.into_iter().map(|(_, p)| p).collect();
    let active = dir.join(LOG_FILE_NAME);
    if active.exists() {
        out.push(active);
    }
    out
}

fn span_field(v: &serde_json::Value, name: &str) -> Option<String> {
    let from_span = |s: &serde_json::Value| s.get(name).and_then(|f| f.as_str().map(str::to_owned));
    v.get("span")
        .and_then(from_span)
        .or_else(|| v.get("spans")?.as_array()?.iter().rev().find_map(from_span))
        .or_else(|| v.get("fields").and_then(from_span))
}

/// Parses one JSON log line produced by the file layer.
pub fn parse_line(line: &str) -> Option<LogEntry> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let level = LogLevel::parse(v.get("level")?.as_str()?)?;
    Some(LogEntry {
        timestamp: v
            .get("timestamp")
            .and_then(|t| t.as_str())
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&Utc)),
        level,
        target: v
            .get("target")
            .and_then(|t| t.as_str())
            .unwrap_or_default()
            .to_owned(),
        message: v
            .get("fields")
            .and_then(|f| f.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_owned(),
        job_id: span_field(&v, "job_id"),
        run_id: span_field(&v, "run_id"),
        correlation_id: span_field(&v, "correlation_id"),
        raw: line.to_owned(),
    })
}

/// Reads matching entries across all log files, newest last, keeping at
/// most `query.limit` entries (0 means 500).
pub fn query(dir: &Path, query: &LogQuery) -> std::io::Result<Vec<LogEntry>> {
    let limit = if query.limit == 0 { 500 } else { query.limit };
    let search = query.search.as_ref().map(|s| s.to_lowercase());
    let mut out = std::collections::VecDeque::with_capacity(limit);
    for path in log_files(dir) {
        let reader = BufReader::new(File::open(&path)?);
        for line in reader.lines() {
            let line = line?;
            let Some(entry) = parse_line(&line) else {
                continue;
            };
            if query.max_verbosity.is_some_and(|max| entry.level > max) {
                continue;
            }
            if let Some(s) = &search
                && !line.to_lowercase().contains(s)
            {
                continue;
            }
            let eq = |want: &Option<String>, got: &Option<String>| want.is_none() || want == got;
            if !eq(&query.job_id, &entry.job_id)
                || !eq(&query.run_id, &entry.run_id)
                || !eq(&query.correlation_id, &entry.correlation_id)
            {
                continue;
            }
            if out.len() == limit {
                out.pop_front();
            }
            out.push_back(entry);
        }
    }
    Ok(out.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    const LINES: &str = r#"{"timestamp":"2026-10-24T10:00:00.000Z","level":"INFO","fields":{"message":"run started"},"target":"gazabridge_migration","span":{"run_id":"r1","job_id":"j1","name":"run"},"spans":[{"run_id":"r1","job_id":"j1","name":"run"}]}
{"timestamp":"2026-10-24T10:00:01.000Z","level":"WARN","fields":{"message":"lossy conversion approved"},"target":"gazabridge_migration","span":{"run_id":"r1","job_id":"j1","name":"run"}}
not json
{"timestamp":"2026-10-24T10:00:02.000Z","level":"ERROR","fields":{"message":"connection reset"},"target":"gazabridge_connector_postgres","span":{"run_id":"r2","name":"run"}}
"#;

    #[test]
    fn filters_by_level_search_and_run() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::File::create(dir.path().join(LOG_FILE_NAME))
            .unwrap()
            .write_all(LINES.as_bytes())
            .unwrap();

        let all = query(dir.path(), &LogQuery::default()).unwrap();
        assert_eq!(all.len(), 3);

        let warn = query(
            dir.path(),
            &LogQuery {
                max_verbosity: Some(LogLevel::Warn),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(warn.len(), 2);

        let r1 = query(
            dir.path(),
            &LogQuery {
                run_id: Some("r1".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(r1.len(), 2);
        assert_eq!(r1[0].job_id.as_deref(), Some("j1"));

        let s = query(
            dir.path(),
            &LogQuery {
                search: Some("RESET".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].level, LogLevel::Error);

        let limited = query(
            dir.path(),
            &LogQuery {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(limited[0].message, "connection reset");
    }

    #[test]
    fn rotated_files_are_read_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(format!("{LOG_FILE_NAME}.2")), b"").unwrap();
        std::fs::write(dir.path().join(format!("{LOG_FILE_NAME}.1")), b"").unwrap();
        std::fs::write(dir.path().join(LOG_FILE_NAME), b"").unwrap();
        let files = log_files(dir.path());
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            ["gazabridge.log.2", "gazabridge.log.1", "gazabridge.log"]
        );
    }
}
