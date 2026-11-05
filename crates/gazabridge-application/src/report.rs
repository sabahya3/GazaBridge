//! Run reports (roadmap 7.3): what a copy did, per stream, with its latest
//! validation and events, as JSON, HTML, Markdown or CSV.
//!
//! Reports hold what the run view holds: object names, counts, messages
//! (already scrubbed of secrets) and local file paths. No connection
//! settings or credentials.

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::migration::{RunView, StreamPhase};
use crate::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportFormat {
    Json,
    Html,
    Markdown,
    Csv,
}

impl ReportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Html => "html",
            Self::Markdown => "md",
            Self::Csv => "csv",
        }
    }
}

/// Connection names shown in the report (the run itself holds only ids).
#[derive(Debug, Clone, Default)]
pub struct ReportNames {
    pub source: String,
    pub target: String,
}

fn phase(p: StreamPhase) -> &'static str {
    match p {
        StreamPhase::Pending => "pending",
        StreamPhase::Preparing => "preparing",
        StreamPhase::Copying => "copying",
        StreamPhase::Indexing => "indexing",
        StreamPhase::Done => "done",
        StreamPhase::Following => "following",
        StreamPhase::Stopped => "stopped",
        StreamPhase::Failed => "failed",
    }
}

fn status(v: &RunView) -> String {
    serde_json::to_value(v.status)
        .ok()
        .and_then(|s| s.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn validation_summary(v: &RunView) -> String {
    match &v.validation {
        None => "not validated".into(),
        Some(r) if r.running => "validation running".into(),
        Some(r) => {
            let level = serde_json::to_value(r.level)
                .ok()
                .and_then(|s| s.as_str().map(str::to_owned))
                .unwrap_or_default();
            match (&r.error, r.passed) {
                (Some(e), _) => format!("{level} validation failed: {e}"),
                (None, Some(true)) => format!("{level} validation passed"),
                _ => format!("{level} validation found differences"),
            }
        }
    }
}

/// Per stream: whether its validation passed, when there is one.
fn stream_check(
    v: &RunView,
    i: usize,
) -> Option<&gazabridge_migration::validate::StreamValidation> {
    let source = &v.streams.get(i)?.source;
    v.validation
        .as_ref()?
        .streams
        .iter()
        .find(|s| &s.source == source)
}

pub fn render(v: &RunView, names: &ReportNames, format: ReportFormat) -> AppResult<String> {
    Ok(match format {
        ReportFormat::Json => {
            let mut j = serde_json::to_value(v).map_err(|e| AppError::Invalid(e.to_string()))?;
            if let Some(o) = j.as_object_mut() {
                o.insert("source_name".into(), names.source.clone().into());
                o.insert("target_name".into(), names.target.clone().into());
                o.insert("report_format".into(), "gazabridge.run-report.v1".into());
            }
            serde_json::to_string_pretty(&j).map_err(|e| AppError::Invalid(e.to_string()))?
        }
        ReportFormat::Markdown => markdown(v, names),
        ReportFormat::Html => html(v, names),
        ReportFormat::Csv => csv(v)?,
    })
}

fn md(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}

fn markdown(v: &RunView, n: &ReportNames) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# {}", md(&v.name));
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "- **Status:** {}{}",
        status(v),
        if v.dry_run {
            " (dry run: nothing written)"
        } else {
            ""
        }
    );
    let _ = writeln!(s, "- **Source:** {}", md(&n.source));
    let _ = writeln!(s, "- **Target:** {}", md(&n.target));
    let _ = writeln!(s, "- **Created:** {}", v.created_at.to_rfc3339());
    if let Some(t) = v.started_at {
        let _ = writeln!(s, "- **Started:** {}", t.to_rfc3339());
    }
    if let Some(t) = v.ended_at {
        let _ = writeln!(s, "- **Ended:** {}", t.to_rfc3339());
    }
    let _ = writeln!(s, "- **Validation:** {}", md(&validation_summary(v)));
    if let Some(e) = &v.error {
        let _ = writeln!(s, "- **Error:** {}", md(e));
    }
    let _ = writeln!(s, "\n## Streams\n");
    let _ = writeln!(
        s,
        "| Source | Target | Phase | Read | Inserted | Updated | Skipped | Failed | Check |"
    );
    let _ = writeln!(s, "|---|---|---|---:|---:|---:|---:|---:|---|");
    for (i, st) in v.streams.iter().enumerate() {
        let check =
            stream_check(v, i).map_or("—", |c| if c.passed { "passed" } else { "differences" });
        let _ = writeln!(
            s,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            md(&st.source.to_string()),
            md(&st.target.to_string()),
            phase(st.phase),
            st.stats.read,
            st.stats.inserted,
            st.stats.updated,
            st.stats.skipped,
            st.stats.failed,
            check
        );
    }
    let diffs: Vec<_> = v
        .validation
        .iter()
        .flat_map(|r| r.streams.iter())
        .flat_map(|sv| sv.differences.iter())
        .collect();
    if !diffs.is_empty() {
        let _ = writeln!(s, "\n## Differences\n");
        let _ = writeln!(s, "| Target | Key | Kind | Detail |");
        let _ = writeln!(s, "|---|---|---|---|");
        for d in diffs {
            let _ = writeln!(
                s,
                "| {} | {} | {:?} | {} |",
                md(&d.target),
                md(&d.key),
                d.kind,
                md(d.detail.as_deref().unwrap_or(""))
            );
        }
    }
    let problems: Vec<String> = v
        .streams
        .iter()
        .flat_map(|st| {
            st.error
                .iter()
                .cloned()
                .chain(st.rejections.iter().cloned())
                .map(move |m| format!("{}: {m}", st.source))
        })
        .collect();
    if !problems.is_empty() {
        let _ = writeln!(s, "\n## Errors and rejected records\n");
        for p in problems {
            let _ = writeln!(s, "- {}", md(&p));
        }
    }
    if !v.events.is_empty() {
        let _ = writeln!(s, "\n## Events\n");
        let _ = writeln!(s, "| Time | Level | Stream | Message |");
        let _ = writeln!(s, "|---|---|---|---|");
        for e in &v.events {
            let _ = writeln!(
                s,
                "| {} | {} | {} | {} |",
                e.at.to_rfc3339(),
                md(&e.level),
                md(e.stream.as_deref().unwrap_or("")),
                md(&e.message)
            );
        }
    }
    s
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn html(v: &RunView, n: &ReportNames) -> String {
    let mut s = String::new();
    let _ = write!(
        s,
        "<!doctype html>\n<html lang=\"en\" dir=\"auto\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{}</title>\n<style>\n\
         :root {{ color-scheme: light dark; font-family: system-ui, sans-serif; }}\n\
         body {{ margin: 2rem auto; max-width: 70rem; padding: 0 1rem; line-height: 1.5; }}\n\
         table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; font-size: .9rem; }}\n\
         th, td {{ border: 1px solid #8884; padding: .3rem .5rem; text-align: start; vertical-align: top; }}\n\
         td.n {{ text-align: end; font-variant-numeric: tabular-nums; }}\n\
         .ok {{ color: #1a7f37; }} .bad {{ color: #cf222e; }} code {{ font-size: .85rem; }}\n\
         </style>\n</head>\n<body>\n",
        esc(&v.name)
    );
    let _ = writeln!(s, "<h1><bdi>{}</bdi></h1>", esc(&v.name));
    let good = v.error.is_none()
        && v.validation
            .as_ref()
            .is_none_or(|r| r.passed != Some(false));
    let _ = writeln!(s, "<dl>");
    for (k, val) in [
        (
            "Status",
            format!(
                "{}{}",
                status(v),
                if v.dry_run {
                    " (dry run: nothing written)"
                } else {
                    ""
                }
            ),
        ),
        ("Source", n.source.clone()),
        ("Target", n.target.clone()),
        ("Created", v.created_at.to_rfc3339()),
        (
            "Started",
            v.started_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
        ),
        (
            "Ended",
            v.ended_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
        ),
        ("Validation", validation_summary(v)),
    ] {
        let class = if k == "Validation" || k == "Status" {
            if good {
                " class=\"ok\""
            } else {
                " class=\"bad\""
            }
        } else {
            ""
        };
        let _ = writeln!(s, "<dt>{k}</dt><dd{class}><bdi>{}</bdi></dd>", esc(&val));
    }
    if let Some(e) = &v.error {
        let _ = writeln!(s, "<dt>Error</dt><dd class=\"bad\">{}</dd>", esc(e));
    }
    let _ = writeln!(
        s,
        "</dl>\n<h2>Streams</h2>\n<table>\n<thead><tr><th>Source</th><th>Target</th><th>Phase</th><th>Read</th><th>Inserted</th><th>Updated</th><th>Skipped</th><th>Failed</th><th>Check</th></tr></thead>\n<tbody>"
    );
    for (i, st) in v.streams.iter().enumerate() {
        let check = match stream_check(v, i) {
            Some(c) if c.passed => "<span class=\"ok\">passed</span>",
            Some(_) => "<span class=\"bad\">differences</span>",
            None => "—",
        };
        let _ = writeln!(
            s,
            "<tr><td><code><bdi>{}</bdi></code></td><td><code><bdi>{}</bdi></code></td><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td><td>{check}</td></tr>",
            esc(&st.source.to_string()),
            esc(&st.target.to_string()),
            phase(st.phase),
            st.stats.read,
            st.stats.inserted,
            st.stats.updated,
            st.stats.skipped,
            st.stats.failed,
        );
    }
    let _ = writeln!(s, "</tbody>\n</table>");
    let diffs: Vec<_> = v
        .validation
        .iter()
        .flat_map(|r| r.streams.iter())
        .flat_map(|sv| sv.differences.iter())
        .collect();
    if !diffs.is_empty() {
        let _ = writeln!(
            s,
            "<h2>Differences</h2>\n<table>\n<thead><tr><th>Target</th><th>Key</th><th>Kind</th><th>Detail</th></tr></thead>\n<tbody>"
        );
        for d in diffs {
            let _ = writeln!(
                s,
                "<tr><td><code><bdi>{}</bdi></code></td><td><code><bdi>{}</bdi></code></td><td>{:?}</td><td><bdi>{}</bdi></td></tr>",
                esc(&d.target),
                esc(&d.key),
                d.kind,
                esc(d.detail.as_deref().unwrap_or(""))
            );
        }
        let _ = writeln!(s, "</tbody>\n</table>");
    }
    if !v.events.is_empty() {
        let _ = writeln!(
            s,
            "<h2>Events</h2>\n<table>\n<thead><tr><th>Time</th><th>Level</th><th>Stream</th><th>Message</th></tr></thead>\n<tbody>"
        );
        for e in &v.events {
            let _ = writeln!(
                s,
                "<tr><td>{}</td><td>{}</td><td><code><bdi>{}</bdi></code></td><td><bdi>{}</bdi></td></tr>",
                esc(&e.at.to_rfc3339()),
                esc(&e.level),
                esc(e.stream.as_deref().unwrap_or("")),
                esc(&e.message)
            );
        }
        let _ = writeln!(s, "</tbody>\n</table>");
    }
    let _ = writeln!(
        s,
        "<p><small>GazaBridge run report</small></p>\n</body>\n</html>"
    );
    s
}

fn csv(v: &RunView) -> AppResult<String> {
    let mut w = csv::Writer::from_writer(Vec::new());
    let err = |e: csv::Error| AppError::Invalid(e.to_string());
    w.write_record([
        "source",
        "target",
        "phase",
        "read",
        "inserted",
        "updated",
        "skipped",
        "failed",
        "retries",
        "indexes_created",
        "validated",
        "missing",
        "extra",
        "different",
        "error",
    ])
    .map_err(err)?;
    for (i, st) in v.streams.iter().enumerate() {
        let check = stream_check(v, i);
        let sum = |f: fn(&gazabridge_migration::validate::TargetCheck) -> u64| {
            check.map_or(String::new(), |c| {
                c.targets.iter().map(f).sum::<u64>().to_string()
            })
        };
        w.write_record([
            st.source.to_string(),
            st.target.to_string(),
            phase(st.phase).into(),
            st.stats.read.to_string(),
            st.stats.inserted.to_string(),
            st.stats.updated.to_string(),
            st.stats.skipped.to_string(),
            st.stats.failed.to_string(),
            st.stats.retries.to_string(),
            st.indexes_created.to_string(),
            check.map_or(String::new(), |c| {
                if c.passed { "passed" } else { "differences" }.into()
            }),
            sum(|t| t.missing),
            sum(|t| t.extra),
            sum(|t| t.different),
            st.error.clone().unwrap_or_default(),
        ])
        .map_err(err)?;
    }
    let bytes = w
        .into_inner()
        .map_err(|e| AppError::Invalid(e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| AppError::Invalid(e.to_string()))
}

/// Writes a report; never replaces an existing file.
pub fn save(path: &Path, text: &str) -> AppResult<()> {
    if !path.is_absolute() {
        return Err(AppError::Invalid("choose a full file path".into()));
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => AppError::Invalid(format!(
                "{} already exists; choose another name",
                path.display()
            )),
            _ => AppError::Invalid(format!("{}: {e}", path.display())),
        })?;
    f.write_all(text.as_bytes())
        .and_then(|()| f.sync_all())
        .map_err(|e| AppError::Invalid(format!("{}: {e}", path.display())))
}
