//! Logging for GazaBridge.
//!
//! * JSON lines written to size-rotated files with a retention period.
//! * Every line passes through the secret [`Redactor`] before reaching disk.
//! * Spans carry `job_id`, `run_id` and `correlation_id` fields so the log
//!   viewer can filter by them.
//!
//! Records from user databases must never be logged; log messages describe
//! operations and counts only.

pub mod reader;
mod writer;

use std::path::PathBuf;

use gazabridge_secrets::Redactor;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use writer::{RedactingWriter, SizeRotatingFile};

pub const LOG_FILE_NAME: &str = "gazabridge.log";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogConfig {
    pub directory: PathBuf,
    pub level: LogLevel,
    /// Rotate when the active file would exceed this size.
    pub max_file_bytes: u64,
    /// Rotated files to keep in addition to the active one.
    pub max_files: usize,
    /// Rotated files older than this are deleted at start-up.
    pub retention_days: u32,
    /// Also log human-readable lines to stderr.
    pub console: bool,
}

impl LogConfig {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            level: LogLevel::Info,
            max_file_bytes: 10 * 1024 * 1024,
            max_files: 5,
            retention_days: 14,
            console: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoggingError {
    #[error("could not prepare the log directory: {0}")]
    Io(#[from] std::io::Error),
    #[error("a global logger is already installed")]
    AlreadyInitialized,
}

/// Keeps the background log writer alive; dropping it flushes pending lines.
#[derive(Debug)]
pub struct LoggingGuard {
    _worker: tracing_appender::non_blocking::WorkerGuard,
}

/// Installs the global subscriber. Call once at start-up.
pub fn init(config: &LogConfig, redactor: Redactor) -> Result<LoggingGuard, LoggingError> {
    std::fs::create_dir_all(&config.directory)?;
    writer::apply_retention(&config.directory, config.retention_days)?;
    let file = SizeRotatingFile::open(
        config.directory.join(LOG_FILE_NAME),
        config.max_file_bytes,
        config.max_files,
    )?;
    let (non_blocking, worker) =
        tracing_appender::non_blocking(RedactingWriter::new(file, redactor.clone()));

    // RUST_LOG may raise verbosity for diagnostics; the configured level is the default.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{},sqlx=warn", config.level.as_str())));

    let file_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_target(true)
        .with_writer(non_blocking);

    let console_layer = config.console.then(|| {
        let redactor = redactor.clone();
        tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_writer(move || RedactingWriter::new(std::io::stderr(), redactor.clone()))
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(console_layer)
        .try_init()
        .map_err(|_| LoggingError::AlreadyInitialized)?;

    Ok(LoggingGuard { _worker: worker })
}
