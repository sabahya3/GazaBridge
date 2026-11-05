//! Data exchanged between the desktop backend (Tauri commands) and the UI.
//!
//! Everything here is plain data with no secrets: the backend redacts
//! connection targets before they cross the IPC boundary.

use serde::{Deserialize, Serialize};

pub mod backup;
pub mod connections;
pub mod explorer;
pub mod migration;
pub mod query;
pub mod transfer;

pub use backup::*;
pub use connections::*;
pub use explorer::*;
pub use migration::*;
pub use query::*;
pub use transfer::*;

/// Names of the Tauri commands, shared so both sides agree.
pub mod commands {
    pub const APP_INFO: &str = "app_info";
    pub const GET_PREFERENCES: &str = "get_preferences";
    pub const SET_PREFERENCES: &str = "set_preferences";
    pub const LIST_CONNECTIONS: &str = "list_connections";
    pub const LIST_PROFILES: &str = "list_profiles";
    pub const QUERY_LOGS: &str = "query_logs";
    pub const GET_CONNECTION: &str = "get_connection";
    pub const SAVE_CONNECTION: &str = "save_connection";
    pub const CREATE_CONNECTION_FROM_STRING: &str = "create_connection_from_string";
    pub const DELETE_CONNECTION: &str = "delete_connection";
    pub const DUPLICATE_CONNECTION: &str = "duplicate_connection";
    pub const TEST_CONNECTION: &str = "test_connection";
    pub const TEST_CONNECTION_DRAFT: &str = "test_connection_draft";
    pub const PARSE_CONNECTION_STRING: &str = "parse_connection_string";
    pub const EXPLORER_LIST: &str = "explorer_list";
    pub const EXPLORER_BROWSE: &str = "explorer_browse";
    pub const EXPLORER_COUNT: &str = "explorer_count";
    pub const EXPLORER_TREE_COUNTS: &str = "explorer_tree_counts";
    pub const EXPLORER_INSERT: &str = "explorer_insert";
    pub const EXPLORER_PLAN_UPDATE: &str = "explorer_plan_update";
    pub const EXPLORER_APPLY_UPDATE: &str = "explorer_apply_update";
    pub const EXPLORER_DELETE: &str = "explorer_delete";
    pub const EXPLORER_DISCONNECT: &str = "explorer_disconnect";
    pub const QUERY_CONTEXT: &str = "query_context";
    pub const QUERY_RUN: &str = "query_run";
    pub const QUERY_EXPLAIN: &str = "query_explain";
    pub const QUERY_CANCEL: &str = "query_cancel";
    pub const QUERY_HISTORY: &str = "query_history";
    pub const QUERY_LIST_SAVED: &str = "query_list_saved";
    pub const QUERY_SAVE: &str = "query_save";
    pub const QUERY_DELETE_SAVED: &str = "query_delete_saved";
    pub const TRANSFER_EXPORT: &str = "transfer_export";
    pub const TRANSFER_IMPORT: &str = "transfer_import";
    pub const TRANSFER_STATUS: &str = "transfer_status";
    pub const TRANSFER_CANCEL: &str = "transfer_cancel";
    pub const TRANSFER_FORGET: &str = "transfer_forget";
    pub const PICK_FILE_PATH: &str = "pick_file_path";
    pub const MIGRATION_PLAN: &str = "migration_plan";
    pub const MIGRATION_START: &str = "migration_start";
    pub const MIGRATION_STATUS: &str = "migration_status";
    pub const MIGRATION_PAUSE: &str = "migration_pause";
    pub const MIGRATION_RESUME: &str = "migration_resume";
    pub const MIGRATION_CANCEL: &str = "migration_cancel";
    pub const MIGRATION_RECENT: &str = "migration_recent";
    pub const MIGRATION_DRY_RUN: &str = "migration_dry_run";
    pub const MIGRATION_VALIDATE: &str = "migration_validate";
    pub const MIGRATION_REPORT: &str = "migration_report";
    pub const UNLOCK_CONNECTION: &str = "unlock_connection";
    pub const BACKUP_TOOLS: &str = "backup_tools";
    pub const BACKUP_LIST: &str = "backup_list";
    pub const BACKUP_START: &str = "backup_start";
    pub const BACKUP_STATUS: &str = "backup_status";
    pub const BACKUP_CANCEL: &str = "backup_cancel";
    pub const BACKUP_DELETE: &str = "backup_delete";
    pub const RESTORE_PLAN: &str = "restore_plan";
    pub const RESTORE_START: &str = "restore_start";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub connector_api: String,
    pub metadata_schema_version: i64,
    pub data_directory: String,
    pub log_directory: String,
    pub telemetry_enabled: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Theme {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Locale {
    #[default]
    En,
    Ar,
}

impl Locale {
    pub fn code(&self) -> &'static str {
        match self {
            Self::En => "en",
            Self::Ar => "ar",
        }
    }

    pub fn is_rtl(&self) -> bool {
        matches!(self, Self::Ar)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preferences {
    pub theme: Theme,
    pub locale: Locale,
    pub high_contrast: bool,
    /// Default row limit for data views; queries are never unbounded.
    pub default_row_limit: u32,
    pub sidebar_collapsed: bool,
    pub onboarding_done: bool,
    /// Show estimated document counts in the Explorer tree.
    #[serde(default = "yes")]
    pub tree_counts: bool,
}

fn yes() -> bool {
    true
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            theme: Theme::System,
            locale: Locale::En,
            high_contrast: false,
            default_row_limit: 200,
            sidebar_collapsed: false,
            onboarding_done: false,
            tree_counts: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub id: String,
    pub name: String,
    pub family: String,
    pub profile_id: String,
    pub profile_name: String,
    /// Redacted target, e.g. `postgres://db.example:5432/app`.
    pub target: String,
    pub read_only: bool,
    pub favorite: bool,
    pub tags: Vec<String>,
    pub color: Option<String>,
    /// Last known state label, if a health check has run.
    pub state: Option<String>,
    pub latency_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub id: String,
    pub family: String,
    pub family_name: String,
    pub display_name: String,
    pub status: String,
    pub tested_versions: Vec<String>,
    pub known_limitations: Vec<String>,
    pub unsupported_operations: Vec<String>,
    pub auth_methods: Vec<String>,
    pub documentation_url: String,
    /// Whether a connector for this family is available in this build.
    pub connector_available: bool,
    pub cdc_available: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFilter {
    /// `error`, `warn`, `info`, `debug` or `trace`.
    pub max_level: Option<String>,
    pub search: Option<String>,
    pub run_id: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub timestamp: Option<String>,
    pub level: String,
    pub target: String,
    pub message: String,
    pub run_id: Option<String>,
    pub job_id: Option<String>,
}

/// The password of a connection, entered when it is needed.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnlockArgs {
    pub connection_id: String,
    pub password: String,
    /// Save it in the OS keychain again.
    pub remember: bool,
}

impl std::fmt::Debug for UnlockArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnlockArgs")
            .field("connection_id", &self.connection_id)
            .field("password", &"<redacted>")
            .field("remember", &self.remember)
            .finish()
    }
}

/// Errors returned by commands: a stable code plus a redacted message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandError {
    pub code: String,
    pub message: String,
    /// `password_required`: the connection whose password is needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CommandError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_default_is_safe() {
        let p = Preferences::default();
        assert!(p.default_row_limit > 0 && p.default_row_limit <= 10_000);
        let json = serde_json::to_string(&p).unwrap();
        let back: Preferences = serde_json::from_str(&json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn arabic_is_rtl() {
        assert!(Locale::Ar.is_rtl());
        assert!(!Locale::En.is_rtl());
    }
}
