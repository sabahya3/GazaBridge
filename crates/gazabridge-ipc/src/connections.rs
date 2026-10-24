//! Connection editor types.
//!
//! Enumerated settings (SSL modes, read preferences, journal modes) travel
//! as their serialized names, e.g. `"verify-full"`, so the UI does not need
//! the domain crate. The backend validates them.

use std::fmt;

use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresForm {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    /// `disable`, `prefer`, `require`, `verify-ca`, `verify-full`.
    pub ssl_mode: String,
    pub search_path: Option<String>,
    pub application_name: String,
    pub connect_timeout_secs: u32,
    pub statement_timeout_ms: u64,
    pub pool_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlForm {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub username: String,
    /// `disabled`, `preferred`, `required`, `verify-ca`, `verify-identity`.
    pub tls_mode: String,
    pub charset: String,
    pub connect_timeout_secs: u32,
    pub query_timeout_ms: u64,
    pub pool_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoForm {
    /// Connection URI without credentials; when set, `hosts` is ignored.
    pub uri: Option<String>,
    /// `host:port` entries.
    pub hosts: Vec<String>,
    pub replica_set: Option<String>,
    pub auth_source: Option<String>,
    pub username: Option<String>,
    pub default_database: Option<String>,
    pub tls: bool,
    /// `primary`, `primary_preferred`, `secondary`, `secondary_preferred`, `nearest`.
    pub read_preference: String,
    pub direct_connection: bool,
    pub server_selection_timeout_ms: u64,
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteForm {
    pub path: String,
    pub read_only: bool,
    pub create_if_missing: bool,
    pub busy_timeout_ms: u64,
    /// `delete`, `truncate`, `persist`, `memory`, `wal`, `off`.
    pub journal_mode: String,
    pub foreign_keys: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum EngineForm {
    Postgres(PostgresForm),
    MySql(MySqlForm),
    MongoDb(MongoForm),
    Sqlite(SqliteForm),
}

impl EngineForm {
    pub fn family(&self) -> &'static str {
        match self {
            Self::Postgres(_) => "postgres",
            Self::MySql(_) => "mysql",
            Self::MongoDb(_) => "mongodb",
            Self::Sqlite(_) => "sqlite",
        }
    }

    /// Defaults for a new connection of the given family.
    pub fn default_for(family: &str) -> Option<Self> {
        Some(match family {
            "postgres" => Self::Postgres(PostgresForm {
                host: "localhost".into(),
                port: 5432,
                database: "postgres".into(),
                username: String::new(),
                ssl_mode: "verify-full".into(),
                search_path: None,
                application_name: "GazaBridge".into(),
                connect_timeout_secs: 10,
                statement_timeout_ms: 0,
                pool_size: 4,
            }),
            "mysql" => Self::MySql(MySqlForm {
                host: "localhost".into(),
                port: 3306,
                database: None,
                username: String::new(),
                tls_mode: "verify-identity".into(),
                charset: "utf8mb4".into(),
                connect_timeout_secs: 10,
                query_timeout_ms: 0,
                pool_size: 4,
            }),
            "mongodb" => Self::MongoDb(MongoForm {
                uri: None,
                hosts: vec!["localhost:27017".into()],
                replica_set: None,
                auth_source: None,
                username: None,
                default_database: None,
                tls: true,
                read_preference: "primary".into(),
                direct_connection: false,
                server_selection_timeout_ms: 30_000,
                connect_timeout_ms: 10_000,
            }),
            "sqlite" => Self::Sqlite(SqliteForm {
                path: String::new(),
                read_only: false,
                create_if_missing: false,
                busy_timeout_ms: 5_000,
                journal_mode: "wal".into(),
                foreign_keys: true,
            }),
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsForm {
    pub ca_certificate: Option<String>,
    pub client_certificate: Option<String>,
    pub client_key: Option<String>,
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionForm {
    /// `None` for a new connection.
    pub id: Option<String>,
    pub name: String,
    pub profile_id: String,
    pub engine: EngineForm,
    #[serde(default)]
    pub tls: TlsForm,
    pub read_only: bool,
    pub favorite: bool,
    pub color: Option<String>,
    pub tags: Vec<String>,
    pub folder_id: Option<String>,
}

/// How the password is stored, as shown in the editor (never the value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PasswordMode {
    None,
    Keychain,
    Session,
    Env { var: String },
    Prompt,
}

/// The password choice sent when saving or testing. The value is wiped from
/// memory when this is dropped and never printed by `Debug`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "choice", rename_all = "snake_case")]
pub enum PasswordChoice {
    Keep,
    None,
    Remember { password: String },
    SessionOnly { password: String },
    FromEnv { var: String },
    AskEachTime,
}

impl fmt::Debug for PasswordChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Keep => "Keep",
            Self::None => "None",
            Self::Remember { .. } => "Remember(<redacted>)",
            Self::SessionOnly { .. } => "SessionOnly(<redacted>)",
            Self::FromEnv { .. } => "FromEnv",
            Self::AskEachTime => "AskEachTime",
        })
    }
}

impl Drop for PasswordChoice {
    fn drop(&mut self) {
        if let Self::Remember { password } | Self::SessionOnly { password } = self {
            password.zeroize();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionDetails {
    pub form: ConnectionForm,
    pub password_mode: PasswordMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SaveConnectionArgs {
    pub form: ConnectionForm,
    pub password: PasswordChoice,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FromStringArgs {
    pub name: String,
    /// Pasted connection string; its password is moved to the secret store.
    pub text: String,
    pub remember: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindingDto {
    /// `info`, `warning` or `error`.
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestResultDto {
    /// Machine-readable state, e.g. `ready`, `connection_failed`.
    pub state: String,
    /// Display label, e.g. "Connection Failed".
    pub state_label: String,
    pub latency_ms: Option<u64>,
    pub server_product: Option<String>,
    pub server_version: Option<String>,
    pub detected_profile: Option<String>,
    /// Compatibility status label for the selected profile.
    pub status: Option<String>,
    pub findings: Vec<FindingDto>,
    pub error_code: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_choice_never_prints_its_value() {
        let p = PasswordChoice::Remember {
            password: "hunter2".into(),
        };
        assert!(!format!("{p:?}").contains("hunter2"));
    }

    #[test]
    fn forms_round_trip_through_json() {
        for family in ["postgres", "mysql", "mongodb", "sqlite"] {
            let engine = EngineForm::default_for(family).unwrap();
            assert_eq!(engine.family(), family);
            let json = serde_json::to_string(&engine).unwrap();
            let back: EngineForm = serde_json::from_str(&json).unwrap();
            assert_eq!(back, engine);
        }
        assert!(EngineForm::default_for("oracle").is_none());
    }
}
