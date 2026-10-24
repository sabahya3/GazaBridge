//! Connection profiles.
//!
//! A [`ConnectionProfile`] never contains a secret. Passwords, key
//! passphrases and tokens are represented by [`SecretRef`]s that point at the
//! OS keychain, the in-memory session store or an environment variable.

use std::fmt;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{ConnectionId, FolderId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineFamily {
    Postgres,
    MySql,
    MongoDb,
    Sqlite,
}

impl EngineFamily {
    pub const ALL: [EngineFamily; 4] = [Self::Postgres, Self::MySql, Self::MongoDb, Self::Sqlite];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::MongoDb => "mongodb",
            Self::Sqlite => "sqlite",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Postgres => "PostgreSQL Family",
            Self::MySql => "MySQL Family",
            Self::MongoDb => "MongoDB Family",
            Self::Sqlite => "SQLite",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.as_str() == s)
    }

    pub fn default_port(&self) -> Option<u16> {
        match self {
            Self::Postgres => Some(5432),
            Self::MySql => Some(3306),
            Self::MongoDb => Some(27017),
            Self::Sqlite => None,
        }
    }
}

impl fmt::Display for EngineFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a secret lives. The secret value itself is never stored here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "store", rename_all = "snake_case")]
pub enum SecretRef {
    /// No secret is needed.
    #[default]
    None,
    /// OS keychain entry (Keychain, Credential Manager, Secret Service).
    Keychain { service: String, account: String },
    /// Held in memory for the current session only.
    Session { key: String },
    /// Read from an environment variable at connect time.
    Env { var: String },
    /// Ask the user each time; never persisted.
    Prompt,
}

impl SecretRef {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Keychain reference for a connection field, e.g. the password of a
    /// connection. Service names are namespaced to GazaBridge.
    pub fn keychain_for(connection: ConnectionId, field: &str) -> Self {
        Self::Keychain {
            service: "dev.gazabridge.connection".to_owned(),
            account: format!("{connection}:{field}"),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Keychain { .. } => "keychain",
            Self::Session { .. } => "session",
            Self::Env { .. } => "env",
            Self::Prompt => "prompt",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PgSslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    /// Default: encrypt and verify the server certificate and host name.
    #[default]
    VerifyFull,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MySqlTlsMode {
    Disabled,
    Preferred,
    Required,
    VerifyCa,
    #[default]
    VerifyIdentity,
}

/// TLS material shared by all network engines.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_certificate: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "SecretRef::is_none")]
    pub client_key_passphrase: SecretRef,
    /// Overrides the host name used for certificate verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum SshAuth {
    Password {
        password: SecretRef,
    },
    PrivateKey {
        key_path: PathBuf,
        passphrase: SecretRef,
    },
    Agent,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostKeyPolicy {
    /// Only connect to hosts already present in `known_hosts`.
    #[default]
    Strict,
    /// Accept and record a host key on first use, reject changes afterwards.
    AcceptNew,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshTunnel {
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    pub username: String,
    pub auth: SshAuth,
    #[serde(default)]
    pub host_key_policy: HostKeyPolicy,
}

fn default_ssh_port() -> u16 {
    22
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    #[serde(default)]
    pub password: SecretRef,
    #[serde(default)]
    pub ssl_mode: PgSslMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_path: Option<String>,
    #[serde(default = "default_app_name")]
    pub application_name: String,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u32,
    /// `0` disables the statement timeout.
    #[serde(default)]
    pub statement_timeout_ms: u64,
    #[serde(default = "default_pool")]
    pub pool_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlSettings {
    pub host: String,
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    pub username: String,
    #[serde(default)]
    pub password: SecretRef,
    #[serde(default)]
    pub tls_mode: MySqlTlsMode,
    #[serde(default = "default_charset")]
    pub charset: String,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u32,
    #[serde(default)]
    pub query_timeout_ms: u64,
    #[serde(default = "default_pool")]
    pub pool_size: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadPreference {
    #[default]
    Primary,
    PrimaryPreferred,
    Secondary,
    SecondaryPreferred,
    Nearest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoSettings {
    /// Connection URI with any credentials removed. When present it takes
    /// precedence over `hosts`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default)]
    pub hosts: Vec<HostPort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_set: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default)]
    pub password: SecretRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_database: Option<String>,
    #[serde(default = "default_true")]
    pub tls: bool,
    #[serde(default)]
    pub read_preference: ReadPreference,
    #[serde(default)]
    pub direct_connection: bool,
    #[serde(default = "default_server_selection")]
    pub server_selection_timeout_ms: u64,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteJournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    #[default]
    Wal,
    Off,
}

impl SqliteJournalMode {
    pub fn as_pragma(&self) -> &'static str {
        match self {
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Persist => "PERSIST",
            Self::Memory => "MEMORY",
            Self::Wal => "WAL",
            Self::Off => "OFF",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteSettings {
    pub path: PathBuf,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default = "default_busy_timeout")]
    pub busy_timeout_ms: u64,
    #[serde(default)]
    pub journal_mode: SqliteJournalMode,
    #[serde(default = "default_true")]
    pub foreign_keys: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum ConnectionSettings {
    Postgres(PostgresSettings),
    MySql(MySqlSettings),
    MongoDb(MongoSettings),
    Sqlite(SqliteSettings),
}

impl ConnectionSettings {
    pub fn family(&self) -> EngineFamily {
        match self {
            Self::Postgres(_) => EngineFamily::Postgres,
            Self::MySql(_) => EngineFamily::MySql,
            Self::MongoDb(_) => EngineFamily::MongoDb,
            Self::Sqlite(_) => EngineFamily::Sqlite,
        }
    }

    /// Every secret reference held by these settings.
    pub fn secret_refs(&self) -> Vec<&SecretRef> {
        let r = match self {
            Self::Postgres(s) => &s.password,
            Self::MySql(s) => &s.password,
            Self::MongoDb(s) => &s.password,
            Self::Sqlite(_) => return Vec::new(),
        };
        if r.is_none() { Vec::new() } else { vec![r] }
    }

    /// Mutable access to the primary password reference, if the engine has one.
    pub fn password_mut(&mut self) -> Option<&mut SecretRef> {
        match self {
            Self::Postgres(s) => Some(&mut s.password),
            Self::MySql(s) => Some(&mut s.password),
            Self::MongoDb(s) => Some(&mut s.password),
            Self::Sqlite(_) => None,
        }
    }

    /// A short, secret-free description such as `postgres://db.example:5432/app`.
    pub fn display_target(&self) -> String {
        match self {
            Self::Postgres(s) => format!("postgres://{}:{}/{}", s.host, s.port, s.database),
            Self::MySql(s) => format!(
                "mysql://{}:{}/{}",
                s.host,
                s.port,
                s.database.as_deref().unwrap_or("")
            ),
            Self::MongoDb(s) => match &s.uri {
                Some(uri) => crate::connection_string::redact(uri),
                None => {
                    let hosts: Vec<String> = s
                        .hosts
                        .iter()
                        .map(|h| format!("{}:{}", h.host, h.port))
                        .collect();
                    format!("mongodb://{}", hosts.join(","))
                }
            },
            Self::Sqlite(s) => format!("sqlite://{}", s.path.display()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub id: ConnectionId,
    pub name: String,
    /// Compatibility profile id, e.g. `postgres.neon` or `mongodb.documentdb`.
    pub compatibility_profile: String,
    pub settings: ConnectionSettings,
    #[serde(default)]
    pub tls: TlsSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_tunnel: Option<SshTunnel>,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder_id: Option<FolderId>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default)]
    pub favorite: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProfileValidationError {
    #[error("connection name must not be empty")]
    EmptyName,
    #[error("{0} must not be empty")]
    MissingField(&'static str),
    #[error("port must be between 1 and 65535")]
    InvalidPort,
    #[error("pool size must be between 1 and 1000")]
    InvalidPoolSize,
    #[error("the connection URI contains an embedded password; store it as a secret instead")]
    EmbeddedPassword,
    #[error("the connection URI is not valid: {0}")]
    InvalidUri(String),
    #[error("color must be a #RRGGBB hex value")]
    InvalidColor,
    #[error("compatibility profile `{profile}` is not for the {family} engine family")]
    ProfileFamilyMismatch {
        profile: String,
        family: EngineFamily,
    },
}

impl ConnectionProfile {
    pub fn new(
        name: impl Into<String>,
        compatibility_profile: impl Into<String>,
        settings: ConnectionSettings,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: ConnectionId::new(),
            name: name.into(),
            compatibility_profile: compatibility_profile.into(),
            settings,
            tls: TlsSettings::default(),
            ssh_tunnel: None,
            read_only: false,
            folder_id: None,
            tags: Vec::new(),
            color: None,
            favorite: false,
            created_at: now,
            updated_at: now,
            last_used_at: None,
        }
    }

    pub fn family(&self) -> EngineFamily {
        self.settings.family()
    }

    /// Validates structural invariants, including that no credential is
    /// embedded in a stored URI.
    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        use ProfileValidationError as E;
        if self.name.trim().is_empty() {
            return Err(E::EmptyName);
        }
        let family = self.family();
        if !self
            .compatibility_profile
            .split('.')
            .next()
            .is_some_and(|p| p == family.as_str())
        {
            return Err(E::ProfileFamilyMismatch {
                profile: self.compatibility_profile.clone(),
                family,
            });
        }
        if let Some(color) = &self.color {
            let ok = color.len() == 7
                && color.starts_with('#')
                && color[1..].chars().all(|c| c.is_ascii_hexdigit());
            if !ok {
                return Err(E::InvalidColor);
            }
        }
        let check_pool = |n: u32| {
            if (1..=1000).contains(&n) {
                Ok(())
            } else {
                Err(E::InvalidPoolSize)
            }
        };
        match &self.settings {
            ConnectionSettings::Postgres(s) => {
                require(&s.host, "host")?;
                require(&s.database, "database")?;
                require(&s.username, "username")?;
                if s.port == 0 {
                    return Err(E::InvalidPort);
                }
                check_pool(s.pool_size)?;
            }
            ConnectionSettings::MySql(s) => {
                require(&s.host, "host")?;
                require(&s.username, "username")?;
                if s.port == 0 {
                    return Err(E::InvalidPort);
                }
                check_pool(s.pool_size)?;
            }
            ConnectionSettings::MongoDb(s) => match &s.uri {
                Some(uri) => {
                    let lower = uri.to_ascii_lowercase();
                    if !(lower.starts_with("mongodb://") || lower.starts_with("mongodb+srv://")) {
                        return Err(E::InvalidUri(
                            "expected a mongodb:// or mongodb+srv:// URI".into(),
                        ));
                    }
                    if crate::connection_string::mongo_uri_has_password(uri) {
                        return Err(E::EmbeddedPassword);
                    }
                }
                None => {
                    if s.hosts.is_empty() {
                        return Err(E::MissingField("hosts"));
                    }
                    if s.hosts.iter().any(|h| h.port == 0) {
                        return Err(E::InvalidPort);
                    }
                }
            },
            ConnectionSettings::Sqlite(s) => {
                if s.path.as_os_str().is_empty() {
                    return Err(E::MissingField("path"));
                }
            }
        }
        Ok(())
    }
}

fn require(value: &str, field: &'static str) -> Result<(), ProfileValidationError> {
    if value.trim().is_empty() {
        Err(ProfileValidationError::MissingField(field))
    } else {
        Ok(())
    }
}

fn default_app_name() -> String {
    "GazaBridge".to_owned()
}
fn default_connect_timeout() -> u32 {
    10
}
fn default_connect_timeout_ms() -> u64 {
    10_000
}
fn default_server_selection() -> u64 {
    30_000
}
fn default_pool() -> u32 {
    4
}
fn default_charset() -> String {
    "utf8mb4".to_owned()
}
fn default_true() -> bool {
    true
}
fn default_busy_timeout() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg() -> ConnectionProfile {
        ConnectionProfile::new(
            "local",
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: "localhost".into(),
                port: 5432,
                database: "app".into(),
                username: "app".into(),
                password: SecretRef::Session { key: "x".into() },
                ssl_mode: PgSslMode::default(),
                search_path: None,
                application_name: default_app_name(),
                connect_timeout_secs: 10,
                statement_timeout_ms: 0,
                pool_size: 4,
            }),
        )
    }

    #[test]
    fn valid_profile_passes() {
        pg().validate().unwrap();
    }

    #[test]
    fn secure_tls_defaults() {
        assert_eq!(PgSslMode::default(), PgSslMode::VerifyFull);
        assert_eq!(MySqlTlsMode::default(), MySqlTlsMode::VerifyIdentity);
        assert_eq!(HostKeyPolicy::default(), HostKeyPolicy::Strict);
    }

    #[test]
    fn mongo_uri_with_password_is_rejected() {
        let p = ConnectionProfile::new(
            "m",
            "mongodb.community",
            ConnectionSettings::MongoDb(MongoSettings {
                uri: Some("mongodb://user:hunter2@localhost:27017/".into()),
                hosts: vec![],
                replica_set: None,
                auth_source: None,
                username: None,
                password: SecretRef::None,
                default_database: None,
                tls: true,
                read_preference: ReadPreference::default(),
                direct_connection: false,
                server_selection_timeout_ms: 1000,
                connect_timeout_ms: 1000,
            }),
        );
        assert_eq!(p.validate(), Err(ProfileValidationError::EmbeddedPassword));
    }

    #[test]
    fn profile_family_must_match() {
        let mut p = pg();
        p.compatibility_profile = "mysql.mariadb".into();
        assert!(matches!(
            p.validate(),
            Err(ProfileValidationError::ProfileFamilyMismatch { .. })
        ));
    }

    #[test]
    fn serialized_profile_contains_no_secret_values() {
        let json = serde_json::to_string(&pg()).unwrap();
        assert!(json.contains(r#""store":"session""#));
        assert!(!json.to_lowercase().contains("hunter2"));
    }
}
