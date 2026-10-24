//! SQLite connector.
//!
//! Uses sqlx's bundled SQLite. Read-only connections open the file with
//! `SQLITE_OPEN_READONLY` and `PRAGMA query_only = ON`, so the database
//! itself refuses writes as well as the connector.

mod connection;
mod error;
mod meta;
mod read;
mod sql;
mod types;
mod write;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::{ConnectionProfile, ConnectionSettings, EngineFamily};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits, QueryLanguage};
use gazabridge_plugin_sdk::{
    API_VERSION, Capability, Connection, ConnectorDescriptor, ConnectorError, ConnectorFactory,
    ConnectorResult, ResolvedSecrets,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

pub use connection::{SqliteConnection, main_schema};
pub use types::{canonical_for_declared, native_type_for};

pub const CONNECTOR_ID: &str = "sqlite";

/// Builds the SQLite capability manifest.
pub fn base_manifest() -> CapabilityManifest {
    use Capability::*;
    CapabilityManifest {
        connector_id: CONNECTOR_ID.into(),
        family: EngineFamily::Sqlite,
        api_version: API_VERSION,
        capabilities: BTreeSet::from([
            TestConnection,
            DetectServer,
            ListSchemas,
            ListTables,
            ListViews,
            InspectSchema,
            InspectIndexes,
            InspectConstraints,
            InspectTriggers,
            StreamRead,
            PaginatedRead,
            FilteredRead,
            SortedRead,
            ExactCount,
            WriteInsert,
            WriteUpdate,
            WriteDelete,
            WriteUpsert,
            Transactions,
            CreateTable,
            CreateIndex,
            DropObject,
            TruncateObject,
            ExecuteQuery,
            ExplainQuery,
            Checkpoints,
            ReadOnlySessions,
            HealthMonitoring,
        ]),
        write_modes: write::DIRECT_MODES.into_iter().collect(),
        // SQLite has no native change capture; polling strategies only.
        sync_methods: BTreeSet::from([
            SyncMethod::Polling,
            SyncMethod::TimestampIncremental,
            SyncMethod::KeyIncremental,
        ]),
        query_languages: BTreeSet::from([QueryLanguage::SqliteSql]),
        limits: ConnectorLimits {
            max_identifier_bytes: None,
            max_bind_parameters: Some(write::MAX_BIND_PARAMETERS as u32),
            // SQLITE_MAX_LENGTH default: 1,000,000,000 bytes per value.
            max_record_bytes: Some(1_000_000_000),
            max_nesting_depth: None,
        },
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteConnectorFactory;

#[async_trait]
impl ConnectorFactory for SqliteConnectorFactory {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor {
            id: CONNECTOR_ID.into(),
            family: EngineFamily::Sqlite,
            display_name: "SQLite".into(),
            api_version: API_VERSION,
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn base_manifest(&self) -> CapabilityManifest {
        base_manifest()
    }

    async fn connect(
        &self,
        profile: &ConnectionProfile,
        _secrets: ResolvedSecrets,
    ) -> ConnectorResult<Arc<dyn Connection>> {
        let ConnectionSettings::Sqlite(s) = &profile.settings else {
            return Err(ConnectorError::invalid_input(
                "not a SQLite connection profile",
            ));
        };
        let read_only = profile.read_only || s.read_only;
        let mut options = SqliteConnectOptions::from_str("sqlite://")
            .map_err(error::map_sqlx)?
            .filename(&s.path)
            .busy_timeout(Duration::from_millis(s.busy_timeout_ms))
            .foreign_keys(s.foreign_keys)
            .read_only(read_only)
            .create_if_missing(s.create_if_missing && !read_only);
        if read_only {
            options = options.pragma("query_only", "ON");
        } else {
            let mode = match s.journal_mode {
                gazabridge_domain::connection::SqliteJournalMode::Delete => {
                    SqliteJournalMode::Delete
                }
                gazabridge_domain::connection::SqliteJournalMode::Truncate => {
                    SqliteJournalMode::Truncate
                }
                gazabridge_domain::connection::SqliteJournalMode::Persist => {
                    SqliteJournalMode::Persist
                }
                gazabridge_domain::connection::SqliteJournalMode::Memory => {
                    SqliteJournalMode::Memory
                }
                gazabridge_domain::connection::SqliteJournalMode::Wal => SqliteJournalMode::Wal,
                gazabridge_domain::connection::SqliteJournalMode::Off => SqliteJournalMode::Off,
            };
            options = options.journal_mode(mode);
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_millis(s.busy_timeout_ms.max(1_000)))
            .connect_with(options)
            .await
            .map_err(error::map_sqlx)?;
        let manifest = if read_only {
            base_manifest().read_only()
        } else {
            base_manifest()
        };
        Ok(Arc::new(SqliteConnection::new(pool, manifest, read_only)))
    }
}
