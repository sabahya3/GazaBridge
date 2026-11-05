//! PostgreSQL-family connector (PostgreSQL, Neon, Supabase, Citus, EDB,
//! CockroachDB's PostgreSQL interface and generic PostgreSQL-compatible
//! servers, subject to their compatibility profiles).
//!
//! Every pooled session is configured on creation: `TimeZone=UTC`,
//! `DateStyle=ISO`, `IntervalStyle=iso_8601`, `extra_float_digits=3`, the
//! configured statement timeout and search path, and
//! `default_transaction_read_only=on` for read-only connections.

mod connection;
mod error;
mod meta;
mod read;
mod sql;
mod tls;
mod types;
mod write;

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Hook, HookError, Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, PostgresSettings,
};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits, QueryLanguage};
use gazabridge_plugin_sdk::{
    API_VERSION, Capability, Connection, ConnectorDescriptor, ConnectorError, ConnectorFactory,
    ConnectorResult, ErrorKind, ResolvedSecrets,
};

pub use connection::{PostgresConnection, canonical_for_type, public_schema};
pub use types::{canonical_for, native_type_for};

pub const CONNECTOR_ID: &str = "postgres";

pub fn base_manifest() -> CapabilityManifest {
    use Capability::*;
    CapabilityManifest {
        connector_id: CONNECTOR_ID.into(),
        family: EngineFamily::Postgres,
        api_version: API_VERSION,
        capabilities: BTreeSet::from([
            TestConnection,
            DetectServer,
            ListDatabases,
            ListSchemas,
            ListTables,
            ListViews,
            InspectSchema,
            InspectIndexes,
            InspectConstraints,
            InspectRoutines,
            InspectSequences,
            InspectEnums,
            InspectTriggers,
            InspectPartitions,
            StreamRead,
            PaginatedRead,
            FilteredRead,
            SortedRead,
            CountEstimate,
            ExactCount,
            WriteInsert,
            WriteUpdate,
            WriteDelete,
            WriteUpsert,
            Transactions,
            CreateSchema,
            CreateTable,
            CreateIndex,
            DropObject,
            TruncateObject,
            ExecuteQuery,
            CancelQuery,
            ExplainQuery,
            ExplainAnalyze,
            Checkpoints,
            ReadOnlySessions,
            HealthMonitoring,
        ]),
        write_modes: write::DIRECT_MODES.into_iter().collect(),
        // Logical replication arrives with Milestone 8.
        sync_methods: BTreeSet::from([
            SyncMethod::Polling,
            SyncMethod::TimestampIncremental,
            SyncMethod::KeyIncremental,
        ]),
        query_languages: BTreeSet::from([QueryLanguage::PostgresSql]),
        limits: ConnectorLimits {
            max_identifier_bytes: Some(sql::MAX_IDENTIFIER_BYTES as u32),
            max_bind_parameters: Some(write::MAX_BIND_PARAMETERS as u32),
            // Largest single field value (1 GB).
            max_record_bytes: Some(1 << 30),
            max_nesting_depth: None,
        },
    }
}

/// Resolves `host` and returns the first address that accepts TCP.
///
/// tokio-postgres tries every resolved address and keeps going after *any*
/// failure, so an authentication error on the first address would be
/// replaced by, say, "connection refused" from an unreachable IPv6 address.
/// Pinning one reachable address keeps the real error.
async fn pick_address(host: &str, port: u16, timeout: Duration) -> ConnectorResult<Option<IpAddr>> {
    if host.starts_with('/') || host.parse::<IpAddr>().is_ok() {
        return Ok(None);
    }
    let addrs: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| {
            ConnectorError::from_driver(
                ErrorKind::Connection,
                format!("cannot resolve {host}: {e}"),
                None,
            )
        })?
        .collect();
    let mut last = String::from("no addresses");
    for addr in &addrs {
        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_)) => return Ok(Some(addr.ip())),
            Ok(Err(e)) => last = e.to_string(),
            Err(_) => last = "timed out".into(),
        }
    }
    Err(ConnectorError::from_driver(
        if last == "timed out" {
            ErrorKind::Timeout
        } else {
            ErrorKind::Connection
        },
        format!("could not reach {host}:{port} ({last})"),
        None,
    ))
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// SQL run on every new pooled session.
pub fn session_setup_sql(s: &PostgresSettings, read_only: bool) -> ConnectorResult<String> {
    let mut stmts = vec![
        "SET TimeZone = 'UTC'".to_owned(),
        "SET DateStyle = 'ISO'".to_owned(),
        "SET IntervalStyle = 'iso_8601'".to_owned(),
        "SET extra_float_digits = 3".to_owned(),
        format!("SET statement_timeout = {}", s.statement_timeout_ms),
    ];
    if let Some(path) = s.search_path.as_deref().filter(|p| !p.trim().is_empty()) {
        let schemas = path
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| sql::quote_ident(p.trim_matches('"')))
            .collect::<ConnectorResult<Vec<_>>>()?;
        stmts.push(format!("SET search_path = {}", schemas.join(", ")));
    }
    if read_only {
        stmts.push("SET default_transaction_read_only = on".into());
    }
    stmts.push(format!(
        "SET application_name = {}",
        quote_literal(&s.application_name)
    ));
    Ok(stmts.join("; "))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresConnectorFactory;

#[async_trait]
impl ConnectorFactory for PostgresConnectorFactory {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor {
            id: CONNECTOR_ID.into(),
            family: EngineFamily::Postgres,
            display_name: "PostgreSQL".into(),
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
        secrets: ResolvedSecrets,
    ) -> ConnectorResult<Arc<dyn Connection>> {
        let ConnectionSettings::Postgres(s) = &profile.settings else {
            return Err(ConnectorError::invalid_input(
                "not a PostgreSQL connection profile",
            ));
        };
        let read_only = profile.read_only;
        let (tls, ssl_mode) = tls::connector(s.ssl_mode, &profile.tls)?;

        let timeout = Duration::from_secs(u64::from(s.connect_timeout_secs.max(1)));
        // Through a tunnel the driver dials the local end; TLS still checks
        // the database's own name.
        let tunnel = match &profile.ssh_tunnel {
            Some(_) if s.host.starts_with('/') => {
                return Err(ConnectorError::invalid_input(
                    "a Unix socket cannot be reached through an SSH tunnel",
                ));
            }
            Some(t) => Some(gazabridge_ssh::open(t, &secrets, &s.host, s.port, timeout).await?),
            None => None,
        };
        let port = tunnel.as_ref().map_or(s.port, |t| t.local_addr().port());
        // `Config::host` appends, so the TLS name and address are set once.
        let (tls_name, address) = match (&tunnel, &profile.tls.server_name) {
            (Some(t), name) => (
                name.clone().unwrap_or_else(|| s.host.clone()),
                Some(t.local_addr().ip()),
            ),
            (None, Some(name)) => (
                name.clone(),
                Some(s.host.parse::<IpAddr>().map_err(|_| {
                    ConnectorError::invalid_input(
                        "a TLS server name override needs an IP address host",
                    )
                })?),
            ),
            (None, None) => (
                s.host.clone(),
                pick_address(&s.host, s.port, timeout).await?,
            ),
        };
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&tls_name)
            .port(port)
            .dbname(&s.database)
            .user(&s.username)
            .connect_timeout(timeout)
            .ssl_mode(ssl_mode);
        if let Some(addr) = address {
            cfg.hostaddr(addr);
        }
        if let Some(pw) = &secrets.password {
            cfg.password(pw.as_bytes());
        }

        let setup = session_setup_sql(s, read_only)?;
        let manager = Manager::from_config(
            cfg,
            tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(s.pool_size.max(1) as usize)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(timeout))
            .create_timeout(Some(timeout))
            .post_create(Hook::async_fn(move |client, _| {
                let setup = setup.clone();
                Box::pin(async move {
                    client
                        .batch_execute(&setup)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .build()
            .map_err(|e| ConnectorError::internal(e.to_string()))?;
        // Surface connection and authentication errors now.
        drop(pool.get().await.map_err(error::map_pool)?);

        let manifest = if read_only {
            base_manifest().read_only()
        } else {
            base_manifest()
        };
        Ok(Arc::new(PostgresConnection::new(
            pool, tls, manifest, read_only, tunnel,
        )))
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_domain::connection::{PgSslMode, SecretRef};

    use super::*;

    fn settings() -> PostgresSettings {
        PostgresSettings {
            host: "localhost".into(),
            port: 5432,
            database: "app".into(),
            username: "app".into(),
            password: SecretRef::None,
            ssl_mode: PgSslMode::Disable,
            search_path: Some(r#"tenant one, public, "$user""#.into()),
            application_name: "Gaza'Bridge".into(),
            connect_timeout_secs: 5,
            statement_timeout_ms: 30_000,
            pool_size: 4,
        }
    }

    #[test]
    fn session_setup_quotes_everything() {
        let sql = session_setup_sql(&settings(), true).unwrap();
        assert!(
            sql.contains(r#"SET search_path = "tenant one", "public", "$user""#),
            "{sql}"
        );
        assert!(sql.contains("SET default_transaction_read_only = on"));
        assert!(sql.contains("SET statement_timeout = 30000"));
        assert!(sql.contains("SET application_name = 'Gaza''Bridge'"));
    }

    #[test]
    fn read_only_manifest_has_no_mutations() {
        let m = base_manifest().read_only();
        assert!(m.capabilities.iter().all(|c| !c.is_mutating()));
        assert!(m.supports(Capability::ExecuteQuery));
    }

    #[tokio::test]
    async fn unreachable_server_fails_with_a_connection_error() {
        let mut s = settings();
        s.port = 1; // nothing listens here
        s.connect_timeout_secs = 2;
        let profile =
            ConnectionProfile::new("x", "postgres.postgresql", ConnectionSettings::Postgres(s));
        let err = PostgresConnectorFactory
            .connect(&profile, ResolvedSecrets::default())
            .await
            .unwrap_err();
        assert!(
            matches!(err.kind(), ErrorKind::Connection | ErrorKind::Timeout),
            "{err}"
        );
    }
}
