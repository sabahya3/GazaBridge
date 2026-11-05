//! MySQL-family connector (MySQL, MariaDB, Percona Server and compatible
//! servers, subject to their compatibility profiles).
//!
//! Every pooled session is configured on creation and after every reset:
//! `time_zone='+00:00'`, `utf8mb4`, strict SQL mode without
//! `NO_BACKSLASH_ESCAPES`, and `TRANSACTION READ ONLY` for read-only
//! connections. Cancellation and client-side timeouts send `KILL QUERY` on a
//! second connection so the server stops working too.

mod connection;
mod error;
mod meta;
mod read;
mod sql;
mod types;
mod write;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, MySqlSettings, MySqlTlsMode, TlsSettings,
};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits, QueryLanguage};
use gazabridge_plugin_sdk::{
    API_VERSION, Capability, Connection, ConnectorDescriptor, ConnectorError, ConnectorFactory,
    ConnectorResult, ErrorKind, ResolvedSecrets,
};
use mysql_async::{ClientIdentity, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

pub use connection::MySqlConnection;
pub use sql::{is_read_only_statement, quote_ident};
pub use types::{canonical_for, native_type_for};

pub const CONNECTOR_ID: &str = "mysql";

pub fn base_manifest() -> CapabilityManifest {
    use Capability::*;
    CapabilityManifest {
        connector_id: CONNECTOR_ID.into(),
        family: EngineFamily::MySql,
        api_version: API_VERSION,
        capabilities: BTreeSet::from([
            TestConnection,
            DetectServer,
            ListDatabases,
            ListTables,
            ListViews,
            InspectSchema,
            InspectIndexes,
            InspectConstraints,
            InspectRoutines,
            InspectTriggers,
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
        // Binlog CDC arrives with Milestone 8.
        sync_methods: BTreeSet::from([
            SyncMethod::Polling,
            SyncMethod::TimestampIncremental,
            SyncMethod::KeyIncremental,
        ]),
        query_languages: BTreeSet::from([QueryLanguage::MySql]),
        limits: ConnectorLimits {
            max_identifier_bytes: Some((sql::MAX_IDENTIFIER_CHARS * 4) as u32),
            max_bind_parameters: Some(write::MAX_BIND_PARAMETERS as u32),
            // Default max_allowed_packet on 8.0 is 64 MB; LONGBLOB holds 4 GB.
            max_record_bytes: Some(64 << 20),
            max_nesting_depth: Some(100),
        },
    }
}

/// Statements run on every new and every reset pooled session.
pub fn session_setup(read_only: bool) -> Vec<String> {
    let mut v = vec![
        "SET NAMES utf8mb4".to_owned(),
        "SET time_zone = '+00:00'".to_owned(),
        // Strict mode so out-of-range values fail instead of being clipped;
        // NO_BACKSLASH_ESCAPES is removed because literals in generated DDL
        // escape backslashes.
        "SET SESSION sql_mode = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', @@SESSION.sql_mode, ',STRICT_ALL_TABLES,'), ',NO_BACKSLASH_ESCAPES,', ','))".to_owned(),
    ];
    if read_only {
        v.push("SET SESSION TRANSACTION READ ONLY".to_owned());
    }
    v
}

fn ssl_opts(mode: MySqlTlsMode, tls: &TlsSettings) -> ConnectorResult<Option<SslOpts>> {
    if mode == MySqlTlsMode::Disabled {
        return Ok(None);
    }
    let mut o = SslOpts::default();
    if let Some(ca) = &tls.ca_certificate {
        o = o.with_root_certs(vec![ca.clone().into()]);
    }
    match (&tls.client_certificate, &tls.client_key) {
        (Some(cert), Some(key)) => {
            o = o.with_client_identity(Some(ClientIdentity::new(
                cert.clone().into(),
                key.clone().into(),
            )));
        }
        (None, None) => {}
        _ => {
            return Err(ConnectorError::invalid_input(
                "a client certificate needs both the certificate and the key file",
            ));
        }
    }
    if let Some(name) = &tls.server_name {
        o = o.with_danger_tls_hostname_override(Some(name.clone()));
    }
    match mode {
        // Encrypt without checking who answers.
        MySqlTlsMode::Preferred | MySqlTlsMode::Required => {
            o = o
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true);
        }
        MySqlTlsMode::VerifyCa => o = o.with_danger_skip_domain_validation(true),
        MySqlTlsMode::VerifyIdentity | MySqlTlsMode::Disabled => {}
    }
    Ok(Some(o))
}

fn builder(
    s: &MySqlSettings,
    (host, port): &(String, u16),
    secrets: &ResolvedSecrets,
    read_only: bool,
    ssl: Option<SslOpts>,
) -> ConnectorResult<OptsBuilder> {
    let max = s.pool_size.max(1) as usize;
    let constraints = PoolConstraints::new(0, max)
        .ok_or_else(|| ConnectorError::invalid_input("invalid pool size"))?;
    Ok(OptsBuilder::default()
        .ip_or_hostname(host.clone())
        .tcp_port(*port)
        .user(Some(s.username.clone()))
        .pass(secrets.password.as_ref().map(|p| p.as_str().to_owned()))
        .db_name(s.database.clone().filter(|d| !d.is_empty()))
        .prefer_socket(false)
        .ssl_opts(ssl)
        .setup(session_setup(read_only))
        .pool_opts(
            PoolOpts::default()
                .with_constraints(constraints)
                .with_inactive_connection_ttl(Duration::from_secs(300)),
        ))
}

async fn open(opts: OptsBuilder, timeout: Duration) -> ConnectorResult<Pool> {
    let pool = Pool::new(opts);
    let conn = tokio::time::timeout(timeout, pool.get_conn())
        .await
        .map_err(|_| ConnectorError::new(ErrorKind::Timeout, "timed out connecting to the server"));
    match conn {
        Ok(Ok(c)) => {
            drop(c);
            Ok(pool)
        }
        Ok(Err(e)) => {
            let _ = pool.disconnect().await;
            Err(error::map_mysql(e))
        }
        Err(e) => {
            let _ = pool.disconnect().await;
            Err(e)
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlConnectorFactory;

#[async_trait]
impl ConnectorFactory for MySqlConnectorFactory {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor {
            id: CONNECTOR_ID.into(),
            family: EngineFamily::MySql,
            display_name: "MySQL / MariaDB".into(),
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
        let ConnectionSettings::MySql(s) = &profile.settings else {
            return Err(ConnectorError::invalid_input(
                "not a MySQL connection profile",
            ));
        };
        let read_only = profile.read_only;
        let timeout = Duration::from_secs(u64::from(s.connect_timeout_secs.max(1)));
        let tunnel = match &profile.ssh_tunnel {
            Some(t) => Some(gazabridge_ssh::open(t, &secrets, &s.host, s.port, timeout).await?),
            None => None,
        };
        // Through a tunnel the driver dials the local end; TLS still checks
        // the database's own name.
        let endpoint = tunnel.as_ref().map_or_else(
            || (s.host.clone(), s.port),
            |t| (t.local_addr().ip().to_string(), t.local_addr().port()),
        );
        let mut tls = profile.tls.clone();
        if tunnel.is_some() && tls.server_name.is_none() {
            tls.server_name = Some(s.host.clone());
        }
        let ssl = ssl_opts(s.tls_mode, &tls)?;
        let pool = match open(builder(s, &endpoint, &secrets, read_only, ssl)?, timeout).await {
            Ok(p) => p,
            // "Preferred" falls back to plain TCP when the server has no TLS.
            Err(e)
                if s.tls_mode == MySqlTlsMode::Preferred
                    && matches!(e.kind(), ErrorKind::Connection | ErrorKind::Internal)
                    && e.to_string().to_ascii_lowercase().contains("tls") =>
            {
                open(builder(s, &endpoint, &secrets, read_only, None)?, timeout).await?
            }
            Err(e) => return Err(e),
        };
        let manifest = if read_only {
            base_manifest().read_only()
        } else {
            base_manifest()
        };
        let query_timeout =
            (s.query_timeout_ms > 0).then(|| Duration::from_millis(s.query_timeout_ms));
        Ok(Arc::new(
            MySqlConnection::new(pool, manifest, read_only, query_timeout, tunnel).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_domain::connection::SecretRef;

    use super::*;

    fn settings() -> MySqlSettings {
        MySqlSettings {
            host: "127.0.0.1".into(),
            port: 1,
            database: Some("app".into()),
            username: "app".into(),
            password: SecretRef::None,
            tls_mode: MySqlTlsMode::Disabled,
            charset: "utf8mb4".into(),
            connect_timeout_secs: 2,
            query_timeout_ms: 0,
            pool_size: 2,
        }
    }

    #[test]
    fn read_only_manifest_has_no_mutations() {
        let m = base_manifest().read_only();
        assert!(m.capabilities.iter().all(|c| !c.is_mutating()));
        assert!(m.supports(Capability::ExecuteQuery));
    }

    #[test]
    fn read_only_sessions_are_marked() {
        assert!(session_setup(true).iter().any(|s| s.contains("READ ONLY")));
        assert!(!session_setup(false).iter().any(|s| s.contains("READ ONLY")));
        assert!(session_setup(false).iter().any(|s| s.contains("+00:00")));
    }

    #[test]
    fn half_a_client_identity_is_refused() {
        let tls = TlsSettings {
            client_certificate: Some("/x.pem".into()),
            ..Default::default()
        };
        assert!(ssl_opts(MySqlTlsMode::Required, &tls).is_err());
        assert!(
            ssl_opts(MySqlTlsMode::Disabled, &TlsSettings::default())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unreachable_server_fails_with_a_connection_error() {
        let profile =
            ConnectionProfile::new("x", "mysql.mysql", ConnectionSettings::MySql(settings()));
        let err = MySqlConnectorFactory
            .connect(&profile, ResolvedSecrets::default())
            .await
            .unwrap_err();
        assert!(
            matches!(err.kind(), ErrorKind::Connection | ErrorKind::Timeout),
            "{err}"
        );
    }
}
