//! MongoDB-family connector (MongoDB Community/Enterprise, Atlas, Amazon
//! DocumentDB and compatible servers, subject to their profiles).
//!
//! Transactions are offered only when the server is a replica set or a
//! sharded cluster. Read-only connections refuse every write and every
//! aggregation that contains `$out` or `$merge`.

mod connection;
mod error;
mod filter;
mod infer;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::{
    ConnectionProfile, ConnectionSettings, EngineFamily, MongoSettings, ReadPreference,
};
use gazabridge_plugin_sdk::capability::{CapabilityManifest, ConnectorLimits, QueryLanguage};
use gazabridge_plugin_sdk::{
    API_VERSION, Capability, Connection, ConnectorDescriptor, ConnectorError, ConnectorFactory,
    ConnectorResult, ErrorKind, ResolvedSecrets,
};
use mongodb::Client;
use mongodb::bson::doc;
use mongodb::options::{
    ClientOptions, Credential, ReadPreference as MongoReadPreference, SelectionCriteria,
    ServerAddress, Tls, TlsOptions,
};

pub use connection::MongoConnection;
pub(crate) use gazabridge_bson::convert;
pub use gazabridge_bson::{document_to_record, from_bson, record_to_document, to_bson};
pub use infer::Inferrer;

pub const CONNECTOR_ID: &str = "mongodb";

/// Manifest for a connection; transactions depend on the topology.
pub fn manifest(transactions: bool) -> CapabilityManifest {
    use Capability::*;
    let mut capabilities = BTreeSet::from([
        TestConnection,
        DetectServer,
        ListDatabases,
        ListCollections,
        ListViews,
        InspectSchema,
        InferDocumentStructure,
        InspectIndexes,
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
        CreateCollection,
        CreateIndex,
        DropObject,
        TruncateObject,
        ExecuteQuery,
        ExplainQuery,
        ExplainAnalyze,
        Checkpoints,
        ReadOnlySessions,
        HealthMonitoring,
    ]);
    if transactions {
        capabilities.insert(Transactions);
    }
    CapabilityManifest {
        connector_id: CONNECTOR_ID.into(),
        family: EngineFamily::MongoDb,
        api_version: API_VERSION,
        capabilities,
        write_modes: connection::direct_modes().into_iter().collect(),
        // Change streams arrive with the priority track's sync task (P12).
        sync_methods: BTreeSet::from([
            SyncMethod::Polling,
            SyncMethod::TimestampIncremental,
            SyncMethod::KeyIncremental,
        ]),
        query_languages: BTreeSet::from([
            QueryLanguage::MongoFilter,
            QueryLanguage::MongoAggregation,
        ]),
        limits: ConnectorLimits {
            max_identifier_bytes: None,
            max_bind_parameters: None,
            max_record_bytes: Some(16 * 1024 * 1024),
            max_nesting_depth: Some(100),
        },
    }
}

fn read_preference(rp: ReadPreference) -> MongoReadPreference {
    match rp {
        ReadPreference::Primary => MongoReadPreference::Primary,
        ReadPreference::PrimaryPreferred => MongoReadPreference::PrimaryPreferred { options: None },
        ReadPreference::Secondary => MongoReadPreference::Secondary { options: None },
        ReadPreference::SecondaryPreferred => {
            MongoReadPreference::SecondaryPreferred { options: None }
        }
        ReadPreference::Nearest => MongoReadPreference::Nearest { options: None },
    }
}

async fn client_options(
    profile: &ConnectionProfile,
    s: &MongoSettings,
    secrets: &ResolvedSecrets,
) -> ConnectorResult<ClientOptions> {
    let mut opts = match &s.uri {
        Some(uri) => ClientOptions::parse(uri).await.map_err(|e| {
            ConnectorError::from_driver(ErrorKind::InvalidInput, e.to_string(), None)
        })?,
        None => {
            if s.hosts.is_empty() {
                return Err(ConnectorError::invalid_input(
                    "a MongoDB connection needs a URI or at least one host",
                ));
            }
            let mut o = ClientOptions::default();
            o.hosts = s
                .hosts
                .iter()
                .map(|h| ServerAddress::Tcp {
                    host: h.host.clone(),
                    port: Some(h.port),
                })
                .collect();
            o
        }
    };
    if let Some(user) = &s.username {
        let mut c = Credential::default();
        c.username = Some(user.clone());
        c.password = secrets.password.as_ref().map(|p| p.to_string());
        c.source = s
            .auth_source
            .clone()
            .or_else(|| opts.credential.as_ref().and_then(|c| c.source.clone()));
        c.mechanism = opts.credential.as_ref().and_then(|c| c.mechanism.clone());
        opts.credential = Some(c);
    } else if let Some(c) = opts.credential.as_mut()
        && secrets.password.is_some()
    {
        c.password = secrets.password.as_ref().map(|p| p.to_string());
    }
    if s.replica_set.is_some() {
        opts.repl_set_name = s.replica_set.clone();
    }
    opts.direct_connection = Some(s.direct_connection);
    opts.tls = Some(if s.tls {
        if let (Some(cert), Some(key)) = (&profile.tls.client_certificate, &profile.tls.client_key)
            && cert != key
        {
            return Err(ConnectorError::invalid_input(
                "MongoDB needs the client certificate and key in one PEM file; point both settings to that file",
            ));
        }
        if secrets.client_key_passphrase.is_some() {
            return Err(ConnectorError::invalid_input(
                "encrypted client keys are not supported with the rustls TLS backend; provide an unencrypted PEM file with restrictive permissions",
            ));
        }
        let mut t = TlsOptions::default();
        t.ca_file_path = profile.tls.ca_certificate.clone();
        t.cert_key_file_path = profile.tls.client_certificate.clone();
        Tls::Enabled(t)
    } else {
        Tls::Disabled
    });
    opts.selection_criteria = Some(SelectionCriteria::ReadPreference(read_preference(
        s.read_preference,
    )));
    opts.server_selection_timeout =
        Some(Duration::from_millis(s.server_selection_timeout_ms.max(1)));
    opts.connect_timeout = Some(Duration::from_millis(s.connect_timeout_ms.max(1)));
    opts.app_name = Some("GazaBridge".into());
    if profile.compatibility_profile == "mongodb.documentdb" {
        // DocumentDB does not support retryable writes.
        opts.retry_writes = Some(false);
    }
    Ok(opts)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MongoConnectorFactory;

#[async_trait]
impl ConnectorFactory for MongoConnectorFactory {
    fn descriptor(&self) -> ConnectorDescriptor {
        ConnectorDescriptor {
            id: CONNECTOR_ID.into(),
            family: EngineFamily::MongoDb,
            display_name: "MongoDB".into(),
            api_version: API_VERSION,
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }

    fn base_manifest(&self) -> CapabilityManifest {
        manifest(true)
    }

    async fn connect(
        &self,
        profile: &ConnectionProfile,
        secrets: ResolvedSecrets,
    ) -> ConnectorResult<Arc<dyn Connection>> {
        let ConnectionSettings::MongoDb(s) = &profile.settings else {
            return Err(ConnectorError::invalid_input(
                "not a MongoDB connection profile",
            ));
        };
        if profile.ssh_tunnel.is_some() {
            return Err(ConnectorError::new(
                ErrorKind::Unsupported,
                "SSH tunnels are not available in this build yet",
            ));
        }
        let opts = client_options(profile, s, &secrets).await?;
        let client = Client::with_options(opts).map_err(error::map_mongo)?;
        // Surface connection and authentication errors now.
        let hello = client
            .database("admin")
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(error::map_mongo)?;
        let transactions = hello.contains_key("setName") || hello.get_str("msg") == Ok("isdbgrid");
        let base = manifest(transactions);
        let manifest = if profile.read_only {
            base.read_only()
        } else {
            base
        };
        Ok(Arc::new(MongoConnection::new(connection::Inner {
            client,
            manifest,
            read_only: profile.read_only,
            default_db: s.default_database.clone(),
            transactions,
        })))
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_domain::connection::{HostPort, SecretRef};

    use super::*;

    #[test]
    fn transactions_depend_on_topology() {
        assert!(manifest(true).supports(Capability::Transactions));
        assert!(!manifest(false).supports(Capability::Transactions));
        assert!(
            manifest(true)
                .read_only()
                .capabilities
                .iter()
                .all(|c| !c.is_mutating())
        );
    }

    #[tokio::test]
    async fn unreachable_server_fails_cleanly() {
        let profile = ConnectionProfile::new(
            "m",
            "mongodb.community",
            ConnectionSettings::MongoDb(MongoSettings {
                uri: None,
                hosts: vec![HostPort {
                    host: "127.0.0.1".into(),
                    port: 1,
                }],
                replica_set: None,
                auth_source: None,
                username: None,
                password: SecretRef::None,
                default_database: None,
                tls: false,
                read_preference: ReadPreference::Primary,
                direct_connection: true,
                server_selection_timeout_ms: 500,
                connect_timeout_ms: 500,
            }),
        );
        let err = MongoConnectorFactory
            .connect(&profile, ResolvedSecrets::default())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Connection, "{err}");
    }
}
