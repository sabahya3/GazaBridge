//! The connector traits.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gazabridge_domain::CanonicalType;
use gazabridge_domain::connection::{ConnectionProfile, EngineFamily};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::ApiVersion;
use crate::capability::{Capability, CapabilityManifest};
use crate::cdc::{ChangeFeedRequest, ChangeStream};
use crate::ddl::{DdlPlan, SchemaOperation};
use crate::error::{ConnectorError, ConnectorResult};
use crate::filter::Filter;
use crate::profile::ServerInfo;
use crate::query::{ExplainPlan, QueryRequest, QueryResult};
use crate::read::{Page, PageRequest, ReadPosition, ReadRequest, RecordStream};
use crate::schema::{NamespaceEntry, NamespacePath, ObjectRef, ObjectSchema, SamplingMethod};
use crate::write::{DeleteRequest, MutationOutcome, UpdateRequest, WriteBatch, WriteOutcome};

/// Static description of a connector implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorDescriptor {
    pub id: String,
    pub family: EngineFamily,
    pub display_name: String,
    pub api_version: ApiVersion,
    /// Implementation version (crate version).
    pub version: String,
}

/// Secrets resolved for one connection attempt. Zeroized on drop and never
/// printed by `Debug`.
#[derive(Default)]
pub struct ResolvedSecrets {
    pub password: Option<Zeroizing<String>>,
    pub client_key_passphrase: Option<Zeroizing<String>>,
    pub ssh_password: Option<Zeroizing<String>>,
    pub ssh_key_passphrase: Option<Zeroizing<String>>,
}

impl fmt::Debug for ResolvedSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mark = |o: &Option<Zeroizing<String>>| if o.is_some() { "<set>" } else { "<unset>" };
        f.debug_struct("ResolvedSecrets")
            .field("password", &mark(&self.password))
            .field("client_key_passphrase", &mark(&self.client_key_passphrase))
            .field("ssh_password", &mark(&self.ssh_password))
            .field("ssh_key_passphrase", &mark(&self.ssh_key_passphrase))
            .finish()
    }
}

/// Opens connections for one engine family.
#[async_trait]
pub trait ConnectorFactory: Send + Sync + fmt::Debug {
    fn descriptor(&self) -> ConnectorDescriptor;

    /// Capabilities before server detection and profile overrides.
    fn base_manifest(&self) -> CapabilityManifest;

    /// Opens a connection. Implementations must honour `profile.read_only`
    /// by refusing every mutating operation, ideally at the session level too.
    async fn connect(
        &self,
        profile: &ConnectionProfile,
        secrets: ResolvedSecrets,
    ) -> ConnectorResult<Arc<dyn Connection>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReport {
    pub latency: Duration,
    pub checked_at: DateTime<Utc>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectOptions {
    pub include_indexes: bool,
    pub include_constraints: bool,
    /// Documents to sample for schemaless inference.
    pub sample_size: u64,
    pub sampling: SamplingMethod,
}

impl Default for InspectOptions {
    fn default() -> Self {
        Self {
            include_indexes: true,
            include_constraints: true,
            sample_size: 1_000,
            sampling: SamplingMethod::Random,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountResult {
    pub count: u64,
    pub exact: bool,
}

/// A transaction opened by [`Connection::begin`]. Dropping it without
/// committing rolls back.
#[async_trait]
pub trait Transaction: Send {
    async fn write_batch(&mut self, batch: WriteBatch) -> ConnectorResult<WriteOutcome>;
    async fn update(&mut self, request: UpdateRequest) -> ConnectorResult<MutationOutcome>;
    async fn delete(&mut self, request: DeleteRequest) -> ConnectorResult<MutationOutcome>;
    async fn execute(&mut self, request: QueryRequest) -> ConnectorResult<QueryResult>;
    async fn commit(self: Box<Self>) -> ConnectorResult<()>;
    async fn rollback(self: Box<Self>) -> ConnectorResult<()>;
}

/// An open connection.
///
/// Methods with default implementations correspond to optional
/// capabilities: the default returns [`ConnectorError::Unsupported`], and a
/// connector that declares the capability in its manifest must override it.
#[async_trait]
pub trait Connection: Send + Sync + fmt::Debug {
    /// Effective manifest for this connection (server-detected, profile
    /// overrides applied, mutations removed when read-only).
    fn manifest(&self) -> &CapabilityManifest;

    fn is_read_only(&self) -> bool;

    /// Round-trips to the server and reports latency.
    async fn health_check(&self) -> ConnectorResult<HealthReport>;

    async fn server_info(&self) -> ConnectorResult<ServerInfo>;

    /// Lists children of `parent`: databases at the root, then schemas,
    /// tables, views and collections as the engine's hierarchy dictates.
    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>>;

    /// Quotes an identifier for this engine, rejecting ones it cannot represent.
    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String>;

    /// Releases pooled resources.
    async fn close(&self);

    async fn inspect(
        &self,
        _object: &ObjectRef,
        _options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        Err(ConnectorError::unsupported(Capability::InspectSchema))
    }

    async fn read(
        &self,
        _request: ReadRequest,
        _cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        Err(ConnectorError::unsupported(Capability::StreamRead))
    }

    async fn read_page(
        &self,
        _request: PageRequest,
        _cancel: CancellationToken,
    ) -> ConnectorResult<Page> {
        Err(ConnectorError::unsupported(Capability::PaginatedRead))
    }

    async fn count(
        &self,
        _object: &ObjectRef,
        _filter: Option<&Filter>,
        exact: bool,
    ) -> ConnectorResult<CountResult> {
        Err(ConnectorError::unsupported(if exact {
            Capability::ExactCount
        } else {
            Capability::CountEstimate
        }))
    }

    async fn write_batch(
        &self,
        _batch: WriteBatch,
        _cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        Err(ConnectorError::unsupported(Capability::WriteInsert))
    }

    async fn update(&self, _request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        Err(ConnectorError::unsupported(Capability::WriteUpdate))
    }

    async fn delete(&self, _request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        Err(ConnectorError::unsupported(Capability::WriteDelete))
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        Err(ConnectorError::unsupported(Capability::Transactions))
    }

    /// Renders the statements for `ops` without executing anything.
    fn plan_ddl(&self, _ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
        Err(ConnectorError::unsupported(Capability::CreateTable))
    }

    async fn apply_ddl(
        &self,
        _ops: &[SchemaOperation],
        _cancel: CancellationToken,
    ) -> ConnectorResult<()> {
        Err(ConnectorError::unsupported(Capability::CreateTable))
    }

    /// Maps a canonical type to this engine's native column type.
    fn native_type_for(&self, _canonical: &CanonicalType) -> ConnectorResult<String> {
        Err(ConnectorError::unsupported(Capability::CreateTable))
    }

    async fn execute(
        &self,
        _request: QueryRequest,
        _cancel: CancellationToken,
    ) -> ConnectorResult<QueryResult> {
        Err(ConnectorError::unsupported(Capability::ExecuteQuery))
    }

    async fn explain(
        &self,
        _request: QueryRequest,
        analyze: bool,
        _cancel: CancellationToken,
    ) -> ConnectorResult<ExplainPlan> {
        Err(ConnectorError::unsupported(if analyze {
            Capability::ExplainAnalyze
        } else {
            Capability::ExplainQuery
        }))
    }

    async fn change_feed(
        &self,
        _request: ChangeFeedRequest,
        _cancel: CancellationToken,
    ) -> ConnectorResult<ChangeStream> {
        Err(ConnectorError::unsupported(Capability::ChangeDataCapture))
    }

    /// Current change-feed position, used as the snapshot boundary before an
    /// initial load so no change between snapshot and streaming is lost.
    async fn current_position(&self, _objects: &[ObjectRef]) -> ConnectorResult<ReadPosition> {
        Err(ConnectorError::unsupported(Capability::ResumeTokens))
    }
}

/// Fails with [`ConnectorError::ReadOnly`] when `conn` is read-only.
pub fn ensure_writable(conn: &dyn Connection) -> ConnectorResult<()> {
    if conn.is_read_only() {
        Err(ConnectorError::ReadOnly)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_secrets_debug_hides_values() {
        let s = ResolvedSecrets {
            password: Some(Zeroizing::new("hunter2".into())),
            ..Default::default()
        };
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("hunter2"));
        assert!(dbg.contains("<set>"));
    }
}
