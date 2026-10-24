//! A connection wrapper that enforces a compatibility profile.
//!
//! Profiles can remove capabilities a family connector would otherwise
//! offer (CockroachDB has no logical replication, TiDB no binlog, …). The
//! wrapper exposes the effective manifest and refuses removed operations
//! before they reach the connector.

use std::sync::Arc;

use async_trait::async_trait;
use gazabridge_domain::CanonicalType;
use gazabridge_plugin_sdk::cdc::{ChangeFeedRequest, ChangeStream};
use gazabridge_plugin_sdk::connector::{CountResult, HealthReport, InspectOptions, Transaction};
use gazabridge_plugin_sdk::ddl::{DdlPlan, SchemaOperation};
use gazabridge_plugin_sdk::filter::Filter;
use gazabridge_plugin_sdk::query::{ExplainPlan, QueryRequest, QueryResult};
use gazabridge_plugin_sdk::read::{Page, PageRequest, ReadPosition, ReadRequest, RecordStream};
use gazabridge_plugin_sdk::schema::{NamespaceEntry, NamespacePath, ObjectRef, ObjectSchema};
use gazabridge_plugin_sdk::write::{
    DeleteRequest, MutationOutcome, UpdateRequest, WriteBatch, WriteOutcome,
};
use gazabridge_plugin_sdk::{
    Capability, CapabilityManifest, CompatibilityProfile, Connection, ConnectorError,
    ConnectorResult, ServerInfo,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct ProfiledConnection {
    inner: Arc<dyn Connection>,
    manifest: CapabilityManifest,
}

impl ProfiledConnection {
    /// Wraps `inner`; only removals from the profile take effect, since a
    /// profile cannot make a connector implement something it does not.
    pub fn new(inner: Arc<dyn Connection>, profile: &CompatibilityProfile) -> Self {
        let mut manifest = inner.manifest().clone();
        manifest
            .capabilities
            .retain(|c| !profile.capability_overrides.remove.contains(c));
        Self { inner, manifest }
    }

    fn allow(&self, c: Capability) -> ConnectorResult<()> {
        if self.manifest.supports(c) {
            Ok(())
        } else {
            Err(ConnectorError::unsupported(c))
        }
    }

    fn allow_ddl(&self, ops: &[SchemaOperation]) -> ConnectorResult<()> {
        for op in ops {
            self.allow(match op {
                SchemaOperation::CreateNamespace { .. } => Capability::CreateSchema,
                SchemaOperation::CreateTable { .. } => Capability::CreateTable,
                SchemaOperation::CreateCollection { .. } => Capability::CreateCollection,
                SchemaOperation::CreateIndex { .. } => Capability::CreateIndex,
                SchemaOperation::DropObject { .. } => Capability::DropObject,
                SchemaOperation::TruncateObject { .. } => Capability::TruncateObject,
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl Connection for ProfiledConnection {
    fn manifest(&self) -> &CapabilityManifest {
        &self.manifest
    }

    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    async fn health_check(&self) -> ConnectorResult<HealthReport> {
        self.inner.health_check().await
    }

    async fn server_info(&self) -> ConnectorResult<ServerInfo> {
        self.inner.server_info().await
    }

    async fn list(&self, parent: &NamespacePath) -> ConnectorResult<Vec<NamespaceEntry>> {
        self.inner.list(parent).await
    }

    fn quote_identifier(&self, ident: &str) -> ConnectorResult<String> {
        self.inner.quote_identifier(ident)
    }

    async fn close(&self) {
        self.inner.close().await;
    }

    async fn inspect(
        &self,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> ConnectorResult<ObjectSchema> {
        self.allow(Capability::InspectSchema)?;
        self.inner.inspect(object, options).await
    }

    async fn read(
        &self,
        request: ReadRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<RecordStream> {
        self.allow(Capability::StreamRead)?;
        self.inner.read(request, cancel).await
    }

    async fn read_page(
        &self,
        request: PageRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<Page> {
        self.allow(Capability::PaginatedRead)?;
        self.inner.read_page(request, cancel).await
    }

    async fn count(
        &self,
        object: &ObjectRef,
        filter: Option<&Filter>,
        exact: bool,
    ) -> ConnectorResult<CountResult> {
        self.allow(if exact {
            Capability::ExactCount
        } else {
            Capability::CountEstimate
        })?;
        self.inner.count(object, filter, exact).await
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        cancel: CancellationToken,
    ) -> ConnectorResult<WriteOutcome> {
        self.allow(Capability::WriteInsert)?;
        self.inner.write_batch(batch, cancel).await
    }

    async fn update(&self, request: UpdateRequest) -> ConnectorResult<MutationOutcome> {
        self.allow(Capability::WriteUpdate)?;
        self.inner.update(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> ConnectorResult<MutationOutcome> {
        self.allow(Capability::WriteDelete)?;
        self.inner.delete(request).await
    }

    async fn begin(&self) -> ConnectorResult<Box<dyn Transaction>> {
        self.allow(Capability::Transactions)?;
        self.inner.begin().await
    }

    fn plan_ddl(&self, ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
        self.allow_ddl(ops)?;
        self.inner.plan_ddl(ops)
    }

    async fn apply_ddl(
        &self,
        ops: &[SchemaOperation],
        cancel: CancellationToken,
    ) -> ConnectorResult<()> {
        self.allow_ddl(ops)?;
        self.inner.apply_ddl(ops, cancel).await
    }

    fn native_type_for(&self, canonical: &CanonicalType) -> ConnectorResult<String> {
        self.inner.native_type_for(canonical)
    }

    async fn execute(
        &self,
        request: QueryRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<QueryResult> {
        self.allow(Capability::ExecuteQuery)?;
        self.inner.execute(request, cancel).await
    }

    async fn explain(
        &self,
        request: QueryRequest,
        analyze: bool,
        cancel: CancellationToken,
    ) -> ConnectorResult<ExplainPlan> {
        self.allow(if analyze {
            Capability::ExplainAnalyze
        } else {
            Capability::ExplainQuery
        })?;
        self.inner.explain(request, analyze, cancel).await
    }

    async fn change_feed(
        &self,
        request: ChangeFeedRequest,
        cancel: CancellationToken,
    ) -> ConnectorResult<ChangeStream> {
        self.allow(Capability::ChangeDataCapture)?;
        self.inner.change_feed(request, cancel).await
    }

    async fn current_position(&self, objects: &[ObjectRef]) -> ConnectorResult<ReadPosition> {
        self.allow(Capability::ResumeTokens)?;
        self.inner.current_position(objects).await
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_connectors::ProfileCatalog;
    use gazabridge_domain::connection::{
        ConnectionProfile, ConnectionSettings, SqliteJournalMode, SqliteSettings,
    };
    use gazabridge_plugin_sdk::capability::QueryLanguage;
    use gazabridge_plugin_sdk::{ConnectorFactory, ResolvedSecrets};

    use super::*;

    #[tokio::test]
    async fn removed_capabilities_are_hidden_and_refused() {
        let dir = tempfile::tempdir().unwrap();
        let profile = ConnectionProfile::new(
            "p",
            "sqlite.sqlite",
            ConnectionSettings::Sqlite(SqliteSettings {
                path: dir.path().join("p.db"),
                read_only: false,
                create_if_missing: true,
                busy_timeout_ms: 1000,
                journal_mode: SqliteJournalMode::Wal,
                foreign_keys: true,
            }),
        );
        let raw = gazabridge_connector_sqlite::SqliteConnectorFactory
            .connect(&profile, ResolvedSecrets::default())
            .await
            .unwrap();
        let mut catalog_profile = ProfileCatalog::builtin()
            .unwrap()
            .get("sqlite.sqlite")
            .unwrap()
            .clone();
        catalog_profile
            .capability_overrides
            .remove
            .insert(Capability::ExecuteQuery);
        let conn = ProfiledConnection::new(raw, &catalog_profile);
        assert!(!conn.manifest().supports(Capability::ExecuteQuery));
        assert!(conn.manifest().supports(Capability::StreamRead));
        let err = conn
            .execute(
                QueryRequest {
                    language: QueryLanguage::SqliteSql,
                    text: "SELECT 1".into(),
                    params: vec![],
                    max_rows: 1,
                    timeout: None,
                    database: None,
                    collection: None,
                },
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ConnectorError::Unsupported {
                capability: Capability::ExecuteQuery
            }
        ));
        assert!(conn.health_check().await.is_ok());
    }
}
