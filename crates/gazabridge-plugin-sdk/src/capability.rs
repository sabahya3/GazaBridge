//! Capability manifests.

use std::collections::BTreeSet;
use std::fmt;

use gazabridge_domain::compatibility::SyncMethod;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::job::WriteMode;
use serde::{Deserialize, Serialize};

use crate::ApiVersion;

/// An operation a connection may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    TestConnection,
    DetectServer,
    ListDatabases,
    ListSchemas,
    ListTables,
    ListViews,
    ListCollections,
    InspectSchema,
    InferDocumentStructure,
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
    CreateCollection,
    CreateIndex,
    DropObject,
    TruncateObject,
    ExecuteQuery,
    CancelQuery,
    ExplainQuery,
    ExplainAnalyze,
    BulkImport,
    BulkExport,
    ChangeDataCapture,
    ResumeTokens,
    Checkpoints,
    ReadOnlySessions,
    HealthMonitoring,
}

/// Every capability, in declaration order.
pub const ALL_CAPABILITIES: &[Capability] = &[
    Capability::TestConnection,
    Capability::DetectServer,
    Capability::ListDatabases,
    Capability::ListSchemas,
    Capability::ListTables,
    Capability::ListViews,
    Capability::ListCollections,
    Capability::InspectSchema,
    Capability::InferDocumentStructure,
    Capability::InspectIndexes,
    Capability::InspectConstraints,
    Capability::InspectRoutines,
    Capability::InspectSequences,
    Capability::InspectEnums,
    Capability::InspectTriggers,
    Capability::InspectPartitions,
    Capability::StreamRead,
    Capability::PaginatedRead,
    Capability::FilteredRead,
    Capability::SortedRead,
    Capability::CountEstimate,
    Capability::ExactCount,
    Capability::WriteInsert,
    Capability::WriteUpdate,
    Capability::WriteDelete,
    Capability::WriteUpsert,
    Capability::Transactions,
    Capability::CreateSchema,
    Capability::CreateTable,
    Capability::CreateCollection,
    Capability::CreateIndex,
    Capability::DropObject,
    Capability::TruncateObject,
    Capability::ExecuteQuery,
    Capability::CancelQuery,
    Capability::ExplainQuery,
    Capability::ExplainAnalyze,
    Capability::BulkImport,
    Capability::BulkExport,
    Capability::ChangeDataCapture,
    Capability::ResumeTokens,
    Capability::Checkpoints,
    Capability::ReadOnlySessions,
    Capability::HealthMonitoring,
];

impl Capability {
    pub fn as_str(&self) -> &'static str {
        // Must match the serde names; `as_str_matches_serde_name` checks this.
        match self {
            Self::TestConnection => "test_connection",
            Self::DetectServer => "detect_server",
            Self::ListDatabases => "list_databases",
            Self::ListSchemas => "list_schemas",
            Self::ListTables => "list_tables",
            Self::ListViews => "list_views",
            Self::ListCollections => "list_collections",
            Self::InspectSchema => "inspect_schema",
            Self::InferDocumentStructure => "infer_document_structure",
            Self::InspectIndexes => "inspect_indexes",
            Self::InspectConstraints => "inspect_constraints",
            Self::InspectRoutines => "inspect_routines",
            Self::InspectSequences => "inspect_sequences",
            Self::InspectEnums => "inspect_enums",
            Self::InspectTriggers => "inspect_triggers",
            Self::InspectPartitions => "inspect_partitions",
            Self::StreamRead => "stream_read",
            Self::PaginatedRead => "paginated_read",
            Self::FilteredRead => "filtered_read",
            Self::SortedRead => "sorted_read",
            Self::CountEstimate => "count_estimate",
            Self::ExactCount => "exact_count",
            Self::WriteInsert => "write_insert",
            Self::WriteUpdate => "write_update",
            Self::WriteDelete => "write_delete",
            Self::WriteUpsert => "write_upsert",
            Self::Transactions => "transactions",
            Self::CreateSchema => "create_schema",
            Self::CreateTable => "create_table",
            Self::CreateCollection => "create_collection",
            Self::CreateIndex => "create_index",
            Self::DropObject => "drop_object",
            Self::TruncateObject => "truncate_object",
            Self::ExecuteQuery => "execute_query",
            Self::CancelQuery => "cancel_query",
            Self::ExplainQuery => "explain_query",
            Self::ExplainAnalyze => "explain_analyze",
            Self::BulkImport => "bulk_import",
            Self::BulkExport => "bulk_export",
            Self::ChangeDataCapture => "change_data_capture",
            Self::ResumeTokens => "resume_tokens",
            Self::Checkpoints => "checkpoints",
            Self::ReadOnlySessions => "read_only_sessions",
            Self::HealthMonitoring => "health_monitoring",
        }
    }

    /// Capabilities that modify the target. Disabled on read-only connections.
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Self::WriteInsert
                | Self::WriteUpdate
                | Self::WriteDelete
                | Self::WriteUpsert
                | Self::CreateSchema
                | Self::CreateTable
                | Self::CreateCollection
                | Self::CreateIndex
                | Self::DropObject
                | Self::TruncateObject
                | Self::BulkImport
        )
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Query languages a connection can execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    PostgresSql,
    MySql,
    SqliteSql,
    MongoFilter,
    MongoAggregation,
}

/// Hard limits a connector imposes on requests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorLimits {
    /// Maximum identifier length in bytes.
    pub max_identifier_bytes: Option<u32>,
    /// Maximum number of bind parameters per statement.
    pub max_bind_parameters: Option<u32>,
    /// Maximum size of a single record/document in bytes.
    pub max_record_bytes: Option<u64>,
    /// Maximum nesting depth of documents.
    pub max_nesting_depth: Option<u32>,
}

/// What a connector (or a specific connection to a specific server) can do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityManifest {
    pub connector_id: String,
    pub family: EngineFamily,
    pub api_version: ApiVersion,
    pub capabilities: BTreeSet<Capability>,
    pub write_modes: BTreeSet<WriteMode>,
    pub sync_methods: BTreeSet<SyncMethod>,
    pub query_languages: BTreeSet<QueryLanguage>,
    pub limits: ConnectorLimits,
}

impl CapabilityManifest {
    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub fn supports_write_mode(&self, mode: WriteMode) -> bool {
        self.write_modes.contains(&mode)
    }

    /// Returns a copy with every mutating capability and write mode removed.
    pub fn read_only(&self) -> Self {
        let mut m = self.clone();
        m.capabilities.retain(|c| !c.is_mutating());
        m.write_modes.clear();
        m
    }

    /// Applies compatibility-profile overrides.
    pub fn with_overrides(
        &self,
        remove: &BTreeSet<Capability>,
        add: &BTreeSet<Capability>,
    ) -> Self {
        let mut m = self.clone();
        m.capabilities.retain(|c| !remove.contains(c));
        m.capabilities.extend(add.iter().copied());
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_matches_serde_name() {
        for c in ALL_CAPABILITIES {
            assert_eq!(
                serde_json::to_value(c).unwrap(),
                serde_json::Value::String(c.as_str().into())
            );
        }
    }

    #[test]
    fn read_only_strips_mutations() {
        let m = CapabilityManifest {
            connector_id: "x".into(),
            family: EngineFamily::Sqlite,
            api_version: crate::API_VERSION,
            capabilities: [
                Capability::StreamRead,
                Capability::WriteInsert,
                Capability::DropObject,
            ]
            .into(),
            write_modes: [WriteMode::Insert].into(),
            sync_methods: BTreeSet::new(),
            query_languages: BTreeSet::new(),
            limits: ConnectorLimits::default(),
        };
        let ro = m.read_only();
        assert!(ro.supports(Capability::StreamRead));
        assert!(!ro.supports(Capability::WriteInsert));
        assert!(!ro.supports(Capability::DropObject));
        assert!(ro.write_modes.is_empty());
    }
}
