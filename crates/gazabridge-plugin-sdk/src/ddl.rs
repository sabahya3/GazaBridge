//! Engine-neutral schema operations.

use gazabridge_domain::CanonicalType;
use serde::{Deserialize, Serialize};

use crate::schema::{NamespacePath, ObjectRef};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub canonical_type: CanonicalType,
    /// Native type override; when absent the connector maps `canonical_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_type: Option<String>,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyDefinition {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced: ObjectRef,
    pub referenced_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub object: ObjectRef,
    pub columns: Vec<ColumnDefinition>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SchemaOperation {
    CreateNamespace {
        namespace: NamespacePath,
        if_not_exists: bool,
    },
    CreateTable {
        table: TableDefinition,
        if_not_exists: bool,
    },
    CreateCollection {
        object: ObjectRef,
        if_not_exists: bool,
    },
    CreateIndex {
        object: ObjectRef,
        index: IndexDefinition,
        if_not_exists: bool,
    },
    DropObject {
        object: ObjectRef,
        if_exists: bool,
    },
    TruncateObject {
        object: ObjectRef,
    },
}

impl SchemaOperation {
    pub fn is_destructive(&self) -> bool {
        matches!(self, Self::DropObject { .. } | Self::TruncateObject { .. })
    }
}

/// The statements a connector would run for a set of operations, used for
/// previews and Dry Run. Producing a plan never touches the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlPlan {
    pub statements: Vec<String>,
    pub destructive: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
}
