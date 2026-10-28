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
    /// The source engine's full index specification (for MongoDB: the
    /// `listIndexes` document as canonical Extended JSON). A connector of the
    /// same family recreates the index exactly from it (direction, TTL,
    /// partial filter, collation, text/geo); others use `columns`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native: Option<serde_json::Value>,
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
        /// Engine options as Extended JSON (MongoDB: `create` options such
        /// as `capped`, `collation`, `timeseries`, or `viewOn` + `pipeline`
        /// for a view).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        options: Option<serde_json::Value>,
    },
    /// Changes options of an existing collection (MongoDB `collMod`), e.g.
    /// a validator added once the data is in place.
    AlterCollection {
        object: ObjectRef,
        options: serde_json::Value,
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
