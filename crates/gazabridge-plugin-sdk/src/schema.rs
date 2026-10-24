//! Object addressing and schema introspection types.

use std::fmt;

use gazabridge_domain::CanonicalType;
use gazabridge_domain::value::ValueKind;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// A path into a server's namespace hierarchy.
///
/// PostgreSQL uses `database / schema / object`, MySQL `database / object`,
/// MongoDB `database / collection` and SQLite `schema (main, temp, attached) / object`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NamespacePath {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl NamespacePath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn database(db: impl Into<String>) -> Self {
        Self {
            database: Some(db.into()),
            schema: None,
        }
    }

    pub fn schema(db: Option<String>, schema: impl Into<String>) -> Self {
        Self {
            database: db,
            schema: Some(schema.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    Collection,
    Sequence,
    Enum,
    Function,
    Procedure,
    Trigger,
    Index,
    Partition,
}

impl ObjectKind {
    /// Whether records can be read from objects of this kind.
    pub fn is_readable(&self) -> bool {
        matches!(
            self,
            Self::Table | Self::View | Self::MaterializedView | Self::Collection | Self::Partition
        )
    }

    pub fn is_writable(&self) -> bool {
        matches!(self, Self::Table | Self::Collection | Self::Partition)
    }
}

/// A fully addressed object (table, view, collection, …).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectRef {
    pub namespace: NamespacePath,
    pub name: String,
    pub kind: ObjectKind,
}

impl ObjectRef {
    pub fn table(namespace: NamespacePath, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
            kind: ObjectKind::Table,
        }
    }

    pub fn collection(database: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: NamespacePath::database(database),
            name: name.into(),
            kind: ObjectKind::Collection,
        }
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(db) = &self.namespace.database {
            write!(f, "{db}.")?;
        }
        if let Some(schema) = &self.namespace.schema {
            write!(f, "{schema}.")?;
        }
        f.write_str(&self.name)
    }
}

/// One entry returned by namespace listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceEntry {
    pub name: String,
    pub kind: ObjectKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Estimated rows/documents when cheaply available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    /// Native type as the server reports it, e.g. `character varying(255)`.
    pub native_type: String,
    pub canonical_type: CanonicalType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_expression: Option<String>,
    #[serde(default)]
    pub is_generated: bool,
    #[serde(default)]
    pub is_identity: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// 1-based ordinal position.
    pub ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// Native definition (e.g. `CREATE INDEX …` or a Mongo key document).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConstraintKind {
    PrimaryKey,
    Unique,
    ForeignKey {
        referenced: ObjectRef,
        referenced_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
    },
    Check {
        expression: String,
    },
    Exclusion {
        definition: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintInfo {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub kind: ConstraintKind,
}

/// Type statistics for an inferred document field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferredField {
    /// Dotted path, with `[]` marking array traversal, e.g. `items[].sku`.
    pub path: String,
    /// Documents in which the field was present (any value, including null).
    pub present_count: u64,
    pub null_count: u64,
    /// Count per observed value kind.
    pub type_counts: IndexMap<ValueKind, u64>,
    pub canonical_type: CanonicalType,
    /// For arrays: count per element kind.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub array_item_type_counts: IndexMap<ValueKind, u64>,
}

impl InferredField {
    pub fn occurrence_ratio(&self, sampled: u64) -> f64 {
        if sampled == 0 {
            0.0
        } else {
            self.present_count as f64 / sampled as f64
        }
    }

    pub fn is_mixed(&self) -> bool {
        self.type_counts
            .keys()
            .filter(|k| !matches!(k, ValueKind::Null | ValueKind::Missing))
            .count()
            > 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SamplingMethod {
    /// Server-side random sampling (e.g. `$sample`).
    Random,
    /// The first N documents in natural order.
    First,
    /// Every document.
    FullScan,
}

/// Inferred structure of a schemaless collection. Always marked as inferred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferredStructure {
    pub sampled_documents: u64,
    pub sampling: SamplingMethod,
    pub max_depth: u32,
    pub fields: Vec<InferredField>,
    /// Paths that appear both as objects and as scalars or arrays.
    pub conflicting_paths: Vec<String>,
}

/// Full description of a readable object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectSchema {
    pub object: ObjectRef,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
    /// Present for schemaless collections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inferred: Option<InferredStructure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Native DDL when the server can produce it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
}

impl ObjectSchema {
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
}
