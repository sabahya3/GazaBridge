//! Explorer, data viewer and data editor exchange types.
//!
//! Records cross the boundary as typed JSON text (see
//! `gazabridge_domain::ejson`) so the UI never needs the domain model.

use serde::{Deserialize, Serialize};

/// Address of a database object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectAddr {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
    /// `table`, `view`, `collection`, …
    pub kind: String,
}

impl ObjectAddr {
    /// Dotted display name, e.g. `shop.public.orders`.
    pub fn display(&self) -> String {
        [
            self.database.as_deref(),
            self.schema.as_deref(),
            Some(self.name.as_str()),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplorerListArgs {
    pub connection_id: String,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
}

/// One node of the explorer tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDto {
    pub name: String,
    pub kind: String,
    /// Has children (databases, schemas).
    pub expandable: bool,
    /// Records can be browsed (tables, views, collections).
    pub readable: bool,
    #[serde(default)]
    pub estimated_count: Option<u64>,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Contains,
    StartsWith,
    IsNull,
    IsNotNull,
    /// Field absent (documents only).
    IsMissing,
}

impl FilterOp {
    pub const ALL: [FilterOp; 11] = [
        Self::Eq,
        Self::Ne,
        Self::Lt,
        Self::Le,
        Self::Gt,
        Self::Ge,
        Self::Contains,
        Self::StartsWith,
        Self::IsNull,
        Self::IsNotNull,
        Self::IsMissing,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Lt => "lt",
            Self::Le => "le",
            Self::Gt => "gt",
            Self::Ge => "ge",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::IsNull => "is_null",
            Self::IsNotNull => "is_not_null",
            Self::IsMissing => "is_missing",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.as_str() == s)
    }

    pub fn takes_value(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull | Self::IsMissing)
    }
}

/// One filter condition. `value` is typed JSON (`42`, `"text"`,
/// `{"$oid": "…"}`); text that is not valid JSON is taken as a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionDto {
    pub field: String,
    pub op: FilterOp,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterDto {
    /// Match any condition instead of all of them.
    #[serde(default)]
    pub any: bool,
    pub conditions: Vec<ConditionDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortDto {
    pub field: String,
    pub descending: bool,
}

/// A MongoDB query as typed in the query bar (mongosh / Compass syntax):
/// `{ status: "open" }`, `{ name: 1, _id: 0 }`, `{ createdAt: -1 }`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeQueryDto {
    #[serde(default)]
    pub filter: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub sort: String,
}

impl NativeQueryDto {
    pub fn is_empty(&self) -> bool {
        [&self.filter, &self.project, &self.sort].iter().all(|t| {
            let t = t.trim();
            t.is_empty() || t == "{}"
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowseArgs {
    pub connection_id: String,
    pub object: ObjectAddr,
    #[serde(default)]
    pub filter: Option<FilterDto>,
    #[serde(default)]
    pub sort: Option<SortDto>,
    #[serde(default)]
    pub query: Option<NativeQueryDto>,
    pub offset: u64,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDto {
    pub name: String,
    /// Native type, empty for document fields.
    pub native_type: String,
    pub nullable: bool,
    /// Part of the record key (`_id` or primary key).
    pub key: bool,
}

/// One displayed value. `kind` is the value kind (`null`, `missing`,
/// `string`, `object_id`, …) so the UI can tell null from missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellDto {
    pub kind: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowDto {
    pub cells: Vec<CellDto>,
    /// The whole record as pretty typed JSON.
    pub json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowsePageDto {
    pub columns: Vec<ColumnDto>,
    pub rows: Vec<RowDto>,
    pub offset: u64,
    pub has_more: bool,
    pub key_fields: Vec<String>,
    /// Why records cannot be edited: `read_only_connection`,
    /// `not_writable`, `no_primary_key` or `unsupported`.
    #[serde(default)]
    pub edit_blocker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountArgs {
    pub connection_id: String,
    pub object: ObjectAddr,
    #[serde(default)]
    pub filter: Option<FilterDto>,
    #[serde(default)]
    pub query: Option<NativeQueryDto>,
    /// False asks for a cheap estimate (no filter only).
    #[serde(default = "exact_default")]
    pub exact: bool,
    /// Server-side limit for exact counts (default 3 s); past it the count
    /// is reported as unknown instead of loading the server.
    #[serde(default)]
    pub max_time_ms: Option<u32>,
}

fn exact_default() -> bool {
    true
}

/// Objects of one tree level whose estimated sizes are wanted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeCountsArgs {
    pub connection_id: String,
    pub objects: Vec<ObjectAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountDto {
    pub count: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertArgs {
    pub connection_id: String,
    pub object: ObjectAddr,
    pub json: String,
}

/// An edit of one record: the version the user loaded and the edited text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditArgs {
    pub connection_id: String,
    pub object: ObjectAddr,
    pub original: String,
    pub edited: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRecordArgs {
    pub connection_id: String,
    pub object: ObjectAddr,
    pub original: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldEditDto {
    pub field: String,
    /// Compact typed JSON; `None` when the field is added.
    pub before: Option<String>,
    /// Compact typed JSON; `None` when the field is removed.
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditPlanDto {
    pub changes: Vec<FieldEditDto>,
    pub conflict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationDto {
    pub matched: u64,
    pub modified: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_ops_round_trip_their_names() {
        for op in FilterOp::ALL {
            assert_eq!(FilterOp::parse(op.as_str()), Some(op));
            let json = serde_json::to_string(&op).unwrap();
            assert_eq!(json, format!("\"{}\"", op.as_str()));
        }
    }

    #[test]
    fn display_joins_present_parts() {
        let a = ObjectAddr {
            database: Some("shop".into()),
            schema: None,
            name: "orders".into(),
            kind: "collection".into(),
        };
        assert_eq!(a.display(), "shop.orders");
    }
}
