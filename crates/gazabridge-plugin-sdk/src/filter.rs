//! A portable filter language.
//!
//! Filters are expressed over canonical values and translated by each
//! connector into parameterized SQL or a MongoDB filter document. Values
//! are always bound as parameters, never interpolated.

use gazabridge_domain::Value;
use serde::{Deserialize, Serialize};

/// A field reference. Nested document paths use `.` separators.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldPath(pub String);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }

    pub fn is_nested(&self) -> bool {
        self.0.contains('.')
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Filter {
    Eq {
        field: FieldPath,
        value: Value,
    },
    Ne {
        field: FieldPath,
        value: Value,
    },
    Lt {
        field: FieldPath,
        value: Value,
    },
    Le {
        field: FieldPath,
        value: Value,
    },
    Gt {
        field: FieldPath,
        value: Value,
    },
    Ge {
        field: FieldPath,
        value: Value,
    },
    In {
        field: FieldPath,
        values: Vec<Value>,
    },
    IsNull {
        field: FieldPath,
    },
    IsNotNull {
        field: FieldPath,
    },
    /// Field absent (documents only; relational connectors reject it).
    IsMissing {
        field: FieldPath,
    },
    /// Case-sensitive substring match.
    Contains {
        field: FieldPath,
        text: String,
    },
    StartsWith {
        field: FieldPath,
        text: String,
    },
    And {
        filters: Vec<Filter>,
    },
    Or {
        filters: Vec<Filter>,
    },
    Not {
        filter: Box<Filter>,
    },
    /// A query in the engine's own language, as typed JSON (a MongoDB
    /// filter document). Only connectors that declare native queries
    /// accept it; others fail with `Unsupported`.
    Native {
        query: serde_json::Value,
    },
}

impl Filter {
    pub fn eq(field: impl Into<String>, value: Value) -> Self {
        Self::Eq {
            field: FieldPath::new(field),
            value,
        }
    }

    pub fn and(filters: Vec<Filter>) -> Self {
        Self::And { filters }
    }

    /// Fields referenced anywhere in the filter.
    pub fn fields(&self) -> Vec<&FieldPath> {
        let mut out = Vec::new();
        self.collect_fields(&mut out);
        out
    }

    fn collect_fields<'a>(&'a self, out: &mut Vec<&'a FieldPath>) {
        match self {
            Self::Eq { field, .. }
            | Self::Ne { field, .. }
            | Self::Lt { field, .. }
            | Self::Le { field, .. }
            | Self::Gt { field, .. }
            | Self::Ge { field, .. }
            | Self::In { field, .. }
            | Self::IsNull { field }
            | Self::IsNotNull { field }
            | Self::IsMissing { field }
            | Self::Contains { field, .. }
            | Self::StartsWith { field, .. } => out.push(field),
            Self::And { filters } | Self::Or { filters } => {
                for f in filters {
                    f.collect_fields(out);
                }
            }
            Self::Not { filter } => filter.collect_fields(out),
            Self::Native { .. } => {}
        }
    }

    /// True when the filter restricts rows (used by destructive-operation
    /// guards: an `UPDATE`/`DELETE` with a trivially true filter is unbounded).
    pub fn is_restrictive(&self) -> bool {
        match self {
            Self::And { filters } => filters.iter().any(Filter::is_restrictive),
            Self::Or { filters } => {
                !filters.is_empty() && filters.iter().all(Filter::is_restrictive)
            }
            Self::Not { filter } => {
                !matches!(**filter, Self::And { ref filters } if filters.is_empty())
            }
            // An empty native query matches everything.
            Self::Native { query } => query.as_object().is_some_and(|o| !o.is_empty()),
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    pub field: FieldPath,
    pub direction: SortDirection,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_is_not_restrictive() {
        assert!(!Filter::and(vec![]).is_restrictive());
        assert!(Filter::and(vec![Filter::eq("id", Value::Int(1))]).is_restrictive());
        assert!(
            !Filter::Or {
                filters: vec![Filter::eq("a", Value::Int(1)), Filter::and(vec![])]
            }
            .is_restrictive()
        );
    }

    #[test]
    fn collects_nested_fields() {
        let f = Filter::Or {
            filters: vec![
                Filter::eq("a", Value::Int(1)),
                Filter::Not {
                    filter: Box::new(Filter::IsNull {
                        field: FieldPath::new("b.c"),
                    }),
                },
            ],
        };
        let names: Vec<_> = f.fields().iter().map(|p| p.0.as_str()).collect();
        assert_eq!(names, ["a", "b.c"]);
    }
}
