//! Portable filters → MongoDB query documents.
//!
//! MongoDB's `{f: null}` also matches documents where `f` is missing, so
//! null and missing checks use `$type` and `$exists` to keep the canonical
//! distinction.

use gazabridge_domain::Value;
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult};
use mongodb::bson::{Bson, Document, doc};

use crate::convert::to_bson;

fn value(v: &Value) -> ConnectorResult<Bson> {
    to_bson(v, None)?.ok_or_else(|| {
        ConnectorError::invalid_input("missing is not a comparable value; use IsMissing")
    })
}

fn field(path: &str) -> ConnectorResult<String> {
    if path.is_empty() || path.contains('\0') {
        return Err(ConnectorError::invalid_input("invalid field path"));
    }
    if path.starts_with('$') {
        return Err(ConnectorError::invalid_input(
            "field paths cannot start with `$`",
        ));
    }
    Ok(path.to_owned())
}

/// Escapes a literal for use inside a regular expression.
pub fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\^$.|?*+()[]{}/-".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

pub fn to_query(f: &Filter) -> ConnectorResult<Document> {
    let op = |path: &str, op: &str, v: &Value| -> ConnectorResult<Document> {
        Ok(doc! { field(path)?: { op: value(v)? } })
    };
    Ok(match f {
        Filter::Eq {
            field: p,
            value: Value::Null,
        } => doc! { field(&p.0)?: { "$type": "null" } },
        Filter::Eq {
            field: p,
            value: Value::Missing,
        } => doc! { field(&p.0)?: { "$exists": false } },
        Filter::Eq { field: p, value: v } => doc! { field(&p.0)?: { "$eq": value(v)? } },
        Filter::Ne {
            field: p,
            value: Value::Null,
        } => doc! { field(&p.0)?: { "$not": { "$type": "null" } } },
        Filter::Ne {
            field: p,
            value: Value::Missing,
        } => doc! { field(&p.0)?: { "$exists": true } },
        Filter::Ne { field: p, value: v } => op(&p.0, "$ne", v)?,
        Filter::Lt { field: p, value: v } => op(&p.0, "$lt", v)?,
        Filter::Le { field: p, value: v } => op(&p.0, "$lte", v)?,
        Filter::Gt { field: p, value: v } => op(&p.0, "$gt", v)?,
        Filter::Ge { field: p, value: v } => op(&p.0, "$gte", v)?,
        Filter::In { field: p, values } => {
            let items = values
                .iter()
                .map(value)
                .collect::<ConnectorResult<Vec<_>>>()?;
            doc! { field(&p.0)?: { "$in": items } }
        }
        Filter::IsNull { field: p } => doc! { field(&p.0)?: { "$type": "null" } },
        // Present and not null.
        Filter::IsNotNull { field: p } => {
            doc! { field(&p.0)?: { "$exists": true, "$not": { "$type": "null" } } }
        }
        Filter::IsMissing { field: p } => doc! { field(&p.0)?: { "$exists": false } },
        Filter::Contains { field: p, text } => {
            doc! { field(&p.0)?: { "$regex": regex_escape(text) } }
        }
        Filter::StartsWith { field: p, text } => {
            doc! { field(&p.0)?: { "$regex": format!("^{}", regex_escape(text)) } }
        }
        Filter::And { filters } if filters.is_empty() => Document::new(),
        Filter::Or { filters } if filters.is_empty() => doc! { "$expr": false },
        Filter::And { filters } => {
            let parts = filters
                .iter()
                .map(to_query)
                .collect::<ConnectorResult<Vec<_>>>()?;
            doc! { "$and": parts }
        }
        Filter::Or { filters } => {
            let parts = filters
                .iter()
                .map(to_query)
                .collect::<ConnectorResult<Vec<_>>>()?;
            doc! { "$or": parts }
        }
        Filter::Not { filter } => doc! { "$nor": [to_query(filter)?] },
    })
}

pub fn to_sort(keys: &[SortKey]) -> ConnectorResult<Document> {
    let mut d = Document::new();
    for k in keys {
        d.insert(
            field(&k.field.0)?,
            if k.direction == SortDirection::Asc {
                1
            } else {
                -1
            },
        );
    }
    Ok(d)
}

/// Combines filters with `$and`, skipping empty ones.
pub fn and(parts: Vec<Document>) -> Document {
    let parts: Vec<Document> = parts.into_iter().filter(|d| !d.is_empty()).collect();
    match parts.len() {
        0 => Document::new(),
        1 => parts.into_iter().next().unwrap_or_default(),
        _ => doc! { "$and": parts },
    }
}

#[cfg(test)]
mod tests {
    use gazabridge_plugin_sdk::filter::FieldPath;

    use super::*;

    #[test]
    fn null_and_missing_are_distinguished() {
        assert_eq!(
            to_query(&Filter::IsNull {
                field: FieldPath::new("a")
            })
            .unwrap(),
            doc! { "a": { "$type": "null" } }
        );
        assert_eq!(
            to_query(&Filter::IsMissing {
                field: FieldPath::new("a.b")
            })
            .unwrap(),
            doc! { "a.b": { "$exists": false } }
        );
        assert_eq!(
            to_query(&Filter::eq("a", Value::Null)).unwrap(),
            doc! { "a": { "$type": "null" } }
        );
    }

    #[test]
    fn text_matching_escapes_regex_metacharacters() {
        let q = to_query(&Filter::Contains {
            field: FieldPath::new("name"),
            text: "a.b*(c)".into(),
        })
        .unwrap();
        assert_eq!(q, doc! { "name": { "$regex": "a\\.b\\*\\(c\\)" } });
    }

    #[test]
    fn operator_injection_through_field_names_is_rejected() {
        assert!(to_query(&Filter::eq("$where", Value::String("1".into()))).is_err());
        assert_eq!(to_query(&Filter::and(vec![])).unwrap(), Document::new());
    }
}
