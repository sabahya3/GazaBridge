//! SQL rendering: identifier quoting, filters, ordering and DDL.
//!
//! Values are always bound as parameters; only quoted identifiers and
//! user-authored default expressions are interpolated.

use std::collections::HashMap;

use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, DdlPlan, SchemaOperation, TableDefinition};
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::schema::{ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult};

use crate::types::{Bind, encode, is_text_encoded_number, native_type_for};

pub const DEFAULT_SCHEMA: &str = "main";

/// Quotes an identifier with double quotes, doubling embedded quotes.
/// Empty identifiers and NUL characters are rejected.
pub fn quote_ident(ident: &str) -> ConnectorResult<String> {
    if ident.is_empty() {
        return Err(ConnectorError::invalid_input(
            "identifier must not be empty",
        ));
    }
    if ident.contains('\0') {
        return Err(ConnectorError::invalid_input(
            "identifier must not contain NUL",
        ));
    }
    Ok(format!("\"{}\"", ident.replace('"', "\"\"")))
}

pub fn schema_of(object: &ObjectRef) -> &str {
    object.namespace.schema.as_deref().unwrap_or(DEFAULT_SCHEMA)
}

/// `"schema"."name"`.
pub fn qualified(object: &ObjectRef) -> ConnectorResult<String> {
    Ok(format!(
        "{}.{}",
        quote_ident(schema_of(object))?,
        quote_ident(&object.name)?
    ))
}

/// SQL text plus bound parameters.
#[derive(Debug, Default, Clone)]
pub struct SqlParts {
    pub sql: String,
    pub binds: Vec<Bind>,
}

/// Column name → canonical type, used to validate and encode filters.
pub type ColumnTypes = HashMap<String, CanonicalType>;

fn column<'a>(types: &'a ColumnTypes, field: &str) -> ConnectorResult<(&'a CanonicalType, String)> {
    if field.contains('.') {
        return Err(ConnectorError::invalid_input(
            "nested field paths are not supported on relational tables",
        ));
    }
    let t = types
        .get(field)
        .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{field}`")))?;
    Ok((t, quote_ident(field)?))
}

fn range_guard(t: &CanonicalType, field: &str) -> ConnectorResult<()> {
    if is_text_encoded_number(t) {
        return Err(ConnectorError::invalid_input(format!(
            "range comparison on `{field}` is not supported: the column stores numbers as text"
        )));
    }
    Ok(())
}

/// Renders a filter into a boolean SQL expression.
pub fn filter_sql(filter: &Filter, types: &ColumnTypes, out: &mut SqlParts) -> ConnectorResult<()> {
    let cmp = |field: &str,
               op: &str,
               value: &Value,
               out: &mut SqlParts,
               range: bool|
     -> ConnectorResult<()> {
        let (t, col) = column(types, field)?;
        if range {
            range_guard(t, field)?;
        }
        out.sql.push_str(&format!("{col} {op} ?"));
        out.binds.push(encode(value, Some(t))?);
        Ok(())
    };
    match filter {
        Filter::Eq { field, value } if value.is_absent() => {
            let (_, col) = column(types, &field.0)?;
            out.sql.push_str(&format!("{col} IS NULL"));
        }
        Filter::Eq { field, value } => cmp(&field.0, "=", value, out, false)?,
        Filter::Ne { field, value } if value.is_absent() => {
            let (_, col) = column(types, &field.0)?;
            out.sql.push_str(&format!("{col} IS NOT NULL"));
        }
        Filter::Ne { field, value } => cmp(&field.0, "IS NOT", value, out, false)?,
        Filter::Lt { field, value } => cmp(&field.0, "<", value, out, true)?,
        Filter::Le { field, value } => cmp(&field.0, "<=", value, out, true)?,
        Filter::Gt { field, value } => cmp(&field.0, ">", value, out, true)?,
        Filter::Ge { field, value } => cmp(&field.0, ">=", value, out, true)?,
        Filter::In { field, values } => {
            let (t, col) = column(types, &field.0)?;
            if values.is_empty() {
                out.sql.push_str("0 = 1");
            } else {
                let marks = vec!["?"; values.len()].join(", ");
                out.sql.push_str(&format!("{col} IN ({marks})"));
                for v in values {
                    out.binds.push(encode(v, Some(t))?);
                }
            }
        }
        Filter::IsNull { field } => {
            let (_, col) = column(types, &field.0)?;
            out.sql.push_str(&format!("{col} IS NULL"));
        }
        Filter::IsNotNull { field } => {
            let (_, col) = column(types, &field.0)?;
            out.sql.push_str(&format!("{col} IS NOT NULL"));
        }
        Filter::IsMissing { .. } => {
            return Err(ConnectorError::invalid_input(
                "relational columns cannot be missing; use IsNull",
            ));
        }
        Filter::Contains { field, text } => {
            let (_, col) = column(types, &field.0)?;
            out.sql.push_str(&format!("instr({col}, ?) > 0"));
            out.binds.push(Bind::Text(text.clone()));
        }
        Filter::StartsWith { field, text } => {
            let (_, col) = column(types, &field.0)?;
            out.sql
                .push_str(&format!("substr({col}, 1, length(?)) = ?"));
            out.binds.push(Bind::Text(text.clone()));
            out.binds.push(Bind::Text(text.clone()));
        }
        Filter::And { filters } | Filter::Or { filters } => {
            let joiner = if matches!(filter, Filter::And { .. }) {
                " AND "
            } else {
                " OR "
            };
            if filters.is_empty() {
                out.sql.push_str(if matches!(filter, Filter::And { .. }) {
                    "1 = 1"
                } else {
                    "0 = 1"
                });
            } else {
                out.sql.push('(');
                for (i, f) in filters.iter().enumerate() {
                    if i > 0 {
                        out.sql.push_str(joiner);
                    }
                    filter_sql(f, types, out)?;
                }
                out.sql.push(')');
            }
        }
        Filter::Not { filter } => {
            out.sql.push_str("NOT (");
            filter_sql(filter, types, out)?;
            out.sql.push(')');
        }
    }
    Ok(())
}

pub fn order_sql(keys: &[SortKey], types: &ColumnTypes) -> ConnectorResult<String> {
    if keys.is_empty() {
        return Ok(String::new());
    }
    let parts = keys
        .iter()
        .map(|k| {
            let (_, col) = column(types, &k.field.0)?;
            Ok(format!(
                "{col} {}",
                match k.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                }
            ))
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

fn column_def(c: &ColumnDefinition) -> ConnectorResult<String> {
    let ty = match &c.native_type {
        Some(t) => t.clone(),
        None => native_type_for(&c.canonical_type)?,
    };
    let mut s = quote_ident(&c.name)?;
    if !ty.is_empty() {
        s.push(' ');
        s.push_str(&ty);
    }
    if !c.nullable {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = &c.default_expression {
        s.push_str(&format!(" DEFAULT ({d})"));
    }
    Ok(s)
}

fn create_table(t: &TableDefinition, if_not_exists: bool) -> ConnectorResult<String> {
    if t.columns.is_empty() {
        return Err(ConnectorError::invalid_input(
            "a table needs at least one column",
        ));
    }
    let mut parts = t
        .columns
        .iter()
        .map(column_def)
        .collect::<ConnectorResult<Vec<_>>>()?;
    if !t.primary_key.is_empty() {
        let cols = t
            .primary_key
            .iter()
            .map(|c| quote_ident(c))
            .collect::<ConnectorResult<Vec<_>>>()?;
        parts.push(format!("PRIMARY KEY ({})", cols.join(", ")));
    }
    for fk in &t.foreign_keys {
        let cols = fk
            .columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<ConnectorResult<Vec<_>>>()?;
        let refs = fk
            .referenced_columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<ConnectorResult<Vec<_>>>()?;
        let name = match &fk.name {
            Some(n) => format!("CONSTRAINT {} ", quote_ident(n)?),
            None => String::new(),
        };
        // SQLite foreign keys reference tables in the same schema.
        parts.push(format!(
            "{name}FOREIGN KEY ({}) REFERENCES {} ({})",
            cols.join(", "),
            quote_ident(&fk.referenced.name)?,
            refs.join(", ")
        ));
    }
    Ok(format!(
        "CREATE TABLE {}{} (\n  {}\n)",
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        qualified(&t.object)?,
        parts.join(",\n  ")
    ))
}

/// Renders the statements for `ops` without executing anything.
pub fn plan(ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
    let mut statements = Vec::new();
    let mut destructive = false;
    let mut warnings = Vec::new();
    for op in ops {
        destructive |= op.is_destructive();
        match op {
            SchemaOperation::CreateTable {
                table,
                if_not_exists,
            } => {
                statements.push(create_table(table, *if_not_exists)?);
            }
            SchemaOperation::CreateIndex {
                object,
                index,
                if_not_exists,
            } => {
                let cols = index
                    .columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<ConnectorResult<Vec<_>>>()?;
                statements.push(format!(
                    "CREATE {}INDEX {}{}.{} ON {} ({})",
                    if index.unique { "UNIQUE " } else { "" },
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    quote_ident(schema_of(object))?,
                    quote_ident(&index.name)?,
                    quote_ident(&object.name)?,
                    cols.join(", ")
                ));
            }
            SchemaOperation::DropObject { object, if_exists } => {
                let kind = match object.kind {
                    ObjectKind::Table | ObjectKind::Partition => "TABLE",
                    ObjectKind::View => "VIEW",
                    ObjectKind::Index => "INDEX",
                    ObjectKind::Trigger => "TRIGGER",
                    other => {
                        return Err(ConnectorError::invalid_input(format!(
                            "SQLite cannot drop objects of kind {other:?}"
                        )));
                    }
                };
                statements.push(format!(
                    "DROP {kind} {}{}",
                    if *if_exists { "IF EXISTS " } else { "" },
                    qualified(object)?
                ));
            }
            SchemaOperation::TruncateObject { object } => {
                statements.push(format!("DELETE FROM {}", qualified(object)?));
                warnings.push("SQLite has no TRUNCATE; all rows are deleted with DELETE".into());
            }
            SchemaOperation::CreateNamespace { .. } => {
                return Err(ConnectorError::unsupported(Capability::CreateSchema));
            }
            SchemaOperation::CreateCollection { .. } => {
                return Err(ConnectorError::unsupported(Capability::CreateCollection));
            }
        }
    }
    Ok(DdlPlan {
        statements,
        destructive,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use gazabridge_plugin_sdk::filter::FieldPath;
    use gazabridge_plugin_sdk::schema::NamespacePath;

    use super::*;

    fn types() -> ColumnTypes {
        [
            ("id".to_string(), CanonicalType::I64),
            ("name".to_string(), CanonicalType::TEXT),
            (
                "amount".to_string(),
                CanonicalType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
        ]
        .into()
    }

    #[test]
    fn quoting_doubles_quotes_and_rejects_bad_identifiers() {
        assert_eq!(quote_ident(r#"we"ird"#).unwrap(), r#""we""ird""#);
        assert_eq!(quote_ident("عملاء").unwrap(), "\"عملاء\"");
        assert!(quote_ident("").is_err());
        assert!(quote_ident("a\0b").is_err());
    }

    #[test]
    fn filters_bind_values_and_validate_columns() {
        let f = Filter::And {
            filters: vec![
                Filter::Ge {
                    field: FieldPath::new("id"),
                    value: Value::Int(5),
                },
                Filter::In {
                    field: FieldPath::new("name"),
                    values: vec![Value::String("a'; DROP TABLE x;--".into())],
                },
                Filter::Not {
                    filter: Box::new(Filter::IsNull {
                        field: FieldPath::new("name"),
                    }),
                },
            ],
        };
        let mut out = SqlParts::default();
        filter_sql(&f, &types(), &mut out).unwrap();
        assert_eq!(
            out.sql,
            r#"("id" >= ? AND "name" IN (?) AND NOT ("name" IS NULL))"#
        );
        assert_eq!(out.binds.len(), 2);

        let mut out = SqlParts::default();
        assert!(filter_sql(&Filter::eq("nope", Value::Int(1)), &types(), &mut out).is_err());
        let range_on_text_decimal = Filter::Gt {
            field: FieldPath::new("amount"),
            value: Value::Int(1),
        };
        assert!(filter_sql(&range_on_text_decimal, &types(), &mut SqlParts::default()).is_err());
    }

    #[test]
    fn drop_and_truncate_are_destructive() {
        let obj = ObjectRef::table(NamespacePath::schema(None, "main"), "t");
        let p = plan(&[SchemaOperation::TruncateObject {
            object: obj.clone(),
        }])
        .unwrap();
        assert!(p.destructive);
        assert_eq!(p.statements, vec![r#"DELETE FROM "main"."t""#.to_string()]);
        let p = plan(&[SchemaOperation::DropObject {
            object: obj,
            if_exists: true,
        }])
        .unwrap();
        assert_eq!(
            p.statements,
            vec![r#"DROP TABLE IF EXISTS "main"."t""#.to_string()]
        );
    }
}
