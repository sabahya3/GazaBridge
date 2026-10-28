//! SQL rendering for PostgreSQL. Values are always `$n` parameters.

use std::collections::HashMap;

use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, DdlPlan, SchemaOperation, TableDefinition};
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult, ErrorKind};

use crate::types::{ColumnShape, TextParam, encode, native_type_for, shape_of};

/// PostgreSQL's identifier limit (NAMEDATALEN - 1). Longer names would be
/// truncated silently by the server, so they are rejected.
pub const MAX_IDENTIFIER_BYTES: usize = 63;

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
    if ident.len() > MAX_IDENTIFIER_BYTES {
        return Err(ConnectorError::invalid_input(format!(
            "identifier is {} bytes; PostgreSQL allows at most {MAX_IDENTIFIER_BYTES}",
            ident.len()
        )));
    }
    Ok(format!("\"{}\"", ident.replace('"', "\"\"")))
}

fn qualify(namespace: &NamespacePath, name: &str) -> ConnectorResult<String> {
    match &namespace.schema {
        Some(s) => Ok(format!("{}.{}", quote_ident(s)?, quote_ident(name)?)),
        None => quote_ident(name),
    }
}

pub fn qualified(object: &ObjectRef) -> ConnectorResult<String> {
    qualify(&object.namespace, &object.name)
}

/// Numbered text parameters collected while rendering SQL.
#[derive(Debug, Default, Clone)]
pub struct SqlParts {
    pub params: Vec<TextParam>,
}

impl SqlParts {
    /// Adds a parameter and returns its placeholder (`$n`).
    pub fn param(&mut self, p: TextParam) -> String {
        self.params.push(p);
        format!("${}", self.params.len())
    }
}

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

fn encode_for(v: &Value, t: &CanonicalType) -> ConnectorResult<TextParam> {
    encode(v, shape_of(t))
}

pub fn filter_sql(
    filter: &Filter,
    types: &ColumnTypes,
    out: &mut SqlParts,
) -> ConnectorResult<String> {
    let cmp =
        |field: &str, op: &str, value: &Value, out: &mut SqlParts| -> ConnectorResult<String> {
            let (t, col) = column(types, field)?;
            let p = out.param(encode_for(value, t)?);
            Ok(format!("{col} {op} {p}"))
        };
    Ok(match filter {
        Filter::Eq { field, value } if value.is_absent() => {
            format!("{} IS NULL", column(types, &field.0)?.1)
        }
        Filter::Eq { field, value } => cmp(&field.0, "=", value, out)?,
        Filter::Ne { field, value } if value.is_absent() => {
            format!("{} IS NOT NULL", column(types, &field.0)?.1)
        }
        Filter::Ne { field, value } => cmp(&field.0, "IS DISTINCT FROM", value, out)?,
        Filter::Lt { field, value } => cmp(&field.0, "<", value, out)?,
        Filter::Le { field, value } => cmp(&field.0, "<=", value, out)?,
        Filter::Gt { field, value } => cmp(&field.0, ">", value, out)?,
        Filter::Ge { field, value } => cmp(&field.0, ">=", value, out)?,
        Filter::In { field, values } => {
            let (t, col) = column(types, &field.0)?;
            if values.is_empty() {
                "FALSE".into()
            } else {
                let marks = values
                    .iter()
                    .map(|v| Ok(out.param(encode_for(v, t)?)))
                    .collect::<ConnectorResult<Vec<_>>>()?;
                format!("{col} IN ({})", marks.join(", "))
            }
        }
        Filter::IsNull { field } => format!("{} IS NULL", column(types, &field.0)?.1),
        Filter::IsNotNull { field } => format!("{} IS NOT NULL", column(types, &field.0)?.1),
        Filter::IsMissing { .. } => {
            return Err(ConnectorError::invalid_input(
                "relational columns cannot be missing; use IsNull",
            ));
        }
        Filter::Contains { field, text } => {
            let (_, col) = column(types, &field.0)?;
            let p = out.param(TextParam(Some(text.clone())));
            format!("strpos({col}::text, {p}) > 0")
        }
        Filter::StartsWith { field, text } => {
            let (_, col) = column(types, &field.0)?;
            let p = out.param(TextParam(Some(text.clone())));
            format!("starts_with({col}::text, {p})")
        }
        Filter::And { filters } | Filter::Or { filters } => {
            let and = matches!(filter, Filter::And { .. });
            if filters.is_empty() {
                return Ok(if and { "TRUE" } else { "FALSE" }.into());
            }
            let parts = filters
                .iter()
                .map(|f| filter_sql(f, types, out))
                .collect::<ConnectorResult<Vec<_>>>()?;
            format!("({})", parts.join(if and { " AND " } else { " OR " }))
        }
        Filter::Not { filter } => format!("NOT ({})", filter_sql(filter, types, out)?),
        Filter::Native { .. } => {
            return Err(ConnectorError::new(
                ErrorKind::Unsupported,
                "native MongoDB filters cannot be used with PostgreSQL",
            ));
        }
    })
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
    let mut s = format!("{} {ty}", quote_ident(&c.name)?);
    if !c.nullable {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = &c.default_expression {
        s.push_str(&format!(" DEFAULT ({d})"));
    }
    Ok(s)
}

fn idents(names: &[String]) -> ConnectorResult<String> {
    Ok(names
        .iter()
        .map(|c| quote_ident(c))
        .collect::<ConnectorResult<Vec<_>>>()?
        .join(", "))
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
        parts.push(format!("PRIMARY KEY ({})", idents(&t.primary_key)?));
    }
    for fk in &t.foreign_keys {
        let name = match &fk.name {
            Some(n) => format!("CONSTRAINT {} ", quote_ident(n)?),
            None => String::new(),
        };
        parts.push(format!(
            "{name}FOREIGN KEY ({}) REFERENCES {} ({})",
            idents(&fk.columns)?,
            qualified(&fk.referenced)?,
            idents(&fk.referenced_columns)?
        ));
    }
    Ok(format!(
        "CREATE TABLE {}{} (\n  {}\n)",
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        qualified(&t.object)?,
        parts.join(",\n  ")
    ))
}

/// Renders statements for `ops` without touching the server.
pub fn plan(ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
    let mut statements = Vec::new();
    let mut destructive = false;
    let mut warnings = Vec::new();
    for op in ops {
        destructive |= op.is_destructive();
        match op {
            SchemaOperation::CreateNamespace {
                namespace,
                if_not_exists,
            } => {
                let schema = namespace
                    .schema
                    .as_deref()
                    .ok_or_else(|| ConnectorError::invalid_input("schema name required"))?;
                statements.push(format!(
                    "CREATE SCHEMA {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    quote_ident(schema)?
                ));
            }
            SchemaOperation::CreateTable {
                table,
                if_not_exists,
            } => statements.push(create_table(table, *if_not_exists)?),
            SchemaOperation::CreateIndex {
                object,
                index,
                if_not_exists,
            } => statements.push(format!(
                "CREATE {}INDEX {}{} ON {} ({})",
                if index.unique { "UNIQUE " } else { "" },
                if *if_not_exists { "IF NOT EXISTS " } else { "" },
                quote_ident(&index.name)?,
                qualified(object)?,
                idents(&index.columns)?
            )),
            SchemaOperation::DropObject { object, if_exists } => {
                let kind = match object.kind {
                    ObjectKind::Table | ObjectKind::Partition => "TABLE",
                    ObjectKind::View => "VIEW",
                    ObjectKind::MaterializedView => "MATERIALIZED VIEW",
                    ObjectKind::Index => "INDEX",
                    ObjectKind::Sequence => "SEQUENCE",
                    ObjectKind::Enum => "TYPE",
                    ObjectKind::Schema => {
                        statements.push(format!(
                            "DROP SCHEMA {}{}",
                            if *if_exists { "IF EXISTS " } else { "" },
                            quote_ident(&object.name)?
                        ));
                        continue;
                    }
                    other => {
                        return Err(ConnectorError::invalid_input(format!(
                            "dropping objects of kind {other:?} is not supported"
                        )));
                    }
                };
                statements.push(format!(
                    "DROP {kind} {}{}",
                    if *if_exists { "IF EXISTS " } else { "" },
                    qualified(object)?
                ));
                warnings.push(
                    "dependent objects make DROP fail (RESTRICT); nothing is cascaded".into(),
                );
            }
            SchemaOperation::TruncateObject { object } => {
                statements.push(format!("TRUNCATE TABLE {}", qualified(object)?));
            }
            SchemaOperation::CreateCollection { .. } | SchemaOperation::AlterCollection { .. } => {
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

/// Shape lookup used by the write path.
pub fn shape(types: &ColumnTypes, column: &str) -> ColumnShape {
    types.get(column).map_or(ColumnShape::Unknown, shape_of)
}

#[cfg(test)]
mod tests {
    use gazabridge_plugin_sdk::filter::FieldPath;

    use super::*;

    fn types() -> ColumnTypes {
        [
            ("id".to_string(), CanonicalType::I64),
            (
                "tags".to_string(),
                CanonicalType::Array(Box::new(CanonicalType::TEXT)),
            ),
        ]
        .into()
    }

    #[test]
    fn identifiers_over_63_bytes_are_rejected() {
        assert!(quote_ident(&"a".repeat(63)).is_ok());
        assert!(quote_ident(&"a".repeat(64)).is_err());
        // 32 Arabic letters = 64 bytes in UTF-8.
        assert!(quote_ident(&"ع".repeat(32)).is_err());
        assert_eq!(quote_ident(r#"x"y"#).unwrap(), r#""x""y""#);
    }

    #[test]
    fn filters_number_parameters() {
        let f = Filter::Or {
            filters: vec![
                Filter::In {
                    field: FieldPath::new("id"),
                    values: vec![Value::Int(1), Value::Int(2)],
                },
                Filter::Eq {
                    field: FieldPath::new("tags"),
                    value: Value::Array(vec![Value::String("a".into())]),
                },
            ],
        };
        let mut out = SqlParts::default();
        let sql = filter_sql(&f, &types(), &mut out).unwrap();
        assert_eq!(sql, r#"("id" IN ($1, $2) OR "tags" = $3)"#);
        assert_eq!(out.params[2], TextParam(Some(r#"{"a"}"#.into())));
        assert_eq!(
            filter_sql(&Filter::and(vec![]), &types(), &mut out).unwrap(),
            "TRUE"
        );
    }

    #[test]
    fn ddl_rendering() {
        let obj = ObjectRef::table(NamespacePath::schema(None, "public"), "t");
        let p = plan(&[SchemaOperation::TruncateObject {
            object: obj.clone(),
        }])
        .unwrap();
        assert!(p.destructive);
        assert_eq!(p.statements[0], r#"TRUNCATE TABLE "public"."t""#);
        let p = plan(&[SchemaOperation::CreateNamespace {
            namespace: NamespacePath::schema(None, "stage"),
            if_not_exists: true,
        }])
        .unwrap();
        assert_eq!(p.statements[0], r#"CREATE SCHEMA IF NOT EXISTS "stage""#);
        assert!(!p.destructive);
    }
}
