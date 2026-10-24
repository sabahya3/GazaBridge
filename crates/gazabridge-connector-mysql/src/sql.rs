//! SQL rendering for MySQL. Values are always `?` parameters.

use std::collections::HashMap;

use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, DdlPlan, SchemaOperation, TableDefinition};
use gazabridge_plugin_sdk::filter::{Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult};
use mysql_async::Value as My;

use crate::types::{encode, native_type_for};

/// MySQL identifiers are limited to 64 characters.
pub const MAX_IDENTIFIER_CHARS: usize = 64;

pub fn quote_ident(ident: &str) -> ConnectorResult<String> {
    if ident.is_empty() {
        return Err(ConnectorError::invalid_input("identifier must not be empty"));
    }
    if ident.contains('\0') {
        return Err(ConnectorError::invalid_input("identifier must not contain NUL"));
    }
    if ident.chars().count() > MAX_IDENTIFIER_CHARS {
        return Err(ConnectorError::invalid_input(format!(
            "identifier is longer than MySQL's limit of {MAX_IDENTIFIER_CHARS} characters"
        )));
    }
    if ident.ends_with(' ') {
        return Err(ConnectorError::invalid_input("MySQL identifiers cannot end with a space"));
    }
    Ok(format!("`{}`", ident.replace('`', "``")))
}

/// The MySQL database (schema) of a namespace: `database`, else `schema`.
pub fn database_of(ns: &NamespacePath) -> Option<&str> {
    ns.database.as_deref().or(ns.schema.as_deref())
}

pub fn qualified(object: &ObjectRef) -> ConnectorResult<String> {
    match database_of(&object.namespace) {
        Some(db) => Ok(format!("{}.{}", quote_ident(db)?, quote_ident(&object.name)?)),
        None => quote_ident(&object.name),
    }
}

pub type ColumnTypes = HashMap<String, CanonicalType>;

#[derive(Debug, Default, Clone)]
pub struct Params {
    pub values: Vec<My>,
}

impl Params {
    pub fn push(&mut self, v: My) -> &'static str {
        self.values.push(v);
        "?"
    }
}

fn column<'a>(types: &'a ColumnTypes, field: &str) -> ConnectorResult<(&'a CanonicalType, String)> {
    if field.contains('.') {
        return Err(ConnectorError::invalid_input("nested field paths are not supported on relational tables"));
    }
    let t = types
        .get(field)
        .ok_or_else(|| ConnectorError::invalid_input(format!("unknown column `{field}`")))?;
    Ok((t, quote_ident(field)?))
}

pub fn filter_sql(filter: &Filter, types: &ColumnTypes, out: &mut Params) -> ConnectorResult<String> {
    let cmp = |field: &str, op: &str, value: &Value, out: &mut Params| -> ConnectorResult<String> {
        let (t, col) = column(types, field)?;
        let p = out.push(encode(value, Some(t))?);
        Ok(format!("{col} {op} {p}"))
    };
    Ok(match filter {
        Filter::Eq { field, value } if value.is_absent() => format!("{} IS NULL", column(types, &field.0)?.1),
        Filter::Eq { field, value } => cmp(&field.0, "=", value, out)?,
        Filter::Ne { field, value } if value.is_absent() => format!("{} IS NOT NULL", column(types, &field.0)?.1),
        Filter::Ne { field, value } => {
            let (t, col) = column(types, &field.0)?;
            let p = out.push(encode(value, Some(t))?);
            format!("NOT ({col} <=> {p})")
        }
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
                    .map(|v| Ok(out.push(encode(v, Some(t))?)))
                    .collect::<ConnectorResult<Vec<_>>>()?;
                format!("{col} IN ({})", marks.join(", "))
            }
        }
        Filter::IsNull { field } => format!("{} IS NULL", column(types, &field.0)?.1),
        Filter::IsNotNull { field } => format!("{} IS NOT NULL", column(types, &field.0)?.1),
        Filter::IsMissing { .. } => {
            return Err(ConnectorError::invalid_input("relational columns cannot be missing; use IsNull"));
        }
        Filter::Contains { field, text } => {
            let (_, col) = column(types, &field.0)?;
            // BINARY makes the comparison case-sensitive regardless of collation.
            let p = out.push(My::Bytes(text.clone().into_bytes()));
            format!("LOCATE(BINARY {p}, BINARY {col}) > 0")
        }
        Filter::StartsWith { field, text } => {
            let (_, col) = column(types, &field.0)?;
            out.push(My::Bytes(text.clone().into_bytes()));
            out.push(My::Bytes(text.clone().into_bytes()));
            format!("LEFT(BINARY {col}, CHAR_LENGTH(?)) = BINARY ?")
        }
        Filter::And { filters } | Filter::Or { filters } => {
            let and = matches!(filter, Filter::And { .. });
            if filters.is_empty() {
                return Ok(if and { "TRUE" } else { "FALSE" }.into());
            }
            let parts = filters.iter().map(|f| filter_sql(f, types, out)).collect::<ConnectorResult<Vec<_>>>()?;
            format!("({})", parts.join(if and { " AND " } else { " OR " }))
        }
        Filter::Not { filter } => format!("NOT ({})", filter_sql(filter, types, out)?),
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
            Ok(format!("{col} {}", if k.direction == SortDirection::Asc { "ASC" } else { "DESC" }))
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn column_def(c: &ColumnDefinition) -> ConnectorResult<String> {
    let (ty, marker) = match &c.native_type {
        Some(t) => (t.clone(), None),
        None => native_type_for(&c.canonical_type)?,
    };
    let mut s = format!("{} {ty}", quote_ident(&c.name)?);
    s.push_str(if c.nullable { " NULL" } else { " NOT NULL" });
    if let Some(d) = &c.default_expression {
        s.push_str(&format!(" DEFAULT ({d})"));
    }
    if let Some(m) = marker {
        s.push_str(&format!(" COMMENT {}", sql_string(m)));
    }
    Ok(s)
}

fn idents(names: &[String]) -> ConnectorResult<String> {
    Ok(names.iter().map(|c| quote_ident(c)).collect::<ConnectorResult<Vec<_>>>()?.join(", "))
}

fn create_table(t: &TableDefinition, if_not_exists: bool) -> ConnectorResult<String> {
    if t.columns.is_empty() {
        return Err(ConnectorError::invalid_input("a table needs at least one column"));
    }
    let mut parts = t.columns.iter().map(column_def).collect::<ConnectorResult<Vec<_>>>()?;
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
        "CREATE TABLE {}{} (\n  {}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        qualified(&t.object)?,
        parts.join(",\n  ")
    ))
}

/// Renders statements without touching the server.
///
/// MySQL DDL is not transactional: each statement commits on its own, so a
/// failure part-way leaves earlier statements applied (reported in warnings).
pub fn plan(ops: &[SchemaOperation]) -> ConnectorResult<DdlPlan> {
    let mut statements = Vec::new();
    let mut destructive = false;
    let mut warnings = Vec::new();
    for op in ops {
        destructive |= op.is_destructive();
        match op {
            SchemaOperation::CreateNamespace { namespace, if_not_exists } => {
                let db = database_of(namespace).ok_or_else(|| ConnectorError::invalid_input("database name required"))?;
                statements.push(format!(
                    "CREATE DATABASE {}{} DEFAULT CHARACTER SET utf8mb4",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    quote_ident(db)?
                ));
            }
            SchemaOperation::CreateTable { table, if_not_exists } => statements.push(create_table(table, *if_not_exists)?),
            SchemaOperation::CreateIndex { object, index, if_not_exists } => {
                if *if_not_exists {
                    warnings.push("MySQL has no CREATE INDEX IF NOT EXISTS; an existing index makes this fail".into());
                }
                statements.push(format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    if index.unique { "UNIQUE " } else { "" },
                    quote_ident(&index.name)?,
                    qualified(object)?,
                    idents(&index.columns)?
                ));
            }
            SchemaOperation::DropObject { object, if_exists } => {
                let exists = if *if_exists { "IF EXISTS " } else { "" };
                statements.push(match object.kind {
                    ObjectKind::Table | ObjectKind::Partition => format!("DROP TABLE {exists}{}", qualified(object)?),
                    ObjectKind::View => format!("DROP VIEW {exists}{}", qualified(object)?),
                    ObjectKind::Trigger => format!("DROP TRIGGER {exists}{}", qualified(object)?),
                    ObjectKind::Procedure => format!("DROP PROCEDURE {exists}{}", qualified(object)?),
                    ObjectKind::Function => format!("DROP FUNCTION {exists}{}", qualified(object)?),
                    ObjectKind::Database | ObjectKind::Schema => format!("DROP DATABASE {exists}{}", quote_ident(&object.name)?),
                    other => {
                        return Err(ConnectorError::invalid_input(format!(
                            "dropping objects of kind {other:?} is not supported"
                        )));
                    }
                });
            }
            SchemaOperation::TruncateObject { object } => statements.push(format!("TRUNCATE TABLE {}", qualified(object)?)),
            SchemaOperation::CreateCollection { .. } => return Err(ConnectorError::unsupported(Capability::CreateCollection)),
        }
    }
    if statements.len() > 1 {
        warnings.push("MySQL DDL commits each statement; a failure part-way leaves earlier statements applied".into());
    }
    Ok(DdlPlan { statements, destructive, warnings })
}

/// Classifies a statement for read-only connections. Only statements that
/// cannot modify data are allowed; DDL in MySQL commits implicitly and would
/// escape a read-only transaction.
pub fn is_read_only_statement(sql: &str) -> bool {
    let mut s = sql.trim_start();
    // Strip leading comments.
    loop {
        if let Some(rest) = s.strip_prefix("--").or_else(|| s.strip_prefix('#')) {
            s = rest.split_once('\n').map_or("", |(_, r)| r).trim_start();
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = rest.split_once("*/").map_or("", |(_, r)| r).trim_start();
        } else {
            break;
        }
    }
    let upper = s.to_ascii_uppercase();
    let first = upper.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    let words: Vec<&str> = upper.split(|c: char| !c.is_ascii_alphanumeric() && c != '_').filter(|w| !w.is_empty()).collect();
    let has = |w: &str| words.contains(&w);
    if has("INTO") && (has("OUTFILE") || has("DUMPFILE")) {
        return false;
    }
    // One statement only (conservatively also rejects ';' inside literals).
    if upper.trim_end().trim_end_matches(';').contains(';') {
        return false;
    }
    match first {
        "SELECT" | "SHOW" | "DESCRIBE" | "DESC" | "TABLE" | "VALUES" => true,
        "EXPLAIN" => !has("ANALYZE"),
        "WITH" => !(has("UPDATE") || has("DELETE") || has("INSERT") || has("REPLACE")),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_rules() {
        assert_eq!(quote_ident("we`ird").unwrap(), "`we``ird`");
        assert!(quote_ident(&"ع".repeat(64)).is_ok());
        assert!(quote_ident(&"a".repeat(65)).is_err());
        assert!(quote_ident("trailing ").is_err());
        assert!(quote_ident("").is_err());
    }

    #[test]
    fn read_only_classification() {
        assert!(is_read_only_statement("SELECT * FROM t"));
        assert!(is_read_only_statement("  -- note\n/* c */ show tables"));
        assert!(!is_read_only_statement("DELETE FROM t"));
        assert!(!is_read_only_statement("DROP TABLE t"));
        assert!(!is_read_only_statement("SELECT * FROM t INTO OUTFILE '/tmp/x'"));
        assert!(!is_read_only_statement("WITH x AS (SELECT 1) DELETE FROM t"));
        assert!(!is_read_only_statement("SET SESSION TRANSACTION READ WRITE"));
        assert!(!is_read_only_statement("SELECT 1; DROP TABLE t"));
        assert!(!is_read_only_statement("EXPLAIN ANALYZE SELECT 1"));
    }

    #[test]
    fn utc_columns_carry_a_marker_comment() {
        let obj = ObjectRef::table(NamespacePath::database("app"), "t");
        let p = plan(&[SchemaOperation::CreateTable {
            table: TableDefinition {
                object: obj,
                columns: vec![ColumnDefinition {
                    name: "at".into(),
                    canonical_type: CanonicalType::UtcDateTime { fractional_digits: 3 },
                    native_type: None,
                    nullable: true,
                    default_expression: None,
                }],
                primary_key: vec![],
                foreign_keys: vec![],
            },
            if_not_exists: false,
        }])
        .unwrap();
        assert!(p.statements[0].contains("`at` datetime(3) NULL COMMENT 'gazabridge:utc_datetime'"), "{}", p.statements[0]);
        assert!(p.statements[0].starts_with("CREATE TABLE `app`.`t`"));
    }
}
