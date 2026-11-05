//! Catalog introspection through `information_schema`.

use std::collections::BTreeMap;

use gazabridge_domain::CanonicalType;
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, ConstraintInfo, ConstraintKind, IndexInfo, NamespacePath, ObjectKind, ObjectRef,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Row, Value as My};

use crate::error::map_mysql;
use crate::sql::{ColumnTypes, database_of};
use crate::types::{MyType, canonical_for};

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub kind: ObjectKind,
    pub database: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    pub estimated_rows: Option<u64>,
    pub comment: Option<String>,
}

impl TableMeta {
    pub fn types(&self) -> ColumnTypes {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.canonical_type.clone()))
            .collect()
    }

    pub fn column_type(&self, name: &str) -> Option<&CanonicalType> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.canonical_type)
    }
}

pub fn cell_str(row: &Row, i: usize) -> Option<String> {
    match row.as_ref(i)? {
        My::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        My::Int(n) => Some(n.to_string()),
        My::UInt(n) => Some(n.to_string()),
        _ => None,
    }
}

pub fn cell_u64(row: &Row, i: usize) -> Option<u64> {
    match row.as_ref(i)? {
        My::Int(n) => u64::try_from(*n).ok(),
        My::UInt(n) => Some(*n),
        My::Bytes(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

async fn rows(conn: &mut Conn, sql: &str, params: Vec<My>) -> ConnectorResult<Vec<Row>> {
    conn.exec::<Row, _, _>(sql, Params::Positional(params))
        .await
        .map_err(map_mysql)
}

pub async fn current_database(conn: &mut Conn) -> ConnectorResult<Option<String>> {
    let r = rows(conn, "SELECT DATABASE()", vec![]).await?;
    Ok(r.first().and_then(|row| cell_str(row, 0)))
}

async fn resolve_db(conn: &mut Conn, ns: &NamespacePath) -> ConnectorResult<String> {
    match database_of(ns) {
        Some(db) => Ok(db.to_owned()),
        None => current_database(conn).await?.ok_or_else(|| {
            ConnectorError::invalid_input(
                "no database selected; qualify the object with a database",
            )
        }),
    }
}

pub async fn load(conn: &mut Conn, object: &ObjectRef) -> ConnectorResult<TableMeta> {
    let db = resolve_db(conn, &object.namespace).await?;
    let p = || {
        vec![
            My::Bytes(db.clone().into_bytes()),
            My::Bytes(object.name.clone().into_bytes()),
        ]
    };
    let t = rows(
        conn,
        "SELECT TABLE_TYPE, TABLE_ROWS, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        p(),
    )
    .await?;
    let t = t.first().ok_or_else(|| {
        ConnectorError::new(ErrorKind::NotFound, format!("{object} does not exist"))
    })?;
    let kind = if cell_str(t, 0).as_deref() == Some("VIEW") {
        ObjectKind::View
    } else {
        ObjectKind::Table
    };

    let cols = rows(
        conn,
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH,
                DATETIME_PRECISION, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, ORDINAL_POSITION
         FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        p(),
    )
    .await?;
    let mut columns = Vec::with_capacity(cols.len());
    for r in &cols {
        let my = MyType {
            data_type: cell_str(r, 1).unwrap_or_default().to_ascii_lowercase(),
            column_type: cell_str(r, 2).unwrap_or_default().to_ascii_lowercase(),
            precision: cell_u64(r, 3),
            scale: cell_u64(r, 4),
            char_length: cell_u64(r, 5),
            datetime_precision: cell_u64(r, 6),
            comment: cell_str(r, 10).unwrap_or_default(),
        };
        let extra = cell_str(r, 9).unwrap_or_default().to_ascii_lowercase();
        columns.push(ColumnInfo {
            name: cell_str(r, 0).unwrap_or_default(),
            canonical_type: canonical_for(&my),
            native_type: cell_str(r, 2).unwrap_or_default(),
            nullable: cell_str(r, 7).as_deref() == Some("YES"),
            default_expression: cell_str(r, 8),
            is_generated: extra.contains("generated") && !extra.contains("default_generated"),
            is_identity: extra.contains("auto_increment"),
            comment: Some(my.comment).filter(|c| !c.is_empty()),
            ordinal: cell_u64(r, 11).unwrap_or(0) as u32,
        });
    }
    let pk = rows(
        conn,
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY ORDINAL_POSITION",
        p(),
    )
    .await?;
    Ok(TableMeta {
        kind,
        database: db.clone(),
        columns,
        primary_key: pk.iter().filter_map(|r| cell_str(r, 0)).collect(),
        estimated_rows: cell_u64(t, 1),
        comment: cell_str(t, 2).filter(|c| !c.is_empty()),
    })
}

pub async fn indexes(conn: &mut Conn, db: &str, table: &str) -> ConnectorResult<Vec<IndexInfo>> {
    let r = rows(
        conn,
        "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, INDEX_TYPE FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        vec![
            My::Bytes(db.as_bytes().to_vec()),
            My::Bytes(table.as_bytes().to_vec()),
        ],
    )
    .await?;
    let mut by_name: BTreeMap<String, IndexInfo> = BTreeMap::new();
    for row in &r {
        let name = cell_str(row, 0).unwrap_or_default();
        let entry = by_name.entry(name.clone()).or_insert_with(|| IndexInfo {
            primary: name == "PRIMARY",
            unique: cell_u64(row, 1) == Some(0),
            method: cell_str(row, 3).map(|m| m.to_ascii_lowercase()),
            columns: Vec::new(),
            predicate: None,
            definition: None,
            native: None,
            name,
        });
        entry
            .columns
            .push(cell_str(row, 2).unwrap_or_else(|| "<expression>".into()));
    }
    Ok(by_name.into_values().collect())
}

pub async fn constraints(
    conn: &mut Conn,
    db: &str,
    table: &str,
) -> ConnectorResult<Vec<ConstraintInfo>> {
    let p = || {
        vec![
            My::Bytes(db.as_bytes().to_vec()),
            My::Bytes(table.as_bytes().to_vec()),
        ]
    };
    let mut out = Vec::new();
    let keys = rows(
        conn,
        "SELECT tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, k.COLUMN_NAME, k.REFERENCED_TABLE_SCHEMA,
                k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE
         FROM information_schema.TABLE_CONSTRAINTS tc
         JOIN information_schema.KEY_COLUMN_USAGE k
           ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
          AND k.TABLE_NAME = tc.TABLE_NAME
         LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
         ORDER BY tc.CONSTRAINT_NAME, k.ORDINAL_POSITION",
        p(),
    )
    .await?;
    let mut grouped: BTreeMap<String, ConstraintInfo> = BTreeMap::new();
    for r in &keys {
        let name = cell_str(r, 0).unwrap_or_default();
        let ctype = cell_str(r, 1).unwrap_or_default();
        let entry = grouped
            .entry(name.clone())
            .or_insert_with(|| ConstraintInfo {
                name: Some(name.clone()),
                columns: Vec::new(),
                kind: match ctype.as_str() {
                    "PRIMARY KEY" => ConstraintKind::PrimaryKey,
                    "FOREIGN KEY" => ConstraintKind::ForeignKey {
                        referenced: ObjectRef::table(
                            NamespacePath::database(cell_str(r, 3).unwrap_or_default()),
                            cell_str(r, 4).unwrap_or_default(),
                        ),
                        referenced_columns: Vec::new(),
                        on_delete: cell_str(r, 6),
                        on_update: cell_str(r, 7),
                    },
                    _ => ConstraintKind::Unique,
                },
            });
        entry.columns.push(cell_str(r, 2).unwrap_or_default());
        if let ConstraintKind::ForeignKey {
            referenced_columns, ..
        } = &mut entry.kind
            && let Some(c) = cell_str(r, 5)
        {
            referenced_columns.push(c);
        }
    }
    out.extend(grouped.into_values());
    // CHECK constraints (MySQL 8.0.16+, MariaDB 10.2+); older servers lack the view.
    if let Ok(checks) = rows(
        conn,
        "SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS cc
         JOIN information_schema.TABLE_CONSTRAINTS tc
           ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
         WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'CHECK'",
        p(),
    )
    .await
    {
        for r in &checks {
            out.push(ConstraintInfo {
                name: cell_str(r, 0),
                columns: Vec::new(),
                kind: ConstraintKind::Check {
                    expression: cell_str(r, 1).unwrap_or_default(),
                },
            });
        }
    }
    Ok(out)
}
