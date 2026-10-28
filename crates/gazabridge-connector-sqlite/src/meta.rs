//! Table metadata loaded from SQLite's catalogue pragmas.

use std::collections::HashMap;

use gazabridge_domain::CanonicalType;
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, ConstraintInfo, ConstraintKind, IndexInfo, NamespacePath, ObjectKind, ObjectRef,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use sqlx::{Row, SqlitePool};

use crate::error::map_sqlx;
use crate::sql::{ColumnTypes, quote_ident, schema_of};
use crate::types::canonical_for_declared;

/// Columns and keys of a table or view.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub kind: ObjectKind,
    pub columns: Vec<ColumnInfo>,
    /// Primary-key columns in key order.
    pub primary_key: Vec<String>,
    pub has_rowid: bool,
    pub definition: Option<String>,
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

pub async fn load(pool: &SqlitePool, object: &ObjectRef) -> ConnectorResult<TableMeta> {
    let schema = schema_of(object).to_owned();
    let master = format!(
        "SELECT type, sql FROM {}.sqlite_master WHERE name = ? AND type IN ('table', 'view')",
        quote_ident(&schema)?
    );
    let row = sqlx::query(&master)
        .bind(&object.name)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            ConnectorError::new(ErrorKind::NotFound, format!("{object} does not exist"))
        })?;
    let kind = match row.try_get::<String, _>("type").map_err(map_sqlx)?.as_str() {
        "view" => ObjectKind::View,
        _ => ObjectKind::Table,
    };
    let definition: Option<String> = row.try_get("sql").map_err(map_sqlx)?;

    let rows = sqlx::query(
        r#"SELECT cid, name, type, "notnull", dflt_value, pk, hidden FROM pragma_table_xinfo(?1, ?2) ORDER BY cid"#,
    )
    .bind(&object.name)
    .bind(&schema)
    .fetch_all(pool)
    .await
    .map_err(map_sqlx)?;

    let mut columns = Vec::with_capacity(rows.len());
    let mut pk: Vec<(i64, String)> = Vec::new();
    for r in &rows {
        let hidden: i64 = r.try_get("hidden").map_err(map_sqlx)?;
        // hidden=1 marks virtual-table hidden columns; skip them.
        if hidden == 1 {
            continue;
        }
        let name: String = r.try_get("name").map_err(map_sqlx)?;
        let decl: String = r
            .try_get::<Option<String>, _>("type")
            .map_err(map_sqlx)?
            .unwrap_or_default();
        let pk_pos: i64 = r.try_get("pk").map_err(map_sqlx)?;
        if pk_pos > 0 {
            pk.push((pk_pos, name.clone()));
        }
        let cid: i64 = r.try_get("cid").map_err(map_sqlx)?;
        columns.push(ColumnInfo {
            canonical_type: canonical_for_declared(&decl),
            native_type: decl,
            nullable: r.try_get::<i64, _>("notnull").map_err(map_sqlx)? == 0,
            default_expression: r.try_get("dflt_value").map_err(map_sqlx)?,
            is_generated: hidden == 2 || hidden == 3,
            is_identity: false,
            comment: None,
            ordinal: u32::try_from(cid + 1).unwrap_or(u32::MAX),
            name,
        });
    }
    pk.sort();
    let primary_key: Vec<String> = pk.into_iter().map(|(_, n)| n).collect();
    // An INTEGER PRIMARY KEY column aliases the rowid.
    if let [only] = primary_key.as_slice()
        && let Some(c) = columns.iter_mut().find(|c| &c.name == only)
        && c.native_type.eq_ignore_ascii_case("INTEGER")
    {
        c.is_identity = true;
    }

    let has_rowid = kind == ObjectKind::Table && {
        let probe = format!(
            "SELECT rowid FROM {}.{} LIMIT 0",
            quote_ident(&schema)?,
            quote_ident(&object.name)?
        );
        sqlx::query(&probe).fetch_optional(pool).await.is_ok()
    };

    Ok(TableMeta {
        kind,
        columns,
        primary_key,
        has_rowid,
        definition,
    })
}

pub async fn indexes(pool: &SqlitePool, object: &ObjectRef) -> ConnectorResult<Vec<IndexInfo>> {
    let schema = schema_of(object);
    let list =
        sqlx::query(r#"SELECT name, "unique", origin, partial FROM pragma_index_list(?1, ?2)"#)
            .bind(&object.name)
            .bind(schema)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx)?;
    let mut out = Vec::with_capacity(list.len());
    for r in list {
        let name: String = r.try_get("name").map_err(map_sqlx)?;
        let cols = sqlx::query("SELECT name FROM pragma_index_info(?1, ?2) ORDER BY seqno")
            .bind(&name)
            .bind(schema)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx)?
            .iter()
            .map(|c| {
                c.try_get::<Option<String>, _>("name")
                    .map(|n| n.unwrap_or_else(|| "<expression>".into()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlx)?;
        let definition: Option<String> = sqlx::query_scalar(&format!(
            "SELECT sql FROM {}.sqlite_master WHERE type = 'index' AND name = ?",
            quote_ident(schema)?
        ))
        .bind(&name)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx)?
        .flatten();
        let origin: String = r.try_get("origin").map_err(map_sqlx)?;
        out.push(IndexInfo {
            name,
            columns: cols,
            unique: r.try_get::<i64, _>("unique").map_err(map_sqlx)? == 1,
            primary: origin == "pk",
            method: Some("btree".into()),
            predicate: (r.try_get::<i64, _>("partial").map_err(map_sqlx)? == 1)
                .then(|| "partial".into()),
            definition,
            native: None,
        });
    }
    Ok(out)
}

pub async fn foreign_keys(
    pool: &SqlitePool,
    object: &ObjectRef,
) -> ConnectorResult<Vec<ConstraintInfo>> {
    let schema = schema_of(object);
    let rows = sqlx::query(
        r#"SELECT id, seq, "table", "from", "to", on_update, on_delete FROM pragma_foreign_key_list(?1, ?2) ORDER BY id, seq"#,
    )
    .bind(&object.name)
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(map_sqlx)?;
    let mut grouped: HashMap<i64, ConstraintInfo> = HashMap::new();
    let mut order = Vec::new();
    for r in rows {
        let id: i64 = r.try_get("id").map_err(map_sqlx)?;
        let from: String = r.try_get("from").map_err(map_sqlx)?;
        let to: Option<String> = r.try_get("to").map_err(map_sqlx)?;
        let table: String = r.try_get("table").map_err(map_sqlx)?;
        let entry = grouped.entry(id).or_insert_with(|| {
            order.push(id);
            ConstraintInfo {
                name: None,
                columns: Vec::new(),
                kind: ConstraintKind::ForeignKey {
                    referenced: ObjectRef::table(
                        NamespacePath::schema(None, schema),
                        table.clone(),
                    ),
                    referenced_columns: Vec::new(),
                    on_delete: r.try_get("on_delete").ok(),
                    on_update: r.try_get("on_update").ok(),
                },
            }
        });
        entry.columns.push(from);
        if let ConstraintKind::ForeignKey {
            referenced_columns, ..
        } = &mut entry.kind
        {
            // NULL `to` means "the referenced table's primary key".
            referenced_columns.push(to.unwrap_or_default());
        }
    }
    Ok(order
        .into_iter()
        .filter_map(|id| grouped.remove(&id))
        .collect())
}
