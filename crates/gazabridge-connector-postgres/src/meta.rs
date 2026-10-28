//! Catalog introspection (`pg_catalog`), supported from PostgreSQL 12.

use gazabridge_domain::CanonicalType;
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, ConstraintInfo, ConstraintKind, IndexInfo, NamespacePath, ObjectKind, ObjectRef,
};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use tokio_postgres::Client;

use crate::error::map_pg;
use crate::sql::{ColumnTypes, quote_ident};
use crate::types::{PgType, TextParam, as_params, canonical_for, needs_text_cast};

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub kind: ObjectKind,
    pub schema: String,
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

    /// SELECT expression for a column: extension types are read as text.
    pub fn select_expr(&self, name: &str) -> ConnectorResult<String> {
        let q = quote_ident(name)?;
        Ok(match self.column_type(name) {
            Some(t) if needs_text_cast(t) => format!("{q}::text"),
            _ => q,
        })
    }
}

fn schema_param(namespace: &NamespacePath) -> TextParam {
    TextParam(namespace.schema.clone())
}

const COLUMNS_SQL: &str = r#"
SELECT a.attname::text AS name,
       format_type(a.atttypid, a.atttypmod) AS native,
       a.attnotnull AS not_null,
       pg_get_expr(d.adbin, d.adrelid) AS default_expr,
       a.attidentity::text AS identity,
       a.attgenerated::text AS generated,
       col_description(a.attrelid, a.attnum) AS comment,
       a.attnum AS ordinal,
       t.typname::text AS typname, tn.nspname::text AS typschema, t.typtype::text AS typtype, a.atttypmod AS typmod,
       et.typname::text AS elem_name, et.typtype::text AS elem_typtype, et.oid::int8 AS elem_oid,
       bt.typname::text AS base_name, t.typtypmod AS base_typmod,
       t.oid::int8 AS type_oid
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_type t ON t.oid = a.atttypid
JOIN pg_namespace tn ON tn.oid = t.typnamespace
LEFT JOIN pg_type et ON et.oid = t.typelem AND t.typcategory = 'A'
LEFT JOIN pg_type bt ON bt.oid = t.typbasetype AND t.typtype = 'd'
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = COALESCE($1::text, current_schema()) AND c.relname = $2::text
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"#;

async fn enum_labels(client: &Client, type_oid: i64) -> ConnectorResult<Vec<String>> {
    let rows = client
        .query(
            "SELECT enumlabel::text FROM pg_enum WHERE enumtypid = $1::text::oid ORDER BY enumsortorder",
            &as_params(&[TextParam(Some(type_oid.to_string()))]),
        )
        .await
        .map_err(map_pg)?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

pub async fn load(client: &Client, object: &ObjectRef) -> ConnectorResult<TableMeta> {
    let params = [
        schema_param(&object.namespace),
        TextParam(Some(object.name.clone())),
    ];
    let rel = client
        .query_opt(
            r#"SELECT c.relkind::text, n.nspname::text, c.reltuples::float8, obj_description(c.oid, 'pg_class'), c.relispartition
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = COALESCE($1::text, current_schema()) AND c.relname = $2::text
                 AND c.relkind IN ('r', 'p', 'v', 'm', 'f')"#,
            &as_params(&params),
        )
        .await
        .map_err(map_pg)?
        .ok_or_else(|| ConnectorError::new(ErrorKind::NotFound, format!("{object} does not exist")))?;
    let relkind: String = rel.get(0);
    let partition: bool = rel.get(4);
    let kind = match relkind.as_str() {
        "v" => ObjectKind::View,
        "m" => ObjectKind::MaterializedView,
        _ if partition => ObjectKind::Partition,
        _ => ObjectKind::Table,
    };
    let reltuples: f64 = rel.get(2);

    let rows = client
        .query(COLUMNS_SQL, &as_params(&params))
        .await
        .map_err(map_pg)?;
    let mut columns = Vec::with_capacity(rows.len());
    for r in &rows {
        let typtype: String = r.get("typtype");
        let mut pg = PgType {
            name: r.get("typname"),
            schema: r.get("typschema"),
            typtype: typtype.clone(),
            typmod: r.get("typmod"),
            ..Default::default()
        };
        if let Some(elem_name) = r.get::<_, Option<String>>("elem_name") {
            let elem_typtype: String = r
                .get::<_, Option<String>>("elem_typtype")
                .unwrap_or_default();
            let mut elem = PgType {
                name: elem_name,
                typtype: elem_typtype.clone(),
                typmod: -1,
                ..Default::default()
            };
            if elem_typtype == "e"
                && let Some(oid) = r.get::<_, Option<i64>>("elem_oid")
            {
                elem.enum_labels = enum_labels(client, oid).await?;
            }
            pg.element = Some(Box::new(elem));
        }
        if let Some(base) = r.get::<_, Option<String>>("base_name") {
            pg.base = Some(Box::new(PgType {
                name: base,
                typtype: "b".into(),
                typmod: r.get("base_typmod"),
                ..Default::default()
            }));
        }
        if typtype == "e" {
            pg.enum_labels = enum_labels(client, r.get("type_oid")).await?;
        }
        let identity: String = r.get("identity");
        let generated: String = r.get("generated");
        let ordinal: i16 = r.get("ordinal");
        columns.push(ColumnInfo {
            name: r.get("name"),
            native_type: r.get("native"),
            canonical_type: canonical_for(&pg),
            nullable: !r.get::<_, bool>("not_null"),
            default_expression: r.get("default_expr"),
            is_generated: !generated.is_empty(),
            is_identity: !identity.is_empty(),
            comment: r.get("comment"),
            ordinal: ordinal.max(0) as u32,
        });
    }

    let pk_rows = client
        .query(
            r#"SELECT a.attname::text
               FROM pg_index i
               JOIN pg_class c ON c.oid = i.indrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, pos) ON TRUE
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
               WHERE i.indisprimary AND n.nspname = COALESCE($1::text, current_schema()) AND c.relname = $2::text
               ORDER BY k.pos"#,
            &as_params(&params),
        )
        .await
        .map_err(map_pg)?;

    Ok(TableMeta {
        kind,
        schema: rel.get(1),
        columns,
        primary_key: pk_rows.iter().map(|r| r.get(0)).collect(),
        estimated_rows: (reltuples >= 0.0).then_some(reltuples as u64),
        comment: rel.get(3),
    })
}

pub async fn indexes(client: &Client, object: &ObjectRef) -> ConnectorResult<Vec<IndexInfo>> {
    let rows = client
        .query(
            r#"SELECT ic.relname::text, i.indisunique, i.indisprimary, am.amname::text,
                      pg_get_expr(i.indpred, i.indrelid), pg_get_indexdef(i.indexrelid),
                      ARRAY(SELECT COALESCE(a.attname::text, '<expression>')
                            FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, pos)
                            LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                            ORDER BY k.pos)
               FROM pg_index i
               JOIN pg_class c ON c.oid = i.indrelid
               JOIN pg_class ic ON ic.oid = i.indexrelid
               JOIN pg_am am ON am.oid = ic.relam
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = COALESCE($1::text, current_schema()) AND c.relname = $2::text
               ORDER BY ic.relname"#,
            &as_params(&[schema_param(&object.namespace), TextParam(Some(object.name.clone()))]),
        )
        .await
        .map_err(map_pg)?;
    Ok(rows
        .iter()
        .map(|r| IndexInfo {
            name: r.get(0),
            unique: r.get(1),
            primary: r.get(2),
            method: r.get(3),
            predicate: r.get(4),
            definition: r.get(5),
            native: None,
            columns: r.get(6),
        })
        .collect())
}

pub async fn constraints(
    client: &Client,
    object: &ObjectRef,
    schema: &str,
) -> ConnectorResult<Vec<ConstraintInfo>> {
    let rows = client
        .query(
            r#"SELECT con.conname::text, con.contype::text, pg_get_constraintdef(con.oid),
                      ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY k(n, p)
                            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.n ORDER BY k.p),
                      rn.nspname::text, rc.relname::text,
                      ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY k(n, p)
                            JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.n ORDER BY k.p),
                      con.confdeltype::text, con.confupdtype::text
               FROM pg_constraint con
               JOIN pg_class c ON c.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
               LEFT JOIN pg_class rc ON rc.oid = con.confrelid
               LEFT JOIN pg_namespace rn ON rn.oid = rc.relnamespace
               WHERE n.nspname = $1::text AND c.relname = $2::text
               ORDER BY con.conname"#,
            &as_params(&[TextParam(Some(schema.to_owned())), TextParam(Some(object.name.clone()))]),
        )
        .await
        .map_err(map_pg)?;
    let action = |c: &str| -> Option<String> {
        Some(
            match c {
                "a" => "NO ACTION",
                "r" => "RESTRICT",
                "c" => "CASCADE",
                "n" => "SET NULL",
                "d" => "SET DEFAULT",
                _ => return None,
            }
            .to_owned(),
        )
    };
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let contype: String = r.get(1);
        let definition: String = r.get(2);
        let kind = match contype.as_str() {
            "p" => ConstraintKind::PrimaryKey,
            "u" => ConstraintKind::Unique,
            "c" => ConstraintKind::Check {
                expression: definition,
            },
            "x" => ConstraintKind::Exclusion { definition },
            "f" => ConstraintKind::ForeignKey {
                referenced: ObjectRef::table(
                    NamespacePath::schema(None, r.get::<_, Option<String>>(4).unwrap_or_default()),
                    r.get::<_, Option<String>>(5).unwrap_or_default(),
                ),
                referenced_columns: r.get(6),
                on_delete: action(&r.get::<_, String>(7)),
                on_update: action(&r.get::<_, String>(8)),
            },
            _ => continue,
        };
        out.push(ConstraintInfo {
            name: r.get(0),
            columns: r.get(3),
            kind,
        });
    }
    Ok(out)
}
