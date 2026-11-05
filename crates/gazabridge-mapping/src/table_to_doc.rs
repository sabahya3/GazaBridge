//! PostgreSQL rows → MongoDB documents.
//!
//! * **Reference**: one document per row. The primary key becomes `_id`
//!   (a document for composite keys); foreign keys stay as values.
//! * **Embed**: rows of a child table that references the parent's key are
//!   embedded as an array in the parent document (one level).
//!
//! Identity: `character(24)` hexadecimal values become ObjectIds again, so a
//! MongoDB → PostgreSQL → MongoDB round trip keeps `_id`s. JSONB holding
//! Extended JSON (`{"$oid": …}`) is read back with its types, and tables
//! made by the JSONB or hybrid modes give back the stored document.

use std::collections::{BTreeMap, BTreeSet};

use gazabridge_domain::value::{CanonicalValue, ValueMeta};
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::ddl::IndexDefinition;
use gazabridge_plugin_sdk::schema::{ConstraintKind, ObjectRef, ObjectSchema};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::doc_to_table::{DOC, HYBRID_DOC, ID};
use crate::from_ext_json;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableMode {
    #[default]
    Reference,
    Embed,
}

/// How a row's columns become document fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Columns {
    /// `character(24)`: hexadecimal text becomes an ObjectId.
    pub object_ids: BTreeSet<String>,
    /// JSON and JSONB: read as Extended JSON.
    pub json: BTreeSet<String>,
    /// 32-bit integers, kept as BSON `Int32`.
    pub int32: BTreeSet<String>,
    /// Date-times without a time zone (PostgreSQL `timestamp`, MySQL
    /// `DATETIME`): read as UTC and stored as BSON dates, since BSON has no
    /// local date-time. (Older stored mappings lack the field.)
    #[serde(default)]
    pub local_datetimes: BTreeSet<String>,
}

impl Columns {
    fn of(schema: &ObjectSchema) -> Self {
        let mut c = Self::default();
        for col in &schema.columns {
            let native = col.native_type.to_lowercase();
            if native == "character(24)" || native == "char(24)" || native == "bpchar(24)" {
                c.object_ids.insert(col.name.clone());
            }
            if matches!(
                col.canonical_type,
                CanonicalType::Json | CanonicalType::Object
            ) {
                c.json.insert(col.name.clone());
            }
            if matches!(col.canonical_type, CanonicalType::Int { bits } if bits <= 32) {
                c.int32.insert(col.name.clone());
            }
            if matches!(col.canonical_type, CanonicalType::LocalDateTime { .. }) {
                c.local_datetimes.insert(col.name.clone());
            }
        }
        c
    }

    fn value(&self, column: &str, v: &Value) -> Value {
        match v {
            Value::String(s) if self.object_ids.contains(column) => match hex24(s.trim_end()) {
                Some(b) => Value::ObjectId(b),
                None => v.clone(),
            },
            Value::Json(j) if self.json.contains(column) => from_ext_json(j),
            Value::LocalDateTime(dt) if self.local_datetimes.contains(column) => {
                Value::UtcDateTime(dt.and_utc())
            }
            other => other.clone(),
        }
    }
}

fn hex24(s: &str) -> Option<[u8; 12]> {
    if s.len() != 24 {
        return None;
    }
    let mut out = [0u8; 12];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedMap {
    pub child: ObjectRef,
    /// Array field in the parent document.
    pub field: String,
    /// Child column referencing the parent key (left out of embedded items).
    pub foreign_key: String,
    /// The parent column it references.
    pub parent_key: String,
    /// Child primary key, for a stable item order.
    pub order_by: Vec<String>,
    pub columns: Columns,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDocMapping {
    pub source: ObjectRef,
    pub target: ObjectRef,
    /// Primary key columns; one becomes `_id`, several an `_id` document.
    pub key: Vec<String>,
    pub columns: Columns,
    /// Column holding the whole document (tables made by the JSONB or
    /// hybrid modes).
    pub stored_document: Option<String>,
    pub embeds: Vec<EmbedMap>,
    pub warnings: Vec<String>,
}

/// Plans one table → collection. Fails for tables without a primary key:
/// their rows have no identity, so a copy could not resume safely.
pub fn plan(schema: &ObjectSchema, target: ObjectRef) -> Result<TableDocMapping, String> {
    if schema.primary_key.is_empty() {
        return Err(format!(
            "`{}` has no primary key; add one (or export it to a file) so the copy can resume without duplicates",
            schema.object
        ));
    }
    let columns = Columns::of(schema);
    let names: BTreeSet<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    let stored_document = if names.contains(HYBRID_DOC) && columns.json.contains(HYBRID_DOC) {
        Some(HYBRID_DOC.to_owned())
    } else if names == BTreeSet::from([ID, DOC]) && columns.json.contains(DOC) {
        Some(DOC.to_owned())
    } else {
        None
    };
    let mut warnings = Vec::new();
    if stored_document.is_some() {
        warnings.push(
            "the table holds whole documents (JSONB/hybrid copy): they are restored as stored"
                .into(),
        );
    }
    if !columns.local_datetimes.is_empty() {
        warnings.push(format!(
            "{} has no time zone: values are read as UTC and stored as dates",
            columns
                .local_datetimes
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(TableDocMapping {
        source: schema.object.clone(),
        target,
        key: schema.primary_key.clone(),
        columns,
        stored_document,
        embeds: Vec::new(),
        warnings,
    })
}

/// An embedding of `child` into `parent`, when the child has a single-column
/// foreign key to the parent's single-column primary key.
pub fn embed(parent: &ObjectSchema, child: &ObjectSchema) -> Option<EmbedMap> {
    let [pk] = parent.primary_key.as_slice() else {
        return None;
    };
    child.constraints.iter().find_map(|c| match &c.kind {
        ConstraintKind::ForeignKey {
            referenced,
            referenced_columns,
            ..
        } if referenced.name == parent.object.name
            && referenced.namespace.schema == parent.object.namespace.schema
            && referenced_columns.as_slice() == [pk.clone()]
            && c.columns.len() == 1 =>
        {
            Some(EmbedMap {
                child: child.object.clone(),
                field: child.object.name.clone(),
                foreign_key: c.columns[0].clone(),
                parent_key: pk.clone(),
                order_by: child.primary_key.clone(),
                columns: Columns::of(child),
            })
        }
        _ => None,
    })
}

/// MongoDB indexes for the table's plain indexes (the primary key is `_id`).
pub fn indexes(schema: &ObjectSchema, m: &TableDocMapping) -> Vec<IndexDefinition> {
    let single_key = (m.key.len() == 1).then(|| m.key[0].clone());
    schema
        .indexes
        .iter()
        .filter(|i| !i.primary && i.predicate.is_none() && !i.columns.is_empty())
        .filter(|i| i.columns.iter().all(|c| schema.column(c).is_some()))
        .map(|i| IndexDefinition {
            name: i.name.clone(),
            columns: i
                .columns
                .iter()
                .map(|c| {
                    if Some(c) == single_key.as_ref() {
                        ID.to_owned()
                    } else {
                        c.clone()
                    }
                })
                .collect(),
            unique: i.unique,
            native: None,
        })
        .filter(|i| i.columns != [ID])
        .collect()
}

/// Key used to match child rows to parents.
pub fn match_key(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

fn with_int32(v: Value, int32: bool) -> CanonicalValue {
    if int32 && matches!(v, Value::Int(_)) {
        CanonicalValue::with_meta(
            v,
            ValueMeta {
                original_type: Some("int".into()),
                ..Default::default()
            },
        )
    } else {
        CanonicalValue::new(v)
    }
}

/// The document for one row; `children[i]` are the rows of `m.embeds[i]`
/// that belong to it.
pub fn document(m: &TableDocMapping, row: &Record, children: &[Vec<Record>]) -> Record {
    if let Some(col) = &m.stored_document
        && let Value::Json(j) = row.get(col)
        && let Ok(gazabridge_bson::bson::Bson::Document(d)) =
            gazabridge_bson::bson::Bson::try_from(j.clone())
    {
        return gazabridge_bson::document_to_record(&d);
    }
    let mut doc = Record::with_capacity(row.fields.len() + 1);
    let id = match m.key.as_slice() {
        [k] => with_int32(m.columns.value(k, row.get(k)), m.columns.int32.contains(k)),
        keys => CanonicalValue::new(Value::Object(
            keys.iter()
                .map(|k| (k.clone(), m.columns.value(k, row.get(k))))
                .collect::<IndexMap<_, _>>(),
        )),
    };
    doc.fields.insert(ID.to_owned(), id);
    for (name, v) in &row.fields {
        if m.key.contains(name) {
            continue;
        }
        doc.fields.insert(
            name.clone(),
            with_int32(
                m.columns.value(name, &v.value),
                m.columns.int32.contains(name),
            ),
        );
    }
    for (e, rows) in m.embeds.iter().zip(children) {
        let mut paths: Vec<Vec<String>> = Vec::new();
        let items: Vec<Value> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let fields: IndexMap<String, Value> = r
                    .fields
                    .iter()
                    .filter(|(k, _)| **k != e.foreign_key)
                    .map(|(k, v)| {
                        if e.columns.int32.contains(k) && matches!(v.value, Value::Int(_)) {
                            paths.push(vec![i.to_string(), k.clone()]);
                        }
                        (k.clone(), e.columns.value(k, &v.value))
                    })
                    .collect();
                Value::Object(fields)
            })
            .collect();
        let mut meta = ValueMeta::default();
        if !paths.is_empty()
            && let Ok(raw) = serde_json::to_string(&paths)
        {
            meta.connector
                .insert(gazabridge_bson::INT32_PATHS_KEY.to_owned(), raw);
        }
        doc.fields.insert(
            e.field.clone(),
            CanonicalValue::with_meta(Value::Array(items), meta),
        );
    }
    doc
}

/// Groups child rows by the parent key they reference, keeping their order.
pub fn group_children(e: &EmbedMap, rows: Vec<Record>) -> BTreeMap<String, Vec<Record>> {
    let mut out: BTreeMap<String, Vec<Record>> = BTreeMap::new();
    for r in rows {
        out.entry(match_key(r.get(&e.foreign_key)))
            .or_default()
            .push(r);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use gazabridge_plugin_sdk::schema::{ColumnInfo, ConstraintInfo, NamespacePath};

    use super::*;

    fn col(name: &str, native: &str, t: CanonicalType) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            native_type: native.into(),
            canonical_type: t,
            nullable: true,
            default_expression: None,
            is_generated: false,
            is_identity: false,
            comment: None,
            ordinal: 1,
        }
    }

    fn table(
        name: &str,
        cols: Vec<ColumnInfo>,
        pk: &[&str],
        constraints: Vec<ConstraintInfo>,
    ) -> ObjectSchema {
        ObjectSchema {
            object: ObjectRef::table(NamespacePath::schema(None, "public"), name),
            columns: cols,
            primary_key: pk.iter().map(|s| (*s).to_owned()).collect(),
            indexes: vec![],
            constraints,
            inferred: None,
            estimated_count: None,
            comment: None,
            definition: None,
        }
    }

    #[test]
    fn rows_become_documents_with_identity_and_embedded_children() {
        let customers = table(
            "customers",
            vec![
                col(
                    "_id",
                    "character(24)",
                    CanonicalType::String {
                        max_chars: Some(24),
                    },
                ),
                col("name", "text", CanonicalType::TEXT),
                col("visits", "integer", CanonicalType::Int { bits: 32 }),
                col("prefs", "jsonb", CanonicalType::Json),
            ],
            &["_id"],
            vec![],
        );
        let orders = table(
            "orders",
            vec![
                col("id", "bigint", CanonicalType::I64),
                col(
                    "customer_id",
                    "character(24)",
                    CanonicalType::String {
                        max_chars: Some(24),
                    },
                ),
                col("qty", "integer", CanonicalType::Int { bits: 32 }),
            ],
            &["id"],
            vec![ConstraintInfo {
                name: None,
                columns: vec!["customer_id".into()],
                kind: ConstraintKind::ForeignKey {
                    referenced: customers.object.clone(),
                    referenced_columns: vec!["_id".into()],
                    on_delete: None,
                    on_update: None,
                },
            }],
        );
        let mut m = plan(&customers, ObjectRef::collection("shop", "customers")).unwrap();
        let e = embed(&customers, &orders).unwrap();
        assert_eq!(
            (e.foreign_key.as_str(), e.field.as_str()),
            ("customer_id", "orders")
        );
        m.embeds.push(e.clone());
        let oid = "65134b87d147eab2a5d15d10";
        let row = Record::from_pairs([
            ("_id", Value::String(oid.into())),
            ("name", Value::String("Ahmed".into())),
            ("visits", Value::Int(3)),
            (
                "prefs",
                Value::Json(serde_json::json!({ "since": { "$date": "2026-01-01T00:00:00Z" } })),
            ),
        ]);
        let kids = vec![Record::from_pairs([
            ("id", Value::Int(7)),
            ("customer_id", Value::String(oid.into())),
            ("qty", Value::Int(2)),
        ])];
        let groups = group_children(&e, kids);
        let d = document(
            &m,
            &row,
            &[groups[&match_key(&Value::String(oid.into()))].clone()],
        );
        let bson = gazabridge_bson::record_to_document(&d).unwrap();
        assert_eq!(bson.get_object_id("_id").unwrap().to_hex(), oid);
        assert!(matches!(
            bson.get("visits"),
            Some(gazabridge_bson::bson::Bson::Int32(3))
        ));
        assert!(
            bson.get_document("prefs")
                .unwrap()
                .get_datetime("since")
                .is_ok()
        );
        let item = bson.get_array("orders").unwrap()[0].as_document().unwrap();
        assert!(item.get("customer_id").is_none());
        assert!(matches!(
            item.get("qty"),
            Some(gazabridge_bson::bson::Bson::Int32(2))
        ));
        assert!(matches!(
            item.get("id"),
            Some(gazabridge_bson::bson::Bson::Int64(7))
        ));
    }

    #[test]
    fn tables_without_keys_are_refused_and_stored_documents_restored() {
        let t = table(
            "logs",
            vec![col("line", "text", CanonicalType::TEXT)],
            &[],
            vec![],
        );
        assert!(
            plan(&t, ObjectRef::collection("x", "logs"))
                .unwrap_err()
                .contains("primary key")
        );
        let j = table(
            "people",
            vec![
                col(
                    "_id",
                    "character(24)",
                    CanonicalType::String {
                        max_chars: Some(24),
                    },
                ),
                col("doc", "jsonb", CanonicalType::Json),
            ],
            &["_id"],
            vec![],
        );
        let m = plan(&j, ObjectRef::collection("x", "people")).unwrap();
        assert_eq!(m.stored_document.as_deref(), Some("doc"));
        let row = Record::from_pairs([
            ("_id", Value::String("65134b87d147eab2a5d15d10".into())),
            (
                "doc",
                Value::Json(
                    serde_json::json!({ "_id": { "$oid": "65134b87d147eab2a5d15d10" }, "n": { "$numberDecimal": "1.50" } }),
                ),
            ),
        ]);
        let d = gazabridge_bson::record_to_document(&document(&m, &row, &[])).unwrap();
        assert!(d.get_object_id("_id").is_ok());
        assert!(matches!(
            d.get("n"),
            Some(gazabridge_bson::bson::Bson::Decimal128(_))
        ));
    }
}
