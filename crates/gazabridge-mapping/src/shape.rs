//! Rebuilding documents from tables that [`crate::doc_to_table`] made with
//! the flatten or normalize modes.
//!
//! The mapping used to make the tables (which field went to which column,
//! which array became which child table, what went to `_extra`) is saved
//! next to them, and read back here: flattened columns become nested
//! objects again, child rows become arrays in their order, `_extra` is
//! merged back, and `_parent_id` / `_index` are dropped. It is driven by
//! the columns, so rows added or changed in SQL come back too.
//!
//! What SQL could not keep stays lost: a NULL is `null` for fields that
//! every sampled document had and is left out otherwise; key order follows
//! the mapping.

use std::collections::BTreeSet;

use gazabridge_bson::bson::Bson;
use gazabridge_domain::value::{CanonicalValue, ValueMeta};
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, TableDefinition};
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectRef};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::doc_to_table::{self, ColumnSource, DocMapping, Handling, TableMap};
use crate::from_ext_json;
use crate::table_to_doc::Columns;

/// Name of the table that holds, per root table, the mapping it was made
/// with. One per database (MySQL) or schema (PostgreSQL).
pub const SHAPES_TABLE: &str = "_gazabridge_shapes";
/// Version of the saved form.
pub const SHAPE_VERSION: i64 = 1;

/// A saved mapping and how to read each of its tables' columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub mapping: DocMapping,
    /// Per table of the mapping, in the same order.
    pub columns: Vec<Columns>,
}

/// How a table's columns read back, from the mapping alone (when the live
/// table cannot be inspected): ObjectId columns as ObjectIds, JSON columns
/// as Extended JSON.
pub fn columns_from_map(t: &TableMap) -> Columns {
    let mut c = Columns::default();
    for col in &t.columns {
        match col.canonical_type {
            CanonicalType::ObjectId => {
                c.object_ids.insert(col.name.clone());
            }
            CanonicalType::Json | CanonicalType::Object => {
                c.json.insert(col.name.clone());
            }
            _ => {}
        }
    }
    c
}

/// Whether a mapping can be rebuilt from (flatten and normalize; the JSONB
/// and hybrid modes keep the whole document already).
pub fn rebuildable(m: &DocMapping) -> bool {
    matches!(
        m.mode,
        doc_to_table::DocMode::Flatten | doc_to_table::DocMode::Normalize
    )
}

/// Column names of [`SHAPES_TABLE`].
pub const SHAPE_TABLE_COLUMN: &str = "table_name";
pub const SHAPE_SOURCE_COLUMN: &str = "source";
pub const SHAPE_VERSION_COLUMN: &str = "version";
pub const SHAPE_MAPPING_COLUMN: &str = "mapping";

/// The table of saved mappings in `namespace`.
pub fn shapes_table(namespace: &NamespacePath) -> TableDefinition {
    let col = |name: &str, canonical_type, nullable| ColumnDefinition {
        name: name.into(),
        canonical_type,
        native_type: None,
        nullable,
        default_expression: None,
    };
    TableDefinition {
        object: ObjectRef::table(namespace.clone(), SHAPES_TABLE),
        columns: vec![
            col(
                SHAPE_TABLE_COLUMN,
                CanonicalType::String {
                    max_chars: Some(255),
                },
                false,
            ),
            col(
                SHAPE_SOURCE_COLUMN,
                CanonicalType::String { max_chars: None },
                true,
            ),
            col(SHAPE_VERSION_COLUMN, CanonicalType::Int { bits: 64 }, false),
            col(SHAPE_MAPPING_COLUMN, CanonicalType::Json, false),
        ],
        primary_key: vec![SHAPE_TABLE_COLUMN.into()],
        foreign_keys: Vec::new(),
    }
}

/// The row saving `m` (made from `source`) under its root table's name.
pub fn shape_row(m: &DocMapping, source: &str) -> Option<Record> {
    let mapping = serde_json::to_value(m).ok()?;
    let mut r = Record::with_capacity(4);
    let mut put = |k: &str, v: Value| {
        r.fields.insert(k.to_owned(), CanonicalValue::new(v));
    };
    put(
        SHAPE_TABLE_COLUMN,
        Value::String(m.tables.first()?.target.name.clone()),
    );
    put(SHAPE_SOURCE_COLUMN, Value::String(source.to_owned()));
    put(SHAPE_VERSION_COLUMN, Value::Int(SHAPE_VERSION));
    put(SHAPE_MAPPING_COLUMN, Value::Json(mapping));
    Some(r)
}

/// The mapping saved in a row of [`SHAPES_TABLE`], if this version can read
/// it and it still names `table` as its root.
pub fn mapping_of(row: &Record, table: &str) -> Option<DocMapping> {
    let version = match row.get(SHAPE_VERSION_COLUMN) {
        Value::Int(n) => *n,
        Value::UInt(n) => i64::try_from(*n).ok()?,
        _ => return None,
    };
    if version > SHAPE_VERSION {
        return None;
    }
    let m: DocMapping = match row.get(SHAPE_MAPPING_COLUMN) {
        Value::Json(j) => serde_json::from_value(j.clone()).ok()?,
        Value::String(s) => serde_json::from_str(s).ok()?,
        _ => return None,
    };
    (rebuildable(&m) && m.tables.first().is_some_and(|t| t.target.name == table)).then_some(m)
}

/// The document for one root row; `children[i]` are the rows of table
/// `i + 1` that belong to it, in `_index` order.
pub fn rebuild(shape: &Shape, row: &Record, children: &[Vec<Record>]) -> Record {
    let b = Builder {
        shape,
        children,
        int32: Vec::new(),
    };
    b.root(row)
}

struct Builder<'a> {
    shape: &'a Shape,
    children: &'a [Vec<Record>],
    /// Paths of 32-bit integers in the document being built.
    int32: Vec<Vec<String>>,
}

fn is_empty(v: &Value) -> bool {
    matches!(v, Value::Null | Value::Missing)
}

fn has_value(obj: &IndexMap<String, Value>) -> bool {
    obj.values().any(|v| match v {
        Value::Object(o) => has_value(o),
        v => !is_empty(v),
    })
}

/// Sets `value` at a dotted `path`, creating objects on the way. A path
/// through a value that is not an object is kept as a literal key.
fn set_path(obj: &mut IndexMap<String, Value>, path: &str, value: Value) {
    match path.split_once('.') {
        None => {
            obj.insert(path.to_owned(), value);
        }
        Some((head, rest)) => {
            let slot = obj
                .entry(head.to_owned())
                .or_insert_with(|| Value::Object(IndexMap::new()));
            match slot {
                Value::Object(inner) => set_path(inner, rest, value),
                _ => {
                    obj.insert(path.to_owned(), value);
                }
            }
        }
    }
}

/// Positions (below `at`) of the 32-bit integers in a BSON value.
fn int32_paths(b: &Bson, at: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    match b {
        Bson::Int32(_) => out.push(at.clone()),
        Bson::Document(d) => {
            for (k, v) in d {
                at.push(k.clone());
                int32_paths(v, at, out);
                at.pop();
            }
        }
        Bson::Array(a) => {
            for (i, v) in a.iter().enumerate() {
                at.push(i.to_string());
                int32_paths(v, at, out);
                at.pop();
            }
        }
        _ => {}
    }
}

impl Builder<'_> {
    /// Reads stored JSON at document position `at`, keeping its 32-bit
    /// integers (plain JSON numbers that fit) as `Int32`.
    fn json(&mut self, j: &serde_json::Value, at: &[String]) -> Value {
        match Bson::try_from(j.clone()) {
            Ok(b) => {
                let mut here = at.to_vec();
                int32_paths(&b, &mut here, &mut self.int32);
                gazabridge_bson::from_bson(&b)
            }
            Err(_) => from_ext_json(j),
        }
    }

    /// A column value at `at`: JSON columns through [`Self::json`].
    fn column_value(&mut self, cols: &Columns, name: &str, raw: &Value, at: &[String]) -> Value {
        match raw {
            Value::Json(j) if cols.json.contains(name) => self.json(j, at),
            other => cols.value(name, other),
        }
    }

    /// Merges an `_extra` object into `obj` at position `at`.
    fn spread(
        &mut self,
        extra: &serde_json::Map<String, serde_json::Value>,
        at: &[String],
        obj: &mut IndexMap<String, Value>,
    ) {
        for (k, v) in extra {
            if k == "$item" {
                continue;
            }
            let mut here = at.to_vec();
            here.extend(k.split('.').map(str::to_owned));
            let value = self.json(v, &here);
            set_path(obj, k, value);
        }
    }

    fn root(mut self, row: &Record) -> Record {
        let table = &self.shape.mapping.tables[0];
        let cols = &self.shape.columns[0];
        let mut obj = IndexMap::new();
        let id_col = &table.columns[0].name;
        obj.insert(
            doc_to_table::ID.to_owned(),
            cols.value(id_col, row.get(id_col)),
        );
        self.fields(0, &table.fields, row, "", &mut Vec::new(), &mut obj);
        self.merge_extra(table, row, &mut obj);
        // Columns added in SQL after the copy come back as top-level fields.
        let mapped: BTreeSet<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        for (name, v) in &row.fields {
            if !mapped.contains(name.as_str()) && !is_empty(&v.value) && !obj.contains_key(name) {
                let value = self.column_value(cols, name, &v.value, std::slice::from_ref(name));
                obj.insert(name.clone(), value);
            }
        }
        self.record(obj)
    }

    /// Fields of `fields` read from `row` into `obj`. `path` is the hint
    /// path of the containing object, `at` its position in the document
    /// (for 32-bit integer tags).
    fn fields(
        &mut self,
        t: usize,
        fields: &std::collections::BTreeMap<String, Handling>,
        row: &Record,
        path: &str,
        at: &mut Vec<String>,
        obj: &mut IndexMap<String, Value>,
    ) {
        let shape = self.shape;
        let table = &shape.mapping.tables[t];
        let cols = &shape.columns[t];
        let hints = &shape.mapping.hints;
        for (name, h) in fields {
            if t == 0 && path.is_empty() && name == doc_to_table::ID {
                continue;
            }
            let here = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}.{name}")
            };
            let always = hints.always.contains(&here);
            at.push(name.clone());
            match h {
                Handling::Column { column } => {
                    let c = &table.columns[*column];
                    let raw = row.get(&c.name);
                    if is_empty(raw) {
                        if always {
                            obj.insert(name.clone(), Value::Null);
                        }
                    } else {
                        let v = self.column_value(cols, &c.name, raw, at);
                        if matches!(v, Value::Int(_)) && hints.int32.contains(&here) {
                            self.int32.push(at.clone());
                        }
                        obj.insert(name.clone(), v);
                    }
                }
                Handling::Flatten { fields: sub } => {
                    let mut inner = IndexMap::new();
                    self.fields(t, sub, row, &here, at, &mut inner);
                    // Nulls alone do not prove the object was there.
                    if always || has_value(&inner) {
                        obj.insert(name.clone(), Value::Object(inner));
                    }
                }
                Handling::Child { table: ct } => {
                    // Only root tables have child tables (one level).
                    let items = self.items(*ct, &here, at);
                    if !items.is_empty() || always {
                        obj.insert(name.clone(), Value::Array(items));
                    }
                }
            }
            at.pop();
        }
    }

    /// The array made from child table `t`.
    fn items(&mut self, t: usize, path: &str, at: &mut Vec<String>) -> Vec<Value> {
        let shape = self.shape;
        let table = &shape.mapping.tables[t];
        let cols = &shape.columns[t];
        let children = self.children;
        let Some(rows) = children.get(t - 1) else {
            return Vec::new();
        };
        let mut rows: Vec<&Record> = rows.iter().collect();
        if let Some(c) = table.column(&ColumnSource::Index) {
            let name = &table.columns[c].name;
            rows.sort_by_key(|r| match r.get(name) {
                Value::Int(n) => *n,
                Value::UInt(n) => i64::try_from(*n).unwrap_or(i64::MAX),
                _ => i64::MAX,
            });
        }
        let item_path = format!("{path}[]");
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            at.push(i.to_string());
            let extra = extra_of(table, row);
            let item = if let Some(v) = extra.as_ref().and_then(|e| e.get("$item")) {
                self.json(v, at)
            } else if let Some(c) = table.item_column {
                let name = &table.columns[c].name;
                let v = self.column_value(cols, name, row.get(name), at);
                if matches!(v, Value::Int(_)) && shape.mapping.hints.int32.contains(&item_path) {
                    self.int32.push(at.clone());
                }
                if matches!(v, Value::Missing) {
                    Value::Null
                } else {
                    v
                }
            } else {
                let mut obj = IndexMap::new();
                self.fields(t, &table.fields, row, &item_path, at, &mut obj);
                if let Some(serde_json::Value::Object(e)) = &extra {
                    self.spread(e, at, &mut obj);
                }
                Value::Object(obj)
            };
            at.pop();
            out.push(item);
        }
        out
    }

    fn merge_extra(&mut self, table: &TableMap, row: &Record, obj: &mut IndexMap<String, Value>) {
        if let Some(serde_json::Value::Object(e)) = extra_of(table, row) {
            self.spread(&e, &[], obj);
        }
    }

    fn record(self, obj: IndexMap<String, Value>) -> Record {
        let mut r = Record::with_capacity(obj.len());
        for (k, v) in obj {
            let nested: Vec<Vec<String>> = self
                .int32
                .iter()
                .filter(|p| p.len() > 1 && p[0] == k)
                .map(|p| p[1..].to_vec())
                .collect();
            let top = self.int32.iter().any(|p| p.len() == 1 && p[0] == k);
            let cv = if !top && nested.is_empty() {
                CanonicalValue::new(v)
            } else {
                let mut meta = ValueMeta {
                    original_type: top.then(|| "int".to_owned()),
                    ..Default::default()
                };
                if !nested.is_empty()
                    && let Ok(raw) = serde_json::to_string(&nested)
                {
                    meta.connector
                        .insert(gazabridge_bson::INT32_PATHS_KEY.to_owned(), raw);
                }
                CanonicalValue::with_meta(v, meta)
            };
            r.fields.insert(k, cv);
        }
        r
    }
}

/// A row's `_extra` object, if any.
fn extra_of(table: &TableMap, row: &Record) -> Option<serde_json::Value> {
    let c = table.column(&ColumnSource::Extra)?;
    match row.get(&table.columns[c].name) {
        Value::Json(j @ serde_json::Value::Object(_)) => Some(j.clone()),
        Value::String(s) => serde_json::from_str(s).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use gazabridge_bson::bson::{Bson, Document, doc, oid::ObjectId};
    use gazabridge_domain::{CanonicalType, ValueKind};
    use gazabridge_plugin_sdk::schema::{
        InferredField, InferredStructure, ObjectRef, SamplingMethod,
    };

    use super::*;
    use crate::doc_to_table::{DocMode, PlanOptions};

    /// The sampled structure, as the MongoDB connector infers it.
    fn structure(docs: &[Document]) -> InferredStructure {
        let mut fields: IndexMap<String, InferredField> = IndexMap::new();
        fn visit(fields: &mut IndexMap<String, InferredField>, path: &str, b: &Bson) {
            let v = gazabridge_bson::from_bson(b);
            let f = fields
                .entry(path.to_owned())
                .or_insert_with(|| InferredField {
                    path: path.to_owned(),
                    present_count: 0,
                    null_count: 0,
                    type_counts: IndexMap::new(),
                    canonical_type: CanonicalType::Null,
                    array_item_type_counts: IndexMap::new(),
                    int32_count: 0,
                    array_item_int32_count: 0,
                });
            f.present_count += 1;
            if matches!(b, Bson::Null) {
                f.null_count += 1;
            }
            *f.type_counts.entry(v.kind()).or_default() += 1;
            if matches!(b, Bson::Int32(_)) {
                f.int32_count += 1;
            }
            let t = match (&v, b) {
                (Value::Object(_), _) => CanonicalType::Object,
                (Value::Array(items), _) => CanonicalType::Array(Box::new(
                    CanonicalType::unify_all(items.iter().map(Value::canonical_type)),
                )),
                (o, _) => o.canonical_type(),
            };
            f.canonical_type = f.canonical_type.clone().unify(t);
            match b {
                Bson::Document(d) => d
                    .iter()
                    .for_each(|(k, x)| visit(fields, &format!("{path}.{k}"), x)),
                Bson::Array(a) => {
                    for item in a {
                        let f = fields.get_mut(path).unwrap();
                        *f.array_item_type_counts
                            .entry(gazabridge_bson::from_bson(item).kind())
                            .or_default() += 1;
                        if matches!(item, Bson::Int32(_)) {
                            f.array_item_int32_count += 1;
                        }
                        if let Bson::Document(d) = item {
                            d.iter()
                                .for_each(|(k, x)| visit(fields, &format!("{path}[].{k}"), x));
                        }
                    }
                }
                _ => {}
            }
        }
        for d in docs {
            for (k, v) in d {
                visit(&mut fields, k, v);
            }
        }
        InferredStructure {
            sampled_documents: docs.len() as u64,
            sampling: SamplingMethod::Random,
            max_depth: 3,
            fields: fields.into_values().collect(),
            conflicting_paths: vec![],
        }
    }

    /// Keys sorted at every level, for comparing documents.
    fn sorted(b: &Bson) -> Bson {
        match b {
            Bson::Document(d) => {
                let mut keys: Vec<&String> = d.keys().collect();
                keys.sort();
                Bson::Document(
                    keys.into_iter()
                        .map(|k| (k.clone(), sorted(&d[k])))
                        .collect(),
                )
            }
            Bson::Array(a) => Bson::Array(a.iter().map(sorted).collect()),
            o => o.clone(),
        }
    }

    fn round_trip(docs: &[Document], mode: DocMode) -> Vec<Document> {
        let m = doc_to_table::plan(
            ObjectRef::table(
                gazabridge_plugin_sdk::schema::NamespacePath::schema(None, "public"),
                "tasks",
            ),
            &structure(docs),
            mode,
            PlanOptions::default(),
        );
        let shape = Shape {
            columns: m.tables.iter().map(columns_from_map).collect(),
            mapping: m.clone(),
        };
        docs.iter()
            .map(|d| {
                let tables =
                    doc_to_table::rows(&m, &gazabridge_bson::document_to_record(d)).unwrap();
                let root = &tables[0][0];
                let rebuilt = rebuild(&shape, root, &tables[1..]);
                gazabridge_bson::record_to_document(&rebuilt).unwrap()
            })
            .collect()
    }

    fn docs() -> Vec<Document> {
        let sub = ObjectId::new();
        vec![
            doc! {
                "_id": ObjectId::new(), "title": "عنصر 🚀", "count": 3_i32, "big": 5_000_000_000_i64,
                "price": 2.5, "done": false, "note": Bson::Null,
                "address": { "city": "Gaza", "geo": { "lat": 31.5, "lng": 34.4 } },
                "tags": ["a", "b"],
                "tours": [ { "_id": sub, "title": "t1", "n": 1_i32 }, { "_id": sub, "title": "t2", "n": 2_i32, "rare": "x" } ],
                "mixed": [1_i32, "two"],
            },
            doc! {
                "_id": ObjectId::new(), "title": "second", "count": 4_i32, "big": 1_i64,
                "price": 1.0, "done": true, "note": "kept",
                "address": { "city": "Rafah" },
                "tags": [],
                "tours": [],
                "mixed": [],
                "odd": { "$weird": 1_i32 },
            },
        ]
    }

    #[test]
    fn normalized_documents_come_back_as_they_were() {
        let originals = docs();
        for (orig, back) in originals
            .iter()
            .zip(round_trip(&originals, DocMode::Normalize))
        {
            assert_eq!(
                sorted(&Bson::Document(orig.clone())),
                sorted(&Bson::Document(back.clone())),
                "\noriginal: {orig}\nrebuilt:  {back}"
            );
        }
    }

    #[test]
    fn flattened_documents_come_back_as_they_were() {
        let originals = docs();
        for (orig, back) in originals
            .iter()
            .zip(round_trip(&originals, DocMode::Flatten))
        {
            assert_eq!(
                sorted(&Bson::Document(orig.clone())),
                sorted(&Bson::Document(back.clone())),
                "\noriginal: {orig}\nrebuilt:  {back}"
            );
        }
    }

    #[test]
    fn rows_written_in_sql_are_rebuilt_too() {
        let originals = docs();
        let m = doc_to_table::plan(
            ObjectRef::table(
                gazabridge_plugin_sdk::schema::NamespacePath::schema(None, "public"),
                "tasks",
            ),
            &structure(&originals),
            DocMode::Normalize,
            PlanOptions::default(),
        );
        let shape = Shape {
            columns: m.tables.iter().map(columns_from_map).collect(),
            mapping: m.clone(),
        };
        // An INSERT from application code: an ObjectId-shaped key, a few
        // columns, NULL elsewhere, and a column added later.
        let id = "65134b87d147eab2a5d15d10";
        let root = Record::from_pairs([
            ("_id", Value::String(id.into())),
            ("title", Value::String("from sql".into())),
            ("address_city", Value::String("Khan Younis".into())),
            ("count", Value::Int(7)),
            ("note", Value::Null),
            ("added_later", Value::Bool(true)),
        ]);
        let tours = m
            .tables
            .iter()
            .position(|t| t.target.name == "tasks_tours")
            .unwrap();
        let mut children = vec![Vec::new(); m.tables.len() - 1];
        children[tours - 1] = vec![Record::from_pairs([
            ("_parent_id", Value::String(id.into())),
            ("_index", Value::Int(0)),
            ("title", Value::String("only".into())),
        ])];
        let d = gazabridge_bson::record_to_document(&rebuild(&shape, &root, &children)).unwrap();
        assert_eq!(d.get_object_id("_id").unwrap().to_hex(), id);
        assert_eq!(
            d.get_document("address").unwrap().get_str("city").unwrap(),
            "Khan Younis"
        );
        assert_eq!(d.get_i32("count").unwrap(), 7, "{d}");
        assert_eq!(
            d.get("note"),
            Some(&Bson::Null),
            "every sampled document had `note`"
        );
        assert!(d.get("big").is_some_and(|b| matches!(b, Bson::Null)));
        assert!(d.get_bool("added_later").unwrap());
        let tours = d.get_array("tours").unwrap();
        assert_eq!(tours.len(), 1);
        let t = tours[0].as_document().unwrap();
        assert!(
            t.get("_parent_id").is_none() && t.get("_index").is_none(),
            "{t}"
        );
        assert_eq!(t.get_str("title").unwrap(), "only");
        assert_eq!(
            d.get_array("tags").unwrap().len(),
            0,
            "every document had `tags`"
        );
        let _ = ValueKind::Null;
    }
}
