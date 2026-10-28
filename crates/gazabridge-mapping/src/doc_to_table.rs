//! MongoDB documents → PostgreSQL rows.
//!
//! | Mode | Tables | Lossless |
//! |---|---|---|
//! | `jsonb` | `_id` + `doc jsonb` | yes |
//! | `flatten` | one column per field (`address_city`), arrays as JSONB | yes, via `_extra` |
//! | `normalize` | like flatten, arrays become child tables `(_parent_id, _index, …)` | yes, via `_extra` |
//! | `hybrid` | typed top-level columns + `_doc jsonb` holding the whole document | yes |
//!
//! Column types come from a sample. A field the sample did not see, or a
//! value whose type does not fit its column, goes into the row's `_extra`
//! JSONB object under its dotted path, so nothing is dropped silently.
//! Missing and null fields both become SQL `NULL` in typed columns.

use std::collections::{BTreeMap, BTreeSet};

use gazabridge_bson::bson::Bson;
use gazabridge_domain::{CanonicalType, Record, Value, ValueKind};
use gazabridge_plugin_sdk::ddl::{
    ColumnDefinition, ForeignKeyDefinition, IndexDefinition, TableDefinition,
};
use gazabridge_plugin_sdk::schema::{IndexInfo, InferredField, InferredStructure, ObjectRef};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{display, ext_json, ident};

pub const ID: &str = "_id";
pub const PARENT: &str = "_parent_id";
pub const INDEX: &str = "_index";
pub const EXTRA: &str = "_extra";
pub const DOC: &str = "doc";
pub const HYBRID_DOC: &str = "_doc";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocMode {
    #[default]
    Flatten,
    Normalize,
    Jsonb,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnSource {
    /// A typed scalar.
    Value,
    /// The value as JSONB (objects, arrays, mixed types).
    Json,
    /// The whole document as JSONB.
    Document,
    /// Fields no other column holds, keyed by dotted path.
    Extra,
    /// `_id` of the owning document (child tables).
    Parent,
    /// Position in the array (child tables).
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMap {
    pub name: String,
    pub source: ColumnSource,
    pub canonical_type: CanonicalType,
    pub nullable: bool,
    /// Where the value comes from, for previews (`address.city`).
    pub from: String,
}

/// How a document field is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "as", rename_all = "snake_case")]
pub enum Handling {
    /// In this column of the current table.
    Column { column: usize },
    /// An object whose fields are flattened into the current table.
    Flatten { fields: BTreeMap<String, Handling> },
    /// An array whose items are rows of this child table.
    Child { table: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMap {
    pub target: ObjectRef,
    pub columns: Vec<ColumnMap>,
    pub primary_key: Vec<String>,
    /// Field handling for the row source (the document, or an array item).
    pub fields: BTreeMap<String, Handling>,
    /// Child tables: the array's dotted path in the document.
    pub array: Option<String>,
    /// Child tables of scalar arrays: the item goes to this column.
    pub item_column: Option<usize>,
}

impl TableMap {
    fn column(&self, source: &ColumnSource) -> Option<usize> {
        self.columns.iter().position(|c| &c.source == source)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocMapping {
    pub mode: DocMode,
    /// The root table first, then child tables.
    pub tables: Vec<TableMap>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanOptions {
    /// Object nesting flattened into columns; deeper objects become JSONB.
    pub max_depth: usize,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self { max_depth: 3 }
    }
}

fn kinds(f: &InferredField) -> BTreeSet<ValueKind> {
    f.type_counts
        .keys()
        .copied()
        .filter(|k| !matches!(k, ValueKind::Null | ValueKind::Missing))
        .collect()
}

/// Column type for a scalar canonical type; `None` means "store as JSONB".
fn column_type(t: &CanonicalType) -> Option<CanonicalType> {
    use CanonicalType as T;
    Some(match t {
        T::Boolean => T::Boolean,
        T::Int { .. }
        | T::UInt {
            bits: 8 | 16 | 24 | 32,
        } => T::I64,
        T::UInt { .. } | T::BigInt | T::Decimal { .. } => T::Decimal {
            precision: None,
            scale: None,
        },
        T::Float { .. } => T::F64,
        T::String { .. } | T::Enum { .. } => T::TEXT,
        T::Binary { .. } => T::BYTES,
        T::Uuid => T::Uuid,
        T::ObjectId => T::ObjectId,
        T::Date => T::Date,
        T::LocalDateTime { .. } => T::LocalDateTime {
            fractional_digits: 6,
        },
        T::UtcDateTime { .. } => T::UtcDateTime {
            fractional_digits: 3,
        },
        T::Null => T::TEXT,
        _ => return None,
    })
}

fn scalar_type(f: &InferredField) -> Option<CanonicalType> {
    let ks = kinds(f);
    if ks.len() > 1
        || ks
            .iter()
            .any(|k| matches!(k, ValueKind::Object | ValueKind::Array))
    {
        return None;
    }
    column_type(&f.canonical_type)
}

/// Whether `v` can be stored in a column of type `t` without changing it.
fn fits(v: &Value, t: &CanonicalType) -> bool {
    use CanonicalType as T;
    match (t, v) {
        (T::Boolean, Value::Bool(_))
        | (T::Int { .. }, Value::Int(_))
        | (T::Float { .. }, Value::Float(_) | Value::Int(_))
        | (
            T::Decimal { .. },
            Value::Decimal(_) | Value::Int(_) | Value::BigInt(_) | Value::UInt(_),
        )
        | (T::String { .. }, Value::String(_))
        | (T::Binary { .. }, Value::Binary(_))
        | (T::Uuid, Value::Uuid(_))
        | (T::ObjectId, Value::ObjectId(_))
        | (T::Date, Value::Date(_))
        | (T::LocalDateTime { .. }, Value::LocalDateTime(_))
        | (T::UtcDateTime { .. }, Value::UtcDateTime(_)) => true,
        (T::Int { .. }, Value::UInt(u)) => i64::try_from(*u).is_ok(),
        _ => false,
    }
}

/// The value as stored in a typed column.
fn column_value(v: &Value) -> Value {
    match v {
        // character(24), the identity mapping back to ObjectId.
        Value::ObjectId(b) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        other => other.clone(),
    }
}

fn json_value(v: &Value) -> Value {
    match v {
        Value::Null | Value::Missing => Value::Null,
        other => Value::Json(ext_json(other)),
    }
}

/// Unique, safe column names within one table.
#[derive(Default)]
struct Names(BTreeSet<String>);

impl Names {
    fn take(&mut self, raw: &str) -> String {
        let base = ident(raw);
        let mut name = base.clone();
        let mut n = 2;
        while !self.0.insert(name.to_lowercase()) {
            name = ident(&format!("{base}_{n}"));
            n += 1;
        }
        name
    }
}

struct Planner<'a> {
    fields: &'a [InferredField],
    mode: DocMode,
    options: PlanOptions,
    tables: Vec<TableMap>,
    warnings: Vec<String>,
    id_type: CanonicalType,
    root: ObjectRef,
}

/// Direct children of `prefix` ("" for the document, `a` for an object,
/// `items[]` for array items).
fn children<'a>(fields: &'a [InferredField], prefix: &str) -> Vec<(&'a str, &'a InferredField)> {
    fields
        .iter()
        .filter_map(|f| {
            let rest = if prefix.is_empty() {
                f.path.as_str()
            } else {
                f.path.strip_prefix(prefix)?.strip_prefix('.')?
            };
            (!rest.is_empty() && !rest.contains('.') && !rest.contains("[]")).then_some((rest, f))
        })
        .collect()
}

impl Planner<'_> {
    #[allow(clippy::too_many_arguments)] // recursion state
    fn columns(
        &mut self,
        table: &mut TableMap,
        names: &mut Names,
        prefix: &str,
        name_prefix: &str,
        depth: usize,
        in_array: bool,
    ) -> BTreeMap<String, Handling> {
        let mut out = BTreeMap::new();
        for (name, f) in children(self.fields, prefix) {
            if prefix.is_empty() && name == ID && !in_array {
                continue;
            }
            let col_name = if name_prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{name_prefix}_{name}")
            };
            let ks = kinds(f);
            let flattenable = matches!(self.mode, DocMode::Flatten | DocMode::Normalize);
            let handling = if ks.len() == 1
                && ks.contains(&ValueKind::Object)
                && flattenable
                && depth < self.options.max_depth
            {
                let fields = self.columns(table, names, &f.path, &col_name, depth + 1, in_array);
                Handling::Flatten { fields }
            } else if ks.len() == 1
                && ks.contains(&ValueKind::Array)
                && self.mode == DocMode::Normalize
                && !in_array
            {
                match self.child_table(f, &col_name) {
                    Some(t) => Handling::Child { table: t },
                    None => self.push(
                        table,
                        names,
                        &col_name,
                        ColumnSource::Json,
                        CanonicalType::Json,
                        &f.path,
                    ),
                }
            } else if let Some(t) = scalar_type(f) {
                self.push(table, names, &col_name, ColumnSource::Value, t, &f.path)
            } else {
                if ks.len() > 1 {
                    self.warnings.push(format!(
                        "`{}` holds {}: stored as JSONB",
                        f.path,
                        ks.iter()
                            .map(ValueKind::as_str)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                self.push(
                    table,
                    names,
                    &col_name,
                    ColumnSource::Json,
                    CanonicalType::Json,
                    &f.path,
                )
            };
            out.insert(name.to_owned(), handling);
        }
        out
    }

    fn push(
        &self,
        table: &mut TableMap,
        names: &mut Names,
        raw: &str,
        source: ColumnSource,
        t: CanonicalType,
        from: &str,
    ) -> Handling {
        table.columns.push(ColumnMap {
            name: names.take(raw),
            source,
            canonical_type: t,
            nullable: true,
            from: from.to_owned(),
        });
        Handling::Column {
            column: table.columns.len() - 1,
        }
    }

    /// A child table for an array of objects or of same-typed scalars.
    fn child_table(&mut self, f: &InferredField, col_name: &str) -> Option<usize> {
        let items: BTreeSet<ValueKind> = f
            .array_item_type_counts
            .keys()
            .copied()
            .filter(|k| !matches!(k, ValueKind::Null | ValueKind::Missing))
            .collect();
        let item_type = match &f.canonical_type {
            CanonicalType::Array(inner) => column_type(inner),
            _ => None,
        };
        let objects = items.len() == 1 && items.contains(&ValueKind::Object);
        if !objects && (items.len() != 1 || item_type.is_none()) {
            if !items.is_empty() {
                self.warnings
                    .push(format!("`{}` mixes item types: kept as JSONB", f.path));
            }
            return None;
        }
        let mut names = Names::default();
        for reserved in [PARENT, INDEX, EXTRA] {
            names.take(reserved);
        }
        let mut table = TableMap {
            target: ObjectRef::table(
                self.root.namespace.clone(),
                ident(&format!("{}_{col_name}", self.root.name)),
            ),
            columns: vec![
                ColumnMap {
                    name: PARENT.into(),
                    source: ColumnSource::Parent,
                    canonical_type: self.id_type.clone(),
                    nullable: false,
                    from: "_id".into(),
                },
                ColumnMap {
                    name: INDEX.into(),
                    source: ColumnSource::Index,
                    canonical_type: CanonicalType::Int { bits: 32 },
                    nullable: false,
                    from: format!("{}[i]", f.path),
                },
            ],
            primary_key: vec![PARENT.into(), INDEX.into()],
            fields: BTreeMap::new(),
            array: Some(f.path.clone()),
            item_column: None,
        };
        if objects {
            let prefix = format!("{}[]", f.path);
            table.fields = self.columns(&mut table, &mut names, &prefix, "", 1, true);
        } else if let Some(t) = item_type {
            table.columns.push(ColumnMap {
                name: names.take("value"),
                source: ColumnSource::Value,
                canonical_type: t,
                nullable: true,
                from: format!("{}[]", f.path),
            });
            table.item_column = Some(table.columns.len() - 1);
        }
        table.columns.push(extra_column());
        self.tables.push(table);
        Some(self.tables.len() - 1)
    }
}

fn extra_column() -> ColumnMap {
    ColumnMap {
        name: EXTRA.into(),
        source: ColumnSource::Extra,
        canonical_type: CanonicalType::Json,
        nullable: true,
        from: "fields outside the mapping".into(),
    }
}

/// Plans tables for a collection from its sampled structure.
pub fn plan(
    target: ObjectRef,
    structure: &InferredStructure,
    mode: DocMode,
    options: PlanOptions,
) -> DocMapping {
    let fields = structure.fields.as_slice();
    let id_field = fields.iter().find(|f| f.path == ID);
    let id_type = id_field
        .map_or(Some(CanonicalType::ObjectId), scalar_type)
        .unwrap_or(CanonicalType::Json);
    let mut p = Planner {
        fields,
        mode,
        options,
        tables: Vec::new(),
        warnings: Vec::new(),
        id_type: id_type.clone(),
        root: target.clone(),
    };
    if id_type == CanonicalType::Json {
        p.warnings
            .push("`_id` values have several types: the key column is JSONB".into());
    }
    let mut names = Names::default();
    for reserved in [ID, EXTRA, HYBRID_DOC] {
        names.take(reserved);
    }
    let mut root = TableMap {
        target,
        columns: vec![ColumnMap {
            name: ID.into(),
            source: if id_type == CanonicalType::Json {
                ColumnSource::Json
            } else {
                ColumnSource::Value
            },
            canonical_type: id_type,
            nullable: false,
            from: "_id".into(),
        }],
        primary_key: vec![ID.into()],
        fields: BTreeMap::from([(ID.to_owned(), Handling::Column { column: 0 })]),
        array: None,
        item_column: None,
    };
    p.tables.push(root.clone());
    match mode {
        DocMode::Jsonb => {
            root.columns.push(ColumnMap {
                name: DOC.into(),
                source: ColumnSource::Document,
                canonical_type: CanonicalType::Json,
                nullable: false,
                from: "the whole document".into(),
            });
        }
        DocMode::Hybrid => {
            for (name, f) in children(fields, "") {
                if name == ID {
                    continue;
                }
                if let Some(t) = scalar_type(f) {
                    let h = p.push(&mut root, &mut names, name, ColumnSource::Value, t, &f.path);
                    root.fields.insert(name.to_owned(), h);
                }
            }
            root.columns.push(ColumnMap {
                name: HYBRID_DOC.into(),
                source: ColumnSource::Document,
                canonical_type: CanonicalType::Json,
                nullable: false,
                from: "the whole document".into(),
            });
        }
        DocMode::Flatten | DocMode::Normalize => {
            let more = p.columns(&mut root, &mut names, "", "", 0, false);
            root.fields.extend(more);
            root.columns.push(extra_column());
        }
    }
    if matches!(mode, DocMode::Flatten | DocMode::Normalize)
        && fields
            .iter()
            .any(|f| f.null_count > 0 || f.present_count < structure.sampled_documents)
    {
        p.warnings
            .push("missing and null fields both become NULL in typed columns".into());
    }
    if !structure.conflicting_paths.is_empty() {
        p.warnings.push(format!(
            "paths used both as objects and as other values: {} (stored as JSONB)",
            structure.conflicting_paths.join(", ")
        ));
    }
    p.tables[0] = root;
    DocMapping {
        mode,
        tables: p.tables,
        warnings: p.warnings,
    }
}

/// `CREATE TABLE` definitions, parents first.
pub fn table_definitions(m: &DocMapping) -> Vec<TableDefinition> {
    let root = m.tables[0].target.clone();
    m.tables
        .iter()
        .enumerate()
        .map(|(i, t)| TableDefinition {
            object: t.target.clone(),
            columns: t
                .columns
                .iter()
                .map(|c| ColumnDefinition {
                    name: c.name.clone(),
                    canonical_type: c.canonical_type.clone(),
                    native_type: None,
                    nullable: c.nullable,
                    default_expression: None,
                })
                .collect(),
            primary_key: t.primary_key.clone(),
            foreign_keys: if i == 0 {
                Vec::new()
            } else {
                vec![ForeignKeyDefinition {
                    name: Some(ident(&format!("{}_parent_fk", t.target.name))),
                    columns: vec![PARENT.into()],
                    referenced: root.clone(),
                    referenced_columns: vec![ID.into()],
                }]
            },
        })
        .collect()
}

/// Root-table indexes for the source's simple indexes (ascending or
/// descending keys on fields stored in typed columns).
pub fn indexes(m: &DocMapping, source: &[IndexInfo]) -> (Vec<IndexDefinition>, Vec<String>) {
    let root = &m.tables[0];
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for ix in source.iter().filter(|i| !i.primary) {
        let spec = ix
            .native
            .as_ref()
            .and_then(|n| match Bson::try_from(n.clone()) {
                Ok(Bson::Document(d)) => Some(d),
                _ => None,
            });
        let Some(spec) = spec else { continue };
        if spec.contains_key("partialFilterExpression") {
            warnings.push(format!(
                "index `{}` has a partial filter: not created",
                ix.name
            ));
            continue;
        }
        let Ok(key) = spec.get_document("key") else {
            continue;
        };
        let mut columns = Vec::new();
        for (path, dir) in key {
            let numeric = matches!(dir, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_));
            let column = numeric.then(|| column_for(root, path)).flatten();
            match column {
                Some(c) => columns.push(c),
                None => {
                    columns.clear();
                    break;
                }
            }
        }
        if columns.is_empty() {
            warnings.push(format!(
                "index `{}` does not map to typed columns: not created",
                ix.name
            ));
            continue;
        }
        if spec.contains_key("expireAfterSeconds") {
            warnings.push(format!(
                "index `{}` expires documents in MongoDB; PostgreSQL keeps the rows",
                ix.name
            ));
        }
        out.push(IndexDefinition {
            name: ident(&format!("{}_{}", root.target.name, ix.name)),
            columns,
            unique: ix.unique,
            native: None,
        });
    }
    (out, warnings)
}

fn column_for(t: &TableMap, path: &str) -> Option<String> {
    let mut fields = &t.fields;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        match fields.get(part)? {
            Handling::Column { column } if parts.peek().is_none() => {
                let c = &t.columns[*column];
                return (c.source == ColumnSource::Value).then(|| c.name.clone());
            }
            Handling::Flatten { fields: f } => fields = f,
            _ => return None,
        }
    }
    None
}

/// Rows for one table, each tagged with nothing else: callers keep the
/// source record's position.
pub type Rows = Vec<Vec<Record>>;

struct Builder<'m> {
    m: &'m DocMapping,
    out: Rows,
}

impl Builder<'_> {
    fn row(&self, t: usize) -> Vec<Value> {
        vec![Value::Missing; self.m.tables[t].columns.len()]
    }

    #[allow(clippy::too_many_arguments)] // recursion state
    fn walk<'v>(
        &mut self,
        t: usize,
        entries: impl Iterator<Item = (&'v String, &'v Value)>,
        fields: &BTreeMap<String, Handling>,
        path: &str,
        row: &mut [Value],
        extra: &mut serde_json::Map<String, serde_json::Value>,
        parent: &Value,
    ) {
        let table = &self.m.tables[t];
        let keep_extra = table.column(&ColumnSource::Extra).is_some();
        for (k, v) in entries {
            let here = if path.is_empty() {
                k.clone()
            } else {
                format!("{path}.{k}")
            };
            let spill = |extra: &mut serde_json::Map<_, _>, v: &Value| {
                if keep_extra && !matches!(v, Value::Missing) {
                    extra.insert(here.clone(), ext_json(v));
                }
            };
            match fields.get(k) {
                Some(Handling::Column { column }) => {
                    let c = &table.columns[*column];
                    row[*column] = match (&c.source, v) {
                        (_, Value::Null | Value::Missing) => Value::Null,
                        (ColumnSource::Json, v) => json_value(v),
                        (_, v) if fits(v, &c.canonical_type) => column_value(v),
                        (_, v) => {
                            spill(extra, v);
                            Value::Null
                        }
                    };
                }
                Some(Handling::Flatten { fields: sub }) => match v {
                    Value::Object(o) => self.walk(t, o.iter(), sub, &here, row, extra, parent),
                    Value::Null | Value::Missing => {}
                    other => spill(extra, other),
                },
                Some(Handling::Child { table: ct }) => match v {
                    Value::Array(items) => {
                        for (i, item) in items.iter().enumerate() {
                            self.child(*ct, item, i, parent);
                        }
                    }
                    Value::Null | Value::Missing => {}
                    other => spill(extra, other),
                },
                None => spill(extra, v),
            }
        }
    }

    fn child(&mut self, t: usize, item: &Value, index: usize, parent: &Value) {
        let table = &self.m.tables[t];
        let mut row = self.row(t);
        let mut extra = serde_json::Map::new();
        if let Some(c) = table.column(&ColumnSource::Parent) {
            row[c] = parent.clone();
        }
        if let Some(c) = table.column(&ColumnSource::Index) {
            row[c] = Value::Int(i64::try_from(index).unwrap_or(i64::MAX));
        }
        let fields = table.fields.clone();
        match (table.item_column, item) {
            (Some(c), v) => {
                row[c] = match v {
                    Value::Null => Value::Null,
                    v if fits(v, &table.columns[c].canonical_type) => column_value(v),
                    v => {
                        extra.insert("$item".into(), ext_json(v));
                        Value::Null
                    }
                };
            }
            (None, Value::Object(o)) => {
                self.walk(t, o.iter(), &fields, "", &mut row, &mut extra, parent)
            }
            (None, other) => {
                extra.insert("$item".into(), ext_json(other));
            }
        }
        self.finish(t, row, extra);
    }

    fn finish(
        &mut self,
        t: usize,
        mut row: Vec<Value>,
        extra: serde_json::Map<String, serde_json::Value>,
    ) {
        let table = &self.m.tables[t];
        if let Some(c) = table.column(&ColumnSource::Extra) {
            row[c] = if extra.is_empty() {
                Value::Null
            } else {
                Value::Json(serde_json::Value::Object(extra))
            };
        }
        let record = Record::from_pairs(table.columns.iter().zip(row).map(|(c, v)| {
            (
                c.name.clone(),
                if matches!(v, Value::Missing) {
                    Value::Null
                } else {
                    v
                },
            )
        }));
        self.out[t].push(record);
    }
}

/// Converts one document into rows for every table of the mapping.
pub fn rows(m: &DocMapping, doc: &Record) -> Result<Rows, String> {
    let mut b = Builder {
        m,
        out: vec![Vec::new(); m.tables.len()],
    };
    let root = &m.tables[0];
    let id = doc.get(ID);
    if matches!(id, Value::Missing | Value::Null) {
        return Err("the document has no `_id`".into());
    }
    let key = if root.columns[0].source == ColumnSource::Json {
        json_value(id)
    } else if fits(id, &root.columns[0].canonical_type) {
        column_value(id)
    } else {
        return Err(format!(
            "`_id` is {} but the key column holds {:?} values",
            id.kind().as_str(),
            root.columns[0].canonical_type
        ));
    };
    let mut row = b.row(0);
    let mut extra = serde_json::Map::new();
    let entries: IndexMap<String, Value> = doc
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), v.value.clone()))
        .collect();
    let fields = root.fields.clone();
    b.walk(0, entries.iter(), &fields, "", &mut row, &mut extra, &key);
    row[0] = key;
    if let Some(c) = root.column(&ColumnSource::Document) {
        let d = gazabridge_bson::record_to_document(doc).map_err(|e| e.to_string())?;
        row[c] = Value::Json(Bson::Document(d).into_relaxed_extjson());
    }
    b.finish(0, row, extra);
    Ok(b.out)
}

/// One table of a preview: column names and the first rows as text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePreview {
    pub table: ObjectRef,
    pub columns: Vec<String>,
    pub types: Vec<String>,
    pub sources: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// What the first documents become. Rows per table are capped at `max_rows`.
pub fn preview(
    m: &DocMapping,
    docs: &[Record],
    native: impl Fn(&CanonicalType) -> String,
    max_rows: usize,
) -> Vec<TablePreview> {
    let mut out: Vec<TablePreview> = m
        .tables
        .iter()
        .map(|t| TablePreview {
            table: t.target.clone(),
            columns: t.columns.iter().map(|c| c.name.clone()).collect(),
            types: t
                .columns
                .iter()
                .map(|c| native(&c.canonical_type))
                .collect(),
            sources: t.columns.iter().map(|c| c.from.clone()).collect(),
            rows: Vec::new(),
        })
        .collect();
    for doc in docs {
        let Ok(tables) = rows(m, doc) else { continue };
        for (i, rs) in tables.into_iter().enumerate() {
            for r in rs {
                if out[i].rows.len() < max_rows {
                    out[i]
                        .rows
                        .push(r.fields.values().map(|v| display(&v.value)).collect());
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use gazabridge_bson::bson::{doc, oid::ObjectId};
    use gazabridge_plugin_sdk::schema::SamplingMethod;

    use super::*;

    fn structure(docs: &[gazabridge_bson::bson::Document]) -> InferredStructure {
        // Mirrors the MongoDB connector's inference closely enough for planning.
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
                });
            f.present_count += 1;
            *f.type_counts.entry(v.kind()).or_default() += 1;
            let t = match &v {
                Value::Object(_) => CanonicalType::Object,
                Value::Array(items) => CanonicalType::Array(Box::new(CanonicalType::unify_all(
                    items.iter().map(Value::canonical_type),
                ))),
                o => o.canonical_type(),
            };
            f.canonical_type = f.canonical_type.clone().unify(t);
            match b {
                Bson::Document(d) => d
                    .iter()
                    .for_each(|(k, x)| visit(fields, &format!("{path}.{k}"), x)),
                Bson::Array(a) => {
                    for item in a {
                        *fields
                            .get_mut(path)
                            .unwrap()
                            .array_item_type_counts
                            .entry(gazabridge_bson::from_bson(item).kind())
                            .or_default() += 1;
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

    fn sample() -> Vec<gazabridge_bson::bson::Document> {
        vec![
            doc! { "_id": ObjectId::new(), "name": "Ahmed", "age": 30, "address": { "city": "Gaza", "zip": "1" }, "tags": ["a", "b"], "items": [ { "sku": "A", "qty": 2 }, { "sku": "B", "qty": 1 } ] },
            doc! { "_id": ObjectId::new(), "name": "Sara", "age": "unknown", "address": { "city": "Rafah" }, "tags": [], "items": [] },
        ]
    }

    fn rec(d: &gazabridge_bson::bson::Document) -> Record {
        gazabridge_bson::document_to_record(d)
    }

    #[test]
    fn flatten_makes_columns_and_keeps_misfits_in_extra() {
        let docs = sample();
        let m = plan(
            ObjectRef::table(Default::default(), "people"),
            &structure(&docs),
            DocMode::Flatten,
            PlanOptions::default(),
        );
        let names: Vec<&str> = m.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "_id",
                "name",
                "age",
                "address_city",
                "address_zip",
                "tags",
                "items",
                "_extra"
            ]
        );
        assert_eq!(m.tables.len(), 1);
        assert!(
            m.warnings.iter().any(|w| w.contains("`age`")),
            "{:?}",
            m.warnings
        );

        // A field the sample never saw and a value that does not fit are kept.
        let mut odd = docs[0].clone();
        odd.insert("nickname", "Abu");
        odd.get_document_mut("address").unwrap().insert("zip", 970);
        let r = rows(&m, &rec(&odd)).unwrap();
        let row = &r[0][0];
        assert_eq!(row.get("address_zip"), &Value::Null);
        let Value::Json(extra) = row.get("_extra") else {
            panic!()
        };
        assert_eq!(extra["nickname"], "Abu");
        assert_eq!(extra["address.zip"], 970);
        assert!(matches!(row.get("_id"), Value::String(s) if s.len() == 24));
        assert!(matches!(row.get("items"), Value::Json(j) if j[0]["sku"] == "A"));
    }

    #[test]
    fn normalize_moves_arrays_to_child_tables() {
        let docs = sample();
        let m = plan(
            ObjectRef::table(Default::default(), "people"),
            &structure(&docs),
            DocMode::Normalize,
            PlanOptions::default(),
        );
        let tables: Vec<&str> = m.tables.iter().map(|t| t.target.name.as_str()).collect();
        assert_eq!(tables, ["people", "people_tags", "people_items"]);
        let items: Vec<&str> = m.tables[2]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(items, ["_parent_id", "_index", "sku", "qty", "_extra"]);
        let r = rows(&m, &rec(&docs[0])).unwrap();
        assert_eq!((r[0].len(), r[1].len(), r[2].len()), (1, 2, 2));
        assert_eq!(r[2][1].get("sku"), &Value::String("B".into()));
        assert_eq!(r[2][1].get("_index"), &Value::Int(1));
        assert_eq!(r[2][0].get("_parent_id"), r[0][0].get("_id"));
        assert_eq!(r[1][0].get("value"), &Value::String("a".into()));
        let defs = table_definitions(&m);
        assert_eq!(defs[1].foreign_keys[0].referenced.name, "people");
        assert_eq!(defs[1].primary_key, ["_parent_id", "_index"]);
    }

    #[test]
    fn jsonb_and_hybrid_keep_the_whole_document() {
        let docs = sample();
        let s = structure(&docs);
        let j = plan(
            ObjectRef::table(Default::default(), "p"),
            &s,
            DocMode::Jsonb,
            PlanOptions::default(),
        );
        let r = rows(&j, &rec(&docs[0])).unwrap();
        let Value::Json(doc) = r[0][0].get("doc") else {
            panic!()
        };
        assert_eq!(doc["address"]["city"], "Gaza");
        assert!(doc["_id"]["$oid"].is_string());
        let h = plan(
            ObjectRef::table(Default::default(), "p"),
            &s,
            DocMode::Hybrid,
            PlanOptions::default(),
        );
        let names: Vec<&str> = h.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            names,
            ["_id", "name", "_doc"],
            "age has mixed types, objects stay in _doc"
        );
    }

    #[test]
    fn simple_indexes_map_to_columns() {
        let docs = sample();
        let m = plan(
            ObjectRef::table(Default::default(), "people"),
            &structure(&docs),
            DocMode::Flatten,
            PlanOptions::default(),
        );
        let ix = |name: &str, spec: gazabridge_bson::bson::Document| IndexInfo {
            name: name.into(),
            columns: vec![],
            unique: false,
            primary: false,
            method: None,
            predicate: None,
            definition: None,
            native: Some(Bson::Document(spec).into_canonical_extjson()),
        };
        let (defs, warnings) = indexes(
            &m,
            &[
                ix("by_city", doc! { "key": { "address.city": 1, "name": -1 } }),
                ix("text", doc! { "key": { "name": "text" } }),
                ix(
                    "partial",
                    doc! { "key": { "name": 1 }, "partialFilterExpression": { "age": { "$gt": 1 } } },
                ),
            ],
        );
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].columns, ["address_city", "name"]);
        assert_eq!(warnings.len(), 2);
    }
}
