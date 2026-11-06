//! MongoDB ⇄ PostgreSQL conversions for [`crate::MigrationService`]:
//! planning each stream's mapping, previewing it, preparing the target and
//! converting records on the way.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_mapping::doc_to_table::{self, DocMapping, PlanOptions};
use gazabridge_mapping::shape;
use gazabridge_mapping::table_to_doc::{self, TableDocMapping, TableMode};
use gazabridge_migration::{TargetSpec, Transform, Transformed};
use gazabridge_plugin_sdk::ConnectorError;
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions};
use gazabridge_plugin_sdk::ddl::{IndexDefinition, SchemaOperation};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::{
    InferredStructure, ObjectKind, ObjectRef, ObjectSchema, SamplingMethod,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::AppResult;
use crate::migration::{CopyItem, ExistingData, MigrationRequest, PlanItem};
use crate::table_copy::{self, TableCopy, TableCopyMapping};

/// Documents sampled to choose column types.
const SAMPLE: u64 = 2_000;
/// Source records shown in a preview.
const PREVIEW_RECORDS: u64 = 3;
/// Bound for counts that may scan a table.
const COUNT_TIME: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Copy,
    DocToTable,
    TableToDoc,
    /// Relational tables → relational tables.
    TableCopy,
}

pub fn direction(source: EngineFamily, target: EngineFamily) -> Option<Direction> {
    match (source, target) {
        (EngineFamily::MongoDb, EngineFamily::MongoDb) => Some(Direction::Copy),
        (EngineFamily::MongoDb, EngineFamily::Postgres) => Some(Direction::DocToTable),
        (EngineFamily::Postgres, EngineFamily::MongoDb) => Some(Direction::TableToDoc),
        (EngineFamily::MongoDb, EngineFamily::MySql) => Some(Direction::DocToTable),
        (EngineFamily::MySql, EngineFamily::MongoDb) => Some(Direction::TableToDoc),
        (s, t) if table_copy::is_relational(s) && table_copy::is_relational(t) => {
            Some(Direction::TableCopy)
        }
        _ => None,
    }
}

/// What one stream does, decided when the run starts and stored with it so
/// a resumed run converts exactly like the first attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ItemMapping {
    /// MongoDB → MongoDB data and indexes (runs recorded before collection
    /// options were copied).
    Copy,
    /// MongoDB → MongoDB: data, indexes and collection options.
    Collection {
        /// `create` options (capped, collation, time series…).
        #[serde(default)]
        options: Option<serde_json::Value>,
        /// `validator`, `validationLevel`, `validationAction`, applied after
        /// the data (only to collections the copy creates).
        #[serde(default)]
        validator: Option<serde_json::Value>,
    },
    /// A view recreated from its definition (`viewOn`, `pipeline`).
    View { options: serde_json::Value },
    DocToTable {
        mapping: Box<DocMapping>,
        indexes: Vec<IndexDefinition>,
        /// Long doubles inside JSON columns written tagged (MySQL targets,
        /// see [`gazabridge_mapping::exact_doubles`]).
        #[serde(default)]
        exact_doubles: bool,
    },
    TableToDoc {
        mapping: Box<TableDocMapping>,
        indexes: Vec<IndexDefinition>,
    },
    /// A relational table copied into another relational database.
    TableCopy(Box<TableCopyMapping>),
    /// Rows embedded in another stream's documents; not copied on their own.
    Embedded { into: ObjectRef },
}

/// Whether the target needs `CREATE SCHEMA` (PostgreSQL, other than
/// `public`) or `CREATE DATABASE` (MySQL namespaces have no schema level).
/// `database.schema.name` of an object, for messages and saved shapes.
fn object_label(o: &ObjectRef) -> String {
    let mut parts: Vec<&str> = Vec::new();
    parts.extend(o.namespace.database.as_deref());
    parts.extend(o.namespace.schema.as_deref());
    parts.push(&o.name);
    parts.join(".")
}

fn needs_namespace(ns: &gazabridge_plugin_sdk::schema::NamespacePath) -> bool {
    match (&ns.schema, &ns.database) {
        (Some(s), _) => s != "public" && s != "main",
        (None, Some(_)) => true,
        (None, None) => false,
    }
}

impl ItemMapping {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Copy | Self::Collection { .. } => "copy",
            Self::View { .. } => "view",
            Self::DocToTable { .. } => "doc_to_table",
            Self::TableToDoc { .. } => "table_to_doc",
            Self::TableCopy(_) => "table_copy",
            Self::Embedded { .. } => "embedded",
        }
    }

    /// Target preparation: drop (when replacing) and create.
    /// The row saving how flattened or normalized tables were made, for
    /// [`shape::SHAPES_TABLE`], so a copy back rebuilds the documents.
    pub fn shape_row(&self, item: &CopyItem) -> Option<Record> {
        match self {
            Self::DocToTable { mapping, .. } if shape::rebuildable(mapping) => {
                shape::shape_row(mapping, &object_label(&item.source))
            }
            _ => None,
        }
    }

    pub fn prepare_ops(&self, item: &CopyItem, existing: ExistingData) -> Vec<SchemaOperation> {
        let replace = existing == ExistingData::Replace;
        match self {
            Self::Copy | Self::TableToDoc { .. } | Self::Collection { .. } | Self::View { .. } => {
                let mut ops = Vec::new();
                if replace {
                    ops.push(SchemaOperation::DropObject {
                        object: item.target.clone(),
                        if_exists: true,
                    });
                }
                ops.push(SchemaOperation::CreateCollection {
                    object: item.target.clone(),
                    if_not_exists: true,
                    options: match self {
                        Self::Collection { options, .. } => options.clone(),
                        Self::View { options } => Some(options.clone()),
                        _ => None,
                    },
                });
                ops
            }
            Self::DocToTable { mapping, .. } => {
                let mut ops = Vec::new();
                if replace {
                    // Children first: they reference the root table.
                    for t in mapping.tables.iter().rev() {
                        ops.push(SchemaOperation::DropObject {
                            object: t.target.clone(),
                            if_exists: true,
                        });
                    }
                }
                if needs_namespace(&item.target.namespace) {
                    ops.push(SchemaOperation::CreateNamespace {
                        namespace: item.target.namespace.clone(),
                        if_not_exists: true,
                    });
                }
                for table in doc_to_table::table_definitions(mapping) {
                    ops.push(SchemaOperation::CreateTable {
                        table,
                        if_not_exists: true,
                    });
                }
                // Shared by every copy into this namespace: never dropped.
                if shape::rebuildable(mapping) {
                    ops.push(SchemaOperation::CreateTable {
                        table: shape::shapes_table(&item.target.namespace),
                        if_not_exists: true,
                    });
                }
                ops
            }
            Self::TableCopy(t) => {
                let mut ops = Vec::new();
                if replace {
                    ops.push(SchemaOperation::DropObject {
                        object: item.target.clone(),
                        if_exists: true,
                    });
                }
                if needs_namespace(&item.target.namespace) {
                    ops.push(SchemaOperation::CreateNamespace {
                        namespace: item.target.namespace.clone(),
                        if_not_exists: true,
                    });
                }
                ops.push(SchemaOperation::CreateTable {
                    table: t.table.clone(),
                    if_not_exists: true,
                });
                ops
            }
            Self::Embedded { .. } => Vec::new(),
        }
    }

    /// What is applied once the data is in place (validation rules).
    pub fn finish_ops(&self, item: &CopyItem) -> Vec<SchemaOperation> {
        match self {
            Self::Collection {
                validator: Some(v), ..
            } => vec![SchemaOperation::AlterCollection {
                object: item.target.clone(),
                options: v.clone(),
            }],
            _ => Vec::new(),
        }
    }

    /// Indexes created after the data (conversions; copies read them live).
    pub fn index_ops(&self, item: &CopyItem) -> Vec<SchemaOperation> {
        let (object, indexes) = match self {
            Self::DocToTable {
                mapping, indexes, ..
            } => (mapping.tables[0].target.clone(), indexes),
            Self::TableToDoc { indexes, .. } => (item.target.clone(), indexes),
            Self::TableCopy(t) => (item.target.clone(), &t.indexes),
            _ => return Vec::new(),
        };
        indexes
            .iter()
            .map(|ix| SchemaOperation::CreateIndex {
                object: object.clone(),
                index: ix.clone(),
                if_not_exists: true,
            })
            .collect()
    }

    /// Identity of a source record, for messages about rejected records.
    pub fn source_keys(&self) -> Vec<String> {
        match self {
            Self::TableToDoc { mapping, .. } => mapping.key.clone(),
            Self::TableCopy(t) => t.table.primary_key.clone(),
            _ => vec!["_id".into()],
        }
    }

    pub fn transform(
        &self,
        existing: ExistingData,
        source: Arc<dyn Connection>,
    ) -> Option<Arc<dyn Transform>> {
        match self {
            Self::DocToTable {
                mapping,
                exact_doubles,
                ..
            } => Some(Arc::new(DocToTable {
                mapping: mapping.as_ref().clone(),
                exact_doubles: *exact_doubles,
                replace_children: existing == ExistingData::Update,
            })),
            Self::TableToDoc { mapping, .. } => Some(Arc::new(TableToDoc {
                mapping: mapping.as_ref().clone(),
                source,
            })),
            Self::TableCopy(t) => Some(Arc::new(TableCopy {
                mapping: t.as_ref().clone(),
            })),
            Self::Copy | Self::Collection { .. } | Self::View { .. } | Self::Embedded { .. } => {
                None
            }
        }
    }
}

/// Documents → rows of the mapped tables.
struct DocToTable {
    mapping: DocMapping,
    replace_children: bool,
    exact_doubles: bool,
}

#[async_trait]
impl Transform for DocToTable {
    fn targets(&self) -> Vec<TargetSpec> {
        self.mapping
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| TargetSpec {
                object: t.target.clone(),
                key_fields: t.primary_key.clone(),
                owner: Some(
                    if i == 0 {
                        doc_to_table::ID
                    } else {
                        doc_to_table::PARENT
                    }
                    .into(),
                ),
                replace: i > 0 && self.replace_children,
            })
            .collect()
    }

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError> {
        let mut out = Transformed {
            rows: vec![Vec::new(); self.mapping.tables.len()],
            ..Transformed::default()
        };
        for (i, r) in records.iter().enumerate() {
            match doc_to_table::rows(&self.mapping, r) {
                Ok(tables) => {
                    if let Some(root) = tables.first().and_then(|t| t.first()) {
                        out.owners.push((i, root.get(doc_to_table::ID).clone()));
                    }
                    for (t, mut rows) in tables.into_iter().enumerate() {
                        if self.exact_doubles {
                            for row in &mut rows {
                                for v in row.fields.values_mut() {
                                    if let Value::Json(j) = &mut v.value {
                                        gazabridge_mapping::exact_doubles(j);
                                    }
                                }
                            }
                        }
                        out.rows[t].extend(rows.into_iter().map(|row| (i, row)));
                    }
                }
                Err(m) => out.rejected.push((i, m)),
            }
        }
        Ok(out)
    }
}

/// Rows → documents, with child rows read and embedded per batch.
struct TableToDoc {
    mapping: TableDocMapping,
    source: Arc<dyn Connection>,
}

async fn read_all(
    source: &dyn Connection,
    request: ReadRequest,
) -> Result<Vec<Record>, ConnectorError> {
    let mut stream = source.read(request, CancellationToken::new()).await?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.extend(batch?.records);
    }
    Ok(out)
}

#[async_trait]
impl Transform for TableToDoc {
    fn targets(&self) -> Vec<TargetSpec> {
        vec![TargetSpec {
            object: self.mapping.target.clone(),
            key_fields: vec!["_id".into()],
            owner: None,
            replace: false,
        }]
    }

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError> {
        let mut groups = Vec::with_capacity(self.mapping.embeds.len());
        for e in &self.mapping.embeds {
            let mut seen = BTreeSet::new();
            let values: Vec<Value> = records
                .iter()
                .map(|r| r.get(&e.parent_key).clone())
                .filter(|v| !matches!(v, Value::Null | Value::Missing))
                .filter(|v| seen.insert(table_to_doc::match_key(v)))
                .collect();
            let rows = if values.is_empty() {
                Vec::new()
            } else {
                let mut req = ReadRequest::new(e.child.clone(), 5_000);
                req.filter = Some(Filter::In {
                    field: FieldPath::new(e.foreign_key.clone()),
                    values,
                });
                req.order_by = e
                    .order_by
                    .iter()
                    .map(|c| SortKey {
                        field: FieldPath::new(c.clone()),
                        direction: SortDirection::Asc,
                    })
                    .collect();
                read_all(self.source.as_ref(), req).await?
            };
            groups.push(table_to_doc::group_children(e, rows));
        }
        let docs = records
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let children: Vec<Vec<Record>> = self
                    .mapping
                    .embeds
                    .iter()
                    .zip(&groups)
                    .map(|(e, g)| {
                        g.get(&table_to_doc::match_key(r.get(&e.parent_key)))
                            .cloned()
                            .unwrap_or_default()
                    })
                    .collect();
                (i, table_to_doc::document(&self.mapping, r, &children))
            })
            .collect();
        Ok(Transformed {
            rows: vec![docs],
            rejected: Vec::new(),
            owners: Vec::new(),
        })
    }
}

/// A MongoDB collection's `listCollections` entry: kind and options.
pub async fn collection_spec(
    conn: &dyn Connection,
    object: &ObjectRef,
) -> Option<(String, serde_json::Map<String, serde_json::Value>)> {
    let schema = conn
        .inspect(
            object,
            &InspectOptions {
                include_indexes: false,
                include_constraints: false,
                sample_size: 1,
                sampling: SamplingMethod::First,
            },
        )
        .await
        .ok()?;
    let spec: serde_json::Value = serde_json::from_str(schema.definition.as_deref()?).ok()?;
    let kind = spec.get("type")?.as_str()?.to_owned();
    let options = spec.get("options")?.as_object()?.clone();
    Some((kind, options))
}

const VALIDATION: [&str; 3] = ["validator", "validationLevel", "validationAction"];

/// The copy mapping for a collection or view. `target_kept`: an existing
/// target stays (no replace), so its own validation rules are left alone.
pub fn copy_mapping(
    spec: Option<(String, serde_json::Map<String, serde_json::Value>)>,
    target_kept: bool,
) -> ItemMapping {
    let Some((kind, mut options)) = spec else {
        return ItemMapping::Collection {
            options: None,
            validator: None,
        };
    };
    if kind == "view" {
        return ItemMapping::View {
            options: serde_json::Value::Object(options),
        };
    }
    let mut validator = serde_json::Map::new();
    for k in VALIDATION {
        if let Some(v) = options.remove(k) {
            validator.insert(k.to_owned(), v);
        }
    }
    ItemMapping::Collection {
        options: (!options.is_empty()).then(|| serde_json::Value::Object(options)),
        validator: (!validator.is_empty() && !target_kept)
            .then(|| serde_json::Value::Object(validator)),
    }
}

/// Counts that never keep a server busy: an estimate, else a bounded count.
pub async fn quick_count(conn: &dyn Connection, object: &ObjectRef) -> Option<u64> {
    if let Ok(c) = conn.count(object, None, false).await {
        return Some(c.count);
    }
    conn.count_within(object, None, COUNT_TIME)
        .await
        .ok()
        .map(|c| c.count)
}

pub(crate) async fn sample(conn: &dyn Connection, object: &ObjectRef) -> Vec<Record> {
    let mut req = ReadRequest::new(object.clone(), PREVIEW_RECORDS as usize);
    req.limit = Some(PREVIEW_RECORDS);
    read_all(conn, req).await.unwrap_or_default()
}

fn pretty(doc: &Record) -> String {
    gazabridge_bson::record_to_document(doc)
        .map(|d| gazabridge_bson::bson::Bson::Document(d).into_relaxed_extjson())
        .ok()
        .and_then(|j| serde_json::to_string_pretty(&j).ok())
        .unwrap_or_default()
}

/// Names of the objects in each target namespace, listed once.
#[derive(Debug, Default)]
pub struct Existing(BTreeMap<String, BTreeSet<String>>);

impl Existing {
    pub async fn contains(&mut self, conn: &dyn Connection, object: &ObjectRef) -> bool {
        let key = format!("{:?}", object.namespace);
        if !self.0.contains_key(&key) {
            let names = conn
                .list(&object.namespace)
                .await
                .map(|l| l.into_iter().map(|n| n.name).collect())
                .unwrap_or_default();
            self.0.insert(key.clone(), names);
        }
        self.0.get(&key).is_some_and(|n| n.contains(&object.name))
    }
}

pub(crate) fn empty_item(item: &CopyItem, kind: &str) -> PlanItem {
    PlanItem {
        source: item.source.clone(),
        target: item.target.clone(),
        kind: kind.into(),
        source_count: None,
        target_exists: false,
        target_count: None,
        indexes: Vec::new(),
        statements: Vec::new(),
        warnings: Vec::new(),
        tables: Vec::new(),
        documents: Vec::new(),
        embedded_into: None,
    }
}

#[derive(Debug)]
pub struct Planned {
    pub items: Vec<PlanItem>,
    pub mappings: Vec<ItemMapping>,
    pub blockers: Vec<String>,
    pub destructive: bool,
}

/// Plans collections → tables.
pub async fn plan_doc_to_table(
    req: &MigrationRequest,
    source: &dyn Connection,
    target: &dyn Connection,
) -> AppResult<Planned> {
    let mut out = Planned {
        items: Vec::new(),
        mappings: Vec::new(),
        blockers: Vec::new(),
        destructive: false,
    };
    let mut existing = Existing::default();
    let native = |t: &CanonicalType| target.native_type_for(t).unwrap_or_else(|_| "jsonb".into());
    for item in &req.items {
        let mut p = empty_item(item, "doc_to_table");
        let schema = source
            .inspect(
                &item.source,
                &InspectOptions {
                    include_indexes: true,
                    include_constraints: false,
                    sample_size: SAMPLE,
                    sampling: SamplingMethod::Random,
                },
            )
            .await?;
        let structure = schema.inferred.clone().unwrap_or(InferredStructure {
            sampled_documents: 0,
            sampling: SamplingMethod::Random,
            max_depth: 0,
            fields: Vec::new(),
            conflicting_paths: Vec::new(),
        });
        if structure.sampled_documents == 0 {
            p.warnings.push(
                "the collection is empty: only `_id` and `_extra` columns are planned".into(),
            );
        }
        let mapping = doc_to_table::plan(
            item.target.clone(),
            &structure,
            req.doc_mode,
            PlanOptions::default(),
        );
        p.warnings.extend(mapping.warnings.iter().cloned());
        let indexes = if req.copy_indexes {
            let (ix, w) = doc_to_table::indexes(&mapping, &schema.indexes);
            p.warnings.extend(w);
            ix
        } else {
            Vec::new()
        };
        let (mapping, indexes) = if target.manifest().family == EngineFamily::MySql {
            let mut mapping = mapping;
            if let Err(e) = fit_mysql_key(&mut mapping, &mut p.warnings) {
                out.blockers.push(format!("{}: {e}", item.target));
            }
            fit_mysql_indexes(mapping, indexes, &mut p.warnings)
        } else {
            (mapping, indexes)
        };
        p.indexes = indexes.iter().map(|i| i.name.clone()).collect();
        p.source_count = quick_count(source, &item.source).await;
        // Existing tables must already have every planned column.
        for t in &mapping.tables {
            if !existing.contains(target, &t.target).await {
                continue;
            }
            if t.target == mapping.tables[0].target {
                p.target_exists = true;
                p.target_count = quick_count(target, &t.target).await;
            }
            if req.existing == ExistingData::Replace {
                out.destructive = true;
                p.warnings.push(format!(
                    "table `{}` and its rows will be dropped first",
                    t.target.name
                ));
                continue;
            }
            let have: BTreeSet<String> = target
                .inspect(
                    &t.target,
                    &InspectOptions {
                        include_indexes: false,
                        include_constraints: false,
                        sample_size: 0,
                        sampling: SamplingMethod::First,
                    },
                )
                .await
                .map(|s| s.columns.into_iter().map(|c| c.name).collect())
                .unwrap_or_default();
            let missing: Vec<&str> = t
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .filter(|c| !have.contains(*c))
                .collect();
            if !missing.is_empty() {
                out.blockers.push(format!(
                    "table `{}` already exists without the column(s) {}; choose \"Delete the target first\" or another table name",
                    t.target.name,
                    missing.join(", ")
                ));
            }
        }
        let m = ItemMapping::DocToTable {
            mapping: Box::new(mapping),
            indexes,
            exact_doubles: target.manifest().family == EngineFamily::MySql,
        };
        let mut ops = m.prepare_ops(item, req.existing);
        ops.extend(m.index_ops(item));
        match target.plan_ddl(&ops) {
            Ok(d) => p.statements = d.statements,
            Err(e) => out.blockers.push(format!("{}: {e}", item.target)),
        }
        if let ItemMapping::DocToTable { mapping, .. } = &m {
            let docs = sample(source, &item.source).await;
            p.tables = doc_to_table::preview(mapping, &docs, native, 5);
            p.documents = docs.iter().map(pretty).collect();
        }
        out.items.push(p);
        out.mappings.push(m);
    }
    Ok(out)
}

/// InnoDB keys hold at most 3,072 bytes; utf8mb4 needs 4 bytes a character
/// and other key columns take up to 16 bytes each.
const MYSQL_KEY_BYTES: u64 = 3_072;

/// MySQL cannot use TEXT, BLOB or JSON columns as a primary key. A text or
/// binary `_id` becomes `varchar(n)` / `varbinary(n)` within the key limit
/// (child tables key on `_parent_id` plus a 16-byte `_index`, and their
/// `_parent_id` follows); an `_id` of several types cannot be a MySQL key.
fn fit_mysql_key(mapping: &mut DocMapping, warnings: &mut Vec<String>) -> Result<(), String> {
    let budget = MYSQL_KEY_BYTES - 16;
    let id = mapping.tables[0].columns[0].canonical_type.clone();
    let fitted = match id {
        CanonicalType::String { max_chars: None } => CanonicalType::String {
            max_chars: Some(budget / 4),
        },
        CanonicalType::String { max_chars: Some(n) } if n > budget / 4 => CanonicalType::String {
            max_chars: Some(budget / 4),
        },
        CanonicalType::Binary { max_bytes: None } => CanonicalType::Binary {
            max_bytes: Some(budget),
        },
        CanonicalType::Json
        | CanonicalType::Object
        | CanonicalType::Array(_)
        | CanonicalType::Mixed(_) => {
            return Err(
                "`_id` values have several types (or are objects or arrays), and MySQL cannot use a JSON column as the primary key. Copy this collection to PostgreSQL, or give its documents `_id` values of one type".into(),
            );
        }
        _ => return Ok(()),
    };
    let shown = match &fitted {
        CanonicalType::String { max_chars: Some(n) } => format!("varchar({n})"),
        CanonicalType::Binary { max_bytes: Some(n) } => format!("varbinary({n})"),
        _ => String::new(),
    };
    warnings.push(format!(
        "`_id` is the primary key, so it becomes {shown} in MySQL; longer values are rejected"
    ));
    for (i, t) in mapping.tables.iter_mut().enumerate() {
        let key = if i == 0 {
            doc_to_table::ID
        } else {
            doc_to_table::PARENT
        };
        if let Some(c) = t.columns.iter_mut().find(|c| c.name == key) {
            c.canonical_type = fitted.clone();
        }
    }
    Ok(())
}

/// MySQL cannot index TEXT or JSON columns without a prefix length (and a
/// prefix would change what a unique index means). Indexed text columns
/// become `varchar(n)`, with the key's byte budget shared between an
/// index's text columns; indexes on JSON columns are left out.
fn fit_mysql_indexes(
    mut mapping: DocMapping,
    indexes: Vec<IndexDefinition>,
    warnings: &mut Vec<String>,
) -> (DocMapping, Vec<IndexDefinition>) {
    let root = &mut mapping.tables[0];
    let is_text = |t: &CanonicalType| matches!(t, CanonicalType::String { max_chars: None });
    let mut limits: BTreeMap<String, u64> = BTreeMap::new();
    let mut kept = Vec::with_capacity(indexes.len());
    for ix in indexes {
        let cols: Vec<&gazabridge_mapping::doc_to_table::ColumnMap> = ix
            .columns
            .iter()
            .filter_map(|c| root.columns.iter().find(|m| &m.name == c))
            .collect();
        if let Some(c) = cols.iter().find(|m| {
            matches!(
                m.canonical_type,
                CanonicalType::Json
                    | CanonicalType::Object
                    | CanonicalType::Array(_)
                    | CanonicalType::Mixed(_)
            )
        }) {
            warnings.push(format!(
                "index `{}` is left out: MySQL cannot index the JSON column `{}`",
                ix.name, c.name
            ));
            continue;
        }
        let texts = cols.iter().filter(|m| is_text(&m.canonical_type)).count() as u64;
        if texts > 0 {
            let others = cols.len() as u64 - texts;
            let chars = (MYSQL_KEY_BYTES.saturating_sub(16 * others) / (4 * texts)).max(1);
            for m in cols.iter().filter(|m| is_text(&m.canonical_type)) {
                let e = limits.entry(m.name.clone()).or_insert(chars);
                *e = (*e).min(chars);
            }
        }
        kept.push(ix);
    }
    for (name, chars) in limits {
        if let Some(m) = root.columns.iter_mut().find(|m| m.name == name) {
            m.canonical_type = CanonicalType::String {
                max_chars: Some(chars),
            };
            warnings.push(format!(
                "`{name}` is indexed, so it becomes varchar({chars}) in MySQL; longer values are rejected"
            ));
        }
    }
    (mapping, kept)
}

/// Saved mappings (see [`shape`]) for the selected tables, by item index.
async fn saved_shapes(source: &dyn Connection, items: &[CopyItem]) -> BTreeMap<usize, DocMapping> {
    let mut rows: BTreeMap<String, Vec<Record>> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (i, item) in items.iter().enumerate() {
        if item.source.name == shape::SHAPES_TABLE {
            continue;
        }
        let key = format!("{:?}", item.source.namespace);
        if !rows.contains_key(&key) {
            // Most databases have none: a missing table is no error.
            let table = ObjectRef::table(item.source.namespace.clone(), shape::SHAPES_TABLE);
            let saved = read_all(source, ReadRequest::new(table, 1_000))
                .await
                .unwrap_or_default();
            rows.insert(key.clone(), saved);
        }
        if let Some(m) = rows[&key]
            .iter()
            .find_map(|r| shape::mapping_of(r, &item.source.name))
        {
            out.insert(i, m);
        }
    }
    out
}

/// The shape for a root table and the embeds that read its child tables,
/// checked against the live tables. Notes say what changed since.
async fn shape_mapping(
    source: &dyn Connection,
    schema: &ObjectSchema,
    m: &DocMapping,
    mapping: &TableDocMapping,
    options: &InspectOptions,
) -> Result<(shape::Shape, Vec<table_to_doc::EmbedMap>, Vec<String>), String> {
    if mapping.key != [doc_to_table::ID] {
        return Err(format!(
            "its primary key is no longer `{}`",
            doc_to_table::ID
        ));
    }
    let gone = |t: &doc_to_table::TableMap, s: &ObjectSchema| -> Vec<String> {
        t.columns
            .iter()
            .filter(|c| !s.columns.iter().any(|l| l.name == c.name))
            .map(|c| c.name.clone())
            .collect()
    };
    let mut notes = Vec::new();
    let missing = gone(&m.tables[0], schema);
    if !missing.is_empty() {
        notes.push(format!(
            "columns dropped since the copy ({}) leave their fields out",
            missing.join(", ")
        ));
    }
    let mut columns = vec![mapping.columns.clone()];
    let mut embeds = Vec::new();
    for t in &m.tables[1..] {
        let child = ObjectRef::table(schema.object.namespace.clone(), t.target.name.clone());
        let live = source
            .inspect(&child, options)
            .await
            .map_err(|_| format!("its child table `{}` is gone", t.target.name))?;
        let missing = gone(t, &live);
        if missing.iter().any(|c| c == doc_to_table::PARENT) {
            return Err(format!(
                "its child table `{}` no longer has `{}`",
                t.target.name,
                doc_to_table::PARENT
            ));
        }
        if !missing.is_empty() {
            notes.push(format!(
                "columns dropped from `{}` since the copy ({}) leave their fields out",
                t.target.name,
                missing.join(", ")
            ));
        }
        let cols = table_to_doc::Columns::of(&live);
        let has_index = live.columns.iter().any(|c| c.name == doc_to_table::INDEX);
        embeds.push(table_to_doc::EmbedMap {
            child,
            field: t.array.clone().unwrap_or_default(),
            foreign_key: doc_to_table::PARENT.into(),
            parent_key: doc_to_table::ID.into(),
            order_by: if has_index {
                vec![doc_to_table::INDEX.into()]
            } else {
                live.primary_key.clone()
            },
            columns: cols.clone(),
        });
        columns.push(cols);
    }
    Ok((
        shape::Shape {
            mapping: m.clone(),
            columns,
        },
        embeds,
        notes,
    ))
}

/// Plans tables → collections, embedding child tables when asked.
pub async fn plan_table_to_doc(
    req: &MigrationRequest,
    source_conn: &Arc<dyn Connection>,
    target: &dyn Connection,
) -> AppResult<Planned> {
    let source = source_conn.as_ref();
    let mut out = Planned {
        items: Vec::new(),
        mappings: Vec::new(),
        blockers: Vec::new(),
        destructive: false,
    };
    let options = InspectOptions {
        include_indexes: true,
        include_constraints: true,
        sample_size: 0,
        sampling: SamplingMethod::First,
    };
    let mut schemas: Vec<ObjectSchema> = Vec::with_capacity(req.items.len());
    for item in &req.items {
        schemas.push(source.inspect(&item.source, &options).await?);
    }
    // Tables made from documents by flattening or normalizing: their saved
    // mappings, and which selected tables are their child tables.
    let shaped = saved_shapes(source, &req.items).await;
    let mut shape_child: BTreeMap<usize, (usize, String)> = BTreeMap::new();
    for (root, m) in &shaped {
        for t in &m.tables[1..] {
            if let Some(j) = req.items.iter().position(|it| {
                it.source.namespace == req.items[*root].source.namespace
                    && it.source.name == t.target.name
            }) {
                shape_child.insert(j, (*root, t.array.clone().unwrap_or_default()));
            }
        }
    }
    // One level of embedding: a child goes into the first selected parent it
    // references, unless it is a parent itself.
    let mut embedded_into: BTreeMap<usize, (usize, table_to_doc::EmbedMap)> = BTreeMap::new();
    let mut notes: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    if req.table_mode == TableMode::Embed {
        let links: Vec<(usize, usize, table_to_doc::EmbedMap)> = (0..schemas.len())
            .flat_map(|p| (0..schemas.len()).map(move |c| (p, c)))
            .filter(|(p, c)| p != c)
            .filter_map(|(p, c)| table_to_doc::embed(&schemas[p], &schemas[c]).map(|e| (p, c, e)))
            .collect();
        let parents: BTreeSet<usize> = links.iter().map(|(p, _, _)| *p).collect();
        for (p, c, e) in links {
            if parents.contains(&c) {
                notes.entry(c).or_default().push(format!(
                    "has its own child tables, so it stays a collection that references `{}`",
                    schemas[p].object.name
                ));
                continue;
            }
            if parents.contains(&p) && embedded_into.contains_key(&p) {
                continue;
            }
            embedded_into.entry(c).or_insert((p, e));
        }
    }
    // A saved shape decides where its own child tables go.
    embedded_into.retain(|c, (p, _)| !shape_child.contains_key(c) && !shaped.contains_key(p));
    let mut existing = Existing::default();
    for (i, item) in req.items.iter().enumerate() {
        let schema = &schemas[i];
        let mut p = empty_item(item, "table_to_doc");
        p.warnings.extend(notes.remove(&i).unwrap_or_default());
        p.source_count = quick_count(source, &item.source).await;
        if matches!(
            item.source.kind,
            ObjectKind::View | ObjectKind::MaterializedView
        ) {
            out.blockers.push(format!(
                "`{}` is a view; copy its tables instead",
                item.source
            ));
        }
        if item.source.name == shape::SHAPES_TABLE {
            p.warnings.push(
                "holds the saved mappings GazaBridge uses to rebuild documents; it is not needed in MongoDB"
                    .into(),
            );
        }
        if let Some((root, field)) = shape_child.get(&i) {
            p.kind = "embedded".into();
            p.embedded_into = Some(req.items[*root].target.clone());
            p.warnings.push(format!(
                "rebuilt as the `{field}` array of `{}` documents, as it was before",
                req.items[*root].target
            ));
            out.items.push(p);
            out.mappings.push(ItemMapping::Embedded {
                into: req.items[*root].target.clone(),
            });
            continue;
        }
        if let Some((parent, _)) = embedded_into.get(&i) {
            p.kind = "embedded".into();
            p.embedded_into = Some(req.items[*parent].target.clone());
            p.warnings.push(format!(
                "embedded as an array in `{}` documents",
                req.items[*parent].target
            ));
            out.items.push(p);
            out.mappings.push(ItemMapping::Embedded {
                into: req.items[*parent].target.clone(),
            });
            continue;
        }
        let mut mapping = match table_to_doc::plan(schema, item.target.clone()) {
            Ok(m) => m,
            Err(e) => {
                out.blockers.push(e);
                out.items.push(p);
                out.mappings.push(ItemMapping::Copy);
                continue;
            }
        };
        for (c, (parent, e)) in &embedded_into {
            if *parent == i {
                p.warnings.push(format!(
                    "rows of `{}` are embedded as `{}`",
                    schemas[*c].object.name, e.field
                ));
                mapping.embeds.push(e.clone());
            }
        }
        if let Some(m) = shaped.get(&i) {
            match shape_mapping(source, schema, m, &mapping, &options).await {
                Ok((shape, embeds, notes)) => {
                    p.warnings.push(format!(
                        "documents are rebuilt in their original shape, from the mapping saved when `{}` was made from MongoDB",
                        item.source.name
                    ));
                    p.warnings.extend(notes);
                    mapping.embeds = embeds;
                    mapping.shape = Some(shape);
                }
                Err(why) => p.warnings.push(format!(
                    "a mapping was saved when this table was made from MongoDB, but {why}; rows are copied as flat documents"
                )),
            }
        }
        p.warnings.extend(mapping.warnings.iter().cloned());
        let indexes = if req.copy_indexes {
            table_to_doc::indexes(schema, &mapping)
        } else {
            Vec::new()
        };
        p.indexes = indexes.iter().map(|ix| ix.name.clone()).collect();
        p.target_exists = existing.contains(target, &item.target).await;
        if p.target_exists {
            p.target_count = quick_count(target, &item.target).await;
            if req.existing == ExistingData::Replace {
                out.destructive = true;
                p.warnings
                    .push("the target collection and its documents will be dropped first".into());
            }
        }
        let m = ItemMapping::TableToDoc {
            mapping: Box::new(mapping),
            indexes,
        };
        let mut ops = m.prepare_ops(item, req.existing);
        ops.extend(m.index_ops(item));
        match target.plan_ddl(&ops) {
            Ok(d) => p.statements = d.statements,
            Err(e) => out.blockers.push(format!("{}: {e}", item.target)),
        }
        if let Some(t) = m.transform(req.existing, source_conn.clone()) {
            let rows = sample(source, &item.source).await;
            if let Ok(converted) = t.apply(&rows).await {
                p.documents = converted
                    .rows
                    .first()
                    .map(|r| r.iter().map(|(_, d)| pretty(d)).collect())
                    .unwrap_or_default();
            }
        }
        out.items.push(p);
        out.mappings.push(m);
    }
    Ok(out)
}
