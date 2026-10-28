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
}

pub fn direction(source: EngineFamily, target: EngineFamily) -> Option<Direction> {
    match (source, target) {
        (EngineFamily::MongoDb, EngineFamily::MongoDb) => Some(Direction::Copy),
        (EngineFamily::MongoDb, EngineFamily::Postgres) => Some(Direction::DocToTable),
        (EngineFamily::Postgres, EngineFamily::MongoDb) => Some(Direction::TableToDoc),
        _ => None,
    }
}

/// What one stream does, decided when the run starts and stored with it so
/// a resumed run converts exactly like the first attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ItemMapping {
    Copy,
    DocToTable {
        mapping: Box<DocMapping>,
        indexes: Vec<IndexDefinition>,
    },
    TableToDoc {
        mapping: Box<TableDocMapping>,
        indexes: Vec<IndexDefinition>,
    },
    /// Rows embedded in another stream's documents; not copied on their own.
    Embedded {
        into: ObjectRef,
    },
}

impl ItemMapping {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::DocToTable { .. } => "doc_to_table",
            Self::TableToDoc { .. } => "table_to_doc",
            Self::Embedded { .. } => "embedded",
        }
    }

    /// Target preparation: drop (when replacing) and create.
    pub fn prepare_ops(&self, item: &CopyItem, existing: ExistingData) -> Vec<SchemaOperation> {
        let replace = existing == ExistingData::Replace;
        match self {
            Self::Copy | Self::TableToDoc { .. } => {
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
                if item
                    .target
                    .namespace
                    .schema
                    .as_deref()
                    .is_some_and(|s| s != "public")
                {
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
                ops
            }
            Self::Embedded { .. } => Vec::new(),
        }
    }

    /// Indexes created after the data (conversions; copies read them live).
    pub fn index_ops(&self, item: &CopyItem) -> Vec<SchemaOperation> {
        let (object, indexes) = match self {
            Self::DocToTable { mapping, indexes } => (mapping.tables[0].target.clone(), indexes),
            Self::TableToDoc { indexes, .. } => (item.target.clone(), indexes),
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
            _ => vec!["_id".into()],
        }
    }

    pub fn transform(
        &self,
        existing: ExistingData,
        source: Arc<dyn Connection>,
    ) -> Option<Arc<dyn Transform>> {
        match self {
            Self::DocToTable { mapping, .. } => Some(Arc::new(DocToTable {
                mapping: mapping.as_ref().clone(),
                replace_children: existing == ExistingData::Update,
            })),
            Self::TableToDoc { mapping, .. } => Some(Arc::new(TableToDoc {
                mapping: mapping.as_ref().clone(),
                source,
            })),
            Self::Copy | Self::Embedded { .. } => None,
        }
    }
}

/// Documents → rows of the mapped tables.
struct DocToTable {
    mapping: DocMapping,
    replace_children: bool,
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
                    for (t, rows) in tables.into_iter().enumerate() {
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

async fn sample(conn: &dyn Connection, object: &ObjectRef) -> Vec<Record> {
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

fn empty_item(item: &CopyItem, kind: &str) -> PlanItem {
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
