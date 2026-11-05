//! Table → table copies between relational engines (PostgreSQL and MySQL,
//! in any direction, including the same engine).
//!
//! The target table is created from the source's canonical column types,
//! so each engine picks its own native type (`jsonb` ⇄ `json`,
//! `timestamptz` ⇄ `datetime`, `uuid` ⇄ `char(36)`…). Columns whose type
//! the target cannot hold are copied as text, with a warning. The primary
//! key is required (it is how resumed, skipped and upserted rows are found);
//! simple indexes are recreated, foreign keys are not (tables are copied in
//! parallel, in any order).

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_domain::conversion::scalar_to_text;
use gazabridge_domain::{CanonicalType, Record, Value};
use gazabridge_mapping::doc_to_table::TablePreview;
use gazabridge_migration::{TargetSpec, Transform, Transformed};
use gazabridge_plugin_sdk::ConnectorError;
use gazabridge_plugin_sdk::connector::{Connection, InspectOptions};
use gazabridge_plugin_sdk::ddl::{ColumnDefinition, IndexDefinition, TableDefinition};
use gazabridge_plugin_sdk::schema::{ObjectSchema, SamplingMethod};
use serde::{Deserialize, Serialize};

use crate::AppResult;
use crate::convert::{Existing, ItemMapping, Planned, empty_item, quick_count, sample};
use crate::migration::{ExistingData, MigrationRequest};

/// InnoDB keys hold at most 3,072 bytes; utf8mb4 needs 4 bytes a character
/// and other key columns take up to 16 bytes each.
const MYSQL_KEY_BYTES: u64 = 3_072;

/// How one table is copied; stored with the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableCopyMapping {
    pub table: TableDefinition,
    pub indexes: Vec<IndexDefinition>,
    /// Columns written as text because the target has no matching type.
    #[serde(default)]
    pub text_columns: BTreeSet<String>,
}

pub fn is_relational(f: EngineFamily) -> bool {
    // SQLite would plan the same way, but has no contract evidence for it.
    matches!(f, EngineFamily::Postgres | EngineFamily::MySql)
}

/// MySQL cannot index TEXT columns without a prefix length (a prefix would
/// change what a unique key means): indexed text becomes `varchar(n)`, the
/// key's byte budget shared between its text columns. Returns warnings.
pub fn fit_mysql_keys(columns: &mut [ColumnDefinition], keys: &[Vec<String>]) -> Vec<String> {
    let is_text = |t: &CanonicalType| matches!(t, CanonicalType::String { max_chars: None });
    let mut limits: BTreeMap<String, u64> = BTreeMap::new();
    for key in keys {
        let cols: Vec<&ColumnDefinition> = key
            .iter()
            .filter_map(|k| columns.iter().find(|c| &c.name == k))
            .collect();
        let texts = cols.iter().filter(|c| is_text(&c.canonical_type)).count() as u64;
        if texts == 0 {
            continue;
        }
        let others = cols.len() as u64 - texts;
        let chars = (MYSQL_KEY_BYTES.saturating_sub(16 * others) / (4 * texts)).max(1);
        for c in cols.iter().filter(|c| is_text(&c.canonical_type)) {
            let e = limits.entry(c.name.clone()).or_insert(chars);
            *e = (*e).min(chars);
        }
    }
    let mut warnings = Vec::new();
    for (name, chars) in limits {
        if let Some(c) = columns.iter_mut().find(|c| c.name == name) {
            c.canonical_type = CanonicalType::String {
                max_chars: Some(chars),
            };
            warnings.push(format!(
                "`{name}` is part of a key, so it becomes varchar({chars}) in MySQL; longer values are rejected"
            ));
        }
    }
    warnings
}

/// `char(n)` stays fixed-length (both engines spell it the same): it is how
/// ObjectIds are recognised when the table becomes documents again.
fn fixed_chars(native: &str, t: &CanonicalType) -> Option<String> {
    let n = native.to_ascii_lowercase();
    let inner = ["character(", "char(", "bpchar("]
        .iter()
        .find_map(|p| n.strip_prefix(p))?
        .strip_suffix(')')?;
    let len: u64 = inner.trim().parse().ok()?;
    (matches!(t, CanonicalType::String { max_chars: Some(m) } if *m == len) && len <= 255)
        .then(|| format!("char({len})"))
}

/// Plans one table copy from the source's schema. `Err` is a blocker.
pub fn plan_table(
    schema: &ObjectSchema,
    item: &crate::migration::CopyItem,
    target: &dyn Connection,
) -> Result<(TableCopyMapping, Vec<String>), String> {
    if schema.primary_key.is_empty() {
        return Err(format!(
            "`{}` has no primary key; copies need one to resume and to find existing rows",
            schema.object
        ));
    }
    let mut warnings = Vec::new();
    let mut text_columns = BTreeSet::new();
    let mut columns: Vec<ColumnDefinition> = schema
        .columns
        .iter()
        .filter(|c| !c.is_generated)
        .map(|c| {
            let mut t = c.canonical_type.clone();
            if target.native_type_for(&t).is_err() {
                warnings.push(format!(
                    "`{}` ({}) has no equivalent in the target; it is copied as text",
                    c.name, c.native_type
                ));
                text_columns.insert(c.name.clone());
                t = CanonicalType::TEXT;
            }
            ColumnDefinition {
                name: c.name.clone(),
                native_type: fixed_chars(&c.native_type, &t),
                canonical_type: t,
                nullable: c.nullable && !schema.primary_key.contains(&c.name),
                default_expression: None,
            }
        })
        .collect();
    let generated: Vec<&str> = schema
        .columns
        .iter()
        .filter(|c| c.is_generated)
        .map(|c| c.name.as_str())
        .collect();
    if !generated.is_empty() {
        warnings.push(format!(
            "generated column(s) {} are left out",
            generated.join(", ")
        ));
    }
    let target_family = target.manifest().family;
    let mut indexes = Vec::new();
    for ix in schema.indexes.iter().filter(|i| !i.primary) {
        if ix.predicate.is_some() {
            warnings.push(format!("partial index `{}` is not copied", ix.name));
            continue;
        }
        if ix
            .columns
            .iter()
            .any(|c| !columns.iter().any(|d| &d.name == c))
        {
            warnings.push(format!(
                "index `{}` uses expressions or left-out columns and is not copied",
                ix.name
            ));
            continue;
        }
        if target_family == EngineFamily::MySql
            && ix.columns.iter().any(|c| {
                columns.iter().any(|d| {
                    &d.name == c
                        && matches!(
                            d.canonical_type,
                            CanonicalType::Json
                                | CanonicalType::Object
                                | CanonicalType::Array(_)
                                | CanonicalType::Map { .. }
                                | CanonicalType::BYTES
                        )
                })
            })
        {
            warnings.push(format!(
                "index `{}` is not copied: MySQL cannot index JSON or BLOB columns",
                ix.name
            ));
            continue;
        }
        // PostgreSQL index names are unique per schema, MySQL's per table.
        let name = if ix.name.starts_with(&item.target.name) {
            ix.name.clone()
        } else {
            format!("{}_{}", item.target.name, ix.name)
        };
        indexes.push(IndexDefinition {
            name,
            columns: ix.columns.clone(),
            unique: ix.unique,
            native: None,
        });
    }
    if target_family == EngineFamily::MySql
        && columns.iter().any(|c| {
            matches!(
                c.canonical_type,
                CanonicalType::Json | CanonicalType::Object
            )
        })
    {
        warnings.push(
            "MySQL rounds numbers with more than 15 significant digits inside JSON by a unit or two in the last place".into(),
        );
    }
    if target_family == EngineFamily::MySql {
        let mut keys = vec![schema.primary_key.clone()];
        keys.extend(indexes.iter().map(|i| i.columns.clone()));
        warnings.extend(fit_mysql_keys(&mut columns, &keys));
    }
    if schema.constraints.iter().any(|c| {
        matches!(
            c.kind,
            gazabridge_plugin_sdk::schema::ConstraintKind::ForeignKey { .. }
        )
    }) {
        warnings.push("foreign keys are not copied (tables are copied in parallel)".into());
    }
    Ok((
        TableCopyMapping {
            table: TableDefinition {
                object: item.target.clone(),
                columns,
                primary_key: schema.primary_key.clone(),
                foreign_keys: Vec::new(),
            },
            indexes,
            text_columns,
        },
        warnings,
    ))
}

fn cell(v: &Value) -> String {
    match v {
        Value::Null | Value::Missing => "NULL".into(),
        other => {
            scalar_to_text(other).unwrap_or_else(|| gazabridge_mapping::ext_json(other).to_string())
        }
    }
}

/// Plans tables → tables.
pub async fn plan(
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
    let options = InspectOptions {
        include_indexes: true,
        include_constraints: true,
        sample_size: 0,
        sampling: SamplingMethod::First,
    };
    for item in &req.items {
        let mut p = empty_item(item, "table_copy");
        let schema = source.inspect(&item.source, &options).await?;
        let (mapping, warnings) = match plan_table(&schema, item, target) {
            Ok(m) => m,
            Err(b) => {
                out.blockers.push(b);
                out.items.push(p);
                out.mappings.push(ItemMapping::Embedded {
                    into: item.target.clone(),
                });
                continue;
            }
        };
        p.warnings = warnings;
        p.source_count = quick_count(source, &item.source).await;
        if existing.contains(target, &item.target).await {
            p.target_exists = true;
            p.target_count = quick_count(target, &item.target).await;
            if req.existing == ExistingData::Replace {
                out.destructive = true;
                p.warnings.push(format!(
                    "table `{}` and its rows will be dropped first",
                    item.target.name
                ));
            } else {
                let have: BTreeSet<String> = target
                    .inspect(&item.target, &InspectOptions::default())
                    .await
                    .map(|s| s.columns.into_iter().map(|c| c.name).collect())
                    .unwrap_or_default();
                let missing: Vec<&str> = mapping
                    .table
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .filter(|c| !have.contains(*c))
                    .collect();
                if !missing.is_empty() {
                    out.blockers.push(format!(
                        "table `{}` already exists without the column(s) {}; choose \"Delete the target first\" or another table name",
                        item.target.name,
                        missing.join(", ")
                    ));
                }
            }
        }
        p.indexes = mapping.indexes.iter().map(|i| i.name.clone()).collect();
        let m = ItemMapping::TableCopy(Box::new(mapping));
        let mut ops = m.prepare_ops(item, req.existing);
        ops.extend(m.index_ops(item));
        match target.plan_ddl(&ops) {
            Ok(d) => p.statements = d.statements,
            Err(e) => out.blockers.push(format!("{}: {e}", item.target)),
        }
        if let ItemMapping::TableCopy(t) = &m {
            let rows = sample(source, &item.source).await;
            p.tables = vec![TablePreview {
                table: item.target.clone(),
                columns: t.table.columns.iter().map(|c| c.name.clone()).collect(),
                types: t
                    .table
                    .columns
                    .iter()
                    .map(|c| {
                        c.native_type.clone().unwrap_or_else(|| {
                            target
                                .native_type_for(&c.canonical_type)
                                .unwrap_or_else(|_| "?".into())
                        })
                    })
                    .collect(),
                sources: t.table.columns.iter().map(|c| c.name.clone()).collect(),
                rows: rows
                    .iter()
                    .map(|r| {
                        t.table
                            .columns
                            .iter()
                            .map(|c| cell(r.get(&c.name)))
                            .collect()
                    })
                    .collect(),
            }];
        }
        out.items.push(p);
        out.mappings.push(m);
    }
    Ok(out)
}

/// Rows → rows: only columns without a target type change (to text).
#[derive(Debug)]
pub struct TableCopy {
    pub mapping: TableCopyMapping,
}

#[async_trait]
impl Transform for TableCopy {
    fn targets(&self) -> Vec<TargetSpec> {
        vec![TargetSpec {
            object: self.mapping.table.object.clone(),
            key_fields: self.mapping.table.primary_key.clone(),
            owner: None,
            replace: false,
        }]
    }

    async fn apply(&self, records: &[Record]) -> Result<Transformed, ConnectorError> {
        let columns: BTreeSet<&str> = self
            .mapping
            .table
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let rows = records
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let mut row = Record::with_capacity(columns.len());
                for (k, v) in &r.fields {
                    if !columns.contains(k.as_str()) {
                        continue;
                    }
                    let v = &v.value;
                    let v = if self.mapping.text_columns.contains(k) && !v.is_absent() {
                        Value::String(cell(v))
                    } else {
                        v.clone()
                    };
                    row.insert(k.clone(), v);
                }
                (i, row)
            })
            .collect();
        Ok(Transformed {
            rows: vec![rows],
            rejected: Vec::new(),
            owners: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: CanonicalType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            canonical_type: t,
            native_type: None,
            nullable: true,
            default_expression: None,
        }
    }

    #[test]
    fn fixed_length_text_stays_fixed() {
        let t = CanonicalType::String {
            max_chars: Some(24),
        };
        assert_eq!(
            fixed_chars("character(24)", &t).as_deref(),
            Some("char(24)")
        );
        assert_eq!(fixed_chars("char(24)", &t).as_deref(), Some("char(24)"));
        assert_eq!(fixed_chars("character varying(24)", &t), None);
        assert_eq!(fixed_chars("varchar(24)", &t), None);
        assert_eq!(fixed_chars("character(24)", &CanonicalType::TEXT), None);
    }

    #[test]
    fn mysql_keys_share_the_byte_budget() {
        let mut cols = vec![
            col("a", CanonicalType::TEXT),
            col("b", CanonicalType::TEXT),
            col("n", CanonicalType::I64),
            col(
                "c",
                CanonicalType::String {
                    max_chars: Some(20),
                },
            ),
        ];
        let w = fit_mysql_keys(
            &mut cols,
            &[vec!["a".into()], vec!["a".into(), "b".into(), "n".into()]],
        );
        assert_eq!(
            cols[0].canonical_type,
            CanonicalType::String {
                max_chars: Some(382)
            }
        );
        assert_eq!(
            cols[1].canonical_type,
            CanonicalType::String {
                max_chars: Some(382)
            }
        );
        assert_eq!(
            cols[3].canonical_type,
            CanonicalType::String {
                max_chars: Some(20)
            }
        );
        assert_eq!(w.len(), 2);
    }
}
