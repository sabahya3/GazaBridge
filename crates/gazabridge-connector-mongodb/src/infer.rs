//! Schema inference for collections.
//!
//! Every sampled document contributes to per-path statistics: how often the
//! path is present, how often it is null, the distribution of value kinds,
//! element kinds for arrays, and a unified canonical type. Paths seen both
//! as objects and as other kinds are reported as conflicting. Results are
//! always marked as inferred; the first document is never taken as the
//! schema.

use std::collections::{BTreeMap, BTreeSet};

use gazabridge_domain::value::ValueKind;
use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::schema::{InferredField, InferredStructure, SamplingMethod};
use indexmap::IndexMap;
use mongodb::bson::{Bson, Document};

use crate::convert::from_bson;

#[derive(Debug, Default)]
struct Stats {
    present: u64,
    nulls: u64,
    kinds: BTreeMap<ValueKind, u64>,
    items: BTreeMap<ValueKind, u64>,
    canonical: Option<CanonicalType>,
}

#[derive(Debug, Default)]
pub struct Inferrer {
    sampled: u64,
    max_depth: u32,
    fields: BTreeMap<String, Stats>,
    order: Vec<String>,
}

fn shallow_type(v: &Value) -> CanonicalType {
    match v {
        // Containers are described by their own child paths.
        Value::Object(_) => CanonicalType::Object,
        Value::Array(items) => CanonicalType::Array(Box::new(CanonicalType::unify_all(
            items.iter().map(shallow_type),
        ))),
        other => other.canonical_type(),
    }
}

impl Inferrer {
    pub fn new() -> Self {
        Self::default()
    }

    fn stats(&mut self, path: &str) -> &mut Stats {
        if !self.fields.contains_key(path) {
            self.order.push(path.to_owned());
        }
        self.fields.entry(path.to_owned()).or_default()
    }

    fn visit(&mut self, path: &str, b: &Bson, depth: u32) {
        self.max_depth = self.max_depth.max(depth);
        let value = from_bson(b);
        let kind = value.kind();
        {
            let s = self.stats(path);
            // Inside arrays a path can occur several times per document;
            // presence is counted per occurrence there.
            s.present += 1;
            if kind == ValueKind::Null {
                s.nulls += 1;
            }
            *s.kinds.entry(kind).or_default() += 1;
            let t = shallow_type(&value);
            s.canonical = Some(match s.canonical.take() {
                Some(prev) => prev.unify(t),
                None => t,
            });
        }
        match b {
            Bson::Document(d) => {
                for (k, v) in d {
                    self.visit(&format!("{path}.{k}"), v, depth + 1);
                }
            }
            Bson::Array(items) => {
                for item in items {
                    let item_kind = from_bson(item).kind();
                    *self.stats(path).items.entry(item_kind).or_default() += 1;
                    if let Bson::Document(d) = item {
                        for (k, v) in d {
                            self.visit(&format!("{path}[].{k}"), v, depth + 1);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub fn add(&mut self, doc: &Document) {
        self.sampled += 1;
        for (k, v) in doc {
            self.visit(k, v, 1);
        }
    }

    pub fn finish(self, sampling: SamplingMethod) -> InferredStructure {
        let mut conflicting = BTreeSet::new();
        let mut fields = Vec::with_capacity(self.order.len());
        for path in &self.order {
            let Some(s) = self.fields.get(path) else {
                continue;
            };
            let object = s.kinds.contains_key(&ValueKind::Object);
            let other = s
                .kinds
                .keys()
                .any(|k| !matches!(k, ValueKind::Object | ValueKind::Null | ValueKind::Missing));
            if object && other {
                conflicting.insert(path.clone());
            }
            fields.push(InferredField {
                path: path.clone(),
                present_count: s.present,
                null_count: s.nulls,
                type_counts: s
                    .kinds
                    .iter()
                    .map(|(k, v)| (*k, *v))
                    .collect::<IndexMap<_, _>>(),
                canonical_type: s.canonical.clone().unwrap_or(CanonicalType::Null),
                array_item_type_counts: s.items.iter().map(|(k, v)| (*k, *v)).collect(),
            });
        }
        InferredStructure {
            sampled_documents: self.sampled,
            sampling,
            max_depth: self.max_depth,
            fields,
            conflicting_paths: conflicting.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson::doc;

    use super::*;

    #[test]
    fn statistics_cover_optional_mixed_nested_and_array_fields() {
        let docs = [
            doc! { "_id": 1, "name": "a", "tags": ["x", 1], "addr": { "city": "Gaza" }, "items": [ { "sku": "s1" } ] },
            doc! { "_id": 2, "name": null, "addr": "unknown", "items": [ { "sku": "s2", "qty": 3 }, { "sku": 4 } ] },
            doc! { "_id": 3, "name": 5 },
        ];
        let mut inf = Inferrer::new();
        for d in &docs {
            inf.add(d);
        }
        let s = inf.finish(SamplingMethod::FullScan);
        assert_eq!(s.sampled_documents, 3);
        let get = |p: &str| s.fields.iter().find(|f| f.path == p).unwrap();

        let name = get("name");
        assert_eq!((name.present_count, name.null_count), (3, 1));
        assert!(name.is_mixed(), "string and int");
        assert!((name.occurrence_ratio(3) - 1.0).abs() < f64::EPSILON);

        let tags = get("tags");
        assert_eq!(tags.present_count, 1);
        assert_eq!(tags.array_item_type_counts.len(), 2, "mixed array items");

        assert_eq!(s.conflicting_paths, vec!["addr".to_string()]);
        assert_eq!(get("addr.city").present_count, 1);
        assert_eq!(get("items[].sku").present_count, 3);
        assert!(get("items[].sku").is_mixed());
        assert_eq!(s.max_depth, 2);
    }
}
