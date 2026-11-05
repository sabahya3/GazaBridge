//! Comparing a source with its target, and probing a target for a dry run.
//!
//! Source records go through the same transform as the copy, so validation
//! checks what the target *should* hold. Target rows are looked up by key
//! (`IN` on the key, or on the owner column for child tables), which works
//! whatever order each engine sorts keys in.
//!
//! Levels: **basic** compares counts; **standard** checks that every
//! expected row exists and nothing extra hangs off the same owners;
//! **deep** also compares content, chunk by chunk, and names the first
//! differing field of each mismatch.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use gazabridge_domain::conversion::scalar_to_text;
use gazabridge_domain::{Record, Value};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter};
use gazabridge_plugin_sdk::read::ReadRequest;
use gazabridge_plugin_sdk::schema::ObjectRef;
use gazabridge_plugin_sdk::{Connection, ConnectorError, ErrorKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::transform::{TargetSpec, Transform};

/// Differences kept per stream; the counters keep counting.
const MAX_DIFFERENCES: usize = 50;
/// Keys per `IN` lookup.
const LOOKUP_CHUNK: usize = 1_000;
/// Bound for exact counts.
const COUNT_TIME: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    #[default]
    Basic,
    Standard,
    Deep,
}

/// How content is compared at the deep level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparison {
    /// Byte-identical BSON (same engine copies): field order and types count.
    Exact,
    /// Identical BSON values and types, field order aside (MongoDB time
    /// series collections rebuild documents from buckets on every read).
    ExactUnordered,
    /// Same values once normalized across engines (number forms, key order
    /// in JSON, ObjectId as hexadecimal text).
    Normalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    /// Expected in the target but absent (or not convertible).
    Missing,
    /// In the target, owned by a source record, but not expected.
    Extra,
    /// Present with other content.
    Different,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    pub target: String,
    pub key: String,
    pub kind: DiffKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Count {
    pub count: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetCheck {
    pub target: ObjectRef,
    pub expected: u64,
    pub matched: u64,
    pub missing: u64,
    pub extra: u64,
    pub different: u64,
    /// Lookup chunks compared, and those with any difference.
    pub chunks: u64,
    pub mismatched_chunks: u64,
    /// SHA-256 over every expected row hash, in source order (deep only);
    /// equal to `actual_checksum` when the target matches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_checksum: Option<String>,
}

impl TargetCheck {
    fn new(target: ObjectRef) -> Self {
        Self {
            target,
            expected: 0,
            matched: 0,
            missing: 0,
            extra: 0,
            different: 0,
            chunks: 0,
            mismatched_chunks: 0,
            expected_checksum: None,
            actual_checksum: None,
        }
    }

    fn clean(&self) -> bool {
        self.missing == 0 && self.extra == 0 && self.different == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamValidation {
    pub level: Level,
    pub source: ObjectRef,
    pub source_count: Option<Count>,
    pub target_count: Option<Count>,
    /// Source records compared so far.
    pub checked: u64,
    pub targets: Vec<TargetCheck>,
    pub differences: Vec<Difference>,
    pub passed: bool,
    /// Why the result is partial (stopped, counts not exact…).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// An exact count when the server answers within a minute, else an estimate.
pub async fn count(conn: &dyn Connection, object: &ObjectRef) -> Option<Count> {
    match conn.count_within(object, None, COUNT_TIME).await {
        Ok(c) => Some(Count {
            count: c.count,
            exact: c.exact,
        }),
        Err(e) if e.kind() == ErrorKind::NotFound => Some(Count {
            count: 0,
            exact: true,
        }),
        Err(_) => conn.count(object, None, false).await.ok().map(|c| Count {
            count: c.count,
            exact: false,
        }),
    }
}

/// A JSON object with keys sorted at every level.
fn sorted(j: &serde_json::Value) -> serde_json::Value {
    match j {
        serde_json::Value::Object(m) => {
            let ordered: BTreeMap<&String, serde_json::Value> =
                m.iter().map(|(k, v)| (k, sorted(v))).collect();
            serde_json::Value::Object(ordered.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

fn number(text: &str) -> serde_json::Value {
    // Normalized decimal text: 5, 5.0 and 5.00 compare equal.
    let t = if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text.to_owned()
    };
    serde_json::Value::String(format!(
        "n:{}",
        if t.is_empty() || t == "-" {
            "0".into()
        } else {
            t
        }
    ))
}

/// A value in a form comparable across engines.
pub fn normalize(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null | Value::Missing => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => number(&i.to_string()),
        Value::UInt(u) => number(&u.to_string()),
        Value::BigInt(b) => number(&b.to_string()),
        Value::Decimal(d) => number(&d.to_plain_string()),
        Value::Float(f) if f.fract() == 0.0 && f.abs() < 1e15 => number(&format!("{f:.0}")),
        Value::Float(f) => J::String(format!("f:{f:?}")),
        Value::String(s) => J::String(s.clone()),
        Value::ObjectId(b) => J::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        Value::UtcDateTime(t) => J::String(format!("t:{}", t.timestamp_micros())),
        Value::Binary(b) => J::String(format!(
            "b:{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        )),
        Value::Json(j) => sorted(j),
        Value::Array(items) => J::Array(items.iter().map(normalize).collect()),
        Value::Object(m) => {
            let ordered: BTreeMap<&String, J> = m.iter().map(|(k, v)| (k, normalize(v))).collect();
            J::Object(ordered.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        Value::Map(pairs) => J::Array(
            pairs
                .iter()
                .map(|(k, v)| J::Array(vec![normalize(k), normalize(v)]))
                .collect(),
        ),
        Value::Extension {
            type_name,
            text,
            binary,
        } => J::String(format!(
            "x:{type_name}:{}",
            binary
                .as_ref()
                .map(|b| b.iter().map(|x| format!("{x:02x}")).collect::<String>())
                .or_else(|| text.clone())
                .unwrap_or_default()
        )),
        other => J::String(format!(
            "{}:{}",
            other.kind().as_str(),
            scalar_to_text(other).unwrap_or_default()
        )),
    }
}

fn normalized(r: &Record) -> BTreeMap<String, serde_json::Value> {
    r.fields
        .iter()
        .filter(|(_, v)| !matches!(v.value, Value::Missing))
        .map(|(k, v)| (k.clone(), normalize(&v.value)))
        .collect()
}

fn short(j: &serde_json::Value) -> String {
    let s = j.to_string();
    if s.chars().count() > 80 {
        format!("{}…", s.chars().take(80).collect::<String>())
    } else {
        s
    }
}

/// BSON bytes of a record, with keys sorted at every level when `sorted`.
fn bson_bytes(r: &Record, sorted: bool) -> Option<Vec<u8>> {
    fn sort(b: gazabridge_bson::bson::Bson) -> gazabridge_bson::bson::Bson {
        use gazabridge_bson::bson::{Bson, Document};
        match b {
            Bson::Document(d) => {
                let mut pairs: Vec<(String, Bson)> =
                    d.into_iter().map(|(k, v)| (k, sort(v))).collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                Bson::Document(pairs.into_iter().collect::<Document>())
            }
            Bson::Array(a) => Bson::Array(a.into_iter().map(sort).collect()),
            other => other,
        }
    }
    let d = gazabridge_bson::record_to_document(r).ok()?;
    let d = if sorted {
        match sort(gazabridge_bson::bson::Bson::Document(d)) {
            gazabridge_bson::bson::Bson::Document(d) => d,
            _ => return None,
        }
    } else {
        d
    };
    gazabridge_bson::bson::to_vec(&d).ok()
}

/// `None` when equal, else the first differing field.
fn compare(expected: &Record, actual: &Record, how: Comparison) -> Option<String> {
    if how == Comparison::ExactUnordered {
        let e = bson_bytes(expected, true);
        if e.is_some() && e == bson_bytes(actual, true) {
            return None;
        }
        return compare(expected, actual, Comparison::Exact)
            .or_else(|| Some("BSON types differ".into()));
    }
    if how == Comparison::Exact {
        let bytes = |r: &Record| {
            gazabridge_bson::record_to_document(r)
                .ok()
                .and_then(|d| gazabridge_bson::bson::to_vec(&d).ok())
        };
        if bytes(expected).is_some() && bytes(expected) == bytes(actual) {
            return None;
        }
        let order = |r: &Record| r.fields.keys().cloned().collect::<Vec<_>>();
        if normalized(expected) == normalized(actual) && order(expected) != order(actual) {
            return Some("same values, different field order".into());
        }
    }
    let (e, a) = (normalized(expected), normalized(actual));
    if e == a {
        // Exact comparison found a type difference the normal form hides.
        return (how == Comparison::Exact).then(|| {
            expected
                .fields
                .iter()
                .find(|(k, v)| {
                    actual.get(k).kind() != v.value.kind() || {
                        let (x, y) = (
                            gazabridge_bson::to_bson(
                                &v.value,
                                v.meta.as_ref().and_then(|m| m.original_type.as_deref()),
                            ),
                            actual.get_canonical(k).map(|c| {
                                gazabridge_bson::to_bson(
                                    &c.value,
                                    c.meta.as_ref().and_then(|m| m.original_type.as_deref()),
                                )
                            }),
                        );
                        !matches!((x, y), (Ok(a), Some(Ok(b))) if a == b)
                    }
                })
                .map_or_else(
                    || "BSON types differ".into(),
                    |(k, _)| format!("field `{k}` has another BSON type"),
                )
        });
    }
    let null = serde_json::Value::Null;
    let field = e
        .keys()
        .chain(a.keys())
        .find(|k| e.get(*k).unwrap_or(&null) != a.get(*k).unwrap_or(&null))?;
    Some(format!(
        "field `{field}`: expected {}, found {}",
        short(e.get(field).unwrap_or(&null)),
        short(a.get(field).unwrap_or(&null))
    ))
}

fn row_hash(r: &Record, how: Comparison) -> [u8; 32] {
    let bytes = match how {
        Comparison::Exact => bson_bytes(r, false).unwrap_or_default(),
        Comparison::ExactUnordered => bson_bytes(r, true).unwrap_or_default(),
        Comparison::Normalized => serde_json::to_vec(&normalized(r)).unwrap_or_default(),
    };
    Sha256::digest(bytes).into()
}

/// Comparable text for a row's key.
pub(crate) fn key_of(r: &Record, keys: &[String]) -> String {
    let parts: Vec<serde_json::Value> = keys.iter().map(|k| normalize(r.get(k))).collect();
    serde_json::to_string(&parts).unwrap_or_default()
}

/// Target rows that share a key (or owner) with `rows`, by key. A target
/// object that does not exist yet holds nothing.
pub(crate) async fn fetch_existing(
    target: &dyn Connection,
    spec: &TargetSpec,
    rows: &[&Record],
) -> Result<HashMap<String, Record>, ConnectorError> {
    // With a composite key and no owner, rows are looked up by the first key
    // column and only the asked-for keys are kept (a batch may hold some of
    // a value's rows and the next batch the rest).
    let only: Option<std::collections::HashSet<String>> = (spec.key_fields.len() > 1
        && spec.owner.is_none())
    .then(|| rows.iter().map(|r| key_of(r, &spec.key_fields)).collect());
    let lookup = match (spec.key_fields.as_slice(), &spec.owner) {
        ([k], _) => k.clone(),
        (_, Some(owner)) => owner.clone(),
        ([first, ..], None) => first.clone(),
        _ => {
            return Err(ConnectorError::new(
                ErrorKind::Unsupported,
                format!("`{}` has no key to look rows up by", spec.object),
            ));
        }
    };
    let mut seen = std::collections::BTreeSet::new();
    let values: Vec<Value> = rows
        .iter()
        .map(|r| r.get(&lookup).clone())
        .filter(|v| !matches!(v, Value::Null | Value::Missing))
        .filter(|v| seen.insert(serde_json::to_string(&normalize(v)).unwrap_or_default()))
        .collect();
    let mut out = HashMap::new();
    for chunk in values.chunks(LOOKUP_CHUNK) {
        let mut req = ReadRequest::new(spec.object.clone(), LOOKUP_CHUNK * 4);
        req.filter = Some(Filter::In {
            field: FieldPath::new(lookup.clone()),
            values: chunk.to_vec(),
        });
        let mut stream = match target.read(req, CancellationToken::new()).await {
            Ok(s) => s,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e),
        };
        while let Some(batch) = stream.next().await {
            let batch = match batch {
                Ok(b) => b,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(e),
            };
            for r in batch.records {
                let key = key_of(&r, &spec.key_fields);
                if only.as_ref().is_none_or(|o| o.contains(&key)) {
                    out.insert(key, r);
                }
            }
        }
    }
    Ok(out)
}

/// Compares `source` with `target` through `transform`, at `level`.
#[allow(clippy::too_many_arguments)] // the whole comparison, kept explicit
pub async fn validate_stream(
    source: Arc<dyn Connection>,
    target: Arc<dyn Connection>,
    object: &ObjectRef,
    transform: Arc<dyn Transform>,
    level: Level,
    how: Comparison,
    batch_size: usize,
    stop: CancellationToken,
    on_progress: &(dyn Fn(&StreamValidation) + Send + Sync),
) -> Result<StreamValidation, ConnectorError> {
    let targets = transform.targets();
    let mut v = StreamValidation {
        level,
        source: object.clone(),
        source_count: count(source.as_ref(), object).await,
        target_count: match targets.first() {
            Some(t) => count(target.as_ref(), &t.object).await,
            None => None,
        },
        checked: 0,
        targets: targets
            .iter()
            .map(|t| TargetCheck::new(t.object.clone()))
            .collect(),
        differences: Vec::new(),
        passed: false,
        notes: Vec::new(),
    };
    let counts_match = match (v.source_count, v.target_count) {
        (Some(s), Some(t)) => {
            if !(s.exact && t.exact) {
                v.notes
                    .push("a count is an estimate; compare keys for certainty".into());
            }
            s.count == t.count
        }
        _ => {
            v.notes.push("a count could not be read".into());
            false
        }
    };
    if level == Level::Basic {
        v.passed = counts_match;
        on_progress(&v);
        return Ok(v);
    }
    let mut hashes: Vec<(Sha256, Sha256)> = targets
        .iter()
        .map(|_| (Sha256::new(), Sha256::new()))
        .collect();
    let mut stream = source
        .read(
            ReadRequest::new(object.clone(), batch_size.max(1)),
            stop.clone(),
        )
        .await?;
    while let Some(batch) = stream.next().await {
        if stop.is_cancelled() {
            v.notes.push("stopped before the end".into());
            break;
        }
        let records = match batch {
            Ok(b) => b.records,
            Err(e) if e.kind() == ErrorKind::Cancelled => {
                v.notes.push("stopped before the end".into());
                break;
            }
            Err(e) => return Err(e),
        };
        let out = transform.apply(&records).await?;
        for (_, why) in &out.rejected {
            v.targets[0].missing += 1;
            if v.differences.len() < MAX_DIFFERENCES {
                v.differences.push(Difference {
                    target: targets[0].object.to_string(),
                    key: String::new(),
                    kind: DiffKind::Missing,
                    detail: Some(format!("cannot be converted: {why}")),
                });
            }
        }
        for (t, rows) in out.rows.iter().enumerate() {
            let Some(spec) = targets.get(t) else { continue };
            let expected: Vec<&Record> = rows.iter().map(|(_, r)| r).collect();
            if expected.is_empty() {
                continue;
            }
            let mut actual = fetch_existing(target.as_ref(), spec, &expected).await?;
            let check = &mut v.targets[t];
            let before = check.clone();
            for e in &expected {
                check.expected += 1;
                let key = key_of(e, &spec.key_fields);
                let found = actual.remove(&key);
                if level == Level::Deep {
                    hashes[t].0.update(row_hash(e, how));
                    if let Some(a) = &found {
                        hashes[t].1.update(row_hash(a, how));
                    }
                }
                let diff = match &found {
                    None => Some((DiffKind::Missing, None)),
                    Some(a) if level == Level::Deep => {
                        compare(e, a, how).map(|d| (DiffKind::Different, Some(d)))
                    }
                    Some(_) => None,
                };
                match diff {
                    None => check.matched += 1,
                    Some((kind, detail)) => {
                        match kind {
                            DiffKind::Missing => check.missing += 1,
                            _ => check.different += 1,
                        }
                        if v.differences.len() < MAX_DIFFERENCES {
                            v.differences.push(Difference {
                                target: spec.object.to_string(),
                                key: key.clone(),
                                kind,
                                detail,
                            });
                        }
                    }
                }
            }
            // Rows fetched by owner but not expected (e.g. stale child rows).
            for (key, _) in actual {
                check.extra += 1;
                if v.differences.len() < MAX_DIFFERENCES {
                    v.differences.push(Difference {
                        target: spec.object.to_string(),
                        key,
                        kind: DiffKind::Extra,
                        detail: None,
                    });
                }
            }
            check.chunks += 1;
            if (check.missing, check.extra, check.different)
                != (before.missing, before.extra, before.different)
            {
                check.mismatched_chunks += 1;
            }
        }
        v.checked += records.len() as u64;
        on_progress(&v);
    }
    if level == Level::Deep {
        for (check, (e, a)) in v.targets.iter_mut().zip(hashes) {
            check.expected_checksum = Some(hex(e.finalize().as_slice()));
            check.actual_checksum = Some(hex(a.finalize().as_slice()));
        }
    }
    let root_extra = match (v.target_count, v.targets.first()) {
        (Some(t), Some(root)) if t.exact => t.count.saturating_sub(root.matched + root.different),
        _ => 0,
    };
    if root_extra > 0 {
        v.notes.push(format!(
            "the target holds {root_extra} row(s)/document(s) that do not come from the source"
        ));
    }
    v.passed = v.notes.iter().all(|n| !n.starts_with("stopped"))
        && v.targets.iter().all(TargetCheck::clean)
        && root_extra == 0;
    on_progress(&v);
    Ok(v)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use bigdecimal::BigDecimal;

    use super::*;

    #[test]
    fn normalization_hides_engine_forms_but_not_values() {
        use std::str::FromStr;
        assert_eq!(
            normalize(&Value::Int(5)),
            normalize(&Value::Decimal(BigDecimal::from_str("5.00").unwrap()))
        );
        assert_ne!(normalize(&Value::Int(5)), normalize(&Value::Int(6)));
        assert_eq!(
            normalize(&Value::Json(serde_json::json!({ "b": 1, "a": [2] }))),
            normalize(&Value::Json(serde_json::json!({ "a": [2], "b": 1 })))
        );
        let oid = [7u8; 12];
        assert_eq!(
            normalize(&Value::ObjectId(oid)),
            normalize(&Value::String("070707070707070707070707".into()))
        );
    }

    #[test]
    fn differences_name_the_first_field() {
        let a = Record::from_pairs([("_id", Value::Int(1)), ("name", Value::String("x".into()))]);
        let b = Record::from_pairs([("_id", Value::Int(1)), ("name", Value::String("y".into()))]);
        let d = compare(&a, &b, Comparison::Normalized).unwrap();
        assert!(
            d.contains("`name`") && d.contains("\"x\"") && d.contains("\"y\""),
            "{d}"
        );
        assert_eq!(compare(&a, &a.clone(), Comparison::Exact), None);
        let reordered =
            Record::from_pairs([("name", Value::String("x".into())), ("_id", Value::Int(1))]);
        assert_eq!(compare(&a, &reordered, Comparison::Normalized), None);
        assert_eq!(
            compare(&a, &reordered, Comparison::Exact).as_deref(),
            Some("same values, different field order")
        );
    }
}
