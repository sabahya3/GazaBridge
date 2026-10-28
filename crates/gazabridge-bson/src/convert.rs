//! BSON ⇄ canonical value mapping.
//!
//! * Exotic BSON types (regex, timestamp, JavaScript, symbol, min/max key,
//!   DBPointer, uncommon binary subtypes) become `Extension` values named
//!   `bson:<kind>` carrying the exact BSON bytes, so MongoDB → MongoDB copies
//!   restore them unchanged.
//! * MongoDB has no date-only, time-of-day, duration or interval types, and
//!   Decimal128 holds at most 34 digits. Such canonical values are stored as
//!   their exact canonical text (a lossless conversion in the canonical
//!   model); documented in the MongoDB profile.
//! * `Int32` fields are tagged `original_type = "int"` on read so writing
//!   them back keeps the 32-bit type.

use bigdecimal::BigDecimal;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Decimal128, Document, oid::ObjectId};
use chrono::DateTime;
use gazabridge_domain::conversion::{json_to_value, scalar_to_text};
use gazabridge_domain::value::ValueMeta;
use gazabridge_domain::{CanonicalValue, Record, Value};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind};
use indexmap::IndexMap;

use crate::decimal128::{self, Dec128};

const BSON_EXT_PREFIX: &str = "bson:";

fn conversion(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ErrorKind::Conversion, msg)
}

/// Keeps a BSON value exactly by serializing it inside a one-field document.
fn exotic(kind: &str, display: String, b: &Bson) -> Value {
    let mut doc = Document::new();
    doc.insert("v", b.clone());
    let bytes = bson::to_vec(&doc).ok();
    Value::Extension {
        type_name: format!("{BSON_EXT_PREFIX}{kind}"),
        text: Some(display),
        binary: bytes,
    }
}

fn restore_exotic(bytes: &[u8]) -> ConnectorResult<Bson> {
    let doc: Document =
        bson::from_slice(bytes).map_err(|e| conversion(format!("corrupt BSON extension: {e}")))?;
    doc.get("v")
        .cloned()
        .ok_or_else(|| conversion("corrupt BSON extension"))
}

fn canonical_decimal(bytes: [u8; 16]) -> bool {
    let again = match decimal128::decode(bytes) {
        Dec128::Finite(v) => decimal128::encode(&v),
        Dec128::NaN => Some(decimal128::nan()),
        Dec128::Infinity { negative } => Some(decimal128::infinity(negative)),
    };
    again == Some(bytes)
}

/// Converts a BSON value to a canonical value.
pub fn from_bson(b: &Bson) -> Value {
    match b {
        Bson::Null => Value::Null,
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::Int32(i) => Value::Int(i64::from(*i)),
        Bson::Int64(i) => Value::Int(*i),
        Bson::Double(f) => Value::Float(*f),
        Bson::String(s) => Value::String(s.clone()),
        Bson::ObjectId(oid) => Value::ObjectId(oid.bytes()),
        Bson::DateTime(dt) => match DateTime::from_timestamp_millis(dt.timestamp_millis()) {
            Some(d) => Value::UtcDateTime(d),
            None => exotic("datetime", dt.timestamp_millis().to_string(), b),
        },
        // Non-canonical encodings (stray bits the standard ignores) keep
        // their exact bytes so a copy is byte-identical.
        Bson::Decimal128(d) if !canonical_decimal(d.bytes()) => {
            exotic("decimal", format!("{:?}", decimal128::decode(d.bytes())), b)
        }
        Bson::Decimal128(d) => match decimal128::decode(d.bytes()) {
            Dec128::Finite(v) => Value::Decimal(v),
            Dec128::NaN => Value::Float(f64::NAN),
            Dec128::Infinity { negative } => Value::Float(if negative {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            }),
        },
        Bson::Binary(bin) => match bin.subtype {
            BinarySubtype::Generic => Value::Binary(bin.bytes.clone()),
            BinarySubtype::Uuid if bin.bytes.len() == 16 => uuid::Uuid::from_slice(&bin.bytes)
                .map_or_else(|_| exotic("binary", "uuid".into(), b), Value::Uuid),
            other => exotic("binary", format!("{other:?}"), b),
        },
        Bson::Array(items) => Value::Array(items.iter().map(from_bson).collect()),
        Bson::Document(d) => {
            Value::Object(d.iter().map(|(k, v)| (k.clone(), from_bson(v))).collect())
        }
        Bson::RegularExpression(r) => exotic("regex", format!("/{}/{}", r.pattern, r.options), b),
        Bson::Timestamp(t) => exotic("timestamp", format!("{}:{}", t.time, t.increment), b),
        Bson::JavaScriptCode(code) => exotic("javascript", code.clone(), b),
        Bson::JavaScriptCodeWithScope(c) => exotic("javascript_with_scope", c.code.clone(), b),
        Bson::Symbol(s) => exotic("symbol", s.clone(), b),
        Bson::Undefined => exotic("undefined", "undefined".into(), b),
        Bson::MaxKey => exotic("max_key", "MaxKey".into(), b),
        Bson::MinKey => exotic("min_key", "MinKey".into(), b),
        Bson::DbPointer(_) => exotic("db_pointer", "DBPointer".into(), b),
    }
}

fn decimal_bson(d: &BigDecimal) -> Bson {
    match decimal128::encode(d) {
        Some(bytes) => Bson::Decimal128(Decimal128::from_bytes(bytes)),
        // More than 34 significant digits: keep the exact text.
        None => Bson::String(d.to_plain_string()),
    }
}

fn text_of(v: &Value) -> ConnectorResult<Bson> {
    scalar_to_text(v)
        .map(Bson::String)
        .ok_or_else(|| conversion(format!("{} value has no text form", v.kind())))
}

/// Metadata key listing nested paths that were BSON `Int32` on read.
pub const INT32_PATHS_KEY: &str = "bson.int32_paths";

/// Nested `Int32` locations within one top-level field, as path segments.
type Paths = Vec<Vec<String>>;

fn collect_int32(b: &Bson, path: &mut Vec<String>, out: &mut Paths) {
    match b {
        Bson::Int32(_) if !path.is_empty() => out.push(path.clone()),
        Bson::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                path.push(i.to_string());
                collect_int32(item, path, out);
                path.pop();
            }
        }
        Bson::Document(d) => {
            for (k, item) in d {
                path.push(k.clone());
                collect_int32(item, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

/// Re-narrows a nested 64-bit integer that was `Int32` on read.
fn narrow_at(b: &mut Bson, path: &[String]) {
    match path.split_first() {
        None => {
            if let Bson::Int64(i) = b
                && let Ok(small) = i32::try_from(*i)
            {
                *b = Bson::Int32(small);
            }
        }
        Some((seg, rest)) => match b {
            Bson::Array(items) => {
                if let Some(child) = seg.parse::<usize>().ok().and_then(|i| items.get_mut(i)) {
                    narrow_at(child, rest);
                }
            }
            Bson::Document(d) => {
                if let Some(child) = d.get_mut(seg) {
                    narrow_at(child, rest);
                }
            }
            _ => {}
        },
    }
}

fn apply_int32(b: &mut Bson, paths: &Paths) {
    for path in paths {
        narrow_at(b, path);
    }
}

/// Converts a canonical value to BSON. `Ok(None)` means "omit the field"
/// (a missing value). `original_type` comes from value metadata.
pub fn to_bson(v: &Value, original_type: Option<&str>) -> ConnectorResult<Option<Bson>> {
    Ok(Some(match v {
        Value::Missing => return Ok(None),
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Int(i) => match (original_type, i32::try_from(*i)) {
            (Some("int"), Ok(small)) => Bson::Int32(small),
            _ => Bson::Int64(*i),
        },
        Value::UInt(u) => match i64::try_from(*u) {
            Ok(i) => Bson::Int64(i),
            Err(_) => decimal_bson(&BigDecimal::from(*u)),
        },
        Value::BigInt(b) => match i64::try_from(b) {
            Ok(i) => Bson::Int64(i),
            Err(_) => decimal_bson(&BigDecimal::from(b.clone())),
        },
        Value::Decimal(d) => decimal_bson(d),
        Value::Float(f) => match original_type {
            Some("decimal") if f.is_nan() => {
                Bson::Decimal128(Decimal128::from_bytes(decimal128::nan()))
            }
            Some("decimal") if f.is_infinite() => {
                Bson::Decimal128(Decimal128::from_bytes(decimal128::infinity(*f < 0.0)))
            }
            _ => Bson::Double(*f),
        },
        Value::String(s) => Bson::String(s.clone()),
        Value::Binary(b) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        Value::Uuid(u) => Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: u.as_bytes().to_vec(),
        }),
        Value::ObjectId(o) => Bson::ObjectId(ObjectId::from_bytes(*o)),
        Value::UtcDateTime(dt) if dt.timestamp_subsec_nanos() % 1_000_000 == 0 => {
            Bson::DateTime(bson::DateTime::from_millis(dt.timestamp_millis()))
        }
        // Sub-millisecond precision does not fit a BSON date; keep exact text.
        Value::UtcDateTime(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::LocalDateTime(_)
        | Value::ZonedDateTime { .. }
        | Value::Duration(_)
        | Value::Interval(_) => text_of(v)?,
        Value::Enum { label, .. } => Bson::String(label.clone()),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match to_bson(item, None)? {
                    Some(b) => out.push(b),
                    None => return Err(conversion("arrays cannot contain missing values")),
                }
            }
            Bson::Array(out)
        }
        Value::Object(fields) => Bson::Document(object_to_document(fields)?),
        Value::Map(entries) => {
            let mut doc = Document::new();
            for (k, val) in entries {
                let Value::String(key) = k else {
                    return Err(conversion("MongoDB document keys must be strings"));
                };
                if let Some(b) = to_bson(val, None)? {
                    insert_field(&mut doc, key, b)?;
                }
            }
            Bson::Document(doc)
        }
        Value::Json(j) => return to_bson(&json_to_value(j), None),
        Value::Extension {
            type_name,
            binary: Some(bytes),
            ..
        } if type_name.starts_with(BSON_EXT_PREFIX) => restore_exotic(bytes)?,
        Value::Extension { text: Some(t), .. } => Bson::String(t.clone()),
        Value::Extension {
            binary: Some(b), ..
        } => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: b.clone(),
        }),
        Value::Extension { .. } => return Err(conversion("empty extension value")),
        Value::Geometry { .. } => {
            return Err(conversion(
                "geometry must be converted to GeoJSON before writing to MongoDB",
            ));
        }
        Value::Invalid { reason, .. } => {
            return Err(conversion(format!("invalid source value: {reason}")));
        }
    }))
}

fn insert_field(doc: &mut Document, key: &str, value: Bson) -> ConnectorResult<()> {
    if key.contains('\0') {
        return Err(conversion("MongoDB field names cannot contain NUL"));
    }
    if doc.insert(key.to_owned(), value).is_some() {
        return Err(conversion(format!("duplicate field `{key}`")));
    }
    Ok(())
}

fn object_to_document(fields: &IndexMap<String, Value>) -> ConnectorResult<Document> {
    let mut doc = Document::new();
    for (k, v) in fields {
        if let Some(b) = to_bson(v, None)? {
            insert_field(&mut doc, k, b)?;
        }
    }
    Ok(doc)
}

/// Converts a record to a document, honouring per-field type metadata.
pub fn record_to_document(r: &Record) -> ConnectorResult<Document> {
    let mut doc = Document::new();
    for (k, cv) in &r.fields {
        let original = cv.meta.as_ref().and_then(|m| m.original_type.as_deref());
        if let Some(mut b) = to_bson(&cv.value, original)? {
            if let Some(paths) = cv
                .meta
                .as_ref()
                .and_then(|m| m.connector.get(INT32_PATHS_KEY))
                .and_then(|raw| serde_json::from_str::<Paths>(raw).ok())
            {
                apply_int32(&mut b, &paths);
            }
            insert_field(&mut doc, k, b)?;
        }
    }
    Ok(doc)
}

/// Converts a document to a record, tagging 32-bit integers and decimals.
pub fn document_to_record(doc: &Document) -> Record {
    let mut r = Record::with_capacity(doc.len());
    for (k, b) in doc {
        let value = from_bson(b);
        let tag = match b {
            Bson::Int32(_) => Some("int"),
            Bson::Decimal128(_) => Some("decimal"),
            _ => None,
        };
        let mut nested = Paths::new();
        collect_int32(b, &mut Vec::new(), &mut nested);
        let cv = if tag.is_none() && nested.is_empty() {
            CanonicalValue::new(value)
        } else {
            let mut meta = ValueMeta {
                original_type: tag.map(str::to_owned),
                ..Default::default()
            };
            if !nested.is_empty()
                && let Ok(raw) = serde_json::to_string(&nested)
            {
                meta.connector.insert(INT32_PATHS_KEY.to_owned(), raw);
            }
            CanonicalValue::with_meta(value, meta)
        };
        r.fields.insert(k.clone(), cv);
    }
    r
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bson::{Regex, Timestamp, doc};

    use super::*;

    #[test]
    fn documents_round_trip_including_exotic_types() {
        let original = doc! {
            "_id": ObjectId::new(),
            "i32": 7i32,
            "i64": 7i64,
            "f": 1.5,
            "s": "غزة",
            "n": Bson::Null,
            "dec": Bson::Decimal128(Decimal128::from_bytes(decimal128::encode(&BigDecimal::from_str("12.3400").unwrap()).unwrap())),
            "dt": bson::DateTime::from_millis(1_700_000_000_123),
            "bin": Binary { subtype: BinarySubtype::Generic, bytes: vec![0, 1, 255] },
            "uuid": Binary { subtype: BinarySubtype::Uuid, bytes: uuid::Uuid::from_u128(42).as_bytes().to_vec() },
            "md5": Binary { subtype: BinarySubtype::Md5, bytes: vec![1; 16] },
            "re": Regex { pattern: "^a.*".into(), options: "i".into() },
            "ts": Timestamp { time: 1, increment: 2 },
            "js": Bson::JavaScriptCode("x => x".into()),
            "min": Bson::MinKey,
            "nested": { "arr": [1i32, "two", { "three": 3i64 }], "empty": [] },
            // Non-canonical zero and infinity: stray bits survive.
            "dec_odd": Bson::Decimal128(Decimal128::from_bytes([0x60; 16])),
            "inf_odd": Bson::Decimal128(Decimal128::from_bytes([0x78; 16])),
        };
        let record = document_to_record(&original);
        assert_eq!(record.get("i32"), &Value::Int(7));
        assert!(
            matches!(record.get("re"), Value::Extension { type_name, .. } if type_name == "bson:regex")
        );
        let back = record_to_document(&record).unwrap();
        // Byte-exact: `Document` equality ignores field order.
        assert_eq!(
            bson::to_vec(&back).unwrap(),
            bson::to_vec(&original).unwrap()
        );
    }

    #[test]
    fn canonical_types_without_bson_equivalents_become_exact_text() {
        let big = BigDecimal::from_str("1234567890123456789012345678.0123456789").unwrap();
        assert_eq!(
            to_bson(&Value::Decimal(big), None).unwrap(),
            Some(Bson::String(
                "1234567890123456789012345678.0123456789".into()
            ))
        );
        let date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(
            to_bson(&Value::Date(date), None).unwrap(),
            Some(Bson::String("2024-02-29".into()))
        );
        assert!(matches!(
            to_bson(&Value::UInt(u64::MAX), None).unwrap(),
            Some(Bson::Decimal128(_))
        ));
        assert_eq!(to_bson(&Value::Missing, None).unwrap(), None);
    }

    #[test]
    fn missing_and_null_stay_distinct() {
        let mut fields = IndexMap::new();
        fields.insert("a".to_owned(), Value::Null);
        fields.insert("b".to_owned(), Value::Missing);
        let doc = object_to_document(&fields).unwrap();
        assert_eq!(doc, doc! { "a": Bson::Null });
        assert!(to_bson(&Value::Array(vec![Value::Missing]), None).is_err());
        assert!(
            to_bson(
                &Value::Geometry {
                    srid: None,
                    wkb: vec![1]
                },
                None
            )
            .is_err()
        );
    }
}
