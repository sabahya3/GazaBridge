//! Typed JSON for viewing and editing records.
//!
//! Plain JSON cannot tell an ObjectId from a string or a decimal from a
//! float, so values JSON has no native form for are written as single-key
//! objects whose key starts with `$`, following MongoDB Extended JSON where
//! it has a form (`$oid`, `$date`, `$numberDecimal`, `$binary`, `$uuid`) and
//! GazaBridge names otherwise (`$dateOnly`, `$localDateTime`, …). Parsing
//! the output gives back the same values, which is what makes editing a
//! document in a text box safe.
//!
//! `Missing` fields are omitted from objects. JSON values (`Value::Json`)
//! are written as plain JSON; the reader cannot distinguish them from
//! objects, so writers coerce by target column type.

use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde_json::{Map, Value as J, json};

use crate::conversion::{
    json_to_value, parse_duration_text, parse_interval_text, parse_local_datetime,
    parse_offset_datetime, scalar_to_text,
};
use crate::value::{Record, Value};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {message}")]
pub struct EjsonError {
    /// Dotted location of the offending value (`$` is the root).
    pub path: String,
    pub message: String,
}

fn err(path: &str, message: impl Into<String>) -> EjsonError {
    EjsonError {
        path: if path.is_empty() {
            "$".into()
        } else {
            path.into()
        },
        message: message.into(),
    }
}

fn tagged(tag: &str, inner: J) -> J {
    let mut m = Map::with_capacity(1);
    m.insert(tag.to_owned(), inner);
    J::Object(m)
}

fn text(v: &Value) -> J {
    J::String(scalar_to_text(v).unwrap_or_default())
}

/// Converts a value to typed JSON.
pub fn to_ejson(value: &Value) -> J {
    match value {
        Value::Null => J::Null,
        Value::Missing => tagged("$missing", J::Bool(true)),
        Value::Bool(b) => J::Bool(*b),
        Value::Int(n) => J::from(*n),
        Value::UInt(n) => J::from(*n),
        Value::BigInt(b) => tagged("$numberBigInt", J::String(b.to_string())),
        Value::Decimal(d) => tagged("$numberDecimal", J::String(d.to_plain_string())),
        Value::Float(f) => match serde_json::Number::from_f64(*f) {
            Some(n) => J::Number(n),
            None => tagged("$numberDouble", text(value)),
        },
        Value::String(s) => J::String(s.clone()),
        Value::Binary(b) => tagged(
            "$binary",
            json!({ "base64": STANDARD.encode(b), "subType": "00" }),
        ),
        Value::Uuid(_) => tagged("$uuid", text(value)),
        Value::ObjectId(_) => tagged("$oid", text(value)),
        Value::Date(_) => tagged("$dateOnly", text(value)),
        Value::Time(_) => tagged("$time", text(value)),
        Value::LocalDateTime(_) => tagged("$localDateTime", text(value)),
        Value::UtcDateTime(_) => tagged("$date", text(value)),
        Value::ZonedDateTime { .. } => tagged("$zonedDateTime", text(value)),
        Value::Duration(_) => tagged("$duration", text(value)),
        Value::Interval(_) => tagged("$interval", text(value)),
        Value::Array(items) => J::Array(items.iter().map(to_ejson).collect()),
        Value::Object(fields) => {
            let mut m = Map::with_capacity(fields.len());
            for (k, v) in fields {
                if !matches!(v, Value::Missing) {
                    m.insert(k.clone(), to_ejson(v));
                }
            }
            J::Object(m)
        }
        Value::Map(entries) => tagged(
            "$map",
            J::Array(
                entries
                    .iter()
                    .map(|(k, v)| J::Array(vec![to_ejson(k), to_ejson(v)]))
                    .collect(),
            ),
        ),
        Value::Enum { type_name, label } => {
            tagged("$enum", json!({ "type": type_name, "label": label }))
        }
        Value::Json(j) => j.clone(),
        Value::Geometry { srid, wkb } => tagged(
            "$geometry",
            json!({ "srid": srid, "wkb": STANDARD.encode(wkb) }),
        ),
        Value::Extension {
            type_name,
            text,
            binary,
        } => tagged(
            "$extension",
            json!({
                "type": type_name,
                "text": text,
                "base64": binary.as_ref().map(|b| STANDARD.encode(b)),
            }),
        ),
        Value::Invalid { reason, .. } => tagged("$invalid", J::String(reason.clone())),
    }
}

/// Converts a record to a typed JSON object, omitting missing fields.
pub fn record_to_ejson(record: &Record) -> J {
    let mut m = Map::with_capacity(record.len());
    for (k, v) in &record.fields {
        if !matches!(v.value, Value::Missing) {
            m.insert(k.clone(), to_ejson(&v.value));
        }
    }
    J::Object(m)
}

/// Pretty-printed typed JSON for a record.
pub fn record_to_ejson_string(record: &Record) -> String {
    serde_json::to_string_pretty(&record_to_ejson(record)).unwrap_or_default()
}

fn str_of<'a>(path: &str, tag: &str, j: &'a J) -> Result<&'a str, EjsonError> {
    j.as_str()
        .ok_or_else(|| err(path, format!("`{tag}` expects a string")))
}

fn parse_text<T>(
    path: &str,
    tag: &str,
    j: &J,
    f: impl Fn(&str) -> Option<T>,
) -> Result<T, EjsonError> {
    let s = str_of(path, tag, j)?;
    f(s).ok_or_else(|| err(path, format!("`{s}` is not a valid `{tag}` value")))
}

fn b64(path: &str, j: &J) -> Result<Vec<u8>, EjsonError> {
    let s = j
        .as_str()
        .ok_or_else(|| err(path, "base64 text expected"))?;
    STANDARD
        .decode(s)
        .map_err(|e| err(path, format!("invalid base64: {e}")))
}

fn tagged_value(path: &str, tag: &str, inner: &J) -> Result<Option<Value>, EjsonError> {
    Ok(Some(match tag {
        "$missing" => Value::Missing,
        "$numberBigInt" => {
            Value::BigInt(parse_text(path, tag, inner, |s| BigInt::from_str(s).ok())?)
        }
        "$numberDecimal" => Value::Decimal(parse_text(path, tag, inner, |s| {
            BigDecimal::from_str(s).ok()
        })?),
        "$numberLong" | "$numberInt" => {
            Value::Int(parse_text(path, tag, inner, |s| s.parse().ok())?)
        }
        "$numberDouble" => Value::Float(parse_text(path, tag, inner, |s| match s {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => s.parse().ok(),
        })?),
        "$binary" => {
            let data = inner
                .get("base64")
                .ok_or_else(|| err(path, "`$binary` needs `base64`"))?;
            match inner.get("subType").and_then(J::as_str).unwrap_or("00") {
                "00" | "0" => Value::Binary(b64(path, data)?),
                "04" | "4" => {
                    let bytes = b64(path, data)?;
                    Value::Uuid(
                        uuid::Uuid::from_slice(&bytes)
                            .map_err(|_| err(path, "a UUID needs 16 bytes"))?,
                    )
                }
                other => {
                    return Err(err(
                        path,
                        format!("binary subtype `{other}` cannot be edited"),
                    ));
                }
            }
        }
        "$uuid" => Value::Uuid(parse_text(path, tag, inner, |s| {
            uuid::Uuid::parse_str(s).ok()
        })?),
        "$oid" => Value::ObjectId(parse_text(path, tag, inner, |s| {
            let bytes = hex::decode(s).ok()?;
            <[u8; 12]>::try_from(bytes).ok()
        })?),
        "$date" => match inner {
            // Canonical Extended JSON: {"$date": {"$numberLong": "<millis>"}}.
            J::Object(o) => {
                let millis: i64 = o
                    .get("$numberLong")
                    .and_then(J::as_str)
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        err(
                            path,
                            "`$date` expects RFC 3339 text or {\"$numberLong\": millis}",
                        )
                    })?;
                Value::UtcDateTime(
                    DateTime::<Utc>::from_timestamp_millis(millis)
                        .ok_or_else(|| err(path, "date out of range"))?,
                )
            }
            _ => Value::UtcDateTime(parse_text(path, tag, inner, |s| {
                parse_offset_datetime(s).map(|(dt, _)| dt.with_timezone(&Utc))
            })?),
        },
        "$dateOnly" => Value::Date(parse_text(path, tag, inner, |s| {
            NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()
        })?),
        "$time" => Value::Time(parse_text(path, tag, inner, |s| {
            NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M"))
                .ok()
        })?),
        "$localDateTime" => {
            Value::LocalDateTime(parse_text(path, tag, inner, parse_local_datetime)?)
        }
        "$zonedDateTime" => {
            let (instant, zone) = parse_text(path, tag, inner, parse_offset_datetime)?;
            Value::ZonedDateTime { instant, zone }
        }
        "$duration" => Value::Duration(parse_text(path, tag, inner, parse_duration_text)?),
        "$interval" => Value::Interval(parse_text(path, tag, inner, parse_interval_text)?),
        "$map" => {
            let items = inner
                .as_array()
                .ok_or_else(|| err(path, "`$map` expects an array of [key, value] pairs"))?;
            let mut out = Vec::with_capacity(items.len());
            for (i, pair) in items.iter().enumerate() {
                let p = format!("{path}.$map[{i}]");
                match pair.as_array().map(Vec::as_slice) {
                    Some([k, v]) => out.push((from_ejson_at(&p, k)?, from_ejson_at(&p, v)?)),
                    _ => return Err(err(&p, "expected a [key, value] pair")),
                }
            }
            Value::Map(out)
        }
        "$enum" => Value::Enum {
            type_name: inner.get("type").and_then(J::as_str).map(str::to_owned),
            label: inner
                .get("label")
                .and_then(J::as_str)
                .ok_or_else(|| err(path, "`$enum` needs `label`"))?
                .to_owned(),
        },
        "$geometry" => Value::Geometry {
            srid: inner
                .get("srid")
                .and_then(J::as_u64)
                .and_then(|s| u32::try_from(s).ok()),
            wkb: b64(path, inner.get("wkb").unwrap_or(&J::Null))?,
        },
        "$extension" => Value::Extension {
            type_name: inner
                .get("type")
                .and_then(J::as_str)
                .ok_or_else(|| err(path, "`$extension` needs `type`"))?
                .to_owned(),
            text: inner.get("text").and_then(J::as_str).map(str::to_owned),
            binary: match inner.get("base64") {
                None | Some(J::Null) => None,
                Some(b) => Some(b64(path, b)?),
            },
        },
        "$invalid" => {
            return Err(err(
                path,
                "this value could not be read from the database and cannot be written back",
            ));
        }
        _ => return Ok(None),
    }))
}

fn from_ejson_at(path: &str, j: &J) -> Result<Value, EjsonError> {
    match j {
        J::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                out.push(from_ejson_at(&format!("{path}[{i}]"), item)?);
            }
            Ok(Value::Array(out))
        }
        J::Object(map) => {
            if map.len() == 1
                && let Some((tag, inner)) = map.iter().next()
                && tag.starts_with('$')
                && let Some(v) = tagged_value(path, tag, inner)?
            {
                return Ok(v);
            }
            let mut out = indexmap::IndexMap::with_capacity(map.len());
            for (k, v) in map {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                out.insert(k.clone(), from_ejson_at(&p, v)?);
            }
            Ok(Value::Object(out))
        }
        other => Ok(json_to_value(other)),
    }
}

/// Parses typed JSON into a value.
pub fn from_ejson(j: &J) -> Result<Value, EjsonError> {
    from_ejson_at("", j)
}

/// Parses a typed JSON object into a record, keeping field order.
pub fn record_from_ejson(j: &J) -> Result<Record, EjsonError> {
    let J::Object(map) = j else {
        return Err(err("", "a record must be a JSON object"));
    };
    let mut record = Record::with_capacity(map.len());
    for (k, v) in map {
        record.insert(k.clone(), from_ejson_at(k, v)?);
    }
    Ok(record)
}

/// Parses typed JSON text into a record.
pub fn record_from_ejson_str(text: &str) -> Result<Record, EjsonError> {
    let j: J = serde_json::from_str(text).map_err(|e| err("", format!("invalid JSON: {e}")))?;
    record_from_ejson(&j)
}

/// Short single-line text for a table cell, at most `max_chars` characters.
pub fn display_text(value: &Value, max_chars: usize) -> String {
    let full = match value {
        Value::Null => "null".to_owned(),
        Value::Missing => String::new(),
        Value::Binary(b) => format!("<{} bytes>", b.len()),
        Value::Geometry { wkb, srid } => match srid {
            Some(s) => format!("<geometry SRID {s}, {} bytes>", wkb.len()),
            None => format!("<geometry, {} bytes>", wkb.len()),
        },
        Value::Invalid { reason, .. } => format!("<invalid: {reason}>"),
        Value::Extension {
            type_name,
            text: None,
            binary,
        } => {
            format!(
                "<{type_name}, {} bytes>",
                binary.as_ref().map_or(0, Vec::len)
            )
        }
        Value::Array(_) | Value::Object(_) | Value::Map(_) => to_ejson(value).to_string(),
        Value::Json(j) => j.to_string(),
        other => scalar_to_text(other).unwrap_or_default(),
    };
    truncate(&full, max_chars)
}

fn truncate(s: &str, max_chars: usize) -> String {
    match s.char_indices().nth(max_chars) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use indexmap::IndexMap;

    use super::*;
    use crate::value::{Duration, Interval};

    fn sample() -> Value {
        let mut nested = IndexMap::new();
        nested.insert("_id".to_owned(), Value::ObjectId([7; 12]));
        nested.insert(
            "price".to_owned(),
            Value::Decimal(BigDecimal::from_str("12.50").unwrap()),
        );
        nested.insert("ratio".to_owned(), Value::Float(1.0));
        nested.insert("nan".to_owned(), Value::Float(f64::NAN));
        nested.insert("count".to_owned(), Value::Int(-3));
        nested.insert(
            "huge".to_owned(),
            Value::BigInt(BigInt::from_str("123456789012345678901234567890").unwrap()),
        );
        nested.insert("bytes".to_owned(), Value::Binary(vec![0, 1, 255]));
        nested.insert("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(42)));
        nested.insert(
            "at".to_owned(),
            Value::UtcDateTime(Utc.with_ymd_and_hms(2026, 10, 25, 8, 30, 0).unwrap()),
        );
        nested.insert(
            "day".to_owned(),
            Value::Date(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
        );
        nested.insert(
            "time".to_owned(),
            Value::Time(NaiveTime::from_hms_micro_opt(1, 2, 3, 400).unwrap()),
        );
        nested.insert(
            "dur".to_owned(),
            Value::Duration(Duration {
                seconds: 90,
                nanos: 5,
            }),
        );
        nested.insert(
            "iv".to_owned(),
            Value::Interval(Interval {
                months: 1,
                days: 2,
                microseconds: 3_000_000,
            }),
        );
        nested.insert(
            "tags".to_owned(),
            Value::Array(vec![Value::String("a".into()), Value::Null]),
        );
        nested.insert(
            "state".to_owned(),
            Value::Enum {
                type_name: Some("mood".into()),
                label: "ok".into(),
            },
        );
        nested.insert(
            "ext".to_owned(),
            Value::Extension {
                type_name: "bson:regex".into(),
                text: Some("/a/i".into()),
                binary: Some(vec![1, 2]),
            },
        );
        nested.insert(
            "map".to_owned(),
            Value::Map(vec![(Value::Int(1), Value::Bool(true))]),
        );
        Value::Object(nested)
    }

    #[test]
    fn round_trips_typed_values() {
        let v = sample();
        let j = to_ejson(&v);
        let back = from_ejson(&j).unwrap();
        // NaN != NaN, so compare the JSON forms.
        assert_eq!(to_ejson(&back), j);
        let Value::Object(o) = back else { panic!() };
        assert!(matches!(o["ratio"], Value::Float(f) if f == 1.0));
        assert!(matches!(o["_id"], Value::ObjectId(_)));
        assert!(matches!(o["price"], Value::Decimal(_)));
    }

    #[test]
    fn records_keep_order_and_omit_missing() {
        let mut r = Record::new();
        r.insert("_id", Value::Int(1));
        r.insert("gone", Value::Missing);
        r.insert("a", Value::Null);
        let text = record_to_ejson_string(&r);
        assert!(!text.contains("gone"));
        let back = record_from_ejson_str(&text).unwrap();
        assert_eq!(back.fields.keys().collect::<Vec<_>>(), ["_id", "a"]);
        assert_eq!(*back.get("a"), Value::Null);
    }

    #[test]
    fn accepts_canonical_extended_json_and_reports_paths() {
        let j: J = serde_json::from_str(
            r#"{"d": {"$date": {"$numberLong": "0"}}, "n": {"$numberLong": "5"}}"#,
        )
        .unwrap();
        let r = record_from_ejson(&j).unwrap();
        assert!(matches!(r.get("d"), Value::UtcDateTime(_)));
        assert_eq!(*r.get("n"), Value::Int(5));

        let bad: J = serde_json::from_str(r#"{"a": {"b": [1, {"$oid": "zz"}]}}"#).unwrap();
        let e = record_from_ejson(&bad).unwrap_err();
        assert_eq!(e.path, "a.b[1]");
        assert!(record_from_ejson_str("[1]").is_err());
        assert!(record_from_ejson_str("{").is_err());
    }

    #[test]
    fn plain_dollar_objects_stay_objects() {
        let j: J = serde_json::from_str(r#"{"$gt": 5}"#).unwrap();
        assert!(matches!(from_ejson(&j).unwrap(), Value::Object(_)));
    }

    #[test]
    fn invalid_values_cannot_be_written_back() {
        let v = Value::Invalid {
            reason: "bad bytes".into(),
            raw: None,
        };
        assert!(from_ejson(&to_ejson(&v)).is_err());
    }

    #[test]
    fn display_truncates_by_characters() {
        assert_eq!(display_text(&Value::String("غزةغزة".into()), 3), "غزة…");
        assert_eq!(display_text(&Value::Binary(vec![0; 4]), 50), "<4 bytes>");
        assert_eq!(display_text(&Value::Missing, 5), "");
    }
}
