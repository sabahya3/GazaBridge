//! The loss-aware canonical value model.
//!
//! Every connector converts native values into [`Value`] on read and from
//! [`Value`] on write. Transformations, validation and checksums only ever see
//! canonical values, which is what lets GazaBridge support every
//! source/target pair without pairwise conversion code.
//!
//! Nested values ([`Value::Array`], [`Value::Object`], [`Value::Map`]) are
//! plain [`Value`]s; provenance metadata ([`ValueMeta`]) is attached at the
//! top-level field through [`CanonicalValue`].

use std::fmt;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::conversion::LossRecord;
use crate::serde_helpers::{base64_bytes, display_fromstr, float, hex12, opt_base64_bytes};
use crate::types::CanonicalType;

/// A signed, normalized duration with nanosecond precision.
///
/// `nanos` is always in `0..1_000_000_000`; negative durations carry their
/// sign in `seconds` (floor semantics), e.g. -0.5s is `{seconds: -1, nanos: 500_000_000}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Duration {
    pub seconds: i64,
    pub nanos: u32,
}

impl Duration {
    pub const NANOS_PER_SEC: i128 = 1_000_000_000;

    /// Builds a duration from total nanoseconds. Returns `None` on overflow.
    pub fn from_total_nanos(total: i128) -> Option<Self> {
        let seconds = total.div_euclid(Self::NANOS_PER_SEC);
        let nanos = total.rem_euclid(Self::NANOS_PER_SEC);
        Some(Self {
            seconds: i64::try_from(seconds).ok()?,
            nanos: u32::try_from(nanos).ok()?,
        })
    }

    pub fn total_nanos(&self) -> i128 {
        i128::from(self.seconds) * Self::NANOS_PER_SEC + i128::from(self.nanos)
    }
}

/// A calendar interval in the PostgreSQL sense: months and days are not
/// convertible to a fixed number of seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

/// A canonical value.
///
/// `Null` and `Missing` are distinct: `Null` means the field exists with no
/// value; `Missing` means the field is absent (common in document stores).
/// Serialized externally tagged, e.g. `{"decimal":"1.50"}` or `"null"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Value {
    Null,
    Missing,
    Bool(bool),
    Int(i64),
    UInt(u64),
    BigInt(#[serde(with = "display_fromstr")] BigInt),
    Decimal(#[serde(with = "display_fromstr")] BigDecimal),
    Float(#[serde(with = "float")] f64),
    String(String),
    Binary(#[serde(with = "base64_bytes")] Vec<u8>),
    Uuid(Uuid),
    ObjectId(#[serde(with = "hex12")] [u8; 12]),
    Date(NaiveDate),
    Time(NaiveTime),
    LocalDateTime(NaiveDateTime),
    UtcDateTime(DateTime<Utc>),
    ZonedDateTime {
        instant: DateTime<FixedOffset>,
        /// IANA zone name when known (e.g. `Asia/Gaza`); the offset in
        /// `instant` is authoritative.
        zone: Option<String>,
    },
    Duration(Duration),
    Interval(Interval),
    Array(Vec<Value>),
    /// String-keyed object that preserves field order (documents, structs).
    Object(IndexMap<String, Value>),
    /// Map with arbitrary keys (e.g. Arrow/Parquet maps).
    Map(Vec<(Value, Value)>),
    Enum {
        type_name: Option<String>,
        label: String,
    },
    /// JSON that should stay JSON (PostgreSQL `jsonb`, MySQL `JSON`).
    Json(serde_json::Value),
    /// Geometry as Well-Known Binary.
    Geometry {
        srid: Option<u32>,
        #[serde(with = "base64_bytes")]
        wkb: Vec<u8>,
    },
    /// A database-specific value GazaBridge carries but does not interpret.
    Extension {
        type_name: String,
        text: Option<String>,
        #[serde(with = "opt_base64_bytes")]
        binary: Option<Vec<u8>>,
    },
    /// A value that could not be represented. Never written silently.
    Invalid {
        reason: String,
        #[serde(with = "opt_base64_bytes")]
        raw: Option<Vec<u8>>,
    },
}

/// The discriminant of a [`Value`], used for type distributions and display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueKind {
    Null,
    Missing,
    Bool,
    Int,
    UInt,
    BigInt,
    Decimal,
    Float,
    String,
    Binary,
    Uuid,
    ObjectId,
    Date,
    Time,
    LocalDateTime,
    UtcDateTime,
    ZonedDateTime,
    Duration,
    Interval,
    Array,
    Object,
    Map,
    Enum,
    Json,
    Geometry,
    Extension,
    Invalid,
}

impl ValueKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Missing => "missing",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::UInt => "uint",
            Self::BigInt => "bigint",
            Self::Decimal => "decimal",
            Self::Float => "float",
            Self::String => "string",
            Self::Binary => "binary",
            Self::Uuid => "uuid",
            Self::ObjectId => "object_id",
            Self::Date => "date",
            Self::Time => "time",
            Self::LocalDateTime => "local_datetime",
            Self::UtcDateTime => "utc_datetime",
            Self::ZonedDateTime => "zoned_datetime",
            Self::Duration => "duration",
            Self::Interval => "interval",
            Self::Array => "array",
            Self::Object => "object",
            Self::Map => "map",
            Self::Enum => "enum",
            Self::Json => "json",
            Self::Geometry => "geometry",
            Self::Extension => "extension",
            Self::Invalid => "invalid",
        }
    }
}

impl fmt::Display for ValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::Null => ValueKind::Null,
            Self::Missing => ValueKind::Missing,
            Self::Bool(_) => ValueKind::Bool,
            Self::Int(_) => ValueKind::Int,
            Self::UInt(_) => ValueKind::UInt,
            Self::BigInt(_) => ValueKind::BigInt,
            Self::Decimal(_) => ValueKind::Decimal,
            Self::Float(_) => ValueKind::Float,
            Self::String(_) => ValueKind::String,
            Self::Binary(_) => ValueKind::Binary,
            Self::Uuid(_) => ValueKind::Uuid,
            Self::ObjectId(_) => ValueKind::ObjectId,
            Self::Date(_) => ValueKind::Date,
            Self::Time(_) => ValueKind::Time,
            Self::LocalDateTime(_) => ValueKind::LocalDateTime,
            Self::UtcDateTime(_) => ValueKind::UtcDateTime,
            Self::ZonedDateTime { .. } => ValueKind::ZonedDateTime,
            Self::Duration(_) => ValueKind::Duration,
            Self::Interval(_) => ValueKind::Interval,
            Self::Array(_) => ValueKind::Array,
            Self::Object(_) => ValueKind::Object,
            Self::Map(_) => ValueKind::Map,
            Self::Enum { .. } => ValueKind::Enum,
            Self::Json(_) => ValueKind::Json,
            Self::Geometry { .. } => ValueKind::Geometry,
            Self::Extension { .. } => ValueKind::Extension,
            Self::Invalid { .. } => ValueKind::Invalid,
        }
    }

    /// True for `Null` and `Missing`.
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Null | Self::Missing)
    }

    /// The narrowest canonical type that describes this value.
    pub fn canonical_type(&self) -> CanonicalType {
        match self {
            Self::Null | Self::Missing => CanonicalType::Null,
            Self::Bool(_) => CanonicalType::Boolean,
            Self::Int(_) => CanonicalType::Int { bits: 64 },
            Self::UInt(_) => CanonicalType::UInt { bits: 64 },
            Self::BigInt(_) => CanonicalType::BigInt,
            Self::Decimal(d) => {
                let (_, scale) = d.as_bigint_and_exponent();
                CanonicalType::Decimal {
                    precision: u32::try_from(d.digits()).ok(),
                    scale: i32::try_from(scale).ok(),
                }
            }
            Self::Float(_) => CanonicalType::Float { bits: 64 },
            Self::String(_) => CanonicalType::String { max_chars: None },
            Self::Binary(_) => CanonicalType::Binary { max_bytes: None },
            Self::Uuid(_) => CanonicalType::Uuid,
            Self::ObjectId(_) => CanonicalType::ObjectId,
            Self::Date(_) => CanonicalType::Date,
            Self::Time(_) => CanonicalType::Time {
                fractional_digits: 9,
            },
            Self::LocalDateTime(_) => CanonicalType::LocalDateTime {
                fractional_digits: 9,
            },
            Self::UtcDateTime(_) => CanonicalType::UtcDateTime {
                fractional_digits: 9,
            },
            Self::ZonedDateTime { .. } => CanonicalType::ZonedDateTime,
            Self::Duration(_) => CanonicalType::Duration,
            Self::Interval(_) => CanonicalType::Interval,
            Self::Array(items) => CanonicalType::Array(Box::new(CanonicalType::unify_all(
                items.iter().map(Value::canonical_type),
            ))),
            Self::Object(_) => CanonicalType::Object,
            Self::Map(entries) => CanonicalType::Map {
                key: Box::new(CanonicalType::unify_all(
                    entries.iter().map(|(k, _)| k.canonical_type()),
                )),
                value: Box::new(CanonicalType::unify_all(
                    entries.iter().map(|(_, v)| v.canonical_type()),
                )),
            },
            Self::Enum { type_name, .. } => CanonicalType::Enum {
                name: type_name.clone(),
                labels: Vec::new(),
            },
            Self::Json(_) => CanonicalType::Json,
            Self::Geometry { .. } => CanonicalType::Geometry,
            Self::Extension { type_name, .. } => CanonicalType::Extension {
                name: type_name.clone(),
            },
            Self::Invalid { .. } => CanonicalType::Unknown,
        }
    }

    /// Approximate in-memory payload size in bytes, used for batch sizing
    /// and throughput statistics (not an exact allocation count).
    pub fn approx_size(&self) -> usize {
        match self {
            Self::Null | Self::Missing => 1,
            Self::Bool(_) => 1,
            Self::Int(_) | Self::UInt(_) | Self::Float(_) => 8,
            Self::BigInt(b) => (b.bits() as usize).div_ceil(8).max(1),
            Self::Decimal(d) => d.digits() as usize / 2 + 4,
            Self::String(s) => s.len(),
            Self::Binary(b) => b.len(),
            Self::Uuid(_) => 16,
            Self::ObjectId(_) => 12,
            Self::Date(_) => 4,
            Self::Time(_) => 8,
            Self::LocalDateTime(_) | Self::UtcDateTime(_) => 12,
            Self::ZonedDateTime { zone, .. } => 12 + zone.as_ref().map_or(0, String::len),
            Self::Duration(_) => 12,
            Self::Interval(_) => 16,
            Self::Array(items) => items.iter().map(Value::approx_size).sum::<usize>() + 8,
            Self::Object(fields) => {
                fields
                    .iter()
                    .map(|(k, v)| k.len() + v.approx_size())
                    .sum::<usize>()
                    + 8
            }
            Self::Map(entries) => {
                entries
                    .iter()
                    .map(|(k, v)| k.approx_size() + v.approx_size())
                    .sum::<usize>()
                    + 8
            }
            Self::Enum { label, .. } => label.len(),
            Self::Json(j) => json_size(j),
            Self::Geometry { wkb, .. } => wkb.len() + 4,
            Self::Extension { text, binary, .. } => {
                text.as_ref().map_or(0, String::len) + binary.as_ref().map_or(0, Vec::len)
            }
            Self::Invalid { raw, .. } => raw.as_ref().map_or(0, Vec::len),
        }
    }
}

fn json_size(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 8,
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(items) => items.iter().map(json_size).sum::<usize>() + 8,
        serde_json::Value::Object(map) => {
            map.iter()
                .map(|(k, v)| k.len() + json_size(v))
                .sum::<usize>()
                + 8
        }
    }
}

/// Provenance and fidelity information retained alongside a value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ValueMeta {
    /// Native type name in the source system, e.g. `numeric(12,2)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_type: Option<String>,
    /// Native textual representation, kept when it carries information the
    /// canonical value cannot (e.g. trailing zeros, custom formats).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_field: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<LossRecord>,
    #[serde(default, skip_serializing_if = "indexmap::IndexMap::is_empty")]
    pub connector: IndexMap<String, String>,
}

/// A value plus optional provenance metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalValue {
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Box<ValueMeta>>,
}

impl CanonicalValue {
    pub fn new(value: Value) -> Self {
        Self { value, meta: None }
    }

    pub fn with_meta(value: Value, meta: ValueMeta) -> Self {
        Self {
            value,
            meta: Some(Box::new(meta)),
        }
    }

    pub fn meta_mut(&mut self) -> &mut ValueMeta {
        self.meta.get_or_insert_with(Default::default)
    }

    pub fn has_warnings(&self) -> bool {
        self.meta.as_ref().is_some_and(|m| !m.warnings.is_empty())
    }
}

impl From<Value> for CanonicalValue {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}

/// A row or document as an ordered set of named canonical values.
///
/// A field that is absent from `fields` reads as [`Value::Missing`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub fields: IndexMap<String, CanonicalValue>,
}

static MISSING: Value = Value::Missing;

impl Record {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            fields: IndexMap::with_capacity(n),
        }
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<CanonicalValue>) {
        self.fields.insert(name.into(), value.into());
    }

    /// Returns the field value, or [`Value::Missing`] when absent.
    pub fn get(&self, name: &str) -> &Value {
        self.fields.get(name).map_or(&MISSING, |v| &v.value)
    }

    pub fn get_canonical(&self, name: &str) -> Option<&CanonicalValue> {
        self.fields.get(name)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn approx_size(&self) -> usize {
        self.fields
            .iter()
            .map(|(k, v)| k.len() + v.value.approx_size())
            .sum()
    }

    /// Builds a record from `(name, value)` pairs.
    pub fn from_pairs<I, K>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        Self {
            fields: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), CanonicalValue::new(v)))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn null_and_missing_are_distinct() {
        let rec = Record::from_pairs([("a", Value::Null)]);
        assert_eq!(rec.get("a"), &Value::Null);
        assert_eq!(rec.get("b"), &Value::Missing);
        assert_ne!(Value::Null, Value::Missing);
    }

    #[test]
    fn serde_round_trip_preserves_precision_and_bytes() {
        let values = vec![
            Value::BigInt(BigInt::from_str("123456789012345678901234567890123").unwrap()),
            Value::Decimal(
                BigDecimal::from_str("-0.000000000000000000000000000000000100").unwrap(),
            ),
            Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY),
            Value::Binary(vec![0, 255, 1, 2]),
            Value::ObjectId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
            Value::String("مرحبا بالعالم".into()),
            Value::Duration(Duration::from_total_nanos(-1_500_000_000).unwrap()),
        ];
        for v in values {
            let json = serde_json::to_string(&v).unwrap();
            let back: Value = serde_json::from_str(&json).unwrap();
            match (&v, &back) {
                (Value::Float(a), Value::Float(b)) if a.is_nan() => assert!(b.is_nan()),
                _ => assert_eq!(v, back, "json: {json}"),
            }
        }
    }

    #[test]
    fn decimal_scale_survives_serialization() {
        let d = BigDecimal::from_str("1.500").unwrap();
        let json = serde_json::to_string(&Value::Decimal(d)).unwrap();
        assert!(json.contains("1.500"), "{json}");
    }

    #[test]
    fn duration_normalizes_negative_values() {
        let d = Duration::from_total_nanos(-500_000_000).unwrap();
        assert_eq!(d.seconds, -1);
        assert_eq!(d.nanos, 500_000_000);
        assert_eq!(d.total_nanos(), -500_000_000);
    }

    #[test]
    fn mixed_array_infers_mixed_item_type() {
        let v = Value::Array(vec![Value::Int(1), Value::String("x".into())]);
        match v.canonical_type() {
            CanonicalType::Array(inner) => {
                assert!(matches!(*inner, CanonicalType::Mixed(_)));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
