//! Mapping between SQLite storage and canonical values.
//!
//! Declared names for text-encoded types must not contain `INT`: SQLite
//! checks for `INT` before `TEXT` when deriving affinity, so a name such as
//! `BIGINT_TEXT` would get INTEGER affinity and large values would be
//! silently converted to REAL. [`affinity`] mirrors SQLite's rules and a test
//! checks every name GazaBridge creates.
//!
//! SQLite columns have *affinities* derived from their declared type name,
//! not strict types. GazaBridge-created tables use declared names that both
//! select a safe affinity and record the canonical type, e.g.
//! `DECIMAL_TEXT(38,10)` has TEXT affinity (so no digits are lost to REAL
//! conversion) and maps back to `decimal(38,10)` on inspection.
//!
//! Reads decode the stored value according to the column's canonical type
//! using strict conversion; when the stored value does not fit (SQLite allows
//! any value in any column) the raw value is returned unchanged, so nothing
//! is altered silently.

use gazabridge_domain::conversion::{ConversionPolicy, convert, value_to_json};
use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult, ErrorKind};
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Row, Sqlite, TypeInfo, ValueRef};

/// A value ready to bind to a SQLite statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub type Query<'q> = sqlx::query::Query<'q, Sqlite, SqliteArguments<'q>>;

pub fn bind<'q>(q: Query<'q>, b: &Bind) -> Query<'q> {
    match b {
        Bind::Null => q.bind(None::<i64>),
        Bind::Int(i) => q.bind(*i),
        Bind::Real(f) => q.bind(*f),
        Bind::Text(s) => q.bind(s.clone()),
        Bind::Blob(v) => q.bind(v.clone()),
    }
}

pub fn bind_all<'q>(mut q: Query<'q>, binds: &[Bind]) -> Query<'q> {
    for b in binds {
        q = bind(q, b);
    }
    q
}

/// Declared type name used when GazaBridge creates a column.
pub fn native_type_for(t: &CanonicalType) -> ConnectorResult<String> {
    use CanonicalType as T;
    Ok(match t {
        T::Boolean => "BOOLEAN".into(),
        T::Int { bits } => match bits {
            0..=8 => "TINYINT",
            9..=16 => "SMALLINT",
            17..=24 => "MEDIUMINT",
            25..=32 => "INT",
            _ => "BIGINT",
        }
        .into(),
        T::UInt { bits } => match bits {
            0..=8 => "UNSIGNED TINYINT",
            9..=16 => "UNSIGNED SMALLINT",
            17..=24 => "UNSIGNED MEDIUMINT",
            25..=32 => "UNSIGNED INT",
            // Values above i64::MAX do not fit SQLite integers.
            _ => "U64_TEXT",
        }
        .into(),
        T::BigInt => "BIGNUM_TEXT".into(),
        T::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("DECIMAL_TEXT({p},{s})"),
            (Some(p), None) => format!("DECIMAL_TEXT({p})"),
            _ => "DECIMAL_TEXT".into(),
        },
        T::Float { bits } if *bits <= 32 => "FLOAT".into(),
        T::Float { .. } => "DOUBLE".into(),
        T::String { max_chars: Some(n) } => format!("VARCHAR({n})"),
        T::String { max_chars: None } => "TEXT".into(),
        T::Binary { .. } => "BLOB".into(),
        T::Uuid => "UUID".into(),
        T::ObjectId => "OBJECTID_TEXT".into(),
        T::Date => "DATE".into(),
        T::Time { fractional_digits } => format!("TIME({fractional_digits})"),
        T::LocalDateTime { fractional_digits } => format!("DATETIME({fractional_digits})"),
        T::UtcDateTime { fractional_digits } => format!("TIMESTAMPTZ({fractional_digits})"),
        T::ZonedDateTime => "ZONEDDATETIME_TEXT".into(),
        T::Duration => "DURATION_TEXT".into(),
        T::Interval => "PERIOD_TEXT".into(),
        T::Array(_) | T::Object | T::Map { .. } | T::Json => "JSON".into(),
        T::Enum { .. } => "TEXT".into(),
        T::Geometry => "GEOMETRY_BLOB".into(),
        // No declared type: the column stores any value as-is.
        T::Mixed(_) | T::Null => String::new(),
        T::Extension { .. } | T::Unknown => {
            return Err(ConnectorError::unsupported(Capability::CreateTable));
        }
    })
}

fn param(decl: &str) -> Option<(Option<u32>, Option<u32>)> {
    let open = decl.find('(')?;
    let close = decl.rfind(')')?;
    let inner = &decl[open + 1..close];
    let mut parts = inner.split(',').map(|p| p.trim().parse::<u32>().ok());
    Some((parts.next().flatten(), parts.next().flatten()))
}

/// Canonical type for a declared column type, following SQLite's affinity
/// rules for names GazaBridge did not create.
pub fn canonical_for_declared(decl: &str) -> CanonicalType {
    use CanonicalType as T;
    let upper = decl.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim().to_owned();
    let p = param(&upper);
    let digits = |d: u8| p.and_then(|(a, _)| a).map_or(d, |a| a.min(9) as u8);
    match base.as_str() {
        "" => return T::Mixed(Vec::new()),
        "BOOLEAN" | "BOOL" => return T::Boolean,
        "TINYINT" | "INT1" => return T::Int { bits: 8 },
        "SMALLINT" | "INT2" => return T::Int { bits: 16 },
        "MEDIUMINT" => return T::Int { bits: 24 },
        "INT" | "INT4" => return T::Int { bits: 32 },
        "INTEGER" | "BIGINT" | "INT8" => return T::I64,
        "UNSIGNED TINYINT" => return T::UInt { bits: 8 },
        "UNSIGNED SMALLINT" => return T::UInt { bits: 16 },
        "UNSIGNED MEDIUMINT" => return T::UInt { bits: 24 },
        "UNSIGNED INT" => return T::UInt { bits: 32 },
        "U64_TEXT" | "UNSIGNED BIG INT" => return T::UInt { bits: 64 },
        "BIGNUM_TEXT" => return T::BigInt,
        "DECIMAL_TEXT" => {
            return T::Decimal {
                precision: p.and_then(|(a, _)| a),
                scale: p.and_then(|(_, b)| b).map(|s| s as i32),
            };
        }
        "FLOAT" => return T::Float { bits: 32 },
        "DOUBLE" | "DOUBLE PRECISION" | "REAL" => return T::F64,
        "UUID" => return T::Uuid,
        "OBJECTID_TEXT" => return T::ObjectId,
        "DATE" => return T::Date,
        "TIME" => {
            return T::Time {
                fractional_digits: digits(6),
            };
        }
        "DATETIME" | "TIMESTAMP" => {
            return T::LocalDateTime {
                fractional_digits: digits(6),
            };
        }
        "TIMESTAMPTZ" => {
            return T::UtcDateTime {
                fractional_digits: digits(6),
            };
        }
        "ZONEDDATETIME_TEXT" => return T::ZonedDateTime,
        "DURATION_TEXT" => return T::Duration,
        "PERIOD_TEXT" => return T::Interval,
        "JSON" | "JSONB" => return T::Json,
        "GEOMETRY_BLOB" => return T::Geometry,
        _ => {}
    }
    match affinity(&upper) {
        Affinity::Integer => T::I64,
        Affinity::Text => T::String {
            max_chars: p.and_then(|(a, _)| a).map(u64::from),
        },
        Affinity::Blob => T::BYTES,
        Affinity::Real => T::F64,
        // NUMERIC affinity keeps at most ~15 significant digits; values may
        // already have been rounded by SQLite before GazaBridge sees them.
        Affinity::Numeric if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") => {
            T::Decimal {
                precision: p.and_then(|(a, _)| a),
                scale: p.and_then(|(_, b)| b).map(|s| s as i32),
            }
        }
        Affinity::Numeric => T::Mixed(Vec::new()),
    }
}

/// SQLite column affinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

/// Affinity SQLite derives from a declared type name
/// (<https://www.sqlite.org/datatype3.html#determination_of_column_affinity>).
pub fn affinity(decl: &str) -> Affinity {
    let upper = decl.to_ascii_uppercase();
    if upper.contains("INT") {
        Affinity::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        Affinity::Text
    } else if upper.contains("BLOB") || upper.trim().is_empty() {
        Affinity::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        Affinity::Real
    } else {
        Affinity::Numeric
    }
}

/// Whether the declared type stores numbers as text, where SQL comparison
/// operators would compare lexically.
pub fn is_text_encoded_number(t: &CanonicalType) -> bool {
    matches!(
        t,
        CanonicalType::BigInt | CanonicalType::UInt { bits: 64 } | CanonicalType::Decimal { .. }
    )
}

/// Reads column `idx` as a raw canonical value based on its storage class.
pub fn raw_value(row: &SqliteRow, idx: usize) -> ConnectorResult<Value> {
    let raw = row.try_get_raw(idx).map_err(crate::error::map_sqlx)?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    let class = raw.type_info().name().to_owned();
    drop(raw);
    let v = match class.as_str() {
        "INTEGER" => Value::Int(
            row.try_get_unchecked::<i64, _>(idx)
                .map_err(crate::error::map_sqlx)?,
        ),
        "REAL" => Value::Float(
            row.try_get_unchecked::<f64, _>(idx)
                .map_err(crate::error::map_sqlx)?,
        ),
        "TEXT" => Value::String(
            row.try_get_unchecked::<String, _>(idx)
                .map_err(crate::error::map_sqlx)?,
        ),
        "BLOB" => Value::Binary(
            row.try_get_unchecked::<Vec<u8>, _>(idx)
                .map_err(crate::error::map_sqlx)?,
        ),
        other => {
            return Err(ConnectorError::new(
                ErrorKind::Protocol,
                format!("unexpected SQLite storage class {other}"),
            ));
        }
    };
    Ok(v)
}

/// Decodes a raw stored value for a column of canonical type `t`.
pub fn decode(raw: Value, t: &CanonicalType) -> Value {
    if raw.is_absent() {
        return raw;
    }
    let strict = ConversionPolicy::strict();
    match t {
        CanonicalType::Mixed(_) | CanonicalType::Unknown => raw,
        CanonicalType::UtcDateTime { .. } => match convert(&raw, t, &strict) {
            Ok(c) => c.value,
            // Offsets other than UTC are kept as zoned values rather than
            // normalised.
            Err(_) => {
                convert(&raw, &CanonicalType::ZonedDateTime, &strict).map_or(raw, |c| c.value)
            }
        },
        CanonicalType::Float { .. } => match &raw {
            // Floats written by other tools may be text such as "NaN".
            Value::Float(_) => raw,
            _ => convert(&raw, &CanonicalType::F64, &strict).map_or(raw, |c| c.value),
        },
        CanonicalType::UInt { bits } if *bits < 64 => match raw {
            Value::Int(i) if i >= 0 => Value::UInt(i as u64),
            other => other,
        },
        _ => convert(&raw, t, &strict).map_or(raw, |c| c.value),
    }
}

fn text_of(v: &Value) -> ConnectorResult<String> {
    gazabridge_domain::conversion::scalar_to_text(v).ok_or_else(|| {
        ConnectorError::new(
            ErrorKind::Conversion,
            format!("{} value has no text form", v.kind()),
        )
    })
}

/// Encodes a canonical value for binding into a column of type `column`
/// (when known). Never loses information: values that cannot be stored
/// exactly produce a conversion error.
pub fn encode(v: &Value, column: Option<&CanonicalType>) -> ConnectorResult<Bind> {
    let text_column = column.is_some_and(is_text_encoded_number);
    Ok(match v {
        Value::Null | Value::Missing => Bind::Null,
        Value::Bool(b) => Bind::Int(i64::from(*b)),
        Value::Int(i) if text_column => Bind::Text(i.to_string()),
        Value::Int(i) => Bind::Int(*i),
        Value::UInt(u) if text_column => Bind::Text(u.to_string()),
        Value::UInt(u) => match i64::try_from(*u) {
            Ok(i) => Bind::Int(i),
            Err(_) => Bind::Text(u.to_string()),
        },
        Value::BigInt(_) | Value::Decimal(_) => Bind::Text(text_of(v)?),
        Value::Float(f) if f.is_finite() => Bind::Real(*f),
        // SQLite turns NaN into NULL; keep non-finite values as text.
        Value::Float(_) => Bind::Text(text_of(v)?),
        Value::String(s) => Bind::Text(s.clone()),
        Value::Binary(b) => Bind::Blob(b.clone()),
        Value::Geometry { wkb, .. } => Bind::Blob(wkb.clone()),
        Value::Uuid(_)
        | Value::ObjectId(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::LocalDateTime(_)
        | Value::UtcDateTime(_)
        | Value::ZonedDateTime { .. }
        | Value::Duration(_)
        | Value::Interval(_)
        | Value::Enum { .. }
        | Value::Json(_) => Bind::Text(text_of(v)?),
        Value::Array(_) | Value::Object(_) | Value::Map(_) => {
            let (json, exact) = value_to_json(v);
            if !exact {
                return Err(ConnectorError::new(
                    ErrorKind::Conversion,
                    "nested value contains types without a JSON form; convert it to JSON first",
                ));
            }
            Bind::Text(json.to_string())
        }
        Value::Extension { text: Some(t), .. } => Bind::Text(t.clone()),
        Value::Extension {
            binary: Some(b), ..
        } => Bind::Blob(b.clone()),
        Value::Extension { .. } => {
            return Err(ConnectorError::new(
                ErrorKind::Conversion,
                "empty extension value",
            ));
        }
        Value::Invalid { reason, .. } => {
            return Err(ConnectorError::new(
                ErrorKind::Conversion,
                format!("invalid source value: {reason}"),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_types_map_back_to_their_canonical_type() {
        use CanonicalType as T;
        for t in [
            T::Boolean,
            T::Int { bits: 8 },
            T::Int { bits: 16 },
            T::Int { bits: 32 },
            T::I64,
            T::UInt { bits: 32 },
            T::UInt { bits: 64 },
            T::BigInt,
            T::Decimal {
                precision: Some(38),
                scale: Some(10),
            },
            T::Float { bits: 32 },
            T::F64,
            T::String {
                max_chars: Some(20),
            },
            T::TEXT,
            T::BYTES,
            T::Uuid,
            T::ObjectId,
            T::Date,
            T::Time {
                fractional_digits: 6,
            },
            T::LocalDateTime {
                fractional_digits: 3,
            },
            T::UtcDateTime {
                fractional_digits: 6,
            },
            T::ZonedDateTime,
            T::Duration,
            T::Interval,
            T::Json,
            T::Geometry,
        ] {
            let decl = native_type_for(&t).unwrap();
            assert_eq!(canonical_for_declared(&decl), t, "{decl}");
        }
    }

    #[test]
    fn text_encoded_types_get_text_affinity() {
        use CanonicalType as T;
        for t in [
            T::UInt { bits: 64 },
            T::BigInt,
            T::Decimal {
                precision: Some(38),
                scale: Some(10),
            },
            T::ObjectId,
            T::ZonedDateTime,
            T::Duration,
            T::Interval,
            T::TEXT,
        ] {
            let decl = native_type_for(&t).unwrap();
            assert_eq!(affinity(&decl), Affinity::Text, "{t} → {decl}");
        }
        assert_eq!(
            affinity(&native_type_for(&T::Geometry).unwrap()),
            Affinity::Blob
        );
        assert_eq!(
            affinity(&native_type_for(&T::BYTES).unwrap()),
            Affinity::Blob
        );
        assert_eq!(
            affinity(&native_type_for(&T::I64).unwrap()),
            Affinity::Integer
        );
        assert_eq!(affinity(&native_type_for(&T::F64).unwrap()), Affinity::Real);
    }

    #[test]
    fn foreign_declared_types_follow_affinity_rules() {
        assert_eq!(canonical_for_declared("integer"), CanonicalType::I64);
        assert_eq!(
            canonical_for_declared("NVARCHAR(40)"),
            CanonicalType::String {
                max_chars: Some(40)
            }
        );
        assert_eq!(canonical_for_declared("blob"), CanonicalType::BYTES);
        assert_eq!(
            canonical_for_declared("double precision"),
            CanonicalType::F64
        );
        assert!(matches!(
            canonical_for_declared("NUMERIC(10,2)"),
            CanonicalType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
        ));
        assert!(matches!(
            canonical_for_declared("WHATEVER"),
            CanonicalType::Mixed(_)
        ));
    }

    #[test]
    fn mismatched_stored_values_are_returned_unchanged() {
        let v = decode(Value::String("not a date".into()), &CanonicalType::Date);
        assert_eq!(v, Value::String("not a date".into()));
        let b = decode(Value::Int(1), &CanonicalType::Boolean);
        assert_eq!(b, Value::Bool(true));
        let two = decode(Value::Int(2), &CanonicalType::Boolean);
        assert_eq!(two, Value::Int(2));
    }

    #[test]
    fn non_finite_floats_and_big_unsigned_are_stored_as_text() {
        assert_eq!(
            encode(&Value::Float(f64::NAN), None).unwrap(),
            Bind::Text("NaN".into())
        );
        assert_eq!(
            encode(&Value::UInt(u64::MAX), None).unwrap(),
            Bind::Text(u64::MAX.to_string())
        );
        assert_eq!(
            decode(Value::String("NaN".into()), &CanonicalType::F64).kind(),
            gazabridge_domain::ValueKind::Float
        );
    }

    #[test]
    fn non_json_nested_values_are_rejected() {
        let mut obj = gazabridge_domain::value::Value::Object(Default::default());
        if let Value::Object(m) = &mut obj {
            m.insert("b".into(), Value::Binary(vec![1]));
        }
        assert!(encode(&obj, None).is_err());
    }
}
