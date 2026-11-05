//! MySQL ⇄ canonical type and value mapping.
//!
//! MySQL has no wide-range time-zone-aware timestamp (`TIMESTAMP` stops in
//! 2038), no duration beyond ±838 hours and no UUID type in MySQL proper.
//! Columns GazaBridge creates for such canonical types use a safe native
//! type plus a column comment marker (`gazabridge:<type>`) so inspection maps
//! them back exactly.
//!
//! Values are decoded from the binary protocol and then checked against the
//! column's canonical type with strict conversion; values that do not fit
//! are returned unchanged rather than altered.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use gazabridge_domain::conversion::{ConversionPolicy, convert, scalar_to_text, value_to_json};
use gazabridge_domain::value::Duration;
use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult, ErrorKind};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Column, Value as My};

pub const MARKER_PREFIX: &str = "gazabridge:";

/// Catalog description of a column (from `information_schema.COLUMNS`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MyType {
    pub data_type: String,
    pub column_type: String,
    pub precision: Option<u64>,
    pub scale: Option<u64>,
    pub char_length: Option<u64>,
    pub datetime_precision: Option<u64>,
    pub comment: String,
}

fn enum_labels(column_type: &str) -> Vec<String> {
    // enum('a','b''c') → ["a", "b'c"]
    let Some(inner) = column_type
        .find('(')
        .and_then(|i| column_type.rfind(')').map(|j| &column_type[i + 1..j]))
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, in_quote) {
            ('\'', true) if chars.peek() == Some(&'\'') => {
                cur.push('\'');
                chars.next();
            }
            ('\'', true) => {
                in_quote = false;
                out.push(std::mem::take(&mut cur));
            }
            ('\'', false) => in_quote = true,
            ('\\', true) => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            (c, true) => cur.push(c),
            _ => {}
        }
    }
    out
}

fn digits(p: Option<u64>) -> u8 {
    p.map_or(0, |p| p.min(6) as u8)
}

pub fn canonical_for(t: &MyType) -> CanonicalType {
    use CanonicalType as T;
    if let Some(marker) = t.comment.strip_prefix(MARKER_PREFIX) {
        let marker = marker.split_whitespace().next().unwrap_or("");
        match marker {
            "utc_datetime" => {
                return T::UtcDateTime {
                    fractional_digits: digits(t.datetime_precision),
                };
            }
            "zoned_datetime" => return T::ZonedDateTime,
            "duration" => return T::Duration,
            "interval" => return T::Interval,
            "uuid" => return T::Uuid,
            "object_id" => return T::ObjectId,
            "bigint" => return T::BigInt,
            _ => {}
        }
    }
    let unsigned = t.column_type.contains("unsigned");
    let int = |bits: u8| {
        if unsigned {
            T::UInt { bits }
        } else {
            T::Int { bits }
        }
    };
    match t.data_type.as_str() {
        "tinyint" if t.column_type.starts_with("tinyint(1)") => T::Boolean,
        "tinyint" => int(8),
        "smallint" => int(16),
        "mediumint" => int(24),
        "int" | "integer" => int(32),
        "bigint" => int(64),
        "decimal" | "numeric" => T::Decimal {
            precision: t.precision.map(|p| p as u32),
            scale: t.scale.map(|s| s as i32),
        },
        "float" => T::Float { bits: 32 },
        "double" | "real" => T::F64,
        "bit" => T::Binary {
            max_bytes: t.precision.map(|bits| bits.div_ceil(8)),
        },
        "char" | "varchar" => T::String {
            max_chars: t.char_length,
        },
        "tinytext" | "text" | "mediumtext" | "longtext" => T::TEXT,
        "binary" | "varbinary" => T::Binary {
            max_bytes: t.char_length,
        },
        "tinyblob" | "blob" | "mediumblob" | "longblob" => T::BYTES,
        "date" => T::Date,
        "time" => T::Time {
            fractional_digits: digits(t.datetime_precision),
        },
        "datetime" => T::LocalDateTime {
            fractional_digits: digits(t.datetime_precision),
        },
        // The session time zone is UTC, so TIMESTAMP values are instants.
        "timestamp" => T::UtcDateTime {
            fractional_digits: digits(t.datetime_precision),
        },
        "year" => T::Int { bits: 16 },
        "json" => T::Json,
        "enum" => T::Enum {
            name: None,
            labels: enum_labels(&t.column_type),
        },
        "uuid" => T::Uuid,
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => T::Geometry,
        other => T::Extension {
            name: other.to_owned(),
        },
    }
}

fn quote_label(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// Native column type and optional marker comment for a canonical type.
pub fn native_type_for(t: &CanonicalType) -> ConnectorResult<(String, Option<&'static str>)> {
    use CanonicalType as T;
    let plain = |s: &str| Ok((s.to_owned(), None));
    match t {
        T::Boolean => plain("tinyint(1)"),
        T::Int { bits } | T::UInt { bits } => {
            let base = match bits {
                0..=8 => "tinyint",
                9..=16 => "smallint",
                17..=24 => "mediumint",
                25..=32 => "int",
                _ => "bigint",
            };
            if matches!(t, T::UInt { .. }) {
                plain(&format!("{base} unsigned"))
            } else {
                plain(base)
            }
        }
        T::BigInt => Ok(("decimal(65,0)".into(), Some("gazabridge:bigint"))),
        T::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) if *p <= 65 && (0..=30).contains(s) && *s as u32 <= *p => {
                plain(&format!("decimal({p},{s})"))
            }
            (Some(p), None) if *p <= 65 => plain(&format!("decimal({p},0)")),
            (None, _) => plain("decimal(65,30)"),
            _ => Err(ConnectorError::new(
                ErrorKind::Unsupported,
                "MySQL decimals allow at most 65 digits and a scale of 30",
            )),
        },
        T::Float { bits } if *bits <= 32 => plain("float"),
        T::Float { .. } => plain("double"),
        T::String { max_chars: Some(n) } if *n <= 16_383 => plain(&format!("varchar({n})")),
        T::String { .. } => plain("longtext"),
        T::Binary { max_bytes: Some(n) } if *n <= 65_535 => plain(&format!("varbinary({n})")),
        T::Binary { .. } => plain("longblob"),
        T::Uuid => Ok(("char(36)".into(), Some("gazabridge:uuid"))),
        T::ObjectId => Ok(("char(24)".into(), Some("gazabridge:object_id"))),
        T::Date => plain("date"),
        T::Time { fractional_digits } => plain(&format!("time({})", (*fractional_digits).min(6))),
        T::LocalDateTime { fractional_digits } => {
            plain(&format!("datetime({})", (*fractional_digits).min(6)))
        }
        // DATETIME covers years 1000–9999; TIMESTAMP would stop in 2038.
        T::UtcDateTime { fractional_digits } => Ok((
            format!("datetime({})", (*fractional_digits).min(6)),
            Some("gazabridge:utc_datetime"),
        )),
        T::ZonedDateTime => Ok(("varchar(80)".into(), Some("gazabridge:zoned_datetime"))),
        T::Duration => Ok(("varchar(48)".into(), Some("gazabridge:duration"))),
        T::Interval => Ok(("varchar(64)".into(), Some("gazabridge:interval"))),
        T::Array(_) | T::Object | T::Map { .. } | T::Json => plain("json"),
        T::Enum { labels, .. } if !labels.is_empty() => plain(&format!(
            "enum({})",
            labels
                .iter()
                .map(|l| quote_label(l))
                .collect::<Vec<_>>()
                .join(",")
        )),
        T::Enum { .. } | T::Null => plain("longtext"),
        T::Geometry => plain("geometry"),
        T::Mixed(_) | T::Extension { .. } | T::Unknown => {
            Err(ConnectorError::unsupported(Capability::CreateTable))
        }
    }
}

/// Canonical type of a result-set column (for ad-hoc queries).
pub fn canonical_for_column(c: &Column) -> CanonicalType {
    use CanonicalType as T;
    let unsigned = c.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let binary = c.character_set() == 63;
    let int = |bits: u8| {
        if unsigned {
            T::UInt { bits }
        } else {
            T::Int { bits }
        }
    };
    match c.column_type() {
        ColumnType::MYSQL_TYPE_TINY if c.column_length() == 1 => T::Boolean,
        ColumnType::MYSQL_TYPE_TINY => int(8),
        ColumnType::MYSQL_TYPE_SHORT => int(16),
        ColumnType::MYSQL_TYPE_INT24 => int(24),
        ColumnType::MYSQL_TYPE_LONG => int(32),
        ColumnType::MYSQL_TYPE_LONGLONG => int(64),
        ColumnType::MYSQL_TYPE_YEAR => T::Int { bits: 16 },
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => T::Decimal {
            precision: None,
            scale: Some(i32::from(c.decimals())),
        },
        ColumnType::MYSQL_TYPE_FLOAT => T::Float { bits: 32 },
        ColumnType::MYSQL_TYPE_DOUBLE => T::F64,
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => T::Date,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => T::Time {
            fractional_digits: c.decimals().min(6),
        },
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => T::LocalDateTime {
            fractional_digits: c.decimals().min(6),
        },
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => T::UtcDateTime {
            fractional_digits: c.decimals().min(6),
        },
        ColumnType::MYSQL_TYPE_JSON => T::Json,
        ColumnType::MYSQL_TYPE_GEOMETRY => T::Geometry,
        ColumnType::MYSQL_TYPE_BIT => T::BYTES,
        ColumnType::MYSQL_TYPE_ENUM | ColumnType::MYSQL_TYPE_SET => T::TEXT,
        ColumnType::MYSQL_TYPE_NULL => T::Null,
        _ if binary => T::BYTES,
        _ => T::TEXT,
    }
}

#[allow(clippy::too_many_arguments)] // mirrors the driver's Date variant
fn date_value(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32, as_date: bool) -> Value {
    let date = NaiveDate::from_ymd_opt(i32::from(y), u32::from(mo), u32::from(d));
    let time = NaiveTime::from_hms_micro_opt(u32::from(h), u32::from(mi), u32::from(s), us);
    match (date, time) {
        (Some(date), Some(_)) if as_date && (h, mi, s, us) == (0, 0, 0, 0) => Value::Date(date),
        (Some(date), Some(time)) => Value::LocalDateTime(NaiveDateTime::new(date, time)),
        // Zero dates such as 0000-00-00 have no calendar value.
        _ => Value::Extension {
            type_name: if as_date { "date" } else { "datetime" }.into(),
            text: Some(format!(
                "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
            )),
            binary: None,
        },
    }
}

/// Converts a driver value into a raw canonical value.
pub fn raw_value(v: My, binary: bool, is_date_column: bool) -> Value {
    match v {
        My::NULL => Value::Null,
        My::Int(i) => Value::Int(i),
        My::UInt(u) => Value::UInt(u),
        My::Float(f) => Value::Float(f64::from(f)),
        My::Double(f) => Value::Float(f),
        My::Bytes(b) if binary => Value::Binary(b),
        My::Bytes(b) => match String::from_utf8(b) {
            Ok(s) => Value::String(s),
            Err(e) => Value::Binary(e.into_bytes()),
        },
        My::Date(y, mo, d, h, mi, s, us) => date_value(y, mo, d, h, mi, s, us, is_date_column),
        My::Time(neg, days, h, m, s, us) => {
            if !neg && days == 0 {
                NaiveTime::from_hms_micro_opt(u32::from(h), u32::from(m), u32::from(s), us)
                    .map_or(Value::Null, Value::Time)
            } else {
                let secs = i128::from(days) * 86_400
                    + i128::from(h) * 3600
                    + i128::from(m) * 60
                    + i128::from(s);
                let total = secs * 1_000_000_000 + i128::from(us) * 1000;
                Duration::from_total_nanos(if neg { -total } else { total })
                    .map_or(Value::Null, Value::Duration)
            }
        }
    }
}

/// Parses MySQL's internal geometry format: little-endian SRID + WKB.
fn decode_geometry(raw: &[u8]) -> Option<Value> {
    if raw.len() < 9 {
        return None;
    }
    let srid = u32::from_le_bytes(raw[..4].try_into().ok()?);
    Some(Value::Geometry {
        srid: (srid != 0).then_some(srid),
        wkb: raw[4..].to_vec(),
    })
}

/// Decodes a raw value for a column of canonical type `t`.
pub fn decode(raw: Value, t: &CanonicalType) -> Value {
    if raw.is_absent() {
        return raw;
    }
    let strict = ConversionPolicy::strict();
    match (t, &raw) {
        (CanonicalType::Mixed(_) | CanonicalType::Unknown | CanonicalType::Extension { .. }, _) => {
            raw
        }
        // Session time zone is UTC; DATETIME/TIMESTAMP values are UTC wall clock.
        (CanonicalType::UtcDateTime { .. }, Value::LocalDateTime(dt)) => {
            Value::UtcDateTime(dt.and_utc())
        }
        (CanonicalType::Geometry, Value::Binary(b)) => decode_geometry(b).unwrap_or(raw),
        (CanonicalType::Geometry, Value::String(s)) => decode_geometry(s.as_bytes()).unwrap_or(raw),
        (CanonicalType::Enum { .. }, Value::String(s)) => Value::Enum {
            type_name: None,
            label: s.clone(),
        },
        (CanonicalType::Boolean, Value::Int(0 | 1)) => {
            convert(&raw, t, &strict).map_or(raw, |c| c.value)
        }
        _ => convert(&raw, t, &strict).map_or(raw, |c| c.value),
    }
}

fn conversion(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ErrorKind::Conversion, msg)
}

fn text(v: &Value) -> ConnectorResult<My> {
    scalar_to_text(v)
        .map(|s| My::Bytes(s.into_bytes()))
        .ok_or_else(|| conversion(format!("{} value has no text form", v.kind())))
}

fn mysql_datetime(dt: &NaiveDateTime) -> My {
    My::Bytes(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string().into_bytes())
}

fn time_from_duration(d: &Duration) -> ConnectorResult<My> {
    let total = d.total_nanos();
    if total % 1000 != 0 {
        return Err(conversion("MySQL TIME has microsecond resolution"));
    }
    let neg = total < 0;
    let micros = total.unsigned_abs() / 1000;
    let secs = micros / 1_000_000;
    if secs > 838 * 3600 + 59 * 60 + 59 {
        return Err(conversion("duration exceeds MySQL TIME range (±838:59:59)"));
    }
    Ok(My::Time(
        neg,
        (secs / 86_400) as u32,
        (secs % 86_400 / 3600) as u8,
        (secs % 3600 / 60) as u8,
        (secs % 60) as u8,
        (micros % 1_000_000) as u32,
    ))
}

/// Encodes a canonical value as a statement parameter for a column of type
/// `column` (when known). Values MySQL cannot store exactly are errors.
pub fn encode(v: &Value, column: Option<&CanonicalType>) -> ConnectorResult<My> {
    use CanonicalType as T;
    // JSON columns take the JSON form of any value (a string becomes a JSON
    // string), as PostgreSQL's jsonb columns do.
    if matches!(
        column,
        Some(T::Json | T::Object | T::Map { .. } | T::Array(_))
    ) && !matches!(v, Value::Null | Value::Missing | Value::Json(_))
    {
        let (json, exact) = value_to_json(v);
        if !exact {
            return Err(conversion(
                "value contains types without a JSON form; convert it to JSON first",
            ));
        }
        return Ok(My::Bytes(json.to_string().into_bytes()));
    }
    Ok(match v {
        Value::Null | Value::Missing => My::NULL,
        Value::Bool(b) => My::Int(i64::from(*b)),
        Value::Int(i) => My::Int(*i),
        Value::UInt(u) => My::UInt(*u),
        Value::BigInt(_) | Value::Decimal(_) => text(v)?,
        Value::Float(f) if f.is_finite() => My::Double(*f),
        Value::Float(_) => {
            return Err(conversion(
                "MySQL cannot store NaN or infinite floating-point values",
            ));
        }
        Value::String(s) => My::Bytes(s.clone().into_bytes()),
        Value::Binary(b) => My::Bytes(b.clone()),
        Value::Uuid(u) => match column {
            Some(T::Binary { .. }) => My::Bytes(u.as_bytes().to_vec()),
            _ => My::Bytes(u.hyphenated().to_string().into_bytes()),
        },
        Value::Date(d) => My::Bytes(d.format("%Y-%m-%d").to_string().into_bytes()),
        Value::Time(t) => My::Bytes(t.format("%H:%M:%S%.6f").to_string().into_bytes()),
        Value::LocalDateTime(dt) => mysql_datetime(dt),
        Value::UtcDateTime(dt) => match column {
            Some(T::String { .. } | T::ZonedDateTime) => text(v)?,
            _ => mysql_datetime(&dt.naive_utc()),
        },
        Value::ZonedDateTime { .. } => match column {
            Some(T::ZonedDateTime | T::String { .. }) | None => text(v)?,
            _ => {
                return Err(conversion(
                    "zoned date-times must be converted to the column type first",
                ));
            }
        },
        Value::Duration(d) => match column {
            Some(T::Time { .. }) => time_from_duration(d)?,
            _ => text(v)?,
        },
        Value::Interval(_) | Value::ObjectId(_) | Value::Enum { .. } => text(v)?,
        Value::Json(j) => My::Bytes(j.to_string().into_bytes()),
        Value::Array(_) | Value::Object(_) | Value::Map(_) => {
            let (json, exact) = value_to_json(v);
            if !exact {
                return Err(conversion(
                    "nested value contains types without a JSON form; convert it to JSON first",
                ));
            }
            My::Bytes(json.to_string().into_bytes())
        }
        Value::Geometry { srid, wkb } => {
            let mut out = srid.unwrap_or(0).to_le_bytes().to_vec();
            out.extend_from_slice(wkb);
            My::Bytes(out)
        }
        Value::Extension { text: Some(t), .. } => My::Bytes(t.clone().into_bytes()),
        Value::Extension {
            binary: Some(b), ..
        } => My::Bytes(b.clone()),
        Value::Extension { .. } => return Err(conversion("empty extension value")),
        Value::Invalid { reason, .. } => {
            return Err(conversion(format!("invalid source value: {reason}")));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(data_type: &str, column_type: &str) -> MyType {
        MyType {
            data_type: data_type.into(),
            column_type: column_type.into(),
            ..Default::default()
        }
    }

    #[test]
    fn catalog_types_map() {
        assert_eq!(
            canonical_for(&ty("tinyint", "tinyint(1)")),
            CanonicalType::Boolean
        );
        assert_eq!(
            canonical_for(&ty("int", "int unsigned")),
            CanonicalType::UInt { bits: 32 }
        );
        assert_eq!(canonical_for(&ty("bigint", "bigint")), CanonicalType::I64);
        let e = canonical_for(&ty("enum", "enum('a','it''s','b\\\\c')"));
        assert_eq!(
            e,
            CanonicalType::Enum {
                name: None,
                labels: vec!["a".into(), "it's".into(), "b\\c".into()]
            }
        );
        let mut utc = ty("datetime", "datetime(3)");
        utc.datetime_precision = Some(3);
        utc.comment = "gazabridge:utc_datetime".into();
        assert_eq!(
            canonical_for(&utc),
            CanonicalType::UtcDateTime {
                fractional_digits: 3
            }
        );
    }

    #[test]
    fn created_types_round_trip_through_the_catalog() {
        use CanonicalType as T;
        for t in [
            T::Boolean,
            T::Int { bits: 32 },
            T::UInt { bits: 64 },
            T::BigInt,
            T::Uuid,
            T::ObjectId,
            T::Duration,
            T::Interval,
            T::ZonedDateTime,
        ] {
            let (native, marker) = native_type_for(&t).unwrap();
            let data_type = native.split(['(', ' ']).next().unwrap().to_owned();
            let my = MyType {
                data_type,
                column_type: native.clone(),
                comment: marker.unwrap_or_default().to_owned(),
                ..Default::default()
            };
            assert_eq!(canonical_for(&my), t, "{native}");
        }
        assert!(
            native_type_for(&T::Decimal {
                precision: Some(70),
                scale: Some(2)
            })
            .is_err()
        );
    }

    #[test]
    fn json_columns_take_the_json_form() {
        let j = Some(&CanonicalType::Json);
        assert_eq!(
            encode(&Value::String("text".into()), j).unwrap(),
            My::Bytes(br#""text""#.to_vec())
        );
        assert_eq!(encode(&Value::Int(3), j).unwrap(), My::Bytes(b"3".to_vec()));
        assert_eq!(encode(&Value::Null, j).unwrap(), My::NULL);
        assert_eq!(
            encode(&Value::String("text".into()), None).unwrap(),
            My::Bytes(b"text".to_vec())
        );
    }

    #[test]
    fn values_encode_and_decode_safely() {
        assert!(encode(&Value::Float(f64::NAN), None).is_err());
        let d = Duration::from_total_nanos(-(3_600_000_000_000i128 * 30)).unwrap();
        assert_eq!(
            encode(
                &Value::Duration(d),
                Some(&CanonicalType::Time {
                    fractional_digits: 0
                })
            )
            .unwrap(),
            My::Time(true, 1, 6, 0, 0, 0)
        );
        // 30h back out of TIME is a duration, not a time of day.
        assert!(matches!(
            raw_value(My::Time(true, 1, 6, 0, 0, 0), false, false),
            Value::Duration(_)
        ));
        assert!(matches!(
            raw_value(My::Date(0, 0, 0, 0, 0, 0, 0), false, true),
            Value::Extension { .. }
        ));
        assert_eq!(
            decode(Value::Int(2), &CanonicalType::Boolean),
            Value::Int(2)
        );
        let geom = encode(
            &Value::Geometry {
                srid: Some(4326),
                wkb: vec![1, 1, 0, 0, 0, 0, 0, 0, 0],
            },
            None,
        )
        .unwrap();
        let My::Bytes(b) = geom else { panic!() };
        assert_eq!(
            decode(Value::Binary(b), &CanonicalType::Geometry),
            Value::Geometry {
                srid: Some(4326),
                wkb: vec![1, 1, 0, 0, 0, 0, 0, 0, 0]
            }
        );
    }
}
