//! PostgreSQL ⇄ canonical type and value mapping.
//!
//! * Parameters are always sent in **text format** ([`TextParam`]); the server
//!   parses them with the target type's input function, which is exact for
//!   every PostgreSQL type including extensions.
//! * Results are decoded from the **binary format** for core types
//!   ([`decode_binary`]). Other types are selected as `::text` by table reads
//!   and surface as [`Value::Extension`] with their text form, so nothing is
//!   lost.

use std::error::Error;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use bytes::BytesMut;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use gazabridge_domain::conversion::{scalar_to_text, value_to_json};
use gazabridge_domain::value::Interval;
use gazabridge_domain::{CanonicalType, Value};
use gazabridge_plugin_sdk::{Capability, ConnectorError, ConnectorResult, ErrorKind};
use tokio_postgres::types::{Format, FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// A parameter sent in PostgreSQL text format (`None` is SQL NULL).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextParam(pub Option<String>);

impl ToSql for TextParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match &self.0 {
            Some(s) => {
                out.extend_from_slice(s.as_bytes());
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

pub fn as_params(params: &[TextParam]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

/// What the encoder needs to know about the target column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnShape {
    Scalar,
    Array,
    Json,
    Unknown,
}

fn conversion(msg: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ErrorKind::Conversion, msg)
}

fn array_element_text(v: &Value) -> ConnectorResult<String> {
    match v {
        Value::Null | Value::Missing => Ok("NULL".into()),
        Value::Array(items) => array_literal(items),
        other => {
            let raw = match other {
                Value::Binary(b) => format!("\\x{}", hex::encode(b)),
                Value::Object(_) | Value::Map(_) | Value::Json(_) => json_text(other)?,
                _ => scalar_text(other)?,
            };
            Ok(format!(
                "\"{}\"",
                raw.replace('\\', "\\\\").replace('"', "\\\"")
            ))
        }
    }
}

/// PostgreSQL array literal, e.g. `{"a","b c",NULL}`.
pub fn array_literal(items: &[Value]) -> ConnectorResult<String> {
    let parts = items
        .iter()
        .map(array_element_text)
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(format!("{{{}}}", parts.join(",")))
}

fn json_text(v: &Value) -> ConnectorResult<String> {
    if let Value::Json(j) = v {
        return Ok(j.to_string());
    }
    let (json, exact) = value_to_json(v);
    if !exact {
        return Err(conversion(
            "nested value contains types without a JSON form; convert it to JSON first",
        ));
    }
    Ok(json.to_string())
}

fn scalar_text(v: &Value) -> ConnectorResult<String> {
    match v {
        Value::ZonedDateTime { zone: Some(_), .. } => Err(conversion(
            "PostgreSQL stores instants without zone names; convert to the column type first",
        )),
        Value::ZonedDateTime {
            instant,
            zone: None,
        } => Ok(instant.to_rfc3339()),
        Value::Geometry { srid, wkb } => Ok(hex::encode(ewkb(*srid, wkb)?)),
        _ => scalar_to_text(v)
            .ok_or_else(|| conversion(format!("{} value has no text form", v.kind()))),
    }
}

/// Adds an SRID to WKB, producing PostGIS EWKB.
fn ewkb(srid: Option<u32>, wkb: &[u8]) -> ConnectorResult<Vec<u8>> {
    let Some(srid) = srid else {
        return Ok(wkb.to_vec());
    };
    if wkb.len() < 5 {
        return Err(conversion("geometry is not valid WKB"));
    }
    let little = wkb[0] == 1;
    let raw: [u8; 4] = [wkb[1], wkb[2], wkb[3], wkb[4]];
    let ty = if little {
        u32::from_le_bytes(raw)
    } else {
        u32::from_be_bytes(raw)
    } | 0x2000_0000;
    let mut out = Vec::with_capacity(wkb.len() + 4);
    out.push(wkb[0]);
    if little {
        out.extend_from_slice(&ty.to_le_bytes());
        out.extend_from_slice(&srid.to_le_bytes());
    } else {
        out.extend_from_slice(&ty.to_be_bytes());
        out.extend_from_slice(&srid.to_be_bytes());
    }
    out.extend_from_slice(&wkb[5..]);
    Ok(out)
}

/// Encodes a canonical value as a text-format parameter.
pub fn encode(v: &Value, shape: ColumnShape) -> ConnectorResult<TextParam> {
    let text = match v {
        Value::Null | Value::Missing => return Ok(TextParam(None)),
        Value::Binary(b) => format!("\\x{}", hex::encode(b)),
        Value::Array(items) => match shape {
            ColumnShape::Json => json_text(v)?,
            ColumnShape::Array => array_literal(items)?,
            _ => {
                return Err(conversion(
                    "arrays can only be written to array or JSON columns",
                ));
            }
        },
        Value::Object(_) | Value::Map(_) => json_text(v)?,
        Value::Json(j) => j.to_string(),
        Value::Extension { text: Some(t), .. } => t.clone(),
        Value::Extension { .. } => {
            return Err(conversion(
                "binary-only extension values cannot be written to PostgreSQL",
            ));
        }
        Value::Invalid { reason, .. } => {
            return Err(conversion(format!("invalid source value: {reason}")));
        }
        other => scalar_text(other)?,
    };
    Ok(TextParam(Some(text)))
}

// ---------------------------------------------------------------------------
// Catalog types → canonical types
// ---------------------------------------------------------------------------

/// A column type as described by the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PgType {
    pub name: String,
    pub schema: String,
    /// `pg_type.typtype`: b, c, d, e, p, r, m.
    pub typtype: String,
    pub typmod: i32,
    pub element: Option<Box<PgType>>,
    pub base: Option<Box<PgType>>,
    pub enum_labels: Vec<String>,
}

fn fractional(typmod: i32) -> u8 {
    if typmod < 0 { 6 } else { typmod.min(6) as u8 }
}

pub fn numeric_typmod(typmod: i32) -> (Option<u32>, Option<i32>) {
    if typmod < 4 {
        return (None, None);
    }
    let t = typmod - 4;
    let precision = ((t >> 16) & 0xffff) as u32;
    let mut scale = t & 0x7ff;
    if scale >= 1024 {
        scale -= 2048;
    }
    (Some(precision), Some(scale))
}

fn char_length(typmod: i32) -> Option<u64> {
    (typmod >= 4).then(|| (typmod - 4) as u64)
}

/// Canonical type for a catalog type. Types GazaBridge does not interpret
/// map to [`CanonicalType::Extension`].
pub fn canonical_for(t: &PgType) -> CanonicalType {
    use CanonicalType as T;
    match t.typtype.as_str() {
        "d" => {
            if let Some(base) = &t.base {
                return canonical_for(base);
            }
        }
        "e" => {
            return T::Enum {
                name: Some(t.name.clone()),
                labels: t.enum_labels.clone(),
            };
        }
        "c" | "r" | "m" | "p" => {
            return T::Extension {
                name: t.name.clone(),
            };
        }
        _ => {}
    }
    if let Some(elem) = &t.element {
        return match canonical_for(elem) {
            T::Extension { .. } => T::Extension {
                name: t.name.clone(),
            },
            inner => T::Array(Box::new(inner)),
        };
    }
    match t.name.as_str() {
        "bool" => T::Boolean,
        "int2" => T::Int { bits: 16 },
        "int4" => T::Int { bits: 32 },
        "int8" => T::I64,
        "oid" | "xid" | "cid" => T::UInt { bits: 32 },
        "numeric" => {
            let (precision, scale) = numeric_typmod(t.typmod);
            T::Decimal { precision, scale }
        }
        "float4" => T::Float { bits: 32 },
        "float8" => T::F64,
        "text" | "citext" | "xml" | "unknown" => T::TEXT,
        "varchar" | "bpchar" => T::String {
            max_chars: char_length(t.typmod),
        },
        "name" => T::String {
            max_chars: Some(63),
        },
        "char" => T::String { max_chars: Some(1) },
        "bytea" => T::BYTES,
        "uuid" => T::Uuid,
        "date" => T::Date,
        "time" => T::Time {
            fractional_digits: fractional(t.typmod),
        },
        "timestamp" => T::LocalDateTime {
            fractional_digits: fractional(t.typmod),
        },
        "timestamptz" => T::UtcDateTime {
            fractional_digits: fractional(t.typmod),
        },
        "interval" => T::Interval,
        "json" | "jsonb" => T::Json,
        other => T::Extension {
            name: other.to_owned(),
        },
    }
}

/// Native column type used when GazaBridge creates a column.
pub fn native_type_for(t: &CanonicalType) -> ConnectorResult<String> {
    use CanonicalType as T;
    Ok(match t {
        T::Boolean => "boolean".into(),
        T::Int { bits } if *bits <= 16 => "smallint".into(),
        T::Int { bits } if *bits <= 32 => "integer".into(),
        T::Int { .. } => "bigint".into(),
        T::UInt { bits } if *bits <= 8 => "smallint".into(),
        T::UInt { bits } if *bits <= 16 => "integer".into(),
        T::UInt { bits } if *bits <= 32 => "bigint".into(),
        T::UInt { .. } => "numeric(20,0)".into(),
        T::BigInt => "numeric".into(),
        T::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) if *p <= 1000 => format!("numeric({p},{s})"),
            (Some(p), None) if *p <= 1000 => format!("numeric({p})"),
            _ => "numeric".into(),
        },
        T::Float { bits } if *bits <= 32 => "real".into(),
        T::Float { .. } => "double precision".into(),
        T::String { max_chars: Some(n) } if *n <= 10_485_760 => format!("varchar({n})"),
        T::String { .. } | T::Enum { .. } => "text".into(),
        T::Binary { .. } => "bytea".into(),
        T::Uuid => "uuid".into(),
        T::ObjectId => "character(24)".into(),
        T::Date => "date".into(),
        T::Time { fractional_digits } => format!("time({})", (*fractional_digits).min(6)),
        T::LocalDateTime { fractional_digits } => {
            format!("timestamp({})", (*fractional_digits).min(6))
        }
        T::UtcDateTime { fractional_digits } => {
            format!("timestamptz({})", (*fractional_digits).min(6))
        }
        // Zone names are not stored; converting to this column is classified
        // as lossy and needs approval.
        T::ZonedDateTime => "timestamptz".into(),
        T::Duration | T::Interval => "interval".into(),
        T::Array(inner) => {
            let mut base = inner.as_ref();
            while let T::Array(i) = base {
                base = i;
            }
            if matches!(base, T::Array(_) | T::Object | T::Map { .. } | T::Json) {
                "jsonb".into()
            } else {
                format!("{}[]", native_type_for(base)?)
            }
        }
        T::Object | T::Map { .. } | T::Json => "jsonb".into(),
        T::Geometry => "geometry".into(),
        T::Null => "text".into(),
        T::Mixed(_) | T::Extension { .. } | T::Unknown => {
            return Err(ConnectorError::unsupported(Capability::CreateTable));
        }
    })
}

/// Shape of a column for parameter encoding.
pub fn shape_of(t: &CanonicalType) -> ColumnShape {
    match t {
        CanonicalType::Array(_) => ColumnShape::Array,
        CanonicalType::Json | CanonicalType::Object | CanonicalType::Map { .. } => {
            ColumnShape::Json
        }
        _ => ColumnShape::Scalar,
    }
}

/// Whether table reads must select the column as `::text`.
pub fn needs_text_cast(t: &CanonicalType) -> bool {
    matches!(t, CanonicalType::Extension { .. })
}

// ---------------------------------------------------------------------------
// Binary result decoding
// ---------------------------------------------------------------------------

/// Raw bytes of a result column in binary format.
#[derive(Debug)]
pub struct Raw<'a>(pub Option<&'a [u8]>);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Raw(Some(raw)))
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Raw(None))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

fn pg_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .unwrap_or_default()
}

fn extension_binary(ty: &Type, raw: &[u8]) -> Value {
    Value::Extension {
        type_name: ty.name().to_owned(),
        text: None,
        binary: Some(raw.to_vec()),
    }
}

fn extension_text(ty: &Type, text: &str) -> Value {
    Value::Extension {
        type_name: ty.name().to_owned(),
        text: Some(text.to_owned()),
        binary: None,
    }
}

fn be<const N: usize>(raw: &[u8], at: usize) -> Option<[u8; N]> {
    raw.get(at..at + N)?.try_into().ok()
}

/// Decodes PostgreSQL's binary NUMERIC format exactly.
pub fn decode_numeric(raw: &[u8]) -> Option<Value> {
    let ndigits = u16::from_be_bytes(be(raw, 0)?) as usize;
    let weight = i16::from_be_bytes(be(raw, 2)?) as i64;
    let sign = u16::from_be_bytes(be(raw, 4)?);
    let dscale = u16::from_be_bytes(be(raw, 6)?) as i64;
    match sign {
        0xC000 => return Some(Value::Float(f64::NAN)),
        0xD000 => return Some(Value::Float(f64::INFINITY)),
        0xF000 => return Some(Value::Float(f64::NEG_INFINITY)),
        0x0000 | 0x4000 => {}
        _ => return None,
    }
    let mut unscaled = BigInt::from(0);
    for i in 0..ndigits {
        let d = u16::from_be_bytes(be(raw, 8 + 2 * i)?);
        if d >= 10_000 {
            return None;
        }
        unscaled = unscaled * 10_000 + d;
    }
    if sign == 0x4000 {
        unscaled = -unscaled;
    }
    // value = unscaled × 10000^(weight − ndigits + 1)
    let exp10 = 4 * (weight - ndigits as i64 + 1);
    let value = if ndigits == 0 {
        BigDecimal::new(BigInt::from(0), 0)
    } else {
        BigDecimal::new(unscaled, -exp10)
    };
    Some(Value::Decimal(value.with_scale(dscale)))
}

fn decode_array(elem: &Type, raw: &[u8]) -> Option<Value> {
    let ndim = i32::from_be_bytes(be(raw, 0)?);
    if ndim == 0 {
        return Some(Value::Array(Vec::new()));
    }
    if !(1..=6).contains(&ndim) {
        return None;
    }
    let mut pos = 12;
    let mut dims = Vec::with_capacity(ndim as usize);
    for _ in 0..ndim {
        let len = i32::from_be_bytes(be(raw, pos)?);
        let lower = i32::from_be_bytes(be(raw, pos + 4)?);
        if lower != 1 || len < 0 {
            // Non-default bounds cannot be represented without loss.
            return None;
        }
        dims.push(len as usize);
        pos += 8;
    }
    let total: usize = dims.iter().product();
    let mut flat = Vec::with_capacity(total);
    for _ in 0..total {
        let len = i32::from_be_bytes(be(raw, pos)?);
        pos += 4;
        if len < 0 {
            flat.push(Value::Null);
        } else {
            let bytes = raw.get(pos..pos + len as usize)?;
            flat.push(decode_binary(elem, bytes));
            pos += len as usize;
        }
    }
    fn nest(flat: &mut std::vec::IntoIter<Value>, dims: &[usize]) -> Value {
        match dims {
            [n] => Value::Array(flat.by_ref().take(*n).collect()),
            [n, rest @ ..] => Value::Array((0..*n).map(|_| nest(flat, rest)).collect()),
            [] => Value::Array(Vec::new()),
        }
    }
    Some(nest(&mut flat.into_iter(), &dims))
}

fn format_timetz(micros: i64, west_offset_secs: i32) -> Option<String> {
    let t = NaiveTime::from_num_seconds_from_midnight_opt(
        u32::try_from(micros.div_euclid(1_000_000)).ok()?,
        u32::try_from(micros.rem_euclid(1_000_000) * 1000).ok()?,
    )?;
    let east = -west_offset_secs;
    let sign = if east < 0 { '-' } else { '+' };
    let a = east.unsigned_abs();
    Some(format!(
        "{}{sign}{:02}:{:02}:{:02}",
        t.format("%H:%M:%S%.f"),
        a / 3600,
        a % 3600 / 60,
        a % 60
    ))
}

/// Decodes one binary-format value of type `ty`.
///
/// Malformed input and types without a canonical decoding are returned as
/// [`Value::Extension`] with the raw bytes, never guessed.
pub fn decode_binary(ty: &Type, raw: &[u8]) -> Value {
    decode_known(ty, raw).unwrap_or_else(|| extension_binary(ty, raw))
}

fn decode_known(ty: &Type, raw: &[u8]) -> Option<Value> {
    match ty.kind() {
        Kind::Domain(base) => return Some(decode_binary(base, raw)),
        Kind::Enum(_) => {
            return Some(Value::Enum {
                type_name: Some(ty.name().to_owned()),
                label: std::str::from_utf8(raw).ok()?.to_owned(),
            });
        }
        Kind::Array(elem) => return decode_array(elem, raw),
        Kind::Simple => {}
        _ => return None,
    }
    if ty.name() == "citext" {
        return Some(Value::String(std::str::from_utf8(raw).ok()?.to_owned()));
    }
    Some(match *ty {
        Type::BOOL => Value::Bool(*raw.first()? != 0),
        Type::INT2 => Value::Int(i64::from(i16::from_be_bytes(be(raw, 0)?))),
        Type::INT4 => Value::Int(i64::from(i32::from_be_bytes(be(raw, 0)?))),
        Type::INT8 => Value::Int(i64::from_be_bytes(be(raw, 0)?)),
        Type::OID | Type::XID | Type::CID => {
            Value::UInt(u64::from(u32::from_be_bytes(be(raw, 0)?)))
        }
        Type::FLOAT4 => Value::Float(f64::from(f32::from_be_bytes(be(raw, 0)?))),
        Type::FLOAT8 => Value::Float(f64::from_be_bytes(be(raw, 0)?)),
        Type::NUMERIC => decode_numeric(raw)?,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN | Type::CHAR => {
            Value::String(std::str::from_utf8(raw).ok()?.to_owned())
        }
        Type::XML => Value::String(std::str::from_utf8(raw).ok()?.to_owned()),
        Type::BYTEA => Value::Binary(raw.to_vec()),
        Type::UUID => Value::Uuid(uuid::Uuid::from_slice(raw).ok()?),
        Type::DATE => {
            let days = i32::from_be_bytes(be(raw, 0)?);
            if days == i32::MAX || days == i32::MIN {
                return Some(extension_text(
                    ty,
                    if days == i32::MAX {
                        "infinity"
                    } else {
                        "-infinity"
                    },
                ));
            }
            Value::Date(
                pg_epoch()
                    .date()
                    .checked_add_signed(ChronoDuration::days(i64::from(days)))?,
            )
        }
        Type::TIME => {
            let micros = i64::from_be_bytes(be(raw, 0)?);
            Value::Time(NaiveTime::from_num_seconds_from_midnight_opt(
                u32::try_from(micros / 1_000_000).ok()?,
                u32::try_from(micros % 1_000_000 * 1000).ok()?,
            )?)
        }
        Type::TIMETZ => {
            let micros = i64::from_be_bytes(be(raw, 0)?);
            let offset = i32::from_be_bytes(be(raw, 8)?);
            extension_text(ty, &format_timetz(micros, offset)?)
        }
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            let micros = i64::from_be_bytes(be(raw, 0)?);
            if micros == i64::MAX || micros == i64::MIN {
                return Some(extension_text(
                    ty,
                    if micros == i64::MAX {
                        "infinity"
                    } else {
                        "-infinity"
                    },
                ));
            }
            let local = pg_epoch().checked_add_signed(ChronoDuration::microseconds(micros))?;
            if *ty == Type::TIMESTAMP {
                Value::LocalDateTime(local)
            } else {
                Value::UtcDateTime(DateTime::<Utc>::from_naive_utc_and_offset(local, Utc))
            }
        }
        Type::INTERVAL => Value::Interval(Interval {
            microseconds: i64::from_be_bytes(be(raw, 0)?),
            days: i32::from_be_bytes(be(raw, 8)?),
            months: i32::from_be_bytes(be(raw, 12)?),
        }),
        Type::JSON => Value::Json(serde_json::from_slice(raw).ok()?),
        Type::JSONB => {
            // Version byte (1) followed by JSON text.
            if raw.first() != Some(&1) {
                return None;
            }
            Value::Json(serde_json::from_slice(&raw[1..]).ok()?)
        }
        _ => return None,
    })
}

/// Builds an extension value from a `::text`-cast column.
pub fn extension_from_text(type_name: &str, text: &str) -> Value {
    Value::Extension {
        type_name: type_name.to_owned(),
        text: Some(text.to_owned()),
        binary: None,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn numeric_bytes(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut v = Vec::new();
        for x in [ndigits, weight as u16, sign, dscale] {
            v.extend_from_slice(&x.to_be_bytes());
        }
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    #[test]
    fn numeric_binary_decoding_is_exact() {
        // 1234567.0089 → digits [123, 4567, 0089], weight 1, dscale 4
        let v = decode_numeric(&numeric_bytes(3, 1, 0, 4, &[123, 4567, 89])).unwrap();
        assert_eq!(
            v,
            Value::Decimal(BigDecimal::from_str("1234567.0089").unwrap())
        );
        // -0.00010 → digits [1], weight -1, dscale 5
        let v = decode_numeric(&numeric_bytes(1, -1, 0x4000, 5, &[1])).unwrap();
        assert_eq!(scalar_to_text(&v).unwrap(), "-0.00010");
        // 0 with scale 2
        let v = decode_numeric(&numeric_bytes(0, 0, 0, 2, &[])).unwrap();
        assert_eq!(scalar_to_text(&v).unwrap(), "0.00");
        // 10^30
        let v = decode_numeric(&numeric_bytes(1, 7, 0, 0, &[100])).unwrap();
        assert_eq!(
            scalar_to_text(&v).unwrap(),
            "1000000000000000000000000000000"
        );
        assert!(
            matches!(decode_numeric(&numeric_bytes(0, 0, 0xC000, 0, &[])), Some(Value::Float(f)) if f.is_nan())
        );
        assert!(decode_numeric(&[0, 1]).is_none());
    }

    #[test]
    fn typmods_decode() {
        // numeric(38,10): ((38 << 16) | 10) + 4
        assert_eq!(numeric_typmod((38 << 16 | 10) + 4), (Some(38), Some(10)));
        assert_eq!(numeric_typmod(-1), (None, None));
        let vc = PgType {
            name: "varchar".into(),
            typtype: "b".into(),
            typmod: 24,
            ..Default::default()
        };
        assert_eq!(
            canonical_for(&vc),
            CanonicalType::String {
                max_chars: Some(20)
            }
        );
    }

    #[test]
    fn created_types_map_back() {
        use CanonicalType as T;
        let cases = [
            (T::Boolean, "bool", -1),
            (T::Int { bits: 16 }, "int2", -1),
            (T::Int { bits: 32 }, "int4", -1),
            (T::I64, "int8", -1),
            (T::F64, "float8", -1),
            (T::TEXT, "text", -1),
            (T::BYTES, "bytea", -1),
            (T::Uuid, "uuid", -1),
            (T::Date, "date", -1),
            (
                T::Time {
                    fractional_digits: 6,
                },
                "time",
                6,
            ),
            (
                T::UtcDateTime {
                    fractional_digits: 3,
                },
                "timestamptz",
                3,
            ),
            (T::Interval, "interval", -1),
            (T::Json, "jsonb", -1),
        ];
        for (t, name, typmod) in cases {
            let pg = PgType {
                name: name.into(),
                typtype: "b".into(),
                typmod,
                ..Default::default()
            };
            assert_eq!(canonical_for(&pg), t, "{name}");
            assert!(native_type_for(&t).is_ok());
        }
        assert_eq!(
            native_type_for(&T::UInt { bits: 64 }).unwrap(),
            "numeric(20,0)"
        );
        assert_eq!(
            native_type_for(&T::Array(Box::new(T::Int { bits: 32 }))).unwrap(),
            "integer[]"
        );
    }

    #[test]
    fn arrays_and_enums_and_domains_map() {
        let int4 = PgType {
            name: "int4".into(),
            typtype: "b".into(),
            typmod: -1,
            ..Default::default()
        };
        let arr = PgType {
            name: "_int4".into(),
            typtype: "b".into(),
            typmod: -1,
            element: Some(Box::new(int4.clone())),
            ..Default::default()
        };
        assert_eq!(
            canonical_for(&arr),
            CanonicalType::Array(Box::new(CanonicalType::Int { bits: 32 }))
        );
        let dom = PgType {
            name: "posint".into(),
            typtype: "d".into(),
            base: Some(Box::new(int4)),
            ..Default::default()
        };
        assert_eq!(canonical_for(&dom), CanonicalType::Int { bits: 32 });
        let inet = PgType {
            name: "inet".into(),
            typtype: "b".into(),
            ..Default::default()
        };
        assert!(needs_text_cast(&canonical_for(&inet)));
    }

    #[test]
    fn array_literals_quote_correctly() {
        let v = vec![
            Value::String("a\"b".into()),
            Value::Null,
            Value::Array(vec![Value::Int(1)]),
            Value::String("back\\slash".into()),
        ];
        assert_eq!(
            array_literal(&v).unwrap(),
            r#"{"a\"b",NULL,{"1"},"back\\slash"}"#
        );
    }

    #[test]
    fn encoding_rules() {
        assert_eq!(
            encode(&Value::Binary(vec![0xde, 0xad]), ColumnShape::Scalar).unwrap(),
            TextParam(Some("\\xdead".into()))
        );
        assert_eq!(
            encode(&Value::Float(f64::NAN), ColumnShape::Scalar).unwrap(),
            TextParam(Some("NaN".into()))
        );
        assert!(encode(&Value::Array(vec![]), ColumnShape::Scalar).is_err());
        let zoned = Value::ZonedDateTime {
            instant: chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00+02:00").unwrap(),
            zone: Some("Asia/Gaza".into()),
        };
        assert!(encode(&zoned, ColumnShape::Scalar).is_err());
    }

    #[test]
    fn temporal_binary_decoding() {
        let d = decode_binary(&Type::DATE, &0i32.to_be_bytes());
        assert_eq!(d, Value::Date(NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()));
        let inf = decode_binary(&Type::TIMESTAMPTZ, &i64::MAX.to_be_bytes());
        assert!(matches!(inf, Value::Extension { text: Some(ref t), .. } if t == "infinity"));
        let mut iv = Vec::new();
        iv.extend_from_slice(&(-4_500_000i64).to_be_bytes());
        iv.extend_from_slice(&3i32.to_be_bytes());
        iv.extend_from_slice(&(-14i32).to_be_bytes());
        assert_eq!(
            decode_binary(&Type::INTERVAL, &iv),
            Value::Interval(Interval {
                months: -14,
                days: 3,
                microseconds: -4_500_000
            })
        );
        let mut tz = Vec::new();
        tz.extend_from_slice(&(3_600_000_000i64).to_be_bytes());
        tz.extend_from_slice(&(-7200i32).to_be_bytes());
        assert!(
            matches!(decode_binary(&Type::TIMETZ, &tz), Value::Extension { text: Some(ref t), .. } if t == "01:00:00+02:00:00")
        );
    }

    #[test]
    fn binary_arrays_decode() {
        // int4[] {1,NULL,3}
        let mut a = Vec::new();
        for x in [1i32, 1, 23] {
            a.extend_from_slice(&x.to_be_bytes());
        }
        a.extend_from_slice(&3i32.to_be_bytes());
        a.extend_from_slice(&1i32.to_be_bytes());
        a.extend_from_slice(&4i32.to_be_bytes());
        a.extend_from_slice(&1i32.to_be_bytes());
        a.extend_from_slice(&(-1i32).to_be_bytes());
        a.extend_from_slice(&4i32.to_be_bytes());
        a.extend_from_slice(&3i32.to_be_bytes());
        assert_eq!(
            decode_binary(&Type::INT4_ARRAY, &a),
            Value::Array(vec![Value::Int(1), Value::Null, Value::Int(3)])
        );
    }
}
