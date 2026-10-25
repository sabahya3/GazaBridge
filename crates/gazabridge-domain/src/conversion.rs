//! Loss-aware conversion between canonical values and canonical types.
//!
//! Two layers:
//!
//! * [`classify`] is a type-level, worst-case estimate used for planning,
//!   Dry Run reports and the mapping UI.
//! * [`convert`] performs a value-level conversion. It reports the loss that
//!   actually happened for that value (never worse than [`classify`]), and
//!   refuses any loss the [`ConversionPolicy`] has not approved.
//!
//! Nothing in this module drops information silently: every lossy result
//! carries a [`LossRecord`], and strict policies turn loss into errors.

use std::fmt;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, RoundingMode};
use chrono::{
    DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, TimeZone, Timelike,
    Utc,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::CanonicalType;
use crate::value::{Duration, Interval, Value, ValueKind};

/// Severity of a conversion, ordered from safest to most dangerous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversionClass {
    /// Every source value maps to exactly one target value and back.
    Lossless,
    /// Lossless for values that fit; values that do not fit are rejected
    /// with an error rather than altered.
    ConditionallyLossless,
    /// Some values may lose information (precision, range, type identity).
    PotentiallyLossy,
    /// Information is always discarded (e.g. dropping a time zone).
    DefinitelyLossy,
    /// No conversion is defined.
    Unsupported,
}

impl ConversionClass {
    pub fn is_lossy(&self) -> bool {
        matches!(self, Self::PotentiallyLossy | Self::DefinitelyLossy)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lossless => "lossless",
            Self::ConditionallyLossless => "conditionally_lossless",
            Self::PotentiallyLossy => "potentially_lossy",
            Self::DefinitelyLossy => "definitely_lossy",
            Self::Unsupported => "unsupported",
        }
    }
}

impl fmt::Display for ConversionClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which losses a job is allowed to apply. The default is strict: no loss.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversionPolicy {
    /// Approve conversions classified as potentially lossy.
    #[serde(default)]
    pub allow_potentially_lossy: bool,
    /// Approve conversions classified as definitely lossy.
    #[serde(default)]
    pub allow_definitely_lossy: bool,
    /// UTC offset (seconds east) assumed when a local date/time must become an
    /// instant. Without it such conversions fail.
    #[serde(default)]
    pub assume_utc_offset_seconds: Option<i32>,
}

impl ConversionPolicy {
    /// No loss of any kind is permitted.
    pub fn strict() -> Self {
        Self::default()
    }

    /// Every loss class is permitted (still recorded). Only for policies a
    /// user has explicitly approved.
    pub fn permissive() -> Self {
        Self {
            allow_potentially_lossy: true,
            allow_definitely_lossy: true,
            assume_utc_offset_seconds: None,
        }
    }

    pub fn allows(&self, class: ConversionClass) -> bool {
        match class {
            ConversionClass::Lossless | ConversionClass::ConditionallyLossless => true,
            ConversionClass::PotentiallyLossy => self.allow_potentially_lossy,
            ConversionClass::DefinitelyLossy => self.allow_definitely_lossy,
            ConversionClass::Unsupported => false,
        }
    }
}

/// A record of information lost while converting a single value.
///
/// It intentionally contains no data values, only kinds and a description,
/// so it is safe to log and include in reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LossRecord {
    pub class: ConversionClass,
    pub from: ValueKind,
    pub to: String,
    pub detail: String,
}

impl fmt::Display for LossRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} → {} ({}): {}",
            self.from, self.to, self.class, self.detail
        )
    }
}

/// Result of a successful conversion.
#[derive(Debug, Clone, PartialEq)]
pub struct Converted {
    pub value: Value,
    pub loss: Option<LossRecord>,
}

impl Converted {
    fn exact(value: Value) -> Self {
        Self { value, loss: None }
    }
}

/// Why a conversion failed. Messages never include the data value itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConversionError {
    #[error("{from} value is out of range for {to}: {reason}")]
    OutOfRange {
        from: ValueKind,
        to: String,
        reason: String,
    },
    #[error("{from} value cannot be parsed as {to}: {reason}")]
    InvalidFormat {
        from: ValueKind,
        to: String,
        reason: String,
    },
    #[error("conversion from {from} to {to} is not supported")]
    Unsupported { from: ValueKind, to: String },
    #[error("lossy conversion not approved by policy: {0}")]
    LossNotApproved(LossRecord),
    #[error("source value is invalid: {0}")]
    InvalidSource(String),
}

// ---------------------------------------------------------------------------
// Type-level classification
// ---------------------------------------------------------------------------

/// Worst-case classification of converting values of type `from` to `to`.
pub fn classify(from: &CanonicalType, to: &CanonicalType) -> ConversionClass {
    use CanonicalType as T;
    use ConversionClass::*;

    if from == to {
        return Lossless;
    }
    match (from, to) {
        (T::Null, _) => Lossless,
        (T::Mixed(parts), to) => parts
            .iter()
            .map(|p| classify(p, to))
            .max()
            .unwrap_or(Lossless),
        (from, T::Mixed(parts)) => parts
            .iter()
            .map(|p| classify(from, p))
            .min()
            .unwrap_or(Unsupported),

        // Integers
        (T::Boolean, T::Int { .. } | T::UInt { .. } | T::BigInt | T::Decimal { .. }) => Lossless,
        (T::Int { bits: a }, T::Int { bits: b }) => widen(*a <= *b),
        (T::UInt { bits: a }, T::UInt { bits: b }) => widen(*a <= *b),
        (T::UInt { bits: a }, T::Int { bits: b }) => widen(*a < *b),
        (T::Int { .. }, T::UInt { .. }) => ConditionallyLossless,
        (T::Int { .. } | T::UInt { .. }, T::BigInt) => Lossless,
        (T::BigInt, T::Int { .. } | T::UInt { .. }) => ConditionallyLossless,
        (T::Int { bits } | T::UInt { bits }, T::Decimal { precision, scale }) => match precision {
            None => Lossless,
            Some(p) => {
                let int_digits = i64::from(*p) - i64::from(scale.unwrap_or(0));
                widen(int_digits >= max_decimal_digits(*bits))
            }
        },
        (
            T::BigInt,
            T::Decimal {
                precision: None, ..
            },
        ) => Lossless,
        (T::BigInt, T::Decimal { .. }) => ConditionallyLossless,
        (T::Int { bits } | T::UInt { bits }, T::Float { bits: fb }) => {
            let mantissa = if *fb >= 64 { 53 } else { 24 };
            if u32::from(*bits) <= mantissa {
                Lossless
            } else {
                PotentiallyLossy
            }
        }
        (T::BigInt, T::Float { .. }) => PotentiallyLossy,
        (T::Int { .. } | T::UInt { .. } | T::BigInt, T::Boolean) => ConditionallyLossless,

        // Decimals and floats
        (
            T::Decimal {
                precision: pa,
                scale: sa,
            },
            T::Decimal {
                precision: pb,
                scale: sb,
            },
        ) => classify_decimal(*pa, *sa, *pb, *sb),
        (T::Decimal { .. }, T::Int { .. } | T::UInt { .. } | T::BigInt) => PotentiallyLossy,
        (T::Decimal { .. }, T::Float { .. }) => PotentiallyLossy,
        (T::Float { bits: a }, T::Float { bits: b }) => {
            if a <= b {
                Lossless
            } else {
                PotentiallyLossy
            }
        }
        (
            T::Float { .. },
            T::Decimal {
                precision: None, ..
            },
        ) => ConditionallyLossless,
        (T::Float { .. }, T::Decimal { .. }) => PotentiallyLossy,
        (T::Float { .. }, T::Int { .. } | T::UInt { .. } | T::BigInt) => PotentiallyLossy,

        // Strings
        (T::String { max_chars: a }, T::String { max_chars: b }) => match (a, b) {
            (_, None) => Lossless,
            (Some(a), Some(b)) if a <= b => Lossless,
            _ => DefinitelyLossy,
        },
        (T::Binary { .. }, T::String { .. }) => ConditionallyLossless,
        (T::Array(_) | T::Object | T::Map { .. }, T::String { .. }) => PotentiallyLossy,
        (T::Geometry, T::String { .. }) => Unsupported,
        (T::Extension { .. }, T::String { .. }) => ConditionallyLossless,
        (T::Unknown, _) | (_, T::Unknown) => Unsupported,
        (_, T::String { max_chars: Some(_) }) => DefinitelyLossy,
        (_, T::String { max_chars: None }) => Lossless,
        (T::String { .. }, T::UtcDateTime { .. }) => PotentiallyLossy,
        (T::String { .. }, T::Time { .. } | T::LocalDateTime { .. } | T::ZonedDateTime) => {
            PotentiallyLossy
        }
        (T::String { .. }, T::Float { .. }) => PotentiallyLossy,
        (
            T::String { .. },
            T::Decimal {
                precision: Some(_), ..
            },
        ) => PotentiallyLossy,
        (T::String { .. }, T::Geometry | T::Extension { .. }) => Unsupported,
        (T::String { .. }, T::Binary { .. }) => Lossless,
        (T::String { .. }, _) => ConditionallyLossless,

        // Binary
        (T::Binary { max_bytes: a }, T::Binary { max_bytes: b }) => match (a, b) {
            (_, None) => Lossless,
            (Some(a), Some(b)) if a <= b => Lossless,
            _ => ConditionallyLossless,
        },
        (T::Uuid | T::ObjectId | T::Geometry, T::Binary { .. }) => Lossless,
        (T::Extension { .. }, T::Binary { .. }) => ConditionallyLossless,
        (T::Binary { .. }, T::Uuid | T::ObjectId | T::Geometry) => ConditionallyLossless,

        // Temporal
        (T::Date, T::LocalDateTime { .. }) => Lossless,
        (T::Date, T::UtcDateTime { .. } | T::ZonedDateTime) => PotentiallyLossy,
        (T::LocalDateTime { .. } | T::UtcDateTime { .. } | T::ZonedDateTime, T::Date) => {
            PotentiallyLossy
        }
        (T::LocalDateTime { .. }, T::Time { .. }) => DefinitelyLossy,
        (
            T::Time {
                fractional_digits: a,
            },
            T::Time {
                fractional_digits: b,
            },
        )
        | (
            T::LocalDateTime {
                fractional_digits: a,
            },
            T::LocalDateTime {
                fractional_digits: b,
            },
        )
        | (
            T::UtcDateTime {
                fractional_digits: a,
            },
            T::UtcDateTime {
                fractional_digits: b,
            },
        ) => {
            if a <= b {
                Lossless
            } else {
                PotentiallyLossy
            }
        }
        (T::LocalDateTime { .. }, T::UtcDateTime { .. } | T::ZonedDateTime) => PotentiallyLossy,
        (T::UtcDateTime { .. } | T::ZonedDateTime, T::LocalDateTime { .. }) => DefinitelyLossy,
        (T::UtcDateTime { .. }, T::ZonedDateTime) => Lossless,
        (T::ZonedDateTime, T::UtcDateTime { .. }) => DefinitelyLossy,
        (T::Duration, T::Interval) => ConditionallyLossless,
        (T::Interval, T::Duration) => PotentiallyLossy,

        // Containers
        (T::Array(a), T::Array(b)) => classify(a, b),
        (T::Json, T::Array(_) | T::Object | T::Map { .. }) => ConditionallyLossless,
        (T::Map { .. }, T::Object) => ConditionallyLossless,
        (T::Object, T::Map { .. }) => Lossless,
        (T::Map { key: ka, value: va }, T::Map { key: kb, value: vb }) => {
            classify(ka, kb).max(classify(va, vb))
        }
        (T::Array(_) | T::Object | T::Map { .. }, T::Json) => PotentiallyLossy,
        (T::Boolean | T::Int { .. } | T::UInt { .. }, T::Json) => Lossless,
        // NaN and infinities have no JSON number form.
        (T::Float { .. }, T::Json) => PotentiallyLossy,
        (_, T::Json) => PotentiallyLossy,

        (T::Enum { .. }, T::Enum { labels, .. }) if labels.is_empty() => Lossless,
        (T::Enum { .. }, T::Enum { .. }) => ConditionallyLossless,

        _ => Unsupported,
    }
}

fn widen(fits: bool) -> ConversionClass {
    if fits {
        ConversionClass::Lossless
    } else {
        ConversionClass::ConditionallyLossless
    }
}

fn max_decimal_digits(bits: u8) -> i64 {
    match bits {
        0..=8 => 3,
        9..=16 => 5,
        17..=24 => 8,
        25..=32 => 10,
        _ => 20,
    }
}

fn classify_decimal(
    pa: Option<u32>,
    sa: Option<i32>,
    pb: Option<u32>,
    sb: Option<i32>,
) -> ConversionClass {
    use ConversionClass::*;
    let Some(pb) = pb else {
        return Lossless;
    };
    let (Some(pa), Some(sa)) = (pa, sa) else {
        return PotentiallyLossy;
    };
    let sb = sb.unwrap_or(0);
    if sb < sa {
        return PotentiallyLossy;
    }
    let int_a = i64::from(pa) - i64::from(sa);
    let int_b = i64::from(pb) - i64::from(sb);
    widen(int_b >= int_a)
}

// ---------------------------------------------------------------------------
// Value-level conversion
// ---------------------------------------------------------------------------

struct Ctx<'a> {
    from: ValueKind,
    to: &'a CanonicalType,
    policy: &'a ConversionPolicy,
}

impl Ctx<'_> {
    fn target(&self) -> String {
        self.to.to_string()
    }

    fn unsupported(&self) -> ConversionError {
        ConversionError::Unsupported {
            from: self.from,
            to: self.target(),
        }
    }

    fn out_of_range(&self, reason: impl Into<String>) -> ConversionError {
        ConversionError::OutOfRange {
            from: self.from,
            to: self.target(),
            reason: reason.into(),
        }
    }

    fn invalid(&self, reason: impl Into<String>) -> ConversionError {
        ConversionError::InvalidFormat {
            from: self.from,
            to: self.target(),
            reason: reason.into(),
        }
    }

    /// Produces a lossy result if the policy approves it, otherwise an error.
    fn lossy(
        &self,
        value: Value,
        class: ConversionClass,
        detail: impl Into<String>,
    ) -> Result<Converted, ConversionError> {
        let record = LossRecord {
            class,
            from: self.from,
            to: self.target(),
            detail: detail.into(),
        };
        if self.policy.allows(class) {
            Ok(Converted {
                value,
                loss: Some(record),
            })
        } else {
            Err(ConversionError::LossNotApproved(record))
        }
    }
}

/// Converts `value` to `target` under `policy`.
///
/// `Null` and `Missing` pass through unchanged for every target type;
/// nullability is enforced by the mapping layer, not here.
pub fn convert(
    value: &Value,
    target: &CanonicalType,
    policy: &ConversionPolicy,
) -> Result<Converted, ConversionError> {
    use CanonicalType as T;

    if value.is_absent() {
        return Ok(Converted::exact(value.clone()));
    }
    if let Value::Invalid { reason, .. } = value {
        return Err(ConversionError::InvalidSource(reason.clone()));
    }
    let cx = Ctx {
        from: value.kind(),
        to: target,
        policy,
    };

    match target {
        T::Null => Err(cx.unsupported()),
        T::Unknown => Err(cx.unsupported()),
        T::Boolean => to_bool(&cx, value),
        T::Int { bits } => to_integer(&cx, value, *bits, true),
        T::UInt { bits } => to_integer(&cx, value, *bits, false),
        T::BigInt => to_bigint(&cx, value),
        T::Decimal { precision, scale } => to_decimal(&cx, value, *precision, *scale),
        T::Float { bits } => to_float(&cx, value, *bits),
        T::String { max_chars } => to_string(&cx, value, *max_chars),
        T::Binary { max_bytes } => to_binary(&cx, value, *max_bytes),
        T::Uuid => to_uuid(&cx, value),
        T::ObjectId => to_object_id(&cx, value),
        T::Date => to_date(&cx, value),
        T::Time { fractional_digits } => to_time(&cx, value, *fractional_digits),
        T::LocalDateTime { fractional_digits } => to_local_datetime(&cx, value, *fractional_digits),
        T::UtcDateTime { fractional_digits } => to_utc_datetime(&cx, value, *fractional_digits),
        T::ZonedDateTime => to_zoned_datetime(&cx, value),
        T::Duration => to_duration(&cx, value),
        T::Interval => to_interval(&cx, value),
        T::Array(inner) => to_array(&cx, value, inner),
        T::Object => to_object(&cx, value),
        T::Map { key, value: vt } => to_map(&cx, value, key, vt),
        T::Enum { name, labels } => to_enum(&cx, value, name, labels),
        T::Json => to_json_value(&cx, value),
        T::Geometry => to_geometry(&cx, value),
        T::Extension { name } => match value {
            Value::Extension { type_name, .. } if type_name == name => {
                Ok(Converted::exact(value.clone()))
            }
            _ => Err(cx.unsupported()),
        },
        T::Mixed(parts) => {
            let own = value.canonical_type();
            if parts
                .iter()
                .any(|p| classify(&own, p) == ConversionClass::Lossless)
            {
                Ok(Converted::exact(value.clone()))
            } else {
                Err(cx.unsupported())
            }
        }
    }
}

/// Combines the losses of several element conversions into one record.
fn merge_loss(cx: &Ctx<'_>, losses: Vec<LossRecord>) -> Option<LossRecord> {
    let worst = losses.iter().map(|l| l.class).max()?;
    let first = losses.first().map(|l| l.detail.clone()).unwrap_or_default();
    Some(LossRecord {
        class: worst,
        from: cx.from,
        to: cx.target(),
        detail: if losses.len() == 1 {
            first
        } else {
            format!("{} nested values lossy; first: {first}", losses.len())
        },
    })
}

fn to_bool(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    let b = match value {
        Value::Bool(b) => *b,
        Value::Int(0) | Value::UInt(0) => false,
        Value::Int(1) | Value::UInt(1) => true,
        Value::Int(_) | Value::UInt(_) => {
            return Err(cx.out_of_range("only 0 and 1 map to boolean"));
        }
        Value::BigInt(b) => {
            if *b == BigInt::from(0) {
                false
            } else if *b == BigInt::from(1) {
                true
            } else {
                return Err(cx.out_of_range("only 0 and 1 map to boolean"));
            }
        }
        Value::String(s) => {
            parse_bool(s).ok_or_else(|| cx.invalid("not a recognised boolean literal"))?
        }
        Value::Json(serde_json::Value::Bool(b)) => *b,
        _ => return Err(cx.unsupported()),
    };
    Ok(Converted::exact(Value::Bool(b)))
}

/// Accepted boolean literals (case-insensitive, surrounding whitespace ignored).
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" | "on" => Some(true),
        "false" | "f" | "no" | "n" | "0" | "off" => Some(false),
        _ => None,
    }
}

fn int_bounds(bits: u8, signed: bool) -> (i128, i128) {
    let bits = u32::from(bits.clamp(1, 64));
    if signed {
        let max = (1i128 << (bits - 1)) - 1;
        (-max - 1, max)
    } else {
        (0, (1i128 << bits) - 1)
    }
}

fn make_int(n: i128, signed: bool) -> Value {
    // Callers have already range-checked `n` against the target width.
    if signed {
        Value::Int(n as i64)
    } else {
        Value::UInt(n as u64)
    }
}

fn to_integer(
    cx: &Ctx<'_>,
    value: &Value,
    bits: u8,
    signed: bool,
) -> Result<Converted, ConversionError> {
    let (min, max) = int_bounds(bits, signed);
    let check = |n: i128| -> Result<Value, ConversionError> {
        if n < min || n > max {
            Err(cx.out_of_range(format!("allowed range is {min}..={max}")))
        } else {
            Ok(make_int(n, signed))
        }
    };
    match value {
        Value::Bool(b) => Ok(Converted::exact(check(i128::from(*b))?)),
        Value::Int(n) => Ok(Converted::exact(check(i128::from(*n))?)),
        Value::UInt(n) => Ok(Converted::exact(check(i128::from(*n))?)),
        Value::BigInt(b) => {
            let n = i128::try_from(b).map_err(|_| cx.out_of_range("integer exceeds 128 bits"))?;
            Ok(Converted::exact(check(n)?))
        }
        Value::Decimal(d) => decimal_to_integer(cx, d, |b| {
            let n = i128::try_from(&b).map_err(|_| cx.out_of_range("integer exceeds 128 bits"))?;
            check(n)
        }),
        Value::Float(f) => float_to_integer(cx, *f, |b| {
            let n = i128::try_from(&b).map_err(|_| cx.out_of_range("integer exceeds 128 bits"))?;
            check(n)
        }),
        Value::String(s) => {
            let b = BigInt::from_str(s.trim()).map_err(|_| cx.invalid("not an integer literal"))?;
            let n = i128::try_from(&b).map_err(|_| cx.out_of_range("integer exceeds 128 bits"))?;
            Ok(Converted::exact(check(n)?))
        }
        Value::Json(serde_json::Value::Number(num)) => {
            if let Some(i) = num.as_i64() {
                Ok(Converted::exact(check(i128::from(i))?))
            } else if let Some(u) = num.as_u64() {
                Ok(Converted::exact(check(i128::from(u))?))
            } else {
                let f = num
                    .as_f64()
                    .ok_or_else(|| cx.invalid("unrepresentable JSON number"))?;
                float_to_integer(cx, f, |b| {
                    let n = i128::try_from(&b)
                        .map_err(|_| cx.out_of_range("integer exceeds 128 bits"))?;
                    check(n)
                })
            }
        }
        _ => Err(cx.unsupported()),
    }
}

fn decimal_to_integer(
    cx: &Ctx<'_>,
    d: &BigDecimal,
    finish: impl Fn(BigInt) -> Result<Value, ConversionError>,
) -> Result<Converted, ConversionError> {
    if d.is_integer() {
        let (int, _) = d.with_scale(0).into_bigint_and_exponent();
        Ok(Converted::exact(finish(int)?))
    } else {
        let (int, _) = d
            .with_scale_round(0, RoundingMode::HalfEven)
            .into_bigint_and_exponent();
        let v = finish(int)?;
        cx.lossy(
            v,
            ConversionClass::PotentiallyLossy,
            "fractional digits rounded half-even to an integer",
        )
    }
}

fn float_to_integer(
    cx: &Ctx<'_>,
    f: f64,
    finish: impl Fn(BigInt) -> Result<Value, ConversionError>,
) -> Result<Converted, ConversionError> {
    if !f.is_finite() {
        return Err(cx.out_of_range("non-finite float has no integer value"));
    }
    let d = float_to_decimal(f).ok_or_else(|| cx.invalid("float has no decimal representation"))?;
    decimal_to_integer(cx, &d, finish)
}

/// Shortest decimal that round-trips to the same `f64`.
fn float_to_decimal(f: f64) -> Option<BigDecimal> {
    if !f.is_finite() {
        return None;
    }
    BigDecimal::from_str(&format!("{f:?}")).ok()
}

fn to_bigint(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Bool(b) => Ok(Converted::exact(Value::BigInt(BigInt::from(u8::from(*b))))),
        Value::Int(n) => Ok(Converted::exact(Value::BigInt(BigInt::from(*n)))),
        Value::UInt(n) => Ok(Converted::exact(Value::BigInt(BigInt::from(*n)))),
        Value::BigInt(_) => Ok(Converted::exact(value.clone())),
        Value::Decimal(d) => decimal_to_integer(cx, d, |b| Ok(Value::BigInt(b))),
        Value::Float(f) => float_to_integer(cx, *f, |b| Ok(Value::BigInt(b))),
        Value::String(s) => BigInt::from_str(s.trim())
            .map(|b| Converted::exact(Value::BigInt(b)))
            .map_err(|_| cx.invalid("not an integer literal")),
        Value::Json(serde_json::Value::Number(num)) => BigInt::from_str(&num.to_string())
            .map(|b| Converted::exact(Value::BigInt(b)))
            .or_else(|_| {
                let f = num
                    .as_f64()
                    .ok_or_else(|| cx.invalid("unrepresentable JSON number"))?;
                float_to_integer(cx, f, |b| Ok(Value::BigInt(b)))
            }),
        _ => Err(cx.unsupported()),
    }
}

fn to_decimal(
    cx: &Ctx<'_>,
    value: &Value,
    precision: Option<u32>,
    scale: Option<i32>,
) -> Result<Converted, ConversionError> {
    let (d, mut loss) = match value {
        Value::Bool(b) => (BigDecimal::from(u8::from(*b)), None),
        Value::Int(n) => (BigDecimal::from(*n), None),
        Value::UInt(n) => (BigDecimal::from(*n), None),
        Value::BigInt(b) => (BigDecimal::from(b.clone()), None),
        Value::Decimal(d) => (d.clone(), None),
        Value::Float(f) => {
            let d = float_to_decimal(*f).ok_or_else(|| cx.out_of_range("non-finite float"))?;
            (d, None)
        }
        Value::String(s) => (
            BigDecimal::from_str(s.trim()).map_err(|_| cx.invalid("not a decimal literal"))?,
            None,
        ),
        Value::Json(serde_json::Value::Number(n)) => (
            BigDecimal::from_str(&n.to_string())
                .map_err(|_| cx.invalid("not a decimal literal"))?,
            None,
        ),
        _ => return Err(cx.unsupported()),
    };

    let mut d = d;
    if let Some(target_scale) = scale {
        let (_, current_scale) = d.as_bigint_and_exponent();
        if current_scale > i64::from(target_scale) {
            let rounded = d.with_scale_round(i64::from(target_scale), RoundingMode::HalfEven);
            if rounded != d {
                loss = Some(ConversionClass::PotentiallyLossy);
            }
            d = rounded;
        } else {
            d = d.with_scale(i64::from(target_scale));
        }
    }
    if let Some(p) = precision {
        let (int, exp) = d.as_bigint_and_exponent();
        let digits = if int == BigInt::from(0) {
            1
        } else {
            int.magnitude().to_string().len() as i64
        };
        let integer_digits = digits - exp.max(0);
        let allowed = i64::from(p) - i64::from(scale.unwrap_or(0));
        if integer_digits > allowed.max(0) && !(integer_digits == 1 && int == BigInt::from(0)) {
            return Err(cx.out_of_range(format!(
                "needs {integer_digits} integer digits, target allows {allowed}"
            )));
        }
    }
    match loss {
        None => Ok(Converted::exact(Value::Decimal(d))),
        Some(class) => cx.lossy(
            Value::Decimal(d),
            class,
            "rounded half-even to the target scale",
        ),
    }
}

fn to_float(cx: &Ctx<'_>, value: &Value, bits: u8) -> Result<Converted, ConversionError> {
    let single = bits <= 32;
    let finish = |f: f64, exact_source: Option<BigDecimal>| -> Result<Converted, ConversionError> {
        let (out, narrowed) = if single {
            let s = f as f32;
            if f.is_finite() && !s.is_finite() {
                return Err(cx.out_of_range("magnitude exceeds 32-bit float range"));
            }
            (f64::from(s), f64::from(s) != f && !f.is_nan())
        } else {
            (f, false)
        };
        let inexact = match exact_source {
            Some(src) => match float_to_decimal(out) {
                Some(back) => back != src,
                None => true,
            },
            None => narrowed,
        };
        if inexact {
            cx.lossy(
                Value::Float(out),
                ConversionClass::PotentiallyLossy,
                "value is not exactly representable as a binary float",
            )
        } else {
            Ok(Converted::exact(Value::Float(out)))
        }
    };
    match value {
        Value::Float(f) => finish(*f, None),
        Value::Bool(b) => finish(f64::from(u8::from(*b)), None),
        Value::Int(n) => finish(*n as f64, Some(BigDecimal::from(*n))),
        Value::UInt(n) => finish(*n as f64, Some(BigDecimal::from(*n))),
        Value::BigInt(b) => {
            let f = f64::from_str(&b.to_string())
                .map_err(|_| cx.invalid("integer not parseable as float"))?;
            if !f.is_finite() {
                return Err(cx.out_of_range("magnitude exceeds float range"));
            }
            finish(f, Some(BigDecimal::from(b.clone())))
        }
        Value::Decimal(d) => {
            let f = f64::from_str(&d.to_string())
                .map_err(|_| cx.invalid("decimal not parseable as float"))?;
            if !f.is_finite() {
                return Err(cx.out_of_range("magnitude exceeds float range"));
            }
            finish(f, Some(d.normalized()))
        }
        Value::String(s) => {
            let t = s.trim();
            match t {
                "NaN" | "nan" => finish(f64::NAN, None),
                "Infinity" | "inf" | "+Infinity" => finish(f64::INFINITY, None),
                "-Infinity" | "-inf" => finish(f64::NEG_INFINITY, None),
                _ => {
                    let d =
                        BigDecimal::from_str(t).map_err(|_| cx.invalid("not a numeric literal"))?;
                    let f = f64::from_str(t).map_err(|_| cx.invalid("not a numeric literal"))?;
                    if !f.is_finite() {
                        return Err(cx.out_of_range("magnitude exceeds float range"));
                    }
                    finish(f, Some(d.normalized()))
                }
            }
        }
        Value::Json(serde_json::Value::Number(n)) => {
            let f = n
                .as_f64()
                .ok_or_else(|| cx.invalid("unrepresentable JSON number"))?;
            let src = BigDecimal::from_str(&n.to_string())
                .ok()
                .map(|d| d.normalized());
            finish(f, src)
        }
        _ => Err(cx.unsupported()),
    }
}

/// Canonical text for scalar values. Returns `None` for containers and
/// values without a defined textual form.
pub fn scalar_to_text(value: &Value) -> Option<String> {
    Some(match value {
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        Value::BigInt(b) => b.to_string(),
        Value::Decimal(d) => d.to_plain_string(),
        Value::Float(f) => format_float(*f),
        Value::String(s) => s.clone(),
        Value::Uuid(u) => u.hyphenated().to_string(),
        Value::ObjectId(oid) => hex::encode(oid),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Time(t) => t.format("%H:%M:%S%.f").to_string(),
        Value::LocalDateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        Value::UtcDateTime(dt) => dt.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        Value::ZonedDateTime { instant, zone } => {
            let base = instant.to_rfc3339_opts(SecondsFormat::AutoSi, false);
            match zone {
                // RFC 9557 zone suffix keeps the IANA name.
                Some(z) => format!("{base}[{z}]"),
                None => base,
            }
        }
        Value::Duration(d) => format_duration(d),
        Value::Interval(i) => format_interval(i),
        Value::Enum { label, .. } => label.clone(),
        Value::Json(j) => j.to_string(),
        Value::Extension { text: Some(t), .. } => t.clone(),
        _ => return None,
    })
}

fn format_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        }
    } else {
        format!("{f:?}")
    }
}

fn format_fraction(seconds: i128, nanos: u32) -> String {
    if nanos == 0 {
        seconds.to_string()
    } else {
        let frac = format!("{nanos:09}");
        format!("{seconds}.{}", frac.trim_end_matches('0'))
    }
}

/// ISO-8601 duration in seconds, e.g. `PT-1.5S`.
fn format_duration(d: &Duration) -> String {
    let total = d.total_nanos();
    let sign = if total < 0 { "-" } else { "" };
    let abs = total.unsigned_abs();
    let secs = (abs / 1_000_000_000) as i128;
    let nanos = (abs % 1_000_000_000) as u32;
    format!("PT{sign}{}S", format_fraction(secs, nanos))
}

/// ISO-8601-style interval, e.g. `P14M3DT4.5S`.
fn format_interval(i: &Interval) -> String {
    let sign = if i.microseconds < 0 { "-" } else { "" };
    let abs = i.microseconds.unsigned_abs();
    let secs = i128::from(abs / 1_000_000);
    let nanos = ((abs % 1_000_000) * 1000) as u32;
    format!(
        "P{}M{}DT{sign}{}S",
        i.months,
        i.days,
        format_fraction(secs, nanos)
    )
}

fn to_string(
    cx: &Ctx<'_>,
    value: &Value,
    max_chars: Option<u64>,
) -> Result<Converted, ConversionError> {
    let (text, loss) = match value {
        Value::Binary(b) => match std::str::from_utf8(b) {
            Ok(s) => (s.to_owned(), None),
            Err(_) => return Err(cx.invalid("binary value is not valid UTF-8")),
        },
        Value::Array(_) | Value::Object(_) | Value::Map(_) => {
            let (json, exact) = value_to_json(value);
            let class = if exact {
                None
            } else {
                Some(ConversionClass::PotentiallyLossy)
            };
            (json.to_string(), class)
        }
        Value::Geometry { .. } => return Err(cx.unsupported()),
        other => (scalar_to_text(other).ok_or_else(|| cx.unsupported())?, None),
    };
    if let Some(max) = max_chars
        && text.chars().count() as u64 > max
    {
        let truncated: String = text.chars().take(max as usize).collect();
        return cx.lossy(
            Value::String(truncated),
            ConversionClass::DefinitelyLossy,
            "text truncated to the target length",
        );
    }
    match loss {
        None => Ok(Converted::exact(Value::String(text))),
        Some(class) => cx.lossy(
            Value::String(text),
            class,
            "non-JSON-native nested values encoded as strings",
        ),
    }
}

fn to_binary(
    cx: &Ctx<'_>,
    value: &Value,
    max_bytes: Option<u64>,
) -> Result<Converted, ConversionError> {
    let bytes = match value {
        Value::Binary(b) => b.clone(),
        Value::String(s) => s.as_bytes().to_vec(),
        Value::Uuid(u) => u.as_bytes().to_vec(),
        Value::ObjectId(oid) => oid.to_vec(),
        Value::Geometry { wkb, .. } => wkb.clone(),
        Value::Extension {
            binary: Some(b), ..
        } => b.clone(),
        _ => return Err(cx.unsupported()),
    };
    if let Some(max) = max_bytes
        && bytes.len() as u64 > max
    {
        return Err(cx.out_of_range(format!("{} bytes exceed limit of {max}", bytes.len())));
    }
    Ok(Converted::exact(Value::Binary(bytes)))
}

fn to_uuid(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    let u = match value {
        Value::Uuid(u) => *u,
        Value::String(s) => Uuid::parse_str(s.trim()).map_err(|_| cx.invalid("not a UUID"))?,
        Value::Binary(b) => {
            Uuid::from_slice(b).map_err(|_| cx.invalid("UUID needs exactly 16 bytes"))?
        }
        _ => return Err(cx.unsupported()),
    };
    Ok(Converted::exact(Value::Uuid(u)))
}

fn to_object_id(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    let mut out = [0u8; 12];
    match value {
        Value::ObjectId(oid) => out = *oid,
        Value::String(s) => hex::decode_to_slice(s.trim(), &mut out)
            .map_err(|_| cx.invalid("ObjectId needs 24 hex digits"))?,
        Value::Binary(b) if b.len() == 12 => out.copy_from_slice(b),
        Value::Binary(_) => return Err(cx.invalid("ObjectId needs exactly 12 bytes")),
        _ => return Err(cx.unsupported()),
    }
    Ok(Converted::exact(Value::ObjectId(out)))
}

const LOCAL_DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
];

pub(crate) fn parse_local_datetime(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim();
    LOCAL_DATETIME_FORMATS
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(s, f).ok())
}

/// Parses a timestamp with an explicit offset. Accepts RFC 3339 and the
/// space-separated forms PostgreSQL and MySQL print, plus an optional RFC 9557
/// `[Zone/Name]` suffix.
pub(crate) fn parse_offset_datetime(s: &str) -> Option<(DateTime<FixedOffset>, Option<String>)> {
    let s = s.trim();
    let (body, zone) = match s.strip_suffix(']').and_then(|r| r.rsplit_once('[')) {
        Some((body, zone)) => (body, Some(zone.to_owned())),
        None => (s, None),
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(body) {
        return Some((dt, zone));
    }
    let normalized = body.replace(' ', "T");
    if let Ok(dt) = DateTime::parse_from_rfc3339(&normalized) {
        return Some((dt, zone));
    }
    for f in ["%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.fZ"] {
        if let Ok(dt) = DateTime::parse_from_str(&normalized, f) {
            return Some((dt, zone));
        }
        if f.ends_with('Z')
            && let Ok(naive) = NaiveDateTime::parse_from_str(&normalized, f)
        {
            return Some((naive.and_utc().fixed_offset(), zone));
        }
    }
    None
}

fn truncate_nanos(nanos: u32, digits: u8) -> u32 {
    let digits = u32::from(digits.min(9));
    let factor = 10u32.pow(9 - digits);
    // Leap-second nanos (>= 1e9) are preserved modulo truncation.
    nanos - nanos % factor
}

fn to_date(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Date(_) => Ok(Converted::exact(value.clone())),
        Value::LocalDateTime(dt) => {
            let d = Value::Date(dt.date());
            if dt.time() == NaiveTime::MIN {
                Ok(Converted::exact(d))
            } else {
                cx.lossy(
                    d,
                    ConversionClass::PotentiallyLossy,
                    "time of day discarded",
                )
            }
        }
        Value::UtcDateTime(dt) => cx.lossy(
            Value::Date(dt.date_naive()),
            ConversionClass::PotentiallyLossy,
            "time of day and UTC designation discarded",
        ),
        Value::ZonedDateTime { instant, .. } => cx.lossy(
            Value::Date(instant.date_naive()),
            ConversionClass::PotentiallyLossy,
            "time of day and zone discarded (local calendar date kept)",
        ),
        Value::String(s) => NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
            .map(|d| Converted::exact(Value::Date(d)))
            .map_err(|_| cx.invalid("expected YYYY-MM-DD")),
        _ => Err(cx.unsupported()),
    }
}

fn time_with_precision(
    cx: &Ctx<'_>,
    t: NaiveTime,
    digits: u8,
) -> Result<Converted, ConversionError> {
    let truncated = truncate_nanos(t.nanosecond(), digits);
    if truncated == t.nanosecond() {
        return Ok(Converted::exact(Value::Time(t)));
    }
    let nt = t
        .with_nanosecond(truncated)
        .ok_or_else(|| cx.out_of_range("invalid fractional seconds"))?;
    cx.lossy(
        Value::Time(nt),
        ConversionClass::PotentiallyLossy,
        format!("fractional seconds truncated to {digits} digits"),
    )
}

fn to_time(cx: &Ctx<'_>, value: &Value, digits: u8) -> Result<Converted, ConversionError> {
    match value {
        Value::Time(t) => time_with_precision(cx, *t, digits),
        Value::LocalDateTime(dt) => {
            let inner = time_with_precision(cx, dt.time(), digits)?;
            cx.lossy(
                inner.value,
                ConversionClass::DefinitelyLossy,
                "date discarded",
            )
        }
        Value::String(s) => {
            let t = NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M"))
                .map_err(|_| cx.invalid("expected HH:MM[:SS[.fff]]"))?;
            time_with_precision(cx, t, digits)
        }
        _ => Err(cx.unsupported()),
    }
}

fn local_with_precision(
    cx: &Ctx<'_>,
    dt: NaiveDateTime,
    digits: u8,
    prior: Option<(ConversionClass, &str)>,
) -> Result<Converted, ConversionError> {
    let truncated = truncate_nanos(dt.nanosecond(), digits);
    let (out, precision_loss) = if truncated == dt.nanosecond() {
        (dt, false)
    } else {
        (
            dt.with_nanosecond(truncated)
                .ok_or_else(|| cx.out_of_range("invalid fractional seconds"))?,
            true,
        )
    };
    let v = Value::LocalDateTime(out);
    match (prior, precision_loss) {
        (Some((class, detail)), _) => cx.lossy(v, class, detail),
        (None, true) => cx.lossy(
            v,
            ConversionClass::PotentiallyLossy,
            format!("fractional seconds truncated to {digits} digits"),
        ),
        (None, false) => Ok(Converted::exact(v)),
    }
}

fn to_local_datetime(
    cx: &Ctx<'_>,
    value: &Value,
    digits: u8,
) -> Result<Converted, ConversionError> {
    match value {
        Value::LocalDateTime(dt) => local_with_precision(cx, *dt, digits, None),
        Value::Date(d) => Ok(Converted::exact(Value::LocalDateTime(
            d.and_time(NaiveTime::MIN),
        ))),
        Value::UtcDateTime(dt) => local_with_precision(
            cx,
            dt.naive_utc(),
            digits,
            Some((
                ConversionClass::DefinitelyLossy,
                "UTC designation discarded",
            )),
        ),
        Value::ZonedDateTime { instant, .. } => local_with_precision(
            cx,
            instant.naive_local(),
            digits,
            Some((
                ConversionClass::DefinitelyLossy,
                "offset and zone discarded (wall-clock time kept)",
            )),
        ),
        Value::String(s) => {
            let dt = parse_local_datetime(s)
                .ok_or_else(|| cx.invalid("expected YYYY-MM-DD HH:MM[:SS[.fff]]"))?;
            local_with_precision(cx, dt, digits, None)
        }
        _ => Err(cx.unsupported()),
    }
}

fn assumed_offset(cx: &Ctx<'_>) -> Result<FixedOffset, ConversionError> {
    let secs = cx.policy.assume_utc_offset_seconds.ok_or_else(|| {
        cx.invalid("local time has no offset and the policy defines no assumed offset")
    })?;
    FixedOffset::east_opt(secs).ok_or_else(|| cx.invalid("assumed offset is out of range"))
}

fn utc_with_precision(
    cx: &Ctx<'_>,
    dt: DateTime<Utc>,
    digits: u8,
    prior: Option<(ConversionClass, String)>,
) -> Result<Converted, ConversionError> {
    let truncated = truncate_nanos(dt.nanosecond(), digits);
    let (out, precision_loss) = if truncated == dt.nanosecond() {
        (dt, false)
    } else {
        (
            dt.with_nanosecond(truncated)
                .ok_or_else(|| cx.out_of_range("invalid fractional seconds"))?,
            true,
        )
    };
    let v = Value::UtcDateTime(out);
    match (prior, precision_loss) {
        (Some((class, detail)), _) => cx.lossy(v, class, detail),
        (None, true) => cx.lossy(
            v,
            ConversionClass::PotentiallyLossy,
            format!("fractional seconds truncated to {digits} digits"),
        ),
        (None, false) => Ok(Converted::exact(v)),
    }
}

fn to_utc_datetime(cx: &Ctx<'_>, value: &Value, digits: u8) -> Result<Converted, ConversionError> {
    match value {
        Value::UtcDateTime(dt) => utc_with_precision(cx, *dt, digits, None),
        Value::ZonedDateTime { instant, .. } => {
            let prior = if instant.offset().local_minus_utc() == 0 {
                None
            } else {
                Some((
                    ConversionClass::DefinitelyLossy,
                    "offset and zone discarded (instant kept)".to_owned(),
                ))
            };
            utc_with_precision(cx, instant.with_timezone(&Utc), digits, prior)
        }
        Value::LocalDateTime(dt) => {
            let off = assumed_offset(cx)?;
            let instant = off
                .from_local_datetime(dt)
                .single()
                .ok_or_else(|| cx.invalid("ambiguous local time"))?;
            utc_with_precision(
                cx,
                instant.with_timezone(&Utc),
                digits,
                Some((
                    ConversionClass::PotentiallyLossy,
                    format!("local time interpreted with assumed offset {off}"),
                )),
            )
        }
        Value::Date(d) => {
            let off = assumed_offset(cx)?;
            let instant = off
                .from_local_datetime(&d.and_time(NaiveTime::MIN))
                .single()
                .ok_or_else(|| cx.invalid("ambiguous local time"))?;
            utc_with_precision(
                cx,
                instant.with_timezone(&Utc),
                digits,
                Some((
                    ConversionClass::PotentiallyLossy,
                    format!("date interpreted as midnight at assumed offset {off}"),
                )),
            )
        }
        Value::String(s) => {
            let (dt, zone) = parse_offset_datetime(s)
                .ok_or_else(|| cx.invalid("expected an RFC 3339 timestamp with offset"))?;
            let prior = if dt.offset().local_minus_utc() == 0 && zone.is_none() {
                None
            } else {
                Some((
                    ConversionClass::PotentiallyLossy,
                    "offset normalised to UTC".to_owned(),
                ))
            };
            utc_with_precision(cx, dt.with_timezone(&Utc), digits, prior)
        }
        _ => Err(cx.unsupported()),
    }
}

fn to_zoned_datetime(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::ZonedDateTime { .. } => Ok(Converted::exact(value.clone())),
        Value::UtcDateTime(dt) => Ok(Converted::exact(Value::ZonedDateTime {
            instant: dt.fixed_offset(),
            zone: None,
        })),
        Value::LocalDateTime(dt) => {
            let off = assumed_offset(cx)?;
            let instant = off
                .from_local_datetime(dt)
                .single()
                .ok_or_else(|| cx.invalid("ambiguous local time"))?;
            cx.lossy(
                Value::ZonedDateTime {
                    instant,
                    zone: None,
                },
                ConversionClass::PotentiallyLossy,
                format!("local time interpreted with assumed offset {off}"),
            )
        }
        Value::String(s) => {
            let (instant, zone) = parse_offset_datetime(s)
                .ok_or_else(|| cx.invalid("expected an RFC 3339 timestamp with offset"))?;
            Ok(Converted::exact(Value::ZonedDateTime { instant, zone }))
        }
        _ => Err(cx.unsupported()),
    }
}

fn to_duration(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Duration(_) => Ok(Converted::exact(value.clone())),
        Value::Interval(i) => {
            if i.months != 0 {
                return Err(cx.out_of_range("intervals with months have no fixed length"));
            }
            let total = i128::from(i.days) * 86_400 * Duration::NANOS_PER_SEC
                + i128::from(i.microseconds) * 1000;
            let d = Duration::from_total_nanos(total)
                .ok_or_else(|| cx.out_of_range("duration overflow"))?;
            if i.days == 0 {
                Ok(Converted::exact(Value::Duration(d)))
            } else {
                cx.lossy(
                    Value::Duration(d),
                    ConversionClass::PotentiallyLossy,
                    "calendar days converted to 24-hour days",
                )
            }
        }
        Value::String(s) => parse_duration_text(s)
            .map(|d| Converted::exact(Value::Duration(d)))
            .ok_or_else(|| cx.invalid("expected an ISO-8601 duration such as PT1.5S")),
        _ => Err(cx.unsupported()),
    }
}

fn to_interval(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Interval(_) => Ok(Converted::exact(value.clone())),
        Value::Duration(d) => {
            let total = d.total_nanos();
            if total % 1000 != 0 {
                return Err(cx.out_of_range("interval resolution is one microsecond"));
            }
            let micros =
                i64::try_from(total / 1000).map_err(|_| cx.out_of_range("interval overflow"))?;
            Ok(Converted::exact(Value::Interval(Interval {
                months: 0,
                days: 0,
                microseconds: micros,
            })))
        }
        Value::String(s) => parse_interval_text(s)
            .map(|i| Converted::exact(Value::Interval(i)))
            .ok_or_else(|| cx.invalid("expected an interval such as P14M3DT4.5S")),
        _ => Err(cx.unsupported()),
    }
}

/// Parses `[-]seconds[.fraction]` into total nanoseconds.
fn parse_seconds_to_nanos(s: &str) -> Option<i128> {
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let secs: i128 = whole.parse().ok()?;
    let nanos: i128 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<9}").parse().ok()?
    };
    let total = secs
        .checked_mul(Duration::NANOS_PER_SEC)?
        .checked_add(nanos)?;
    Some(if negative { -total } else { total })
}

/// Parses the duration text produced by [`scalar_to_text`] (`PT[-]n[.f]S`).
pub fn parse_duration_text(s: &str) -> Option<Duration> {
    let body = s.trim().strip_prefix("PT")?.strip_suffix('S')?;
    Duration::from_total_nanos(parse_seconds_to_nanos(body)?)
}

/// Parses the interval text produced by [`scalar_to_text`] (`P{m}M{d}DT[-]n[.f]S`).
pub fn parse_interval_text(s: &str) -> Option<Interval> {
    let body = s.trim().strip_prefix('P')?;
    let (months, rest) = body.split_once('M')?;
    let (days, rest) = rest.split_once("DT")?;
    let nanos = parse_seconds_to_nanos(rest.strip_suffix('S')?)?;
    if nanos % 1000 != 0 {
        return None;
    }
    Some(Interval {
        months: months.parse().ok()?,
        days: days.parse().ok()?,
        microseconds: i64::try_from(nanos / 1000).ok()?,
    })
}

fn to_array(
    cx: &Ctx<'_>,
    value: &Value,
    inner: &CanonicalType,
) -> Result<Converted, ConversionError> {
    let items: Vec<Value> = match value {
        Value::Array(items) => items.clone(),
        Value::Json(serde_json::Value::Array(items)) => items.iter().map(json_to_value).collect(),
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(serde_json::Value::Array(items)) => items.iter().map(json_to_value).collect(),
            _ => return Err(cx.invalid("text is not a JSON array")),
        },
        _ => return Err(cx.unsupported()),
    };
    let mut out = Vec::with_capacity(items.len());
    let mut losses = Vec::new();
    for item in &items {
        let c = convert(item, inner, cx.policy)?;
        if let Some(l) = c.loss {
            losses.push(l);
        }
        out.push(c.value);
    }
    Ok(Converted {
        value: Value::Array(out),
        loss: merge_loss(cx, losses),
    })
}

fn to_object(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Object(_) => Ok(Converted::exact(value.clone())),
        Value::Json(j @ serde_json::Value::Object(_)) => Ok(Converted::exact(json_to_value(j))),
        Value::Map(entries) => {
            let mut obj = IndexMap::with_capacity(entries.len());
            for (k, v) in entries {
                match k {
                    Value::String(s) => {
                        if obj.insert(s.clone(), v.clone()).is_some() {
                            return Err(cx.invalid("duplicate map key"));
                        }
                    }
                    _ => return Err(cx.invalid("object keys must be strings")),
                }
            }
            Ok(Converted::exact(Value::Object(obj)))
        }
        Value::String(s) => match serde_json::from_str::<serde_json::Value>(s) {
            Ok(j @ serde_json::Value::Object(_)) => Ok(Converted::exact(json_to_value(&j))),
            _ => Err(cx.invalid("text is not a JSON object")),
        },
        _ => Err(cx.unsupported()),
    }
}

fn to_map(
    cx: &Ctx<'_>,
    value: &Value,
    key_t: &CanonicalType,
    value_t: &CanonicalType,
) -> Result<Converted, ConversionError> {
    let entries: Vec<(Value, Value)> = match value {
        Value::Map(e) => e.clone(),
        Value::Object(o) => o
            .iter()
            .map(|(k, v)| (Value::String(k.clone()), v.clone()))
            .collect(),
        _ => return Err(cx.unsupported()),
    };
    let mut out = Vec::with_capacity(entries.len());
    let mut losses = Vec::new();
    for (k, v) in &entries {
        let kc = convert(k, key_t, cx.policy)?;
        let vc = convert(v, value_t, cx.policy)?;
        losses.extend(kc.loss);
        losses.extend(vc.loss);
        out.push((kc.value, vc.value));
    }
    Ok(Converted {
        value: Value::Map(out),
        loss: merge_loss(cx, losses),
    })
}

fn to_enum(
    cx: &Ctx<'_>,
    value: &Value,
    name: &Option<String>,
    labels: &[String],
) -> Result<Converted, ConversionError> {
    let label = match value {
        Value::Enum { label, .. } | Value::String(label) => label.clone(),
        _ => return Err(cx.unsupported()),
    };
    if !labels.is_empty() && !labels.contains(&label) {
        return Err(cx.out_of_range("label is not a member of the target enum"));
    }
    Ok(Converted::exact(Value::Enum {
        type_name: name.clone(),
        label,
    }))
}

fn to_json_value(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Json(_) => Ok(Converted::exact(value.clone())),
        Value::String(s) => serde_json::from_str::<serde_json::Value>(s)
            .map(|j| Converted::exact(Value::Json(j)))
            .map_err(|_| cx.invalid("text is not valid JSON")),
        other => {
            let (json, exact) = value_to_json(other);
            if exact {
                Ok(Converted::exact(Value::Json(json)))
            } else {
                cx.lossy(
                    Value::Json(json),
                    ConversionClass::PotentiallyLossy,
                    "non-JSON-native values encoded as JSON strings",
                )
            }
        }
    }
}

fn to_geometry(cx: &Ctx<'_>, value: &Value) -> Result<Converted, ConversionError> {
    match value {
        Value::Geometry { .. } => Ok(Converted::exact(value.clone())),
        Value::Binary(b) => {
            // Minimal WKB sanity check: byte-order marker plus a 4-byte type.
            if b.len() < 5 || b[0] > 1 {
                return Err(cx.invalid("binary value is not Well-Known Binary"));
            }
            Ok(Converted::exact(Value::Geometry {
                srid: None,
                wkb: b.clone(),
            }))
        }
        _ => Err(cx.unsupported()),
    }
}

/// Converts plain JSON into canonical values.
pub fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                Value::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => Value::Array(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// Converts a canonical value to plain JSON.
///
/// The boolean is `true` when the conversion is exact: every nested value
/// was JSON-native. Otherwise non-native values (decimals, bytes, dates, …)
/// were encoded as strings and their type identity was lost.
pub fn value_to_json(value: &Value) -> (serde_json::Value, bool) {
    use serde_json::Value as J;
    match value {
        Value::Null => (J::Null, true),
        // Missing has no JSON form inside arrays; inside objects the caller
        // omits the key (see the Object arm).
        Value::Missing => (J::Null, false),
        Value::Bool(b) => (J::Bool(*b), true),
        Value::Int(n) => (J::from(*n), true),
        Value::UInt(n) => (J::from(*n), true),
        Value::Float(f) => match serde_json::Number::from_f64(*f) {
            Some(n) => (J::Number(n), true),
            None => (J::String(format_float(*f)), false),
        },
        Value::String(s) => (J::String(s.clone()), true),
        Value::Json(j) => (j.clone(), true),
        Value::Array(items) => {
            let mut exact = true;
            let out = items
                .iter()
                .map(|v| {
                    let (j, e) = value_to_json(v);
                    exact &= e;
                    j
                })
                .collect();
            (J::Array(out), exact)
        }
        Value::Object(fields) => {
            let mut exact = true;
            let mut out = serde_json::Map::with_capacity(fields.len());
            for (k, v) in fields {
                if matches!(v, Value::Missing) {
                    continue;
                }
                let (j, e) = value_to_json(v);
                exact &= e;
                out.insert(k.clone(), j);
            }
            (J::Object(out), exact)
        }
        Value::Map(entries) => {
            let mut out = serde_json::Map::with_capacity(entries.len());
            let mut exact = true;
            for (k, v) in entries {
                let key = match k {
                    Value::String(s) => s.clone(),
                    other => {
                        exact = false;
                        scalar_to_text(other).unwrap_or_else(|| value_to_json(other).0.to_string())
                    }
                };
                let (j, e) = value_to_json(v);
                exact &= e;
                out.insert(key, j);
            }
            (J::Object(out), exact)
        }
        Value::Binary(b) => (J::String(BASE64.encode(b)), false),
        Value::Geometry { wkb, .. } => (J::String(BASE64.encode(wkb)), false),
        Value::Extension {
            binary: Some(b),
            text: None,
            ..
        } => (J::String(BASE64.encode(b)), false),
        Value::Invalid { .. } => (J::Null, false),
        other => (J::String(scalar_to_text(other).unwrap_or_default()), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict() -> ConversionPolicy {
        ConversionPolicy::strict()
    }

    #[test]
    fn classification_examples() {
        use CanonicalType as T;
        assert_eq!(
            classify(&T::Int { bits: 32 }, &T::I64),
            ConversionClass::Lossless
        );
        assert_eq!(
            classify(&T::I64, &T::Int { bits: 32 }),
            ConversionClass::ConditionallyLossless
        );
        assert_eq!(
            classify(&T::I64, &T::F64),
            ConversionClass::PotentiallyLossy
        );
        assert_eq!(
            classify(&T::Int { bits: 32 }, &T::F64),
            ConversionClass::Lossless
        );
        assert_eq!(
            classify(
                &T::ZonedDateTime,
                &T::LocalDateTime {
                    fractional_digits: 6
                }
            ),
            ConversionClass::DefinitelyLossy
        );
        assert_eq!(
            classify(&T::Geometry, &T::Duration),
            ConversionClass::Unsupported
        );
        assert_eq!(
            classify(
                &T::Decimal {
                    precision: Some(10),
                    scale: Some(2)
                },
                &T::Decimal {
                    precision: Some(12),
                    scale: Some(4)
                }
            ),
            ConversionClass::Lossless
        );
        assert_eq!(
            classify(
                &T::Decimal {
                    precision: Some(10),
                    scale: Some(4)
                },
                &T::Decimal {
                    precision: Some(10),
                    scale: Some(2)
                }
            ),
            ConversionClass::PotentiallyLossy
        );
    }

    #[test]
    fn integer_overflow_is_an_error_not_a_wrap() {
        let err = convert(
            &Value::Int(40_000),
            &CanonicalType::Int { bits: 16 },
            &strict(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::OutOfRange { .. }));
        let err = convert(
            &Value::Int(-1),
            &CanonicalType::UInt { bits: 64 },
            &strict(),
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::OutOfRange { .. }));
        let ok = convert(&Value::UInt(u64::MAX), &CanonicalType::BigInt, &strict()).unwrap();
        assert_eq!(ok.value, Value::BigInt(BigInt::from(u64::MAX)));
    }

    #[test]
    fn decimal_rounding_requires_approval() {
        let v = Value::Decimal(BigDecimal::from_str("12.345").unwrap());
        let target = CanonicalType::Decimal {
            precision: Some(10),
            scale: Some(2),
        };
        let err = convert(&v, &target, &strict()).unwrap_err();
        assert!(matches!(err, ConversionError::LossNotApproved(_)));

        let mut policy = strict();
        policy.allow_potentially_lossy = true;
        let ok = convert(&v, &target, &policy).unwrap();
        assert_eq!(
            ok.value,
            Value::Decimal(BigDecimal::from_str("12.34").unwrap())
        );
        assert!(ok.loss.is_some());
    }

    #[test]
    fn decimal_rescale_without_loss_is_exact() {
        let v = Value::Decimal(BigDecimal::from_str("12.30").unwrap());
        let target = CanonicalType::Decimal {
            precision: Some(5),
            scale: Some(1),
        };
        let ok = convert(&v, &target, &strict()).unwrap();
        assert!(ok.loss.is_none());
        let target = CanonicalType::Decimal {
            precision: Some(3),
            scale: Some(2),
        };
        assert!(matches!(
            convert(&v, &target, &strict()),
            Err(ConversionError::OutOfRange { .. })
        ));
    }

    #[test]
    fn large_integer_to_float_reports_precision_loss() {
        let v = Value::Int((1 << 53) + 1);
        assert!(matches!(
            convert(&v, &CanonicalType::F64, &strict()),
            Err(ConversionError::LossNotApproved(_))
        ));
        let ok = convert(&Value::Int(1 << 53), &CanonicalType::F64, &strict()).unwrap();
        assert!(ok.loss.is_none());
    }

    #[test]
    fn decimal_point_one_to_float_is_round_trippable() {
        let v = Value::String("0.1".into());
        let ok = convert(&v, &CanonicalType::F64, &strict()).unwrap();
        assert_eq!(ok.value, Value::Float(0.1));
        assert!(ok.loss.is_none());
    }

    #[test]
    fn string_truncation_is_definitely_lossy() {
        let v = Value::String("غزة جسر".into());
        let target = CanonicalType::String { max_chars: Some(3) };
        assert!(matches!(
            convert(&v, &target, &strict()),
            Err(ConversionError::LossNotApproved(_))
        ));
        let ok = convert(&v, &target, &ConversionPolicy::permissive()).unwrap();
        assert_eq!(ok.value, Value::String("غزة".into()));
        assert_eq!(ok.loss.unwrap().class, ConversionClass::DefinitelyLossy);
    }

    #[test]
    fn zoned_to_utc_drops_offset_only_with_approval() {
        let instant = DateTime::parse_from_rfc3339("2024-03-31T02:30:00+03:00").unwrap();
        let v = Value::ZonedDateTime {
            instant,
            zone: Some("Asia/Gaza".into()),
        };
        let target = CanonicalType::UtcDateTime {
            fractional_digits: 6,
        };
        assert!(convert(&v, &target, &strict()).is_err());
        let ok = convert(&v, &target, &ConversionPolicy::permissive()).unwrap();
        assert_eq!(ok.value, Value::UtcDateTime(instant.with_timezone(&Utc)));
    }

    #[test]
    fn local_to_utc_needs_an_assumed_offset() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let v = Value::LocalDateTime(dt);
        let target = CanonicalType::UtcDateTime {
            fractional_digits: 6,
        };
        assert!(matches!(
            convert(&v, &target, &ConversionPolicy::permissive()),
            Err(ConversionError::InvalidFormat { .. })
        ));
        let mut policy = ConversionPolicy::permissive();
        policy.assume_utc_offset_seconds = Some(2 * 3600);
        let ok = convert(&v, &target, &policy).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(ok.value, Value::UtcDateTime(expected));
    }

    #[test]
    fn fractional_seconds_truncation_is_reported() {
        let t = NaiveTime::from_hms_nano_opt(1, 2, 3, 123_456_789).unwrap();
        let target = CanonicalType::Time {
            fractional_digits: 6,
        };
        assert!(convert(&Value::Time(t), &target, &strict()).is_err());
        let t6 = NaiveTime::from_hms_micro_opt(1, 2, 3, 123_456).unwrap();
        assert!(
            convert(&Value::Time(t6), &target, &strict())
                .unwrap()
                .loss
                .is_none()
        );
    }

    #[test]
    fn object_to_json_flags_non_native_values() {
        let mut obj = IndexMap::new();
        obj.insert("n".to_owned(), Value::Int(1));
        let exact = convert(&Value::Object(obj.clone()), &CanonicalType::Json, &strict()).unwrap();
        assert!(exact.loss.is_none());
        obj.insert(
            "d".to_owned(),
            Value::Decimal(BigDecimal::from_str("1.10").unwrap()),
        );
        assert!(convert(&Value::Object(obj), &CanonicalType::Json, &strict()).is_err());
    }

    #[test]
    fn object_to_json_omits_missing_fields() {
        let mut obj = IndexMap::new();
        obj.insert("a".to_owned(), Value::Missing);
        obj.insert("b".to_owned(), Value::Null);
        let (json, exact) = value_to_json(&Value::Object(obj));
        assert!(exact);
        assert_eq!(json.to_string(), r#"{"b":null}"#);
    }

    #[test]
    fn object_id_round_trips_through_text() {
        let oid = Value::ObjectId([0x65, 0x1f, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let text = convert(&oid, &CanonicalType::TEXT, &strict())
            .unwrap()
            .value;
        let back = convert(&text, &CanonicalType::ObjectId, &strict())
            .unwrap()
            .value;
        assert_eq!(back, oid);
    }

    #[test]
    fn zoned_text_keeps_iana_zone() {
        let instant = DateTime::parse_from_rfc3339("2024-10-27T01:30:00+02:00").unwrap();
        let v = Value::ZonedDateTime {
            instant,
            zone: Some("Asia/Gaza".into()),
        };
        let text = convert(&v, &CanonicalType::TEXT, &strict()).unwrap().value;
        assert_eq!(
            text,
            Value::String("2024-10-27T01:30:00+02:00[Asia/Gaza]".into())
        );
        let back = convert(&text, &CanonicalType::ZonedDateTime, &strict())
            .unwrap()
            .value;
        assert_eq!(back, v);
    }

    #[test]
    fn durations_and_intervals_round_trip_through_text() {
        let d = Value::Duration(Duration::from_total_nanos(-1_500_000_001).unwrap());
        let text = convert(&d, &CanonicalType::TEXT, &strict()).unwrap().value;
        assert_eq!(
            convert(&text, &CanonicalType::Duration, &strict())
                .unwrap()
                .value,
            d
        );

        let i = Value::Interval(Interval {
            months: -14,
            days: 3,
            microseconds: -4_500_000,
        });
        let text = convert(&i, &CanonicalType::TEXT, &strict()).unwrap().value;
        assert_eq!(text, Value::String("P-14M3DT-4.5S".into()));
        assert_eq!(
            convert(&text, &CanonicalType::Interval, &strict())
                .unwrap()
                .value,
            i
        );
        assert!(
            convert(
                &Value::String("PT1.0000000001S".into()),
                &CanonicalType::Duration,
                &strict()
            )
            .is_err()
        );
    }

    #[test]
    fn null_and_missing_pass_through_every_target() {
        for t in [
            CanonicalType::Boolean,
            CanonicalType::Uuid,
            CanonicalType::Json,
            CanonicalType::Geometry,
        ] {
            assert_eq!(
                convert(&Value::Null, &t, &strict()).unwrap().value,
                Value::Null
            );
            assert_eq!(
                convert(&Value::Missing, &t, &strict()).unwrap().value,
                Value::Missing
            );
        }
    }

    #[test]
    fn invalid_utf8_binary_is_not_silently_converted() {
        let v = Value::Binary(vec![0xff, 0xfe]);
        assert!(matches!(
            convert(&v, &CanonicalType::TEXT, &ConversionPolicy::permissive()),
            Err(ConversionError::InvalidFormat { .. })
        ));
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    fn scalar() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(Value::UInt),
            any::<f64>().prop_map(Value::Float),
            ".{0,40}".prop_map(Value::String),
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(Value::Binary),
            (any::<i64>(), 0u32..8)
                .prop_map(|(m, s)| Value::Decimal(BigDecimal::new(BigInt::from(m), i64::from(s)))),
            any::<[u8; 12]>().prop_map(Value::ObjectId),
        ]
    }

    fn target() -> impl Strategy<Value = CanonicalType> {
        prop_oneof![
            Just(CanonicalType::Boolean),
            (prop::sample::select(vec![8u8, 16, 32, 64]))
                .prop_map(|bits| CanonicalType::Int { bits }),
            (prop::sample::select(vec![8u8, 16, 32, 64]))
                .prop_map(|bits| CanonicalType::UInt { bits }),
            Just(CanonicalType::BigInt),
            (1u32..30, 0i32..6).prop_map(|(p, s)| CanonicalType::Decimal {
                precision: Some(p.max(s as u32)),
                scale: Some(s)
            }),
            Just(CanonicalType::Float { bits: 32 }),
            Just(CanonicalType::F64),
            (0u64..20).prop_map(|n| CanonicalType::String { max_chars: Some(n) }),
            Just(CanonicalType::TEXT),
            Just(CanonicalType::BYTES),
            Just(CanonicalType::ObjectId),
            Just(CanonicalType::Json),
        ]
    }

    proptest! {
        /// Under a strict policy a conversion either fails or is exact.
        #[test]
        fn strict_policy_never_loses_data(v in scalar(), t in target()) {
            if let Ok(c) = convert(&v, &t, &ConversionPolicy::strict()) {
                prop_assert!(c.loss.is_none(), "loss under strict policy: {:?}", c.loss);
            }
        }

        /// Recorded loss never exceeds what the type-level classifier predicted.
        #[test]
        fn value_loss_is_bounded_by_classification(v in scalar(), t in target()) {
            if let Ok(c) = convert(&v, &t, &ConversionPolicy::permissive())
                && let Some(loss) = c.loss
            {
                let predicted = classify(&v.canonical_type(), &t);
                prop_assert!(loss.class <= predicted, "{loss:?} vs predicted {predicted}");
            }
        }

        #[test]
        fn integers_round_trip_through_text(n in any::<i64>()) {
            let text = convert(&Value::Int(n), &CanonicalType::TEXT, &ConversionPolicy::strict()).unwrap().value;
            let back = convert(&text, &CanonicalType::I64, &ConversionPolicy::strict()).unwrap().value;
            prop_assert_eq!(back, Value::Int(n));
        }

        #[test]
        fn finite_floats_round_trip_through_text(f in any::<f64>().prop_filter("finite", |f| f.is_finite())) {
            let text = convert(&Value::Float(f), &CanonicalType::TEXT, &ConversionPolicy::strict()).unwrap().value;
            let back = convert(&text, &CanonicalType::F64, &ConversionPolicy::strict()).unwrap().value;
            prop_assert_eq!(back, Value::Float(f));
        }

        #[test]
        fn decimals_round_trip_through_text(m in any::<i64>(), s in 0i64..12) {
            let d = Value::Decimal(BigDecimal::new(BigInt::from(m), s));
            let text = convert(&d, &CanonicalType::TEXT, &ConversionPolicy::strict()).unwrap().value;
            let back = convert(&text, &CanonicalType::Decimal { precision: None, scale: None }, &ConversionPolicy::strict()).unwrap().value;
            prop_assert_eq!(back, d);
        }
    }
}
