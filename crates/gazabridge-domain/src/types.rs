//! Canonical logical types.
//!
//! Connectors map native column/field types to [`CanonicalType`]; the
//! conversion classifier and mapping engine reason only in these terms.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalType {
    /// Only ever null (e.g. an all-null inferred field).
    Null,
    Boolean,
    /// Signed integer of `bits` width (8, 16, 24, 32 or 64).
    Int {
        bits: u8,
    },
    /// Unsigned integer of `bits` width (8, 16, 24, 32 or 64).
    UInt {
        bits: u8,
    },
    /// Arbitrary-precision integer.
    BigInt,
    /// Exact decimal. `None` precision means unbounded.
    Decimal {
        precision: Option<u32>,
        scale: Option<i32>,
    },
    /// IEEE-754 float of 32 or 64 bits.
    Float {
        bits: u8,
    },
    String {
        max_chars: Option<u64>,
    },
    Binary {
        max_bytes: Option<u64>,
    },
    Uuid,
    ObjectId,
    Date,
    Time {
        fractional_digits: u8,
    },
    LocalDateTime {
        fractional_digits: u8,
    },
    UtcDateTime {
        fractional_digits: u8,
    },
    ZonedDateTime,
    Duration,
    Interval,
    Array(Box<CanonicalType>),
    /// Open, string-keyed document/struct.
    Object,
    Map {
        key: Box<CanonicalType>,
        value: Box<CanonicalType>,
    },
    Enum {
        name: Option<String>,
        labels: Vec<String>,
    },
    Json,
    Geometry,
    Extension {
        name: String,
    },
    /// Union of several observed types (document stores, mixed arrays).
    Mixed(Vec<CanonicalType>),
    Unknown,
}

impl CanonicalType {
    pub const I64: Self = Self::Int { bits: 64 };
    pub const F64: Self = Self::Float { bits: 64 };
    pub const TEXT: Self = Self::String { max_chars: None };
    pub const BYTES: Self = Self::Binary { max_bytes: None };

    /// Combines two observed types into one that describes both.
    ///
    /// Same-kind types widen (e.g. `Int{16}` ∪ `Int{32}` = `Int{32}`); different
    /// kinds become [`CanonicalType::Mixed`]. `Null` is absorbed, since
    /// nullability is tracked separately from the type.
    pub fn unify(self, other: Self) -> Self {
        use CanonicalType as T;
        match (self, other) {
            (a, b) if a == b => a,
            (T::Null, t) | (t, T::Null) => t,
            (T::Int { bits: a }, T::Int { bits: b }) => T::Int { bits: a.max(b) },
            (T::UInt { bits: a }, T::UInt { bits: b }) => T::UInt { bits: a.max(b) },
            (T::Float { bits: a }, T::Float { bits: b }) => T::Float { bits: a.max(b) },
            (T::String { max_chars: a }, T::String { max_chars: b }) => T::String {
                max_chars: a.zip(b).map(|(a, b)| a.max(b)),
            },
            (T::Binary { max_bytes: a }, T::Binary { max_bytes: b }) => T::Binary {
                max_bytes: a.zip(b).map(|(a, b)| a.max(b)),
            },
            (
                T::Decimal {
                    precision: pa,
                    scale: sa,
                },
                T::Decimal {
                    precision: pb,
                    scale: sb,
                },
            ) => {
                let scale = sa.zip(sb).map(|(a, b)| a.max(b));
                let precision = match (pa, sa, pb, sb, scale) {
                    (Some(pa), Some(sa), Some(pb), Some(sb), Some(s)) => {
                        let int_a = i64::from(pa) - i64::from(sa);
                        let int_b = i64::from(pb) - i64::from(sb);
                        u32::try_from(int_a.max(int_b) + i64::from(s)).ok()
                    }
                    _ => None,
                };
                T::Decimal { precision, scale }
            }
            (
                T::Time {
                    fractional_digits: a,
                },
                T::Time {
                    fractional_digits: b,
                },
            ) => T::Time {
                fractional_digits: a.max(b),
            },
            (
                T::LocalDateTime {
                    fractional_digits: a,
                },
                T::LocalDateTime {
                    fractional_digits: b,
                },
            ) => T::LocalDateTime {
                fractional_digits: a.max(b),
            },
            (
                T::UtcDateTime {
                    fractional_digits: a,
                },
                T::UtcDateTime {
                    fractional_digits: b,
                },
            ) => T::UtcDateTime {
                fractional_digits: a.max(b),
            },
            (T::Array(a), T::Array(b)) => T::Array(Box::new(a.unify(*b))),
            (T::Mixed(mut a), T::Mixed(b)) => {
                for t in b {
                    push_unique(&mut a, t);
                }
                T::Mixed(a)
            }
            (T::Mixed(mut a), t) | (t, T::Mixed(mut a)) => {
                push_unique(&mut a, t);
                T::Mixed(a)
            }
            (a, b) => T::Mixed(vec![a, b]),
        }
    }

    pub fn unify_all(types: impl IntoIterator<Item = Self>) -> Self {
        types.into_iter().fold(Self::Null, Self::unify)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int { .. }
                | Self::UInt { .. }
                | Self::BigInt
                | Self::Decimal { .. }
                | Self::Float { .. }
        )
    }

    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date
                | Self::Time { .. }
                | Self::LocalDateTime { .. }
                | Self::UtcDateTime { .. }
                | Self::ZonedDateTime
        )
    }

    pub fn is_container(&self) -> bool {
        matches!(
            self,
            Self::Array(_) | Self::Object | Self::Map { .. } | Self::Json
        )
    }
}

fn push_unique(list: &mut Vec<CanonicalType>, t: CanonicalType) {
    if t == CanonicalType::Null {
        return;
    }
    if let Some(pos) = list
        .iter()
        .position(|e| std::mem::discriminant(e) == std::mem::discriminant(&t))
    {
        let existing = list.remove(pos);
        let widened = existing.unify(t);
        match widened {
            CanonicalType::Mixed(parts) => list.extend(parts),
            other => list.insert(pos, other),
        }
    } else {
        list.push(t);
    }
}

impl fmt::Display for CanonicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::Boolean => f.write_str("boolean"),
            Self::Int { bits } => write!(f, "int{bits}"),
            Self::UInt { bits } => write!(f, "uint{bits}"),
            Self::BigInt => f.write_str("bigint"),
            Self::Decimal { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => write!(f, "decimal({p},{s})"),
                (Some(p), None) => write!(f, "decimal({p})"),
                _ => f.write_str("decimal"),
            },
            Self::Float { bits } => write!(f, "float{bits}"),
            Self::String { max_chars: Some(n) } => write!(f, "string({n})"),
            Self::String { max_chars: None } => f.write_str("string"),
            Self::Binary { max_bytes: Some(n) } => write!(f, "binary({n})"),
            Self::Binary { max_bytes: None } => f.write_str("binary"),
            Self::Uuid => f.write_str("uuid"),
            Self::ObjectId => f.write_str("object_id"),
            Self::Date => f.write_str("date"),
            Self::Time { fractional_digits } => write!(f, "time({fractional_digits})"),
            Self::LocalDateTime { fractional_digits } => {
                write!(f, "local_datetime({fractional_digits})")
            }
            Self::UtcDateTime { fractional_digits } => {
                write!(f, "utc_datetime({fractional_digits})")
            }
            Self::ZonedDateTime => f.write_str("zoned_datetime"),
            Self::Duration => f.write_str("duration"),
            Self::Interval => f.write_str("interval"),
            Self::Array(inner) => write!(f, "array<{inner}>"),
            Self::Object => f.write_str("object"),
            Self::Map { key, value } => write!(f, "map<{key},{value}>"),
            Self::Enum { name: Some(n), .. } => write!(f, "enum({n})"),
            Self::Enum { name: None, .. } => f.write_str("enum"),
            Self::Json => f.write_str("json"),
            Self::Geometry => f.write_str("geometry"),
            Self::Extension { name } => write!(f, "extension({name})"),
            Self::Mixed(parts) => {
                f.write_str("mixed<")?;
                for (i, p) in parts.iter().enumerate() {
                    if i > 0 {
                        f.write_str("|")?;
                    }
                    write!(f, "{p}")?;
                }
                f.write_str(">")
            }
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unify_widens_same_kind() {
        let t = CanonicalType::Int { bits: 16 }.unify(CanonicalType::Int { bits: 32 });
        assert_eq!(t, CanonicalType::Int { bits: 32 });
    }

    #[test]
    fn unify_absorbs_null() {
        assert_eq!(
            CanonicalType::Null.unify(CanonicalType::Boolean),
            CanonicalType::Boolean
        );
    }

    #[test]
    fn unify_creates_flat_mixed() {
        let t = CanonicalType::unify_all([
            CanonicalType::I64,
            CanonicalType::TEXT,
            CanonicalType::Int { bits: 32 },
            CanonicalType::Boolean,
        ]);
        assert_eq!(
            t,
            CanonicalType::Mixed(vec![
                CanonicalType::I64,
                CanonicalType::TEXT,
                CanonicalType::Boolean
            ])
        );
    }

    #[test]
    fn unify_decimal_keeps_integer_digits_and_scale() {
        let a = CanonicalType::Decimal {
            precision: Some(10),
            scale: Some(2),
        };
        let b = CanonicalType::Decimal {
            precision: Some(6),
            scale: Some(4),
        };
        assert_eq!(
            a.unify(b),
            CanonicalType::Decimal {
                precision: Some(12),
                scale: Some(4)
            }
        );
    }
}
