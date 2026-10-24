//! Canonical value fixtures for round-trip tests.

use std::str::FromStr;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use gazabridge_domain::{CanonicalType, Value};

/// A column used by round-trip tests with the values written to it.
#[derive(Debug, Clone)]
pub struct RoundTripColumn {
    pub name: &'static str,
    pub canonical_type: CanonicalType,
    /// One value per row; row `i` uses `values[i % values.len()]`.
    pub values: Vec<Value>,
}

fn s(v: &str) -> Value {
    Value::String(v.to_owned())
}

fn utc(text: &str) -> Value {
    Value::UtcDateTime(
        DateTime::parse_from_rfc3339(text)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_default(),
    )
}

/// Standard round-trip columns. Every connector must store and return these
/// values exactly (after loss-aware conversion back to the column type).
pub fn round_trip_columns() -> Vec<RoundTripColumn> {
    let big_text: String = "غزة ".repeat(20_000);
    vec![
        RoundTripColumn {
            name: "id",
            canonical_type: CanonicalType::I64,
            values: vec![], // filled with the row number
        },
        RoundTripColumn {
            name: "flag",
            canonical_type: CanonicalType::Boolean,
            values: vec![Value::Bool(true), Value::Bool(false), Value::Null],
        },
        RoundTripColumn {
            name: "small_int",
            canonical_type: CanonicalType::Int { bits: 32 },
            values: vec![
                Value::Int(i64::from(i32::MIN)),
                Value::Int(0),
                Value::Int(i64::from(i32::MAX)),
            ],
        },
        RoundTripColumn {
            name: "big_int",
            canonical_type: CanonicalType::I64,
            values: vec![Value::Int(i64::MIN), Value::Int(-1), Value::Int(i64::MAX)],
        },
        RoundTripColumn {
            name: "unsigned_max",
            canonical_type: CanonicalType::UInt { bits: 64 },
            values: vec![Value::UInt(0), Value::UInt(u64::MAX), Value::UInt(1 << 63)],
        },
        RoundTripColumn {
            name: "huge",
            canonical_type: CanonicalType::BigInt,
            values: vec![
                Value::BigInt(
                    "1000000000000000000000000000000"
                        .parse()
                        .unwrap_or_default(),
                ),
                Value::BigInt(
                    "-99999999999999999999999999999999"
                        .parse()
                        .unwrap_or_default(),
                ),
            ],
        },
        RoundTripColumn {
            name: "amount",
            canonical_type: CanonicalType::Decimal {
                precision: Some(38),
                scale: Some(10),
            },
            values: vec![
                Value::Decimal(
                    BigDecimal::from_str("1234567890123456789012345678.0123456789")
                        .unwrap_or_default(),
                ),
                Value::Decimal(BigDecimal::from_str("-0.0000000001").unwrap_or_default()),
                Value::Decimal(BigDecimal::from_str("0").unwrap_or_default()),
            ],
        },
        RoundTripColumn {
            name: "ratio",
            canonical_type: CanonicalType::F64,
            values: vec![
                Value::Float(0.1),
                Value::Float(-1.5e300),
                Value::Float(f64::MIN_POSITIVE),
            ],
        },
        RoundTripColumn {
            name: "label",
            canonical_type: CanonicalType::TEXT,
            values: vec![
                s("مرحبا بالعالم — Hello, world"),
                s("emoji 🌉 and combining é"),
                s(""),
                s("quote ' double \" backslash \\ newline \n tab \t"),
            ],
        },
        RoundTripColumn {
            name: "long_text",
            canonical_type: CanonicalType::TEXT,
            values: vec![Value::String(big_text), Value::Null],
        },
        RoundTripColumn {
            name: "payload",
            canonical_type: CanonicalType::BYTES,
            values: vec![
                Value::Binary((0..=255).collect()),
                Value::Binary(vec![0; 256 * 1024]),
                Value::Binary(Vec::new()),
            ],
        },
        RoundTripColumn {
            name: "day",
            canonical_type: CanonicalType::Date,
            values: vec![
                Value::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default()),
                Value::Date(NaiveDate::from_ymd_opt(2024, 2, 29).unwrap_or_default()),
                Value::Date(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap_or_default()),
            ],
        },
        RoundTripColumn {
            name: "at_time",
            canonical_type: CanonicalType::Time {
                fractional_digits: 6,
            },
            values: vec![
                Value::Time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap_or_default()),
                Value::Time(NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap_or_default()),
            ],
        },
        RoundTripColumn {
            name: "local_ts",
            canonical_type: CanonicalType::LocalDateTime {
                fractional_digits: 6,
            },
            values: vec![Value::LocalDateTime(
                NaiveDate::from_ymd_opt(2024, 3, 31)
                    .and_then(|d| d.and_hms_micro_opt(2, 30, 0, 123_456))
                    .unwrap_or_default(),
            )],
        },
        RoundTripColumn {
            name: "utc_ts",
            canonical_type: CanonicalType::UtcDateTime {
                fractional_digits: 3,
            },
            values: vec![
                // Around DST transitions in Asia/Gaza and Europe/Berlin.
                utc("2024-03-30T23:59:59.999Z"),
                utc("2024-10-26T22:00:00Z"),
                utc("1969-12-31T23:59:59Z"),
            ],
        },
        RoundTripColumn {
            name: "uid",
            canonical_type: CanonicalType::Uuid,
            values: vec![Value::Uuid(uuid::Uuid::from_u128(
                0x0192_b3c4_d5e6_7f80_9a1b_2c3d_4e5f_6071,
            ))],
        },
    ]
}

/// Builds `rows` records from [`round_trip_columns`]; `id` is the row number.
pub fn round_trip_records(rows: usize) -> Vec<gazabridge_domain::Record> {
    let cols = round_trip_columns();
    (0..rows)
        .map(|i| {
            let mut r = gazabridge_domain::Record::with_capacity(cols.len());
            for c in &cols {
                let v = if c.name == "id" {
                    Value::Int(i as i64)
                } else {
                    c.values[i % c.values.len()].clone()
                };
                r.insert(c.name, v);
            }
            r
        })
        .collect()
}
