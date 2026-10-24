//! Exact conversion between IEEE 754-2008 decimal128 (BID encoding, as used
//! by BSON) and arbitrary-precision decimals.
//!
//! Decimal128 holds up to 34 significant digits with an exponent between
//! -6176 and 6111. Encoding never rounds: values outside that range are
//! rejected so the caller can choose another representation.

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::{BigInt, BigUint, Sign};

const EXP_BIAS: i64 = 6176;
const EXP_MIN: i64 = -6176;
const EXP_MAX: i64 = 6111;

/// A decoded decimal128.
#[derive(Debug, Clone, PartialEq)]
pub enum Dec128 {
    Finite(BigDecimal),
    NaN,
    Infinity { negative: bool },
}

fn max_coefficient() -> BigUint {
    BigUint::from(10u8).pow(34) - BigUint::from(1u8)
}

/// Decodes the 16 little-endian bytes of a BSON decimal128.
pub fn decode(bytes: [u8; 16]) -> Dec128 {
    let v = u128::from_le_bytes(bytes);
    let negative = v >> 127 == 1;
    let comb = (v >> 122) & 0x1f;
    if comb == 0x1f {
        return Dec128::NaN;
    }
    if comb == 0x1e {
        return Dec128::Infinity { negative };
    }
    let (exponent, coefficient) = if (v >> 125) & 0b11 == 0b11 {
        // The "11" form implies a coefficient ≥ 2^113, which exceeds 10^34
        // and is non-canonical; the standard treats it as zero.
        (((v >> 111) & 0x3fff) as i64, 0u128)
    } else {
        (((v >> 113) & 0x3fff) as i64, v & ((1u128 << 113) - 1))
    };
    let coefficient = if BigUint::from(coefficient) > max_coefficient() {
        0
    } else {
        coefficient
    };
    let mut int = BigInt::from(coefficient);
    if negative {
        int = -int;
    }
    Dec128::Finite(BigDecimal::new(int, -(exponent - EXP_BIAS)))
}

/// Encodes a decimal exactly, or returns `None` if it needs more than 34
/// significant digits or an exponent outside decimal128's range.
pub fn encode(d: &BigDecimal) -> Option<[u8; 16]> {
    let (int, scale) = d.as_bigint_and_exponent();
    let negative = int.sign() == Sign::Minus;
    let mut coef = int.magnitude().clone();
    let mut exponent = -scale;
    let ten = BigUint::from(10u8);
    let zero = BigUint::from(0u8);
    // Drop trailing zeros only when needed to fit (value is unchanged).
    while coef > max_coefficient() && &coef % &ten == zero && exponent < EXP_MAX {
        coef /= &ten;
        exponent += 1;
    }
    if coef > max_coefficient() {
        return None;
    }
    // Exponent too large: shift zeros into the coefficient if room remains.
    while exponent > EXP_MAX {
        let shifted = &coef * &ten;
        if shifted > max_coefficient() {
            return None;
        }
        coef = shifted;
        exponent -= 1;
    }
    if exponent < EXP_MIN {
        // Would need rounding; only exact when trailing zeros can be removed.
        while exponent < EXP_MIN && &coef % &ten == zero && coef != zero {
            coef /= &ten;
            exponent += 1;
        }
        if exponent < EXP_MIN {
            if coef == zero {
                exponent = EXP_MIN;
            } else {
                return None;
            }
        }
    }
    let digits = coef.to_u64_digits();
    let mut c: u128 = 0;
    for (i, d) in digits.iter().enumerate() {
        c |= u128::from(*d) << (64 * i);
    }
    let mut v = c | (((exponent + EXP_BIAS) as u128) << 113);
    if negative {
        v |= 1u128 << 127;
    }
    Some(v.to_le_bytes())
}

pub fn nan() -> [u8; 16] {
    (0x7c00_u128 << 112).to_le_bytes()
}

pub fn infinity(negative: bool) -> [u8; 16] {
    let mut v = 0x7800_u128 << 112;
    if negative {
        v |= 1u128 << 127;
    }
    v.to_le_bytes()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use proptest::prelude::*;

    use super::*;

    fn d(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).unwrap()
    }

    #[test]
    fn known_encodings_match_the_bson_spec() {
        // From the BSON corpus: "1" = 0x3040000000000000_0000000000000001
        assert_eq!(
            u128::from_le_bytes(encode(&d("1")).unwrap()),
            0x3040_0000_0000_0000_0000_0000_0000_0001
        );
        // "-1.0" = 0xB03E000000000000_000000000000000A
        assert_eq!(
            u128::from_le_bytes(encode(&d("-1.0")).unwrap()),
            0xB03E_0000_0000_0000_0000_0000_0000_000A
        );
        // "0E-6176"
        assert_eq!(u128::from_le_bytes(encode(&d("0E-6176")).unwrap()), 0);
        assert_eq!(decode(nan()), Dec128::NaN);
        assert_eq!(decode(infinity(true)), Dec128::Infinity { negative: true });
    }

    #[test]
    fn trailing_zeros_and_scale_survive() {
        let v = d("1.500");
        let Dec128::Finite(back) = decode(encode(&v).unwrap()) else {
            panic!()
        };
        assert_eq!(back, v);
        assert_eq!(
            back.as_bigint_and_exponent().1,
            3,
            "scale is kept when it fits"
        );
    }

    #[test]
    fn values_needing_more_than_34_digits_are_rejected() {
        assert!(encode(&d("1234567890123456789012345678.0123456789")).is_none());
        assert!(encode(&d("9999999999999999999999999999999999")).is_some());
        assert!(encode(&d("99999999999999999999999999999999999")).is_none());
        // Trailing zeros beyond 34 digits are folded into the exponent.
        let big = d("1000000000000000000000000000000000000000");
        let Dec128::Finite(back) = decode(encode(&big).unwrap()) else {
            panic!()
        };
        assert_eq!(back, big);
    }

    proptest! {
        #[test]
        fn round_trip_is_exact(m in any::<i64>(), s in -40i64..40) {
            let v = BigDecimal::new(BigInt::from(m), s);
            let Dec128::Finite(back) = decode(encode(&v).unwrap()) else { panic!() };
            prop_assert_eq!(back, v);
        }
    }
}
