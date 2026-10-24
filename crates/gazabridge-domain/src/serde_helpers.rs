//! Serde helpers that keep the canonical model lossless when serialized.
//!
//! JSON cannot carry arbitrary-precision numbers, raw bytes or non-finite
//! floats, so those are encoded as strings with an explicit format.

pub(crate) mod display_fromstr {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<T: Display, S: Serializer>(value: &T, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(value)
    }

    pub fn deserialize<'de, T, D>(d: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(d)?;
        raw.parse().map_err(D::Error::custom)
    }
}

pub(crate) mod base64_bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let raw = String::deserialize(d)?;
        STANDARD.decode(raw).map_err(D::Error::custom)
    }
}

pub(crate) mod opt_base64_bytes {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => s.serialize_some(&STANDARD.encode(bytes)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|raw| STANDARD.decode(raw).map_err(D::Error::custom))
            .transpose()
    }
}

pub(crate) mod hex12 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &[u8; 12], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 12], D::Error> {
        let raw = String::deserialize(d)?;
        let mut out = [0u8; 12];
        hex::decode_to_slice(raw, &mut out).map_err(D::Error::custom)?;
        Ok(out)
    }
}

/// Floats are serialized as numbers when finite and as the strings `NaN`,
/// `Infinity` or `-Infinity` otherwise, so no value is lost through JSON.
pub(crate) mod float {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
        if value.is_finite() {
            s.serialize_f64(*value)
        } else if value.is_nan() {
            s.serialize_str("NaN")
        } else if value.is_sign_positive() {
            s.serialize_str("Infinity")
        } else {
            s.serialize_str("-Infinity")
        }
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Num(f64),
        Str(String),
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        match Repr::deserialize(d)? {
            Repr::Num(n) => Ok(n),
            Repr::Str(s) => match s.as_str() {
                "NaN" => Ok(f64::NAN),
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                other => Err(D::Error::custom(format!("invalid float literal `{other}`"))),
            },
        }
    }
}
