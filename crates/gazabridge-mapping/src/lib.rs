//! Document ⇄ relational mappings.
//!
//! Pure functions, no I/O: a mapping is planned from a sampled document
//! structure (or a table schema), shown to the user as a preview, stored with
//! the job so a resumed run converts exactly like the first one, and applied
//! record by record by the migration engine.
//!
//! * [`doc_to_table`]: MongoDB collections → PostgreSQL tables, as one JSONB
//!   column, flattened columns, normalized child tables for arrays, or a
//!   hybrid of typed columns plus the whole document.
//! * [`table_to_doc`]: PostgreSQL tables → MongoDB collections, one document
//!   per row with references kept, or child rows embedded in their parent.

pub mod doc_to_table;
pub mod table_to_doc;

use gazabridge_bson::bson::Bson;
use gazabridge_domain::Value;
use gazabridge_domain::conversion::value_to_json;
use sha2::{Digest, Sha256};

/// PostgreSQL's identifier limit in bytes.
const MAX_IDENT: usize = 63;

/// A safe identifier: letters (any script), digits and `_`, at most 63
/// bytes (long names keep a hash suffix so two long names stay distinct).
pub fn ident(raw: &str) -> String {
    let mut s: String = raw
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() {
        s.push('_');
    }
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        s.insert(0, '_');
    }
    if s.len() > MAX_IDENT {
        let hash = hex::encode(Sha256::digest(raw.as_bytes()));
        let mut cut = MAX_IDENT - 9;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
        s.push('_');
        s.push_str(&hash[..8]);
    }
    s
}

/// MongoDB relaxed Extended JSON for a value, the readable JSON form that
/// keeps ObjectIds, dates and decimals recognisable (`{"$oid": …}`).
pub fn ext_json(v: &Value) -> serde_json::Value {
    match gazabridge_bson::to_bson(v, None) {
        Ok(Some(b)) => b.into_relaxed_extjson(),
        _ => value_to_json(v).0,
    }
}

/// Relaxed Extended JSON for a whole stored document, except that Int64
/// values small enough for Int32 keep their type as `{"$numberLong": …}`
/// (relaxed JSON writes both as a plain number, and a plain number that fits
/// is read back as Int32).
pub fn stored_json(b: &Bson) -> serde_json::Value {
    match b {
        Bson::Int64(n) if i32::try_from(*n).is_ok() => {
            serde_json::json!({ "$numberLong": n.to_string() })
        }
        Bson::Document(d) => {
            serde_json::Value::Object(d.iter().map(|(k, v)| (k.clone(), stored_json(v))).collect())
        }
        Bson::Array(a) => serde_json::Value::Array(a.iter().map(stored_json).collect()),
        other => other.clone().into_relaxed_extjson(),
    }
}

/// Writes doubles with more than 15 significant digits as Extended JSON
/// `{"$numberDouble": "…"}`. MySQL's JSON parser rounds such numbers by a
/// unit or two in the last place; the tagged form keeps them exact and
/// [`from_ext_json`] reads it back as the same double.
pub fn exact_doubles(j: &mut serde_json::Value) {
    match j {
        serde_json::Value::Number(n) if n.is_f64() => {
            if let Some(f) = n.as_f64()
                && f.is_finite()
            {
                let text = format!("{f:?}");
                let mantissa = text.split(['e', 'E']).next().unwrap_or_default();
                let digits = mantissa
                    .chars()
                    .filter(char::is_ascii_digit)
                    .skip_while(|c| *c == '0')
                    .count();
                if digits > 15 {
                    *j = serde_json::json!({ "$numberDouble": text });
                }
            }
        }
        serde_json::Value::Array(a) => a.iter_mut().for_each(exact_doubles),
        serde_json::Value::Object(m) => m.values_mut().for_each(exact_doubles),
        _ => {}
    }
}

/// Reads JSON written by [`ext_json`] (or any JSON) back into a value,
/// restoring Extended JSON types.
pub fn from_ext_json(j: &serde_json::Value) -> Value {
    match Bson::try_from(j.clone()) {
        Ok(b) => gazabridge_bson::from_bson(&b),
        Err(_) => gazabridge_domain::conversion::json_to_value(j),
    }
}

/// Short display text for previews.
pub fn display(v: &Value) -> String {
    match v {
        Value::Null | Value::Missing => "NULL".into(),
        Value::Json(j) => j.to_string(),
        Value::String(s) => s.clone(),
        Value::ObjectId(b) => format!("ObjectId(\"{}\")", hex::encode(b)),
        other => gazabridge_domain::conversion::scalar_to_text(other)
            .unwrap_or_else(|| ext_json(other).to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_safe_and_bounded() {
        assert_eq!(ident("address.city"), "address_city");
        assert_eq!(ident("2nd"), "_2nd");
        assert_eq!(ident("عنوان المنزل"), "عنوان_المنزل");
        assert!(ident(&"ع".repeat(40)).len() <= 63);
        let long = "x".repeat(100);
        let (a, b) = (ident(&long), ident(&format!("{long}y")));
        assert!(a.len() <= 63 && b.len() <= 63 && a != b);
    }
}

#[cfg(test)]
mod exact_double_tests {
    use super::*;

    #[test]
    fn long_doubles_are_tagged_and_read_back_exactly() {
        let mut j = serde_json::json!({
            "a": 90.33333333333333_f64,
            "b": 1.5,
            "c": [0.1, 1e300, 2.0000000000000004],
            "d": 7,
            "e": { "f": -123.45678901234567_f64 }
        });
        exact_doubles(&mut j);
        assert_eq!(
            j["a"],
            serde_json::json!({ "$numberDouble": "90.33333333333333" })
        );
        assert_eq!(j["b"], serde_json::json!(1.5));
        assert_eq!(j["c"][0], serde_json::json!(0.1));
        assert_eq!(j["c"][1], serde_json::json!(1e300));
        assert!(j["c"][2].is_object());
        assert_eq!(j["d"], serde_json::json!(7));
        assert!(j["e"]["f"].is_object());
        assert_eq!(from_ext_json(&j["a"]), Value::Float(90.33333333333333));
        assert_eq!(
            from_ext_json(&j["e"]["f"]),
            Value::Float(-123.45678901234567)
        );
    }

    #[test]
    fn stored_documents_keep_small_longs() {
        let d = Bson::Document(
            gazabridge_bson::bson::doc! { "a": 0_i64, "b": 5_i32, "c": [1_i64], "d": { "e": 3_000_000_000_i64 } },
        );
        let j = stored_json(&d);
        assert_eq!(j["a"], serde_json::json!({ "$numberLong": "0" }));
        assert_eq!(j["b"], serde_json::json!(5));
        assert_eq!(j["c"][0], serde_json::json!({ "$numberLong": "1" }));
        assert_eq!(j["d"]["e"], serde_json::json!(3_000_000_000_i64));
        assert_eq!(Bson::try_from(j).unwrap(), d);
    }
}
