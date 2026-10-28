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
