//! Message catalogues.
//!
//! Catalogues are JSON files under `locales/`, embedded at compile time.
//! Lookups fall back to English and then to the key itself, so a missing
//! translation is visible rather than blank. A test keeps catalogues in sync.

use std::collections::HashMap;
use std::sync::OnceLock;

use gazabridge_ipc::Locale;

const EN: &str = include_str!("../locales/en.json");
const AR: &str = include_str!("../locales/ar.json");

fn parse(raw: &str) -> HashMap<String, String> {
    serde_json::from_str(raw).unwrap_or_default()
}

fn catalogue(locale: Locale) -> &'static HashMap<String, String> {
    static EN_CAT: OnceLock<HashMap<String, String>> = OnceLock::new();
    static AR_CAT: OnceLock<HashMap<String, String>> = OnceLock::new();
    match locale {
        Locale::En => EN_CAT.get_or_init(|| parse(EN)),
        Locale::Ar => AR_CAT.get_or_init(|| parse(AR)),
    }
}

/// Translates `key` for `locale`.
pub fn t(locale: Locale, key: &str) -> String {
    catalogue(locale)
        .get(key)
        .or_else(|| catalogue(Locale::En).get(key))
        .cloned()
        .unwrap_or_else(|| key.to_owned())
}

/// Maps a backend compatibility-status label to a translation key.
pub fn status_key(label: &str) -> &'static str {
    match label {
        "Production Ready" => "status.production_ready",
        "Beta" => "status.beta",
        "Protocol Compatible" => "status.protocol_compatible",
        "Limited Compatibility" => "status.limited",
        "Ready" => "status.ready",
        _ => "status.experimental",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn catalogues_parse_and_have_identical_keys() {
        let en: BTreeSet<_> = parse(EN).into_keys().collect();
        let ar: BTreeSet<_> = parse(AR).into_keys().collect();
        assert!(!en.is_empty());
        let missing_ar: Vec<_> = en.difference(&ar).collect();
        let extra_ar: Vec<_> = ar.difference(&en).collect();
        assert!(missing_ar.is_empty(), "missing Arabic keys: {missing_ar:?}");
        assert!(
            extra_ar.is_empty(),
            "Arabic keys not in English: {extra_ar:?}"
        );
    }

    #[test]
    fn lookup_falls_back() {
        assert_eq!(t(Locale::Ar, "nav.home"), "الرئيسية");
        assert_eq!(t(Locale::En, "no.such.key"), "no.such.key");
    }
}
