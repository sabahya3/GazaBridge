//! Redaction of secrets from text bound for logs, errors and reports.

use std::sync::{Arc, RwLock};

use zeroize::Zeroizing;

const MASK: &str = "****";

/// Secrets shorter than this are not scrubbed by value: masking every
/// occurrence of e.g. `ab` would destroy unrelated text. Such secrets are
/// still caught by the structural (`password=…`, URI) rules.
const MIN_SCRUB_LEN: usize = 4;

/// Scrubs known secret values and credential-shaped fragments from text.
///
/// Clones share the same set of known secrets.
#[derive(Clone, Default)]
pub struct Redactor {
    known: Arc<RwLock<Vec<Zeroizing<String>>>>,
}

impl std::fmt::Debug for Redactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.known.read().map(|k| k.len()).unwrap_or(0);
        f.debug_struct("Redactor")
            .field("known_secrets", &n)
            .finish()
    }
}

impl Redactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remembers a secret value so future output never contains it.
    pub fn register(&self, secret: &str) {
        if secret.len() < MIN_SCRUB_LEN {
            return;
        }
        if let Ok(mut known) = self.known.write()
            && !known.iter().any(|k| k.as_str() == secret)
        {
            known.push(Zeroizing::new(secret.to_owned()));
            // Longest first so a secret containing another is masked whole.
            known.sort_by_key(|k| std::cmp::Reverse(k.len()));
        }
    }

    /// Masks registered secret values, URI passwords and `key=value` secrets.
    pub fn scrub(&self, text: &str) -> String {
        let mut out = gazabridge_domain::connection_string::redact(text);
        if let Ok(known) = self.known.read() {
            for secret in known.iter() {
                if out.contains(secret.as_str()) {
                    out = out.replace(secret.as_str(), MASK);
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn scrubs_known_and_structural_secrets() {
        let r = Redactor::new();
        r.register("s3cr3t-token");
        let s = r.scrub("token s3cr3t-token used; dsn=postgres://u:pw@h/db password=abc");
        assert!(!s.contains("s3cr3t-token"));
        assert!(!s.contains(":pw@"));
        assert!(!s.contains("password=abc"));
    }

    #[test]
    fn short_secrets_are_not_scrubbed_by_value() {
        let r = Redactor::new();
        r.register("ab");
        assert_eq!(r.scrub("tab"), "tab");
    }

    proptest! {
        #[test]
        fn registered_secret_never_survives(secret in "[A-Za-z0-9!@#%^&*]{4,24}", prefix in ".{0,20}", suffix in ".{0,20}") {
            let r = Redactor::new();
            r.register(&secret);
            let out = r.scrub(&format!("{prefix}{secret}{suffix}"));
            prop_assert!(!out.contains(&secret));
        }
    }
}
