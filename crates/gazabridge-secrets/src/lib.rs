//! Secret storage and redaction.
//!
//! Secrets live in the OS keychain (macOS Keychain, Windows Credential
//! Manager, Linux Secret Service), in process memory for session-only
//! credentials, or in environment variables for CI. Metadata only ever holds
//! a [`SecretRef`] pointing at one of those.
//!
//! All secret values are [`Zeroizing`] strings so they are wiped on drop.

pub mod redact;
mod stores;

pub use redact::Redactor;
pub use stores::{EnvStore, KeychainStore, SecretStore, SessionStore};

use std::fmt;
use std::sync::Arc;

use gazabridge_domain::connection::SecretRef;
use zeroize::Zeroizing;

pub type SecretString = Zeroizing<String>;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("no secret is stored for this reference")]
    NotFound,
    #[error("the secret must be entered interactively")]
    PromptRequired,
    #[error("environment variable `{0}` is not set or not valid UTF-8")]
    EnvMissing(String),
    #[error("{0} secrets cannot be written")]
    ReadOnlyStore(&'static str),
    #[error("the OS secret store is unavailable: {0}")]
    Unavailable(String),
    #[error("secret store error: {0}")]
    Backend(String),
}

/// Resolves any [`SecretRef`] by dispatching to the matching store and
/// registers every resolved value with the [`Redactor`] so it can never
/// appear in logs.
#[derive(Clone)]
pub struct SecretResolver {
    keychain: Arc<dyn SecretStore>,
    session: Arc<SessionStore>,
    env: Arc<EnvStore>,
    redactor: Redactor,
}

impl fmt::Debug for SecretResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretResolver").finish_non_exhaustive()
    }
}

impl SecretResolver {
    pub fn new(keychain: Arc<dyn SecretStore>, redactor: Redactor) -> Self {
        Self {
            keychain,
            session: Arc::new(SessionStore::default()),
            env: Arc::new(EnvStore),
            redactor,
        }
    }

    /// Resolver backed by the OS keychain.
    pub fn os_default(redactor: Redactor) -> Self {
        Self::new(Arc::new(KeychainStore), redactor)
    }

    pub fn session(&self) -> &SessionStore {
        &self.session
    }

    pub fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    fn store_for(&self, r: &SecretRef) -> Result<&dyn SecretStore, SecretError> {
        match r {
            SecretRef::Keychain { .. } => Ok(self.keychain.as_ref()),
            SecretRef::Session { .. } => Ok(self.session.as_ref()),
            SecretRef::Env { .. } => Ok(self.env.as_ref()),
            SecretRef::Prompt => Err(SecretError::PromptRequired),
            SecretRef::None => Err(SecretError::NotFound),
        }
    }

    /// Returns the secret, or `None` for [`SecretRef::None`].
    pub fn resolve(&self, r: &SecretRef) -> Result<Option<SecretString>, SecretError> {
        if r.is_none() {
            return Ok(None);
        }
        let value = self.store_for(r)?.get(r)?;
        self.redactor.register(&value);
        Ok(Some(value))
    }

    pub fn store(&self, r: &SecretRef, value: &str) -> Result<(), SecretError> {
        self.redactor.register(value);
        self.store_for(r)?.set(r, value)
    }

    /// Deletes the secret. Deleting a missing secret is not an error.
    pub fn delete(&self, r: &SecretRef) -> Result<(), SecretError> {
        match r {
            SecretRef::None | SecretRef::Prompt | SecretRef::Env { .. } => Ok(()),
            _ => match self.store_for(r)?.delete(r) {
                Ok(()) | Err(SecretError::NotFound) => Ok(()),
                Err(e) => Err(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_secrets_resolve_and_are_registered_for_redaction() {
        let redactor = Redactor::new();
        let resolver = SecretResolver::new(Arc::new(SessionStore::default()), redactor.clone());
        let r = SecretRef::Session {
            key: "conn-1:password".into(),
        };
        resolver.store(&r, "correct horse battery").unwrap();
        let v = resolver.resolve(&r).unwrap().unwrap();
        assert_eq!(v.as_str(), "correct horse battery");
        assert_eq!(
            redactor.scrub("login failed for correct horse battery"),
            "login failed for ****"
        );
        resolver.delete(&r).unwrap();
        assert!(matches!(resolver.resolve(&r), Err(SecretError::NotFound)));
    }

    #[test]
    fn prompt_and_none_refs() {
        let resolver = SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new());
        assert!(resolver.resolve(&SecretRef::None).unwrap().is_none());
        assert!(matches!(
            resolver.resolve(&SecretRef::Prompt),
            Err(SecretError::PromptRequired)
        ));
    }
}
