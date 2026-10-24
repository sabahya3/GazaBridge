use std::collections::HashMap;
use std::sync::Mutex;

use gazabridge_domain::connection::SecretRef;
use zeroize::Zeroizing;

use crate::{SecretError, SecretString};

/// A backing store for secrets. Implementations are synchronous; callers on
/// async runtimes should use `spawn_blocking` for the keychain store.
pub trait SecretStore: Send + Sync {
    fn get(&self, r: &SecretRef) -> Result<SecretString, SecretError>;
    fn set(&self, r: &SecretRef, value: &str) -> Result<(), SecretError>;
    fn delete(&self, r: &SecretRef) -> Result<(), SecretError>;
}

/// The OS-native credential store.
#[derive(Debug, Default, Clone, Copy)]
pub struct KeychainStore;

impl KeychainStore {
    fn entry(r: &SecretRef) -> Result<keyring::Entry, SecretError> {
        match r {
            SecretRef::Keychain { service, account } => {
                keyring::Entry::new(service, account).map_err(map_keyring)
            }
            _ => Err(SecretError::Backend("not a keychain reference".into())),
        }
    }
}

fn map_keyring(e: keyring::Error) -> SecretError {
    match e {
        keyring::Error::NoEntry => SecretError::NotFound,
        keyring::Error::NoStorageAccess(inner) => SecretError::Unavailable(inner.to_string()),
        keyring::Error::PlatformFailure(inner) => SecretError::Unavailable(inner.to_string()),
        other => SecretError::Backend(other.to_string()),
    }
}

impl SecretStore for KeychainStore {
    fn get(&self, r: &SecretRef) -> Result<SecretString, SecretError> {
        Self::entry(r)?
            .get_password()
            .map(Zeroizing::new)
            .map_err(map_keyring)
    }

    fn set(&self, r: &SecretRef, value: &str) -> Result<(), SecretError> {
        Self::entry(r)?.set_password(value).map_err(map_keyring)
    }

    fn delete(&self, r: &SecretRef) -> Result<(), SecretError> {
        Self::entry(r)?.delete_credential().map_err(map_keyring)
    }
}

/// In-memory store for "don't remember" credentials. Values are wiped when
/// removed or when the store is dropped.
#[derive(Default)]
pub struct SessionStore {
    values: Mutex<HashMap<String, SecretString>>,
}

impl std::fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.values.lock().map(|v| v.len()).unwrap_or(0);
        f.debug_struct("SessionStore").field("entries", &n).finish()
    }
}

impl SessionStore {
    fn key(r: &SecretRef) -> Result<&str, SecretError> {
        match r {
            SecretRef::Session { key } => Ok(key),
            // Tests and the session-only flow may also park keychain refs here.
            SecretRef::Keychain { account, .. } => Ok(account),
            _ => Err(SecretError::Backend("not a session reference".into())),
        }
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, SecretString>>, SecretError> {
        self.values
            .lock()
            .map_err(|_| SecretError::Backend("session store lock poisoned".into()))
    }

    /// Wipes every session secret (e.g. on "lock" or application exit).
    pub fn clear(&self) {
        if let Ok(mut v) = self.values.lock() {
            v.clear();
        }
    }
}

impl SecretStore for SessionStore {
    fn get(&self, r: &SecretRef) -> Result<SecretString, SecretError> {
        let key = Self::key(r)?;
        self.lock()?.get(key).cloned().ok_or(SecretError::NotFound)
    }

    fn set(&self, r: &SecretRef, value: &str) -> Result<(), SecretError> {
        let key = Self::key(r)?.to_owned();
        self.lock()?.insert(key, Zeroizing::new(value.to_owned()));
        Ok(())
    }

    fn delete(&self, r: &SecretRef) -> Result<(), SecretError> {
        let key = Self::key(r)?;
        self.lock()?
            .remove(key)
            .map(drop)
            .ok_or(SecretError::NotFound)
    }
}

/// Reads secrets from environment variables (CI/CD and headless use).
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvStore;

impl SecretStore for EnvStore {
    fn get(&self, r: &SecretRef) -> Result<SecretString, SecretError> {
        match r {
            SecretRef::Env { var } => std::env::var(var)
                .map(Zeroizing::new)
                .map_err(|_| SecretError::EnvMissing(var.clone())),
            _ => Err(SecretError::Backend("not an environment reference".into())),
        }
    }

    fn set(&self, _r: &SecretRef, _value: &str) -> Result<(), SecretError> {
        Err(SecretError::ReadOnlyStore("environment"))
    }

    fn delete(&self, _r: &SecretRef) -> Result<(), SecretError> {
        Err(SecretError::ReadOnlyStore("environment"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_store_reads_variables() {
        // PATH is always present in test environments.
        let v = EnvStore
            .get(&SecretRef::Env { var: "PATH".into() })
            .unwrap();
        assert!(!v.is_empty());
        assert!(matches!(
            EnvStore.get(&SecretRef::Env {
                var: "GAZABRIDGE_DEFINITELY_UNSET_VAR".into()
            }),
            Err(SecretError::EnvMissing(_))
        ));
        assert!(
            EnvStore
                .set(&SecretRef::Env { var: "X".into() }, "y")
                .is_err()
        );
    }

    #[test]
    fn session_store_debug_hides_values() {
        let s = SessionStore::default();
        s.set(&SecretRef::Session { key: "k".into() }, "hunter2")
            .unwrap();
        assert!(!format!("{s:?}").contains("hunter2"));
    }

    /// Exercises the real OS keychain. Opt in with GAZABRIDGE_TEST_KEYCHAIN=1
    /// because it may prompt for access on desktop systems.
    #[test]
    #[ignore = "touches the OS keychain; run with GAZABRIDGE_TEST_KEYCHAIN=1 --ignored"]
    fn keychain_round_trip() {
        if std::env::var("GAZABRIDGE_TEST_KEYCHAIN").as_deref() != Ok("1") {
            return;
        }
        let r = SecretRef::Keychain {
            service: "dev.gazabridge.test".into(),
            account: format!("roundtrip-{}", std::process::id()),
        };
        KeychainStore
            .set(&r, "test-value-not-a-real-secret")
            .unwrap();
        assert_eq!(
            KeychainStore.get(&r).unwrap().as_str(),
            "test-value-not-a-real-secret"
        );
        KeychainStore.delete(&r).unwrap();
        assert!(matches!(KeychainStore.get(&r), Err(SecretError::NotFound)));
    }
}
