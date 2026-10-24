//! Runtime registry of connector factories.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use gazabridge_domain::connection::EngineFamily;
use gazabridge_plugin_sdk::{
    API_VERSION, CapabilityManifest, CompatibilityProfile, ConnectorFactory,
};

use crate::catalog::ProfileCatalog;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("connector `{id}` targets API {connector}, incompatible with host API {host}")]
    IncompatibleApi {
        id: String,
        connector: gazabridge_plugin_sdk::ApiVersion,
        host: gazabridge_plugin_sdk::ApiVersion,
    },
    #[error("a connector for the {0} family is already registered")]
    AlreadyRegistered(EngineFamily),
    #[error("no connector is registered for the {0} family")]
    NotRegistered(EngineFamily),
    #[error("unknown compatibility profile `{0}`")]
    UnknownProfile(String),
}

/// Holds one factory per engine family plus the profile catalogue.
#[derive(Clone)]
pub struct ConnectorRegistry {
    factories: BTreeMap<EngineFamily, Arc<dyn ConnectorFactory>>,
    catalog: Arc<ProfileCatalog>,
}

impl fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorRegistry")
            .field("families", &self.factories.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConnectorRegistry {
    pub fn new(catalog: ProfileCatalog) -> Self {
        Self {
            factories: BTreeMap::new(),
            catalog: Arc::new(catalog),
        }
    }

    pub fn register(&mut self, factory: Arc<dyn ConnectorFactory>) -> Result<(), RegistryError> {
        let d = factory.descriptor();
        if !d.api_version.is_compatible_with_host(API_VERSION) {
            return Err(RegistryError::IncompatibleApi {
                id: d.id,
                connector: d.api_version,
                host: API_VERSION,
            });
        }
        if self.factories.contains_key(&d.family) {
            return Err(RegistryError::AlreadyRegistered(d.family));
        }
        self.factories.insert(d.family, factory);
        Ok(())
    }

    pub fn factory(
        &self,
        family: EngineFamily,
    ) -> Result<&Arc<dyn ConnectorFactory>, RegistryError> {
        self.factories
            .get(&family)
            .ok_or(RegistryError::NotRegistered(family))
    }

    pub fn catalog(&self) -> &ProfileCatalog {
        &self.catalog
    }

    pub fn profile(&self, id: &str) -> Result<&CompatibilityProfile, RegistryError> {
        self.catalog
            .get(id)
            .ok_or_else(|| RegistryError::UnknownProfile(id.to_owned()))
    }

    pub fn registered_families(&self) -> impl Iterator<Item = EngineFamily> + '_ {
        self.factories.keys().copied()
    }

    /// Manifest a connection with this profile would have before server
    /// detection. Used by the UI to disable unsupported features early.
    pub fn planned_manifest(
        &self,
        profile_id: &str,
        read_only: bool,
    ) -> Result<CapabilityManifest, RegistryError> {
        let profile = self.profile(profile_id)?;
        let base = self.factory(profile.family)?.base_manifest();
        let m = profile.apply(&base);
        Ok(if read_only { m.read_only() } else { m })
    }
}
