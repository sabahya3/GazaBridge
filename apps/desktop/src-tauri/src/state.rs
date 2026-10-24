use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
use gazabridge_observability::{LogConfig, LoggingGuard};
use gazabridge_secrets::{Redactor, SecretResolver};
use gazabridge_storage::MetadataStore;

pub const METADATA_FILE: &str = "metadata.db";

/// Process-wide services shared by all commands.
#[derive(Debug)]
pub struct AppState {
    pub store: MetadataStore,
    pub registry: ConnectorRegistry,
    pub secrets: SecretResolver,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    _logging: Mutex<Option<LoggingGuard>>,
}

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error(transparent)]
    Storage(#[from] gazabridge_storage::StorageError),
    #[error(transparent)]
    Catalog(#[from] gazabridge_connectors::CatalogError),
    #[error(transparent)]
    Logging(#[from] gazabridge_observability::LoggingError),
    #[error(transparent)]
    Registry(#[from] gazabridge_connectors::RegistryError),
}

impl AppState {
    pub async fn initialize(data_dir: PathBuf, log_dir: PathBuf) -> Result<Self, InitError> {
        let redactor = Redactor::new();
        let logging =
            gazabridge_observability::init(&LogConfig::new(log_dir.clone()), redactor.clone()).ok();
        let store = MetadataStore::open(&data_dir.join(METADATA_FILE)).await?;
        store.check_integrity().await?;
        let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin()?);
        registry.register(Arc::new(
            gazabridge_connector_sqlite::SqliteConnectorFactory,
        ))?;
        registry.register(Arc::new(
            gazabridge_connector_postgres::PostgresConnectorFactory,
        ))?;
        tracing::info!(version = env!("CARGO_PKG_VERSION"), "GazaBridge started");
        Ok(Self {
            store,
            registry,
            secrets: SecretResolver::os_default(redactor),
            data_dir,
            log_dir,
            _logging: Mutex::new(logging),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initializes_metadata_and_catalogue_in_fresh_directories() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        assert!(dir.path().join("data").join(METADATA_FILE).exists());
        assert!(state.store.schema_version().await.unwrap() > 0);
        assert!(state.registry.catalog().get("sqlite.sqlite").is_some());
        assert!(
            state
                .registry
                .factory(gazabridge_domain::connection::EngineFamily::Sqlite)
                .is_ok()
        );
        assert!(
            state
                .registry
                .factory(gazabridge_domain::connection::EngineFamily::Postgres)
                .is_ok()
        );
    }
}
