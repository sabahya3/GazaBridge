//! Application services: the use cases shared by the desktop app and the
//! CLI. Drivers (Tauri commands, CLI subcommands) stay thin and call these.

pub mod connections;
pub mod profiled;
pub mod schema;

pub use connections::{ConnectionService, PasswordInput, TestOutcome};
pub use profiled::ProfiledConnection;
pub use schema::{FieldChange, SchemaService};

use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Storage(#[from] gazabridge_storage::StorageError),
    #[error(transparent)]
    Secret(#[from] gazabridge_secrets::SecretError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error(transparent)]
    Registry(#[from] gazabridge_connectors::RegistryError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0} not found")]
    NotFound(&'static str),
}

impl AppError {
    /// Stable machine-readable code for IPC and CLI exit mapping.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Storage(_) => "storage",
            Self::Secret(gazabridge_secrets::SecretError::PromptRequired) => "password_required",
            Self::Secret(_) => "secret_store",
            Self::Connector(e) => match e.kind() {
                ErrorKind::Authentication => "authentication",
                ErrorKind::Connection | ErrorKind::Timeout => "connection",
                ErrorKind::ReadOnly => "read_only",
                ErrorKind::Unsupported => "unsupported",
                ErrorKind::InvalidInput => "invalid_input",
                _ => "connector",
            },
            Self::Registry(_) => "connector_unavailable",
            Self::Invalid(_) => "invalid_input",
            Self::NotFound(_) => "not_found",
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;
