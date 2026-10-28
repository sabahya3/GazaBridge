//! Application services: the use cases shared by the desktop app and the
//! CLI. Drivers (Tauri commands, CLI subcommands) stay thin and call these.

pub mod backup;
pub mod connections;
pub mod convert;
pub mod explorer;
pub mod migration;
pub mod profiled;
pub mod query;
pub mod schema;
pub mod tools;
pub mod transfer;

pub use connections::{ConnectionService, PasswordInput, TestOutcome};
pub use explorer::{
    BrowseRequest, BrowseResult, EditBlocker, EditPlan, ExplorerService, FieldEdit,
};
pub use migration::{
    CopyItem, ExistingData, MigrationPlan, MigrationRequest, MigrationService, OnRecordError,
    PlanItem, RunView, StreamPhase, StreamView, ValidationReport,
};
pub use profiled::ProfiledConnection;
pub use query::{QueryContext, QueryInput, QueryRisk, QueryService};
pub use schema::{FieldChange, SchemaService};
pub use transfer::{
    ExportRequest, ImportRequest, Phase, Progress, TransferService, TransferSummary,
};

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
    #[error(transparent)]
    File(#[from] gazabridge_files::FileError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0} not found")]
    NotFound(&'static str),
    /// The operation needs explicit confirmation; carries the risk names.
    #[error("confirmation required: {0}")]
    ConfirmationRequired(String),
    /// The connection's password must be entered: it is asked each time,
    /// or the OS keychain did not release it (e.g. after an app update).
    #[error("{reason} Enter the password of “{name}” to continue.")]
    PasswordNeeded {
        connection: gazabridge_domain::ids::ConnectionId,
        name: String,
        reason: String,
    },
}

impl AppError {
    /// Stable machine-readable code for IPC and CLI exit mapping.
    /// The connection a `password_required` error is about.
    pub fn connection_id(&self) -> Option<String> {
        match self {
            Self::PasswordNeeded { connection, .. } => Some(connection.to_string()),
            _ => None,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Storage(gazabridge_storage::StorageError::NotFound(_)) => "not_found",
            Self::Storage(gazabridge_storage::StorageError::InvalidState(_)) => "invalid_input",
            Self::Storage(_) => "storage",
            Self::Secret(gazabridge_secrets::SecretError::PromptRequired) => "password_required",
            Self::PasswordNeeded { .. } => "password_required",
            Self::Secret(_) => "secret_store",
            Self::Connector(e) => match e.kind() {
                ErrorKind::Authentication => "authentication",
                ErrorKind::Connection | ErrorKind::Timeout => "connection",
                ErrorKind::ReadOnly => "read_only",
                ErrorKind::Conflict => "conflict",
                ErrorKind::Cancelled => "cancelled",
                ErrorKind::Unsupported => "unsupported",
                ErrorKind::InvalidInput => "invalid_input",
                _ => "connector",
            },
            Self::Registry(_) => "connector_unavailable",
            Self::File(e) => match e {
                gazabridge_files::FileError::Exists(_) => "file_exists",
                gazabridge_files::FileError::Malformed { .. } => "malformed_input",
                gazabridge_files::FileError::Io { .. } => "file_io",
                _ => "invalid_input",
            },
            Self::Invalid(_) => "invalid_input",
            Self::NotFound(_) => "not_found",
            Self::ConfirmationRequired(_) => "confirmation_required",
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;
