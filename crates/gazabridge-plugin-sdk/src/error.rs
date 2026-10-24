//! Connector errors with retry classification.

use std::fmt;

use gazabridge_domain::ConversionError;
use serde::{Deserialize, Serialize};

use crate::capability::Capability;

/// Broad error category used for retry decisions and UI messaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Network unreachable, connection reset, DNS failure.
    Connection,
    Authentication,
    Permission,
    NotFound,
    /// Duplicate key or write conflict.
    Conflict,
    /// Check, foreign-key or not-null constraint violation.
    Constraint,
    Timeout,
    Cancelled,
    /// The capability is not supported by this connector or server.
    Unsupported,
    /// The request is malformed (bad identifier, bad filter, bad query).
    InvalidInput,
    /// A value could not be converted to or from the canonical model.
    Conversion,
    /// The connection is read-only and the request would mutate data.
    ReadOnly,
    /// Serialization failures and deadlocks the server asks us to retry.
    TransientServer,
    /// Disk full, quota exceeded or out-of-memory on the server.
    ResourceExhausted,
    Protocol,
    Io,
    Internal,
}

impl ErrorKind {
    /// Whether the operation may succeed if retried unchanged.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connection | Self::Timeout | Self::TransientServer
        )
    }

    /// Whether the failure is tied to an individual record (so the record can
    /// be quarantined while the rest of the batch proceeds).
    pub fn is_record_level(&self) -> bool {
        matches!(self, Self::Conflict | Self::Constraint | Self::Conversion)
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Connection => "connection",
            Self::Authentication => "authentication",
            Self::Permission => "permission",
            Self::NotFound => "not found",
            Self::Conflict => "conflict",
            Self::Constraint => "constraint",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Unsupported => "unsupported",
            Self::InvalidInput => "invalid input",
            Self::Conversion => "conversion",
            Self::ReadOnly => "read-only",
            Self::TransientServer => "transient server error",
            Self::ResourceExhausted => "resource exhausted",
            Self::Protocol => "protocol",
            Self::Io => "I/O",
            Self::Internal => "internal",
        };
        f.write_str(s)
    }
}

/// An error raised by a connector.
///
/// `message` must already be free of secrets; connectors pass driver
/// messages through [`ConnectorError::from_driver`], which redacts them.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConnectorError {
    #[error("{capability} is not supported by this connection")]
    Unsupported { capability: Capability },
    #[error("the connection is read-only")]
    ReadOnly,
    #[error("operation cancelled")]
    Cancelled,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("{kind} error: {message}")]
    Other {
        kind: ErrorKind,
        message: String,
        /// Driver/server error code (SQLSTATE, MySQL errno, Mongo code).
        code: Option<String>,
    },
}

impl ConnectorError {
    pub fn unsupported(capability: Capability) -> Self {
        Self::Unsupported { capability }
    }

    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::Other {
            kind,
            message: message.into(),
            code: None,
        }
    }

    pub fn with_code(kind: ErrorKind, message: impl Into<String>, code: impl Into<String>) -> Self {
        Self::Other {
            kind,
            message: message.into(),
            code: Some(code.into()),
        }
    }

    /// Wraps a driver error message, redacting any connection strings or
    /// `password=` fragments it may contain.
    pub fn from_driver(kind: ErrorKind, message: impl AsRef<str>, code: Option<String>) -> Self {
        Self::Other {
            kind,
            message: gazabridge_domain::connection_string::redact(message.as_ref()),
            code,
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Unsupported { .. } => ErrorKind::Unsupported,
            Self::ReadOnly => ErrorKind::ReadOnly,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::Conversion(_) => ErrorKind::Conversion,
            Self::Other { kind, .. } => *kind,
        }
    }

    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Other { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }
}

pub type ConnectorResult<T> = Result<T, ConnectorError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_messages_are_redacted() {
        let e = ConnectorError::from_driver(
            ErrorKind::Connection,
            "could not connect to postgres://admin:hunter2@db:5432/x",
            None,
        );
        let s = e.to_string();
        assert!(!s.contains("hunter2"), "{s}");
        assert!(e.is_retryable());
    }

    #[test]
    fn record_level_classification() {
        assert!(ErrorKind::Conflict.is_record_level());
        assert!(!ErrorKind::Connection.is_record_level());
        assert!(!ErrorKind::Authentication.is_retryable());
    }
}
