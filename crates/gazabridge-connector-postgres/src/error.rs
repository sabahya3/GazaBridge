//! Mapping driver and pool errors to connector errors.

use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};

/// Classifies a SQLSTATE code.
pub fn kind_for_sqlstate(code: &str, message: &str) -> ErrorKind {
    match code {
        "23505" => ErrorKind::Conflict,
        "23503" | "23502" | "23514" | "23P01" | "23000" | "23001" => ErrorKind::Constraint,
        "40001" | "40P01" | "55P03" | "55006" => ErrorKind::TransientServer,
        "57014" if message.contains("statement timeout") => ErrorKind::Timeout,
        "57014" => ErrorKind::Cancelled,
        "25006" => ErrorKind::ReadOnly,
        "28000" | "28P01" => ErrorKind::Authentication,
        "42501" => ErrorKind::Permission,
        "42P01" | "42704" | "42883" | "3D000" | "3F000" | "42703" => ErrorKind::NotFound,
        "57P01" | "57P02" | "57P03" => ErrorKind::Connection,
        c if c.starts_with("08") => ErrorKind::Connection,
        c if c.starts_with("22") => ErrorKind::Conversion,
        c if c.starts_with("53") => ErrorKind::ResourceExhausted,
        c if c.starts_with("42") => ErrorKind::InvalidInput,
        c if c.starts_with("0A") => ErrorKind::Unsupported,
        _ => ErrorKind::Internal,
    }
}

pub fn map_pg(e: tokio_postgres::Error) -> ConnectorError {
    if let Some(db) = e.as_db_error() {
        let code = db.code().code().to_owned();
        let kind = kind_for_sqlstate(&code, db.message());
        let mut message = db.message().to_owned();
        if let Some(detail) = db.detail() {
            message.push_str(" — ");
            message.push_str(detail);
        }
        return ConnectorError::from_driver(kind, message, Some(code));
    }
    let kind = if e.is_closed() {
        ErrorKind::Connection
    } else {
        // I/O, TLS, protocol and authentication-handshake failures.
        let text = e.to_string().to_ascii_lowercase();
        if text.contains("password") || text.contains("authentication") {
            ErrorKind::Authentication
        } else if text.contains("timed out") {
            ErrorKind::Timeout
        } else {
            ErrorKind::Connection
        }
    };
    ConnectorError::from_driver(kind, format!("{e}"), None)
}

pub fn map_pool(e: deadpool_postgres::PoolError) -> ConnectorError {
    match e {
        deadpool_postgres::PoolError::Backend(e) => map_pg(e),
        deadpool_postgres::PoolError::Timeout(_) => ConnectorError::new(
            ErrorKind::Timeout,
            "timed out waiting for a database connection",
        ),
        deadpool_postgres::PoolError::Closed => {
            ConnectorError::new(ErrorKind::Connection, "connection pool is closed")
        }
        other => ConnectorError::from_driver(ErrorKind::Connection, other.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlstates_are_classified() {
        assert_eq!(kind_for_sqlstate("23505", ""), ErrorKind::Conflict);
        assert_eq!(kind_for_sqlstate("23502", ""), ErrorKind::Constraint);
        assert_eq!(kind_for_sqlstate("22P02", ""), ErrorKind::Conversion);
        assert_eq!(kind_for_sqlstate("40001", ""), ErrorKind::TransientServer);
        assert_eq!(
            kind_for_sqlstate("57014", "canceling statement due to statement timeout"),
            ErrorKind::Timeout
        );
        assert_eq!(
            kind_for_sqlstate("57014", "canceling statement due to user request"),
            ErrorKind::Cancelled
        );
        assert_eq!(kind_for_sqlstate("25006", ""), ErrorKind::ReadOnly);
        assert_eq!(kind_for_sqlstate("28P01", ""), ErrorKind::Authentication);
        assert_eq!(kind_for_sqlstate("08006", ""), ErrorKind::Connection);
        assert!(kind_for_sqlstate("23505", "").is_record_level());
        assert!(kind_for_sqlstate("22003", "").is_record_level());
    }
}
