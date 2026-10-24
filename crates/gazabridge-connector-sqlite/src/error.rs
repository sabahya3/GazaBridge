//! Mapping sqlx/SQLite errors to connector errors.

use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};

/// Classifies a SQLite extended result code.
fn kind_for_code(code: i64) -> ErrorKind {
    match code & 0xff {
        // SQLITE_CONSTRAINT: refine with the extended code.
        19 => match code {
            1555 | 2067 => ErrorKind::Conflict, // PRIMARYKEY, UNIQUE
            _ => ErrorKind::Constraint,         // CHECK, FOREIGNKEY, NOTNULL, …
        },
        5 | 6 => ErrorKind::TransientServer, // BUSY, LOCKED
        8 => ErrorKind::ReadOnly,            // READONLY
        9 => ErrorKind::Cancelled,           // INTERRUPT
        13 => ErrorKind::ResourceExhausted,  // FULL
        7 => ErrorKind::ResourceExhausted,   // NOMEM
        23 | 3 => ErrorKind::Permission,     // AUTH, PERM
        14 => ErrorKind::Connection,         // CANTOPEN
        10 | 11 | 26 => ErrorKind::Io,       // IOERR, CORRUPT, NOTADB
        20 => ErrorKind::Conversion,         // MISMATCH
        1 => ErrorKind::InvalidInput,        // generic SQL error (syntax, missing table)
        _ => ErrorKind::Internal,
    }
}

pub fn map_sqlx(e: sqlx::Error) -> ConnectorError {
    match &e {
        sqlx::Error::Database(db) => {
            let code = db.code().map(|c| c.into_owned());
            let kind = code
                .as_deref()
                .and_then(|c| c.parse::<i64>().ok())
                .map_or(ErrorKind::Internal, kind_for_code);
            ConnectorError::from_driver(kind, db.message(), code)
        }
        sqlx::Error::RowNotFound => ConnectorError::new(ErrorKind::NotFound, "no rows returned"),
        sqlx::Error::PoolTimedOut => {
            ConnectorError::new(ErrorKind::Timeout, "timed out waiting for a connection")
        }
        sqlx::Error::PoolClosed => {
            ConnectorError::new(ErrorKind::Connection, "connection pool is closed")
        }
        sqlx::Error::Io(io) => ConnectorError::from_driver(ErrorKind::Io, io.to_string(), None),
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => {
            ConnectorError::from_driver(ErrorKind::Conversion, e.to_string(), None)
        }
        _ => ConnectorError::from_driver(ErrorKind::Internal, e.to_string(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_codes_are_classified() {
        assert_eq!(kind_for_code(2067), ErrorKind::Conflict);
        assert_eq!(kind_for_code(1555), ErrorKind::Conflict);
        assert_eq!(kind_for_code(787), ErrorKind::Constraint);
        assert_eq!(kind_for_code(1299), ErrorKind::Constraint);
        assert_eq!(kind_for_code(5), ErrorKind::TransientServer);
        assert_eq!(kind_for_code(8), ErrorKind::ReadOnly);
        assert_eq!(kind_for_code(13), ErrorKind::ResourceExhausted);
    }
}
