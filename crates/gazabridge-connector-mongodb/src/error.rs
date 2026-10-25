//! Mapping driver errors to connector errors by MongoDB error code.

use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};
use mongodb::error::{Error, ErrorKind as MongoKind, WriteFailure};

pub fn kind_for_code(code: i32) -> ErrorKind {
    match code {
        11000 | 11001 | 12582 => ErrorKind::Conflict,
        121 => ErrorKind::Constraint,
        10334 | 17419 | 17280 | 2 | 14 | 52 | 55 => ErrorKind::Conversion,
        50 | 262 => ErrorKind::Timeout,
        11601 => ErrorKind::Cancelled,
        13 | 8000 | 31 => ErrorKind::Permission,
        18 => ErrorKind::Authentication,
        26 => ErrorKind::NotFound,
        9 | 40324 | 15983 | 16410 => ErrorKind::InvalidInput,
        112 | 251 | 189 | 91 | 11600 | 11602 | 10107 | 13435 | 13436 => ErrorKind::TransientServer,
        20 | 263 => ErrorKind::Unsupported,
        14031 | 16550 => ErrorKind::ResourceExhausted,
        _ => ErrorKind::Internal,
    }
}

/// Separates the plain-language explanation from the driver's own text.
pub const DETAILS_MARKER: &str = "\n\nDetails: ";

/// Turns the driver's server-selection report (a dump of the topology) into
/// a sentence saying what to check, keeping the original as details.
pub fn explain_selection(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    let address = raw
        .split("Address: ")
        .nth(1)
        .and_then(|rest| rest.split([',', ' ']).next())
        .unwrap_or("the server");
    let hint = if lower.contains("tls handshake")
        || lower.contains("invalidcontenttype")
        || lower.contains("corrupt message")
        || (lower.contains("tls") && lower.contains("eof"))
    {
        format!(
            "Could not start an encrypted (TLS) connection to {address}: the server does not seem to use TLS. For a local server without TLS, turn TLS off or remove tls=true from the connection string."
        )
    } else if lower.contains("certificate") {
        format!(
            "The TLS certificate of {address} was not accepted. Check the CA certificate setting or the server name."
        )
    } else if lower.contains("connection refused") {
        format!(
            "Nothing is accepting connections at {address}. Check that MongoDB is running and that the host and port are right."
        )
    } else if lower.contains("lookup") || lower.contains("nodename") || lower.contains("dns") {
        format!(
            "The host name in {address} could not be found. Check the spelling, your network or VPN."
        )
    } else if lower.contains("setname") || lower.contains("replica set") {
        "The server is not part of the replica set named in the connection. Check the replicaSet option, or use a direct connection.".to_owned()
    } else {
        format!(
            "No MongoDB server answered at {address} in time. Check the address, firewall or VPN, and that the server is running."
        )
    };
    format!("{hint}{DETAILS_MARKER}{raw}")
}

pub fn map_mongo(e: Error) -> ConnectorError {
    let transient =
        e.contains_label("TransientTransactionError") || e.contains_label("RetryableWriteError");
    let (kind, message, code) = match e.kind.as_ref() {
        MongoKind::Command(c) => (kind_for_code(c.code), c.message.clone(), Some(c.code)),
        MongoKind::Write(WriteFailure::WriteError(w)) => {
            (kind_for_code(w.code), w.message.clone(), Some(w.code))
        }
        MongoKind::Write(WriteFailure::WriteConcernError(w)) => {
            (ErrorKind::TransientServer, w.message.clone(), Some(w.code))
        }
        MongoKind::InsertMany(m) => {
            let first = m.write_errors.as_ref().and_then(|w| w.first());
            match first {
                Some(w) => (kind_for_code(w.code), w.message.clone(), Some(w.code)),
                None => (ErrorKind::TransientServer, e.to_string(), None),
            }
        }
        MongoKind::Authentication { message, .. } => {
            (ErrorKind::Authentication, message.clone(), None)
        }
        MongoKind::ServerSelection { message, .. } => {
            (ErrorKind::Connection, explain_selection(message), None)
        }
        MongoKind::DnsResolve { message, .. } => (ErrorKind::Connection, message.clone(), None),
        MongoKind::Io(io) => (ErrorKind::Connection, io.to_string(), None),
        MongoKind::ConnectionPoolCleared { message, .. } => {
            (ErrorKind::Connection, message.clone(), None)
        }
        MongoKind::InvalidArgument { message, .. } => {
            (ErrorKind::InvalidInput, message.clone(), None)
        }
        MongoKind::Transaction { message, .. } => {
            (ErrorKind::TransientServer, message.clone(), None)
        }
        MongoKind::IncompatibleServer { message, .. } => {
            (ErrorKind::Unsupported, message.clone(), None)
        }
        _ => (ErrorKind::Internal, e.to_string(), None),
    };
    let kind = if transient && !kind.is_record_level() {
        ErrorKind::TransientServer
    } else {
        kind
    };
    ConnectorError::from_driver(kind, message, code.map(|c| c.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_failures_say_what_to_check() {
        let tls = "Server selection timeout: No available servers. Topology: { Type: Unknown, Servers: [ { Address: localhost:27017, Type: Unknown, Error: Kind: I/O error: tls handshake eof, labels: {} } ] }";
        let msg = explain_selection(tls);
        assert!(
            msg.starts_with("Could not start an encrypted (TLS) connection to localhost:27017"),
            "{msg}"
        );
        assert!(msg.contains(DETAILS_MARKER));
        assert!(
            explain_selection("Address: h:1, Error: Connection refused (os error 61)")
                .starts_with("Nothing is accepting connections at h:1")
        );
        assert!(
            explain_selection("something else")
                .starts_with("No MongoDB server answered at the server")
        );
    }

    #[test]
    fn codes_are_classified() {
        assert_eq!(kind_for_code(11000), ErrorKind::Conflict);
        assert_eq!(kind_for_code(121), ErrorKind::Constraint);
        assert_eq!(kind_for_code(50), ErrorKind::Timeout);
        assert_eq!(kind_for_code(18), ErrorKind::Authentication);
        assert_eq!(kind_for_code(13), ErrorKind::Permission);
        assert_eq!(kind_for_code(112), ErrorKind::TransientServer);
        assert!(
            kind_for_code(10334).is_record_level(),
            "oversized documents fail individually"
        );
    }
}
