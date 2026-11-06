//! Mapping driver errors to connector errors by MySQL error number.

use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};

pub fn kind_for_code(code: u16) -> ErrorKind {
    match code {
        1062 | 1586 | 1061 | 1050 | 1007 => ErrorKind::Conflict,
        1048 | 1364 | 1451 | 1452 | 1216 | 1217 | 3819 => ErrorKind::Constraint,
        1264 | 1265 | 1292 | 1366 | 1367 | 1406 | 1690 | 3140 | 1411 | 1416 | 4025 => {
            ErrorKind::Conversion
        }
        1205 | 1213 | 1614 => ErrorKind::TransientServer,
        1290 | 1792 | 1836 => ErrorKind::ReadOnly,
        1045 | 1251 | 2049 => ErrorKind::Authentication,
        1044 | 1142 | 1143 | 1227 | 1370 => ErrorKind::Permission,
        1146 | 1049 | 1054 | 1305 | 1051 => ErrorKind::NotFound,
        1064 | 1149 | 1295 | 1065 => ErrorKind::InvalidInput,
        1317 => ErrorKind::Cancelled,
        3024 | 1969 => ErrorKind::Timeout,
        1114 | 1021 | 1041 | 1037 | 1038 => ErrorKind::ResourceExhausted,
        1040 | 1053 | 1152 | 1158..=1161 | 2002 | 2003 | 2006 | 2013 => ErrorKind::Connection,
        1235 | 1289 | 1071 | 1170 => ErrorKind::Unsupported,
        _ => ErrorKind::Internal,
    }
}

pub fn map_mysql(e: mysql_async::Error) -> ConnectorError {
    match e {
        mysql_async::Error::Server(s) => {
            let message = match s.code {
                1044 | 1142 => format!(
                    "{} (choose a target database this MySQL user can write to, or have an administrator grant access with GRANT ALL ON `database`.* TO the user)",
                    s.message
                ),
                _ => s.message,
            };
            ConnectorError::from_driver(kind_for_code(s.code), message, Some(s.code.to_string()))
        }
        mysql_async::Error::Io(io) => {
            ConnectorError::from_driver(ErrorKind::Connection, io.to_string(), None)
        }
        mysql_async::Error::Url(u) => {
            ConnectorError::from_driver(ErrorKind::InvalidInput, u.to_string(), None)
        }
        other => {
            let text = other.to_string();
            let lower = text.to_ascii_lowercase();
            let kind = if lower.contains("timed out") {
                ErrorKind::Timeout
            } else if lower.contains("closed") || lower.contains("connection") {
                ErrorKind::Connection
            } else {
                ErrorKind::Internal
            };
            ConnectorError::from_driver(kind, text, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_classified() {
        assert_eq!(kind_for_code(1062), ErrorKind::Conflict);
        assert_eq!(kind_for_code(1048), ErrorKind::Constraint);
        assert_eq!(kind_for_code(3819), ErrorKind::Constraint);
        assert_eq!(kind_for_code(1366), ErrorKind::Conversion);
        assert_eq!(kind_for_code(1406), ErrorKind::Conversion);
        assert_eq!(kind_for_code(1213), ErrorKind::TransientServer);
        assert_eq!(kind_for_code(1792), ErrorKind::ReadOnly);
        assert_eq!(kind_for_code(1045), ErrorKind::Authentication);
        assert_eq!(kind_for_code(1317), ErrorKind::Cancelled);
        assert_eq!(kind_for_code(3024), ErrorKind::Timeout);
        assert!(kind_for_code(1062).is_record_level() && kind_for_code(1292).is_record_level());
    }
}
