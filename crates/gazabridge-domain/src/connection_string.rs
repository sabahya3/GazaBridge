//! Connection-string parsing and redaction.
//!
//! Parsing splits a pasted connection string into secret-free
//! [`ConnectionSettings`] plus the extracted password (zeroized on drop), so
//! the password can go straight to the secret store and never into metadata.

use std::path::PathBuf;

use percent_encoding::percent_decode_str;
use zeroize::Zeroizing;

use crate::connection::{
    ConnectionSettings, HostPort, MongoSettings, MySqlSettings, MySqlTlsMode, PgSslMode,
    PostgresSettings, ReadPreference, SecretRef, SqliteSettings,
};

const MASK: &str = "****";

/// Query/DSN keys whose values are always secret.
const SECRET_KEYS: &[&str] = &[
    "password",
    "pwd",
    "passwd",
    "pass",
    "sslpassword",
    "sslkey_password",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "apikey",
    "api_key",
    "aws_secret_access_key",
    "aws_session_token",
    "authmechanismproperties",
];

/// Masks credentials in any text that may contain connection strings:
/// URI user-info passwords and `key=value` secrets in DSNs or query strings.
pub fn redact(input: &str) -> String {
    let uri_masked = redact_uri_userinfo(input);
    redact_key_values(&uri_masked)
}

fn redact_uri_userinfo(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find("://") {
        let (before, after_scheme) = rest.split_at(pos + 3);
        out.push_str(before);
        // The token ends at whitespace or a quote; credentials sometimes
        // contain unescaped '/', so look for the last '@' in the token.
        let token_end = after_scheme
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '`')
            .unwrap_or(after_scheme.len());
        let token = &after_scheme[..token_end];
        match token.rfind('@') {
            Some(at) => {
                let userinfo = &token[..at];
                match userinfo.find(':') {
                    Some(colon) => {
                        out.push_str(&userinfo[..colon]);
                        out.push(':');
                        out.push_str(MASK);
                    }
                    None => out.push_str(userinfo),
                }
                out.push_str(&token[at..]);
            }
            None => out.push_str(token),
        }
        rest = &after_scheme[token_end..];
    }
    out.push_str(rest);
    out
}

fn redact_key_values(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        let mut matched = None;
        let at_boundary = i == 0
            || matches!(
                bytes[i - 1],
                b' ' | b'\t' | b'\n' | b'&' | b'?' | b';' | b',' | b'"' | b'\''
            );
        if at_boundary {
            for key in SECRET_KEYS {
                let candidate = &lower[i..];
                if candidate.starts_with(key) {
                    let after = i + key.len();
                    let mut j = after;
                    // JSON-style `"password": "..."` has a closing quote first.
                    if j < input.len() && (bytes[j] == b'"' || bytes[j] == b'\'') {
                        j += 1;
                    }
                    while j < input.len() && bytes[j] == b' ' {
                        j += 1;
                    }
                    if j < input.len() && (bytes[j] == b'=' || bytes[j] == b':') {
                        matched = Some(j + 1);
                        break;
                    }
                }
            }
        }
        match matched {
            Some(value_start) => {
                out.push_str(&input[i..value_start]);
                let mut j = value_start;
                while j < input.len() && bytes[j] == b' ' {
                    j += 1;
                }
                out.push_str(&input[value_start..j]);
                let end = if j < input.len() && (bytes[j] == b'\'' || bytes[j] == b'"') {
                    let quote = bytes[j];
                    let mut k = j + 1;
                    while k < input.len() && bytes[k] != quote {
                        if bytes[k] == b'\\' {
                            k += 1;
                        }
                        k += 1;
                    }
                    (k + 1).min(input.len())
                } else {
                    input[j..]
                        .find([' ', '\t', '\n', '&', ';', ',', '"', '\''])
                        .map_or(input.len(), |p| j + p)
                };
                if end > j {
                    out.push_str(MASK);
                }
                i = end;
            }
            None => {
                let ch_len = input[i..].chars().next().map_or(1, char::len_utf8);
                out.push_str(&input[i..i + ch_len]);
                i += ch_len;
            }
        }
    }
    out
}

/// A parsed connection string: secret-free settings plus extracted secrets.
#[derive(Debug)]
pub struct ParsedConnection {
    pub settings: ConnectionSettings,
    /// Suggested compatibility profile id.
    pub compatibility_profile: String,
    /// Password found in the string, if any. Store it with the secret store.
    pub password: Option<Zeroizing<String>>,
    /// Parameters that were recognised but not applied, or ignored.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("unrecognised connection string scheme")]
    UnknownScheme,
    #[error("malformed connection string: {0}")]
    Malformed(String),
    #[error("invalid value for `{0}`")]
    InvalidValue(String),
}

/// Parses PostgreSQL, MySQL, MongoDB and SQLite connection strings.
///
/// Errors never echo the input, since it may contain credentials.
pub fn parse(input: &str) -> Result<ParsedConnection, ParseError> {
    let s = input.trim();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        parse_postgres_uri(s)
    } else if lower.starts_with("mysql://") || lower.starts_with("mariadb://") {
        parse_mysql_uri(s)
    } else if lower.starts_with("mongodb://") || lower.starts_with("mongodb+srv://") {
        parse_mongo_uri(s)
    } else if lower.starts_with("sqlite:") {
        parse_sqlite(s)
    } else if s.contains('=') && (lower.contains("host=") || lower.contains("dbname=")) {
        parse_postgres_dsn(s)
    } else {
        Err(ParseError::UnknownScheme)
    }
}

fn decode(s: &str) -> Result<String, ParseError> {
    percent_decode_str(s)
        .decode_utf8()
        .map(|c| c.into_owned())
        .map_err(|_| ParseError::Malformed("invalid percent-encoding".into()))
}

fn decode_secret(s: &str) -> Result<Zeroizing<String>, ParseError> {
    decode(s).map(Zeroizing::new)
}

struct UriParts<'a> {
    username: Option<&'a str>,
    password: Option<&'a str>,
    hosts: &'a str,
    path: &'a str,
    query: Vec<(String, String)>,
}

/// Splits `scheme://[user[:pass]@]hosts[/path][?query]`. Works for
/// multi-host MongoDB URIs, which `url::Url` cannot parse.
fn split_uri(s: &str) -> Result<UriParts<'_>, ParseError> {
    let after = s
        .split_once("://")
        .map(|(_, a)| a)
        .ok_or_else(|| ParseError::Malformed("missing ://".into()))?;
    let (before_query, query_str) = after.split_once('?').unwrap_or((after, ""));
    let (userinfo, host_and_path) = match before_query.rfind('@') {
        Some(at) => (Some(&before_query[..at]), &before_query[at + 1..]),
        None => (None, before_query),
    };
    let (hosts, path) = match host_and_path.find('/') {
        Some(p) => (&host_and_path[..p], &host_and_path[p + 1..]),
        None => (host_and_path, ""),
    };
    let (username, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(u), Some(p)),
            None => (Some(ui), None),
        },
        None => (None, None),
    };
    let mut query = Vec::new();
    for pair in query_str.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        query.push((decode(k)?, decode(v)?));
    }
    Ok(UriParts {
        username,
        password,
        hosts,
        path,
        query,
    })
}

/// True when a MongoDB URI carries a password in its user-info.
pub fn mongo_uri_has_password(uri: &str) -> bool {
    split_uri(uri).is_ok_and(|p| p.password.is_some_and(|pw| !pw.is_empty()))
}

/// Returns the URI with user-info removed entirely.
fn strip_userinfo(s: &str) -> String {
    match s.split_once("://") {
        Some((scheme, after)) => {
            let (before_query, query) = match after.split_once('?') {
                Some((b, q)) => (b, Some(q)),
                None => (after, None),
            };
            let rest = before_query
                .rfind('@')
                .map_or(before_query, |at| &before_query[at + 1..]);
            match query {
                Some(q) => format!("{scheme}://{rest}?{q}"),
                None => format!("{scheme}://{rest}"),
            }
        }
        None => s.to_owned(),
    }
}

fn parse_host_port(hosts: &str, default_port: u16) -> Result<Vec<HostPort>, ParseError> {
    let mut out = Vec::new();
    for h in hosts.split(',').filter(|h| !h.is_empty()) {
        let (host, port) = if let Some(rest) = h.strip_prefix('[') {
            // IPv6 literal
            let (addr, tail) = rest
                .split_once(']')
                .ok_or_else(|| ParseError::Malformed("unterminated IPv6 address".into()))?;
            let port = match tail.strip_prefix(':') {
                Some(p) => p
                    .parse()
                    .map_err(|_| ParseError::InvalidValue("port".into()))?,
                None => default_port,
            };
            (addr.to_owned(), port)
        } else {
            match h.rsplit_once(':') {
                Some((host, p)) => (
                    decode(host)?,
                    p.parse()
                        .map_err(|_| ParseError::InvalidValue("port".into()))?,
                ),
                None => (decode(h)?, default_port),
            }
        };
        if port == 0 {
            return Err(ParseError::InvalidValue("port".into()));
        }
        out.push(HostPort { host, port });
    }
    Ok(out)
}

fn parse_bool_param(key: &str, v: &str) -> Result<bool, ParseError> {
    match v.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ParseError::InvalidValue(key.into())),
    }
}

fn pg_ssl_mode(v: &str) -> Result<PgSslMode, ParseError> {
    Ok(match v {
        "disable" => PgSslMode::Disable,
        // `allow` is weaker than `prefer`; treat it as `prefer`.
        "allow" | "prefer" => PgSslMode::Prefer,
        "require" => PgSslMode::Require,
        "verify-ca" => PgSslMode::VerifyCa,
        "verify-full" => PgSslMode::VerifyFull,
        _ => return Err(ParseError::InvalidValue("sslmode".into())),
    })
}

fn guess_pg_profile(host: &str) -> &'static str {
    let h = host.to_ascii_lowercase();
    if h.ends_with(".neon.tech") {
        "postgres.neon"
    } else if h.ends_with(".supabase.co")
        || h.ends_with(".supabase.com")
        || h.contains("pooler.supabase")
    {
        "postgres.supabase"
    } else if h.contains("cockroachlabs.cloud") {
        "postgres.cockroachdb"
    } else {
        "postgres.postgresql"
    }
}

fn parse_postgres_uri(s: &str) -> Result<ParsedConnection, ParseError> {
    let parts = split_uri(s)?;
    let hosts = parse_host_port(parts.hosts, 5432)?;
    let mut warnings = Vec::new();
    if hosts.len() > 1 {
        warnings.push("multiple hosts given; only the first is used".to_owned());
    }
    let first = hosts
        .into_iter()
        .next()
        .ok_or_else(|| ParseError::Malformed("missing host".into()))?;
    let mut settings = PostgresSettings {
        host: first.host,
        port: first.port,
        database: decode(parts.path)?,
        username: parts.username.map(decode).transpose()?.unwrap_or_default(),
        password: SecretRef::None,
        ssl_mode: PgSslMode::default(),
        search_path: None,
        application_name: "GazaBridge".into(),
        connect_timeout_secs: 10,
        statement_timeout_ms: 0,
        pool_size: 4,
    };
    let mut password = parts.password.map(decode_secret).transpose()?;
    for (k, v) in parts.query {
        match k.as_str() {
            "sslmode" => settings.ssl_mode = pg_ssl_mode(&v)?,
            "application_name" => settings.application_name = v,
            "connect_timeout" => {
                settings.connect_timeout_secs =
                    v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?
            }
            "user" => settings.username = v,
            "dbname" => settings.database = v,
            "password" => password = Some(Zeroizing::new(v)),
            "options" => {
                if let Some(sp) = v.split_whitespace().find_map(|o| {
                    o.strip_prefix("-csearch_path=")
                        .or_else(|| o.strip_prefix("-c search_path="))
                }) {
                    settings.search_path = Some(sp.to_owned());
                } else {
                    warnings.push("`options` parameter ignored".to_owned());
                }
            }
            other => warnings.push(format!("parameter `{other}` ignored")),
        }
    }
    if settings.database.is_empty() {
        settings.database = "postgres".into();
    }
    let profile = guess_pg_profile(&settings.host).to_owned();
    Ok(ParsedConnection {
        settings: ConnectionSettings::Postgres(settings),
        compatibility_profile: profile,
        password,
        warnings,
    })
}

/// Parses a libpq `key=value` DSN, honouring single-quoted values.
fn parse_postgres_dsn(s: &str) -> Result<ParsedConnection, ParseError> {
    let mut pairs = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let key_start = i;
        while i < chars.len() && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let key: String = chars[key_start..i].iter().collect();
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() || chars[i] != '=' {
            return Err(ParseError::Malformed("expected key=value".into()));
        }
        i += 1;
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let mut value = Zeroizing::new(String::new());
        if i < chars.len() && chars[i] == '\'' {
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err(ParseError::Malformed("unterminated quote".into())),
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some('\\') => {
                        if let Some(c) = chars.get(i + 1) {
                            value.push(*c);
                        }
                        i += 2;
                    }
                    Some(c) => {
                        value.push(*c);
                        i += 1;
                    }
                }
            }
        } else {
            while i < chars.len() && !chars[i].is_whitespace() {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                value.push(chars[i]);
                i += 1;
            }
        }
        pairs.push((key, value));
    }

    let mut settings = PostgresSettings {
        host: "localhost".into(),
        port: 5432,
        database: "postgres".into(),
        username: String::new(),
        password: SecretRef::None,
        ssl_mode: PgSslMode::default(),
        search_path: None,
        application_name: "GazaBridge".into(),
        connect_timeout_secs: 10,
        statement_timeout_ms: 0,
        pool_size: 4,
    };
    let mut password = None;
    let mut warnings = Vec::new();
    for (k, v) in pairs {
        match k.as_str() {
            "host" | "hostaddr" => settings.host = v.to_string(),
            "port" => settings.port = v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?,
            "dbname" => settings.database = v.to_string(),
            "user" => settings.username = v.to_string(),
            "password" => password = Some(v),
            "sslmode" => settings.ssl_mode = pg_ssl_mode(&v)?,
            "application_name" => settings.application_name = v.to_string(),
            "connect_timeout" => {
                settings.connect_timeout_secs =
                    v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?
            }
            other => warnings.push(format!("parameter `{other}` ignored")),
        }
    }
    let profile = guess_pg_profile(&settings.host).to_owned();
    Ok(ParsedConnection {
        settings: ConnectionSettings::Postgres(settings),
        compatibility_profile: profile,
        password,
        warnings,
    })
}

fn parse_mysql_uri(s: &str) -> Result<ParsedConnection, ParseError> {
    let parts = split_uri(s)?;
    let first = parse_host_port(parts.hosts, 3306)?
        .into_iter()
        .next()
        .ok_or_else(|| ParseError::Malformed("missing host".into()))?;
    let db = decode(parts.path)?;
    let mut settings = MySqlSettings {
        host: first.host,
        port: first.port,
        database: (!db.is_empty()).then_some(db),
        username: parts.username.map(decode).transpose()?.unwrap_or_default(),
        password: SecretRef::None,
        tls_mode: MySqlTlsMode::default(),
        charset: "utf8mb4".into(),
        connect_timeout_secs: 10,
        query_timeout_ms: 0,
        pool_size: 4,
    };
    let password = parts.password.map(decode_secret).transpose()?;
    let mut warnings = Vec::new();
    for (k, v) in parts.query {
        match k.to_ascii_lowercase().as_str() {
            "ssl-mode" | "sslmode" | "ssl_mode" => {
                settings.tls_mode = match v.to_ascii_uppercase().replace('-', "_").as_str() {
                    "DISABLED" => MySqlTlsMode::Disabled,
                    "PREFERRED" => MySqlTlsMode::Preferred,
                    "REQUIRED" => MySqlTlsMode::Required,
                    "VERIFY_CA" => MySqlTlsMode::VerifyCa,
                    "VERIFY_IDENTITY" => MySqlTlsMode::VerifyIdentity,
                    _ => return Err(ParseError::InvalidValue(k)),
                }
            }
            "charset" => settings.charset = v,
            "connect_timeout" => {
                settings.connect_timeout_secs =
                    v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?
            }
            other => warnings.push(format!("parameter `{other}` ignored")),
        }
    }
    let profile = if s.to_ascii_lowercase().starts_with("mariadb://") {
        "mysql.mariadb"
    } else if settings.host.contains("tidbcloud.com") {
        "mysql.tidb"
    } else {
        "mysql.mysql"
    };
    Ok(ParsedConnection {
        settings: ConnectionSettings::MySql(settings),
        compatibility_profile: profile.to_owned(),
        password,
        warnings,
    })
}

fn parse_mongo_uri(s: &str) -> Result<ParsedConnection, ParseError> {
    let parts = split_uri(s)?;
    let srv = s.to_ascii_lowercase().starts_with("mongodb+srv://");
    let hosts = if srv {
        Vec::new()
    } else {
        parse_host_port(parts.hosts, 27017)?
    };
    let password = parts.password.map(decode_secret).transpose()?;
    let db = decode(parts.path)?;
    let mut settings = MongoSettings {
        uri: Some(strip_userinfo(s)),
        hosts,
        replica_set: None,
        auth_source: None,
        username: parts.username.map(decode).transpose()?,
        password: SecretRef::None,
        default_database: (!db.is_empty()).then_some(db),
        tls: srv,
        read_preference: ReadPreference::default(),
        direct_connection: false,
        server_selection_timeout_ms: 10_000,
        connect_timeout_ms: 10_000,
    };
    let mut warnings = Vec::new();
    for (k, v) in &parts.query {
        match k.to_ascii_lowercase().as_str() {
            "replicaset" => settings.replica_set = Some(v.clone()),
            "authsource" => settings.auth_source = Some(v.clone()),
            "tls" | "ssl" => settings.tls = parse_bool_param(k, v)?,
            "directconnection" => settings.direct_connection = parse_bool_param(k, v)?,
            "readpreference" => {
                settings.read_preference = match v.as_str() {
                    "primary" => ReadPreference::Primary,
                    "primaryPreferred" => ReadPreference::PrimaryPreferred,
                    "secondary" => ReadPreference::Secondary,
                    "secondaryPreferred" => ReadPreference::SecondaryPreferred,
                    "nearest" => ReadPreference::Nearest,
                    _ => return Err(ParseError::InvalidValue(k.clone())),
                }
            }
            "serverselectiontimeoutms" => {
                settings.server_selection_timeout_ms =
                    v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?
            }
            "connecttimeoutms" => {
                settings.connect_timeout_ms =
                    v.parse().map_err(|_| ParseError::InvalidValue(k.clone()))?
            }
            // Passed through untouched in the stored URI.
            _ => {}
        }
        if k.eq_ignore_ascii_case("password") {
            warnings.push("password in query string was removed".into());
        }
    }
    let lower_hosts = parts.hosts.to_ascii_lowercase();
    let profile =
        if lower_hosts.contains("docdb.amazonaws.com") || lower_hosts.contains("docdb-elastic") {
            "mongodb.documentdb"
        } else if lower_hosts.contains(".mongodb.net") {
            "mongodb.atlas"
        } else {
            "mongodb.community"
        };
    Ok(ParsedConnection {
        settings: ConnectionSettings::MongoDb(settings),
        compatibility_profile: profile.to_owned(),
        password,
        warnings,
    })
}

fn parse_sqlite(s: &str) -> Result<ParsedConnection, ParseError> {
    let rest = s
        .strip_prefix("sqlite://")
        .or_else(|| s.strip_prefix("sqlite:"))
        .ok_or(ParseError::UnknownScheme)?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    if path.is_empty() {
        return Err(ParseError::Malformed("missing file path".into()));
    }
    let mut settings = SqliteSettings {
        path: PathBuf::from(decode(path)?),
        read_only: false,
        create_if_missing: false,
        busy_timeout_ms: 5_000,
        journal_mode: Default::default(),
        foreign_keys: true,
    };
    let mut warnings = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        match k {
            "mode" => match v {
                "ro" => settings.read_only = true,
                "rwc" => settings.create_if_missing = true,
                "rw" => {}
                _ => return Err(ParseError::InvalidValue("mode".into())),
            },
            other => warnings.push(format!("parameter `{other}` ignored")),
        }
    }
    Ok(ParsedConnection {
        settings: ConnectionSettings::Sqlite(settings),
        compatibility_profile: "sqlite.sqlite".into(),
        password: None,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_uri_passwords() {
        assert_eq!(
            redact("postgres://app:s3cr%2Ft@db.internal:5432/app"),
            "postgres://app:****@db.internal:5432/app"
        );
        assert_eq!(
            redact("connect mongodb+srv://u:p/a@ss@cluster0.mongodb.net/x now"),
            "connect mongodb+srv://u:****@cluster0.mongodb.net/x now"
        );
        assert_eq!(
            redact("mysql://root@localhost/db"),
            "mysql://root@localhost/db"
        );
    }

    #[test]
    fn redacts_key_value_secrets() {
        assert_eq!(
            redact("host=db user=app password='a b\\'c' sslmode=require"),
            "host=db user=app password=**** sslmode=require"
        );
        assert_eq!(
            redact("x://h/?sslpassword=abc&a=1"),
            "x://h/?sslpassword=****&a=1"
        );
        assert_eq!(redact("PWD=hunter2;Server=x"), "PWD=****;Server=x");
        assert_eq!(redact("passwordless=1"), "passwordless=1");
        assert_eq!(
            redact(r#"{"password": "abc", "u": 1}"#),
            r#"{"password": ****, "u": 1}"#
        );
    }

    #[test]
    fn parses_postgres_uri_and_extracts_password() {
        let p = parse("postgresql://alice:p%40ss@ep-cool.eu-central-1.aws.neon.tech/neondb?sslmode=require&options=-csearch_path%3Dapp").unwrap();
        assert_eq!(p.password.as_deref().map(String::as_str), Some("p@ss"));
        assert_eq!(p.compatibility_profile, "postgres.neon");
        match p.settings {
            ConnectionSettings::Postgres(s) => {
                assert_eq!(s.username, "alice");
                assert_eq!(s.database, "neondb");
                assert_eq!(s.ssl_mode, PgSslMode::Require);
                assert_eq!(s.search_path.as_deref(), Some("app"));
                assert_eq!(s.password, SecretRef::None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_libpq_dsn() {
        let p =
            parse("host=localhost port=6543 dbname=app user=bob password='it''s' sslmode=disable")
                .unwrap_err();
        assert!(matches!(p, ParseError::Malformed(_)));
        let p =
            parse(r"host=localhost port=6543 dbname=app user=bob password='it\'s' sslmode=disable")
                .unwrap();
        assert_eq!(p.password.as_deref().map(String::as_str), Some("it's"));
    }

    #[test]
    fn parses_multi_host_mongo_uri_and_strips_credentials() {
        let p = parse(
            "mongodb://admin:pw@h1:27017,h2:27018/shop?replicaSet=rs0&authSource=admin&tls=true",
        )
        .unwrap();
        assert_eq!(p.password.as_deref().map(String::as_str), Some("pw"));
        match p.settings {
            ConnectionSettings::MongoDb(s) => {
                assert_eq!(s.hosts.len(), 2);
                assert_eq!(s.replica_set.as_deref(), Some("rs0"));
                assert_eq!(s.username.as_deref(), Some("admin"));
                let uri = s.uri.unwrap();
                assert!(!uri.contains("pw"), "{uri}");
                assert!(!mongo_uri_has_password(&uri));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn detects_documentdb_and_atlas() {
        assert_eq!(
            parse("mongodb://u:p@docdb-1.cluster-x.us-east-1.docdb.amazonaws.com:27017/?tls=true")
                .unwrap()
                .compatibility_profile,
            "mongodb.documentdb"
        );
        assert_eq!(
            parse("mongodb+srv://c0.abcd.mongodb.net")
                .unwrap()
                .compatibility_profile,
            "mongodb.atlas"
        );
    }

    #[test]
    fn parses_sqlite() {
        let p = parse("sqlite:///tmp/a%20b.db?mode=ro").unwrap();
        match p.settings {
            ConnectionSettings::Sqlite(s) => {
                assert_eq!(s.path, PathBuf::from("/tmp/a b.db"));
                assert!(s.read_only);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parse_errors_do_not_echo_input() {
        let err = parse("postgres://u:topsecret@host:notaport/db").unwrap_err();
        assert!(!err.to_string().contains("topsecret"));
    }
}
