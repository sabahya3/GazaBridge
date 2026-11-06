//! Native database tools: `mongodump`, `mongorestore`, `pg_dump`,
//! `pg_restore`.
//!
//! Passwords never appear in process arguments (where other users can read
//! them): MongoDB tools read the URI and password from a temporary YAML
//! config file, PostgreSQL tools from a temporary `pgpass` file named by
//! `PGPASSFILE`. Both files are private to the user (0600) and deleted as
//! soon as the process ends. Previews show the command without secrets.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use gazabridge_domain::connection::{ConnectionProfile, ConnectionSettings, PgSslMode};
use gazabridge_plugin_sdk::ResolvedSecrets;
use gazabridge_plugin_sdk::schema::ObjectRef;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;

use crate::backup::Manifest;
use crate::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    MongoDump,
    MongoRestore,
    PgDump,
    PgRestore,
}

impl ToolKind {
    pub const ALL: [ToolKind; 4] = [
        Self::MongoDump,
        Self::MongoRestore,
        Self::PgDump,
        Self::PgRestore,
    ];

    pub fn program(self) -> &'static str {
        match self {
            Self::MongoDump => "mongodump",
            Self::MongoRestore => "mongorestore",
            Self::PgDump => "pg_dump",
            Self::PgRestore => "pg_restore",
        }
    }

    pub fn install_hint(self) -> &'static str {
        match self {
            Self::MongoDump | Self::MongoRestore => {
                "Install the MongoDB Database Tools (macOS: `brew install mongodb-database-tools`; Windows: the MSI from mongodb.com/try/download/database-tools; Linux: the `mongodb-database-tools` package)."
            }
            Self::PgDump | Self::PgRestore => {
                "Install the PostgreSQL client tools, matching or newer than the server version (macOS: `brew install libpq`; Windows: the PostgreSQL installer, \"Command Line Tools\" only; Linux: the `postgresql-client` package)."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolInfo {
    pub kind: ToolKind,
    pub path: Option<PathBuf>,
    pub version: Option<String>,
    pub install_hint: String,
}

/// Where the tools usually live besides `PATH` (apps started from the Dock
/// or the Start menu do not get the shell's `PATH`).
const EXTRA_DIRS: [&str; 6] = [
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/opt/homebrew/opt/libpq/bin",
    "/usr/local/opt/libpq/bin",
    "/Applications/Postgres.app/Contents/Versions/latest/bin",
    "/usr/bin",
];

/// Installers that put each version in its own folder: Windows
/// (`Program Files\PostgreSQL\16\bin`, `Program Files\MongoDB\Tools\100\bin`)
/// and Debian/Ubuntu (`/usr/lib/postgresql/16/bin`). Newest version first.
fn versioned_dirs() -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = vec![PathBuf::from("/usr/lib/postgresql")];
    for var in ["ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"] {
        if let Some(pf) = std::env::var_os(var) {
            let pf = PathBuf::from(pf);
            roots.push(pf.join("PostgreSQL"));
            roots.push(pf.join("MongoDB").join("Tools"));
        }
    }
    roots.iter().flat_map(|r| versions_in(r)).collect()
}

/// `root/<version>/bin` for each version folder, newest first.
fn versions_in(root: &std::path::Path) -> Vec<PathBuf> {
    let mut found: Vec<(Vec<u64>, PathBuf)> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let version: Vec<u64> = name
                .split('.')
                .map(|p| p.parse().ok())
                .collect::<Option<_>>()?;
            Some((version, e.path().join("bin")))
        })
        .collect();
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn find(program: &str) -> Option<PathBuf> {
    let file = format!("{program}{}", std::env::consts::EXE_SUFFIX);
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .chain(EXTRA_DIRS.iter().map(PathBuf::from))
        .chain(versioned_dirs())
        .map(|d| d.join(&file))
        .find(|p| p.is_file())
}

/// Finds a tool and asks it for its version.
pub async fn detect(kind: ToolKind) -> ToolInfo {
    let path = find(kind.program());
    let version = match &path {
        Some(p) => {
            let out = tokio::time::timeout(
                Duration::from_secs(10),
                tokio::process::Command::new(p)
                    .arg("--version")
                    .stdin(Stdio::null())
                    .kill_on_drop(true)
                    .output(),
            )
            .await;
            match out {
                Ok(Ok(o)) => String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .next()
                    .map(|l| l.trim().to_owned())
                    .filter(|l| !l.is_empty()),
                _ => None,
            }
        }
        None => None,
    };
    ToolInfo {
        kind,
        path,
        version,
        install_hint: kind.install_hint().into(),
    }
}

/// A command ready to run, with its secret file (if any) kept alive.
#[derive(Debug)]
pub struct Invocation {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// The command as shown to the user: no secrets.
    pub preview: String,
    _secret: Option<tempfile::NamedTempFile>,
}

fn quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_=./:@,*".contains(c))
    {
        s.to_owned()
    } else {
        format!("'{}'", s.replace('\'', r"'\''"))
    }
}

fn preview(
    program: &Path,
    args: &[String],
    env: &[(String, String)],
    secret_names: &[&str],
) -> String {
    let mut parts: Vec<String> = env
        .iter()
        .map(|(k, v)| {
            if secret_names.contains(&k.as_str()) {
                format!("{k}=<temporary file>")
            } else {
                format!("{k}={}", quote(v))
            }
        })
        .collect();
    parts.push(program.file_name().map_or_else(
        || program.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    ));
    parts.extend(args.iter().map(|a| {
        if a.starts_with("--config=") {
            "--config=<temporary file>".into()
        } else {
            quote(a)
        }
    }));
    parts.join(" ")
}

/// A private file holding `content`, deleted when dropped.
fn secret_file(content: &str) -> AppResult<tempfile::NamedTempFile> {
    use std::io::Write;
    let mut f = tempfile::Builder::new()
        .prefix(".gazabridge-")
        .tempfile()
        .map_err(|e| AppError::Invalid(format!("cannot create a private file: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600))
            .map_err(|e| AppError::Invalid(format!("cannot protect the private file: {e}")))?;
    }
    f.write_all(content.as_bytes())
        .and_then(|()| f.flush())
        .map_err(|e| AppError::Invalid(format!("cannot write the private file: {e}")))?;
    Ok(f)
}

fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// YAML double-quoted scalar.
fn yaml(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// MongoDB URI without a password (the password goes to the config file).
pub fn mongo_uri(profile: &ConnectionProfile) -> AppResult<String> {
    let ConnectionSettings::MongoDb(s) = &profile.settings else {
        return Err(AppError::Invalid("not a MongoDB connection".into()));
    };
    let (scheme, rest, mut params) = match &s.uri {
        Some(uri) => {
            let (scheme, rest) = uri
                .split_once("://")
                .ok_or_else(|| AppError::Invalid("the connection URI has no scheme".into()))?;
            let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
            let params: Vec<(String, String)> = query
                .split('&')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let (k, v) = p.split_once('=').unwrap_or((p, ""));
                    (k.to_owned(), v.to_owned())
                })
                .collect();
            (scheme.to_owned(), rest.to_owned(), params)
        }
        None => {
            if s.hosts.is_empty() {
                return Err(AppError::Invalid("the connection has no host".into()));
            }
            let hosts: Vec<String> = s
                .hosts
                .iter()
                .map(|h| format!("{}:{}", h.host, h.port))
                .collect();
            (
                "mongodb".to_owned(),
                format!("{}/", hosts.join(",")),
                Vec::new(),
            )
        }
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest.as_str(), ""));
    // Any user info in the stored URI is replaced by the profile's user.
    let hosts = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let user = s
        .username
        .as_deref()
        .map(|u| format!("{}@", pct(u)))
        .unwrap_or_default();
    let mut set = |k: &str, v: String| {
        if let Some(p) = params.iter_mut().find(|(pk, _)| pk.eq_ignore_ascii_case(k)) {
            p.1 = v;
        } else {
            params.push((k.to_owned(), v));
        }
    };
    if s.username.is_some()
        && let Some(src) = s.auth_source.clone().or_else(|| s.default_database.clone())
    {
        set("authSource", pct(&src));
    }
    if let Some(rs) = &s.replica_set {
        set("replicaSet", pct(rs));
    }
    if scheme == "mongodb" {
        set("directConnection", s.direct_connection.to_string());
    }
    set("tls", s.tls.to_string());
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    Ok(format!("{scheme}://{user}{hosts}/{path}?{query}"))
}

fn mongo_base(
    profile: &ConnectionProfile,
    secrets: &ResolvedSecrets,
) -> AppResult<(Vec<String>, tempfile::NamedTempFile)> {
    let mut config = format!("uri: {}\n", yaml(&mongo_uri(profile)?));
    if let Some(pw) = &secrets.password {
        config.push_str(&format!("password: {}\n", yaml(pw)));
    }
    let file = secret_file(&config)?;
    let mut args = vec![format!("--config={}", file.path().display())];
    if let Some(ca) = &profile.tls.ca_certificate {
        args.push(format!("--sslCAFile={}", ca.display()));
    }
    if let Some(cert) = &profile.tls.client_certificate {
        args.push(format!("--sslPEMKeyFile={}", cert.display()));
    }
    Ok((args, file))
}

/// Connection arguments, environment and the password file (kept alive).
type PgBase = (
    Vec<String>,
    Vec<(String, String)>,
    Option<tempfile::NamedTempFile>,
);

fn pg_base(profile: &ConnectionProfile, secrets: &ResolvedSecrets) -> AppResult<PgBase> {
    let ConnectionSettings::Postgres(s) = &profile.settings else {
        return Err(AppError::Invalid("not a PostgreSQL connection".into()));
    };
    let args = vec![
        format!("--host={}", s.host),
        format!("--port={}", s.port),
        format!("--username={}", s.username),
        format!("--dbname={}", s.database),
        "--no-password".into(),
    ];
    let mode = match s.ssl_mode {
        PgSslMode::Disable => "disable",
        PgSslMode::Prefer => "prefer",
        PgSslMode::Require => "require",
        PgSslMode::VerifyCa => "verify-ca",
        PgSslMode::VerifyFull => "verify-full",
    };
    let mut env = vec![
        ("PGSSLMODE".to_owned(), mode.to_owned()),
        ("PGAPPNAME".to_owned(), "GazaBridge".to_owned()),
        (
            "PGCONNECT_TIMEOUT".to_owned(),
            s.connect_timeout_secs.max(1).to_string(),
        ),
    ];
    if let Some(ca) = &profile.tls.ca_certificate {
        env.push(("PGSSLROOTCERT".into(), ca.display().to_string()));
    }
    if let (Some(cert), Some(key)) = (&profile.tls.client_certificate, &profile.tls.client_key) {
        env.push(("PGSSLCERT".into(), cert.display().to_string()));
        env.push(("PGSSLKEY".into(), key.display().to_string()));
    }
    let file = match &secrets.password {
        Some(pw) => {
            let esc = |v: &str| v.replace('\\', "\\\\").replace(':', "\\:");
            let line = format!(
                "{}:{}:{}:{}:{}\n",
                esc(&s.host),
                s.port,
                esc(&s.database),
                esc(&s.username),
                esc(pw)
            );
            let f = secret_file(&line)?;
            env.push(("PGPASSFILE".into(), f.path().display().to_string()));
            Some(f)
        }
        None => None,
    };
    Ok((args, env, file))
}

/// `mongodump`/`pg_dump` for `objects` into `out`. `all` lists every
/// collection of the database (MongoDB), so the others can be excluded.
pub fn dump(
    kind: ToolKind,
    path: &Path,
    profile: &ConnectionProfile,
    secrets: &ResolvedSecrets,
    objects: &[ObjectRef],
    all: &[String],
    out: &Path,
) -> AppResult<Invocation> {
    match kind {
        ToolKind::MongoDump => {
            let (mut args, file) = mongo_base(profile, secrets)?;
            let db = objects
                .first()
                .and_then(|o| o.namespace.database.clone())
                .ok_or_else(|| AppError::Invalid("choose a database".into()))?;
            args.push(format!("--db={db}"));
            let chosen: Vec<&str> = objects.iter().map(|o| o.name.as_str()).collect();
            if let [one] = chosen.as_slice() {
                args.push(format!("--collection={one}"));
            } else {
                for other in all.iter().filter(|c| !chosen.contains(&c.as_str())) {
                    args.push(format!("--excludeCollection={other}"));
                }
            }
            args.push(format!("--archive={}", out.display()));
            args.push("--gzip".into());
            Ok(Invocation {
                preview: preview(path, &args, &[], &[]),
                program: path.to_path_buf(),
                args,
                env: Vec::new(),
                _secret: Some(file),
            })
        }
        ToolKind::PgDump => {
            let (mut args, env, file) = pg_base(profile, secrets)?;
            args.push("--format=custom".into());
            for o in objects {
                let schema = o.namespace.schema.as_deref().unwrap_or("public");
                args.push(format!(
                    "--table=\"{}\".\"{}\"",
                    schema.replace('"', "\"\""),
                    o.name.replace('"', "\"\"")
                ));
            }
            args.push(format!("--file={}", out.display()));
            Ok(Invocation {
                preview: preview(path, &args, &env, &["PGPASSFILE"]),
                program: path.to_path_buf(),
                args,
                env,
                _secret: file,
            })
        }
        _ => Err(AppError::Invalid(format!(
            "{} does not make backups",
            kind.program()
        ))),
    }
}

/// `mongorestore`/`pg_restore` of a native backup.
#[allow(clippy::too_many_arguments)] // the whole restore, kept explicit
pub fn restore(
    kind: ToolKind,
    path: &Path,
    profile: &ConnectionProfile,
    secrets: &ResolvedSecrets,
    manifest: &Manifest,
    dir: &Path,
    rename_to: Option<&str>,
    only: &[ObjectRef],
    drop: bool,
) -> AppResult<Invocation> {
    let archive = manifest
        .archive
        .as_ref()
        .ok_or_else(|| AppError::Invalid("this backup has no tool archive".into()))?;
    let file = dir.join(&archive.file);
    let rename = rename_to.map(str::trim).filter(|r| !r.is_empty());
    match kind {
        ToolKind::MongoRestore => {
            let (mut args, secret) = mongo_base(profile, secrets)?;
            args.push(format!("--archive={}", file.display()));
            args.push("--gzip".into());
            let db = manifest.database.clone().unwrap_or_default();
            for o in only {
                args.push(format!("--nsInclude={db}.{}", o.name));
            }
            if let Some(to) = rename {
                args.push(format!("--nsFrom={db}.*"));
                args.push(format!("--nsTo={to}.*"));
            }
            if drop {
                args.push("--drop".into());
            }
            args.push("--stopOnError".into());
            Ok(Invocation {
                preview: preview(path, &args, &[], &[]),
                program: path.to_path_buf(),
                args,
                env: Vec::new(),
                _secret: Some(secret),
            })
        }
        ToolKind::PgRestore => {
            if rename.is_some_and(|r| Some(r) != manifest.schema.as_deref()) {
                return Err(AppError::Invalid(
                    "pg_restore cannot move tables to another schema; use a GazaBridge backup for that".into(),
                ));
            }
            let (mut args, env, secret) = pg_base(profile, secrets)?;
            args.extend([
                "--no-owner".into(),
                "--no-privileges".into(),
                "--exit-on-error".into(),
            ]);
            if drop {
                args.extend(["--clean".into(), "--if-exists".into()]);
            }
            if !only.is_empty() {
                if let Some(schema) = &manifest.schema {
                    args.push(format!("--schema={schema}"));
                }
                for o in only {
                    args.push(format!("--table={}", o.name));
                }
            }
            args.push(file.display().to_string());
            Ok(Invocation {
                preview: preview(path, &args, &env, &["PGPASSFILE"]),
                program: path.to_path_buf(),
                args,
                env,
                _secret: secret,
            })
        }
        _ => Err(AppError::Invalid(format!(
            "{} does not restore",
            kind.program()
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    pub success: bool,
    pub code: Option<i32>,
    pub preview: String,
}

/// Runs a tool, passing each output line to `on_line`; cancelling kills it.
pub async fn run(
    inv: Invocation,
    cancel: CancellationToken,
    on_line: &(dyn Fn(&str) + Send + Sync),
) -> AppResult<RunOutcome> {
    let mut child = tokio::process::Command::new(&inv.program)
        .args(&inv.args)
        .envs(inv.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        // Never let an inherited password override the private file.
        .env_remove("PGPASSWORD")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| AppError::Invalid(format!("cannot start {}: {e}", inv.program.display())))?;
    let mut out = child.stdout.take().map(|s| BufReader::new(s).lines());
    let mut err = child.stderr.take().map(|s| BufReader::new(s).lines());
    let status = loop {
        tokio::select! {
            () = cancel.cancelled() => {
                let _ = child.kill().await;
                return Err(AppError::Connector(gazabridge_plugin_sdk::ConnectorError::Cancelled));
            }
            line = async { match &mut out { Some(l) => l.next_line().await, None => std::future::pending().await } } => {
                match line { Ok(Some(l)) => on_line(&l), _ => out = None }
            }
            line = async { match &mut err { Some(l) => l.next_line().await, None => std::future::pending().await } } => {
                match line { Ok(Some(l)) => on_line(&l), _ => err = None }
            }
            status = child.wait(), if out.is_none() && err.is_none() => {
                break status.map_err(|e| AppError::Invalid(format!("{} did not finish: {e}", inv.program.display())))?;
            }
        }
    };
    Ok(RunOutcome {
        success: status.success(),
        code: status.code(),
        preview: inv.preview.clone(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use gazabridge_domain::connection::{
        HostPort, MongoSettings, PostgresSettings, ReadPreference, SecretRef,
    };
    use gazabridge_plugin_sdk::schema::NamespacePath;
    use zeroize::Zeroizing;

    use super::*;

    #[test]
    fn versioned_install_folders_are_searched_newest_first() {
        let root = tempfile::tempdir().unwrap();
        for v in ["9.6", "16", "13", "notes"] {
            std::fs::create_dir_all(root.path().join(v).join("bin")).unwrap();
        }
        let found = versions_in(root.path());
        assert_eq!(
            found,
            ["16", "13", "9.6"].map(|v| root.path().join(v).join("bin"))
        );
        assert!(versions_in(&root.path().join("missing")).is_empty());
    }

    fn mongo(uri: Option<&str>) -> ConnectionProfile {
        ConnectionProfile::new(
            "m",
            "mongodb.community",
            ConnectionSettings::MongoDb(MongoSettings {
                uri: uri.map(String::from),
                hosts: vec![HostPort {
                    host: "db.example.test".into(),
                    port: 27017,
                }],
                replica_set: Some("rs0".into()),
                auth_source: None,
                username: Some("app user".into()),
                password: SecretRef::None,
                default_database: Some("shop".into()),
                tls: true,
                read_preference: ReadPreference::Primary,
                direct_connection: false,
                server_selection_timeout_ms: 1_000,
                connect_timeout_ms: 1_000,
            }),
        )
    }

    fn secrets(pw: &str) -> ResolvedSecrets {
        ResolvedSecrets {
            password: Some(Zeroizing::new(pw.into())),
            ..Default::default()
        }
    }

    #[test]
    fn mongo_uris_carry_the_user_but_never_the_password() {
        let u = mongo_uri(&mongo(None)).unwrap();
        assert_eq!(
            u,
            "mongodb://app%20user@db.example.test:27017/?authSource=shop&replicaSet=rs0&directConnection=false&tls=true"
        );
        let u = mongo_uri(&mongo(Some(
            "mongodb+srv://old:secret@cluster0.example.test/shop?retryWrites=true",
        )))
        .unwrap();
        assert!(u.starts_with("mongodb+srv://app%20user@cluster0.example.test/shop?retryWrites=true&authSource=shop"), "{u}");
        assert!(
            !u.contains("secret") && !u.contains("directConnection"),
            "{u}"
        );
    }

    #[test]
    fn dumps_keep_passwords_out_of_arguments_and_previews() {
        let dir = tempfile::tempdir().unwrap();
        let objects = vec![
            ObjectRef::collection("shop", "orders"),
            ObjectRef::collection("shop", "users"),
        ];
        let all = vec!["orders".to_owned(), "users".to_owned(), "logs".to_owned()];
        let inv = dump(
            ToolKind::MongoDump,
            Path::new("/x/mongodump"),
            &mongo(None),
            &secrets("p\"w:d"),
            &objects,
            &all,
            &dir.path().join("a.gz"),
        )
        .unwrap();
        assert!(inv.args.iter().all(|a| !a.contains("p\"w")));
        assert!(inv.args.contains(&"--excludeCollection=logs".to_owned()));
        assert!(
            inv.preview.contains("--config=<temporary file>") && !inv.preview.contains("p\"w"),
            "{}",
            inv.preview
        );
        let config_path = inv.args[0].strip_prefix("--config=").unwrap().to_owned();
        let config = std::fs::read_to_string(&config_path).unwrap();
        assert!(config.contains("password: \"p\\\"w:d\""), "{config}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&config_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop(inv);
        assert!(
            !Path::new(&config_path).exists(),
            "the secret file is deleted with the invocation"
        );

        let pg = ConnectionProfile::new(
            "p",
            "postgres.postgresql",
            ConnectionSettings::Postgres(PostgresSettings {
                host: "pg.example.test".into(),
                port: 5432,
                database: "app".into(),
                username: "u".into(),
                password: SecretRef::None,
                ssl_mode: PgSslMode::Require,
                search_path: None,
                application_name: "x".into(),
                connect_timeout_secs: 10,
                statement_timeout_ms: 0,
                pool_size: 1,
            }),
        );
        let t = ObjectRef::table(
            NamespacePath::schema(Some("app".into()), "public"),
            "order\"s",
        );
        let inv = dump(
            ToolKind::PgDump,
            Path::new("/x/pg_dump"),
            &pg,
            &secrets("a:b\\c"),
            &[t],
            &[],
            &dir.path().join("d"),
        )
        .unwrap();
        assert!(
            inv.args
                .contains(&"--table=\"public\".\"order\"\"s\"".to_owned()),
            "{:?}",
            inv.args
        );
        assert!(inv.args.iter().all(|a| !a.contains("a:b")));
        let pass = inv
            .env
            .iter()
            .find(|(k, _)| k == "PGPASSFILE")
            .unwrap()
            .1
            .clone();
        assert_eq!(
            std::fs::read_to_string(&pass).unwrap(),
            "pg.example.test:5432:app:u:a\\:b\\\\c\n"
        );
        assert!(inv.preview.starts_with("PGSSLMODE=require PGAPPNAME=GazaBridge PGCONNECT_TIMEOUT=10 PGPASSFILE=<temporary file> pg_dump"), "{}", inv.preview);
    }
}
