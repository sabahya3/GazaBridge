//! SSH tunnels for database connections (roadmap 2.7, decision D3).
//!
//! [`open`] signs in to an SSH server, checks its host key, and listens on
//! a free port of `127.0.0.1`. Every connection made to that port is
//! carried to the database through the SSH server (local port forward).
//! The [`Tunnel`] lives as long as the database connection that owns it:
//! dropping it stops listening and closes the SSH session.
//!
//! Host keys: `Strict` (the default) accepts only keys already recorded in
//! the user's `~/.ssh/known_hosts` or in GazaBridge's own file;
//! `AcceptNew` records an unknown host's key in GazaBridge's own file on
//! first use. A recorded key that changed is always refused. The user's
//! `~/.ssh/known_hosts` is never written.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use gazabridge_domain::connection::{HostKeyPolicy, SshAuth, SshTunnel};
use gazabridge_plugin_sdk::{ConnectorError, ConnectorResult, ErrorKind, ResolvedSecrets};
use russh::client::{self, Handle};
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Where host keys are looked up and recorded.
#[derive(Debug, Clone, Default)]
pub struct KnownHosts {
    /// Files searched for the host's key, in order.
    pub files: Vec<PathBuf>,
    /// The file `AcceptNew` records new keys in (GazaBridge's own).
    pub record: Option<PathBuf>,
}

impl KnownHosts {
    /// The user's `~/.ssh/known_hosts` (read only) and `own` (read and
    /// recorded into).
    pub fn with_own_file(own: PathBuf) -> Self {
        let mut files: Vec<PathBuf> = std::env::home_dir()
            .map(|h| h.join(".ssh").join("known_hosts"))
            .into_iter()
            .collect();
        files.push(own.clone());
        Self {
            files,
            record: Some(own),
        }
    }
}

static KNOWN_HOSTS: OnceLock<RwLock<KnownHosts>> = OnceLock::new();

fn known_hosts_lock() -> &'static RwLock<KnownHosts> {
    KNOWN_HOSTS.get_or_init(|| {
        RwLock::new(KnownHosts {
            files: std::env::home_dir()
                .map(|h| h.join(".ssh").join("known_hosts"))
                .into_iter()
                .collect(),
            record: None,
        })
    })
}

/// Sets the app's own known-hosts file (once, at startup).
pub fn set_known_hosts_file(own: PathBuf) {
    if let Ok(mut k) = known_hosts_lock().write() {
        *k = KnownHosts::with_own_file(own);
    }
}

fn known_hosts() -> KnownHosts {
    known_hosts_lock()
        .read()
        .map(|k| k.clone())
        .unwrap_or_default()
}

tokio::task_local! {
    static AGENT_SOCKET: PathBuf;
}

/// Runs `f` with the SSH agent at `socket` instead of `SSH_AUTH_SOCK`.
pub async fn with_agent_socket<F, Fut, T>(socket: &Path, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    AGENT_SOCKET.scope(socket.to_path_buf(), f()).await
}

/// A local port forwarded to the database through an SSH server.
pub struct Tunnel {
    local: SocketAddr,
    forward: JoinHandle<()>,
    session: Arc<Handle<Client>>,
}

impl std::fmt::Debug for Tunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tunnel")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl Tunnel {
    /// `127.0.0.1:<port>`: connect the database driver here.
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    /// The SSH session ended (server gone, network lost).
    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.forward.abort();
    }
}

/// Why the host key was refused, kept for the error message.
type Verdict = Arc<Mutex<Option<ConnectorError>>>;

struct Client {
    host: String,
    port: u16,
    policy: HostKeyPolicy,
    known: KnownHosts,
    verdict: Verdict,
}

fn fingerprint(key: &PublicKey) -> String {
    key.fingerprint(HashAlg::Sha256).to_string()
}

impl Client {
    fn check(&self, key: &PublicKey) -> Result<bool, ConnectorError> {
        let refuse = |kind, message: String| Err(ConnectorError::new(kind, message));
        for file in self.known.files.iter().filter(|f| f.exists()) {
            match russh::keys::check_known_hosts_path(&self.host, self.port, key, file) {
                Ok(true) => return Ok(true),
                Ok(false) => {}
                Err(russh::keys::Error::KeyChanged { line }) => {
                    return refuse(
                        ErrorKind::Authentication,
                        format!(
                            "the SSH host key of {} changed (now {}; {} line {line} has another). \
                             This can mean someone is intercepting the connection. If the server \
                             was reinstalled, remove the old line and connect again",
                            self.host,
                            fingerprint(key),
                            file.display()
                        ),
                    );
                }
                Err(e) => {
                    tracing::warn!(file = %file.display(), error = %e, "unreadable known_hosts file");
                }
            }
        }
        match (self.policy, &self.known.record) {
            (HostKeyPolicy::AcceptNew, Some(record)) => {
                russh::keys::known_hosts::learn_known_hosts_path(
                    &self.host, self.port, key, record,
                )
                .map_err(|e| {
                    ConnectorError::new(
                        ErrorKind::Io,
                        format!("recording the SSH host key in {}: {e}", record.display()),
                    )
                })?;
                tracing::info!(host = %self.host, port = self.port, fingerprint = %fingerprint(key), "recorded a new SSH host key");
                Ok(true)
            }
            _ => refuse(
                ErrorKind::Authentication,
                format!(
                    "the SSH host {}:{} is not in known_hosts (its key is {} {}). Check the \
                     fingerprint with the server's administrator, then add it to \
                     ~/.ssh/known_hosts or choose \"accept on first use\"",
                    self.host,
                    self.port,
                    key.algorithm(),
                    fingerprint(key)
                ),
            ),
        }
    }
}

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let key = match server {
            PublicKeyOrCertificate::PublicKey { key, .. } => key.clone(),
            PublicKeyOrCertificate::Certificate(c) => PublicKey::from(c.public_key().clone()),
        };
        match self.check(&key) {
            Ok(ok) => Ok(ok),
            Err(e) => {
                if let Ok(mut v) = self.verdict.lock() {
                    *v = Some(e);
                }
                Ok(false)
            }
        }
    }
}

fn conn_err(what: &str, e: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::new(ErrorKind::Connection, format!("{what}: {e}"))
}

/// Opens a tunnel to `target_host:target_port` (as the SSH server sees it)
/// using the app's known-hosts files.
pub async fn open(
    ssh: &SshTunnel,
    secrets: &ResolvedSecrets,
    target_host: &str,
    target_port: u16,
    timeout: Duration,
) -> ConnectorResult<Tunnel> {
    open_with(
        ssh,
        secrets,
        target_host,
        target_port,
        timeout,
        known_hosts(),
    )
    .await
}

/// [`open`] with explicit known-hosts files.
pub async fn open_with(
    ssh: &SshTunnel,
    secrets: &ResolvedSecrets,
    target_host: &str,
    target_port: u16,
    timeout: Duration,
    known: KnownHosts,
) -> ConnectorResult<Tunnel> {
    if ssh.host.trim().is_empty() || ssh.username.trim().is_empty() {
        return Err(ConnectorError::invalid_input(
            "an SSH tunnel needs a host and a user name",
        ));
    }
    let config = Arc::new(client::Config {
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 3,
        nodelay: true,
        ..Default::default()
    });
    let verdict: Verdict = Arc::default();
    let handler = Client {
        host: ssh.host.clone(),
        port: ssh.port,
        policy: ssh.host_key_policy,
        known,
        verdict: verdict.clone(),
    };
    let addr = (ssh.host.as_str(), ssh.port);
    let connected = tokio::time::timeout(timeout, client::connect(config, addr, handler)).await;
    let mut session = match connected {
        Err(_) => {
            return Err(ConnectorError::new(
                ErrorKind::Timeout,
                format!(
                    "the SSH server {}:{} did not answer in time",
                    ssh.host, ssh.port
                ),
            ));
        }
        Ok(Err(e)) => {
            let refused = verdict.lock().ok().and_then(|mut v| v.take());
            return Err(refused.unwrap_or_else(|| {
                conn_err(
                    &format!("connecting to the SSH server {}:{}", ssh.host, ssh.port),
                    e,
                )
            }));
        }
        Ok(Ok(s)) => s,
    };
    tokio::time::timeout(timeout, authenticate(&mut session, ssh, secrets))
        .await
        .map_err(|_| ConnectorError::new(ErrorKind::Timeout, "SSH sign-in took too long"))??;

    let session = Arc::new(session);
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|e| conn_err("opening the local tunnel port", e))?;
    let local = listener
        .local_addr()
        .map_err(|e| conn_err("opening the local tunnel port", e))?;
    // Check once that the SSH server may reach the database, so a refused
    // forward is reported now rather than as an obscure driver error.
    let probe = session
        .channel_open_direct_tcpip(
            target_host,
            u32::from(target_port),
            "127.0.0.1",
            u32::from(local.port()),
        )
        .await
        .map_err(|e| {
            ConnectorError::new(
                ErrorKind::Connection,
                format!(
                    "the SSH server could not reach {target_host}:{target_port} ({e}); check the \
                     database host as the SSH server sees it, and that the server allows TCP \
                     forwarding"
                ),
            )
        })?;
    drop(probe);
    let forward = tokio::spawn(forward(
        listener,
        session.clone(),
        target_host.to_owned(),
        target_port,
    ));
    tracing::info!(ssh = %ssh.host, target = %format!("{target_host}:{target_port}"), local = %local, "SSH tunnel open");
    Ok(Tunnel {
        local,
        forward,
        session,
    })
}

async fn authenticate(
    session: &mut Handle<Client>,
    ssh: &SshTunnel,
    secrets: &ResolvedSecrets,
) -> ConnectorResult<()> {
    let user = ssh.username.clone();
    let failed = |how: &str| {
        ConnectorError::new(
            ErrorKind::Authentication,
            format!("the SSH server refused {how} for user {user}"),
        )
    };
    let ok = match &ssh.auth {
        SshAuth::Password { .. } => {
            let password = secrets.ssh_password.as_ref().ok_or_else(|| {
                ConnectorError::new(ErrorKind::Authentication, "the SSH password is missing")
            })?;
            session
                .authenticate_password(user.clone(), password.as_str())
                .await
                .map_err(|e| conn_err("SSH sign-in", e))?
                .success()
        }
        SshAuth::PrivateKey { key_path, .. } => {
            let key = load_key(
                key_path,
                secrets.ssh_key_passphrase.as_deref().map(|s| s.as_str()),
            )?;
            let hash = if key.algorithm().is_rsa() {
                session
                    .best_supported_rsa_hash()
                    .await
                    .map_err(|e| conn_err("SSH sign-in", e))?
                    .flatten()
            } else {
                None
            };
            session
                .authenticate_publickey(
                    user.clone(),
                    PrivateKeyWithHashAlg::new(Arc::new(key), hash),
                )
                .await
                .map_err(|e| conn_err("SSH sign-in", e))?
                .success()
        }
        SshAuth::Agent => agent_sign_in(session, &user).await?,
    };
    if ok {
        Ok(())
    } else {
        Err(failed(match ssh.auth {
            SshAuth::Password { .. } => "the password",
            SshAuth::PrivateKey { .. } => "the key",
            SshAuth::Agent => "every key in the SSH agent",
        }))
    }
}

fn load_key(path: &Path, passphrase: Option<&str>) -> ConnectorResult<russh::keys::PrivateKey> {
    russh::keys::load_secret_key(path, passphrase).map_err(|e| {
        let detail = e.to_string();
        let hint = if detail.to_lowercase().contains("rsa") || detail.contains("algorithm") {
            " (RSA key files are not supported; use an Ed25519 or ECDSA key, or the SSH agent)"
        } else if passphrase.is_none() && detail.to_lowercase().contains("encrypt") {
            " (the key is protected by a passphrase)"
        } else {
            ""
        };
        ConnectorError::new(
            ErrorKind::Authentication,
            format!("reading the SSH key {}: {detail}{hint}", path.display()),
        )
    })
}

#[cfg(unix)]
async fn agent_sign_in(session: &mut Handle<Client>, user: &str) -> ConnectorResult<bool> {
    use russh::keys::agent::AgentIdentity;
    let socket = AGENT_SOCKET
        .try_with(Clone::clone)
        .ok()
        .or_else(|| std::env::var_os("SSH_AUTH_SOCK").map(PathBuf::from));
    let Some(socket) = socket else {
        return Err(ConnectorError::new(
            ErrorKind::Authentication,
            "no SSH agent is running (SSH_AUTH_SOCK is not set)",
        ));
    };
    let mut agent = russh::keys::agent::client::AgentClient::connect_uds(&socket)
        .await
        .map_err(|e| {
            ConnectorError::new(
                ErrorKind::Authentication,
                format!("no SSH agent is reachable (SSH_AUTH_SOCK): {e}"),
            )
        })?;
    let identities = agent
        .request_identities()
        .await
        .map_err(|e| conn_err("asking the SSH agent for keys", e))?;
    let rsa_hash = session
        .best_supported_rsa_hash()
        .await
        .map_err(|e| conn_err("SSH sign-in", e))?
        .flatten();
    for identity in identities {
        let AgentIdentity::PublicKey { key, .. } = identity else {
            continue;
        };
        let hash = key.algorithm().is_rsa().then_some(rsa_hash).flatten();
        match session
            .authenticate_publickey_with(user.to_owned(), key, hash, &mut agent)
            .await
        {
            Ok(r) if r.success() => return Ok(true),
            Ok(_) => {}
            Err(e) => tracing::debug!(error = %e, "the SSH agent did not sign"),
        }
    }
    Ok(false)
}

#[cfg(not(unix))]
async fn agent_sign_in(_: &mut Handle<Client>, _: &str) -> ConnectorResult<bool> {
    Err(ConnectorError::new(
        ErrorKind::Unsupported,
        "the SSH agent is not supported on this platform yet",
    ))
}

/// Carries every local connection through the SSH session.
async fn forward(listener: TcpListener, session: Arc<Handle<Client>>, host: String, port: u16) {
    loop {
        let Ok((mut socket, peer)) = listener.accept().await else {
            return;
        };
        if session.is_closed() {
            tracing::warn!("the SSH session closed; the tunnel stops");
            return;
        }
        let session = session.clone();
        let host = host.clone();
        tokio::spawn(async move {
            let channel = match session
                .channel_open_direct_tcpip(
                    host.clone(),
                    u32::from(port),
                    peer.ip().to_string(),
                    u32::from(peer.port()),
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, target = %format!("{host}:{port}"), "SSH forward refused");
                    return;
                }
            };
            let mut stream = channel.into_stream();
            let _ = tokio::io::copy_bidirectional(&mut socket, &mut stream).await;
        });
    }
}
