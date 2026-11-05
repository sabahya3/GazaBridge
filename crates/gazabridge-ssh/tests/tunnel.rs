//! SSH tunnels against a real OpenSSH server in a container, forwarding to
//! a PostgreSQL server that is reachable only from inside the container
//! network. Run with `cargo test -- --ignored` (needs Docker).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use gazabridge_domain::connection::{HostKeyPolicy, SecretRef, SshAuth, SshTunnel};
use gazabridge_plugin_sdk::{ErrorKind, ResolvedSecrets};
use gazabridge_ssh::{KnownHosts, open_with};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use zeroize::Zeroizing;

// Test-only values for throwaway containers.
const SSH_PASSWORD: &str = "tunnel-test-pw";
const KEY_PASSPHRASE: &str = "tunnel-test-passphrase";
const PG_PASSWORD: &str = "postgres";

fn keygen(dir: &Path, name: &str, kind: &str, passphrase: &str) -> PathBuf {
    let path = dir.join(name);
    let status = Command::new("ssh-keygen")
        .args([
            "-q",
            "-t",
            kind,
            "-N",
            passphrase,
            "-C",
            "gazabridge-test",
            "-f",
        ])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

fn public(path: &Path) -> String {
    std::fs::read_to_string(path.with_extension("pub"))
        .unwrap()
        .trim()
        .to_owned()
}

struct Env {
    _pg: ContainerAsync<Postgres>,
    _sshd: ContainerAsync<GenericImage>,
    ssh_port: u16,
    pg_name: String,
    dir: tempfile::TempDir,
    key: PathBuf,
    locked_key: PathBuf,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let key = keygen(dir.path(), "id_plain", "ed25519", "");
    let locked_key = keygen(dir.path(), "id_locked", "ecdsa", KEY_PASSPHRASE);
    let id = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let network = format!("gb-ssh-{id}-{nanos}");
    let pg_name = format!("gb-pg-{id}-{nanos}");
    let pg = Postgres::default()
        .with_network(&network)
        .with_container_name(&pg_name)
        .start()
        .await
        .unwrap();
    let authorized = format!("{}\n{}", public(&key), public(&locked_key));
    let script = format!(
        "apk add --no-cache openssh-server >/dev/null && ssh-keygen -A && \
         adduser -D tunnel && echo 'tunnel:{SSH_PASSWORD}' | chpasswd && \
         mkdir -p /home/tunnel/.ssh && printf '%s\\n' \"$KEYS\" > /home/tunnel/.ssh/authorized_keys && \
         chown -R tunnel /home/tunnel/.ssh && chmod 700 /home/tunnel/.ssh && \
         chmod 600 /home/tunnel/.ssh/authorized_keys && \
         exec /usr/sbin/sshd -D -e -o AllowTcpForwarding=yes -o PasswordAuthentication=yes"
    );
    let sshd = GenericImage::new("alpine", "3.20")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_network(&network)
        .with_env_var("KEYS", authorized)
        .with_cmd(["sh", "-c", script.as_str()])
        .start()
        .await
        .unwrap();
    let ssh_port = sshd.get_host_port_ipv4(22).await.unwrap();
    Env {
        _pg: pg,
        _sshd: sshd,
        ssh_port,
        pg_name,
        dir,
        key,
        locked_key,
    }
}

fn tunnel_cfg(e: &Env, auth: SshAuth, policy: HostKeyPolicy) -> SshTunnel {
    SshTunnel {
        host: "127.0.0.1".into(),
        port: e.ssh_port,
        username: "tunnel".into(),
        auth,
        host_key_policy: policy,
    }
}

fn password() -> SshAuth {
    SshAuth::Password {
        password: SecretRef::Session { key: "test".into() },
    }
}

fn secrets(ssh_password: Option<&str>, passphrase: Option<&str>) -> ResolvedSecrets {
    ResolvedSecrets {
        ssh_password: ssh_password.map(|p| Zeroizing::new(p.to_owned())),
        ssh_key_passphrase: passphrase.map(|p| Zeroizing::new(p.to_owned())),
        ..Default::default()
    }
}

fn known(e: &Env, file: &str) -> KnownHosts {
    let own = e.dir.path().join(file);
    KnownHosts {
        files: vec![own.clone()],
        record: Some(own),
    }
}

const WAIT: Duration = Duration::from_secs(20);

/// Runs a query on PostgreSQL through the tunnel's local port.
async fn query_through(local: std::net::SocketAddr) -> i32 {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=postgres password={PG_PASSWORD} dbname=postgres",
            local.ip(),
            local.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client.query_one("SELECT 40 + 2", &[]).await.unwrap().get(0)
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn tunnels_check_host_keys_sign_in_and_carry_traffic() {
    let e = env().await;
    let pg = e.pg_name.as_str();

    // Strict and unknown host: refused, with the fingerprint to check.
    let err = open_with(
        &tunnel_cfg(&e, password(), HostKeyPolicy::Strict),
        &secrets(Some(SSH_PASSWORD), None),
        pg,
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Authentication, "{err}");
    assert!(err.to_string().contains("SHA256:"), "{err}");
    assert!(
        !e.dir.path().join("known_hosts").exists(),
        "Strict records nothing"
    );

    // Accept on first use records the key; the tunnel carries queries.
    let tunnel = open_with(
        &tunnel_cfg(&e, password(), HostKeyPolicy::AcceptNew),
        &secrets(Some(SSH_PASSWORD), None),
        pg,
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap();
    assert!(tunnel.local_addr().ip().is_loopback());
    assert_eq!(query_through(tunnel.local_addr()).await, 42);
    // Several connections share one tunnel.
    let (a, b) = tokio::join!(
        query_through(tunnel.local_addr()),
        query_through(tunnel.local_addr())
    );
    assert_eq!((a, b), (42, 42));
    let recorded = std::fs::read_to_string(e.dir.path().join("known_hosts")).unwrap();
    assert!(
        recorded.contains(&format!("[127.0.0.1]:{}", e.ssh_port)),
        "{recorded}"
    );

    // Dropping the tunnel closes its port.
    let local = tunnel.local_addr();
    drop(tunnel);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(tokio::net::TcpStream::connect(local).await.is_err());

    // Strict now knows the host. Key files, with and without passphrase.
    let tunnel = open_with(
        &tunnel_cfg(
            &e,
            SshAuth::PrivateKey {
                key_path: e.key.clone(),
                passphrase: SecretRef::None,
            },
            HostKeyPolicy::Strict,
        ),
        &secrets(None, None),
        pg,
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap();
    assert_eq!(query_through(tunnel.local_addr()).await, 42);
    let locked = |pass: Option<&str>| {
        let cfg = tunnel_cfg(
            &e,
            SshAuth::PrivateKey {
                key_path: e.locked_key.clone(),
                passphrase: SecretRef::Session { key: "test".into() },
            },
            HostKeyPolicy::Strict,
        );
        let secrets = secrets(None, pass);
        let known = known(&e, "known_hosts");
        async move { open_with(&cfg, &secrets, pg, 5432, WAIT, known).await }
    };
    let t = locked(Some(KEY_PASSPHRASE)).await.unwrap();
    assert_eq!(query_through(t.local_addr()).await, 42);
    let err = locked(Some("wrong")).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Authentication, "{err}");

    // Wrong password and an unknown key are refused.
    let err = open_with(
        &tunnel_cfg(&e, password(), HostKeyPolicy::Strict),
        &secrets(Some("not-the-password"), None),
        pg,
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Authentication, "{err}");
    let stranger = keygen(e.dir.path(), "id_stranger", "ed25519", "");
    let err = open_with(
        &tunnel_cfg(
            &e,
            SshAuth::PrivateKey {
                key_path: stranger,
                passphrase: SecretRef::None,
            },
            HostKeyPolicy::Strict,
        ),
        &secrets(None, None),
        pg,
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Authentication, "{err}");

    // A target the SSH server cannot reach is reported up front.
    let err = open_with(
        &tunnel_cfg(&e, password(), HostKeyPolicy::Strict),
        &secrets(Some(SSH_PASSWORD), None),
        "no-such-host.invalid",
        5432,
        WAIT,
        known(&e, "known_hosts"),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("could not reach"), "{err}");

    // A changed host key is refused even with accept-on-first-use.
    let other = keygen(e.dir.path(), "id_other_host", "ed25519", "");
    let fake = public(&other);
    let fake_key = fake
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ");
    std::fs::write(
        e.dir.path().join("changed"),
        format!("[127.0.0.1]:{} {fake_key}\n", e.ssh_port),
    )
    .unwrap();
    let err = open_with(
        &tunnel_cfg(&e, password(), HostKeyPolicy::AcceptNew),
        &secrets(Some(SSH_PASSWORD), None),
        pg,
        5432,
        WAIT,
        known(&e, "changed"),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("changed"), "{err}");

    // The SSH agent signs in with keys it holds (ssh-agent from the system).
    let sock = e.dir.path().join("agent.sock");
    let mut agent = Command::new("ssh-agent")
        .args(["-D", "-a"])
        .arg(&sock)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..50 {
        if sock.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let added = Command::new("ssh-add")
        .arg(&e.key)
        .env("SSH_AUTH_SOCK", &sock)
        .output()
        .unwrap();
    assert!(added.status.success(), "{added:?}");
    let agent_result = {
        let cfg = tunnel_cfg(&e, SshAuth::Agent, HostKeyPolicy::Strict);
        let none = secrets(None, None);
        gazabridge_ssh::with_agent_socket(&sock, || {
            open_with(&cfg, &none, pg, 5432, WAIT, known(&e, "known_hosts"))
        })
        .await
    };
    let _ = agent.kill();
    let _ = agent.wait();
    let t = agent_result.unwrap();
    assert_eq!(query_through(t.local_addr()).await, 42);
}
