# Threat model

Scope: the GazaBridge desktop application and CLI running on a user's
workstation, connecting to databases and files the user configures.
Method: STRIDE per asset and trust boundary. Status column: **M** =
mitigated in code with tests, **P** = planned (milestone), **D** = documented
residual risk.

## Assets

| Asset | Why it matters |
|---|---|
| Database credentials, SSH keys, TLS client keys | Grant access to production data |
| Data in transit between source and target | May be personal or regulated |
| Target databases | Destructive jobs can delete or overwrite data |
| Local metadata (connection hosts, queries, job definitions) | Reveals infrastructure; query history may contain literals |
| Logs, reports, diagnostic bundles | Often shared with others |
| Update channel | Compromise would execute code on every install |

## Trust boundaries

1. WebView (Leptos UI) ↔ Rust backend (Tauri IPC).
2. Application ↔ OS secret store.
3. Application ↔ remote database servers / SSH bastions.
4. Application ↔ local files the user imports (untrusted content).
5. Application ↔ native tools (`pg_dump`, `mongodump`, …).
6. Application ↔ update server.

## Threats and mitigations

### Spoofing

| Threat | Mitigation | Status |
|---|---|---|
| Man-in-the-middle on database connections | TLS verification on by default (`verify-full`, `VERIFY_IDENTITY`, MongoDB TLS); weaker modes must be chosen explicitly and are flagged in the UI | M (defaults tested), P (UI flagging, M2) |
| SSH host impersonation | `HostKeyPolicy::Strict` default; `AcceptNew` records first key and rejects changes | M (default), P (tunnel, M2) |
| Malicious update server | Signed updates only; signature verified before install; never install unsigned | P (M10) |

### Tampering

| Threat | Mitigation | Status |
|---|---|---|
| SQL injection via identifiers or filter values | Values always bound as parameters; identifiers quoted by connector `quote_identifier`, rejecting NUL/empty; contract suite tests hostile identifiers | M (contract), P (per connector, M2) |
| Malicious import files (zip bombs, path traversal, huge rows) | Decompression ratio and size limits, entry-name validation, streaming parsers with per-record size limits | P (M4) |
| Corrupted metadata database | `integrity_check` and `foreign_key_check` at start-up; backups via `VACUUM INTO`; STRICT tables and JSON validity checks | M |
| Resuming a job after configuration changed | Run config fingerprint must match the checkpoint (`verify_resumable`) | M |

### Repudiation

| Threat | Mitigation | Status |
|---|---|---|
| Unclear who ran a destructive operation | Local audit log of connection, data-edit and job actions with correlation ids | M (store), P (wiring, M3/M6) |

### Information disclosure

| Threat | Mitigation | Status |
|---|---|---|
| Plaintext credentials in metadata | Profiles hold `SecretRef`s only (type-enforced); schema `CHECK` rejects plaintext passwords | M |
| Credentials in logs, errors, reports | Writer-level redaction of URI passwords, `key=value`/JSON secrets and all resolved secret values; driver errors redacted at construction | M |
| Credentials in process arguments (native tools) | Pass via environment or 0600 password files; sanitized command preview | P (M7) |
| Secrets lingering in memory | `Zeroizing` strings for resolved secrets, parsed passwords and session store | M |
| Record data in logs | Coding rule: log counts/operations only; `LossRecord` and conversion errors carry kinds, never values | M (types), D (review) |
| Query history reveals literals | History is local, prunable, and can be disabled (setting) | M (prune), P (toggle, M3) |
| Telemetry leakage | No telemetry exists; any future telemetry must be opt-in and exclude names, queries, data, PII | M |
| WebView compromise reads files | No fs/shell/http plugins; `core:default` only; strict CSP; frozen prototype | M |
| Sensitive fields in exports | Masking/redaction transformations, export-time masking option | P (M5) |

### Denial of service

| Threat | Mitigation | Status |
|---|---|---|
| Unbounded queries freezing the UI or exhausting memory | Pages are always bounded (`PageRequest::MAX_LIMIT`), streaming reads, bounded channels | M (API), P (engine, M6) |
| Runaway migrations overloading production | Rate limiting, bounded concurrency, cancellation tokens | P (M6) |

### Elevation of privilege

| Threat | Mitigation | Status |
|---|---|---|
| Accidental writes through a "read-only" connection | Read-only manifests strip mutating capabilities; connectors refuse writes; contract suite verifies; session-level read-only where the engine supports it | M (manifest + contract), P (per connector) |
| Destructive jobs run without review | Dry Run mandatory for new destructive/cross-engine jobs; edits reset the Dry Run; confirmation dialogs | M (state), P (Dry Run, M7) |
| Custom transformation expressions escaping the sandbox | Expression language without filesystem/network access; deterministic evaluation | P (M5) |
| Malicious third-party connector | Plugins are compiled Rust with full process rights; only install trusted connectors; API version checks | D |

## Residual risks

- A compromised user account or workstation can read the OS keychain entries
  GazaBridge created (the OS controls access).
- Data written to a target is outside GazaBridge's control once committed.
- Users can explicitly choose weak TLS modes or approve lossy conversions.
