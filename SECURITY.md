# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's "Report a
vulnerability" (security advisories) on this repository. Do not open public
issues for security problems. Include a description, affected versions,
reproduction steps and impact. We aim to acknowledge reports within three
working days.

Never include real credentials, connection strings or production data in a
report. Use synthetic data.

## Supported versions

GazaBridge is pre-1.0. Only the latest release receives security fixes.

## Security properties

These are design commitments; tests enforce the ones marked *(tested)*.

- **No plaintext credentials at rest.** Connection metadata stores
  `SecretRef`s pointing to the OS keychain (macOS Keychain, Windows
  Credential Manager, Linux Secret Service), the in-memory session store or
  an environment variable. The metadata schema rejects a plaintext
  `password` value with a `CHECK` constraint. *(tested)*
- **Redaction.** Log lines, command errors and driver messages pass through
  a redactor that masks URI passwords, `password=`/`token=`-style fragments
  and every secret value resolved during the session. *(tested, including a
  property test)*
- **Secrets are zeroized** in memory when dropped (`zeroize`).
- **Secrets never appear in process arguments.** Native tools (when added)
  receive passwords through environment variables or password files with
  restrictive permissions.
- **Secure TLS defaults:** PostgreSQL `verify-full`, MySQL
  `VERIFY_IDENTITY`, MongoDB TLS on; SSH host keys strictly checked. *(tested)*
- **No telemetry.** GazaBridge has no telemetry code.
- **Least-privilege WebView.** The Tauri WebView has only `core:default`
  permissions, a strict CSP and a frozen prototype; all data access goes
  through Rust commands.
- **Read-only connections** remove every mutating capability before any
  operation is attempted; the connector contract suite verifies refusal.
- **No silent data loss.** Lossy conversions fail unless an explicit policy
  approves them, and approved losses are recorded. *(tested, property test)*

See [docs/threat-model.md](docs/threat-model.md) for the full threat model.
