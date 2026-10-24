# Architecture

GazaBridge is a Rust Cargo workspace organised along hexagonal (ports and
adapters) lines. The engine never depends on the desktop UI, so the same
crates can later power a CLI, a headless worker, a daemon or a REST API.

## Layers

```
            ┌────────────────────────────┐   ┌──────────────────────┐
 Adapters   │ apps/desktop (Tauri+Leptos)│   │ gazabridge-cli (M10) │
 (drivers)  └─────────────┬──────────────┘   └──────────┬───────────┘
                          │  gazabridge-ipc DTOs         │
            ┌─────────────▼─────────────────────────────▼───────────┐
 Application│ gazabridge-application (M2+): use cases, job runner   │
            │ migration · sync · validation · scheduler · reporting │
            └─────────────┬─────────────────────────────┬───────────┘
                          │ ports                        │ ports
            ┌─────────────▼──────────────┐   ┌──────────▼───────────┐
 Domain     │ gazabridge-domain          │   │ gazabridge-plugin-sdk│
            │ values · types · conversion│   │ Connector traits,    │
            │ jobs · connections         │   │ capabilities, CDC    │
            └────────────────────────────┘   └──────────▲───────────┘
                                                        │ implements
            ┌────────────────────────────┐   ┌──────────┴───────────┐
 Infra      │ storage · secrets ·        │   │ connector-postgres / │
 (driven)   │ observability              │   │ mysql / mongodb /    │
            │                            │   │ sqlite · files       │
            └────────────────────────────┘   └──────────────────────┘
```

| Crate | Responsibility | Depends on |
|---|---|---|
| `gazabridge-domain` | Canonical values and types, loss-aware conversion, job state machine, connection profiles, connection-string parsing/redaction | serde, chrono, bigdecimal, uuid |
| `gazabridge-plugin-sdk` | Versioned connector API: `ConnectorFactory`, `Connection`, `Transaction`, capability manifests, read/write/DDL/query/CDC types, compatibility profiles | domain |
| `gazabridge-connectors` | Connector registry; compatibility-profile catalogue (`profiles.toml`) with validation and server detection | sdk |
| `gazabridge-secrets` | `SecretStore` trait; OS keychain, session and environment stores; `SecretResolver`; `Redactor` | domain, keyring |
| `gazabridge-storage` | SQLite metadata store with migrations and repositories | domain, sqlx |
| `gazabridge-observability` | `tracing` setup, size-rotating redacting writer, retention, log reader | secrets |
| `gazabridge-testkit` | Connector contract suite and round-trip fixtures | sdk |
| `gazabridge-ipc` | Serializable DTOs shared by the Tauri backend and the wasm UI | serde |
| `apps/desktop/src-tauri` | Tauri commands and application state | all infrastructure crates |
| `apps/desktop/ui` | Leptos client-side UI | ipc |

Crates listed in the specification but not yet present
(`gazabridge-core`, `-application`, `-connector-*`, `-files`, `-schema`,
`-mapping`, `-transform`, `-migration`, `-sync`, `-replication`,
`-validation`, `-jobs`, `-scheduler`, `-reporting`, `-cli`) are created in
the milestone that implements them, so the workspace never contains empty
crates. See [ADR 0001](adr/0001-workspace-and-layering.md).

## Data flow of a migration (target design)

```
source Connection::read ──► RecordBatch stream ──► bounded channel
   (streaming, resumable)        │                      │ backpressure
                                 ▼                      ▼
                         mapping + transform (parallel, deterministic)
                                 │ canonical Records, LossRecords
                                 ▼
                    convert() to target column types (policy-checked)
                                 │
                                 ▼
             target Connection::write_batch ──► WriteOutcome
                                 │                  │ record failures
                                 ▼                  ▼
             checkpoint (after durable write)   quarantine file
```

Key properties:

- **Streaming and bounded memory.** Sources yield bounded `RecordBatch`es;
  stages communicate over bounded channels.
- **Checkpoints after durability.** A checkpoint is saved only after the
  target confirms the batch, so a crash replays at most one batch; writes are
  idempotent (upsert/skip-existing) or deduplicated by key.
- **Config fingerprints.** Each run records a hash of source, target, mapping
  and transformation configuration; resuming requires the same fingerprint
  (`JobRepo::verify_resumable`).
- **Loss is explicit.** Every conversion goes through
  `gazabridge_domain::conversion::convert` with a `ConversionPolicy`.

## Desktop process model

The Tauri backend owns all I/O. The WebView loads a static Leptos bundle and
calls named commands (`gazabridge_ipc::commands`). The WebView has no
filesystem, shell or HTTP plugins; its CSP forbids remote content. Long
operations will run on Tokio tasks and stream progress events to the UI,
derived from engine events, never timers.

## Local state

| What | Where |
|---|---|
| Metadata (settings, connections, jobs, runs, checkpoints, history, audit) | `<app data>/metadata.db` (SQLite, WAL) |
| Secrets | OS keychain / process memory / environment |
| Logs | `<app log dir>/gazabridge.log[.N]` (JSON lines, redacted) |
| Quarantined records | files under `<app data>/quarantine/` (planned, M6) |

## Cross-cutting decisions

See the ADRs in [docs/adr](adr/):

1. [Workspace and layering](adr/0001-workspace-and-layering.md)
2. [Canonical, loss-aware value model](adr/0002-canonical-value-model.md)
3. [Capability-manifest connector SDK](adr/0003-connector-sdk-capabilities.md)
4. [Compatibility profiles and honest status](adr/0004-compatibility-profiles.md)
5. [Secrets only by reference](adr/0005-secrets-by-reference.md)
6. [SQLite metadata store with sqlx](adr/0006-sqlite-metadata-store.md)
7. [Tauri 2 + Leptos desktop UI](adr/0007-tauri-leptos-ui.md)
8. [Redaction at the log writer](adr/0008-redaction-at-writer.md)
