# GazaBridge

**Connect. Explore. Transform. Migrate. Sync.**

GazaBridge is a secure, cross-platform desktop application for exploring,
querying, converting, migrating, validating and synchronizing data between
PostgreSQL-, MySQL- and MongoDB-family databases, SQLite and common file
formats. It runs entirely on your machine: there is no account and no hosted
service.

> **Project status (2026-10-29):** the priority track (P1–P12: MongoDB and
> MongoDB ⇄ PostgreSQL) is complete and tested against real servers —
> connections, explorer and editor, Query Studio, file export/import,
> whole-database copies, conversions, dry runs and validation, backups and
> continuous sync. MySQL, monitoring/reports, scheduling, bidirectional sync,
> the CLI and installers are still ahead. **See
> [docs/STATUS.md](docs/STATUS.md)** for what is done, partial and not
> started, and [docs/roadmap.md](docs/roadmap.md) for the detailed plan.

## What works today

| Area | Status |
|---|---|
| Canonical value model with loss classification and strict/approved conversion policies | Implemented, property-tested |
| Connector SDK v1.0 (capability manifests, read/write/DDL/query/CDC contracts) | Implemented |
| Connector contract test-suite (25 cases) | Implemented; SQLite passes |
| SQLite connector (streaming keyset reads with resume, batched writes with per-record failure isolation, upsert, transactions, DDL, query, explain, read-only enforced by the database) | Implemented, *Beta* |
| Compatibility profiles for 18 products (PostgreSQL, Neon, Supabase, Citus, EDB, CockroachDB, MySQL, MariaDB, Percona, TiDB, MongoDB Community/Enterprise/Atlas, DocumentDB, SQLite, and generic profiles) | Catalogued; every profile is *Experimental* or weaker until contract tests pass |
| OS keychain / session / environment secrets, redaction | Implemented |
| Local SQLite metadata store (WAL, foreign keys, migrations, backup, integrity check) | Implemented |
| Structured, size-rotated, redacted JSON logs and a log reader | Implemented |
| Desktop shell (Tauri 2 + Leptos): navigation, command palette, light/dark/high-contrast themes, English and Arabic (RTL), connector catalogue, log viewer, settings | Implemented; modules not yet built are shown disabled |
| PostgreSQL connector (TLS modes, server-side cancel, exact NUMERIC decoding, keyset streaming, savepoint-isolated batches) | Implemented, *Beta* (contract suite passed on 13–17) |
| MongoDB connector (exact Decimal128, byte-identical copies of exotic BSON types, type-safe resumable reads, schema inference, snapshots and drift) | Implemented, *Beta* (contract suite passed on 6.0–8.0) |
| Connection editor, explorer and document editor, Query Studio, exports (JSON/JSONL/BSON/CSV/Excel) and imports | Implemented |
| Copies and conversions (MongoDB ⇄ MongoDB, MongoDB ⇄ PostgreSQL), dry runs, validation, backups, continuous sync | Implemented (priority track P8–P12) |
| MySQL connector, monitoring and reports, scheduling, bidirectional sync, CLI, installers | Not yet implemented |

## Repository layout

```
apps/desktop/src-tauri   Tauri 2 backend (commands, app state)
apps/desktop/ui          Leptos UI compiled to WebAssembly
crates/gazabridge-domain         Canonical values, types, conversions, jobs, connections
crates/gazabridge-plugin-sdk     Versioned connector API
crates/gazabridge-connectors     Connector registry + compatibility-profile catalogue
crates/gazabridge-secrets        Keychain/session/env secret stores, redaction
crates/gazabridge-storage        SQLite metadata store
crates/gazabridge-observability  Logging, rotation, redaction, log reader
crates/gazabridge-testkit        Connector contract suite and fixtures
crates/gazabridge-ipc            Types shared by the desktop backend and UI
crates/gazabridge-connector-sqlite    SQLite connector
crates/gazabridge-connector-postgres  PostgreSQL-family connector
crates/gazabridge-connector-mongodb   MongoDB-family connector
crates/gazabridge-application         Use-case services (connections, schema)
database-migrations/metadata     Metadata schema migrations
docs/                            Architecture, ADRs, threat model, guides
```

## Building from source

Requirements: Rust stable (1.90 or newer), and on Linux the WebKitGTK and
Secret Service development packages:

```bash
sudo apt-get install -y libwebkit2gtk-4.1-dev libgtk-3-dev librsvg2-dev libdbus-1-dev pkg-config
```

Run the quality gates:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo audit
```

Build and run the desktop app (needs the `wasm32-unknown-unknown` target,
[Trunk](https://trunkrs.dev) and the [Tauri CLI](https://tauri.app)):

```bash
rustup target add wasm32-unknown-unknown
cargo install trunk tauri-cli --locked
cd apps/desktop/src-tauri && cargo tauri dev
```

Without the Tauri CLI, build the UI bundle and a release binary that embeds it:

```bash
(cd apps/desktop/ui && trunk build --release)
cargo build --release -p gazabridge-desktop --features custom-protocol
target/release/gazabridge-desktop
```

On macOS with Homebrew Rust (which has no wasm target), install `rustup`
(`brew install rustup trunk`) and put `/opt/homebrew/opt/rustup/bin` first in
`PATH` for these commands.

To get a normal Mac application (`GazaBridge.app` with its icon, which you
can move to Applications and open from Launchpad or the Dock):

```bash
scripts/build-mac-app.sh
```

A release build compiles about 800 crates with link-time optimisation, so it
keeps the CPU busy (and warm) for a few minutes. The script runs at low
priority on half the cores; set `JOBS` to change that. For day-to-day work
use `cargo tauri dev` or debug builds, which rebuild only what changed.

Integration tests that need Docker (Testcontainers) or the OS keychain are
marked `#[ignore]` and run in CI; locally:

```bash
GAZABRIDGE_INTEGRATION=1 cargo test --workspace -- --ignored
```

## Security

Credentials are never written to GazaBridge's own files: connection metadata
holds only references into the OS keychain, the in-memory session store or
environment variables, and the metadata schema rejects plaintext passwords.
Logs and error messages are redacted. Telemetry does not exist. See
[SECURITY.md](SECURITY.md) and [docs/threat-model.md](docs/threat-model.md).

## Documentation

- [Architecture](docs/architecture.md) and [ADRs](docs/adr/)
- [Canonical data model](docs/canonical-data-model.md)
- [Connector guide and capability model](docs/connector-guide.md)
- [Metadata schema and recovery](docs/metadata-schema.md)
- [Threat model](docs/threat-model.md)
- [Requirements traceability](docs/requirements-traceability.md)
- [Implementation backlog](docs/backlog.md)
- [Roadmap for the remaining work](docs/roadmap.md)

## License

Apache-2.0. See [LICENSE](LICENSE).
