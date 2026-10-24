# Implementation backlog

Ordered by milestone. Each milestone ends with fmt, clippy, tests, audit,
updated docs and ADRs. The detailed plan (order of work, open decisions,
definitions of done, dependencies, risks, sizes) is in
[roadmap.md](roadmap.md).

## Milestone 1 — Foundations ✅

- [x] Cargo workspace, toolchain, lints, rustfmt/clippy config
- [x] Domain: ids, job state machine, write modes, connection profiles, connection-string parsing and redaction
- [x] Canonical value and type model; loss classification; strict/approved conversion; property tests
- [x] Connector SDK v1.0 and capability manifests
- [x] Compatibility-profile catalogue (18 profiles), validation, detection, assessment
- [x] Secrets: keychain/session/env stores, resolver, redactor
- [x] SQLite metadata schema v1 and repositories (settings, connections, folders, tags, secret index, jobs, runs, events, checkpoints, saved queries, history, audit)
- [x] Logging: JSON, rotation, retention, redaction, reader
- [x] Contract test suite and round-trip fixtures
- [x] Desktop shell: Tauri backend commands, Leptos UI (nav, palette, themes, en/ar RTL, catalogue, logs, settings)
- [x] Branding and icons
- [x] CI pipeline; cargo-deny; gitleaks; audit config
- [x] Architecture, ADRs, threat model, traceability matrix

## Milestone 2 — Connections and connectors

- [ ] `gazabridge-application` crate: connection service (create/edit/duplicate/delete/test, secret handling, audit)
- [x] `gazabridge-connector-sqlite` (sqlx): full manifest, type mapping, streaming with key-based resume, upsert, transactions, DDL, query/explain, read-only via `SQLITE_OPEN_READONLY` + `query_only`
- [x] Run the contract suite on SQLite against real temporary files (25/25), record evidence, promote profile to Beta
- [ ] Apply compatibility-profile capability overrides to live connection manifests (application layer)
- [x] `gazabridge-connector-postgres` (tokio-postgres + deadpool): libpq-style TLS modes on rustls, client certificates, session setup (UTC, ISO, search path, statement timeout, read-only), server-side cancel, catalog inspection, exact binary NUMERIC/interval/array decoding, text-format parameters, keyset streaming, savepoint-isolated batches, upsert counts, explain/analyze (always rolled back)
- [x] Run the PostgreSQL contract suite (13–17) against real servers, record evidence, promote `postgres.postgresql` to Beta
- [ ] PostgreSQL: PostGIS geometry as canonical geometry (currently an extension value), encrypted client keys
- [ ] `gazabridge-connector-mysql`: TLS, charset, SQL-mode detection, binlog-capable driver evaluation (ADR)
- [ ] `gazabridge-connector-mongodb` (official driver): URI/hosts, TLS, read preference, sampling inference, Decimal128 handling
- [ ] Testcontainers harnesses (PostgreSQL, MySQL, MariaDB, MongoDB replica set)
- [ ] SSH tunnels (host-key verification, key/agent auth)
- [ ] Server detection → `connector_metadata`; connection states in UI
- [ ] Connection editor UI (structured fields + connection string), test button with latency, health
- [ ] Record contract evidence; promote profiles that pass

## Milestone 3 — Explorer, Data Viewer/Editor, Query Studio, Schema Studio

- [ ] Explorer tree for all object kinds (views, sequences, enums, routines, triggers, partitions)
- [ ] Virtualized data grid, server-side paging/filter/sort, JSON and tree views, null/missing display, copy
- [ ] Data editor with preview, generated statements, transactions, optimistic concurrency, audit, destructive-operation guards
- [ ] Query Studio editor (ADR: Rust editor vs. Monaco), tabs, autocomplete, parameters, formatting, timeout, cancel, explain/analyze, dangerous-query detection, transaction mode
- [ ] Schema Studio: ER diagrams, inferred structures, schema diff/drift, DDL preview, rollback scripts

## Milestone 4 — Files

- [ ] `gazabridge-files`: CSV/TSV, JSON, JSONL/NDJSON, XLSX (calamine/rust_xlsxwriter), Parquet/Arrow (arrow-rs), SQLite files, BSON
- [ ] gzip/zstd/ZIP with ratio/size limits and path-traversal protection
- [ ] Type inference, malformed-row policies, exact line numbers
- [ ] Export options: splitting, encoding, masking, disk-space checks, no-overwrite

## Milestone 5 — Mapping and transformations

- [ ] `gazabridge-mapping` and `gazabridge-transform` with versioning, templates, preview
- [ ] Sandboxed expression language
- [ ] Document → relational (JSON/flatten/normalize/hybrid) and relational → document (embed/reference) strategies

## Milestone 6 — Migration engine

- [ ] Streaming pipeline with bounded channels, adaptive batching, rate limiting, retries, quarantine files
- [ ] Pause/resume/cancel with checkpoints; resume across restarts
- [ ] Live monitoring from engine events

## Milestone 7 — Dry Run, validation, reports, backups

- [ ] Dry Run with zero-mutation proof tests
- [ ] Basic/standard/deep validation, canonical chunk checksums, diff reports
- [ ] Reports (JSON/HTML/Markdown/CSV, PDF if reliable)
- [ ] Native tool integration with sanitized previews

## Milestone 8 — Scheduling and incremental sync

- [ ] Scheduler (cron, intervals, missed-run/overlap policies, time zones)
- [ ] MongoDB change streams, PostgreSQL logical replication, MySQL binlog, polling fallbacks

## Milestone 9 — Bidirectional sync

- [ ] Identity, origin metadata, loop prevention, conflicts, tombstones, resolution UI, compatibility matrix

## Milestone 10 — CLI, hardening, packaging

- [ ] `gazabridge` CLI with JSON output and stable exit codes
- [ ] Criterion benchmarks, profiling docs
- [ ] Installers, code signing, notarization, signed updates, SBOM, release notes
- [ ] Remaining guides (user, admin, CLI, troubleshooting, packaging, release, data-loss risk)
