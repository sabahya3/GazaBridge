# Requirements traceability matrix

Maps each section of the product specification to its implementation status.
Updated at the end of every milestone.

Legend: **Done** — implemented and tested; **Partial** — foundation in
place, more work scheduled; **Not started**. "M*n*" is the milestone that
completes the item (see [backlog.md](backlog.md)).

_Last updated: Milestone 2 (in progress), 2026-10-24._

| § | Requirement | Status | Milestone | Evidence / notes |
|---|---|---|---|---|
| 1 | Supported platforms and installers | Partial | M10 | Tauri bundle targets configured (`tauri.conf.json`: app, dmg, nsis, msi, deb, rpm, AppImage); CI runs Linux, macOS Intel + Apple Silicon, Windows. No installers produced yet. |
| 1 | Local-only operation; engine reusable headless | Done (design) | — | Engine crates have no UI dependency (ADR 0001). |
| 2 | Required technology | Partial | M2–M10 | Rust stable, Cargo workspace, Tauri 2, Tokio, Leptos, SQLite, sqlx, serde, tracing, thiserror, keyring in use. MongoDB driver, Testcontainers, Criterion arrive with their milestones. |
| 3 | 20 product modules | Partial | M2–M10 | Settings, Logs (viewer), connector catalogue, SDK done; remaining modules not started. |
| 4 | Supported databases + compatibility profiles | Partial | M2 | 18 profiles in `crates/gazabridge-connectors/profiles.toml` with all required fields; detection and assessment implemented and tested. **SQLite connector implemented (Beta, contract evidence for 3.46). PostgreSQL connector implemented; contract suite passed on PostgreSQL 13–17 (Beta). Hosted and derivative PostgreSQL profiles stay Experimental until tested. MongoDB connector implemented; contract suite passed on MongoDB 6.0–8.0 (Beta). MySQL connector drafted, paused.** |
| 4 | Status vocabulary; no untested Production Ready | Done | — | `CompatibilityStatus`; catalogue validation tests reject `production_ready` without evidence. |
| 5 | File formats | Not started | M4 | |
| 6 | Workspace architecture | Partial | M2–M10 | 10 crates; remaining crates created per milestone (ADR 0001). |
| 7 | Connector SDK | Done (API) | M2 (impls) | `gazabridge-plugin-sdk`: capabilities, manifests, versioning, read/write/DDL/query/explain/CDC/transactions; 25-case contract suite in `gazabridge-testkit`, passed by SQLite. |
| 8 | Canonical data model, loss classes, no silent loss | Done | — | `gazabridge-domain` value/types/conversion; 49 unit + property tests. |
| 9 | Connection management | Partial | M2 | Profile model for all four engines with every listed setting; folders, tags, colours, favourites, search, recent, health storage, duplicate, connection-string parsing. UI create/edit/test and connectors pending. |
| 10 | Secret management | Partial | M2 | Keychain/session/env stores, resolver, redaction, zeroization, schema-level plaintext rejection, secret cleanup index. Credential replacement UI pending. |
| 11 | Database Explorer & Data Viewer | Not started | M3 | Bounded `PageRequest` API exists. |
| 12 | Data Editor | Partial (API) | M3 | `UpdateRequest`/`DeleteRequest` with restrictive filters and optimistic concurrency; contract suite checks guards. |
| 13 | Query Studio | Partial (storage) | M3 | Saved queries and history repositories. |
| 14 | Schema Studio | Not started | M3 | |
| 15 | MongoDB schema inference | Done | — | Sampled/random/full-scan inference with presence %, type and array-item distributions, mixed and conflicting paths, max depth; snapshots and drift detection (`SchemaService`). |
| 16 | Schema mapping | Not started | M5 | Versioned `mappings` table exists. |
| 17 | Transformation engine | Not started | M5 | Versioned `transformations` table exists. |
| 18–19 | Document ⇄ relational conversion | Not started | M5 | |
| 20 | Migration engine | Partial (foundations) | M6 | Write modes, checkpoints with fingerprints, runs, events, error classification for retry/quarantine. |
| 21–22 | File import/export | Not started | M4 | |
| 23 | Dry Run | Partial | M7 | `plan_ddl` contract is side-effect free (contract-tested); job edits invalidate Dry Run (tested). |
| 24 | Validation engine | Not started | M7 | |
| 25 | Job management and states | Partial | M6 | All 13 states with validated transitions (domain + storage compare-and-set); clone, archive, run history, checkpoints survive restart (tested). |
| 26 | Incremental sync and CDC | Partial (API) | M8 | `ChangeFeedRequest`/`ChangeEvent`/`SyncMethod` labels. |
| 27 | Bidirectional sync | Not started | M9 | |
| 28 | Scheduler | Partial (storage) | M8 | `schedules` table. |
| 29 | Monitoring | Not started | M6 | |
| 30 | Backup and recovery | Partial | M7 | Metadata backup (`VACUUM INTO`) and integrity check; recovery documented. |
| 31 | Local metadata storage | Done | — | `gazabridge-storage`, migrations, WAL, FKs, busy timeout, corruption detection, backup; 15 tests. |
| 32 | Logging and observability | Partial | M6/M10 | Structured JSON logs, size rotation, retention, redaction, correlation fields, log viewer. Metrics and diagnostic bundle pending. |
| 33 | Reporting | Not started | M7 | `reports` table. |
| 34 | Security | Partial | M10 | See [threat-model.md](threat-model.md); dependency/license audits and secret scanning in CI. |
| 35 | User interface | Partial | M3+ | Shell with all 15 nav items (unbuilt ones disabled), command palette (Ctrl/⌘+K), light/dark/high-contrast, English + Arabic RTL with catalogue sync test, keyboard navigation, skip link, ARIA labels, responsive layout. |
| 36 | Connector catalogue | Partial | M2 | Catalogue UI lists profiles by family with status, tested versions, limitations, auth, CDC availability. Connection states defined; external-tool detection pending. |
| 37 | Branding | Done | — | `docs/branding/`: mark, monochrome, light and dark wordmarks; generated app icons. |
| 38 | CLI | Not started | M10 | |
| 39 | Performance and benchmarks | Not started | M6/M10 | |
| 40 | Testing | Partial | ongoing | Unit, property and real-SQLite tests; contract suite run on real SQLite files; SQLite behaviour tests (DB-level read-only, failure isolation, atomic rollback, upsert counts, defaults vs null, NaN/u64/bigint/JSON/duration round trips, views, explain, inspection). Testcontainers integration with the network connectors. |
| 41 | Packaging and updates | Not started | M10 | |
| 42 | Documentation | Partial | ongoing | README, SECURITY, CONTRIBUTING, architecture, 8 ADRs, threat model, canonical model, connector guide, metadata schema, traceability, backlog. |
| 43 | Quality gates | Done (M1 scope) | — | fmt, clippy `-D warnings`, tests and `cargo audit` pass locally; CI adds cargo-deny (licenses, sources), gitleaks, MSRV check. |
| 44 | Milestones | In progress | — | Milestone 1 complete. |
