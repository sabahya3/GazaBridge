# Roadmap for the remaining work

This is the detailed plan for everything after Milestone 1. The checklist view
lives in [backlog.md](backlog.md); per-requirement status lives in
[requirements-traceability.md](requirements-traceability.md). This document
adds the order of work, the technical decisions still open, what "done" means
for each milestone, dependencies, risks and a rough size.

_Written: 2026-10-24, during Milestone 2._

## How to read the sizes

Sizes are relative, not calendar estimates:

| Size | Meaning |
|---|---|
| S | One focused work session |
| M | 2–3 sessions |
| L | 4–6 sessions |
| XL | 7+ sessions; split before starting |

## Ground rules that apply to every milestone

1. Each milestone ends with: `cargo fmt --check`, `cargo clippy -D warnings`,
   `cargo test --workspace`, `cargo audit`, updated backlog and traceability
   matrix, and an ADR for every new technical decision.
2. A connector feature is "done" only when it passes the shared contract
   suite against a real server or file. Profiles are promoted only with
   recorded evidence (enforced by the catalogue tests).
3. Any operation that can lose data goes through
   `gazabridge_domain::conversion` and a `ConversionPolicy`.
4. No UI element ships before its backend works; unfinished sections stay
   disabled.

## Environment prerequisites (blocking verification today)

| Need | Why | Options |
|---|---|---|
| ✅ A PostgreSQL server | Run the PostgreSQL contract suite | Provided by Testcontainers on Colima |
| ✅ Docker (Colima) | Testcontainers for PostgreSQL, MySQL, MariaDB, MongoDB | Installed 2026-10-24; run `colima start` after a reboot |
| ✅ `rustup` with `wasm32-unknown-unknown`, Trunk | Build and run the desktop UI | Installed 2026-10-25 (`brew install rustup trunk`); release app runs; Tauri CLI not installed (not required, see README) |

Until these exist locally, network connectors can only be verified in CI.

## Dependency map

```
M2 connectors ─┬─► M3 explorer / editor / query / schema studio
               ├─► M4 files ─────────┐
               └─► M5 mapping/xform ─┴─► M6 migration engine ─► M7 dry run, validation, reports
                                                          └─► M8 scheduler + incremental sync ─► M9 bidirectional sync
M10 CLI / packaging can start once M6 exists (CLI wraps the same services).
```

`gazabridge-application` (use-case services shared by desktop and CLI) is
introduced in Milestone 2 and grows with every milestone after it.

---

## Priority track — MongoDB and MongoDB ⇄ PostgreSQL (decided 2026-10-25)

The project owner asked to finish everything MongoDB-related first —
connection, viewing, editing, querying, import/export, copying, backup and
restore, and continuous sync — including transfers between MongoDB and
PostgreSQL. The tasks below are taken from milestones 2–8 and done **first**,
in this order. They are built as generic pieces (so later engines reuse
them) but delivered and tested for MongoDB and PostgreSQL. After the track,
work returns to the remaining milestone tasks (starting with 2.3 MySQL,
paused as committed draft code).

| # | Task (origin) | Size | Status |
|---|---|---|---|
| ✅ P1 | MongoDB connector: URI/SRV/Atlas, replica sets, TLS, auth, list databases/collections, streaming reads resumable by `_id`, batched writes with per-document failures, upsert, transactions on replica sets, null vs missing, ObjectId/Decimal128 fidelity, aggregation and filter execution, explain (2.5) | L | **done 2026-10-25** — server-side kill of running aggregations deferred; cancellation stops reads and drops cursors |
| ✅ P2 | MongoDB contract runs on single-node replica sets 6.0, 7.0, 8.0; record evidence, promote `mongodb.community` (2.6) | S | **done 2026-10-25** — passed on 6.0.28, 7.0.45, 8.0.34; profile is Beta |
| ✅ P3 | Schema inference: sampling (`$sample` with fallback), field occurrence %, type distributions, mixed types, max depth, conflicting paths; saved snapshots (§15) | M | **done 2026-10-25** — plus drift detection between snapshots |
| ✅ P4 | Connection editor UI and connection commands (structured + connection-string), test with latency, health, catalogue states (2.8, subset) | M | **done 2026-10-25** — commands covered by backend tests; UI built and rendered on 2026-10-25 |
| ✅ P5 | Explorer tree, data viewer (table / JSON / tree, paging, filter, sort, null vs missing display) and document editor (insert/edit/delete, `_id`-safe updates, preview, confirmation, audit) for MongoDB and PostgreSQL (3.1–3.3, subset) | L | **done 2026-10-25** — integration-tested on MongoDB 8.0 and PostgreSQL 16 (browse, filter, sort, count, insert, edit with preview, conflict detection, delete, audit); UI rendered and exercised against a stubbed backend; PostgreSQL browses only the connection's own database; grid is paged, not virtualized |
| ✅ P6 | Query Studio for MongoDB filters/aggregations and PostgreSQL SQL: tabs, history, saved queries, params, cancel, explain (3.4, subset) | L | **done 2026-10-25** — integration-tested on PostgreSQL 16 and MongoDB 8.0 (params, row limit, cancel, timeout, explain / explain analyze rolled back, history, saved queries); destructive statements (DROP, TRUNCATE, DELETE/UPDATE without WHERE, `$out`/`$merge`) need confirmation; plain text editor without syntax highlighting (question 4 still open); tabs are not kept across restarts; UI rendered and exercised against a stubbed backend |
| ✅ P7 | Files for MongoDB: JSON, JSONL/NDJSON, BSON (mongodump format), CSV export; gzip; streaming; malformed-line policies (4.2–4.3, 4.6–4.7, subset) | M | **done 2026-10-25** — new crates `gazabridge-files` and `gazabridge-bson`; export/import panel in the Explorer with native file dialogs, progress, cancel and audit; integration-tested on MongoDB 8.0 (exact copies via BSON and canonical Extended JSON, quarantine, upsert, cancel) and PostgreSQL 16 (row-exact round trip, upsert, CSV); typed and relaxed JSON do not keep BSON Int32 vs Int64 (as mongoexport); CSV import, zstd/ZIP, split output and fuzzing remain in 4.2/4.7/4.8/4.9 |
| ✅ P8 | Migration engine core (streaming pipeline, batching, checkpoints, pause/resume/cancel, retries, quarantine) with MongoDB→MongoDB collection copy including indexes (6.1–6.4, subset) | L | **done 2026-10-28** — new crate `gazabridge-migration` (bounded read→write queue, checkpoint after every acknowledged batch, idempotent resumed/retried batches, backoff on connection/timeout/transient errors, read reopened from its position, quarantine as re-importable Extended JSON, optional rate limit); `MigrationService` records jobs, runs, events, checkpoints and audit, pauses, resumes (also after failures), cancels, and turns runs interrupted by a shutdown into paused ones; plan step reads only (counts, existing targets, indexes, commands) and dropping a target needs typed confirmation; indexes recreated from the full `listIndexes` spec; Migrations page in the app. Engine tested with fault injection (dropped reads/writes, lost acknowledgement, crash between write and checkpoint, rejections); end to end between two MongoDB 8.0 servers: byte-exact documents (field order, Int32/Int64, Decimal128 incl. non-canonical, binary) and identical indexes (compound, TTL, unique+partial, text). Found and fixed on the way: skip-existing writes reordered fields (`$setOnInsert` sorts them) — now inserts that skip duplicate `_id`s; upserts are bulk `update` commands. Not yet: adaptive batch size, collection options (validators, capped, time series, collation) are not copied, Monitor page (6.7) |
| ✅ P9 | MongoDB ⇄ PostgreSQL conversion: document→relational (JSONB, flatten, normalize, hybrid) and relational→document (embed, reference), ObjectId identity mapping; mapping preview (5.4–5.5, subset) | L | **done 2026-10-28** — new crate `gazabridge-mapping` (pure planning and conversion); engine transform stage writing parents then children, tagging rows with their source record so a record rejected anywhere is removed from every table and quarantined whole; mappings are stored with the run so resumes convert identically. Documents → tables: column types from a 2,000-document sample, nested objects flattened to depth 3, arrays as JSONB or child tables `(_parent_id, _index)` with foreign keys, mixed types as JSONB, unseen fields and misfit values kept in `_extra` (nothing dropped), simple indexes recreated (partial/text/geo skipped with a warning), new schemas created. Tables → documents: primary key → `_id` (document for composite keys), one-level embedding of child tables by foreign key, `character(24)` → ObjectId, JSONB read as Extended JSON, integer → Int32, tables from the JSONB/hybrid modes restored as stored, tables without a primary key refused. Plan shows tables, columns, types and sample rows, or resulting documents. Tested end to end on MongoDB 8.0 and PostgreSQL 16 (normalize with child tables and indexes, re-run idempotent, JSONB round trip back to identical documents, embed with types and order, blockers). Limits: JSONB does not keep key order; missing and null both become NULL in typed columns; embedding is one level; PostgreSQL → PostgreSQL and MySQL are not offered yet. **2026-11-06:** flatten/normalize copies save their mapping in `_gazabridge_shapes`, and copies back to MongoDB rebuild the original documents from it (nested objects, `_extra`, arrays from child tables in order, ObjectIds, Int32), including rows added in SQL |
| ✅ P10 | Dry Run and validation for these jobs: zero-mutation proof, counts, chunk checksums, differences report (7.1–7.2, subset) | M | **done 2026-10-29** — Dry run: the whole read → convert pipeline with writes replaced by key lookups in the target, predicting inserts, kept, replaced and conflicting rows plus conversion rejections; no DDL, deletes, checkpoints or quarantine files (dropping targets is simulated as empty); a clean dry run marks the job. Validation (`gazabridge-migration::validate`): basic (exact counts within 60 s, else estimates flagged), standard (every expected row found by key; extra rows under the same owner), deep (content per lookup chunk, SHA-256 over row hashes for both sides, first differing field named); same-engine copies compare byte-exact BSON (types and field order), conversions compare normalized values; each finished copy gets an automatic count check; reports are stored with the run and shown live. Tests: engine fakes; on real servers, zero mutations proven by snapshots of every database, collection, count and index (MongoDB) and of tables and schemas (PostgreSQL), and validation finds deleted, edited, kept-different, extra and stale child rows across MongoDB and PostgreSQL. Not yet: report export (7.3), sampling-based validation for very large objects, target-side extras beyond count differences (listed only via counts) |
| ✅ P11 | Backup and restore: native export/import of collections and databases, `mongodump`/`mongorestore` and `pg_dump`/`pg_restore` integration with sanitized previews (7.4–7.5, subset) | M | **done 2026-10-29** — `BackupService`: a folder per backup (never shared), one file per object (MongoDB: gzip BSON; PostgreSQL: gzip typed JSON lines) and `manifest.json` written last with SHA-256 per file, counts, MongoDB index specifications and PostgreSQL column definitions, primary and foreign keys and indexes; failed backups leave nothing behind. Restore: same engine, into the original or a renamed database/schema (schema created, references follow), tables in foreign-key order, keep/update/stop/replace (replace confirmed), checksums verified before any write. Copies that replace a target back it up first (default on; no backup, no drop). `tools`: detection in PATH and Homebrew/Postgres.app paths with versions; passwords only in private temporary files (YAML config for MongoDB tools, `PGPASSFILE` for PostgreSQL), 0600 and deleted after the run; sanitized command previews; output captured, cancellation, exit codes; native archives checksummed. Backups page: tools, new backup, list, check and restore, delete. Tested on MongoDB 8.0 (byte-exact restore with indexes, damaged file refused, real mongodump/mongorestore with rename) and PostgreSQL 16 (rows, types, keys and indexes in a new schema). Not verified here: pg_dump/pg_restore (not installed on the build machine; covered by unit tests of the generated commands and password file); sequences are not backed up (values restored as stored); scheduled backups belong to Milestone 8 Follow-up 2026-10-29: whole-database MongoDB copies now recreate views and copy collection options (capped, collation, time series, clustered) with validation rules applied after the data; time series are validated order-insensitively (MongoDB rebuilds them from buckets); a refused keychain read (ad-hoc signed rebuilds) now asks for the password and retries, optionally saving it again; end-to-end test through `AppState` on MongoDB 8.0 and PostgreSQL 16 |
| ✅ P12 | Continuous sync: MongoDB change streams with resume tokens (MongoDB→MongoDB, MongoDB→PostgreSQL), PostgreSQL→MongoDB via logical replication or polling (8.2–8.4, subset) | L | **done 2026-10-29** — "Keep the target in sync" on copies: the starting point (change stream resume token, or each object's current maximum of the time field) is recorded before the copy, then source changes are applied until paused or stopped; batches coalesced per key, written as upserts/deletes (child rows replaced, deleted children first), checkpointed after each write, resumed after pause, restart or dropped connections. MongoDB connector: `current_position` and `change_feed` (update lookup, drop/rename stop the sync clearly, standalone servers get a replica-set explanation; change streams declared only for replica sets). Polling for any source (MongoDB or PostgreSQL) on a time field with a 5 s re-read window for dates; deletes not followed (stated in the plan). Option to follow new changes only. Tested on MongoDB 8.0 and PostgreSQL 16: changes during and after the copy, pause and resume, deletes and child rows in PostgreSQL, polled inserts/updates, standalone servers refused for change streams but able to poll, deep validation after stopping. Not yet: PostgreSQL logical replication (polling instead), embedded child tables kept in sync, sync on a schedule (Milestone 8) |

---

## Milestone 2 — Connections and connectors (in progress)

**Goal:** connect to every supported engine securely, manage connections in
the app, and prove each connector with the contract suite.

### Done so far
- SQLite connector, contract suite passed, profile promoted to Beta.
- PostgreSQL connector implemented; contract suite passed on 13, 14, 15, 16, 17 plus PostgreSQL-specific behaviour tests; profile promoted to Beta.
- Local Docker (Colima) installed, so Testcontainers suites run on this machine.

### Remaining tasks, in order
| # | Task | Size |
|---|---|---|
| ✅ 2.1 | Run the PostgreSQL contract suite on 13 and 17; fix findings; record evidence; promote `postgres.postgresql` if it passes — **done 2026-10-24**: passed on 13–17, two bugs fixed (EXPLAIN output type, ModifyTable rendering) | S–M |
| ✅ 2.2 | `gazabridge-application` crate: `ConnectionService` (create, edit, duplicate, delete, test), secret storage and cleanup, audit events, server detection → `connector_metadata`, profile overrides applied to the live manifest — **done 2026-10-24**; also fixed two PostgreSQL connection bugs it exposed (auth errors masked by multi-address retries, TLS server-name override appending a host) | M |
| ✅ 2.3 | MySQL/MariaDB connector (decision D1, `mysql_async` on rustls): TLS modes (disabled/preferred with plain fallback/required/verify-CA/verify-identity), utf8mb4 sessions in UTC with strict SQL mode, type mapping (unsigned 64-bit, decimals, JSON, spatial as WKB, bit, year, enum/set), keyset reads with resumable positions, savepoint-isolated multi-row writes retried row by row, upsert (`ON DUPLICATE KEY UPDATE`) and skip-existing with true counts, explain (`FORMAT=JSON`, `ANALYZE` rolled back), cancel and timeouts by `KILL QUERY` from a second connection, read-only sessions, bounded counts (`MAX_EXECUTION_TIME` / `max_statement_time`) — **done 2026-11-05**; registered in the app (connections, explorer, editor, Query Studio, export/import) and in conversions MongoDB ⇄ MySQL (normalize/flatten/JSONB/hybrid, reference/embed; indexed text becomes `varchar(n)` within InnoDB's 3,072-byte key limit; `DATETIME` read as UTC). SSH tunnels done in 2.7. Not done: binlog CDC (8.5) | L |
| ✅ 2.4 | MySQL contract runs — **done 2026-11-05**: contract suite and behaviour tests pass on MySQL 8.0.46 and 8.4.13, MariaDB 10.11.19 and 11.4.13; `mysql.mysql` and `mysql.mariadb` promoted to beta with evidence in `profiles.toml` | S |
| 2.5 | MongoDB connector (official driver): URI/hosts, TLS, read preference, list databases/collections, sampled schema inference (`$sample` with fallback), Decimal128 rules, null vs missing, transactions on replica sets, resumable reads by `_id` | L |
| 2.6 | MongoDB contract runs on a single-node replica set (7.0, 8.0) | S |
| ✅ 2.7 | SSH tunnels (decision D3, `russh` without the `rsa` crate because of RUSTSEC-2023-0071): local port forward on 127.0.0.1, one tunnel per opened connection and closed with it; host keys checked against `~/.ssh/known_hosts` plus the app's own file — **Strict** (default) refuses unknown servers and shows the SHA256 fingerprint, **trust on first use** records the key in the app's file only (never `~/.ssh/known_hosts`) and refuses a changed key; sign-in by password, Ed25519/ECDSA key file (with or without passphrase) or SSH agent; the SSH password or passphrase is kept like the database password (keychain, session, environment variable). PostgreSQL, MySQL/MariaDB and MongoDB; editor section in en/ar — **done 2026-11-06**. Tested on a real OpenSSH server with the databases on a private container network: connection tests, wrong password/key/passphrase, unknown and changed host keys, unreachable target, and copies PostgreSQL → MySQL → MongoDB with deep validation. Limits: RSA key **files** are not read (use the agent); MongoDB through a tunnel needs a single host without TLS (no `mongodb+srv`, no replica-set discovery); SSH passwords cannot be "asked each time" yet | M |
| 2.8 | Desktop: connection editor (structured fields and connection-string paste), test button with latency, health status, folders/tags/favorites, connector catalogue showing live states | M |
| 2.9 | Docs: connector guide per engine, compatibility matrix filled with evidence | S |

### Decisions to make (write ADRs)
- **D1 MySQL driver.** Recommendation: `mysql_async`. It supports TLS on
  rustls and has a binlog stream API, which Milestone 8 needs. `sqlx-mysql`
  has no binlog support.
- **D2 MongoDB numeric policy.** `decimal(38,10)` exceeds Decimal128's 34
  digits. Recommendation: store such values as strings with a recorded loss
  class of *ConditionallyLossless*, never as doubles.
- **D3 SSH library.** Recommendation: `russh` (pure Rust, async, maintained).

### Done when
- The SQLite, PostgreSQL, MySQL, MariaDB and MongoDB contract suites pass in
  CI, with evidence recorded in `profiles.toml`.
- A user can create, test, edit, duplicate and delete a connection in the app
  without any secret reaching the metadata database or logs.

### Risks
- CI-only verification slows iteration (mitigation: install Docker locally).
- Hosted products (Neon, Supabase, Atlas, DocumentDB) cannot run in CI; they
  stay *Experimental* until tested against real accounts the user provides.

---

## Milestone 3 — Explorer, Data Viewer/Editor, Query Studio, Schema Studio

**Depends on:** M2 (at least SQLite and PostgreSQL).

| # | Task | Size |
|---|---|---|
| 3.1 | Explorer tree with lazy loading of every object kind; context menus | M |
| 3.2 | Virtualized data grid (server-side paging, sort, filter, column picker, hidden/reordered columns); JSON and tree views; null vs missing display; binary summaries; copy cell/row/selection | L |
| 3.3 | Data editor: insert/edit/delete rows and documents, JSON document editor, field validation, generated-statement preview, transactions, optimistic concurrency, audit history, undo before commit, destructive-operation confirmations | L |
| 3.4 | Query Studio: tabs, editor (decision D4), schema-aware autocomplete, parameters, formatting, timeout, cancel, explain/analyze view, history, saved/favorite queries, read-only mode, dangerous-query detection, manual transaction mode with commit/rollback | XL (split: editor, execution, history/saved) |
| 3.5 | Schema Studio: table/collection inspection, ER diagram, inferred MongoDB structure view with occurrence percentages, schema diff/drift, DDL preview, rollback scripts with "not reversible" warnings | L |
| 3.6 | UI smoke tests (WebDriver via `tauri-driver`) | M |

### Decisions
- **D4 Query editor.** Recommendation: Monaco, isolated behind a small
  JavaScript interop module (the specification allows it). Rust-native
  editors are not yet mature enough for autocomplete and large files.
- **D5 Dangerous-query detection.** Recommendation: parse with `sqlparser-rs`
  for SQL dialects; flag UPDATE/DELETE without WHERE, TRUNCATE, DROP, ALTER.

### Done when
A user can browse, filter, edit (with preview and confirmation) and query
data on every supported engine; no view ever issues an unbounded query.

---

## Milestone 4 — Files

**Depends on:** M2 (file connectors reuse the connector traits).

| # | Task | Size |
|---|---|---|
| 4.1 | `gazabridge-files` crate implementing the connector traits for files | S |
| 4.2 | CSV/TSV: delimiter/header detection, quoting, encodings (`encoding_rs`), NULL markers, type inference, malformed-row policies, exact line numbers — **mostly done 2026-11-05**: import reads a header row (BOM, duplicate/empty names refused), detects `,` `;` tab `|`, follows quoting and multi-line cells, empty cell = absent field, fail/skip/quarantine with file line numbers; tables convert text to column types (JSON cells parsed, base64 decoded for binary), MongoDB gets unambiguous numbers/booleans (`007`, `1.50`, dates stay text). Exact round trips tested on PostgreSQL, MySQL and MongoDB. Not done: encodings other than UTF-8, custom NULL markers on import | M |
| 4.3 | JSON (streaming arrays, JSON path selection), JSONL/NDJSON with stop/skip/quarantine/repair policies | M |
| 4.4 | Excel: read with `calamine` (sheet, range, header row, formula vs value), write with `rust_xlsxwriter` (writing done: one sheet, frozen header, exact numbers, CSV column rules; reading still to do) | M |
| 4.5 | Parquet and Arrow IPC via `arrow`/`parquet`: schema inspection, projection, predicate pushdown, batched reads, type mapping | M |
| 4.6 | SQLite files as sources/targets (reuse the SQLite connector); BSON exports | S |
| 4.7 | Compression: gzip (`flate2`), zstd, ZIP with entry selection, ratio and size limits, path-traversal checks | M |
| 4.8 | Export options: split by size/rows, headers, pretty/compact JSON, masking, estimated size, disk-space check, never overwrite without confirmation | M |
| 4.9 | Fuzz targets for every parser (`cargo-fuzz`) and Criterion benchmarks for CSV/JSONL/Parquet/Excel reading | M |

### Done when
Every format round-trips the canonical fixtures, and the fuzzers have run
without crashes.

---

## Milestone 5 — Mapping and transformations

**Depends on:** M2, M4 (needs real schemas to map).

| # | Task | Size |
|---|---|---|
| 5.1 | `gazabridge-mapping`: matching (exact, case-insensitive, normalized, fuzzy suggestions via `strsim`), manual mapping, rename/exclude/constant/derived/combine/split, defaults, nullability, key selection, per-field failure policies, versioning, templates, import/export | L |
| 5.2 | `gazabridge-transform`: the full transformation list in the specification, composable pipelines, preview on samples, deterministic execution | L |
| 5.3 | Sandboxed expressions (decision D6) | M |
| 5.4 | Document → relational: JSON storage, flatten, normalize into child tables, hybrid per field; array strategies; ObjectId identity mapping | L |
| 5.5 | Relational → document: embed 1:1 and 1:N, references, `_id` selection, depth limits, cycle detection, orphan handling, multi-table composition | L |
| 5.6 | Mapping UI with drag-and-drop and before/after preview | L |

### Decisions
- **D6 Expression language.** Recommendation: `rhai` with no I/O functions
  registered and strict operation/size limits; values crossing the boundary
  are canonical values only. Alternative: a small custom DSL (more work, full
  control).

### Done when
Every transformation is unit-tested, previews match execution, and
document⇄relational conversion passes round-trip tests including mixed
types, deep nesting and missing vs null.

---

## Milestone 6 — Migration engine

**Depends on:** M2, M4, M5.

| # | Task | Size |
|---|---|---|
| 6.1 | `gazabridge-migration`: pipeline of read → transform → convert → write over bounded channels with backpressure | L |
| 6.2 | Batching (configurable and adaptive), bounded concurrency, rate limiting | M |
| 6.3 | Checkpoints after durable writes; pause, resume, cancel; resume after restart with fingerprint checks | M |
| 6.4 | Retry classification with exponential backoff; quarantine (dead-letter) files | M |
| 6.5 | Write modes orchestrated by the engine: Replace, Truncate-and-insert, Merge | M |
| 6.6 | `gazabridge-jobs`: job runner, run lifecycle, events → monitor | M |
| ✅ 6.7 | Monitor UI driven by engine events (no timers): throughput, ETA, queue pressure, latencies, memory/CPU — **done 2026-11-05**: every running copy with records/s and bytes/s over the last 10 s, time left per stream and per run, progress, rejects and retries; per stream the queue between reader and writer (filled/room), time per batch on the source and on the target (moving average), and what holds the copy back (source, target or the rate limit); the app's memory and CPU. The engine announces every written batch, start and end (`MigrationService::changes`); the backend pushes a `monitor` event (at most twice a second, plus every 2 s while copies run so a stall shows) and the page listens instead of polling. Tested on a real rate-limited MongoDB copy | M |
| 6.8 | Failure tests: network interruption, process kill and resume, duplicate keys, partial batches, disk full | M |
| 6.9 | All 16 database directions tested end to end in CI — **partial (2026-11-05)**: tested end to end against real servers: MongoDB → MongoDB, MongoDB ⇄ PostgreSQL, MongoDB ⇄ MySQL, and table copies PostgreSQL ⇄ MySQL, PostgreSQL → PostgreSQL, MySQL → MySQL (deep validation, reruns, replace). SQLite directions are not offered | M |

### Done when
A large migration can be paused, killed, restarted and resumed without
losing or duplicating records, with memory staying bounded.

---

## Milestone 7 — Dry Run, validation, reports, backups

**Depends on:** M6.

| # | Task | Size |
|---|---|---|
| 7.1 | Dry Run covering every check in the specification; tests proving zero target mutations (row counts, catalogue snapshots, file-system snapshots) | M |
| 7.2 | `gazabridge-validation`: basic, standard and deep validation; canonical chunk checksums; difference reports | L |
| ✅ 7.3 | `gazabridge-reporting`: JSON, HTML, Markdown, CSV; PDF only if a reliable pure-Rust path exists (decision D7) — **done 2026-11-05** (as `gazabridge-application::report`): run status, connections by name, per-stream counts, latest validation with every difference, errors, rejected records and events; standalone HTML (escaped, no scripts, light/dark), never overwrites a file; "Save report" in the run panel. PDF not done | M |
| 7.4 | Backups: export before replace, SQLite file backup, logical exports, configuration/job/mapping backups | M |
| 7.5 | Native tools (`pg_dump`, `mysqldump`, `mongodump`, …): detection, version checks, sanitized previews, passwords via environment or password files, cancellation, exit-code checks | M |

### Decisions
- **D7 PDF reports.** Recommendation: defer unless a pure-Rust renderer
  produces acceptable output; never shell out to a browser.

---

## Milestone 8 — Scheduling and incremental sync

**Depends on:** M6, M7.

| # | Task | Size |
|---|---|---|
| 8.1 | `gazabridge-scheduler`: cron and interval schedules, time zones (`chrono-tz`), missed-run and overlap policies, run-at-startup, run-when-reachable, notifications, clear "app must stay running" indication | M |
| 8.2 | Polling, timestamp-incremental and key-incremental sync for all engines | M |
| 8.3 | MongoDB change streams with resume tokens; DocumentDB behaviour; polling fallback | M |
| 8.4 | PostgreSQL logical replication (decision D8) with slot monitoring | L |
| 8.5 | MySQL binlog with file/position and GTID tracking | L |
| 8.6 | SQLite application-managed change log option | S |
| 8.7 | Snapshot-then-stream handover tests (no change lost between snapshot and stream) | M |

### Decisions
- **D8 PostgreSQL CDC transport.** Options: (a) consume `pgoutput` through
  `pg_logical_slot_peek/get_binary_changes()` over a normal connection;
  (b) a replication-protocol client. Recommendation: start with (a). It is
  simpler and works through poolers, with higher latency; move to (b) only
  if throughput requires it.

---

## Milestone 9 — Bidirectional sync

**Depends on:** M8.

| # | Task | Size |
|---|---|---|
| 9.1 | Stable cross-system identity and origin metadata; loop prevention | M |
| 9.2 | Version tracking, conflict detection, tombstones, delete propagation | L |
| 9.3 | Conflict policies (source/target/latest-version/timestamp-with-warning/manual/field-merge/custom rule) | M |
| 9.4 | Conflict-resolution UI and mandatory configuration review | M |
| 9.5 | Compatibility matrix of which pairs support which sync features | S |

### Done when
Only documented engine pairs allow bidirectional sync, and conflict and
recovery tests pass for each of them.

---

## Milestone 10 — CLI, hardening, packaging, documentation

**Depends on:** M6 for the CLI core; the rest can overlap with M8–M9.

| # | Task | Size |
|---|---|---|
| 10.1 | `gazabridge` CLI (`clap`): every command in the specification, JSON output, stable exit codes, env-var secrets, config files, non-interactive mode, graceful cancellation | L |
| 10.2 | Criterion benchmarks for every area listed in the specification; profiling guide | M |
| 10.3 | Security hardening pass: threat-model review, fuzzing all parsers, dependency and licence audits clean | M |
| 10.4 | Packaging via `tauri-action`: MSI/NSIS, DMG (Intel and Apple Silicon), AppImage, deb, rpm; portable CLI binaries | M |
| 10.5 | Code signing, macOS notarization, signed updates (Tauri updater), stable and beta channels, rollback strategy | M |
| 10.6 | Release checksums, SBOM (`cargo-cyclonedx`), release-note generation | S |
| 10.7 | Remaining guides: user, administrator, CLI reference, troubleshooting, packaging, release, data-loss risks, known limitations | M |

### Decisions
- **D9 Signing.** Needs the user's Apple Developer and Windows code-signing
  certificates; cannot be completed without them.

---

## Open questions for the project owner

1. Install Docker (recommended) or PostgreSQL locally so connector suites run
   outside CI?
2. Which hosted services should get real-account testing (Neon, Supabase,
   Atlas, DocumentDB, TiDB Cloud), and who provides test accounts?
3. Are code-signing certificates available for Milestone 10?
4. Monaco for Query Studio: acceptable, given the "avoid JavaScript" guideline?

## Immediate next steps

1. Priority track done (P1–P12).
2. MySQL/MariaDB (2.3–2.4) done 2026-11-05, with MongoDB ⇄ MySQL conversions and table copies between PostgreSQL and MySQL.
3. MySQL backups/restores in the GazaBridge format done 2026-11-05.
4. CSV import (4.2) and run reports (7.3) done 2026-11-05.
5. Monitor page (6.7) done 2026-11-05; Imports, Exports, Reports and Sync Jobs pages (same day).
6. SSH tunnels (2.7) done 2026-11-06.
7. Next: Schedules (8.1: scheduler and page), Excel reading (4.4).
