# GazaBridge — project status and handoff

**Updated:** 2026-11-05 · **Branch:** `milestone-2` · **Last feature:** MySQL/MariaDB connector, MongoDB ⇄ MySQL conversions, table copies between PostgreSQL and MySQL

This file is the single place to learn where the project stands: what is
done, what is partial, what has not started, and how to keep working. The
detailed plan (task numbers, sizes, decisions) is [roadmap.md](roadmap.md);
keep both in sync when you finish something.

> **ملخص سريع:** GazaBridge تطبيق سطح مكتب (Rust + Tauri 2 + Leptos) لنقل
> وتحويل ومزامنة البيانات بين MongoDB وPostgreSQL وMySQL/MariaDB والملفات. المسار ذو
> الأولوية (P1–P12) خلص بالكامل: الاتصالات، المستكشف والمحرر، Query Studio،
> التصدير والاستيراد (JSON/JSONL/BSON/CSV/Excel)، نسخ MongoDB ⇄ MongoDB
> كامل، التحويل MongoDB ⇄ PostgreSQL، التجربة بدون كتابة والتحقق، النسخ
> الاحتياطي والاسترجاع، والمزامنة المستمرة. بعدها اتضاف MySQL/MariaDB
> (موصل كامل + تحويل MongoDB ⇄ MySQL + نسخ جداول بين PostgreSQL وMySQL).
> ونسخ احتياطي واسترجاع لـ MySQL. واتضافت صفحات المراقبة (بتتحدث بالأحداث من المحرك)، والاستيراد والتصدير، والتقارير، والمزامنات. واتضافت أنفاق SSH لـ PostgreSQL وMySQL وMongoDB (كلمة مرور أو مفتاح أو agent، والتحقق من مفتاح الخادم). الصفحات اللي لسه "قريبًا": Schema Studio والتحويلات والجدولة. اللي لسه: الجدولة، المزامنة
> في الاتجاهين، قراءة Excel، CLI، التغليف والتوقيع للتوزيع.
> واتضاف استيراد CSV واختبار شامل بيعدّي البيانات على كل المحركات ويرجعها زي ما هي.
> والجداول اللي اتعملت من MongoDB بوضع flatten أو normalize بقت ترجع لـ MongoDB بنفس شكل
> المستند الأصلي، من خريطة بتتحفظ في جدول `_gazabridge_shapes` جنب الجداول.

---

## 1. Summary

GazaBridge is a desktop data tool (Rust workspace, Tauri 2 backend, Leptos
WASM UI, local SQLite metadata store). The **priority track P1–P12**
(MongoDB and MongoDB ⇄ PostgreSQL) is **complete and tested against real
servers** (MongoDB 6.0–8.0, PostgreSQL 13–17 in Docker via Testcontainers).

- **327 tests pass** (`cargo test --workspace -- --include-ignored`), clippy
  clean, `cargo audit` clean (8 allowed warnings only). MongoDB 6.0–8.0, PostgreSQL 13–17, MySQL 8.0/8.4, MariaDB 10.11/11.4 and OpenSSH (Alpine) in Docker.
- The macOS app builds as `target/release/bundle/macos/GazaBridge.app`,
  signed with the owner's Apple Development certificate (not notarized).
- UI languages: English and Arabic (RTL).

## 2. Build, run and test

| What | How |
|---|---|
| Toolchain | `export PATH=/opt/homebrew/opt/rustup/bin:$PATH` (rustup has the `wasm32-unknown-unknown` target; Homebrew Rust does not) |
| Docker | Colima. After a reboot: `colima start` |
| Integration tests | `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock cargo test --workspace -- --include-ignored` (Docker tests are `#[ignore]`d by default) |
| Lint | `cargo fmt --all --check`, `cargo clippy --workspace --all-targets`, and for the UI `cd apps/desktop/ui && cargo clippy --target wasm32-unknown-unknown` |
| Audit | `cargo audit` |
| UI only | `cd apps/desktop/ui && trunk build --release` |
| macOS app | `scripts/build-mac-app.sh` (low priority, half the cores; `dmg` argument adds a DMG) |
| Signing | The script reads `.signing-identity` (repository root, **git-ignored**; the SHA-1 of your own code-signing certificate from `security find-identity -v -p codesigning`) or `GAZABRIDGE_SIGNING_IDENTITY`. Without it the app is signed ad hoc and macOS stops releasing saved keychain passwords after every rebuild. |
| UI checks without the backend | Copy `apps/desktop/ui/dist` to a scratch folder, inject a `mock.js` that defines `window.__TAURI__.core.invoke` returning stub data per command, serve it with `python3 -m http.server`, and drive it in a browser |
| End-to-end through the app's wiring | `apps/desktop/src-tauri/tests/e2e.rs` uses `AppState::initialize` against real MongoDB and PostgreSQL containers |

## 3. Done

### Foundations (Milestone 1) ✅
Workspace and lints, domain model, loss-aware canonical value model,
connector SDK v1.0 with capability manifests, 18 compatibility profiles,
secrets by reference (keychain / session / environment) with a redactor,
SQLite metadata store (connections, jobs, runs, events, checkpoints, audit,
query history), redacted JSON logs, contract test suite, desktop shell
(navigation, palette, themes, en/ar), CI config, ADRs 0001–0010, threat
model.

### Connectors
| Connector | State |
|---|---|
| SQLite | Beta; contract suite passes |
| PostgreSQL | Beta; contract suite passes on 13–17 |
| MongoDB | Beta; contract suite passes on 6.0, 7.0, 8.0. Change streams (P12), exact Decimal128 (non-canonical bytes kept), exotic BSON preserved, bulk upserts, order-preserving skip-existing, full index and collection specifications |
| MySQL / MariaDB | Beta; contract suite and behaviour tests pass on MySQL 8.0, 8.4 and MariaDB 10.11, 11.4. Registered in the app (explorer, editor, Query Studio, export/import, conversions, table copies, GazaBridge-format backups and restores, SSH tunnels). No binlog CDC, no `mysqldump` |

### Priority track (all ✅, details per row in [roadmap.md](roadmap.md#priority-track--mongodb-and-mongodb--postgresql-decided-2026-10-25))
| # | Feature | Where |
|---|---|---|
| P1–P2 | MongoDB connector and contract runs | `crates/gazabridge-connector-mongodb` |
| P3 | Schema inference, snapshots, drift | MongoDB `infer.rs`, `SchemaService` |
| P4 | Connection editor, connection strings, tests, saved connections | `pages/connections.rs`, `connection_editor.rs` |
| P5 | Explorer: tree with counts, table/JSON/tree viewer, editor with preview and audit, query bar (builder + mongosh text, sort/project/skip/limit, validation), aggregations | `pages/explorer.rs`, `query_bar.rs`, `aggregation.rs`, `crates/gazabridge-query` |
| P6 | Query Studio (MongoDB and SQL): history, saved queries, params, cancel, explain | `pages/query_studio.rs`, `QueryService` |
| P7 | Files: JSON, JSONL, BSON, CSV, **Excel export**, gzip; export follows the query (filter, project, sort, skip, limit) | `crates/gazabridge-files`, `TransferService`, `pages/transfer.rs` |
| P8 | Migration engine: bounded pipeline, checkpoints, pause/resume/cancel, retries, quarantine; whole-database MongoDB copy with indexes, views, collection options and validators | `crates/gazabridge-migration`, `MigrationService`, `pages/migrations.rs` |
| P9 | MongoDB ⇄ PostgreSQL: flatten / normalize / JSONB / hybrid; reference / embed; ObjectId identity; mapping preview | `crates/gazabridge-mapping`, `convert.rs` |
| P10 | Dry run (zero writes, proven) and validation: counts, keys, deep checksums with named differences | `migration::validate`, run panel |
| P11 | Backups and restores: GazaBridge format with manifest and SHA-256, `mongodump`/`pg_dump` with passwords only in private temp files, backup before replace | `backup.rs`, `tools.rs`, `pages/backups.rs` |
| P12 | Continuous sync after a copy: MongoDB change streams (resume tokens) or timestamp polling for any source; pause/resume/stop | `migration/sync.rs`, MongoDB `change_feed` |

### After the priority track (2026-11-05)
| Feature | Where |
|---|---|
| MySQL/MariaDB connector (2.3–2.4) | `crates/gazabridge-connector-mysql` (`tests/contract.rs`, `tests/behaviour.rs`), app wiring test `gazabridge-application/tests/mysql_service.rs` |
| MongoDB ⇄ MySQL conversions (same modes as PostgreSQL; indexed text → `varchar(n)` within InnoDB's key limit; `DATETIME` read as UTC) | `convert.rs`, `tests/conversion_mysql.rs` |
| Table copies PostgreSQL ⇄ MySQL, PostgreSQL → PostgreSQL, MySQL → MySQL: target table from canonical types, primary key required, simple indexes recreated, foreign keys / partial / expression indexes skipped with warnings, deep validation | `table_copy.rs`, `tests/table_copy.rs` |
| MySQL backups and restores in the GazaBridge format (tables, keys, defaults, indexes, foreign keys; rename to another database); backup before a copy replaces a MySQL target; Backups page offers MySQL | `backup.rs`, `tests/backup_mysql.rs`, `pages/backups.rs` |
| Round-trip test through the app's wiring (`AppState`) on MongoDB 8.0, PostgreSQL 16, MySQL 8.4, MariaDB 11.4: 300 documents with every common BSON type (Arabic/emoji/quotes, doubles, Decimal128, pre-1970 dates, Int64, binary, nested objects, arrays, null vs missing) normalized into PostgreSQL, copied PostgreSQL → MySQL → MariaDB → PostgreSQL and compared value by value; stored whole MongoDB → MySQL → PostgreSQL → MongoDB and compared document by document; BSON and JSONL files; dry run; deep validation at every step | `apps/desktop/src-tauri/tests/round_trip.rs` |
| CSV import (4.2): header row, delimiter detection, quoting, line numbers in errors, skip/quarantine; JSON and binary cells decoded for tables; plain numbers/booleans for MongoDB | `gazabridge-files/src/read.rs`, `transfer.rs`, `pages/transfer.rs` |
| Run reports (7.3): JSON, HTML, Markdown, CSV with counts, validation differences, errors and events; "Save report" in the run panel | `report.rs`, `pages/migrations.rs` |
| Monitor page (6.7): running copies with records/s and bytes/s over 10 s, time left, progress, rejects and retries; per stream the queue fill, source and target time per batch, and what holds the copy back; app memory and CPU. Pushed by the backend on every batch (no polling). Connection lists on Backups and Migrations can add a new connection in place. Tested on a live rate-limited MongoDB copy | `monitor.rs`, `pages/monitor.rs`, `tests/migration.rs` |
| Imports and Exports pages: pick connection → database → schema → collection/table (views for exports; a new MongoDB collection for imports; read-only connections hidden for imports) and use the Explorer's transfer panel, without opening the Explorer | `pages/files.rs` |
| Reports page: the last 200 copies, conversions and syncs with status, start, duration, records, rejects and validation result; filter and search; save any run's report (HTML, Markdown, JSON, CSV) | `pages/reports.rs` |
| Sync Jobs page: every sync with its method (change streams or polling field and interval), objects, changes, deletes, last change and rejects; pause, resume, stop; refreshed when the backend announces applied changes (no polling). Tested on a live MongoDB replica-set sync | `pages/sync_jobs.rs`, `tests/sync.rs` |
| SSH tunnels (2.7): PostgreSQL, MySQL/MariaDB and MongoDB through an SSH server; password, Ed25519/ECDSA key file (passphrase optional) or SSH agent; Strict host keys by default (unknown server refused with its SHA256 fingerprint), or trust on first use recorded in the app's own `known_hosts` (the user's `~/.ssh/known_hosts` is never written) and changed keys refused; SSH secret stored like the database password; editor section. Tested on a real OpenSSH server with the databases reachable only from its private network, including copies PostgreSQL → MySQL → MongoDB | `crates/gazabridge-ssh` (`tests/tunnel.rs`), connectors, `connections.rs`, `tests/ssh_tunnel.rs`, `forms.rs`, `pages/connection_editor.rs` |
| Fixes from the owner's first real copies (2026-11-06): a child table made from an array (`_parent_id`, `_index`) copied back to MongoDB as its own collection lost rows whose subdocument `_id` repeated or was empty — the key is now `_id` and the column is kept as `_id_column`, with a plan warning pointing to Embed; unique indexes over columns that may be NULL are recreated in MongoDB with a partial filter that skips nulls (SQL semantics), instead of failing; a copy into a MySQL database the account may not create tables in is refused by the plan (and Dry run) with the databases it can use and the `GRANT` to run; buttons show a spinner and refuse a second click while their command runs | `table_to_doc.rs`, `tests/conversion.rs`, `migration.rs`, `tests/conversion_mysql.rs`, `connector-mysql/error.rs`, `ui/src/ipc.rs` |
| Original document shape on the way back (2026-11-06): a copy from MongoDB with the flatten or normalize modes saves its mapping in a `_gazabridge_shapes` table next to the tables (PostgreSQL schema or MySQL database; a side table rather than a table comment, since MySQL comments stop at 2,048 characters). Copying those tables back to MongoDB reads it and rebuilds each document: `address_city` → `address.city`, `_extra` merged back, child tables turned back into arrays in `_index` order (read even when not selected; selected ones are marked embedded), `_parent_id`/`_index` dropped, ObjectIds and 32-bit integers restored (also inside JSON columns, now written with their BSON types). Driven by the columns, so rows inserted or changed in SQL come back too; dropped columns or child tables are warned in the plan (a missing child table falls back to flat documents). The MongoDB sampler now counts 32-bit integers per field (`int32_count`). Tested on real servers: 12 varied documents → PostgreSQL and MySQL (both modes) → MongoDB come back identical key by key, SQL-inserted rows included, Deep validation passing. Also fixed: MariaDB JSON columns (`longtext` with a `json_valid` check) are now read as JSON, not text, so copies and validation between MySQL and MariaDB agree on them | `connector-mysql/meta.rs`, `mapping/shape.rs`, `doc_to_table.rs`, `table_to_doc.rs`, `convert.rs`, `migration.rs`, `connector-mongodb/infer.rs`, `tests/conversion.rs`, `tests/conversion_mysql.rs` |
| Fixes from the owner's copies, continued (2026-11-06): a collection whose `_id` is text (not an ObjectId) failed in MySQL with "BLOB/TEXT column '_id' used in key specification" — a text `_id` now becomes `varchar(764)` (binary `varbinary(3056)`), within InnoDB's 3,072-byte key with room for the child tables' `_index`, and child tables' `_parent_id` follows; an `_id` of several types is refused by the plan with the reason (MySQL cannot key on JSON), instead of failing mid-copy. MongoDB Atlas refusing the TLS handshake ("fatal alert: InternalError", which the driver labels "overloaded") now says to add this computer's IP to the Atlas IP Access List | `convert.rs` (`fit_mysql_key`), `tests/conversion_mysql.rs`, `connector-mongodb/error.rs` |
| Fixes the real tests found: concurrent `CREATE SCHEMA IF NOT EXISTS` in PostgreSQL (retried); validation of composite keys without an owner column (and siblings split across batches); "Validate" right after a copy finished was refused, and a finished validation briefly showed the previous result (two races); `char(n)` columns became `varchar(n)` in table copies (ObjectId identity lost); MySQL's JSON parser rounds doubles with 16–17 digits (now stored as `{"$numberDouble": …}` in MongoDB conversions); Int64 values that fit in 32 bits came back as Int32 from JSON-mode tables (now `{"$numberLong": …}`); MySQL literal column defaults were unquoted | `connector-postgres`, `migration/validate.rs`, `migration.rs`, `table_copy.rs`, `gazabridge-mapping`, `connector-mysql/meta.rs` |

### Also done along the way
- Password dialog: when the keychain refuses (or a password is asked each
  time) the app asks, keeps it for the session, can save it again, and
  retries the waiting command.
- Export dialog: result shown next to the buttons; JSON by default.
- Every copy gets an automatic count check; the history keeps every run.

## 4. Partial or known gaps

| Area | Gap |
|---|---|
| PostgreSQL native tools | `pg_dump`/`pg_restore` are **not installed** on the build Mac; their command building and password file are unit-tested only. `pg_restore` cannot rename schemas or keep existing rows |
| Sync | PostgreSQL logical replication not implemented (polling instead); polling does not see deletes; embedded child tables are not kept in sync; time series resumed batches may repeat (no unique `_id`) |
| Conversions | Tables made by the flatten and normalize modes come back in their original shape only when their saved mapping is there (copies made before 2026-11-06 come back flat, as they are in SQL); after a rebuild key order follows the mapping, and a NULL becomes `null` for fields every sampled document had and is left out otherwise; JSONB does not keep key order; missing vs null both become NULL in typed columns; embedding is one level; MySQL unconstrained decimals become `decimal(65,30)`; MySQL `ON DUPLICATE KEY` reacts to any unique key; table copies of JSON columns into MySQL may round 16–17-digit doubles (warned in the plan); SQLite is not offered as a copy source or target |
| MySQL | JSON `null` and SQL NULL look the same in exported files; backups use the GazaBridge format only (`mysqldump` not supported); AUTO_INCREMENT is not recreated by a restore |
| SSH tunnels | RSA key **files** are not read (no `rsa` crate, RUSTSEC-2023-0071) — RSA keys work through the SSH agent; MongoDB through a tunnel needs one host without TLS (no `mongodb+srv`, no replica-set discovery); SSH passwords cannot be "asked each time"; no jump hosts (ProxyJump) |
| Backups | PostgreSQL sequences and MySQL AUTO_INCREMENT are not backed up (values restored as stored); no scheduled backups; no `mysqldump` |
| Validation | Key and deep checks read both sides fully (no sampling for huge objects); target-only extras are reported from counts |
| Files | Excel **reading**, CSV in encodings other than UTF-8, Parquet/Arrow, zstd/ZIP, split output, fuzzing (roadmap 4.2–4.9) |
| UI | Data grid is paged, not virtualized; Query Studio editor is plain text (no highlighting/autocomplete, question 4); no Schema Studio, Transformations or Schedules pages (still "Soon" in the menu); no UI smoke tests (3.6) |
| Docs | `requirements-traceability.md` and `connector-guide.md` were not refreshed during the priority track — verify before relying on them |
| macOS app | Signed for local use only; not notarized; no updater |

## 5. Not started

- **Milestone 2 remainder:** connector docs (2.9).
- **Milestone 3:** Schema Studio (3.5), virtualized grid (3.2), UI smoke tests (3.6).
- **Milestone 5:** general transformation pipelines and sandboxed expressions (5.2–5.3), drag-and-drop mapping UI (5.6).
- **Milestone 6:** Merge/truncate write modes (6.5), job runner (6.6), failure-injection suite on real servers (6.8), all 16 directions in CI (6.9).
- **Milestone 8:** scheduler (8.1), PostgreSQL logical replication (8.4), MySQL binlog (8.5), SQLite change log (8.6).
- **Milestone 9:** bidirectional sync (all).
- **Milestone 10:** CLI, benchmarks, hardening pass, installers for all platforms, notarization and signed updates, SBOM, user documentation.

**Suggested next:** Schedules (8.1, scheduler plus page), Excel reading (4.4). The owner chooses.

## 6. Open questions for the owner

1. Which hosted services get real-account testing (Atlas, Neon, Supabase, DocumentDB…), and who provides accounts?
2. Monaco (JavaScript) for the Query Studio editor: acceptable?
3. Distribution: Developer ID certificate and notarization for other users (Milestone 10)?

## 7. Rules for whoever continues

- **Safety (from the specification):** never use real credentials in code,
  tests, fixtures, screenshots, docs or logs; never connect to or modify the
  owner's databases unless they configure it and start the operation; never
  store plaintext passwords; never put passwords in process arguments; never
  overwrite files without confirmation; telemetry off by default; a
  connector is "Production Ready" only after its contract suite passes.
- **Definition of done:** fmt, clippy (workspace and UI wasm), all tests
  including Docker ones, `cargo audit`; update the roadmap row and this
  file; rebuild the app with the script.
- **Style:** match the surrounding code; tests run against real servers in
  containers, not mocks, whenever a server is involved.

## 8. Map of the code

| Path | What |
|---|---|
| `crates/gazabridge-domain` | Values, types, connection profiles, job states |
| `crates/gazabridge-plugin-sdk` | Connector traits, read/write/DDL/CDC contracts |
| `crates/gazabridge-connector-*` | SQLite, PostgreSQL, MongoDB, MySQL/MariaDB |
| `crates/gazabridge-bson` | BSON ⇄ canonical values |
| `crates/gazabridge-files` | File readers and writers (incl. Excel) |
| `crates/gazabridge-query` | mongosh-syntax parser and query builder (shared with the UI) |
| `crates/gazabridge-mapping` | Document ⇄ relational mappings |
| `crates/gazabridge-migration` | Engine, transforms, validation |
| `crates/gazabridge-application` | Services: connections, explorer, query, transfer, migration (+ `migration/sync.rs`), convert, backup, tools |
| `crates/gazabridge-storage` | SQLite metadata store |
| `crates/gazabridge-ipc` | Types shared by the backend and the UI |
| `apps/desktop/src-tauri` | Tauri commands and `AppState` |
| `apps/desktop/ui` | Leptos pages (`src/pages`), styles, `locales/en.json` + `ar.json` |
| `docs/` | Roadmap, backlog, architecture, ADRs, threat model |
