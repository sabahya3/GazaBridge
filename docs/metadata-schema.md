# Metadata schema and recovery

Migrations: `database-migrations/metadata/*.sql`, applied automatically on
start-up by `MetadataStore::open`.

## Tables

| Table | Contents |
|---|---|
| `settings`, `ui_preferences` | JSON values by key |
| `folders` | Connection folders (nested; deleting a folder moves its connections to the root) |
| `connections` | Profile settings as JSON with `SecretRef`s only; flags, colour, health |
| `connection_tags` | Tags per connection |
| `secret_refs` | Index of secret references (store + locator, never values) |
| `connector_metadata` | Last detected server product/version, assessed status, findings, effective manifest |
| `mappings`, `transformations` | Versioned definitions `(id, version)` |
| `jobs` | Job definitions, status, linked connections, mapping/transformation versions, Dry Run timestamp |
| `job_runs` | Runs with definition snapshot, config fingerprint, batch/retry settings, stats, validation, counts, app version |
| `run_events` | Data-free warnings/errors per run |
| `checkpoints` | Per-stream positions, committed counts, fingerprint |
| `quarantine_files` | Locations of dead-letter files (records are never stored here) |
| `schedules` | Schedule specs, time zone, policies, next/last run |
| `saved_queries`, `query_history` | Query Studio persistence (history is prunable) |
| `audit_events` | Local audit log |
| `reports` | Generated report files |

Pragmas: `journal_mode=WAL`, `foreign_keys=ON`, `synchronous=NORMAL`,
`busy_timeout=5000`. Tables are `STRICT`; JSON columns are checked with
`json_valid`; `connections` rejects a plaintext `password`.

## Corruption detection

At start-up the app runs `PRAGMA integrity_check` and
`PRAGMA foreign_key_check`. A failure stops start-up with a clear error
instead of operating on damaged metadata.

## Backups

`MetadataStore::backup_to(path)` writes a consistent copy with
`VACUUM INTO`; it never overwrites an existing file. (The UI and CLI expose
this in Milestone 7.)

## Recovery procedure

1. Quit GazaBridge.
2. Locate the data directory (shown on the Home page):
   - macOS: `~/Library/Application Support/dev.gazabridge.app/`
   - Windows: `%APPDATA%\dev.gazabridge.app\`
   - Linux: `~/.local/share/dev.gazabridge.app/`
3. Copy `metadata.db`, `metadata.db-wal` and `metadata.db-shm` somewhere
   safe before changing anything.
4. Try SQLite's recovery: `sqlite3 metadata.db ".recover" | sqlite3 recovered.db`,
   then `sqlite3 recovered.db "PRAGMA integrity_check"`.
5. If recovery succeeds, replace `metadata.db` with `recovered.db` (remove
   the old `-wal`/`-shm` files). Otherwise restore the latest backup.
6. Secrets are in the OS keychain and are unaffected; connections restored
   from a backup keep working.
