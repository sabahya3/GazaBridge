# ADR 0006: SQLite metadata store with sqlx

- Status: Accepted
- Date: 2026-10-24

## Decision

- Local metadata lives in one SQLite file using `sqlx` (bundled SQLite),
  in WAL mode with `foreign_keys=ON`, `synchronous=NORMAL` and a 5 s busy
  timeout.
- Schema changes are SQL migrations in `database-migrations/metadata`,
  embedded with `sqlx::migrate!` and applied at start-up.
- Tables are `STRICT`; JSON columns are checked with `json_valid`.
- Runtime (not compile-time) queries are used so builds need no database.
- Job and run status changes are validated by the domain state machine and
  applied with compare-and-set to prevent lost updates.
- Migrated records are never cached in metadata; quarantined records go to
  files and only their location is stored.
- `check_integrity` runs `integrity_check` and `foreign_key_check` at
  start-up; `backup_to` uses `VACUUM INTO` and refuses to overwrite.

## Consequences

- Single-writer semantics are fine for a desktop app; long transactions
  must be avoided.
- See [metadata-schema.md](../metadata-schema.md) for recovery steps.
