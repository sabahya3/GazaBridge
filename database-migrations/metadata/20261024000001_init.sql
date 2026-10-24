-- GazaBridge local metadata schema, version 1.
--
-- Invariants:
-- * No secret values are ever stored here. Connections reference secrets via
--   SecretRef JSON ({"store": "keychain", ...}); the CHECK constraints below
--   reject a plain string in any `password` field as defence in depth.
-- * Migrated records are never stored here. Quarantined records are written
--   to files under the application data directory; only their location is
--   recorded.
-- * Timestamps are RFC 3339 UTC strings; identifiers are UUIDv7 strings.

CREATE TABLE settings (
    key         TEXT PRIMARY KEY NOT NULL,
    value_json  TEXT NOT NULL CHECK (json_valid(value_json)),
    updated_at  TEXT NOT NULL
) STRICT;

CREATE TABLE ui_preferences (
    key         TEXT PRIMARY KEY NOT NULL,
    value_json  TEXT NOT NULL CHECK (json_valid(value_json)),
    updated_at  TEXT NOT NULL
) STRICT;

CREATE TABLE folders (
    id          TEXT PRIMARY KEY NOT NULL,
    name        TEXT NOT NULL CHECK (length(trim(name)) > 0),
    parent_id   TEXT REFERENCES folders(id) ON DELETE CASCADE,
    position    INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
) STRICT;

CREATE TABLE connections (
    id                      TEXT PRIMARY KEY NOT NULL,
    name                    TEXT NOT NULL CHECK (length(trim(name)) > 0),
    family                  TEXT NOT NULL CHECK (family IN ('postgres', 'mysql', 'mongodb', 'sqlite')),
    compatibility_profile   TEXT NOT NULL,
    folder_id               TEXT REFERENCES folders(id) ON DELETE SET NULL,
    settings_json           TEXT NOT NULL CHECK (json_valid(settings_json)),
    tls_json                TEXT NOT NULL CHECK (json_valid(tls_json)),
    ssh_json                TEXT CHECK (ssh_json IS NULL OR json_valid(ssh_json)),
    read_only               INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
    color                   TEXT,
    favorite                INTEGER NOT NULL DEFAULT 0 CHECK (favorite IN (0, 1)),
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    last_used_at            TEXT,
    last_state              TEXT,
    last_latency_ms         INTEGER,
    last_checked_at         TEXT,
    CHECK (json_type(settings_json, '$.password') IS NULL OR json_type(settings_json, '$.password') = 'object'),
    CHECK (ssh_json IS NULL OR json_type(ssh_json, '$.auth.password') IS NULL OR json_type(ssh_json, '$.auth.password') = 'object')
) STRICT;

CREATE INDEX connections_folder ON connections(folder_id);
CREATE INDEX connections_last_used ON connections(last_used_at DESC);

CREATE TABLE connection_tags (
    connection_id   TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    tag             TEXT NOT NULL CHECK (length(trim(tag)) > 0),
    PRIMARY KEY (connection_id, tag)
) STRICT, WITHOUT ROWID;

CREATE INDEX connection_tags_tag ON connection_tags(tag);

-- Index of secret references so secrets can be cleaned up when their owner is
-- deleted. `locator` is the service/account or variable name, never a value.
CREATE TABLE secret_refs (
    owner_kind  TEXT NOT NULL CHECK (owner_kind IN ('connection')),
    owner_id    TEXT NOT NULL,
    field       TEXT NOT NULL,
    store       TEXT NOT NULL CHECK (store IN ('keychain', 'session', 'env', 'prompt')),
    locator     TEXT NOT NULL,
    PRIMARY KEY (owner_kind, owner_id, field)
) STRICT, WITHOUT ROWID;

CREATE TABLE connector_metadata (
    connection_id       TEXT PRIMARY KEY NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    server_product      TEXT NOT NULL,
    server_version      TEXT NOT NULL,
    detected_profile    TEXT,
    assessed_status     TEXT NOT NULL,
    findings_json       TEXT NOT NULL CHECK (json_valid(findings_json)),
    manifest_json       TEXT NOT NULL CHECK (json_valid(manifest_json)),
    detected_at         TEXT NOT NULL
) STRICT;

CREATE TABLE mappings (
    id              TEXT NOT NULL,
    version         INTEGER NOT NULL CHECK (version >= 1),
    name            TEXT NOT NULL,
    definition_json TEXT NOT NULL CHECK (json_valid(definition_json)),
    is_template     INTEGER NOT NULL DEFAULT 0 CHECK (is_template IN (0, 1)),
    created_at      TEXT NOT NULL,
    PRIMARY KEY (id, version)
) STRICT;

CREATE TABLE transformations (
    id              TEXT NOT NULL,
    version         INTEGER NOT NULL CHECK (version >= 1),
    name            TEXT NOT NULL,
    definition_json TEXT NOT NULL CHECK (json_valid(definition_json)),
    created_at      TEXT NOT NULL,
    PRIMARY KEY (id, version)
) STRICT;

CREATE TABLE jobs (
    id                      TEXT PRIMARY KEY NOT NULL,
    name                    TEXT NOT NULL CHECK (length(trim(name)) > 0),
    kind                    TEXT NOT NULL CHECK (kind IN ('migration', 'import', 'export', 'convert', 'sync', 'replication', 'backup', 'restore', 'validation')),
    status                  TEXT NOT NULL,
    definition_json         TEXT NOT NULL CHECK (json_valid(definition_json)),
    source_connection_id    TEXT REFERENCES connections(id) ON DELETE SET NULL,
    target_connection_id    TEXT REFERENCES connections(id) ON DELETE SET NULL,
    mapping_id              TEXT,
    mapping_version         INTEGER,
    transformation_id       TEXT,
    transformation_version  INTEGER,
    dry_run_passed_at       TEXT,
    archived                INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
) STRICT;

CREATE INDEX jobs_status ON jobs(status) WHERE archived = 0;

CREATE TABLE job_runs (
    id                      TEXT PRIMARY KEY NOT NULL,
    job_id                  TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt                 INTEGER NOT NULL DEFAULT 1,
    status                  TEXT NOT NULL,
    dry_run                 INTEGER NOT NULL DEFAULT 0 CHECK (dry_run IN (0, 1)),
    definition_snapshot     TEXT NOT NULL CHECK (json_valid(definition_snapshot)),
    config_fingerprint      TEXT NOT NULL,
    mapping_version         INTEGER,
    transformation_version  INTEGER,
    batch_settings_json     TEXT NOT NULL CHECK (json_valid(batch_settings_json)),
    concurrency             INTEGER NOT NULL CHECK (concurrency >= 1),
    retry_policy_json       TEXT NOT NULL CHECK (json_valid(retry_policy_json)),
    stats_json              TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(stats_json)),
    validation_json         TEXT CHECK (validation_json IS NULL OR json_valid(validation_json)),
    warning_count           INTEGER NOT NULL DEFAULT 0,
    error_count             INTEGER NOT NULL DEFAULT 0,
    error_summary           TEXT,
    app_version             TEXT NOT NULL,
    created_at              TEXT NOT NULL,
    started_at              TEXT,
    ended_at                TEXT
) STRICT;

CREATE INDEX job_runs_job ON job_runs(job_id, created_at DESC);
CREATE INDEX job_runs_status ON job_runs(status);

-- Warnings and errors raised during a run. Messages must be data-free.
CREATE TABLE run_events (
    id          INTEGER PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    at          TEXT NOT NULL,
    level       TEXT NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    code        TEXT NOT NULL,
    stream      TEXT,
    message     TEXT NOT NULL
) STRICT;

CREATE INDEX run_events_run ON run_events(run_id, id);

CREATE TABLE checkpoints (
    run_id              TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    stream              TEXT NOT NULL,
    position_json       TEXT CHECK (position_json IS NULL OR json_valid(position_json)),
    records_committed   INTEGER NOT NULL DEFAULT 0,
    bytes_committed     INTEGER NOT NULL DEFAULT 0,
    completed           INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
    config_fingerprint  TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (run_id, stream)
) STRICT;

-- Location of quarantined (dead-letter) record files.
CREATE TABLE quarantine_files (
    id          INTEGER PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    stream      TEXT NOT NULL,
    path        TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL
) STRICT;

CREATE TABLE schedules (
    id                  TEXT PRIMARY KEY NOT NULL,
    job_id              TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    spec_json           TEXT NOT NULL CHECK (json_valid(spec_json)),
    timezone            TEXT NOT NULL,
    enabled             INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    missed_run_policy   TEXT NOT NULL,
    overlap_policy      TEXT NOT NULL,
    next_run_at         TEXT,
    last_run_at         TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
) STRICT;

CREATE INDEX schedules_next ON schedules(next_run_at) WHERE enabled = 1;

CREATE TABLE saved_queries (
    id              TEXT PRIMARY KEY NOT NULL,
    name            TEXT NOT NULL CHECK (length(trim(name)) > 0),
    connection_id   TEXT REFERENCES connections(id) ON DELETE SET NULL,
    language        TEXT NOT NULL,
    text            TEXT NOT NULL,
    favorite        INTEGER NOT NULL DEFAULT 0 CHECK (favorite IN (0, 1)),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
) STRICT;

-- Query history is pruned by count/age according to settings.
CREATE TABLE query_history (
    id              INTEGER PRIMARY KEY,
    connection_id   TEXT REFERENCES connections(id) ON DELETE CASCADE,
    language        TEXT NOT NULL,
    text            TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    duration_ms     INTEGER,
    row_count       INTEGER,
    affected        INTEGER,
    status          TEXT NOT NULL CHECK (status IN ('ok', 'error', 'cancelled')),
    error_message   TEXT
) STRICT;

CREATE INDEX query_history_conn ON query_history(connection_id, started_at DESC);

CREATE TABLE audit_events (
    id              TEXT PRIMARY KEY NOT NULL,
    at              TEXT NOT NULL,
    action          TEXT NOT NULL,
    target_kind     TEXT NOT NULL,
    target_id       TEXT,
    summary         TEXT NOT NULL,
    details_json    TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details_json)),
    correlation_id  TEXT
) STRICT;

CREATE INDEX audit_events_at ON audit_events(at DESC);
CREATE INDEX audit_events_target ON audit_events(target_kind, target_id);

CREATE TABLE reports (
    id          TEXT PRIMARY KEY NOT NULL,
    run_id      TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    format      TEXT NOT NULL CHECK (format IN ('json', 'html', 'markdown', 'csv', 'pdf')),
    path        TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    created_at  TEXT NOT NULL
) STRICT;
