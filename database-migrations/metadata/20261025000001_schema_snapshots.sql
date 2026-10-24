-- Saved schema snapshots (inferred document structures and inspected
-- relational schemas), used for drift detection and as mapping input.
-- Snapshots contain structure and statistics only, never record values.

CREATE TABLE schema_snapshots (
    id              TEXT PRIMARY KEY NOT NULL,
    connection_id   TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    object_json     TEXT NOT NULL CHECK (json_valid(object_json)),
    object_key      TEXT NOT NULL,
    label           TEXT,
    inferred        INTEGER NOT NULL CHECK (inferred IN (0, 1)),
    schema_json     TEXT NOT NULL CHECK (json_valid(schema_json)),
    created_at      TEXT NOT NULL
) STRICT;

CREATE INDEX schema_snapshots_object ON schema_snapshots(connection_id, object_key, created_at DESC);
