# ADR 0010: Typed JSON for viewing and editing, and conflict-checked edits

- Status: Accepted
- Date: 2026-10-25

## Context

The Explorer shows records from PostgreSQL and MongoDB and lets the user edit
them as text. Plain JSON loses type identity (ObjectId vs string, Decimal128
vs double, dates vs strings), so a naive "edit as JSON, write back" cycle
silently changes stored types. Separately, two people (or a person and an
application) can change the same record between load and save.

## Decision

1. **Typed JSON** (`gazabridge_domain::ejson`). Values without a native JSON
   form are single-key `$` objects: MongoDB Extended JSON names where they
   exist (`$oid`, `$date`, `$numberDecimal`, `$numberLong`, `$binary`,
   `$uuid`), GazaBridge names otherwise (`$dateOnly`, `$time`,
   `$localDateTime`, `$zonedDateTime`, `$duration`, `$interval`,
   `$numberBigInt`, `$map`, `$enum`, `$geometry`, `$extension`). Output parses
   back to the same values. Values GazaBridge could not read (`$invalid`)
   cannot be written back. JSON column values are written as plain JSON and
   coerced by target column type on write.
2. **Edits are identified by key**: `_id` for collections, the primary key
   for tables. Objects without a key, views and read-only connections are not
   editable, and the UI says why.
3. **Edits are checked against the loaded version.** Before saving, the
   backend re-reads the record and refuses with `conflict` if it differs from
   what the user loaded. The update itself carries compare-and-set
   conditions on changed fields (where engines compare reliably) and
   `expect_matched = 1`, so a change slipping in between is also refused.
   Deletes follow the same rule.
4. **Provenance is kept** for changed fields whose kind is unchanged (e.g. a
   BSON `Int32` stays `Int32`).
5. **Audit by field name only**: `data.insert`, `data.update`, `data.delete`
   events record the object and changed field names, never values.

## Consequences

- Editing never changes a stored type unless the user changes it in the text.
- Users see `$`-wrapped values for non-JSON types; the editor help explains
  them.
- A plain JSON object whose single key is a recognised `$` tag is read as
  that type; other `$` keys stay ordinary fields.
