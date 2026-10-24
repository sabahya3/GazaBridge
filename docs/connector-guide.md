# Connector guide

This guide explains the connector capability model and how to implement a
connector. Source: `crates/gazabridge-plugin-sdk`.

## Concepts

- **`ConnectorFactory`** — one per engine family. Provides a
  `ConnectorDescriptor` (id, family, API version, implementation version),
  a base `CapabilityManifest`, and `connect(profile, secrets)`.
- **`Connection`** — an open, pooled connection. Required methods:
  `manifest`, `is_read_only`, `health_check`, `server_info`, `list`,
  `quote_identifier`, `close`. Every other method is optional and defaults
  to `ConnectorError::Unsupported(capability)`.
- **`Transaction`** — returned by `begin` when `Transactions` is declared;
  dropping it without commit rolls back.
- **Compatibility profile** — product-specific data layered over a family
  connector (see [ADR 0004](adr/0004-compatibility-profiles.md)).

## Capability model

A manifest lists:

- `capabilities`: what operations work (`StreamRead`, `PaginatedRead`,
  `FilteredRead`, `WriteInsert`, `WriteUpsert`, `Transactions`,
  `CreateTable`, `ExecuteQuery`, `CancelQuery`, `ExplainAnalyze`,
  `ChangeDataCapture`, `ResumeTokens`, `Checkpoints`, …);
- `write_modes`: supported `WriteMode`s (Insert, Append, Upsert, Merge,
  Replace, TruncateAndInsert, SkipExisting, FailOnConflict);
- `sync_methods`: Native CDC, Change Stream, Logical Replication, Binlog,
  Polling, Timestamp Incremental, Key Incremental;
- `query_languages` and `limits` (identifier length, bind parameters,
  record size, nesting depth).

The effective manifest of a connection is computed as:

```
base manifest  →  server detection adjustments  →  profile overrides  →  read-only stripping
```

The UI and engine must check `manifest.supports(...)` before calling an
operation. Mutating capabilities (`Capability::is_mutating`) are removed for
read-only connections.

### The two rules

1. If a capability is declared, its method must work (positive contract
   tests).
2. If a capability is not declared, its method must return
   `ConnectorError::Unsupported` (negative contract tests).

## Reading

`read(ReadRequest, CancellationToken)` returns a stream of bounded
`RecordBatch`es. Ordered reads by a unique key must attach a `ReadPosition`
to each batch (connector-defined JSON, such as the last primary key or a
resume token); passing it back as `resume_after` continues after that batch.
`read_page` is for interactive viewing and is always bounded
(`PageRequest::MAX_LIMIT`).

## Writing

`write_batch` applies a `WriteBatch` with a `WriteMode`. Record-level
failures (duplicate key, constraint, conversion) are returned in
`WriteOutcome::failures` with the record index so the engine can quarantine
them; connection-level failures are errors. `update`/`delete` require a
restrictive `Filter` and support `expect_matched` for optimistic concurrency.

## Errors

Use `ConnectorError::from_driver(kind, message, code)` for driver errors —
it redacts credentials. Choose the `ErrorKind` carefully: `Connection`,
`Timeout` and `TransientServer` are retried; `Conflict`, `Constraint` and
`Conversion` are record-level.

## Types

Map native types to `CanonicalType` in `inspect` and implement
`native_type_for` for DDL. Values read from the server must be converted to
canonical `Value`s without loss; if a value cannot be represented, return
`Value::Invalid` (never a guess) or keep the native form in
`Value::Extension`.

## Contract suite

Implement `gazabridge_testkit::ContractHarness` for a disposable target and
call `run_contract_suite`. It checks API compatibility, health, server
detection, listing, identifier quoting (hostile and Arabic identifiers),
undeclared-capability rejection, side-effect-free `plan_ddl`, object
creation and inspection, a 30-row round trip of edge-case values (Arabic and
emoji text, 140 KB text, 256 KB binary, decimal(38,10) extremes, integer
limits, DST-adjacent timestamps), pagination, filtering, resume, duplicate
detection, upsert idempotency, update/delete guards, transactions, null vs.
missing (documents), cancellation, read-only enforcement and drop.

Network connectors run the suite against Testcontainers in CI
(`#[ignore]` tests enabled with `GAZABRIDGE_INTEGRATION=1`). A profile's
`tested_versions` and status may only be raised with recorded evidence.
