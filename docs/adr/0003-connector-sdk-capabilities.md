# ADR 0003: Capability-manifest connector SDK

- Status: Accepted
- Date: 2026-10-24

## Context

Engines differ widely (transactions, CDC, schemas vs. schemaless). The UI
must disable unsupported features before execution, and third parties should
be able to add connectors.

## Decision

- `ConnectorFactory` opens `Connection`s. Both are object-safe async traits
  (`async-trait`) so connectors can be registered dynamically.
- Every connection exposes an effective `CapabilityManifest`: base
  capabilities, then compatibility-profile overrides, then read-only
  stripping. Callers must check the manifest; the UI derives enabled actions
  from it.
- Optional operations have default implementations returning
  `ConnectorError::Unsupported(capability)`. A connector that declares a
  capability must override the method. The contract suite tests both
  directions (declared works, undeclared is rejected).
- The API is versioned (`API_VERSION`, currently 1.0). Minor versions only
  add defaulted methods; the registry rejects connectors with an
  incompatible major version or a newer minor version than the host.
- Errors carry an `ErrorKind` that drives retry (`is_retryable`) and
  quarantine (`is_record_level`) decisions; driver messages are redacted on
  construction (`ConnectorError::from_driver`).
- Reads are streams of bounded `RecordBatch`es with opaque, connector-owned
  `ReadPosition`s for resumption. Interactive pages are always bounded
  (`PageRequest::MAX_LIMIT`).
- `plan_ddl` renders statements without side effects (used by previews and
  Dry Run); `apply_ddl` executes them.

## Consequences

- Plugins compiled out-of-tree must match the host's major API version.
- Default "unsupported" methods are part of the contract, not placeholders;
  they are tied to the manifest and tested.
