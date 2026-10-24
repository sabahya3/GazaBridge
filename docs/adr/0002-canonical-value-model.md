# ADR 0002: Canonical, loss-aware value model

- Status: Accepted
- Date: 2026-10-24

## Context

Sixteen database directions plus file formats would need hundreds of
pairwise converters, each a chance to lose data silently.

## Decision

- All connectors convert to and from one canonical `Value` enum covering
  null, missing, booleans, signed/unsigned/arbitrary-precision integers,
  arbitrary-precision decimals, floats, strings, binary, UUID, ObjectId,
  date/time variants, durations, intervals, arrays, objects, maps, enums,
  JSON, geometry (WKB), opaque extension values and invalid values.
- `Null` and `Missing` are distinct values.
- Provenance (`ValueMeta`: original type/text, precision, scale, time zone,
  encoding, source field, warnings, connector metadata) is attached at field
  level via `CanonicalValue`.
- Conversion has two layers: `classify(from_type, to_type)` gives a
  worst-case class (Lossless, ConditionallyLossless, PotentiallyLossy,
  DefinitelyLossy, Unsupported); `convert(value, type, policy)` performs a
  value-level conversion and reports actual loss, never worse than the
  classification. A property test enforces that bound.
- The default `ConversionPolicy` is strict: any loss is an error. Losses
  must be approved per class; approved losses yield a `LossRecord` that
  contains no data values (safe to log and report).
- Decimals use `bigdecimal` (arbitrary precision); 128-bit range checks for
  integers. Floats convert losslessly only when their shortest round-trip
  decimal equals the source.
- Serialization is externally tagged JSON; big numbers and bytes are
  strings, non-finite floats are `"NaN"`/`"Infinity"`, so values survive
  JSON (checkpoints, quarantine files, IPC) intact.

## Consequences

- Each connector needs only `native ⇄ canonical` mappings.
- Some conversions users expect to "just work" (e.g. local time to UTC)
  require an explicit assumed offset in the policy.
- Nested values do not carry per-element metadata.
