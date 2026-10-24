# ADR 0009: MongoDB type mapping and resumable reads

- Status: Accepted
- Date: 2026-10-25
- Resolves roadmap decision D2.

## Context

BSON lacks several canonical types (date-only, time of day, duration,
calendar interval, zoned date-time, decimals beyond 34 digits) and has
several that canonical values do not model directly (regex, timestamp,
JavaScript, symbol, min/max key, DBPointer, binary subtypes, 32-bit
integers). MongoDB's query operators also use *type bracketing*: `{$gt: x}`
only matches values of the same BSON type as `x`.

## Decision

1. **Canonical → BSON**
   - Integers become `Int64` unless the field was read as `Int32`
     (`original_type = "int"`, plus nested paths in
     `ValueMeta.connector["bson.int32_paths"]`).
   - Unsigned and big integers beyond `Int64`, and decimals, become
     Decimal128 when exact (≤ 34 significant digits); otherwise their exact
     canonical text. Converting a number to its exact text is lossless in
     the canonical model, so it needs no approval, but it changes the BSON
     type and is documented in the profile.
   - Date, time, local date-time, zoned date-time, duration and interval
     become their canonical text. UTC instants become BSON dates when they
     have whole milliseconds, otherwise exact text.
   - Geometry is rejected until GeoJSON conversion exists (no silent change).
2. **BSON → canonical**: exotic types become `Extension` values named
   `bson:<kind>` carrying the exact BSON bytes; writing them back restores
   the original value. MongoDB → MongoDB copies are byte-for-byte identical
   (tested).
3. **Decimal128** is encoded and decoded by our own exact codec (BID
   format), verified against the BSON corpus vectors and a property test.
4. **Resumable reads** use `$expr` comparisons, which follow BSON sort order
   across types, with `$ifNull` normalising missing sort keys and `_id` as a
   tie-breaker. Query-operator keysets (`{_id: {$gt: last}}`) would silently
   skip documents of other types and were rejected.
5. **Null vs missing**: filters use `$type: "null"` and `$exists`, never
   `{f: null}` (which matches both).

## Consequences

- Reads with custom sort keys cannot always use indexes as efficiently as
  plain range queries; correctness is preferred.
- Sort keys holding arrays are not supported for resumable reads (array
  sort semantics differ from expression comparison).
