# Canonical data model

Source: `crates/gazabridge-domain/src/{value,types,conversion}.rs`.

## Values

| Variant | Meaning | JSON form |
|---|---|---|
| `Null` | Field present, no value | `"null"` |
| `Missing` | Field absent (documents) | `"missing"` |
| `Bool` | Boolean | `{"bool":true}` |
| `Int` / `UInt` | 64-bit signed / unsigned | `{"int":-5}` |
| `BigInt` | Arbitrary-precision integer | `{"big_int":"123…"}` |
| `Decimal` | Arbitrary-precision decimal, scale kept | `{"decimal":"1.500"}` |
| `Float` | IEEE-754 double; NaN/±∞ allowed | `{"float":0.1}` / `{"float":"NaN"}` |
| `String` | UTF-8 text | `{"string":"…"}` |
| `Binary` | Bytes | `{"binary":"<base64>"}` |
| `Uuid`, `ObjectId` | Identifiers | hyphenated / 24 hex digits |
| `Date`, `Time`, `LocalDateTime` | Calendar values without zone | ISO 8601 |
| `UtcDateTime` | Instant in UTC | RFC 3339 |
| `ZonedDateTime { instant, zone }` | Instant with offset and optional IANA zone | RFC 3339 (+ `[Zone]` in text) |
| `Duration` | Signed, nanosecond precision | `{seconds, nanos}` |
| `Interval` | Months, days, microseconds (PostgreSQL semantics) | `{months, days, microseconds}` |
| `Array`, `Object`, `Map` | Containers; `Object` preserves field order; `Map` allows non-string keys | |
| `Enum { type_name, label }` | Enumerated label | |
| `Json` | JSON that must stay JSON | |
| `Geometry { srid, wkb }` | Geometry as Well-Known Binary | |
| `Extension { type_name, text, binary }` | Engine-specific value carried opaquely | |
| `Invalid { reason, raw }` | Unrepresentable source value; never written silently | |

`CanonicalValue` pairs a value with optional `ValueMeta`: original type and
text, precision, scale, time zone, encoding, source field, loss warnings and
connector-specific key/values. A `Record` is an ordered map of field name to
`CanonicalValue`; an absent field reads as `Missing`.

## Types

`CanonicalType` mirrors the values with parameters (integer width, decimal
precision/scale, string/binary limits, fractional-second digits, element
types) and adds `Mixed` (union of observed types) and `Unknown`.
`CanonicalType::unify` widens types when inferring document structure.

## Conversion classes

| Class | Meaning | Example |
|---|---|---|
| Lossless | Exact, reversible | `int32 → int64`, `uuid → string` |
| ConditionallyLossless | Exact when the value fits, otherwise an error | `int64 → int16`, `string → uuid` |
| PotentiallyLossy | Some values lose information | `int64 → float64`, `decimal(10,4) → decimal(10,2)` |
| DefinitelyLossy | Information is always dropped | `zoned → local datetime`, string truncation |
| Unsupported | No conversion defined | `geometry → duration` |

`classify` gives the worst case for a type pair (used in planning, Dry Run
and the mapping UI). `convert` reports the loss that actually happened for a
given value; a property test guarantees it never exceeds `classify`.

## Policies

`ConversionPolicy::strict()` (the default) permits only lossless and
conditionally lossless conversions. A job may approve
`allow_potentially_lossy` and/or `allow_definitely_lossy` and may define
`assume_utc_offset_seconds` for local-to-instant conversions. Approved losses
produce a `LossRecord { class, from, to, detail }` — kinds and descriptions
only, never data — which flows into reports.

## Notable rules

- Integers never wrap; out-of-range values are errors.
- Decimal rounding (half-even) only happens with approval; rescaling that
  keeps the numeric value is exact.
- Float conversion is lossless when the float's shortest round-trip decimal
  equals the source number (so `"0.1" → 0.1` is exact; `2^53+1 → f64` is not).
- Fractional seconds beyond the target precision are truncated only with
  approval.
- Strings are counted in Unicode scalar values; truncation is definitely
  lossy and requires approval.
- Binary that is not UTF-8 never becomes text implicitly.
- `String → Json` parses JSON text; containers become JSON exactly only when
  every nested value is JSON-native, otherwise the conversion is lossy.
- `Null`/`Missing` pass through all conversions; nullability is enforced by
  the mapping layer.
