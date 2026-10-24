# ADR 0004: Compatibility profiles and honest status

- Status: Accepted
- Date: 2026-10-24

## Context

Neon, Supabase, CockroachDB, TiDB, DocumentDB and others speak a familiar
wire protocol but differ in features. Users need to know what works before
they rely on it.

## Decision

- Products are described by compatibility profiles in
  `crates/gazabridge-connectors/profiles.toml`: display name, icon, defaults,
  capability overrides, known limitations, auth methods, tested versions,
  supported version range, unsupported operations, documentation URL,
  detection rules and contract evidence.
- The catalogue loader enforces: `production_ready` requires contract
  evidence covering every tested version; `tested_versions` must be backed
  by evidence; ids are prefixed with their engine family.
- After connecting, the server's product and version are detected and
  assessed against the selected profile (`CompatibilityProfile::assess`),
  producing findings and an effective status (e.g. `UnsupportedVersion`,
  `LimitedCompatibility`).
- Until contract evidence exists, all built-in profiles are `experimental`,
  `limited_compatibility` or (generic profiles) `protocol_compatible`.

## Consequences

- Status labels in the UI are trustworthy; promoting a profile is a
  reviewed change that adds evidence.
- CI must record contract-suite runs per version to promote profiles.
