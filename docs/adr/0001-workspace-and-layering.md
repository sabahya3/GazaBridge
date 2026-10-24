# ADR 0001: Workspace and layering

- Status: Accepted
- Date: 2026-10-24

## Context

The product must run as a desktop app now and as a CLI, daemon or REST API
later, without rewriting the engine. The specification proposes about 25
crates.

## Decision

- One Cargo workspace with hexagonal layering: a pure `domain` crate, a
  connector SDK defining ports, infrastructure crates (storage, secrets,
  observability, connectors) as adapters, and thin drivers (desktop, CLI).
- The engine crates never depend on Tauri, Leptos or `gazabridge-ipc`.
- Crates are created in the milestone that implements them. We do not
  commit empty crates; the specification's layout remains the target.
- Workspace lints forbid `unsafe` and warn on `unwrap`/`expect` in library
  code.

## Consequences

- Each crate can be tested in isolation; the domain has no I/O.
- The workspace grows over the milestones; `docs/architecture.md` lists
  planned crates explicitly so the gap is visible.
