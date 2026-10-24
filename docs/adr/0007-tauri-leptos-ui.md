# ADR 0007: Tauri 2 + Leptos desktop UI

- Status: Accepted
- Date: 2026-10-24

## Decision

- Tauri 2 hosts the app; the UI is Leptos 0.8 (client-side rendering)
  compiled to WebAssembly with Trunk. No JavaScript framework is used.
- The WebView receives only `core:default` permissions and a strict CSP
  (`script-src 'self' 'wasm-unsafe-eval'`, no remote origins). All data
  access goes through Rust commands that return redacted errors.
- IPC types live in `gazabridge-ipc` (serde only) so both sides share them.
- i18n uses JSON catalogues embedded at compile time; a test keeps the
  English and Arabic catalogues in sync. Arabic switches `dir="rtl"`; the
  stylesheet uses logical properties throughout.
- Themes (system/light/dark) and high contrast are CSS custom properties
  selected by attributes on `<html>`.
- Sections whose module is not implemented yet are rendered disabled with
  an accessible explanation; they never open a mock screen.
- Monaco may be introduced later for Query Studio if no adequate Rust
  editor exists (ADR to follow).

## Consequences

- Building the UI needs the `wasm32-unknown-unknown` target and Trunk.
  `cargo build` of the backend creates an empty `dist` directory so quality
  gates can run without the UI bundle (with a build warning).
