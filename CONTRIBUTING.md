# Contributing to GazaBridge

Thank you for helping. A few rules keep the project trustworthy.

## Ground rules

1. **No placeholders in user-facing code.** A button, menu item or CLI flag
   must work, or it must not exist (or be shown disabled with a reason).
2. **No silent data changes.** Conversions go through
   `gazabridge_domain::conversion`; anything lossy must be approved by
   policy and recorded.
3. **No secrets in code, tests, fixtures, logs or screenshots.** Use obviously
   fake values. Never connect tests to real user databases.
4. **Honest compatibility.** A compatibility profile may only list a version
   under `tested_versions`, or claim `production_ready`, with recorded
   contract-suite evidence. The catalogue tests enforce this.
5. **Record decisions.** Significant design choices get an ADR in
   `docs/adr/`.

## Before opening a pull request

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

New behaviour needs tests. Connector changes must keep the contract suite in
`gazabridge-testkit` passing.

## Code style

- Library code avoids `unwrap`/`expect` (clippy warns); tests may use them.
- `unsafe` is forbidden workspace-wide.
- Errors crossing a process or IPC boundary are redacted.
- Log operations and counts, never record contents.

## Commit messages

Use the imperative mood ("Add MySQL binlog reader") and explain *why* in the
body when it is not obvious.
