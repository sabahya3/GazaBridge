# ADR 0005: Secrets only by reference

- Status: Accepted
- Date: 2026-10-24

## Decision

- Connection profiles hold `SecretRef`s (`Keychain`, `Session`, `Env`,
  `Prompt`, `None`), never secret values. The type system makes it
  impossible to put a password into a profile.
- `keyring` provides the OS stores: Keychain (macOS), Credential Manager
  (Windows), Secret Service (Linux, pure-Rust crypto).
- Session-only passwords live in a `SessionStore` (zeroized on removal).
- Environment references support CI/CD and headless use.
- `SecretResolver` registers every resolved value with the `Redactor`, so
  that value is masked in all later log lines and errors.
- Pasted connection strings are parsed into secret-free settings plus a
  zeroized password, which goes straight to the secret store; stored MongoDB
  URIs are rejected if they still contain a password.
- The metadata schema rejects a plaintext `password` (defence in depth), and
  a `secret_refs` index allows secrets to be deleted with their connection.
- Duplicating a connection re-keys its keychain references so two profiles
  never share (and accidentally delete) one entry.

## Consequences

- Exported connection definitions contain no secrets by default.
- Headless Linux without a Secret Service daemon must use session or
  environment secrets; the app reports the store as unavailable.
