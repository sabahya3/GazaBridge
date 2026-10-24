# ADR 0008: Redaction at the log writer

- Status: Accepted
- Date: 2026-10-24

## Decision

- Logs are JSON lines written by `tracing-subscriber` through a non-blocking
  writer to a `RedactingWriter`, then a `SizeRotatingFile`.
- Redaction happens on the formatted line, so no code path (including
  third-party crates' log events) can bypass it.
- The redactor masks URI user-info passwords, secret-looking `key=value` and
  JSON fields, and every secret value registered during the session.
- Rotated files beyond the retention period are deleted at start-up.
- Span fields `job_id`, `run_id` and `correlation_id` enable filtering in the
  log viewer.
- Record data is never logged; this is a coding rule checked in review.

## Consequences

- Very short secrets (< 4 bytes) are not masked by value, only by structure,
  to avoid destroying unrelated text.
