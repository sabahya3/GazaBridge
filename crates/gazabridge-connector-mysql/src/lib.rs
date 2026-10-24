//! MySQL-family connector — **work in progress, paused**.
//!
//! The type mapping (`types.rs`), SQL rendering (`sql.rs`), catalog
//! inspection (`meta.rs`) and error mapping (`error.rs`) are drafted but not
//! compiled yet: the read/write paths, the `Connection` implementation and
//! the factory are still missing. Work resumes after the MongoDB priority
//! track (see docs/roadmap.md, task 2.3). Nothing here is registered with
//! the application, so no user-facing feature depends on it.
