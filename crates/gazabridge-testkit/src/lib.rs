//! Shared testing utilities.
//!
//! [`contract`] is the connector contract suite. Every connector runs it
//! against real servers (Testcontainers) or real files (SQLite); a profile may
//! only be promoted past `experimental` with a recorded passing run.
//!
//! [`fixtures`] provides canonical values covering the edge cases the
//! specification calls out: Unicode and Arabic text, large text and binary,
//! decimal precision, integer limits, time zones and DST boundaries, and
//! null versus missing.

pub mod contract;
pub mod fixtures;

pub use contract::{CaseOutcome, ContractHarness, ContractReport, run_contract_suite};
