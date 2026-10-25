//! Core domain model for GazaBridge.
//!
//! This crate is deliberately free of I/O, async runtimes, UI frameworks and
//! database drivers. Everything here is pure data and pure logic so it can be
//! shared by connectors, the migration engine, the CLI and the desktop app.

pub mod compatibility;
pub mod connection;
pub mod connection_string;
pub mod conversion;
pub mod ejson;
pub mod ids;
pub mod job;
pub mod types;
pub mod value;

mod serde_helpers;

pub use conversion::{ConversionClass, ConversionError, ConversionPolicy, Converted, LossRecord};
pub use types::CanonicalType;
pub use value::{CanonicalValue, Record, Value, ValueKind, ValueMeta};
