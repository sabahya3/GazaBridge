//! MongoDB query model shared by the UI (compiled to wasm) and the backend:
//!
//! * [`shell`]: reads mongosh / Compass query syntax into Extended JSON;
//! * [`builder`]: the visual query builder (AND/OR groups, operators, typed
//!   values), sort keys and projections, with exact conversion to and from
//!   query documents.

pub mod builder;
pub mod shell;
