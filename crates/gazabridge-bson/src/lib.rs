//! BSON ⇄ canonical value mapping, shared by the MongoDB connector and the
//! BSON / MongoDB Extended JSON file formats so both treat every BSON type
//! the same way.

pub mod convert;
pub mod decimal128;

pub use bson;
pub use convert::{INT32_PATHS_KEY, document_to_record, from_bson, record_to_document, to_bson};
