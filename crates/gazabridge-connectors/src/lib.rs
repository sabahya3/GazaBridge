//! Connector registry and compatibility-profile catalogue.

pub mod catalog;
pub mod registry;

pub use catalog::{CatalogError, ProfileCatalog};
pub use registry::{ConnectorRegistry, RegistryError};
