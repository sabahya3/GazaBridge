//! The GazaBridge connector SDK.
//!
//! A connector is a [`ConnectorFactory`] that opens [`Connection`]s. Every
//! connector publishes a [`CapabilityManifest`]; callers (and the UI) must
//! consult it before invoking an operation, and connectors must return
//! [`ConnectorError::Unsupported`] for every capability they do not declare.
//! The contract test-suite in `gazabridge-testkit` enforces both directions.
//!
//! The API is versioned with [`API_VERSION`]. Minor versions only add
//! methods with default implementations; major versions may break.

pub mod capability;
pub mod cdc;
pub mod connector;
pub mod ddl;
pub mod error;
pub mod filter;
pub mod profile;
pub mod query;
pub mod read;
pub mod schema;
pub mod write;

pub use capability::{Capability, CapabilityManifest};
pub use connector::{Connection, ConnectorDescriptor, ConnectorFactory, ResolvedSecrets};
pub use error::{ConnectorError, ConnectorResult, ErrorKind};
pub use profile::{CompatibilityProfile, ServerInfo};

use serde::{Deserialize, Serialize};

/// Semantic version of the connector API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ApiVersion {
    pub major: u16,
    pub minor: u16,
}

impl ApiVersion {
    /// A connector built against `self` can be hosted by `host` when the
    /// major versions match and the host is at least as new.
    pub fn is_compatible_with_host(&self, host: ApiVersion) -> bool {
        self.major == host.major && self.minor <= host.minor
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

pub const API_VERSION: ApiVersion = ApiVersion { major: 1, minor: 0 };

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_compatibility_rules() {
        let host = ApiVersion { major: 1, minor: 2 };
        assert!(ApiVersion { major: 1, minor: 0 }.is_compatible_with_host(host));
        assert!(!ApiVersion { major: 1, minor: 3 }.is_compatible_with_host(host));
        assert!(!ApiVersion { major: 2, minor: 0 }.is_compatible_with_host(host));
    }
}
