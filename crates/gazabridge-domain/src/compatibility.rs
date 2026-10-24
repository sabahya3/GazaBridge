//! Connector compatibility and connection-state vocabulary.

use std::fmt;

use serde::{Deserialize, Serialize};

/// How well GazaBridge supports a product.
///
/// `ProductionReady` may only be claimed when the connector contract suite
/// has passed against the listed tested versions (enforced by the profile
/// catalogue's tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityStatus {
    ProductionReady,
    Beta,
    Experimental,
    ProtocolCompatible,
    LimitedCompatibility,
    UnsupportedVersion,
}

impl CompatibilityStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::ProductionReady => "Production Ready",
            Self::Beta => "Beta",
            Self::Experimental => "Experimental",
            Self::ProtocolCompatible => "Protocol Compatible",
            Self::LimitedCompatibility => "Limited Compatibility",
            Self::UnsupportedVersion => "Unsupported Version",
        }
    }
}

impl fmt::Display for CompatibilityStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// The state of a saved connection as shown in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Ready,
    ConfigurationRequired,
    ConnectionFailed,
    UnsupportedVersion,
    LimitedCompatibility,
    ExternalToolMissing,
}

impl ConnectionState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::ConfigurationRequired => "Configuration Required",
            Self::ConnectionFailed => "Connection Failed",
            Self::UnsupportedVersion => "Unsupported Version",
            Self::LimitedCompatibility => "Limited Compatibility",
            Self::ExternalToolMissing => "External Tool Missing",
        }
    }
}

/// How a synchronization job detects changes. Shown verbatim in the UI so
/// users always know what guarantees they get.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMethod {
    NativeCdc,
    ChangeStream,
    LogicalReplication,
    Binlog,
    Polling,
    TimestampIncremental,
    KeyIncremental,
}

impl SyncMethod {
    pub fn label(&self) -> &'static str {
        match self {
            Self::NativeCdc => "Native CDC",
            Self::ChangeStream => "Change Stream",
            Self::LogicalReplication => "Logical Replication",
            Self::Binlog => "Binlog",
            Self::Polling => "Polling",
            Self::TimestampIncremental => "Timestamp Incremental",
            Self::KeyIncremental => "Key Incremental",
        }
    }

    /// Whether the method observes deletes without application help.
    pub fn captures_deletes(&self) -> bool {
        matches!(
            self,
            Self::NativeCdc | Self::ChangeStream | Self::LogicalReplication | Self::Binlog
        )
    }
}
