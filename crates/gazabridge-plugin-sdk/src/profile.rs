//! Compatibility profiles and server detection.
//!
//! Protocol compatibility does not imply feature compatibility. A profile
//! records what GazaBridge knows about a specific product (Neon, TiDB,
//! DocumentDB, …) on top of its engine family's connector.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use gazabridge_domain::compatibility::CompatibilityStatus;
use gazabridge_domain::connection::EngineFamily;
use serde::{Deserialize, Serialize};

use crate::capability::{Capability, CapabilityManifest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    None,
    Password,
    Scram,
    ClientCertificate,
    AwsIam,
    Kerberos,
    Ldap,
    Oidc,
}

/// A dotted numeric version such as `16.4` or `8.0.36`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(pub String);

impl Version {
    /// Numeric components; non-numeric suffixes (`-MariaDB`, `beta1`) are ignored.
    pub fn parts(&self) -> Vec<u64> {
        self.0
            .split(['.', '-', ' ', '+'])
            .map_while(|p| {
                let digits: String = p.chars().take_while(char::is_ascii_digit).collect();
                digits.parse().ok()
            })
            .collect()
    }

    /// Compares on the components both versions specify, so `16` matches
    /// `16.4` and `8.0` matches `8.0.36`.
    pub fn cmp_prefix(&self, other: &Version) -> Ordering {
        let a = self.parts();
        let b = other.parts();
        let n = a.len().min(b.len());
        a[..n].cmp(&b[..n])
    }

    pub fn matches_prefix(&self, prefix: &Version) -> bool {
        let p = prefix.parts();
        !p.is_empty() && self.parts().starts_with(&p)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    /// Inclusive minimum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<Version>,
    /// Exclusive maximum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_exclusive: Option<Version>,
}

impl VersionRange {
    pub fn contains(&self, v: &Version) -> bool {
        if v.parts().is_empty() {
            return false;
        }
        let above_min = self
            .min
            .as_ref()
            .is_none_or(|m| v.cmp_prefix(m) != Ordering::Less);
        let below_max = self
            .max_exclusive
            .as_ref()
            .is_none_or(|m| v.cmp_prefix(m) == Ordering::Less);
        above_min && below_max
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityOverrides {
    #[serde(default)]
    pub remove: BTreeSet<Capability>,
    #[serde(default)]
    pub add: BTreeSet<Capability>,
}

/// Connection defaults the UI pre-fills for a profile.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Engine-specific TLS mode name (`verify-full`, `VERIFY_IDENTITY`, `true`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, String>,
}

/// How to recognise the product from what the server reports.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectionRule {
    /// Case-insensitive substrings of the server version string or product
    /// name that identify this product.
    #[serde(default)]
    pub version_string_contains: Vec<String>,
    /// Case-insensitive host-name suffixes that identify a hosted service.
    #[serde(default)]
    pub host_suffixes: Vec<String>,
}

/// Evidence that the contract suite passed for this profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractEvidence {
    pub suite_version: String,
    pub versions: Vec<Version>,
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityProfile {
    pub id: String,
    pub family: EngineFamily,
    pub display_name: String,
    pub vendor_icon: String,
    pub status: CompatibilityStatus,
    #[serde(default)]
    pub defaults: ProfileDefaults,
    #[serde(default)]
    pub capability_overrides: CapabilityOverrides,
    #[serde(default)]
    pub known_limitations: Vec<String>,
    pub auth_methods: Vec<AuthMethod>,
    #[serde(default)]
    pub tested_versions: Vec<Version>,
    #[serde(default)]
    pub supported_versions: VersionRange,
    #[serde(default)]
    pub unsupported_operations: Vec<String>,
    pub documentation_url: String,
    #[serde(default)]
    pub detection: DetectionRule,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_evidence: Option<ContractEvidence>,
}

impl CompatibilityProfile {
    /// Effective manifest after applying this profile's overrides.
    pub fn apply(&self, manifest: &CapabilityManifest) -> CapabilityManifest {
        manifest.with_overrides(
            &self.capability_overrides.remove,
            &self.capability_overrides.add,
        )
    }

    pub fn matches_server(&self, server: &ServerInfo) -> bool {
        let haystack = format!("{} {}", server.product, server.version_string).to_ascii_lowercase();
        self.detection
            .version_string_contains
            .iter()
            .any(|needle| haystack.contains(&needle.to_ascii_lowercase()))
    }

    pub fn matches_host(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        self.detection
            .host_suffixes
            .iter()
            .any(|s| host.ends_with(&s.to_ascii_lowercase()))
    }

    /// Checks a detected server against this profile.
    pub fn assess(&self, server: &ServerInfo) -> CompatibilityAssessment {
        let mut findings = Vec::new();
        let mut status = self.status;

        if server.family != self.family {
            findings.push(Finding::error(format!(
                "server speaks the {} protocol but the profile is for {}",
                server.family, self.family
            )));
            status = CompatibilityStatus::UnsupportedVersion;
        }
        if !self.detection.version_string_contains.is_empty() && !self.matches_server(server) {
            findings.push(Finding::warning(format!(
                "the server reports `{}`, which does not look like {}; capabilities may differ",
                server.product, self.display_name
            )));
            status = status.max(CompatibilityStatus::LimitedCompatibility);
        }
        let range = &self.supported_versions;
        if (range.min.is_some() || range.max_exclusive.is_some())
            && !range.contains(&server.version)
        {
            findings.push(Finding::error(format!(
                "server version {} is outside the supported range for {}",
                server.version, self.display_name
            )));
            status = CompatibilityStatus::UnsupportedVersion;
        } else if !self
            .tested_versions
            .iter()
            .any(|t| server.version.matches_prefix(t))
        {
            findings.push(Finding::info(format!(
                "server version {} has not been tested with {}",
                server.version, self.display_name
            )));
        }
        CompatibilityAssessment { status, findings }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub severity: Severity,
    pub message: String,
}

impl Finding {
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Info,
            message: message.into(),
        }
    }
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            message: message.into(),
        }
    }
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityAssessment {
    pub status: CompatibilityStatus,
    pub findings: Vec<Finding>,
}

/// What a server reports about itself after connecting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    pub family: EngineFamily,
    /// Product name, e.g. `PostgreSQL`, `CockroachDB`, `MariaDB`, `MongoDB`.
    pub product: String,
    pub version: Version,
    /// Full version banner as reported by the server.
    pub version_string: String,
    /// Extra facts (e.g. `wal_level`, `binlog_format`, `replica_set`).
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> CompatibilityProfile {
        CompatibilityProfile {
            id: "postgres.postgresql".into(),
            family: EngineFamily::Postgres,
            display_name: "PostgreSQL".into(),
            vendor_icon: "postgresql".into(),
            status: CompatibilityStatus::Beta,
            defaults: ProfileDefaults::default(),
            capability_overrides: CapabilityOverrides::default(),
            known_limitations: vec![],
            auth_methods: vec![AuthMethod::Password],
            tested_versions: vec![Version("16".into()), Version("17".into())],
            supported_versions: VersionRange {
                min: Some(Version("12".into())),
                max_exclusive: None,
            },
            unsupported_operations: vec![],
            documentation_url: "https://www.postgresql.org/docs/".into(),
            detection: DetectionRule {
                version_string_contains: vec!["postgresql".into()],
                host_suffixes: vec![],
            },
            contract_evidence: None,
        }
    }

    fn server(product: &str, version: &str) -> ServerInfo {
        ServerInfo {
            family: EngineFamily::Postgres,
            product: product.into(),
            version: Version(version.into()),
            version_string: format!("{product} {version}"),
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn version_parsing_and_ranges() {
        assert_eq!(
            Version("10.11.6-MariaDB-log".into()).parts(),
            vec![10, 11, 6]
        );
        let r = VersionRange {
            min: Some(Version("12".into())),
            max_exclusive: Some(Version("18".into())),
        };
        assert!(r.contains(&Version("12.0".into())));
        assert!(r.contains(&Version("17.2".into())));
        assert!(!r.contains(&Version("11.9".into())));
        assert!(!r.contains(&Version("18.0".into())));
        assert!(!r.contains(&Version("garbage".into())));
    }

    #[test]
    fn tested_version_has_no_findings() {
        let a = profile().assess(&server("PostgreSQL", "16.4"));
        assert_eq!(a.status, CompatibilityStatus::Beta);
        assert!(a.findings.is_empty(), "{:?}", a.findings);
    }

    #[test]
    fn old_version_is_unsupported() {
        let a = profile().assess(&server("PostgreSQL", "11.2"));
        assert_eq!(a.status, CompatibilityStatus::UnsupportedVersion);
    }

    #[test]
    fn product_mismatch_limits_compatibility() {
        let a = profile().assess(&server("CockroachDB CCL", "16.0"));
        assert_eq!(a.status, CompatibilityStatus::LimitedCompatibility);
    }
}
