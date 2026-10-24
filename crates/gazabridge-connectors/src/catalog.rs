//! The built-in compatibility-profile catalogue.

use std::collections::BTreeMap;

use gazabridge_domain::compatibility::CompatibilityStatus;
use gazabridge_domain::connection::EngineFamily;
use gazabridge_plugin_sdk::{CompatibilityProfile, ServerInfo};
use serde::Deserialize;

const BUILTIN_PROFILES: &str = include_str!("../profiles.toml");

/// Banner words every server of a family reports; they identify the family,
/// not a specific product.
const FAMILY_BANNERS: &[&str] = &["postgresql", "mysql", "mongodb", "sqlite"];

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("profile catalogue is not valid TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("duplicate profile id `{0}`")]
    Duplicate(String),
    #[error("profile `{id}`: {reason}")]
    Invalid { id: String, reason: String },
}

#[derive(Deserialize)]
struct CatalogFile {
    profile: Vec<CompatibilityProfile>,
}

#[derive(Debug, Clone)]
pub struct ProfileCatalog {
    profiles: BTreeMap<String, CompatibilityProfile>,
}

impl ProfileCatalog {
    /// Loads and validates the profiles bundled with this build.
    pub fn builtin() -> Result<Self, CatalogError> {
        Self::from_toml(BUILTIN_PROFILES)
    }

    pub fn from_toml(text: &str) -> Result<Self, CatalogError> {
        let file: CatalogFile = toml::from_str(text)?;
        let mut profiles = BTreeMap::new();
        for p in file.profile {
            validate(&p)?;
            if profiles.insert(p.id.clone(), p.clone()).is_some() {
                return Err(CatalogError::Duplicate(p.id));
            }
        }
        Ok(Self { profiles })
    }

    pub fn get(&self, id: &str) -> Option<&CompatibilityProfile> {
        self.profiles.get(id)
    }

    pub fn all(&self) -> impl Iterator<Item = &CompatibilityProfile> {
        self.profiles.values()
    }

    pub fn by_family(&self, family: EngineFamily) -> impl Iterator<Item = &CompatibilityProfile> {
        self.profiles.values().filter(move |p| p.family == family)
    }

    /// The default profile for an engine family (e.g. `postgres.postgresql`).
    pub fn default_for(&self, family: EngineFamily) -> Option<&CompatibilityProfile> {
        let id = match family {
            EngineFamily::Postgres => "postgres.postgresql",
            EngineFamily::MySql => "mysql.mysql",
            EngineFamily::MongoDb => "mongodb.community",
            EngineFamily::Sqlite => "sqlite.sqlite",
        };
        self.get(id)
    }

    /// Picks the most specific profile for a detected server.
    ///
    /// Host-suffix matches win (hosted services report the upstream version
    /// banner), then product-banner matches on non-generic profiles, then the
    /// family default.
    pub fn detect(&self, server: &ServerInfo, host: Option<&str>) -> Option<&CompatibilityProfile> {
        let family: Vec<_> = self.by_family(server.family).collect();
        if let Some(host) = host
            && let Some(p) = family.iter().find(|p| p.matches_host(host))
        {
            return Some(p);
        }
        let haystack = format!("{} {}", server.product, server.version_string).to_ascii_lowercase();
        let specific = family.iter().find(|p| {
            p.detection
                .version_string_contains
                .iter()
                .map(|n| n.to_ascii_lowercase())
                .any(|n| !FAMILY_BANNERS.contains(&n.as_str()) && haystack.contains(&n))
        });
        specific
            .copied()
            .or_else(|| self.default_for(server.family))
    }
}

fn validate(p: &CompatibilityProfile) -> Result<(), CatalogError> {
    let invalid = |reason: &str| CatalogError::Invalid {
        id: p.id.clone(),
        reason: reason.to_owned(),
    };
    let prefix = format!("{}.", p.family.as_str());
    if !p.id.starts_with(&prefix) {
        return Err(invalid("id must start with its engine family"));
    }
    if p.display_name.trim().is_empty() {
        return Err(invalid("display_name is empty"));
    }
    if !p.documentation_url.starts_with("https://") {
        return Err(invalid("documentation_url must be an https URL"));
    }
    if p.auth_methods.is_empty() {
        return Err(invalid("at least one auth method is required"));
    }
    if p.status == CompatibilityStatus::ProductionReady {
        let Some(evidence) = &p.contract_evidence else {
            return Err(invalid(
                "production_ready requires recorded contract evidence",
            ));
        };
        if p.tested_versions.is_empty() {
            return Err(invalid("production_ready requires tested versions"));
        }
        for v in &p.tested_versions {
            if !evidence.versions.contains(v) {
                return Err(invalid("every tested version needs contract evidence"));
            }
        }
    }
    if let Some(evidence) = &p.contract_evidence {
        for v in &p.tested_versions {
            if !evidence.versions.contains(v) {
                return Err(invalid(
                    "tested_versions must be backed by contract evidence",
                ));
            }
        }
    } else if !p.tested_versions.is_empty() {
        return Err(invalid(
            "tested_versions must be backed by contract evidence",
        ));
    }
    if !p
        .capability_overrides
        .remove
        .is_disjoint(&p.capability_overrides.add)
    {
        return Err(invalid("a capability cannot be both added and removed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gazabridge_plugin_sdk::profile::Version;

    use super::*;

    #[test]
    fn builtin_catalogue_loads_and_covers_every_family() {
        let c = ProfileCatalog::builtin().unwrap();
        for family in EngineFamily::ALL {
            assert!(c.default_for(family).is_some(), "{family}");
        }
        for id in [
            "postgres.neon",
            "postgres.supabase",
            "postgres.citus",
            "postgres.edb",
            "postgres.cockroachdb",
            "postgres.generic",
            "mysql.mariadb",
            "mysql.percona",
            "mysql.tidb",
            "mysql.generic",
            "mongodb.enterprise",
            "mongodb.atlas",
            "mongodb.documentdb",
            "mongodb.generic",
        ] {
            assert!(c.get(id).is_some(), "{id}");
        }
    }

    #[test]
    fn no_builtin_profile_claims_untested_production_readiness() {
        let c = ProfileCatalog::builtin().unwrap();
        for p in c.all() {
            if p.status == CompatibilityStatus::ProductionReady {
                assert!(p.contract_evidence.is_some(), "{}", p.id);
            }
        }
    }

    #[test]
    fn production_ready_without_evidence_is_rejected() {
        let text = r#"
            [[profile]]
            id = "sqlite.fake"
            family = "sqlite"
            display_name = "Fake"
            vendor_icon = "sqlite"
            status = "production_ready"
            auth_methods = ["none"]
            documentation_url = "https://example.invalid"
        "#;
        assert!(matches!(
            ProfileCatalog::from_toml(text),
            Err(CatalogError::Invalid { .. })
        ));
    }

    fn server(family: EngineFamily, product: &str, banner: &str) -> ServerInfo {
        ServerInfo {
            family,
            product: product.into(),
            version: Version("1.0".into()),
            version_string: banner.into(),
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn detection_prefers_host_then_banner() {
        let c = ProfileCatalog::builtin().unwrap();
        let pg = server(
            EngineFamily::Postgres,
            "PostgreSQL",
            "PostgreSQL 16.4 on x86_64",
        );
        assert_eq!(
            c.detect(&pg, Some("ep-x.eu-central-1.aws.neon.tech"))
                .unwrap()
                .id,
            "postgres.neon"
        );
        assert_eq!(
            c.detect(&pg, Some("localhost")).unwrap().id,
            "postgres.postgresql"
        );

        let crdb = server(
            EngineFamily::Postgres,
            "CockroachDB",
            "CockroachDB CCL v24.2.0",
        );
        assert_eq!(c.detect(&crdb, None).unwrap().id, "postgres.cockroachdb");

        let maria = server(EngineFamily::MySql, "MariaDB", "10.11.6-MariaDB");
        assert_eq!(c.detect(&maria, None).unwrap().id, "mysql.mariadb");

        let tidb = server(EngineFamily::MySql, "TiDB", "8.0.11-TiDB-v7.5.0");
        assert_eq!(c.detect(&tidb, None).unwrap().id, "mysql.tidb");

        let mysql = server(EngineFamily::MySql, "MySQL", "8.4.2");
        assert_eq!(c.detect(&mysql, None).unwrap().id, "mysql.mysql");
    }
}
