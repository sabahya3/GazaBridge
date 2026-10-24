//! Strongly typed identifiers.
//!
//! All identifiers are UUIDv7 so they sort by creation time, which keeps
//! metadata tables and logs naturally ordered.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a new time-ordered identifier.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            /// Short prefix used in human-facing output, e.g. `run_`.
            pub const PREFIX: &'static str = $prefix;
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}{}", $prefix, self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let raw = s.strip_prefix($prefix).unwrap_or(s);
                Uuid::parse_str(raw).map(Self)
            }
        }
    };
}

define_id!(
    /// Identifies a saved connection profile.
    ConnectionId,
    "conn_"
);
define_id!(
    /// Identifies a connection folder.
    FolderId,
    "fold_"
);
define_id!(
    /// Identifies a job definition.
    JobId,
    "job_"
);
define_id!(
    /// Identifies a single execution of a job.
    RunId,
    "run_"
);
define_id!(
    /// Identifies a mapping (all versions share the id).
    MappingId,
    "map_"
);
define_id!(
    /// Identifies a transformation pipeline (all versions share the id).
    TransformationId,
    "xfm_"
);
define_id!(
    /// Identifies a schedule.
    ScheduleId,
    "sch_"
);
define_id!(
    /// Identifies a saved query.
    SavedQueryId,
    "qry_"
);
define_id!(
    /// Identifies an audit event.
    AuditEventId,
    "aud_"
);
define_id!(
    /// Correlates log lines and events belonging to one logical operation.
    CorrelationId,
    "cor_"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_with_and_without_prefix() {
        let id = RunId::new();
        let plain = id.to_string();
        let prefixed = format!("{id:?}");
        assert!(prefixed.starts_with("run_"));
        assert_eq!(plain.parse::<RunId>().unwrap(), id);
        assert_eq!(prefixed.parse::<RunId>().unwrap(), id);
    }

    #[test]
    fn ids_are_time_ordered() {
        let a = JobId::new();
        let b = JobId::new();
        assert!(a < b);
    }
}
