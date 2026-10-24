//! Job and run lifecycle.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Lifecycle state shared by job definitions and runs.
///
/// Definitions move through `Draft → Validating → Ready → Scheduled`; a run
/// moves through `Running`, the pause/cancel states and a terminal state.
/// Terminal states can only be left by starting a new attempt (restart or
/// retry), which re-enters `Validating`, `Ready` or `Running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Draft,
    Validating,
    Ready,
    Scheduled,
    Running,
    Pausing,
    Paused,
    Resuming,
    Cancelling,
    Cancelled,
    Failed,
    CompletedWithWarnings,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job state transition {from} → {to}")]
pub struct InvalidTransition {
    pub from: JobStatus,
    pub to: JobStatus,
}

impl JobStatus {
    pub const ALL: [JobStatus; 13] = [
        Self::Draft,
        Self::Validating,
        Self::Ready,
        Self::Scheduled,
        Self::Running,
        Self::Pausing,
        Self::Paused,
        Self::Resuming,
        Self::Cancelling,
        Self::Cancelled,
        Self::Failed,
        Self::CompletedWithWarnings,
        Self::Completed,
    ];

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Cancelled | Self::Failed | Self::CompletedWithWarnings | Self::Completed
        )
    }

    /// True while an engine task owns the run.
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Validating | Self::Running | Self::Pausing | Self::Resuming | Self::Cancelling
        )
    }

    pub fn can_transition_to(&self, next: JobStatus) -> bool {
        use JobStatus::*;
        matches!(
            (self, next),
            (Draft, Validating | Draft)
                | (Validating, Ready | Draft | Failed | Cancelling)
                | (Ready, Validating | Scheduled | Running | Draft)
                | (Scheduled, Running | Ready | Validating)
                | (
                    Running,
                    Pausing | Cancelling | Failed | Completed | CompletedWithWarnings
                )
                | (Pausing, Paused | Cancelling | Failed)
                | (Paused, Resuming | Cancelling)
                | (Resuming, Running | Paused | Failed | Cancelling)
                | (Cancelling, Cancelled | Failed)
                | (
                    Cancelled | Failed | Completed | CompletedWithWarnings,
                    Ready | Validating | Running | Draft,
                )
        )
    }

    pub fn transition(self, next: JobStatus) -> Result<JobStatus, InvalidTransition> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(InvalidTransition {
                from: self,
                to: next,
            })
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Validating => "validating",
            Self::Ready => "ready",
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Resuming => "resuming",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::CompletedWithWarnings => "completed_with_warnings",
            Self::Completed => "completed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a job does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Migration,
    Import,
    Export,
    Convert,
    Sync,
    Replication,
    Backup,
    Restore,
    Validation,
}

impl JobKind {
    pub const ALL: [JobKind; 9] = [
        Self::Migration,
        Self::Import,
        Self::Export,
        Self::Convert,
        Self::Sync,
        Self::Replication,
        Self::Backup,
        Self::Restore,
        Self::Validation,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Migration => "migration",
            Self::Import => "import",
            Self::Export => "export",
            Self::Convert => "convert",
            Self::Sync => "sync",
            Self::Replication => "replication",
            Self::Backup => "backup",
            Self::Restore => "restore",
            Self::Validation => "validation",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == s)
    }
}

/// How records are written to the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Plain insert; conflicts fail the batch.
    Insert,
    /// Insert into a target that may already contain data.
    Append,
    /// Insert or update by key.
    Upsert,
    /// Upsert plus deletion of target rows absent from the source.
    Merge,
    /// Drop and recreate the target object. Destructive.
    Replace,
    /// Remove all target rows, then insert. Destructive.
    TruncateAndInsert,
    /// Insert, ignoring rows whose key already exists.
    SkipExisting,
    /// Insert, failing the job on the first conflict.
    FailOnConflict,
}

impl WriteMode {
    pub fn is_destructive(&self) -> bool {
        matches!(self, Self::Replace | Self::TruncateAndInsert | Self::Merge)
    }

    pub fn requires_key(&self) -> bool {
        matches!(self, Self::Upsert | Self::Merge | Self::SkipExisting)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_is_allowed() {
        let s = JobStatus::Draft
            .transition(JobStatus::Validating)
            .and_then(|s| s.transition(JobStatus::Ready))
            .and_then(|s| s.transition(JobStatus::Running))
            .and_then(|s| s.transition(JobStatus::Pausing))
            .and_then(|s| s.transition(JobStatus::Paused))
            .and_then(|s| s.transition(JobStatus::Resuming))
            .and_then(|s| s.transition(JobStatus::Running))
            .and_then(|s| s.transition(JobStatus::Completed))
            .unwrap();
        assert!(s.is_terminal());
    }

    #[test]
    fn draft_cannot_jump_to_running() {
        assert!(JobStatus::Draft.transition(JobStatus::Running).is_err());
        assert!(JobStatus::Paused.transition(JobStatus::Completed).is_err());
        assert!(JobStatus::Completed.transition(JobStatus::Paused).is_err());
    }

    #[test]
    fn every_non_terminal_active_state_can_be_cancelled_or_fail() {
        for s in JobStatus::ALL {
            if s.is_active() && s != JobStatus::Cancelling {
                assert!(
                    s.can_transition_to(JobStatus::Cancelling)
                        || s.can_transition_to(JobStatus::Failed),
                    "{s}"
                );
            }
        }
    }

    #[test]
    fn status_strings_round_trip() {
        for s in JobStatus::ALL {
            assert_eq!(JobStatus::parse(s.as_str()), Some(s));
        }
    }
}
