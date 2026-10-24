//! Jobs, runs, run events and checkpoints.
//!
//! State changes go through [`JobStatus::transition`], so the database can
//! never record an impossible lifecycle step.

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::{ConnectionId, JobId, MappingId, RunId, TransformationId};
use gazabridge_domain::job::{JobKind, JobStatus};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use crate::{MetadataStore, StorageError, StorageResult, now, parse_id};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub name: String,
    pub kind: JobKind,
    pub status: JobStatus,
    pub definition: serde_json::Value,
    pub source_connection_id: Option<ConnectionId>,
    pub target_connection_id: Option<ConnectionId>,
    pub mapping: Option<(MappingId, i64)>,
    pub transformation: Option<(TransformationId, i64)>,
    pub dry_run_passed_at: Option<DateTime<Utc>>,
    pub archived: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub name: String,
    pub kind: JobKind,
    pub definition: serde_json::Value,
    pub source_connection_id: Option<ConnectionId>,
    pub target_connection_id: Option<ConnectionId>,
    pub mapping: Option<(MappingId, i64)>,
    pub transformation: Option<(TransformationId, i64)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    pub job_id: JobId,
    pub attempt: i64,
    pub status: JobStatus,
    pub dry_run: bool,
    pub definition_snapshot: serde_json::Value,
    /// Hash of source, target, mapping and transformation configuration.
    /// Resuming requires the current fingerprint to match.
    pub config_fingerprint: String,
    pub mapping_version: Option<i64>,
    pub transformation_version: Option<i64>,
    pub batch_settings: serde_json::Value,
    pub concurrency: i64,
    pub retry_policy: serde_json::Value,
    pub stats: serde_json::Value,
    pub validation: Option<serde_json::Value>,
    pub warning_count: i64,
    pub error_count: i64,
    pub error_summary: Option<String>,
    pub app_version: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewRun {
    pub job_id: JobId,
    pub dry_run: bool,
    pub definition_snapshot: serde_json::Value,
    pub config_fingerprint: String,
    pub mapping_version: Option<i64>,
    pub transformation_version: Option<i64>,
    pub batch_settings: serde_json::Value,
    pub concurrency: i64,
    pub retry_policy: serde_json::Value,
    pub app_version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventLevel {
    Info,
    Warning,
    Error,
}

impl EventLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }

    fn parse(s: &str) -> StorageResult<Self> {
        match s {
            "info" => Ok(Self::Info),
            "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            _ => Err(StorageError::Corrupt("unknown event level".into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEvent {
    pub id: i64,
    pub at: DateTime<Utc>,
    pub level: EventLevel,
    pub code: String,
    pub stream: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub run_id: RunId,
    /// Stream key, e.g. `public.customers` or `shop.orders`.
    pub stream: String,
    pub position: Option<serde_json::Value>,
    pub records_committed: i64,
    pub bytes_committed: i64,
    pub completed: bool,
    pub config_fingerprint: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn jobs(&self) -> JobRepo<'_> {
        JobRepo { store: self }
    }
}

fn parse_status(s: &str) -> StorageResult<JobStatus> {
    JobStatus::parse(s).ok_or_else(|| StorageError::Corrupt(format!("unknown job status `{s}`")))
}

fn opt_id<T: std::str::FromStr>(
    raw: Option<String>,
    what: &'static str,
) -> StorageResult<Option<T>> {
    raw.map(|r| parse_id(&r, what)).transpose()
}

fn row_to_job(r: &SqliteRow) -> StorageResult<JobRecord> {
    let kind: String = r.try_get("kind")?;
    let mapping_id: Option<String> = r.try_get("mapping_id")?;
    let mapping_version: Option<i64> = r.try_get("mapping_version")?;
    let xfm_id: Option<String> = r.try_get("transformation_id")?;
    let xfm_version: Option<i64> = r.try_get("transformation_version")?;
    Ok(JobRecord {
        id: parse_id(&r.try_get::<String, _>("id")?, "job")?,
        name: r.try_get("name")?,
        kind: JobKind::parse(&kind)
            .ok_or_else(|| StorageError::Corrupt("unknown job kind".into()))?,
        status: parse_status(&r.try_get::<String, _>("status")?)?,
        definition: serde_json::from_str(&r.try_get::<String, _>("definition_json")?)?,
        source_connection_id: opt_id(r.try_get("source_connection_id")?, "connection")?,
        target_connection_id: opt_id(r.try_get("target_connection_id")?, "connection")?,
        mapping: match (opt_id(mapping_id, "mapping")?, mapping_version) {
            (Some(id), Some(v)) => Some((id, v)),
            _ => None,
        },
        transformation: match (opt_id(xfm_id, "transformation")?, xfm_version) {
            (Some(id), Some(v)) => Some((id, v)),
            _ => None,
        },
        dry_run_passed_at: r.try_get("dry_run_passed_at")?,
        archived: r.try_get("archived")?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

fn row_to_run(r: &SqliteRow) -> StorageResult<RunRecord> {
    let validation: Option<String> = r.try_get("validation_json")?;
    Ok(RunRecord {
        id: parse_id(&r.try_get::<String, _>("id")?, "run")?,
        job_id: parse_id(&r.try_get::<String, _>("job_id")?, "job")?,
        attempt: r.try_get("attempt")?,
        status: parse_status(&r.try_get::<String, _>("status")?)?,
        dry_run: r.try_get("dry_run")?,
        definition_snapshot: serde_json::from_str(&r.try_get::<String, _>("definition_snapshot")?)?,
        config_fingerprint: r.try_get("config_fingerprint")?,
        mapping_version: r.try_get("mapping_version")?,
        transformation_version: r.try_get("transformation_version")?,
        batch_settings: serde_json::from_str(&r.try_get::<String, _>("batch_settings_json")?)?,
        concurrency: r.try_get("concurrency")?,
        retry_policy: serde_json::from_str(&r.try_get::<String, _>("retry_policy_json")?)?,
        stats: serde_json::from_str(&r.try_get::<String, _>("stats_json")?)?,
        validation: validation.map(|v| serde_json::from_str(&v)).transpose()?,
        warning_count: r.try_get("warning_count")?,
        error_count: r.try_get("error_count")?,
        error_summary: r.try_get("error_summary")?,
        app_version: r.try_get("app_version")?,
        created_at: r.try_get("created_at")?,
        started_at: r.try_get("started_at")?,
        ended_at: r.try_get("ended_at")?,
    })
}

impl JobRepo<'_> {
    pub async fn create(&self, job: NewJob) -> StorageResult<JobRecord> {
        let id = JobId::new();
        let ts = now();
        sqlx::query(
            "INSERT INTO jobs (id, name, kind, status, definition_json, source_connection_id, target_connection_id,
                 mapping_id, mapping_version, transformation_id, transformation_version, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(job.name.trim())
        .bind(job.kind.as_str())
        .bind(JobStatus::Draft.as_str())
        .bind(serde_json::to_string(&job.definition)?)
        .bind(job.source_connection_id.map(|c| c.to_string()))
        .bind(job.target_connection_id.map(|c| c.to_string()))
        .bind(job.mapping.map(|(m, _)| m.to_string()))
        .bind(job.mapping.map(|(_, v)| v))
        .bind(job.transformation.map(|(t, _)| t.to_string()))
        .bind(job.transformation.map(|(_, v)| v))
        .bind(ts)
        .bind(ts)
        .execute(self.store.pool())
        .await?;
        self.require(id).await
    }

    pub async fn get(&self, id: JobId) -> StorageResult<Option<JobRecord>> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.store.pool())
            .await?;
        row.as_ref().map(row_to_job).transpose()
    }

    pub async fn require(&self, id: JobId) -> StorageResult<JobRecord> {
        self.get(id).await?.ok_or(StorageError::NotFound("job"))
    }

    pub async fn find_by_name(&self, name: &str) -> StorageResult<Option<JobRecord>> {
        let row = sqlx::query(
            "SELECT * FROM jobs WHERE name = ? COLLATE NOCASE AND archived = 0 LIMIT 1",
        )
        .bind(name)
        .fetch_optional(self.store.pool())
        .await?;
        row.as_ref().map(row_to_job).transpose()
    }

    pub async fn list(&self, include_archived: bool) -> StorageResult<Vec<JobRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM jobs WHERE (? = 1 OR archived = 0) ORDER BY updated_at DESC",
        )
        .bind(include_archived)
        .fetch_all(self.store.pool())
        .await?;
        rows.iter().map(row_to_job).collect()
    }

    /// Replaces the definition. Any edit invalidates a previous Dry Run and
    /// returns the job to `Draft`.
    pub async fn update_definition(
        &self,
        id: JobId,
        definition: &serde_json::Value,
    ) -> StorageResult<JobRecord> {
        let job = self.require(id).await?;
        if job.status.is_active() {
            return Err(StorageError::InvalidState(
                "cannot edit a job while it is running".into(),
            ));
        }
        sqlx::query(
            "UPDATE jobs SET definition_json = ?, status = 'draft', dry_run_passed_at = NULL, updated_at = ? WHERE id = ?",
        )
        .bind(serde_json::to_string(definition)?)
        .bind(now())
        .bind(id.to_string())
        .execute(self.store.pool())
        .await?;
        self.require(id).await
    }

    /// Moves a job to `next`, rejecting invalid transitions. Uses a
    /// compare-and-set on the current status to avoid lost updates.
    pub async fn transition(&self, id: JobId, next: JobStatus) -> StorageResult<JobRecord> {
        let job = self.require(id).await?;
        job.status
            .transition(next)
            .map_err(|e| StorageError::InvalidState(e.to_string()))?;
        let r =
            sqlx::query("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ? AND status = ?")
                .bind(next.as_str())
                .bind(now())
                .bind(id.to_string())
                .bind(job.status.as_str())
                .execute(self.store.pool())
                .await?;
        if r.rows_affected() == 0 {
            return Err(StorageError::InvalidState(
                "job status changed concurrently".into(),
            ));
        }
        self.require(id).await
    }

    pub async fn mark_dry_run_passed(&self, id: JobId) -> StorageResult<()> {
        sqlx::query("UPDATE jobs SET dry_run_passed_at = ?, updated_at = ? WHERE id = ?")
            .bind(now())
            .bind(now())
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn set_archived(&self, id: JobId, archived: bool) -> StorageResult<()> {
        let job = self.require(id).await?;
        if archived && job.status.is_active() {
            return Err(StorageError::InvalidState(
                "cannot archive a running job".into(),
            ));
        }
        sqlx::query("UPDATE jobs SET archived = ?, updated_at = ? WHERE id = ?")
            .bind(archived)
            .bind(now())
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    /// Copies a job definition into a new `Draft` job.
    pub async fn clone_job(&self, id: JobId, new_name: &str) -> StorageResult<JobRecord> {
        let job = self.require(id).await?;
        self.create(NewJob {
            name: new_name.to_owned(),
            kind: job.kind,
            definition: job.definition,
            source_connection_id: job.source_connection_id,
            target_connection_id: job.target_connection_id,
            mapping: job.mapping,
            transformation: job.transformation,
        })
        .await
    }

    // -------------------------------------------------------------- runs

    pub async fn create_run(&self, run: NewRun) -> StorageResult<RunRecord> {
        let id = RunId::new();
        let attempt: i64 = sqlx::query_scalar("SELECT COUNT(*) + 1 FROM job_runs WHERE job_id = ?")
            .bind(run.job_id.to_string())
            .fetch_one(self.store.pool())
            .await?;
        sqlx::query(
            "INSERT INTO job_runs (id, job_id, attempt, status, dry_run, definition_snapshot, config_fingerprint,
                 mapping_version, transformation_version, batch_settings_json, concurrency, retry_policy_json,
                 app_version, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(run.job_id.to_string())
        .bind(attempt)
        .bind(JobStatus::Ready.as_str())
        .bind(run.dry_run)
        .bind(serde_json::to_string(&run.definition_snapshot)?)
        .bind(&run.config_fingerprint)
        .bind(run.mapping_version)
        .bind(run.transformation_version)
        .bind(serde_json::to_string(&run.batch_settings)?)
        .bind(run.concurrency.max(1))
        .bind(serde_json::to_string(&run.retry_policy)?)
        .bind(&run.app_version)
        .bind(now())
        .execute(self.store.pool())
        .await?;
        self.require_run(id).await
    }

    pub async fn get_run(&self, id: RunId) -> StorageResult<Option<RunRecord>> {
        let row = sqlx::query("SELECT * FROM job_runs WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.store.pool())
            .await?;
        row.as_ref().map(row_to_run).transpose()
    }

    pub async fn require_run(&self, id: RunId) -> StorageResult<RunRecord> {
        self.get_run(id).await?.ok_or(StorageError::NotFound("run"))
    }

    pub async fn runs_for(&self, job_id: JobId, limit: u32) -> StorageResult<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM job_runs WHERE job_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(job_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter().map(row_to_run).collect()
    }

    /// Runs left in an active state, e.g. after the application was killed.
    pub async fn interrupted_runs(&self) -> StorageResult<Vec<RunRecord>> {
        let rows = sqlx::query(
            "SELECT * FROM job_runs WHERE status IN ('validating', 'running', 'pausing', 'resuming', 'cancelling')",
        )
        .fetch_all(self.store.pool())
        .await?;
        rows.iter().map(row_to_run).collect()
    }

    /// Moves a run to `next` with compare-and-set; stamps start/end times.
    pub async fn transition_run(&self, id: RunId, next: JobStatus) -> StorageResult<RunRecord> {
        let run = self.require_run(id).await?;
        run.status
            .transition(next)
            .map_err(|e| StorageError::InvalidState(e.to_string()))?;
        let ts = now();
        let started = (next == JobStatus::Running && run.started_at.is_none()).then_some(ts);
        let ended = next.is_terminal().then_some(ts);
        let r = sqlx::query(
            "UPDATE job_runs SET status = ?, started_at = COALESCE(started_at, ?), ended_at = COALESCE(?, ended_at)
             WHERE id = ? AND status = ?",
        )
        .bind(next.as_str())
        .bind(started)
        .bind(ended)
        .bind(id.to_string())
        .bind(run.status.as_str())
        .execute(self.store.pool())
        .await?;
        if r.rows_affected() == 0 {
            return Err(StorageError::InvalidState(
                "run status changed concurrently".into(),
            ));
        }
        self.require_run(id).await
    }

    pub async fn update_run_stats(
        &self,
        id: RunId,
        stats: &serde_json::Value,
    ) -> StorageResult<()> {
        sqlx::query("UPDATE job_runs SET stats_json = ? WHERE id = ?")
            .bind(serde_json::to_string(stats)?)
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn set_run_validation(
        &self,
        id: RunId,
        validation: &serde_json::Value,
    ) -> StorageResult<()> {
        sqlx::query("UPDATE job_runs SET validation_json = ? WHERE id = ?")
            .bind(serde_json::to_string(validation)?)
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    pub async fn set_run_error_summary(&self, id: RunId, summary: &str) -> StorageResult<()> {
        sqlx::query("UPDATE job_runs SET error_summary = ? WHERE id = ?")
            .bind(summary)
            .bind(id.to_string())
            .execute(self.store.pool())
            .await?;
        Ok(())
    }

    /// Appends a run event and bumps the run's warning/error counters.
    pub async fn add_event(
        &self,
        run_id: RunId,
        level: EventLevel,
        code: &str,
        stream: Option<&str>,
        message: &str,
    ) -> StorageResult<()> {
        let mut tx = self.store.pool().begin().await?;
        sqlx::query("INSERT INTO run_events (run_id, at, level, code, stream, message) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(run_id.to_string())
            .bind(now())
            .bind(level.as_str())
            .bind(code)
            .bind(stream)
            .bind(message)
            .execute(&mut *tx)
            .await?;
        let counter = match level {
            EventLevel::Warning => {
                Some("UPDATE job_runs SET warning_count = warning_count + 1 WHERE id = ?")
            }
            EventLevel::Error => {
                Some("UPDATE job_runs SET error_count = error_count + 1 WHERE id = ?")
            }
            EventLevel::Info => None,
        };
        if let Some(sql) = counter {
            sqlx::query(sql)
                .bind(run_id.to_string())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn events(&self, run_id: RunId, limit: u32) -> StorageResult<Vec<RunEvent>> {
        let rows = sqlx::query(
            "SELECT id, at, level, code, stream, message FROM run_events WHERE run_id = ? ORDER BY id LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter()
            .map(|r| {
                Ok(RunEvent {
                    id: r.try_get("id")?,
                    at: r.try_get("at")?,
                    level: EventLevel::parse(&r.try_get::<String, _>("level")?)?,
                    code: r.try_get("code")?,
                    stream: r.try_get("stream")?,
                    message: r.try_get("message")?,
                })
            })
            .collect()
    }

    // -------------------------------------------------------- checkpoints

    /// Records progress for one stream. Checkpoints must only be saved after
    /// the corresponding target writes are durable.
    pub async fn save_checkpoint(&self, cp: &Checkpoint) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO checkpoints (run_id, stream, position_json, records_committed, bytes_committed, completed,
                 config_fingerprint, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(run_id, stream) DO UPDATE SET
                 position_json = excluded.position_json, records_committed = excluded.records_committed,
                 bytes_committed = excluded.bytes_committed, completed = excluded.completed,
                 config_fingerprint = excluded.config_fingerprint, updated_at = excluded.updated_at",
        )
        .bind(cp.run_id.to_string())
        .bind(&cp.stream)
        .bind(cp.position.as_ref().map(serde_json::to_string).transpose()?)
        .bind(cp.records_committed)
        .bind(cp.bytes_committed)
        .bind(cp.completed)
        .bind(&cp.config_fingerprint)
        .bind(cp.updated_at)
        .execute(self.store.pool())
        .await?;
        Ok(())
    }

    pub async fn checkpoints(&self, run_id: RunId) -> StorageResult<Vec<Checkpoint>> {
        let rows = sqlx::query("SELECT * FROM checkpoints WHERE run_id = ? ORDER BY stream")
            .bind(run_id.to_string())
            .fetch_all(self.store.pool())
            .await?;
        rows.iter()
            .map(|r| {
                let pos: Option<String> = r.try_get("position_json")?;
                Ok(Checkpoint {
                    run_id: parse_id(&r.try_get::<String, _>("run_id")?, "run")?,
                    stream: r.try_get("stream")?,
                    position: pos.map(|p| serde_json::from_str(&p)).transpose()?,
                    records_committed: r.try_get("records_committed")?,
                    bytes_committed: r.try_get("bytes_committed")?,
                    completed: r.try_get("completed")?,
                    config_fingerprint: r.try_get("config_fingerprint")?,
                    updated_at: r.try_get("updated_at")?,
                })
            })
            .collect()
    }

    /// Fails unless every checkpoint of the run was taken with `fingerprint`.
    pub async fn verify_resumable(&self, run_id: RunId, fingerprint: &str) -> StorageResult<()> {
        let run = self.require_run(run_id).await?;
        if run.config_fingerprint != fingerprint {
            return Err(StorageError::InvalidState(
                "source, target, mapping or transformation changed since the checkpoint was taken"
                    .into(),
            ));
        }
        if self
            .checkpoints(run_id)
            .await?
            .iter()
            .any(|c| c.config_fingerprint != fingerprint)
        {
            return Err(StorageError::InvalidState(
                "checkpoint was taken with a different configuration".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_job(name: &str) -> NewJob {
        NewJob {
            name: name.into(),
            kind: JobKind::Migration,
            definition: serde_json::json!({"tables": ["customers"]}),
            source_connection_id: None,
            target_connection_id: None,
            mapping: None,
            transformation: None,
        }
    }

    fn new_run(job_id: JobId) -> NewRun {
        NewRun {
            job_id,
            dry_run: false,
            definition_snapshot: serde_json::json!({}),
            config_fingerprint: "fp1".into(),
            mapping_version: None,
            transformation_version: None,
            batch_settings: serde_json::json!({"batch_size": 1000}),
            concurrency: 2,
            retry_policy: serde_json::json!({"max_attempts": 3}),
            app_version: "0.1.0".into(),
        }
    }

    #[tokio::test]
    async fn job_lifecycle_is_enforced() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let jobs = store.jobs();
        let job = jobs.create(new_job("customers")).await.unwrap();
        assert_eq!(job.status, JobStatus::Draft);
        assert!(jobs.transition(job.id, JobStatus::Running).await.is_err());
        jobs.transition(job.id, JobStatus::Validating)
            .await
            .unwrap();
        jobs.transition(job.id, JobStatus::Ready).await.unwrap();
        jobs.mark_dry_run_passed(job.id).await.unwrap();
        assert!(
            jobs.require(job.id)
                .await
                .unwrap()
                .dry_run_passed_at
                .is_some()
        );

        let edited = jobs
            .update_definition(job.id, &serde_json::json!({"tables": []}))
            .await
            .unwrap();
        assert_eq!(edited.status, JobStatus::Draft);
        assert!(
            edited.dry_run_passed_at.is_none(),
            "edits invalidate the dry run"
        );

        let clone = jobs.clone_job(job.id, "customers copy").await.unwrap();
        assert_eq!(clone.definition, edited.definition);
        jobs.set_archived(job.id, true).await.unwrap();
        assert_eq!(jobs.list(false).await.unwrap().len(), 1);
        assert_eq!(jobs.list(true).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn runs_events_and_checkpoints() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        let jobs = store.jobs();
        let job = jobs.create(new_job("orders")).await.unwrap();
        let run = jobs.create_run(new_run(job.id)).await.unwrap();
        assert_eq!(run.attempt, 1);
        let run = jobs
            .transition_run(run.id, JobStatus::Running)
            .await
            .unwrap();
        assert!(run.started_at.is_some());
        assert_eq!(jobs.interrupted_runs().await.unwrap().len(), 1);

        jobs.add_event(
            run.id,
            EventLevel::Warning,
            "lossy_conversion",
            Some("orders"),
            "decimal rounded",
        )
        .await
        .unwrap();
        jobs.add_event(
            run.id,
            EventLevel::Error,
            "constraint",
            Some("orders"),
            "duplicate key",
        )
        .await
        .unwrap();
        let reloaded = jobs.require_run(run.id).await.unwrap();
        assert_eq!((reloaded.warning_count, reloaded.error_count), (1, 1));
        assert_eq!(jobs.events(run.id, 10).await.unwrap().len(), 2);

        let cp = Checkpoint {
            run_id: run.id,
            stream: "public.orders".into(),
            position: Some(serde_json::json!({"last_pk": [42]})),
            records_committed: 42,
            bytes_committed: 4200,
            completed: false,
            config_fingerprint: "fp1".into(),
            updated_at: Utc::now(),
        };
        jobs.save_checkpoint(&cp).await.unwrap();
        jobs.save_checkpoint(&Checkpoint {
            records_committed: 84,
            ..cp.clone()
        })
        .await
        .unwrap();
        let cps = jobs.checkpoints(run.id).await.unwrap();
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].records_committed, 84);

        jobs.verify_resumable(run.id, "fp1").await.unwrap();
        assert!(jobs.verify_resumable(run.id, "fp2").await.is_err());

        jobs.transition_run(run.id, JobStatus::Pausing)
            .await
            .unwrap();
        jobs.transition_run(run.id, JobStatus::Paused)
            .await
            .unwrap();
        assert!(
            jobs.transition_run(run.id, JobStatus::Completed)
                .await
                .is_err()
        );
        jobs.transition_run(run.id, JobStatus::Resuming)
            .await
            .unwrap();
        jobs.transition_run(run.id, JobStatus::Running)
            .await
            .unwrap();
        let done = jobs
            .transition_run(run.id, JobStatus::Completed)
            .await
            .unwrap();
        assert!(done.ended_at.is_some());

        let second = jobs.create_run(new_run(job.id)).await.unwrap();
        assert_eq!(second.attempt, 2);
    }

    #[tokio::test]
    async fn checkpoints_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let run_id;
        {
            let store = MetadataStore::open(&path).await.unwrap();
            let job = store.jobs().create(new_job("persist")).await.unwrap();
            let run = store.jobs().create_run(new_run(job.id)).await.unwrap();
            run_id = run.id;
            store
                .jobs()
                .save_checkpoint(&Checkpoint {
                    run_id,
                    stream: "s".into(),
                    position: Some(serde_json::json!({"k": 1})),
                    records_committed: 1,
                    bytes_committed: 1,
                    completed: false,
                    config_fingerprint: "fp1".into(),
                    updated_at: Utc::now(),
                })
                .await
                .unwrap();
            store.close().await;
        }
        let store = MetadataStore::open(&path).await.unwrap();
        let cps = store.jobs().checkpoints(run_id).await.unwrap();
        assert_eq!(cps[0].position, Some(serde_json::json!({"k": 1})));
    }
}
