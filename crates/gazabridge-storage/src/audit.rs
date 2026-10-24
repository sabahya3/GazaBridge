//! Local audit log.
//!
//! Audit entries describe *what* was done to *which* object; they never
//! contain record data or secrets.

use chrono::{DateTime, Utc};
use gazabridge_domain::ids::{AuditEventId, CorrelationId};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::{MetadataStore, StorageResult, now, parse_id};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub at: DateTime<Utc>,
    /// Dotted action name, e.g. `connection.create`, `data.delete`.
    pub action: String,
    pub target_kind: String,
    pub target_id: Option<String>,
    pub summary: String,
    pub details: serde_json::Value,
    pub correlation_id: Option<CorrelationId>,
}

#[derive(Debug, Clone)]
pub struct NewAuditEvent<'a> {
    pub action: &'a str,
    pub target_kind: &'a str,
    pub target_id: Option<String>,
    pub summary: String,
    pub details: serde_json::Value,
    pub correlation_id: Option<CorrelationId>,
}

#[derive(Debug, Clone)]
pub struct AuditRepo<'a> {
    store: &'a MetadataStore,
}

impl MetadataStore {
    pub fn audit(&self) -> AuditRepo<'_> {
        AuditRepo { store: self }
    }
}

impl AuditRepo<'_> {
    pub async fn record(&self, event: NewAuditEvent<'_>) -> StorageResult<AuditEventId> {
        let id = AuditEventId::new();
        sqlx::query(
            "INSERT INTO audit_events (id, at, action, target_kind, target_id, summary, details_json, correlation_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(now())
        .bind(event.action)
        .bind(event.target_kind)
        .bind(event.target_id)
        .bind(event.summary)
        .bind(serde_json::to_string(&event.details)?)
        .bind(event.correlation_id.map(|c| c.to_string()))
        .execute(self.store.pool())
        .await?;
        Ok(id)
    }

    /// Most recent events first, optionally filtered by target.
    pub async fn list(
        &self,
        target_kind: Option<&str>,
        target_id: Option<&str>,
        limit: u32,
    ) -> StorageResult<Vec<AuditEvent>> {
        let rows = sqlx::query(
            "SELECT id, at, action, target_kind, target_id, summary, details_json, correlation_id
             FROM audit_events
             WHERE (?1 IS NULL OR target_kind = ?1) AND (?2 IS NULL OR target_id = ?2)
             ORDER BY at DESC, id DESC LIMIT ?3",
        )
        .bind(target_kind)
        .bind(target_id)
        .bind(i64::from(limit))
        .fetch_all(self.store.pool())
        .await?;
        rows.iter()
            .map(|r| {
                let correlation: Option<String> = r.try_get("correlation_id")?;
                Ok(AuditEvent {
                    id: parse_id(&r.try_get::<String, _>("id")?, "audit event")?,
                    at: r.try_get("at")?,
                    action: r.try_get("action")?,
                    target_kind: r.try_get("target_kind")?,
                    target_id: r.try_get("target_id")?,
                    summary: r.try_get("summary")?,
                    details: serde_json::from_str(&r.try_get::<String, _>("details_json")?)?,
                    correlation_id: correlation
                        .map(|c| parse_id(&c, "correlation"))
                        .transpose()?,
                })
            })
            .collect()
    }

    /// Deletes events older than `before`; returns how many were removed.
    pub async fn prune_before(&self, before: DateTime<Utc>) -> StorageResult<u64> {
        let r = sqlx::query("DELETE FROM audit_events WHERE at < ?")
            .bind(before)
            .execute(self.store.pool())
            .await?;
        Ok(r.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_and_lists_events() {
        let store = MetadataStore::open_in_memory().await.unwrap();
        for i in 0..3 {
            store
                .audit()
                .record(NewAuditEvent {
                    action: "connection.create",
                    target_kind: "connection",
                    target_id: Some(format!("c{i}")),
                    summary: format!("created c{i}"),
                    details: serde_json::json!({}),
                    correlation_id: Some(CorrelationId::new()),
                })
                .await
                .unwrap();
        }
        let all = store.audit().list(None, None, 10).await.unwrap();
        assert_eq!(all.len(), 3);
        let one = store
            .audit()
            .list(Some("connection"), Some("c1"), 10)
            .await
            .unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].summary, "created c1");
        let removed = store
            .audit()
            .prune_before(Utc::now() + chrono::Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(removed, 3);
    }
}
