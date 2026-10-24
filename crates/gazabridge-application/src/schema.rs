//! Schema inspection, snapshots and drift detection.

use gazabridge_domain::ids::ConnectionId;
use gazabridge_plugin_sdk::connector::InspectOptions;
use gazabridge_plugin_sdk::schema::{ObjectRef, ObjectSchema};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::snapshots::SchemaSnapshot;
use serde::{Deserialize, Serialize};

use crate::connections::ConnectionService;
use crate::{AppError, AppResult};

/// One difference between two snapshots of the same object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum FieldChange {
    Added {
        path: String,
        canonical_type: String,
    },
    Removed {
        path: String,
        canonical_type: String,
    },
    TypeChanged {
        path: String,
        before: String,
        after: String,
    },
    NullabilityChanged {
        path: String,
        nullable_before: bool,
        nullable_after: bool,
    },
    /// Presence ratio moved by more than the threshold (inferred schemas only).
    PresenceChanged {
        path: String,
        before: f64,
        after: f64,
    },
}

#[derive(Debug, Clone)]
pub struct SchemaService {
    store: MetadataStore,
    connections: ConnectionService,
}

/// Stable key for an object's snapshot history.
pub fn object_key(object: &ObjectRef) -> String {
    format!("{object}")
}

struct Shape {
    path: String,
    canonical: String,
    nullable: bool,
    presence: Option<f64>,
}

fn shapes(schema: &ObjectSchema) -> Vec<Shape> {
    if let Some(inferred) = &schema.inferred {
        let n = inferred.sampled_documents;
        inferred
            .fields
            .iter()
            .map(|f| Shape {
                path: f.path.clone(),
                canonical: f.canonical_type.to_string(),
                nullable: f.null_count > 0 || f.present_count < n,
                presence: Some(f.occurrence_ratio(n)),
            })
            .collect()
    } else {
        schema
            .columns
            .iter()
            .map(|c| Shape {
                path: c.name.clone(),
                canonical: c.canonical_type.to_string(),
                nullable: c.nullable,
                presence: None,
            })
            .collect()
    }
}

/// Differences from `before` to `after`. Presence changes smaller than
/// `presence_threshold` (0.0–1.0) are ignored to absorb sampling noise.
pub fn diff(
    before: &ObjectSchema,
    after: &ObjectSchema,
    presence_threshold: f64,
) -> Vec<FieldChange> {
    let old = shapes(before);
    let new = shapes(after);
    let mut out = Vec::new();
    for o in &old {
        match new.iter().find(|n| n.path == o.path) {
            None => out.push(FieldChange::Removed {
                path: o.path.clone(),
                canonical_type: o.canonical.clone(),
            }),
            Some(n) => {
                if n.canonical != o.canonical {
                    out.push(FieldChange::TypeChanged {
                        path: o.path.clone(),
                        before: o.canonical.clone(),
                        after: n.canonical.clone(),
                    });
                }
                if n.nullable != o.nullable {
                    out.push(FieldChange::NullabilityChanged {
                        path: o.path.clone(),
                        nullable_before: o.nullable,
                        nullable_after: n.nullable,
                    });
                }
                if let (Some(a), Some(b)) = (o.presence, n.presence)
                    && (a - b).abs() > presence_threshold
                {
                    out.push(FieldChange::PresenceChanged {
                        path: o.path.clone(),
                        before: a,
                        after: b,
                    });
                }
            }
        }
    }
    for n in &new {
        if !old.iter().any(|o| o.path == n.path) {
            out.push(FieldChange::Added {
                path: n.path.clone(),
                canonical_type: n.canonical.clone(),
            });
        }
    }
    out
}

impl SchemaService {
    pub fn new(store: MetadataStore, connections: ConnectionService) -> Self {
        Self { store, connections }
    }

    /// Inspects (or infers, for document stores) an object.
    pub async fn inspect(
        &self,
        connection: ConnectionId,
        object: &ObjectRef,
        options: &InspectOptions,
    ) -> AppResult<ObjectSchema> {
        let conn = self.connections.open(connection).await?;
        let result = conn.inspect(object, options).await;
        conn.close().await;
        Ok(result?)
    }

    /// Inspects an object and saves the result as a snapshot.
    pub async fn snapshot(
        &self,
        connection: ConnectionId,
        object: &ObjectRef,
        options: &InspectOptions,
        label: Option<&str>,
    ) -> AppResult<(ObjectSchema, SchemaSnapshot)> {
        let schema = self.inspect(connection, object, options).await?;
        let saved = self
            .store
            .snapshots()
            .save(
                connection,
                &serde_json::to_value(object).map_err(|e| AppError::Invalid(e.to_string()))?,
                &object_key(object),
                label,
                schema.inferred.is_some(),
                &serde_json::to_value(&schema).map_err(|e| AppError::Invalid(e.to_string()))?,
            )
            .await?;
        Ok((schema, saved))
    }

    /// Compares the live object against its latest snapshot.
    pub async fn drift(
        &self,
        connection: ConnectionId,
        object: &ObjectRef,
        options: &InspectOptions,
        presence_threshold: f64,
    ) -> AppResult<Vec<FieldChange>> {
        let history = self
            .store
            .snapshots()
            .history(connection, &object_key(object), 1)
            .await?;
        let latest = history.first().ok_or(AppError::NotFound("snapshot"))?;
        let before: ObjectSchema = serde_json::from_value(latest.schema.clone())
            .map_err(|e| AppError::Invalid(format!("stored snapshot is unreadable: {e}")))?;
        let now = self.inspect(connection, object, options).await?;
        Ok(diff(&before, &now, presence_threshold))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gazabridge_connectors::{ConnectorRegistry, ProfileCatalog};
    use gazabridge_domain::CanonicalType;
    use gazabridge_domain::connection::{
        ConnectionProfile, ConnectionSettings, SqliteJournalMode, SqliteSettings,
    };
    use gazabridge_plugin_sdk::capability::QueryLanguage;
    use gazabridge_plugin_sdk::query::QueryRequest;
    use gazabridge_plugin_sdk::schema::NamespacePath;
    use gazabridge_secrets::{Redactor, SecretResolver, SessionStore};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::PasswordInput;

    #[tokio::test]
    async fn snapshot_then_detect_drift_on_a_real_sqlite_table() {
        let dir = tempfile::tempdir().unwrap();
        let store = MetadataStore::open_in_memory().await.unwrap();
        let mut registry = ConnectorRegistry::new(ProfileCatalog::builtin().unwrap());
        registry
            .register(Arc::new(
                gazabridge_connector_sqlite::SqliteConnectorFactory,
            ))
            .unwrap();
        let connections = ConnectionService::new(
            store.clone(),
            registry,
            SecretResolver::new(Arc::new(SessionStore::default()), Redactor::new()),
        );
        let profile = connections
            .create(
                ConnectionProfile::new(
                    "s",
                    "sqlite.sqlite",
                    ConnectionSettings::Sqlite(SqliteSettings {
                        path: dir.path().join("d.db"),
                        read_only: false,
                        create_if_missing: true,
                        busy_timeout_ms: 1000,
                        journal_mode: SqliteJournalMode::Wal,
                        foreign_keys: true,
                    }),
                ),
                PasswordInput::None,
            )
            .await
            .unwrap();
        let conn = connections.open(profile.id).await.unwrap();
        let sql = |text: &str| QueryRequest {
            language: QueryLanguage::SqliteSql,
            text: text.into(),
            params: vec![],
            max_rows: 10,
            timeout: None,
            database: None,
            collection: None,
        };
        conn.execute(
            sql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let svc = SchemaService::new(store, connections);
        let object = ObjectRef::table(NamespacePath::schema(None, "main"), "t");
        let (schema, snap) = svc
            .snapshot(profile.id, &object, &Default::default(), Some("v1"))
            .await
            .unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert!(!snap.inferred);
        assert!(
            svc.drift(profile.id, &object, &Default::default(), 0.1)
                .await
                .unwrap()
                .is_empty()
        );

        conn.execute(
            sql("ALTER TABLE t ADD COLUMN note TEXT"),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let changes = svc
            .drift(profile.id, &object, &Default::default(), 0.1)
            .await
            .unwrap();
        assert_eq!(
            changes,
            vec![FieldChange::Added {
                path: "note".into(),
                canonical_type: CanonicalType::TEXT.to_string()
            }]
        );
    }
}
