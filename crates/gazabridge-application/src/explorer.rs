//! Browsing and editing data: the Explorer and Data Editor use cases.
//!
//! Connections opened for browsing are kept per saved connection until they
//! are disconnected, edited or fail, so paging does not reconnect.
//!
//! Every edit is identified by the record's key (`_id`, or the primary key
//! columns) and checked against the version the user loaded: if the record
//! changed in the meantime the edit is refused instead of overwriting
//! someone else's change. Edits are audited by field name; record values
//! never reach the audit log.

use std::collections::HashMap;
use std::sync::Arc;

use gazabridge_domain::ejson::{self, EjsonError};
use gazabridge_domain::ids::ConnectionId;
use gazabridge_domain::job::WriteMode;
use gazabridge_domain::{CanonicalType, Record, Value, ValueKind};
use gazabridge_plugin_sdk::capability::Capability;
use gazabridge_plugin_sdk::connector::{Connection, CountResult, InspectOptions};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortKey};
use gazabridge_plugin_sdk::read::PageRequest;
use gazabridge_plugin_sdk::schema::{
    ColumnInfo, NamespaceEntry, NamespacePath, ObjectKind, ObjectRef,
};
use gazabridge_plugin_sdk::write::{DeleteRequest, MutationOutcome, UpdateRequest, WriteBatch};
use gazabridge_plugin_sdk::{ConnectorError, ErrorKind};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::audit::NewAuditEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value as J;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::connections::ConnectionService;
use crate::{AppError, AppResult};

/// Why records of an object cannot be edited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditBlocker {
    ReadOnlyConnection,
    NotWritable,
    NoPrimaryKey,
    Unsupported,
    /// Only some fields are shown, so a record cannot be edited safely.
    Projected,
}

#[derive(Debug, Clone)]
pub struct BrowseRequest {
    pub object: ObjectRef,
    pub filter: Option<Filter>,
    /// Fields to show; empty means all.
    pub projection: Vec<String>,
    /// Fields to hide (documents), when `projection` is empty.
    pub exclude: Vec<String>,
    pub order_by: Vec<SortKey>,
    pub offset: u64,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct BrowseResult {
    /// Declared columns (relational objects); empty for collections.
    pub columns: Vec<ColumnInfo>,
    /// Columns to show: declared columns, or the union of document fields
    /// in first-seen order.
    pub fields: Vec<String>,
    pub records: Vec<Record>,
    pub offset: u64,
    pub has_more: bool,
    pub key_fields: Vec<String>,
    pub edit_blocker: Option<EditBlocker>,
}

/// One field difference between the loaded and the edited record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldEdit {
    pub field: String,
    /// Typed JSON before; `None` when the field is added.
    pub before: Option<J>,
    /// Typed JSON after; `None` when the field is removed.
    pub after: Option<J>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EditPlan {
    pub changes: Vec<FieldEdit>,
    /// The stored record no longer matches the version the user loaded.
    pub conflict: bool,
}

#[derive(Debug, Clone)]
struct ObjectInfo {
    columns: Vec<ColumnInfo>,
    key_fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExplorerService {
    store: MetadataStore,
    connections: ConnectionService,
    sessions: Arc<Mutex<HashMap<ConnectionId, Arc<dyn Connection>>>>,
    objects: Arc<Mutex<HashMap<(ConnectionId, ObjectRef), ObjectInfo>>>,
}

fn invalid(e: EjsonError) -> AppError {
    AppError::Invalid(e.to_string())
}

fn conflict(message: &str) -> AppError {
    AppError::Connector(ConnectorError::new(ErrorKind::Conflict, message.to_owned()))
}

/// Field names in display order: `_id` first, then first-seen order.
pub fn field_union(records: &[Record]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for r in records {
        for k in r.fields.keys() {
            if !out.iter().any(|f| f == k) {
                out.push(k.clone());
            }
        }
    }
    if let Some(i) = out.iter().position(|f| f == "_id") {
        let id = out.remove(i);
        out.insert(0, id);
    }
    out
}

/// Kinds whose stored value can be compared for equality on every engine.
fn comparable(kind: ValueKind) -> bool {
    use ValueKind as K;
    matches!(
        kind,
        K::Null
            | K::Bool
            | K::Int
            | K::UInt
            | K::BigInt
            | K::Decimal
            | K::String
            | K::Uuid
            | K::ObjectId
            | K::Date
            | K::Time
            | K::LocalDateTime
            | K::UtcDateTime
    )
}

/// Compare-and-set condition: the field still holds the loaded value.
fn unchanged_condition(field: &str, value: &Value, document: bool) -> Option<Filter> {
    let field = FieldPath::new(field);
    match value {
        Value::Null => Some(Filter::IsNull { field }),
        Value::Missing if document => Some(Filter::IsMissing { field }),
        v if comparable(v.kind()) => Some(Filter::Eq {
            field,
            value: v.clone(),
        }),
        _ => None,
    }
}

/// Differences between two typed JSON objects, in the edited record's order
/// followed by removed fields.
pub fn diff_records(
    before: &serde_json::Map<String, J>,
    after: &serde_json::Map<String, J>,
) -> Vec<FieldEdit> {
    let mut out = Vec::new();
    for (k, v) in after {
        match before.get(k) {
            Some(old) if old == v => {}
            old => out.push(FieldEdit {
                field: k.clone(),
                before: old.cloned(),
                after: Some(v.clone()),
            }),
        }
    }
    for (k, v) in before {
        if !after.contains_key(k) {
            out.push(FieldEdit {
                field: k.clone(),
                before: Some(v.clone()),
                after: None,
            });
        }
    }
    out
}

fn as_object(j: J, what: &str) -> AppResult<serde_json::Map<String, J>> {
    match j {
        J::Object(m) => Ok(m),
        _ => Err(AppError::Invalid(format!("{what} must be a JSON object"))),
    }
}

fn parse_json(text: &str, what: &str) -> AppResult<J> {
    serde_json::from_str(text).map_err(|e| AppError::Invalid(format!("{what}: invalid JSON: {e}")))
}

/// Writers cannot tell plain JSON from documents; JSON columns take it as JSON.
fn coerce(value: Value, column: Option<&ColumnInfo>) -> Value {
    match (column.map(|c| &c.canonical_type), value) {
        (Some(CanonicalType::Json), v) if !v.is_absent() && !matches!(v, Value::Json(_)) => {
            Value::Json(gazabridge_domain::conversion::value_to_json(&v).0)
        }
        (_, v) => v,
    }
}

impl ExplorerService {
    pub fn new(store: MetadataStore, connections: ConnectionService) -> Self {
        Self {
            store,
            connections,
            sessions: Arc::default(),
            objects: Arc::default(),
        }
    }

    pub(crate) async fn session(&self, id: ConnectionId) -> AppResult<Arc<dyn Connection>> {
        let mut sessions = self.sessions.lock().await;
        if let Some(conn) = sessions.get(&id) {
            return Ok(conn.clone());
        }
        let conn = self.connections.open(id).await?;
        sessions.insert(id, conn.clone());
        Ok(conn)
    }

    /// Closes the browsing session of a connection (after edits, deletes or failures).
    pub async fn disconnect(&self, id: ConnectionId) {
        let removed = self.sessions.lock().await.remove(&id);
        self.objects.lock().await.retain(|(c, _), _| *c != id);
        if let Some(conn) = removed {
            conn.close().await;
        }
    }

    /// Drops the session when the failure means it is no longer usable.
    pub(crate) async fn checked<T>(
        &self,
        id: ConnectionId,
        r: Result<T, ConnectorError>,
    ) -> AppResult<T> {
        match r {
            Ok(v) => Ok(v),
            Err(e) => {
                if matches!(
                    e.kind(),
                    ErrorKind::Connection | ErrorKind::Timeout | ErrorKind::Authentication
                ) {
                    self.disconnect(id).await;
                }
                Err(e.into())
            }
        }
    }

    pub async fn list(
        &self,
        id: ConnectionId,
        parent: &NamespacePath,
    ) -> AppResult<Vec<NamespaceEntry>> {
        let conn = self.session(id).await?;
        let r = conn.list(parent).await;
        self.checked(id, r).await
    }

    async fn info(
        &self,
        id: ConnectionId,
        conn: &dyn Connection,
        object: &ObjectRef,
    ) -> AppResult<ObjectInfo> {
        let cache_key = (id, object.clone());
        if let Some(info) = self.objects.lock().await.get(&cache_key) {
            return Ok(info.clone());
        }
        let info = if object.kind == ObjectKind::Collection {
            ObjectInfo {
                columns: Vec::new(),
                key_fields: vec!["_id".to_owned()],
            }
        } else {
            let options = InspectOptions {
                include_indexes: false,
                include_constraints: true,
                ..InspectOptions::default()
            };
            let r = conn.inspect(object, &options).await;
            let schema = self.checked(id, r).await?;
            ObjectInfo {
                columns: schema.columns,
                key_fields: schema.primary_key,
            }
        };
        self.objects.lock().await.insert(cache_key, info.clone());
        Ok(info)
    }

    fn blocker(
        conn: &dyn Connection,
        object: &ObjectRef,
        info: &ObjectInfo,
    ) -> Option<EditBlocker> {
        if conn.is_read_only() {
            Some(EditBlocker::ReadOnlyConnection)
        } else if !object.kind.is_writable() {
            Some(EditBlocker::NotWritable)
        } else if info.key_fields.is_empty() {
            Some(EditBlocker::NoPrimaryKey)
        } else if !conn.manifest().supports(Capability::WriteUpdate) {
            Some(EditBlocker::Unsupported)
        } else {
            None
        }
    }

    pub async fn browse(&self, id: ConnectionId, req: BrowseRequest) -> AppResult<BrowseResult> {
        let conn = self.session(id).await?;
        let info = self.info(id, conn.as_ref(), &req.object).await?;
        let projected = !req.projection.is_empty() || !req.exclude.is_empty();
        let page_req = PageRequest {
            object: req.object.clone(),
            projection: req.projection,
            exclude: req.exclude,
            filter: req.filter,
            order_by: req.order_by,
            offset: req.offset,
            limit: req.limit,
        };
        let r = conn.read_page(page_req, CancellationToken::new()).await;
        let page = self.checked(id, r).await?;
        let columns = if page.columns.is_empty() {
            info.columns.clone()
        } else {
            page.columns
        };
        let fields = if columns.is_empty() {
            field_union(&page.records)
        } else {
            columns.iter().map(|c| c.name.clone()).collect()
        };
        Ok(BrowseResult {
            edit_blocker: Self::blocker(conn.as_ref(), &req.object, &info)
                .or(projected.then_some(EditBlocker::Projected)),
            columns,
            fields,
            records: page.records,
            offset: page.offset,
            has_more: page.has_more,
            key_fields: info.key_fields,
        })
    }

    /// Counts records. An estimate (`exact = false`) is only asked for
    /// without a filter; `None` means the engine cannot estimate.
    pub async fn count(
        &self,
        id: ConnectionId,
        object: &ObjectRef,
        filter: Option<&Filter>,
        exact: bool,
    ) -> AppResult<Option<CountResult>> {
        let conn = self.session(id).await?;
        let exact = exact || filter.is_some();
        match conn.count(object, filter, exact).await {
            Err(e) if !exact && e.kind() == ErrorKind::Unsupported => Ok(None),
            r => self.checked(id, r).await.map(Some),
        }
    }

    async fn editable(
        &self,
        id: ConnectionId,
        object: &ObjectRef,
    ) -> AppResult<(Arc<dyn Connection>, ObjectInfo)> {
        let conn = self.session(id).await?;
        let info = self.info(id, conn.as_ref(), object).await?;
        match Self::blocker(conn.as_ref(), object, &info) {
            None => Ok((conn, info)),
            Some(EditBlocker::ReadOnlyConnection) => Err(ConnectorError::ReadOnly.into()),
            Some(b) => Err(AppError::Invalid(format!(
                "records of `{object}` cannot be edited ({b:?})"
            ))),
        }
    }

    fn key_filter(info: &ObjectInfo, key: &Record) -> AppResult<Filter> {
        let mut parts = Vec::with_capacity(info.key_fields.len());
        for f in &info.key_fields {
            let v = key.get(f);
            if v.is_absent() {
                return Err(AppError::Invalid(format!("key field `{f}` has no value")));
            }
            parts.push(Filter::eq(f.clone(), v.clone()));
        }
        Ok(Filter::and(parts))
    }

    /// Reads the stored record identified by `key`.
    async fn current(
        &self,
        id: ConnectionId,
        conn: &dyn Connection,
        object: &ObjectRef,
        filter: &Filter,
    ) -> AppResult<Record> {
        let req = PageRequest {
            object: object.clone(),
            projection: Vec::new(),
            exclude: Vec::new(),
            filter: Some(filter.clone()),
            order_by: Vec::new(),
            offset: 0,
            limit: 2,
        };
        let r = conn.read_page(req, CancellationToken::new()).await;
        let mut page = self.checked(id, r).await?;
        match page.records.len() {
            0 => Err(conflict(
                "the record no longer exists; reload and try again",
            )),
            1 => Ok(page.records.remove(0)),
            _ => Err(AppError::Invalid(
                "the key matches more than one record".into(),
            )),
        }
    }

    async fn audit(
        &self,
        action: &str,
        id: ConnectionId,
        object: &ObjectRef,
        summary: String,
        fields: &[String],
    ) -> AppResult<()> {
        self.store
            .audit()
            .record(NewAuditEvent {
                action,
                target_kind: "object",
                target_id: Some(format!("{id}/{object}")),
                summary,
                details: serde_json::json!({ "connection": id.to_string(), "object": object, "fields": fields }),
                correlation_id: None,
            })
            .await?;
        Ok(())
    }

    /// Inserts one record given as typed JSON.
    pub async fn insert(&self, id: ConnectionId, object: &ObjectRef, text: &str) -> AppResult<()> {
        let (conn, info) = self.editable(id, object).await?;
        let parsed = ejson::record_from_ejson_str(text).map_err(invalid)?;
        if parsed.is_empty() {
            return Err(AppError::Invalid("the record has no fields".into()));
        }
        let mut record = Record::with_capacity(parsed.len());
        for (k, v) in parsed.fields {
            let column = info.columns.iter().find(|c| c.name == k);
            if !info.columns.is_empty() && column.is_none() {
                return Err(AppError::Invalid(format!("unknown column `{k}`")));
            }
            record.insert(k, coerce(v.value, column));
        }
        let fields: Vec<String> = record.fields.keys().cloned().collect();
        let batch = WriteBatch {
            object: object.clone(),
            mode: WriteMode::Insert,
            records: vec![record],
            key_fields: Vec::new(),
            atomic: false,
        };
        let r = conn.write_batch(batch, CancellationToken::new()).await;
        let outcome = self.checked(id, r).await?;
        if let Some(f) = outcome.failures.first() {
            return Err(AppError::Connector(ConnectorError::new(
                f.kind,
                f.message.clone(),
            )));
        }
        self.audit(
            "data.insert",
            id,
            object,
            format!("Inserted 1 record into {object}"),
            &fields,
        )
        .await
    }

    /// Computes what saving `edited` would change, and whether the stored
    /// record still matches `original` (both typed JSON objects).
    pub async fn plan_update(
        &self,
        id: ConnectionId,
        object: &ObjectRef,
        original: &str,
        edited: &str,
    ) -> AppResult<EditPlan> {
        let (conn, info) = self.editable(id, object).await?;
        self.plan(id, conn.as_ref(), &info, object, original, edited)
            .await
            .map(|(plan, _, _)| plan)
    }

    async fn plan(
        &self,
        id: ConnectionId,
        conn: &dyn Connection,
        info: &ObjectInfo,
        object: &ObjectRef,
        original: &str,
        edited: &str,
    ) -> AppResult<(EditPlan, Record, Filter)> {
        let original = as_object(parse_json(original, "loaded record")?, "the loaded record")?;
        let edited_json = parse_json(edited, "edited record")?;
        // Validate the edited text before touching the server.
        ejson::record_from_ejson(&edited_json).map_err(invalid)?;
        let edited = as_object(edited_json, "the edited record")?;
        let key = ejson::record_from_ejson(&J::Object(original.clone())).map_err(invalid)?;
        let filter = Self::key_filter(info, &key)?;
        let current = self.current(id, conn, object, &filter).await?;
        let conflict = ejson::record_to_ejson(&current) != J::Object(original.clone());
        let changes = diff_records(&original, &edited);
        for c in &changes {
            if info.key_fields.contains(&c.field) {
                return Err(AppError::Invalid(format!(
                    "key field `{}` cannot be changed in the editor",
                    c.field
                )));
            }
            if !info.columns.is_empty() {
                if c.after.is_none() {
                    return Err(AppError::Invalid(format!(
                        "column `{}` cannot be removed; set it to null instead",
                        c.field
                    )));
                }
                if !info.columns.iter().any(|col| col.name == c.field) {
                    return Err(AppError::Invalid(format!("unknown column `{}`", c.field)));
                }
            }
        }
        Ok((EditPlan { changes, conflict }, current, filter))
    }

    /// Applies an edit after re-checking it. Fails with a conflict when the
    /// record changed since it was loaded.
    pub async fn apply_update(
        &self,
        id: ConnectionId,
        object: &ObjectRef,
        original: &str,
        edited: &str,
    ) -> AppResult<MutationOutcome> {
        let (conn, info) = self.editable(id, object).await?;
        let (plan, current, key_filter) = self
            .plan(id, conn.as_ref(), &info, object, original, edited)
            .await?;
        if plan.conflict {
            return Err(conflict(
                "the record was changed by someone else since it was loaded; reload it and edit again",
            ));
        }
        if plan.changes.is_empty() {
            return Err(AppError::Invalid("nothing changed".into()));
        }
        let document = info.columns.is_empty();
        let mut set = Record::with_capacity(plan.changes.len());
        let mut conditions = vec![key_filter];
        for c in &plan.changes {
            let value = match &c.after {
                None => Value::Missing,
                Some(j) => ejson::from_ejson(j).map_err(invalid)?,
            };
            let column = info.columns.iter().find(|col| col.name == c.field);
            let value = coerce(value, column);
            // Keep provenance (e.g. BSON Int32) when the kind is unchanged.
            let mut cv = gazabridge_domain::CanonicalValue::new(value);
            if let Some(old) = current.get_canonical(&c.field)
                && old.value.kind() == cv.value.kind()
            {
                cv.meta = old.meta.clone();
            }
            set.fields.insert(c.field.clone(), cv);
            if let Some(cond) = unchanged_condition(&c.field, current.get(&c.field), document) {
                conditions.push(cond);
            }
        }
        let request = UpdateRequest {
            object: object.clone(),
            filter: Filter::and(conditions),
            set,
            expect_matched: Some(1),
        };
        let r = conn.update(request).await;
        let outcome = match self.checked(id, r).await {
            Err(AppError::Connector(e)) if e.kind() == ErrorKind::Conflict => {
                return Err(conflict(
                    "the record was changed by someone else while saving; reload it and edit again",
                ));
            }
            other => other?,
        };
        let fields: Vec<String> = plan.changes.iter().map(|c| c.field.clone()).collect();
        self.audit(
            "data.update",
            id,
            object,
            format!("Updated 1 record in {object} ({} field(s))", fields.len()),
            &fields,
        )
        .await?;
        Ok(outcome)
    }

    /// Deletes the record loaded as `original`, refusing if it changed since.
    pub async fn delete(
        &self,
        id: ConnectionId,
        object: &ObjectRef,
        original: &str,
    ) -> AppResult<MutationOutcome> {
        let (conn, info) = self.editable(id, object).await?;
        if !conn.manifest().supports(Capability::WriteDelete) {
            return Err(ConnectorError::unsupported(Capability::WriteDelete).into());
        }
        let original = parse_json(original, "loaded record")?;
        let key = ejson::record_from_ejson(&original).map_err(invalid)?;
        let filter = Self::key_filter(&info, &key)?;
        let current = self.current(id, conn.as_ref(), object, &filter).await?;
        if ejson::record_to_ejson(&current) != original {
            return Err(conflict(
                "the record was changed by someone else since it was loaded; reload it before deleting",
            ));
        }
        let request = DeleteRequest {
            object: object.clone(),
            filter,
            expect_matched: Some(1),
        };
        let r = conn.delete(request).await;
        let outcome = self.checked(id, r).await?;
        self.audit(
            "data.delete",
            id,
            object,
            format!("Deleted 1 record from {object}"),
            &info.key_fields,
        )
        .await?;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn diff_lists_changed_added_and_removed_fields() {
        let before = json!({"_id": 1, "a": 1, "b": "x", "gone": true});
        let after = json!({"_id": 1, "a": 2, "b": "x", "new": null});
        let d = diff_records(before.as_object().unwrap(), after.as_object().unwrap());
        let fields: Vec<_> = d
            .iter()
            .map(|c| (c.field.as_str(), c.before.is_some(), c.after.is_some()))
            .collect();
        assert_eq!(
            fields,
            [
                ("a", true, true),
                ("new", false, true),
                ("gone", true, false)
            ]
        );
    }

    #[test]
    fn union_puts_id_first() {
        let mut a = Record::new();
        a.insert("x", Value::Int(1));
        a.insert("_id", Value::Int(1));
        let mut b = Record::new();
        b.insert("y", Value::Int(1));
        assert_eq!(field_union(&[a, b]), ["_id", "x", "y"]);
    }

    #[test]
    fn json_columns_take_documents_as_json() {
        let col = ColumnInfo {
            name: "doc".into(),
            native_type: "jsonb".into(),
            canonical_type: CanonicalType::Json,
            nullable: true,
            default_expression: None,
            is_generated: false,
            is_identity: false,
            comment: None,
            ordinal: 1,
        };
        let v = coerce(Value::Array(vec![Value::Int(1)]), Some(&col));
        assert_eq!(v, Value::Json(json!([1])));
        assert_eq!(coerce(Value::Null, Some(&col)), Value::Null);
    }

    #[test]
    fn compare_and_set_skips_values_without_reliable_equality() {
        assert!(unchanged_condition("a", &Value::Int(1), false).is_some());
        assert!(unchanged_condition("a", &Value::Float(1.5), false).is_none());
        assert!(matches!(
            unchanged_condition("a", &Value::Missing, true),
            Some(Filter::IsMissing { .. })
        ));
        assert!(unchanged_condition("a", &Value::Missing, false).is_none());
    }
}
