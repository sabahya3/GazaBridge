//! Query Studio: running, cancelling and explaining ad-hoc queries, with
//! history and saved queries.
//!
//! Statements that destroy data in bulk (DROP, TRUNCATE, DELETE or UPDATE
//! without WHERE, `$out`/`$merge`) are refused until the user confirms them.
//! History text and errors are redacted before they are stored.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use gazabridge_domain::Value;
use gazabridge_domain::ids::{ConnectionId, SavedQueryId};
use gazabridge_plugin_sdk::ErrorKind;
use gazabridge_plugin_sdk::capability::{Capability, QueryLanguage};
use gazabridge_plugin_sdk::query::{ExplainPlan, QueryRequest, QueryResult};
use gazabridge_storage::MetadataStore;
use gazabridge_storage::queries::{HistoryEntry, HistoryStatus, NewHistoryEntry, SavedQuery};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::explorer::ExplorerService;
use crate::{AppError, AppResult};

/// Upper bound on rows returned to the UI for one query.
pub const MAX_ROWS_LIMIT: u32 = 50_000;
/// History entries kept per installation.
pub const HISTORY_KEEP: u32 = 2_000;

/// Something a query does that needs explicit confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryRisk {
    Drop,
    Truncate,
    DeleteWithoutWhere,
    UpdateWithoutWhere,
    OverwritesCollection,
}

#[derive(Debug, Clone)]
pub struct QueryInput {
    pub language: QueryLanguage,
    pub text: String,
    pub params: Vec<Value>,
    pub database: Option<String>,
    pub collection: Option<String>,
    pub max_rows: u32,
    pub timeout: Option<Duration>,
    /// The user confirmed the risks [`assess`] reported.
    pub confirmed: bool,
}

/// What a connection can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryContext {
    pub languages: Vec<QueryLanguage>,
    pub explain: bool,
    pub explain_analyze: bool,
    pub read_only: bool,
}

#[derive(Debug, Clone)]
pub struct QueryService {
    store: MetadataStore,
    explorer: ExplorerService,
    redactor: gazabridge_secrets::Redactor,
    running: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

/// Splits SQL into statements and words, ignoring comments, string
/// literals, quoted identifiers and dollar-quoted bodies.
fn sql_statements(text: &str) -> Vec<Vec<String>> {
    let chars: Vec<char> = text.chars().collect();
    let mut statements = Vec::new();
    let mut words: Vec<String> = Vec::new();
    let mut word = String::new();
    let mut i = 0;
    let flush = |word: &mut String, words: &mut Vec<String>| {
        if !word.is_empty() {
            words.push(word.to_uppercase());
            word.clear();
        }
    };
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            '-' if next == Some('-') => {
                flush(&mut word, &mut words);
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                flush(&mut word, &mut words);
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 1;
            }
            '\'' | '"' | '`' => {
                flush(&mut word, &mut words);
                let backslash = c == '\'' && i > 0 && matches!(chars[i - 1], 'e' | 'E');
                i += 1;
                while i < chars.len() {
                    if backslash && chars[i] == '\\' {
                        i += 2;
                        continue;
                    }
                    if chars[i] == c {
                        if chars.get(i + 1) == Some(&c) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            '$' if word.is_empty() => {
                // Dollar quote: $tag$ ... $tag$
                let start = i;
                let mut j = i + 1;
                while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                if j < chars.len() && chars[j] == '$' {
                    let tag: String = chars[start..=j].iter().collect();
                    let rest: String = chars[j + 1..].iter().collect();
                    match rest.find(&tag) {
                        Some(end) => {
                            i = j + 1 + rest[..end].chars().count() + tag.chars().count() - 1
                        }
                        None => i = chars.len(),
                    }
                } else {
                    i = j - 1;
                }
            }
            ';' => {
                flush(&mut word, &mut words);
                if !words.is_empty() {
                    statements.push(std::mem::take(&mut words));
                }
            }
            c if c.is_alphanumeric() || c == '_' => word.push(c),
            _ => flush(&mut word, &mut words),
        }
        i += 1;
    }
    flush(&mut word, &mut words);
    if !words.is_empty() {
        statements.push(words);
    }
    statements
}

/// Risks of running `text`, for the confirmation prompt.
pub fn assess(language: QueryLanguage, text: &str) -> Vec<QueryRisk> {
    let mut risks = Vec::new();
    let mut add = |r: QueryRisk| {
        if !risks.contains(&r) {
            risks.push(r);
        }
    };
    match language {
        QueryLanguage::MongoAggregation => {
            let writes = match serde_json::from_str::<serde_json::Value>(text) {
                Ok(serde_json::Value::Array(stages)) => stages
                    .iter()
                    .filter_map(serde_json::Value::as_object)
                    .any(|stage| stage.contains_key("$out") || stage.contains_key("$merge")),
                // Unparseable here: be conservative.
                _ => text.contains("$out") || text.contains("$merge"),
            };
            if writes {
                add(QueryRisk::OverwritesCollection);
            }
        }
        QueryLanguage::MongoFilter => {}
        _ => {
            for words in sql_statements(text) {
                let has = |w: &str| words.iter().any(|x| x == w);
                if has("DROP") {
                    add(QueryRisk::Drop);
                }
                if has("TRUNCATE") {
                    add(QueryRisk::Truncate);
                }
                if has("DELETE") && !has("WHERE") {
                    add(QueryRisk::DeleteWithoutWhere);
                }
                // `ON CONFLICT … DO UPDATE` is an upsert, not a bulk update.
                if words.first().is_some_and(|w| w == "UPDATE") && !has("WHERE") {
                    add(QueryRisk::UpdateWithoutWhere);
                }
            }
        }
    }
    risks
}

pub fn language_name(language: QueryLanguage) -> &'static str {
    match language {
        QueryLanguage::PostgresSql => "postgres_sql",
        QueryLanguage::MySql => "my_sql",
        QueryLanguage::SqliteSql => "sqlite_sql",
        QueryLanguage::MongoFilter => "mongo_filter",
        QueryLanguage::MongoAggregation => "mongo_aggregation",
    }
}

impl QueryService {
    pub fn new(
        store: MetadataStore,
        explorer: ExplorerService,
        redactor: gazabridge_secrets::Redactor,
    ) -> Self {
        Self {
            store,
            explorer,
            redactor,
            running: Arc::default(),
        }
    }

    pub async fn context(&self, id: ConnectionId) -> AppResult<QueryContext> {
        let conn = self.explorer.session(id).await?;
        let m = conn.manifest();
        Ok(QueryContext {
            languages: m.query_languages.iter().copied().collect(),
            explain: m.supports(Capability::ExplainQuery),
            explain_analyze: m.supports(Capability::ExplainAnalyze),
            read_only: conn.is_read_only(),
        })
    }

    fn request(input: &QueryInput) -> AppResult<QueryRequest> {
        if input.text.trim().is_empty() {
            return Err(AppError::Invalid("the query is empty".into()));
        }
        Ok(QueryRequest {
            language: input.language,
            text: input.text.clone(),
            params: input.params.clone(),
            max_rows: input.max_rows.clamp(1, MAX_ROWS_LIMIT),
            timeout: input.timeout,
            database: input.database.clone(),
            collection: input.collection.clone(),
        })
    }

    fn require_confirmation(input: &QueryInput) -> AppResult<()> {
        let risks = assess(input.language, &input.text);
        if risks.is_empty() || input.confirmed {
            return Ok(());
        }
        let names: Vec<String> = risks
            .iter()
            .filter_map(|r| {
                serde_json::to_value(r)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
            })
            .collect();
        Err(AppError::ConfirmationRequired(names.join(",")))
    }

    /// Runs a query. `run_id` lets [`QueryService::cancel`] stop it.
    pub async fn run(
        &self,
        id: ConnectionId,
        run_id: &str,
        input: QueryInput,
    ) -> AppResult<QueryResult> {
        Self::require_confirmation(&input)?;
        let request = Self::request(&input)?;
        let conn = self.explorer.session(id).await?;
        let token = CancellationToken::new();
        self.running
            .lock()
            .await
            .insert(run_id.to_owned(), token.clone());
        let started_at = Utc::now();
        let started = Instant::now();
        let r = conn.execute(request, token).await;
        self.running.lock().await.remove(run_id);
        let elapsed = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        let (status, error) = match &r {
            Ok(_) => (HistoryStatus::Ok, None),
            Err(e) if e.kind() == ErrorKind::Cancelled => (HistoryStatus::Cancelled, None),
            Err(e) => (
                HistoryStatus::Error,
                Some(self.redactor.scrub(&e.to_string())),
            ),
        };
        let (rows, affected) = match &r {
            Ok(res) => (Some(res.rows.len() as i64), res.affected.map(|a| a as i64)),
            Err(_) => (None, None),
        };
        self.store
            .queries()
            .record_history(NewHistoryEntry {
                connection_id: Some(id),
                language: language_name(input.language).to_owned(),
                text: self.redactor.scrub(&input.text),
                started_at,
                duration_ms: Some(elapsed),
                row_count: rows,
                affected,
                status,
                error_message: error,
            })
            .await?;
        self.store.queries().prune_history(HISTORY_KEEP).await?;
        self.explorer.checked(id, r).await
    }

    /// Cancels a running query. Returns false when nothing was running.
    pub async fn cancel(&self, run_id: &str) -> bool {
        match self.running.lock().await.remove(run_id) {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    /// Explains a query. `analyze` executes it inside a transaction that is
    /// always rolled back (PostgreSQL) or with execution statistics (MongoDB).
    pub async fn explain(
        &self,
        id: ConnectionId,
        run_id: &str,
        input: QueryInput,
        analyze: bool,
    ) -> AppResult<ExplainPlan> {
        if analyze {
            Self::require_confirmation(&input)?;
        }
        let request = Self::request(&input)?;
        let conn = self.explorer.session(id).await?;
        let token = CancellationToken::new();
        self.running
            .lock()
            .await
            .insert(run_id.to_owned(), token.clone());
        let r = conn.explain(request, analyze, token).await;
        self.running.lock().await.remove(run_id);
        self.explorer.checked(id, r).await
    }

    pub async fn history(
        &self,
        id: Option<ConnectionId>,
        limit: u32,
    ) -> AppResult<Vec<HistoryEntry>> {
        Ok(self
            .store
            .queries()
            .history(id, limit.clamp(1, 500))
            .await?)
    }

    pub async fn saved(&self, id: Option<ConnectionId>) -> AppResult<Vec<SavedQuery>> {
        Ok(self.store.queries().list_saved(id).await?)
    }

    /// Creates (`id` = None) or updates a saved query.
    pub async fn save(
        &self,
        id: Option<SavedQueryId>,
        name: &str,
        connection_id: Option<ConnectionId>,
        language: QueryLanguage,
        text: &str,
        favorite: bool,
    ) -> AppResult<SavedQuery> {
        if name.trim().is_empty() {
            return Err(AppError::Invalid("a saved query needs a name".into()));
        }
        let now = Utc::now();
        let created_at = match id {
            Some(existing) => self
                .store
                .queries()
                .list_saved(None)
                .await?
                .into_iter()
                .find(|q| q.id == existing)
                .map(|q| q.created_at)
                .ok_or(AppError::NotFound("saved query"))?,
            None => now,
        };
        let q = SavedQuery {
            id: id.unwrap_or_default(),
            name: name.trim().to_owned(),
            connection_id,
            language: language_name(language).to_owned(),
            text: text.to_owned(),
            favorite,
            created_at,
            updated_at: now,
        };
        self.store.queries().save(&q).await?;
        Ok(q)
    }

    pub async fn delete_saved(&self, id: SavedQueryId) -> AppResult<()> {
        Ok(self.store.queries().delete_saved(id).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(text: &str) -> Vec<QueryRisk> {
        assess(QueryLanguage::PostgresSql, text)
    }

    #[test]
    fn flags_bulk_destruction_only() {
        assert!(sql("SELECT * FROM t WHERE a = 1").is_empty());
        assert!(sql("DELETE FROM t WHERE id = 1").is_empty());
        assert_eq!(sql("delete from t"), [QueryRisk::DeleteWithoutWhere]);
        assert_eq!(sql("UPDATE t SET a = 1"), [QueryRisk::UpdateWithoutWhere]);
        assert!(sql("UPDATE t SET a = 1 WHERE id = 2").is_empty());
        assert_eq!(
            sql("SELECT 1; DROP TABLE t; TRUNCATE u"),
            [QueryRisk::Drop, QueryRisk::Truncate]
        );
        assert!(sql("INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET a = 2").is_empty());
    }

    #[test]
    fn ignores_keywords_in_strings_comments_and_identifiers() {
        assert!(sql("SELECT 'drop table x; delete from y' -- truncate\nFROM t").is_empty());
        assert!(sql("SELECT \"drop\" FROM t /* delete from t */").is_empty());
        assert!(sql("SELECT $$ DROP TABLE t $$, $f$ truncate $f$").is_empty());
        assert!(sql("SELECT E'it\\'s; drop' FROM t").is_empty());
        assert_eq!(
            sql("SELECT 'x'; DELETE FROM t"),
            [QueryRisk::DeleteWithoutWhere]
        );
    }

    #[test]
    fn mongo_pipelines_writing_collections_need_confirmation() {
        assert_eq!(
            assess(
                QueryLanguage::MongoAggregation,
                r#"[{"$match": {}}, {"$out": "copy"}]"#
            ),
            [QueryRisk::OverwritesCollection]
        );
        assert!(
            assess(
                QueryLanguage::MongoAggregation,
                r#"[{"$match": {"a": "$out"}}]"#
            )
            .is_empty()
        );
        assert!(assess(QueryLanguage::MongoFilter, r#"{"$out": 1}"#).is_empty());
    }
}
