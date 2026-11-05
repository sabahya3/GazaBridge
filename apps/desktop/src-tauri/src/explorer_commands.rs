//! Explorer, data viewer and data editor commands.

use gazabridge_application::{BrowseRequest, BrowseResult, EditPlan};
use gazabridge_domain::ejson;
use gazabridge_ipc::{
    BrowseArgs, BrowsePageDto, CellDto, ColumnDto, CommandError, ConditionDto, CountArgs, CountDto,
    DeleteRecordArgs, EditArgs, EditPlanDto, ExplorerListArgs, FieldEditDto, FilterDto, FilterOp,
    InsertArgs, MutationDto, NativeQueryDto, NodeDto, ObjectAddr, TreeCountsArgs,
};
use gazabridge_plugin_sdk::filter::{FieldPath, Filter, SortDirection, SortKey};
use gazabridge_plugin_sdk::schema::{NamespacePath, ObjectKind, ObjectRef};
use tauri::State;

use crate::connection_commands::{app_error, parse_id};
use crate::state::AppState;

/// Longest cell text sent to the table view.
const CELL_CHARS: usize = 200;

fn invalid(message: impl Into<String>) -> CommandError {
    CommandError {
        connection_id: None,
        code: "invalid_input".into(),
        message: message.into(),
    }
}

fn kind_name(kind: ObjectKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

pub fn object_ref(addr: &ObjectAddr) -> Result<ObjectRef, CommandError> {
    let kind: ObjectKind = serde_json::from_value(serde_json::Value::String(addr.kind.clone()))
        .map_err(|_| invalid(format!("unknown object kind `{}`", addr.kind)))?;
    Ok(ObjectRef {
        namespace: NamespacePath {
            database: addr.database.clone(),
            schema: addr.schema.clone(),
        },
        name: addr.name.clone(),
        kind,
    })
}

/// Typed JSON when the text parses as JSON, otherwise the text itself.
fn condition_value(text: &str) -> Result<gazabridge_domain::Value, CommandError> {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(j) => ejson::from_ejson(&j).map_err(|e| invalid(e.to_string())),
        Err(_) => Ok(gazabridge_domain::Value::String(text.to_owned())),
    }
}

fn condition(c: &ConditionDto) -> Result<Filter, CommandError> {
    if c.field.trim().is_empty() {
        return Err(invalid("a filter condition needs a field"));
    }
    let field = FieldPath::new(c.field.trim());
    Ok(match c.op {
        FilterOp::Eq => Filter::Eq {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Ne => Filter::Ne {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Lt => Filter::Lt {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Le => Filter::Le {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Gt => Filter::Gt {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Ge => Filter::Ge {
            field,
            value: condition_value(&c.value)?,
        },
        FilterOp::Contains => Filter::Contains {
            field,
            text: c.value.clone(),
        },
        FilterOp::StartsWith => Filter::StartsWith {
            field,
            text: c.value.clone(),
        },
        FilterOp::IsNull => Filter::IsNull { field },
        FilterOp::IsNotNull => Filter::IsNotNull { field },
        FilterOp::IsMissing => Filter::IsMissing { field },
    })
}

/// The query bar's parts: native filter, included and excluded fields, sort.
#[derive(Debug)]
pub struct NativeParts {
    pub filter: Option<Filter>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub sort: Vec<SortKey>,
}

fn shell(
    what: &str,
    text: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, CommandError> {
    match gazabridge_query::shell::parse(text) {
        Ok(serde_json::Value::Object(m)) => Ok(m),
        Ok(_) => Err(invalid(format!(
            "{what} must be a document, e.g. {{ field: 1 }}"
        ))),
        Err(e) => Err(invalid(format!("{what}: {e}"))),
    }
}

fn truthy(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::Number(n) => n.as_f64().map(|f| f != 0.0),
        _ => None,
    }
}

pub fn native(q: Option<&NativeQueryDto>) -> Result<NativeParts, CommandError> {
    let mut parts = NativeParts {
        filter: None,
        include: vec![],
        exclude: vec![],
        sort: vec![],
    };
    let Some(q) = q else { return Ok(parts) };
    let f = shell("Filter", &q.filter)?;
    if !f.is_empty() {
        parts.filter = Some(Filter::Native {
            query: serde_json::Value::Object(f),
        });
    }
    for (k, v) in shell("Project", &q.project)? {
        match truthy(&v) {
            Some(true) => parts.include.push(k),
            Some(false) => parts.exclude.push(k),
            None => {
                return Err(invalid(format!(
                    "Project: use 1 to show `{k}` or 0 to hide it"
                )));
            }
        }
    }
    if !parts.include.is_empty() && parts.exclude.iter().any(|k| k != "_id") {
        return Err(invalid(
            "Project: either list fields to show (1) or fields to hide (0), not both (except _id: 0)",
        ));
    }
    if !parts.include.is_empty() && parts.exclude.iter().any(|k| k == "_id") {
        parts.exclude.clear();
    } else if !parts.include.is_empty() && !parts.include.iter().any(|k| k == "_id") {
        // MongoDB shows _id unless it is hidden explicitly.
        parts.include.insert(0, "_id".into());
    }
    for (k, v) in shell("Sort", &q.sort)? {
        let direction = match v.as_i64() {
            Some(1) => SortDirection::Asc,
            Some(-1) => SortDirection::Desc,
            _ => {
                return Err(invalid(format!(
                    "Sort: use 1 (ascending) or -1 (descending) for `{k}`"
                )));
            }
        };
        parts.sort.push(SortKey {
            field: FieldPath::new(k),
            direction,
        });
    }
    Ok(parts)
}

/// Both the condition builder and the query bar's filter, when present.
pub fn combine(a: Option<Filter>, b: Option<Filter>) -> Option<Filter> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Filter::And {
            filters: vec![a, b],
        }),
        (a, b) => a.or(b),
    }
}

pub fn filter(dto: Option<&FilterDto>) -> Result<Option<Filter>, CommandError> {
    let Some(dto) = dto.filter(|f| !f.conditions.is_empty()) else {
        return Ok(None);
    };
    let filters = dto
        .conditions
        .iter()
        .map(condition)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(if dto.any {
        Filter::Or { filters }
    } else {
        Filter::And { filters }
    }))
}

/// Columns and rows for display, in `fields` order.
pub(crate) fn table_dto(
    fields: &[String],
    declared: &[gazabridge_plugin_sdk::schema::ColumnInfo],
    key_fields: &[String],
    records: &[gazabridge_domain::Record],
) -> (Vec<ColumnDto>, Vec<gazabridge_ipc::RowDto>) {
    let columns = fields
        .iter()
        .map(|name| {
            let declared = declared.iter().find(|c| &c.name == name);
            ColumnDto {
                name: name.clone(),
                native_type: declared.map(|c| c.native_type.clone()).unwrap_or_default(),
                nullable: declared.is_none_or(|c| c.nullable),
                key: key_fields.contains(name),
            }
        })
        .collect();
    let rows = records
        .iter()
        .map(|r| gazabridge_ipc::RowDto {
            cells: fields
                .iter()
                .map(|f| {
                    let v = r.get(f);
                    CellDto {
                        kind: v.kind().as_str().to_owned(),
                        text: ejson::display_text(v, CELL_CHARS),
                    }
                })
                .collect(),
            json: ejson::record_to_ejson_string(r),
        })
        .collect();
    (columns, rows)
}

fn page_dto(result: BrowseResult) -> BrowsePageDto {
    let (columns, rows) = table_dto(
        &result.fields,
        &result.columns,
        &result.key_fields,
        &result.records,
    );
    BrowsePageDto {
        columns,
        rows,
        offset: result.offset,
        has_more: result.has_more,
        key_fields: result.key_fields,
        edit_blocker: result.edit_blocker.and_then(|b| {
            serde_json::to_value(b)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
        }),
    }
}

fn plan_dto(plan: EditPlan) -> EditPlanDto {
    EditPlanDto {
        changes: plan
            .changes
            .into_iter()
            .map(|c| FieldEditDto {
                field: c.field,
                before: c.before.map(|j| j.to_string()),
                after: c.after.map(|j| j.to_string()),
            })
            .collect(),
        conflict: plan.conflict,
    }
}

pub async fn list_impl(
    state: &AppState,
    args: ExplorerListArgs,
) -> Result<Vec<NodeDto>, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let parent = NamespacePath {
        database: args.database,
        schema: args.schema,
    };
    let entries = state
        .explorer
        .list(id, &parent)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(entries
        .into_iter()
        .map(|e| NodeDto {
            expandable: matches!(e.kind, ObjectKind::Database | ObjectKind::Schema),
            readable: e.kind.is_readable(),
            kind: kind_name(e.kind),
            name: e.name,
            estimated_count: e.estimated_count,
            comment: e.comment,
        })
        .collect())
}

pub async fn browse_impl(
    state: &AppState,
    args: BrowseArgs,
) -> Result<BrowsePageDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let q = native(args.query.as_ref())?;
    let request = BrowseRequest {
        object: object_ref(&args.object)?,
        filter: combine(filter(args.filter.as_ref())?, q.filter),
        projection: q.include,
        exclude: q.exclude,
        order_by: if !q.sort.is_empty() {
            q.sort
        } else {
            args.sort
                .iter()
                .map(|s| SortKey {
                    field: FieldPath::new(s.field.clone()),
                    direction: if s.descending {
                        SortDirection::Desc
                    } else {
                        SortDirection::Asc
                    },
                })
                .collect()
        },
        offset: args.offset,
        limit: args.limit,
    };
    let result = state
        .explorer
        .browse(id, request)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(page_dto(result))
}

pub async fn count_impl(
    state: &AppState,
    args: CountArgs,
) -> Result<Option<CountDto>, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    let filter = combine(
        filter(args.filter.as_ref())?,
        native(args.query.as_ref())?.filter,
    );
    let c = state
        .explorer
        .count(
            id,
            &object,
            filter.as_ref(),
            args.exact,
            std::time::Duration::from_millis(u64::from(
                args.max_time_ms.unwrap_or(3_000).clamp(100, 120_000),
            )),
        )
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(c.map(|c| CountDto {
        count: c.count,
        exact: c.exact,
    }))
}

pub async fn insert_impl(state: &AppState, args: InsertArgs) -> Result<(), CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    state
        .explorer
        .insert(id, &object, &args.json)
        .await
        .map_err(|e| app_error(state, e))
}

pub async fn plan_update_impl(
    state: &AppState,
    args: EditArgs,
) -> Result<EditPlanDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    let plan = state
        .explorer
        .plan_update(id, &object, &args.original, &args.edited)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(plan_dto(plan))
}

pub async fn apply_update_impl(
    state: &AppState,
    args: EditArgs,
) -> Result<MutationDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    let o = state
        .explorer
        .apply_update(id, &object, &args.original, &args.edited)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(MutationDto {
        matched: o.matched,
        modified: o.modified,
    })
}

pub async fn delete_impl(
    state: &AppState,
    args: DeleteRecordArgs,
) -> Result<MutationDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    let o = state
        .explorer
        .delete(id, &object, &args.original)
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(MutationDto {
        matched: o.matched,
        modified: o.modified,
    })
}

pub async fn tree_counts_impl(
    state: &AppState,
    args: TreeCountsArgs,
) -> Result<Vec<Option<u64>>, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let objects = args
        .objects
        .iter()
        .map(object_ref)
        .collect::<Result<Vec<_>, _>>()?;
    state
        .explorer
        .estimated_counts(id, objects)
        .await
        .map_err(|e| app_error(state, e))
}

#[tauri::command]
pub async fn explorer_tree_counts(
    state: State<'_, AppState>,
    args: TreeCountsArgs,
) -> Result<Vec<Option<u64>>, CommandError> {
    tree_counts_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_list(
    state: State<'_, AppState>,
    args: ExplorerListArgs,
) -> Result<Vec<NodeDto>, CommandError> {
    list_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_browse(
    state: State<'_, AppState>,
    args: BrowseArgs,
) -> Result<BrowsePageDto, CommandError> {
    browse_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_count(
    state: State<'_, AppState>,
    args: CountArgs,
) -> Result<Option<CountDto>, CommandError> {
    count_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_insert(
    state: State<'_, AppState>,
    args: InsertArgs,
) -> Result<(), CommandError> {
    insert_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_plan_update(
    state: State<'_, AppState>,
    args: EditArgs,
) -> Result<EditPlanDto, CommandError> {
    plan_update_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_apply_update(
    state: State<'_, AppState>,
    args: EditArgs,
) -> Result<MutationDto, CommandError> {
    apply_update_impl(&state, args).await
}

#[tauri::command]
pub async fn explorer_delete(
    state: State<'_, AppState>,
    args: DeleteRecordArgs,
) -> Result<MutationDto, CommandError> {
    delete_impl(&state, args).await
}

/// Arguments keep their snake_case names on the JS side.
#[tauri::command(rename_all = "snake_case")]
pub async fn explorer_disconnect(
    state: State<'_, AppState>,
    connection_id: String,
) -> Result<(), CommandError> {
    state.explorer.disconnect(parse_id(&connection_id)?).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use gazabridge_ipc::{
        ConnectionForm, EngineForm, PasswordChoice, SaveConnectionArgs, SortDto, TlsForm,
    };

    use gazabridge_domain::ids::ConnectionId;

    use super::*;
    use crate::connection_commands::save_connection_impl;

    #[test]
    fn query_bar_parts_follow_mongodb_rules() {
        let q = |filter: &str, project: &str, sort: &str| NativeQueryDto {
            filter: filter.into(),
            project: project.into(),
            sort: sort.into(),
        };
        let p = native(Some(&q(
            "{status: 'open'}",
            "{name: 1, email: 1}",
            "{createdAt: -1}",
        )))
        .unwrap();
        assert!(matches!(p.filter, Some(Filter::Native { .. })));
        assert_eq!(
            p.include,
            ["_id", "name", "email"],
            "_id is shown unless hidden"
        );
        assert!(matches!(p.sort[0].direction, SortDirection::Desc));
        let p = native(Some(&q("", "{name: 1, _id: 0}", ""))).unwrap();
        assert_eq!(
            (p.include.clone(), p.exclude.clone()),
            (vec!["name".to_owned()], vec![])
        );
        assert!(p.filter.is_none());
        let p = native(Some(&q("{}", "{secret: 0}", ""))).unwrap();
        assert_eq!(p.exclude, ["secret"]);
        assert!(native(Some(&q("", "{a: 1, b: 0}", ""))).is_err());
        assert!(native(Some(&q("", "", "{a: 2}"))).is_err());
        assert!(
            native(Some(&q("{status: open}", "", "")))
                .unwrap_err()
                .message
                .contains("quote")
        );
    }

    #[test]
    fn conditions_parse_typed_json_and_fall_back_to_text() {
        let f = filter(Some(&FilterDto {
            any: true,
            conditions: vec![
                ConditionDto {
                    field: "a".into(),
                    op: FilterOp::Eq,
                    value: "42".into(),
                },
                ConditionDto {
                    field: "b".into(),
                    op: FilterOp::Eq,
                    value: "plain text".into(),
                },
                ConditionDto {
                    field: "_id".into(),
                    op: FilterOp::Eq,
                    value: r#"{"$oid":"0102030405060708090a0b0c"}"#.into(),
                },
                ConditionDto {
                    field: "c".into(),
                    op: FilterOp::IsMissing,
                    value: String::new(),
                },
            ],
        }))
        .unwrap()
        .unwrap();
        let Filter::Or { filters } = f else {
            panic!("{f:?}")
        };
        assert!(matches!(
            &filters[0],
            Filter::Eq {
                value: gazabridge_domain::Value::Int(42),
                ..
            }
        ));
        assert!(matches!(
            &filters[1],
            Filter::Eq {
                value: gazabridge_domain::Value::String(_),
                ..
            }
        ));
        assert!(matches!(
            &filters[2],
            Filter::Eq {
                value: gazabridge_domain::Value::ObjectId(_),
                ..
            }
        ));
        assert!(filter(Some(&FilterDto::default())).unwrap().is_none());
        assert!(
            object_ref(&ObjectAddr {
                database: None,
                schema: None,
                name: "x".into(),
                kind: "spaceship".into()
            })
            .is_err()
        );
    }

    /// Browse, insert, edit (with conflict detection) and delete on SQLite.
    #[tokio::test]
    async fn explorer_end_to_end_on_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        let db = dir.path().join("shop.db");
        let cid = {
            let EngineForm::Sqlite(mut s) = EngineForm::default_for("sqlite").unwrap() else {
                unreachable!()
            };
            s.path = db.display().to_string();
            s.create_if_missing = true;
            let form = ConnectionForm {
                id: None,
                name: "shop".into(),
                profile_id: "sqlite.sqlite".into(),
                engine: EngineForm::Sqlite(s),
                tls: TlsForm::default(),
                ssh: None,
                read_only: false,
                favorite: false,
                color: None,
                tags: vec![],
                folder_id: None,
            };
            let saved = save_connection_impl(
                &state,
                SaveConnectionArgs {
                    form,
                    password: PasswordChoice::None,
                    connection_string: None,
                    ssh_secret: PasswordChoice::Keep,
                },
            )
            .await
            .unwrap();
            let id: ConnectionId = saved.form.id.unwrap().parse().unwrap();
            // Create a table through the connector itself.
            let conn = state.connections.open(id).await.unwrap();
            conn.execute(
                gazabridge_plugin_sdk::query::QueryRequest {
                    language: gazabridge_plugin_sdk::capability::QueryLanguage::SqliteSql,
                    text: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER, note TEXT)".into(),
                    params: vec![],
                    max_rows: 10,
                    timeout: None,
                    database: None,
                    collection: None,
                },
                Default::default(),
            )
            .await
            .unwrap();
            conn.close().await;
            id.to_string()
        };

        let roots = list_impl(
            &state,
            ExplorerListArgs {
                connection_id: cid.clone(),
                database: None,
                schema: None,
            },
        )
        .await
        .unwrap();
        let main = roots
            .iter()
            .find(|n| n.name == "main")
            .expect("main schema");
        assert!(main.expandable);
        let tables = list_impl(
            &state,
            ExplorerListArgs {
                connection_id: cid.clone(),
                database: None,
                schema: Some("main".into()),
            },
        )
        .await
        .unwrap();
        let items = tables.iter().find(|n| n.name == "items").unwrap();
        assert!(items.readable);
        let object = ObjectAddr {
            database: None,
            schema: Some("main".into()),
            name: "items".into(),
            kind: items.kind.clone(),
        };

        for (name, qty) in [("apple", 3), ("pear", 5), ("fig", 1)] {
            insert_impl(
                &state,
                InsertArgs {
                    connection_id: cid.clone(),
                    object: object.clone(),
                    json: format!(r#"{{"name": "{name}", "qty": {qty}}}"#),
                },
            )
            .await
            .unwrap();
        }
        let err = insert_impl(
            &state,
            InsertArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                json: r#"{"nope": 1}"#.into(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, "invalid_input");

        let browse = |filter: Option<FilterDto>, sort: Option<SortDto>, offset, limit| BrowseArgs {
            connection_id: cid.clone(),
            object: object.clone(),
            filter,
            sort,
            query: None,
            offset,
            limit,
        };
        let page = browse_impl(
            &state,
            browse(
                None,
                Some(SortDto {
                    field: "qty".into(),
                    descending: true,
                }),
                0,
                2,
            ),
        )
        .await
        .unwrap();
        assert_eq!(page.key_fields, ["id"]);
        assert_eq!(page.edit_blocker, None);
        assert!(page.has_more);
        let name_col = page.columns.iter().position(|c| c.name == "name").unwrap();
        assert_eq!(page.rows[0].cells[name_col].text, "pear");
        let note_col = page.columns.iter().position(|c| c.name == "note").unwrap();
        assert_eq!(page.rows[0].cells[note_col].kind, "null");

        let filtered = FilterDto {
            any: false,
            conditions: vec![ConditionDto {
                field: "name".into(),
                op: FilterOp::StartsWith,
                value: "f".into(),
            }],
        };
        let count = count_impl(
            &state,
            CountArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                filter: Some(filtered.clone()),
                query: None,
                exact: true,
                max_time_ms: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count.count, 1);

        // Edit pear: preview, then apply.
        let original = page.rows[0].json.clone();
        let edited = original
            .replace("\"qty\": 5", "\"qty\": 6")
            .replace("\"note\": null", "\"note\": \"ripe\"");
        let args = EditArgs {
            connection_id: cid.clone(),
            object: object.clone(),
            original: original.clone(),
            edited: edited.clone(),
        };
        let plan = plan_update_impl(&state, args.clone()).await.unwrap();
        assert!(!plan.conflict);
        let fields: Vec<_> = plan.changes.iter().map(|c| c.field.as_str()).collect();
        assert_eq!(fields, ["qty", "note"]);
        assert_eq!(
            apply_update_impl(&state, args.clone())
                .await
                .unwrap()
                .matched,
            1
        );

        // The same stale edit again is a conflict, not a silent overwrite.
        let again = apply_update_impl(&state, args).await.unwrap_err();
        assert_eq!(again.code, "conflict");
        let stale_plan = plan_update_impl(
            &state,
            EditArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                original: original.clone(),
                edited,
            },
        )
        .await
        .unwrap();
        assert!(stale_plan.conflict);

        // Key changes are refused.
        let page = browse_impl(&state, browse(Some(filtered), None, 0, 10))
            .await
            .unwrap();
        let fig = page.rows[0].json.clone();
        let rekey = fig.replacen("\"id\": 3", "\"id\": 99", 1);
        let err = plan_update_impl(
            &state,
            EditArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                original: fig.clone(),
                edited: rekey,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, "invalid_input", "{}", err.message);

        // Delete fig; deleting it twice fails.
        delete_impl(
            &state,
            DeleteRecordArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                original: fig.clone(),
            },
        )
        .await
        .unwrap();
        let err = delete_impl(
            &state,
            DeleteRecordArgs {
                connection_id: cid.clone(),
                object: object.clone(),
                original: fig,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, "conflict");

        // Audit entries name fields only.
        let audit = state
            .store
            .audit()
            .list(Some("object"), None, 10)
            .await
            .unwrap();
        let actions: Vec<_> = audit.iter().map(|a| a.action.as_str()).collect();
        assert!(
            actions.contains(&"data.update")
                && actions.contains(&"data.delete")
                && actions.contains(&"data.insert")
        );
        assert!(
            audit
                .iter()
                .all(|a| !a.details.to_string().contains("ripe"))
        );
    }
}
