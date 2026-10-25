//! Explorer, data viewer and data editor commands.

use gazabridge_application::{BrowseRequest, BrowseResult, EditPlan};
use gazabridge_domain::ejson;
use gazabridge_ipc::{
    BrowseArgs, BrowsePageDto, CellDto, ColumnDto, CommandError, ConditionDto, CountArgs, CountDto,
    DeleteRecordArgs, EditArgs, EditPlanDto, ExplorerListArgs, FieldEditDto, FilterDto, FilterOp,
    InsertArgs, MutationDto, NodeDto, ObjectAddr,
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

fn page_dto(result: BrowseResult) -> BrowsePageDto {
    let columns = result
        .fields
        .iter()
        .map(|name| {
            let declared = result.columns.iter().find(|c| &c.name == name);
            ColumnDto {
                name: name.clone(),
                native_type: declared.map(|c| c.native_type.clone()).unwrap_or_default(),
                nullable: declared.is_none_or(|c| c.nullable),
                key: result.key_fields.contains(name),
            }
        })
        .collect();
    let rows = result
        .records
        .iter()
        .map(|r| gazabridge_ipc::RowDto {
            cells: result
                .fields
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
    let request = BrowseRequest {
        object: object_ref(&args.object)?,
        filter: filter(args.filter.as_ref())?,
        order_by: args
            .sort
            .iter()
            .map(|s| SortKey {
                field: FieldPath::new(s.field.clone()),
                direction: if s.descending {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
            })
            .collect(),
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

pub async fn count_impl(state: &AppState, args: CountArgs) -> Result<CountDto, CommandError> {
    let id = parse_id(&args.connection_id)?;
    let object = object_ref(&args.object)?;
    let filter = filter(args.filter.as_ref())?;
    let c = state
        .explorer
        .count(id, &object, filter.as_ref())
        .await
        .map_err(|e| app_error(state, e))?;
    Ok(CountDto {
        count: c.count,
        exact: c.exact,
    })
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
) -> Result<CountDto, CommandError> {
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
            },
        )
        .await
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
