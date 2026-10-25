//! Export, import and file-picker commands.
//!
//! File paths come from native dialogs opened here, on the Rust side; the
//! WebView has no file-system permission of its own.

use std::path::PathBuf;

use gazabridge_application::{ExportRequest, ImportRequest, Progress};
use gazabridge_domain::job::WriteMode;
use gazabridge_files::{
    Compression, CsvOptions, ExtraFields, FileFormat, JsonFlavor, MalformedPolicy, ReadOptions,
    WriteOptions,
};
use gazabridge_ipc::{
    CommandError, ExportArgs, FileDto, ImportArgs, OpArgs, PickPathArgs, TransferStatusDto,
    TransferSummaryDto,
};
use tauri::State;
use tauri_plugin_dialog::DialogExt;

use crate::connection_commands::{app_error, parse_id};
use crate::explorer_commands::{combine, filter, native, object_ref};
use crate::state::AppState;

fn invalid(message: impl Into<String>) -> CommandError {
    CommandError {
        code: "invalid_input".into(),
        message: message.into(),
    }
}

fn parse<T: serde::de::DeserializeOwned>(what: &str, value: &str) -> Result<T, CommandError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| invalid(format!("unknown {what} `{value}`")))
}

fn flavor(value: &str) -> Result<JsonFlavor, CommandError> {
    if value.is_empty() {
        Ok(JsonFlavor::Typed)
    } else {
        parse("JSON flavor", value)
    }
}

fn compression(gzip: bool) -> Compression {
    if gzip {
        Compression::Gzip
    } else {
        Compression::None
    }
}

pub fn export_request(args: &ExportArgs) -> Result<ExportRequest, CommandError> {
    let format: FileFormat = parse("format", &args.format)?;
    let delimiter = match args.csv_delimiter.as_str() {
        "" | "," => b',',
        ";" => b';',
        "\t" | "tab" => b'\t',
        "|" => b'|',
        other => return Err(invalid(format!("unsupported CSV delimiter `{other}`"))),
    };
    let q = native(args.query.as_ref())?;
    Ok(ExportRequest {
        object: object_ref(&args.object)?,
        filter: combine(filter(args.filter.as_ref())?, q.filter),
        projection: q.include,
        exclude: q.exclude,
        path: PathBuf::from(&args.path),
        options: WriteOptions {
            format,
            json_flavor: flavor(&args.json_flavor)?,
            compression: compression(args.gzip),
            pretty: args.pretty,
            overwrite: args.overwrite,
            csv: CsvOptions {
                delimiter,
                header: true,
                columns: args
                    .csv_columns
                    .iter()
                    .map(|c| c.trim().to_owned())
                    .filter(|c| !c.is_empty())
                    .collect(),
                null_text: args.csv_null_text.clone(),
                extra_fields: if args.csv_ignore_extra {
                    ExtraFields::Ignore
                } else {
                    ExtraFields::Fail
                },
            },
        },
    })
}

pub fn import_request(args: &ImportArgs) -> Result<ImportRequest, CommandError> {
    let malformed = match args.malformed.as_str() {
        "fail" | "" => MalformedPolicy::Fail,
        "skip" => MalformedPolicy::Skip,
        "quarantine" => MalformedPolicy::Quarantine {
            path: PathBuf::from(
                args.quarantine_path
                    .as_deref()
                    .filter(|p| !p.trim().is_empty())
                    .ok_or_else(|| invalid("choose a file for rejected records"))?,
            ),
        },
        other => {
            return Err(invalid(format!(
                "unknown malformed-record policy `{other}`"
            )));
        }
    };
    let mode: WriteMode = parse("write mode", &args.mode)?;
    Ok(ImportRequest {
        object: object_ref(&args.object)?,
        path: PathBuf::from(&args.path),
        options: ReadOptions {
            format: parse("format", &args.format)?,
            json_flavor: flavor(&args.json_flavor)?,
            compression: compression(args.gzip),
            malformed,
        },
        mode,
        key_fields: args
            .key_fields
            .iter()
            .map(|k| k.trim().to_owned())
            .filter(|k| !k.is_empty())
            .collect(),
        batch_size: args.batch_size as usize,
        stop_on_error: args.stop_on_error,
    })
}

pub fn status_dto(p: Progress) -> TransferStatusDto {
    let s = p.summary;
    TransferStatusDto {
        phase: serde_json::to_value(p.phase)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        records: p.records,
        summary: TransferSummaryDto {
            records_read: s.records_read,
            inserted: s.inserted,
            updated: s.updated,
            skipped: s.skipped,
            failed: s.failed,
            malformed: s.malformed,
            quarantine_path: s.quarantine_path.map(|q| q.display().to_string()),
            messages: s.messages,
            file: s.file.map(|f| FileDto {
                path: f.path.display().to_string(),
                records: f.records,
                bytes: f.bytes,
                sha256: f.sha256,
                ignored_fields: f.ignored_fields.into_iter().collect(),
            }),
        },
        error: p.error,
        error_code: p.error_code,
    }
}

pub async fn export_impl(state: &AppState, args: ExportArgs) -> Result<(), CommandError> {
    let id = parse_id(&args.connection_id)?;
    let req = export_request(&args)?;
    state
        .transfers
        .start_export(id, &args.op_id, req)
        .await
        .map_err(|e| app_error(state, e))
}

pub async fn import_impl(state: &AppState, args: ImportArgs) -> Result<(), CommandError> {
    let id = parse_id(&args.connection_id)?;
    let req = import_request(&args)?;
    state
        .transfers
        .start_import(id, &args.op_id, req)
        .await
        .map_err(|e| app_error(state, e))
}

pub fn status_impl(state: &AppState, op_id: &str) -> Result<TransferStatusDto, CommandError> {
    state
        .transfers
        .status(op_id)
        .map(status_dto)
        .ok_or_else(|| CommandError {
            code: "not_found".into(),
            message: format!("no operation `{op_id}`"),
        })
}

#[tauri::command]
pub async fn transfer_export(
    state: State<'_, AppState>,
    args: ExportArgs,
) -> Result<(), CommandError> {
    export_impl(&state, args).await
}

#[tauri::command]
pub async fn transfer_import(
    state: State<'_, AppState>,
    args: ImportArgs,
) -> Result<(), CommandError> {
    import_impl(&state, args).await
}

#[tauri::command]
pub async fn transfer_status(
    state: State<'_, AppState>,
    args: OpArgs,
) -> Result<TransferStatusDto, CommandError> {
    status_impl(&state, &args.op_id)
}

#[tauri::command]
pub async fn transfer_cancel(
    state: State<'_, AppState>,
    args: OpArgs,
) -> Result<bool, CommandError> {
    Ok(state.transfers.cancel(&args.op_id))
}

#[tauri::command]
pub async fn transfer_forget(state: State<'_, AppState>, args: OpArgs) -> Result<(), CommandError> {
    state.transfers.forget(&args.op_id);
    Ok(())
}

/// Opens a native save or open dialog; `None` when the user cancels.
#[tauri::command]
pub async fn pick_file_path(
    app: tauri::AppHandle,
    args: PickPathArgs,
) -> Result<Option<String>, CommandError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut dialog = app.dialog().file();
    if !args.extensions.is_empty() {
        let exts: Vec<&str> = args.extensions.iter().map(String::as_str).collect();
        dialog = dialog.add_filter("Data files", &exts);
    }
    if let Some(name) = args.default_name.as_deref() {
        dialog = dialog.set_file_name(name);
    }
    let send = move |p: Option<tauri_plugin_dialog::FilePath>| {
        let _ = tx.send(
            p.and_then(|p| p.into_path().ok())
                .map(|p| p.display().to_string()),
        );
    };
    if args.save {
        dialog.save_file(send);
    } else {
        dialog.pick_file(send);
    }
    rx.await.map_err(|_| CommandError {
        code: "ipc".into(),
        message: "the file dialog closed unexpectedly".into(),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gazabridge_ipc::{
        ConnectionForm, EngineForm, ObjectAddr, PasswordChoice, SaveConnectionArgs, TlsForm,
    };

    use super::*;
    use crate::connection_commands::save_connection_impl;

    async fn wait(state: &AppState, op: &str) -> TransferStatusDto {
        for _ in 0..200 {
            let s = status_impl(state, op).unwrap();
            if s.phase != "running" {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("operation {op} did not finish");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn export_then_import_on_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::initialize(dir.path().join("data"), dir.path().join("logs"))
            .await
            .unwrap();
        let EngineForm::Sqlite(mut s) = EngineForm::default_for("sqlite").unwrap() else {
            unreachable!()
        };
        s.path = dir.path().join("t.db").display().to_string();
        s.create_if_missing = true;
        let form = ConnectionForm {
            id: None,
            name: "t".into(),
            profile_id: "sqlite.sqlite".into(),
            engine: EngineForm::Sqlite(s),
            tls: TlsForm::default(),
            read_only: false,
            favorite: false,
            color: None,
            tags: vec![],
            folder_id: None,
        };
        let cid = save_connection_impl(
            &state,
            SaveConnectionArgs {
                form,
                password: PasswordChoice::None,
                connection_string: None,
            },
        )
        .await
        .unwrap()
        .form
        .id
        .unwrap();
        let mut q = gazabridge_ipc::RunQueryArgs {
            connection_id: cid.clone(),
            run_id: "x".into(),
            language: "sqlite_sql".into(),
            text: String::new(),
            params: vec![],
            database: None,
            collection: None,
            max_rows: 10,
            timeout_secs: None,
            confirmed: false,
        };
        for sql in [
            "CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT, doc TEXT)",
            "CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT, doc TEXT)",
            "INSERT INTO a (name, doc) VALUES ('x', NULL), ('y', '{\"k\": 1}'), ('z', 'plain')",
        ] {
            q.text = sql.into();
            crate::query_commands::run_impl(&state, q.clone())
                .await
                .unwrap();
        }
        let table = |name: &str| ObjectAddr {
            database: None,
            schema: Some("main".into()),
            name: name.into(),
            kind: "table".into(),
        };
        let out = dir.path().join("a.jsonl.gz");
        let export = ExportArgs {
            connection_id: cid.clone(),
            op_id: "e1".into(),
            object: table("a"),
            filter: None,
            query: None,
            path: out.display().to_string(),
            format: "jsonl".into(),
            json_flavor: String::new(),
            gzip: true,
            pretty: false,
            overwrite: false,
            csv_columns: vec![],
            csv_delimiter: String::new(),
            csv_null_text: String::new(),
            csv_ignore_extra: false,
        };
        export_impl(&state, export.clone()).await.unwrap();
        let done = wait(&state, "e1").await;
        assert_eq!(done.phase, "done", "{done:?}");
        let file = done.summary.file.unwrap();
        assert_eq!(file.records, 3);
        assert_eq!(file.sha256.len(), 64);

        // Exporting to the same file again is refused up front.
        let mut again = export.clone();
        again.op_id = "e2".into();
        assert_eq!(
            export_impl(&state, again).await.unwrap_err().code,
            "file_exists"
        );

        // CSV with automatic columns.
        let mut csv = export;
        csv.op_id = "e3".into();
        csv.format = "csv".into();
        csv.gzip = false;
        csv.path = dir.path().join("a.csv").display().to_string();
        csv.csv_null_text = "NULL".into();
        export_impl(&state, csv).await.unwrap();
        assert_eq!(wait(&state, "e3").await.phase, "done");
        let text = std::fs::read_to_string(dir.path().join("a.csv")).unwrap();
        assert!(text.starts_with("id,name,doc\n1,x,NULL\n"), "{text}");

        // Import into the second table.
        let import = ImportArgs {
            connection_id: cid.clone(),
            op_id: "i1".into(),
            object: table("b"),
            path: out.display().to_string(),
            format: "jsonl".into(),
            json_flavor: String::new(),
            gzip: true,
            malformed: "fail".into(),
            quarantine_path: None,
            mode: "insert".into(),
            key_fields: vec![],
            batch_size: 2,
            stop_on_error: true,
        };
        import_impl(&state, import.clone()).await.unwrap();
        let done = wait(&state, "i1").await;
        assert_eq!(done.phase, "done", "{done:?}");
        assert_eq!(done.summary.inserted, 3);

        // Importing again hits the primary key: stop at the first rejection.
        let mut dup = import.clone();
        dup.op_id = "i2".into();
        import_impl(&state, dup).await.unwrap();
        let failed = wait(&state, "i2").await;
        assert_eq!(failed.phase, "failed", "{failed:?}");

        // Upsert updates instead.
        let mut up = import;
        up.op_id = "i3".into();
        up.mode = "upsert".into();
        up.key_fields = vec!["id".into()];
        import_impl(&state, up).await.unwrap();
        let done = wait(&state, "i3").await;
        assert_eq!(done.phase, "done", "{done:?}");
        assert_eq!(done.summary.inserted + done.summary.updated, 3);

        state.transfers.forget("i3");
        assert_eq!(status_impl(&state, "i3").unwrap_err().code, "not_found");
        let audit = state
            .store
            .audit()
            .list(Some("object"), None, 20)
            .await
            .unwrap();
        assert!(audit.iter().any(|a| a.action == "data.export"));
        assert!(audit.iter().any(|a| a.action == "data.import"));
    }

    #[test]
    fn bad_arguments_are_rejected() {
        let addr = ObjectAddr {
            database: None,
            schema: None,
            name: "t".into(),
            kind: "table".into(),
        };
        let mut args = ImportArgs {
            connection_id: "x".into(),
            op_id: "o".into(),
            object: addr,
            path: "/tmp/x.jsonl".into(),
            format: "jsonl".into(),
            json_flavor: String::new(),
            gzip: false,
            malformed: "quarantine".into(),
            quarantine_path: None,
            mode: "insert".into(),
            key_fields: vec![],
            batch_size: 10,
            stop_on_error: false,
        };
        assert!(import_request(&args).is_err(), "quarantine needs a path");
        args.malformed = "skip".into();
        args.mode = "truncate_and_insert".into();
        assert!(
            import_request(&args).is_ok(),
            "parsed here, refused by the service"
        );
        args.format = "xlsx".into();
        assert!(import_request(&args).is_err());
    }
}
