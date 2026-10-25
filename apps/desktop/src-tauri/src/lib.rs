//! GazaBridge desktop backend.
//!
//! The WebView only talks to the commands in [`commands`]; it has no direct
//! filesystem, shell or network access. Commands delegate to the same crates
//! the CLI uses.

mod commands;
mod connection_commands;
mod explorer_commands;
mod forms;
mod query_commands;
mod state;

use tauri::Manager;

pub use state::AppState;

pub fn run() {
    let result = tauri::Builder::default()
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            let log_dir = app.path().app_log_dir()?;
            let state = tauri::async_runtime::block_on(AppState::initialize(data_dir, log_dir))?;
            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::app_info,
            commands::get_preferences,
            commands::set_preferences,
            commands::list_connections,
            commands::list_profiles,
            commands::query_logs,
            connection_commands::get_connection,
            connection_commands::save_connection,
            connection_commands::create_connection_from_string,
            connection_commands::delete_connection,
            connection_commands::duplicate_connection,
            connection_commands::test_connection,
            connection_commands::test_connection_draft,
            explorer_commands::explorer_list,
            explorer_commands::explorer_browse,
            explorer_commands::explorer_count,
            explorer_commands::explorer_insert,
            explorer_commands::explorer_plan_update,
            explorer_commands::explorer_apply_update,
            explorer_commands::explorer_delete,
            explorer_commands::explorer_disconnect,
            query_commands::query_context,
            query_commands::query_run,
            query_commands::query_explain,
            query_commands::query_cancel,
            query_commands::query_history,
            query_commands::query_list_saved,
            query_commands::query_save,
            query_commands::query_delete_saved,
        ])
        .run(tauri::generate_context!());
    if let Err(e) = result {
        // Logging may not be initialised if setup failed; stderr is the last resort.
        eprintln!("GazaBridge failed to start: {e}");
        std::process::exit(1);
    }
}
