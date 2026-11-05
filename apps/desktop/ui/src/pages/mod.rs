mod aggregation;
mod backups;
mod connection_editor;
mod connections;
mod explorer;
mod files;
mod home;
mod logs;
mod migrations;
mod monitor;
mod query_bar;
mod query_studio;
mod reports;
mod settings;
mod sync_jobs;
mod transfer;

pub use backups::BackupsPage;
pub use connections::ConnectionsPage;
pub use explorer::ExplorerPage;
pub use files::FilesPage;
pub use home::HomePage;
pub use logs::LogsPage;
pub use migrations::MigrationsPage;
pub use monitor::MonitorPage;
pub use query_studio::QueryStudioPage;
pub use reports::ReportsPage;
pub use settings::SettingsPage;
pub use sync_jobs::SyncJobsPage;
pub use transfer::Direction as TransferDirection;

use leptos::prelude::*;

use crate::app::use_ui;

/// A request's lifecycle, for pages that load data from the backend.
#[derive(Debug, Clone, PartialEq)]
pub enum Load<T> {
    Loading,
    Ready(T),
    Failed(String),
}

/// Inline error box with a retry action.
#[component]
pub fn ErrorBox(message: String, retry: impl Fn() + 'static) -> impl IntoView {
    let ui = use_ui();
    view! {
        <div class="error-box" role="alert">
            <strong>{ui.tr("common.error")}</strong>
            <p class="error-detail">{message}</p>
            <button type="button" class="btn" on:click=move |_| retry()>{ui.tr("common.retry")}</button>
        </div>
    }
}
