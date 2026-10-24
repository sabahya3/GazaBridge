mod app;
mod i18n;
mod ipc;
mod nav;
mod pages;

fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(app::App);
}
