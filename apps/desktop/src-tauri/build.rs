fn main() {
    // `tauri::generate_context!` requires the frontend directory to exist at
    // compile time. `cargo tauri build` fills it via Trunk; plain `cargo
    // build`/`clippy` runs (CI quality gates) only need the directory.
    let dist = std::path::Path::new("../ui/dist");
    if !dist.join("index.html").exists() {
        if let Err(e) = std::fs::create_dir_all(dist) {
            println!("cargo:warning=could not create {}: {e}", dist.display());
        }
        println!(
            "cargo:warning=the UI bundle is not built; run `trunk build` in apps/desktop/ui or use `cargo tauri build`"
        );
    }
    tauri_build::build();
}
