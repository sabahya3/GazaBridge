//! Calls into the Tauri backend.

use gazabridge_ipc::CommandError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::prelude::*;

// Looks the bridge up on every call and rejects instead of throwing when it
// is missing, so a missing backend is an ordinary error, never an exception
// that unwinds through the reactive runtime.
#[wasm_bindgen(inline_js = r#"
export async function gazabridge_invoke(cmd, args) {
    const core = window.__TAURI__ && window.__TAURI__.core;
    if (!core || typeof core.invoke !== "function") {
        throw { code: "ipc", message: "backend unavailable" };
    }
    return await core.invoke(cmd, args);
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = gazabridge_invoke, catch)]
    async fn invoke(cmd: &str, args: JsValue) -> Result<JsValue, JsValue>;
}

fn transport_error(message: impl Into<String>) -> CommandError {
    CommandError {
        connection_id: None,
        code: "ipc".into(),
        message: message.into(),
    }
}

/// Shows the password dialog for a connection; the sender says whether the
/// password was entered (then the call is retried) or the user declined.
pub type PasswordPrompt = Box<dyn Fn(String, String, futures_channel::oneshot::Sender<bool>)>;

thread_local! {
    static PROMPT: std::cell::RefCell<Option<PasswordPrompt>> = const { std::cell::RefCell::new(None) };
}

/// Registers the app's password dialog (once, at startup).
pub fn set_password_prompt(prompt: PasswordPrompt) {
    PROMPT.with(|p| *p.borrow_mut() = Some(prompt));
}

/// Invokes a backend command with named arguments. When the command needs
/// a connection's password, the password dialog opens and the command is
/// retried once the password is entered.
pub async fn call<A: Serialize, R: DeserializeOwned>(
    cmd: &str,
    args: &A,
) -> Result<R, CommandError> {
    match call_once(cmd, args).await {
        Err(e) if e.code == "password_required" && e.connection_id.is_some() => {
            let (tx, rx) = futures_channel::oneshot::channel();
            let shown = PROMPT.with(|p| match p.borrow().as_ref() {
                Some(prompt) => {
                    prompt(
                        e.connection_id.clone().unwrap_or_default(),
                        e.message.clone(),
                        tx,
                    );
                    true
                }
                None => false,
            });
            if shown && rx.await.unwrap_or(false) {
                call_once(cmd, args).await
            } else {
                Err(e)
            }
        }
        other => other,
    }
}

async fn call_once<A: Serialize, R: DeserializeOwned>(
    cmd: &str,
    args: &A,
) -> Result<R, CommandError> {
    let args = serde_wasm_bindgen::to_value(args).map_err(|e| transport_error(e.to_string()))?;
    match invoke(cmd, args).await {
        Ok(value) => {
            serde_wasm_bindgen::from_value(value).map_err(|e| transport_error(e.to_string()))
        }
        Err(err) => Err(serde_wasm_bindgen::from_value::<CommandError>(err.clone())
            .unwrap_or_else(|_| {
                transport_error(
                    err.as_string()
                        .unwrap_or_else(|| "backend unavailable".into()),
                )
            })),
    }
}

/// Arguments for commands that take none.
#[derive(Serialize)]
pub struct NoArgs {}
