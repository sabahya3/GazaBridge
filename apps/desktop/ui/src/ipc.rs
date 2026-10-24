//! Calls into the Tauri backend.

use gazabridge_ipc::CommandError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"], catch)]
    async fn invoke(cmd: &str, args: JsValue) -> Result<JsValue, JsValue>;
}

fn transport_error(message: impl Into<String>) -> CommandError {
    CommandError {
        code: "ipc".into(),
        message: message.into(),
    }
}

/// Invokes a backend command with named arguments.
pub async fn call<A: Serialize, R: DeserializeOwned>(
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
