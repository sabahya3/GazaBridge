//! Calls into the Tauri backend.

use gazabridge_ipc::CommandError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::prelude::*;

// Looks the bridge up on every call and rejects instead of throwing when it
// is missing, so a missing backend is an ordinary error, never an exception
// that unwinds through the reactive runtime.
#[wasm_bindgen(inline_js = r#"
// The button the user just pressed: a command it starts marks it busy
// (spinner, no second click) until the backend answers.
let lastPress = null;
document.addEventListener("click", (e) => {
    const b = e.target && e.target.closest ? e.target.closest("button") : null;
    lastPress = b ? { button: b, at: performance.now() } : null;
}, true);

function markBusy() {
    const p = lastPress;
    if (!p || performance.now() - p.at > 400 || !p.button.isConnected) {
        return () => {};
    }
    const b = p.button;
    const n = (Number(b.dataset.busy) || 0) + 1;
    b.dataset.busy = String(n);
    if (n === 1) {
        b.dataset.ariaBusyBefore = b.getAttribute("aria-busy") || "";
        b.classList.add("is-busy");
        b.setAttribute("aria-busy", "true");
    }
    return () => {
        const left = (Number(b.dataset.busy) || 1) - 1;
        if (left > 0) {
            b.dataset.busy = String(left);
            return;
        }
        delete b.dataset.busy;
        b.classList.remove("is-busy");
        const before = b.dataset.ariaBusyBefore;
        delete b.dataset.ariaBusyBefore;
        if (before) { b.setAttribute("aria-busy", before); } else { b.removeAttribute("aria-busy"); }
    };
}

export async function gazabridge_invoke(cmd, args) {
    const core = window.__TAURI__ && window.__TAURI__.core;
    if (!core || typeof core.invoke !== "function") {
        throw { code: "ipc", message: "backend unavailable" };
    }
    const done = markBusy();
    try {
        return await core.invoke(cmd, args);
    } finally {
        done();
    }
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

#[wasm_bindgen(inline_js = r#"
export async function gazabridge_listen(event, cb) {
    const ev = window.__TAURI__ && window.__TAURI__.event;
    if (!ev || typeof ev.listen !== "function") {
        throw { code: "ipc", message: "backend unavailable" };
    }
    return await ev.listen(event, (e) => cb(e.payload));
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = gazabridge_listen, catch)]
    async fn listen_js(event: &str, cb: &js_sys::Function) -> Result<JsValue, JsValue>;
}

/// A subscription to a backend event; dropping it stops listening.
pub struct Subscription {
    unlisten: Option<js_sys::Function>,
    _callback: Closure<dyn Fn(JsValue)>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription").finish_non_exhaustive()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(f) = self.unlisten.take() {
            let _ = f.call0(&JsValue::NULL);
        }
    }
}

/// Calls `on` with every payload of `event` the backend pushes (see
/// `gazabridge_ipc::events`) until the subscription is dropped.
pub async fn listen<T: DeserializeOwned + 'static>(
    event: &str,
    on: impl Fn(T) + 'static,
) -> Result<Subscription, CommandError> {
    let callback = Closure::<dyn Fn(JsValue)>::new(move |v: JsValue| {
        if let Ok(t) = serde_wasm_bindgen::from_value(v) {
            on(t);
        }
    });
    match listen_js(event, callback.as_ref().unchecked_ref()).await {
        Ok(f) => Ok(Subscription {
            unlisten: f.dyn_into().ok(),
            _callback: callback,
        }),
        Err(e) => Err(serde_wasm_bindgen::from_value::<CommandError>(e)
            .unwrap_or_else(|_| transport_error("backend unavailable"))),
    }
}
