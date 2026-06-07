//! Shared Wasm cdylib bootstrap: install [`console_error_panic_hook`] and a
//! `tracing` subscriber stack shared by every Wasm cdylib in this workspace.
//!
//! Two installation paths are provided:
//!
//! - [`init_basic`] — panic hook + a fixed-WARN console subscriber. Minimal;
//!   retained for compatibility.
//! - [`init_rich`] / [`init_rich_from_env`] — panic hook + a **reloadable**
//!   level filter, the browser-console [`wasm_tracing::WasmLayer`], and a
//!   JS-callback layer ([`set_subduction_logger`]). This is the recommended
//!   path so every bundle exposes the same runtime-controllable logging
//!   (`setSubductionLogLevel`, `set_subduction_logger`).
//!
//! All functions are idempotent and safe to call from chained
//! `#[wasm_bindgen(start)]` functions; the first to install a subscriber wins.
//!
//! See `design/logging.md` for the level/field conventions these layers serve.

#![forbid(unsafe_code)]

extern crate alloc;

#[cfg(target_arch = "wasm32")]
pub mod js_logger;

// Only the basic console stack (`install_basic_tracing`) uses these at the
// crate root, and it is itself gated on `wasm-tracing`. The rich stack has its
// own imports in `rich.rs`.
#[cfg(all(target_arch = "wasm32", feature = "wasm-tracing"))]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(all(target_arch = "wasm32", feature = "wasm-tracing"))]
use wasm_tracing::{WasmLayer, WasmLayerConfig};

/// The key used for reading/writing the log level in `localStorage` (browser)
/// or `process.env` (Node.js).
pub const LOG_LEVEL_KEY: &str = "SUBDUCTION_LOG_LEVEL";

/// Install [`console_error_panic_hook`] so Rust panics produce readable
/// `console.error` output. Idempotent.
pub fn install_panic_hook() {
    console_error_panic_hook::set_once();
}

// ---------------------------------------------------------------------------
// Basic stack (panic hook + fixed-WARN console subscriber)
// ---------------------------------------------------------------------------

/// Install a baseline `tracing` subscriber routing events to the browser
/// console at WARN. No-op on non-wasm32. No-op if a subscriber is already set.
#[cfg(all(target_arch = "wasm32", feature = "wasm-tracing"))]
pub fn install_basic_tracing() {
    if tracing::dispatcher::has_been_set() {
        return;
    }

    let mut config = WasmLayerConfig::new().with_max_level(tracing::Level::WARN);
    config.use_console_methods = true;
    let wasm_layer = WasmLayer::new(config);

    let _result = tracing_subscriber::registry().with(wasm_layer).try_init();
}

/// Stub when the console layer is compiled out.
#[cfg(all(target_arch = "wasm32", not(feature = "wasm-tracing")))]
pub fn install_basic_tracing() {}

/// Stub for non-wasm32 targets so callers can reference this unconditionally.
#[cfg(not(target_arch = "wasm32"))]
pub const fn install_basic_tracing() {}

/// Install the panic hook and the baseline tracing subscriber.
pub fn init_basic() {
    install_panic_hook();
    install_basic_tracing();
}

// ---------------------------------------------------------------------------
// Rich stack (reloadable filter + console layer + JS callback layer)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
pub mod rich;

/// Install the panic hook and the rich tracing stack (reloadable level filter +
/// browser-console layer + JS-callback layer) at `initial_level`.
///
/// Idempotent: if a subscriber is already installed this only refreshes the
/// panic hook. No-op subscriber install on non-wasm32.
#[cfg(target_arch = "wasm32")]
pub fn init_rich(initial_level: tracing_subscriber::filter::LevelFilter) {
    install_panic_hook();
    if !tracing::dispatcher::has_been_set() {
        rich::init(initial_level);
    }
}

/// Non-wasm32 stub.
#[cfg(not(target_arch = "wasm32"))]
pub fn init_rich(_initial_level: tracing_subscriber::filter::LevelFilter) {
    install_panic_hook();
}

/// Install the rich stack, reading the initial level from the environment
/// (`localStorage` then `process.env`), defaulting to WARN.
pub fn init_rich_from_env() {
    let level = read_log_level_from_env()
        .as_deref()
        .and_then(parse_level_filter)
        .unwrap_or(tracing_subscriber::filter::LevelFilter::WARN);

    init_rich(level);
}

/// Set the log level at runtime. The new level takes effect immediately and is
/// persisted to `localStorage` (browser) so it survives reloads.
///
/// Valid levels: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`, `"off"`.
///
/// # Errors
///
/// Returns an error string if the level is invalid or tracing is not initialized.
#[cfg(target_arch = "wasm32")]
pub fn set_log_level(level: &str) -> Result<(), alloc::string::String> {
    let level_filter = parse_level_filter(level).ok_or_else(|| {
        alloc::string::String::from(
            "invalid log level: expected one of trace, debug, info, warn, error, off",
        )
    })?;

    rich::set_level(level_filter).map_err(|e| alloc::format!("failed to set log level: {e}"))?;
    write_to_local_storage(level_filter_name(level_filter));
    Ok(())
}

/// Non-wasm32 stub: validates the level string but installs nothing.
///
/// # Errors
///
/// Returns an error string if `level` is not a recognized level name.
#[cfg(not(target_arch = "wasm32"))]
pub fn set_log_level(level: &str) -> Result<(), alloc::string::String> {
    parse_level_filter(level)
        .map(|_| ())
        .ok_or_else(|| alloc::string::String::from("invalid log level"))
}

/// Set a JavaScript callback to receive every tracing event. See
/// [`js_logger::set_logger`] for the callback contract.
#[cfg(target_arch = "wasm32")]
pub fn set_subduction_logger(callback: js_sys::Function) {
    js_logger::set_logger(callback);
}

/// Clear the JavaScript logger callback.
#[cfg(target_arch = "wasm32")]
pub fn clear_subduction_logger() {
    js_logger::clear_logger();
}

// ---------------------------------------------------------------------------
// Level-string parsing
// ---------------------------------------------------------------------------

/// Parse a level string into a `LevelFilter`. Case-insensitive.
#[must_use]
pub fn parse_level_filter(s: &str) -> Option<tracing_subscriber::filter::LevelFilter> {
    use tracing_subscriber::filter::LevelFilter;
    match s.to_ascii_lowercase().as_str() {
        "trace" => Some(LevelFilter::TRACE),
        "debug" => Some(LevelFilter::DEBUG),
        "info" => Some(LevelFilter::INFO),
        "warn" => Some(LevelFilter::WARN),
        "error" => Some(LevelFilter::ERROR),
        "off" => Some(LevelFilter::OFF),
        _ => None,
    }
}

/// The canonical lowercase name for a `LevelFilter`.
#[must_use]
pub fn level_filter_name(level: tracing_subscriber::filter::LevelFilter) -> &'static str {
    use tracing_subscriber::filter::LevelFilter;
    if level == LevelFilter::TRACE {
        "trace"
    } else if level == LevelFilter::DEBUG {
        "debug"
    } else if level == LevelFilter::INFO {
        "info"
    } else if level == LevelFilter::WARN {
        "warn"
    } else if level == LevelFilter::ERROR {
        "error"
    } else {
        "off"
    }
}

// ---------------------------------------------------------------------------
// Environment access (localStorage / process.env)
// ---------------------------------------------------------------------------

/// Read `SUBDUCTION_LOG_LEVEL` from the environment, checking (in order):
/// 1. `globalThis.localStorage.getItem(...)` (browser)
/// 2. `globalThis.process.env.SUBDUCTION_LOG_LEVEL` (Node.js)
#[cfg(target_arch = "wasm32")]
#[must_use]
pub fn read_log_level_from_env() -> Option<alloc::string::String> {
    read_from_local_storage().or_else(read_from_process_env)
}

/// Non-wasm32 stub (no ambient JS environment).
#[cfg(not(target_arch = "wasm32"))]
#[must_use]
pub const fn read_log_level_from_env() -> Option<alloc::string::String> {
    None
}

#[cfg(target_arch = "wasm32")]
fn read_from_local_storage() -> Option<alloc::string::String> {
    use wasm_bindgen::{JsCast, JsValue};

    let global = js_sys::global();
    let storage = js_sys::Reflect::get(&global, &JsValue::from_str("localStorage")).ok()?;
    if storage.is_undefined() || storage.is_null() {
        return None;
    }

    let get_item = js_sys::Reflect::get(&storage, &JsValue::from_str("getItem")).ok()?;
    let get_item = get_item.dyn_ref::<js_sys::Function>()?;
    let result = get_item
        .call1(&storage, &JsValue::from_str(LOG_LEVEL_KEY))
        .ok()?;
    result.as_string()
}

#[cfg(target_arch = "wasm32")]
fn write_to_local_storage(level: &str) {
    use wasm_bindgen::{JsCast, JsValue};

    let Ok(storage) = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("localStorage"))
    else {
        return;
    };
    if storage.is_undefined() || storage.is_null() {
        return;
    }

    let Ok(set_item) = js_sys::Reflect::get(&storage, &JsValue::from_str("setItem")) else {
        return;
    };
    let Some(set_item) = set_item.dyn_ref::<js_sys::Function>() else {
        return;
    };

    drop(set_item.call2(
        &storage,
        &JsValue::from_str(LOG_LEVEL_KEY),
        &JsValue::from_str(level),
    ));
}

#[cfg(target_arch = "wasm32")]
fn read_from_process_env() -> Option<alloc::string::String> {
    use wasm_bindgen::JsValue;

    let global = js_sys::global();
    let process = js_sys::Reflect::get(&global, &JsValue::from_str("process")).ok()?;
    if process.is_undefined() || process.is_null() {
        return None;
    }

    let env = js_sys::Reflect::get(&process, &JsValue::from_str("env")).ok()?;
    if env.is_undefined() || env.is_null() {
        return None;
    }

    let val = js_sys::Reflect::get(&env, &JsValue::from_str(LOG_LEVEL_KEY)).ok()?;
    val.as_string()
}
