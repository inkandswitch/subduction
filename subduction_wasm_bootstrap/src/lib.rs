//! Shared Wasm cdylib bootstrap: install [`console_error_panic_hook`]
//! and a baseline `tracing` subscriber.
//!
//! Every Wasm cdylib in this workspace calls [`init_basic`] from its
//! `#[wasm_bindgen(start)]`. All functions are idempotent and safe to
//! call from chained start functions; the first to do real work wins.

#![forbid(unsafe_code)]

/// Install [`console_error_panic_hook`] so Rust panics produce readable
/// `console.error` output. Idempotent.
pub fn install_panic_hook() {
    console_error_panic_hook::set_once();
}

/// Install a baseline `tracing` subscriber routing events to the
/// browser console at WARN level. No-op on non-wasm32. No-op if a
/// subscriber has already been set.
#[cfg(target_arch = "wasm32")]
pub fn install_basic_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    use wasm_tracing::{WasmLayer, WasmLayerConfig};

    if tracing::dispatcher::has_been_set() {
        return;
    }

    let mut config = WasmLayerConfig::new().with_max_level(tracing::Level::WARN);
    config.use_console_methods = true;
    let wasm_layer = WasmLayer::new(config);

    let _result = tracing_subscriber::registry().with(wasm_layer).try_init();
}

/// Stub for non-wasm32 targets so callers can reference this
/// unconditionally.
#[cfg(not(target_arch = "wasm32"))]
pub const fn install_basic_tracing() {}

/// Install the panic hook and the baseline tracing subscriber.
pub fn init_basic() {
    install_panic_hook();
    install_basic_tracing();
}
