//! Rich tracing stack: a reloadable level filter, the browser-console
//! [`wasm_tracing::WasmLayer`] (when the `wasm-tracing` feature is enabled),
//! and the [`crate::js_logger::JsCallbackLayer`].
//!
//! Installed once at module startup by [`crate::init_rich`]; the reloadable
//! handle backs [`crate::set_log_level`] so JS can change the level at runtime.

use std::sync::OnceLock;

use tracing_subscriber::{
    Registry, filter::LevelFilter, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

use crate::js_logger::JsCallbackLayer;

/// Global handle for dynamically reloading the log-level filter.
static RELOAD_HANDLE: OnceLock<reload::Handle<LevelFilter, Registry>> = OnceLock::new();

pub(crate) fn init(initial_level: LevelFilter) {
    let (filter, reload_handle) = reload::Layer::new(initial_level);

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(JsCallbackLayer);

    #[cfg(feature = "wasm-tracing")]
    {
        // WasmLayer accepts everything; the reloadable LevelFilter (added
        // first, above) controls what passes through.
        let mut config = wasm_tracing::WasmLayerConfig::new().with_max_level(tracing::Level::TRACE);
        config.use_console_methods = true;
        registry.with(wasm_tracing::WasmLayer::new(config)).init();
    }

    #[cfg(not(feature = "wasm-tracing"))]
    registry.init();

    RELOAD_HANDLE.set(reload_handle).ok();
}

/// Update the live reloadable filter, if the rich stack installed it.
///
/// Returns `true` if the level was applied to the running subscriber, or
/// `false` if no reloadable filter is installed (e.g. a different subscriber —
/// such as the basic stack — won the install race). In the `false` case the
/// caller should still persist the choice so it takes effect on next startup.
pub(crate) fn set_level(level: LevelFilter) -> bool {
    match RELOAD_HANDLE.get() {
        Some(handle) => handle.modify(|filter| *filter = level).is_ok(),
        None => false,
    }
}
