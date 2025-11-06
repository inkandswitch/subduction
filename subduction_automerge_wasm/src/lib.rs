//! # Wasm Bindings for the Subduction/Automerge integration.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use wasm_bindgen::prelude::*;
use wasm_tracing::WasmLayerConfig;

pub use automerge;
pub use sedimentree_automerge_wasm;
pub use subduction_wasm;

/// Set a panic hook to get better error messages if the code panics.
#[wasm_bindgen]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();

    let mut config = WasmLayerConfig::new().with_max_level(tracing::Level::TRACE);
    config.use_console_methods = true;

    wasm_tracing::set_as_global_default_with_config(config).expect("unable to set global default");
}

/// Entry point called when the wasm module is instantiated.
#[wasm_bindgen(start)]
pub fn start() {
    set_panic_hook();

    tracing::info!(
        "🏔️ subduction_automerge_wasm v{} ({})",
        env!("CARGO_PKG_VERSION"),
        build_info::GIT_HASH
    );
}
