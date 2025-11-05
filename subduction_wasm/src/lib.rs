//! # Wasm Bindings for the Subduction sync protocol.

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

pub mod connection_id;
pub mod depth;
pub mod digest;
pub mod error;
pub mod fragment;
pub mod loose_commit;
pub mod peer_id;
pub mod sedimentree;
pub mod sedimentree_id;
pub mod storage;
pub mod subduction;
pub mod websocket;

pub(crate) mod connection_callback_reader;

pub use subduction::WasmSubduction;

use wasm_bindgen::prelude::*;
use wasm_tracing::WasmLayerConfig;

/// Set a panic hook to get better error messages if the code panics.
#[wasm_bindgen]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();

    let config = WasmLayerConfig::new()
        .with_max_level(tracing::Level::TRACE)
        .with_colorless_logs();

    wasm_tracing::set_as_global_default_with_config(config).expect("unable to set global default");
}

/// Entry point called when the wasm module is instantiated.
#[wasm_bindgen(start)]
pub fn start() {
    set_panic_hook();
    tracing::info!(
        "🏔️ subduction_wasm v{} ({})",
        env!("CARGO_PKG_VERSION"),
        build_info::GIT_HASH
    );
}
