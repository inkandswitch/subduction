//! # Wasm Bindings for the Subduction/Automerge integration.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "wasm-tracing")]
use wasm_tracing::WasmLayerConfig;

use wasm_bindgen::prelude::*;

pub use automerge;
pub use automerge_sedimentree_wasm;
pub use subduction_wasm;

/// Set a panic hook to get better error messages if the code panics.
#[wasm_bindgen]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();

    #[cfg(feature = "wasm-tracing")]
    {
        let mut config = WasmLayerConfig::new().with_max_level(tracing::Level::WARN);
        config.use_console_methods = true;

        wasm_tracing::set_as_global_default_with_config(config)
            .expect("unable to set global default");
    }
}

/// Entry point called when the wasm module is instantiated.
#[wasm_bindgen(start)]
pub fn start() {
    set_panic_hook();

    tracing::info!(
        "🏔️ automerge_subduction_wasm v{} ({})",
        env!("CARGO_PKG_VERSION"),
        build_info::GIT_HASH
    );
}
