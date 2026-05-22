//! Shared Wasm cdylib bootstrap: install the [`console_error_panic_hook`]
//! and a baseline `tracing` subscriber.
//!
//! # Why this crate exists
//!
//! The four publishable Wasm cdylibs in this workspace
//! ([`subduction_wasm`], [`sedimentree_wasm`],
//! [`automerge_sedimentree_wasm`], [`automerge_subduction_wasm`]) all
//! need the same baseline at module instantiation:
//!
//! 1. Install the panic hook so Rust panics produce readable
//!    `console.error` output instead of opaque `RuntimeError:
//!    unreachable` traps.
//! 2. Install a baseline `tracing` subscriber so `tracing::warn!` and
//!    `tracing::error!` from any of the workspace's pure-Rust crates
//!    (`subduction_core`, `sedimentree_core`, etc.) reach the browser
//!    console — not silently dropped into a missing-dispatcher void.
//!
//! Before this crate, only the `automerge_subduction_wasm` umbrella
//! installed a subscriber. The three smaller cdylibs (which ALSO
//! ship standalone via npm: `@inkandswitch/sedimentree`,
//! `@inkandswitch/subduction`, `@automerge/sedimentree`) emitted
//! `tracing` events that vanished. This crate closes that gap.
//!
//! # Why this crate is NOT `no_std`
//!
//! Unlike the workspace's foundational data-structure crates
//! (`sedimentree_core`, `subduction_core`, etc.) which use
//! `#![cfg_attr(not(feature = "std"), no_std)]`, this crate is
//! unconditionally `std`. Every dep it has is `std`-only:
//!
//! - `console_error_panic_hook` calls `std::panic::set_hook`.
//!   Installing a panic hook is fundamentally an `std` operation.
//! - `tracing-subscriber`'s `registry` feature pulls in `std`
//!   transitively (see its `[features]`: `registry = [...,  "std"]`).
//! - `wasm-tracing` builds its `WasmLayer` on top of
//!   `tracing_subscriber::registry::*`, also needing `std`.
//!
//! # How callers use it
//!
//! Each cdylib's `#[wasm_bindgen(start)]` calls [`init_basic`]:
//!
//! ```text
//! #[wasm_bindgen::prelude::wasm_bindgen(start, private)]
//! pub fn start_my_crate() {
//!     subduction_wasm_bootstrap::init_basic();
//! }
//! ```
//!
//! For the `automerge_subduction_wasm` umbrella, which has a richer
//! subscriber setup (env-var driven log level, reloadable filter,
//! `JsCallbackLayer`, startup banner), the start function calls
//! [`install_panic_hook`] from this crate directly and runs its own
//! tracing init:
//!
//! ```text
//! #[wasm_bindgen::prelude::wasm_bindgen(start, private)]
//! pub fn start_umbrella() {
//!     subduction_wasm_bootstrap::install_panic_hook();
//!     init_umbrella_tracing();  // not in this crate
//! }
//! ```
//!
//! # Idempotency and chained-start safety
//!
//! `wasm-bindgen` 0.2.118+ chains every `#[wasm_bindgen(start)]`
//! function from every rlib linked into a cdylib via
//! `__wbindgen_start`. When `@automerge/subduction` loads it
//! transitively pulls all four cdylib rlibs, so all four start
//! functions execute (outer-most first). Both [`install_panic_hook`]
//! and [`install_basic_tracing`] are idempotent:
//!
//! - `console_error_panic_hook::set_once` early-returns on second
//!   invocation.
//! - `install_basic_tracing` checks `tracing::dispatcher::has_been_set`
//!   before attempting `try_init`, so the umbrella's richer subscriber
//!   (installed by its start, which runs FIRST) wins.

#![forbid(unsafe_code)]

/// Install the [`console_error_panic_hook`] so Rust panics produce
/// readable messages in the browser's `console.error` instead of
/// opaque `RuntimeError: unreachable` traps.
///
/// Internally uses `console_error_panic_hook::set_once`. Safe to call
/// any number of times from any number of chained start functions —
/// subsequent calls are no-ops.
pub fn install_panic_hook() {
    console_error_panic_hook::set_once();
}

/// Install a baseline `tracing` subscriber routing events to
/// `console.log`/`console.warn`/`console.error` at WARN level.
///
/// No-op on non-wasm32 targets.
///
/// Idempotent and chain-safe: early-returns if any subscriber has
/// already been set (e.g. by the umbrella crate's richer setup).
/// First start function in the `__wbindgen_start` chain wins.
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

    // `try_init` cannot fail in practice because we just checked
    // `has_been_set`, but be defensive: a race in the (currently
    // single-threaded) Wasm runtime is implausible but cheap to
    // guard against.
    let _result = tracing_subscriber::registry().with(wasm_layer).try_init();
}

/// Stub for non-wasm32 targets so callers can always reference the
/// function unconditionally.
#[cfg(not(target_arch = "wasm32"))]
pub const fn install_basic_tracing() {}

/// Install both the panic hook and a baseline tracing subscriber.
///
/// Convenience wrapper around [`install_panic_hook`] + [`install_basic_tracing`].
/// Use this from the `#[wasm_bindgen(start)]` of any cdylib that
/// doesn't have its own richer tracing setup.
pub fn init_basic() {
    install_panic_hook();
    install_basic_tracing();
}
