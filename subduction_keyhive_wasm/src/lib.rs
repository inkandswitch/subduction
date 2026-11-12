//! Subduction's keyhive sync protocol for a keyhive that lives in JavaScript.
//!
//! The keyhive stays in its own Wasm module (for example, `@keyhive/keyhive`).
//! [`KeyhiveSync`] reaches it only through a [`JsKeyhiveAccess`], whose
//! arguments and results are bytes, so no keyhive type crosses between the two
//! modules. `keyhive_core` and `subduction_wasm` are not compiled into this
//! crate.

#![no_std]

extern crate alloc;

pub mod access;
pub mod sync;

pub use access::{JsKeyhiveAccess, KeyhiveAccessError, WasmKeyhive};
pub use sync::{JsFrameOutcome, KeyhiveSync};
