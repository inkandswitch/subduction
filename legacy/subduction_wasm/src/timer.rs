//! Direct `globalThis.setTimeout` / `clearTimeout` bindings.
//!
//! Works in browsers, web workers, service workers, Node.js, Deno, and Bun —
//! anywhere that has a global `setTimeout`.

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    /// `globalThis.setTimeout(closure, ms)` — schedule a callback after `ms` milliseconds.
    #[wasm_bindgen(js_name = setTimeout)]
    pub fn set_timeout(closure: &js_sys::Function, ms: i32) -> JsValue;

    /// `globalThis.clearTimeout(id)` — cancel a pending timeout.
    #[wasm_bindgen(js_name = clearTimeout)]
    pub fn clear_timeout(id: &JsValue);
}
