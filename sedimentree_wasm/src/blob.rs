//! Wasm wrapper for Blob.

use js_sys::Uint8Array;
use sedimentree_core::blob::Blob;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A wrapper around `Blob` for use in JavaScript via wasm-bindgen.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Blob)]
pub struct WasmBlob(Blob);

#[wasm_refgen(js_ref = JsBlob)]
#[wasm_bindgen(js_class = Blob)]
impl WasmBlob {
    /// Create a new blob from raw bytes.
    #[wasm_bindgen(constructor)]
    pub fn new(data: &Uint8Array) -> Self {
        Self(Blob::new(data.to_vec()))
    }

    /// Get the raw bytes of the blob.
    #[wasm_bindgen(getter)]
    pub fn data(&self) -> Uint8Array {
        Uint8Array::from(self.0.contents().as_slice())
    }
}

impl From<Blob> for WasmBlob {
    fn from(blob: Blob) -> Self {
        Self(blob)
    }
}

impl From<WasmBlob> for Blob {
    fn from(wasm: WasmBlob) -> Self {
        wasm.0
    }
}

impl AsRef<Blob> for WasmBlob {
    fn as_ref(&self) -> &Blob {
        &self.0
    }
}
