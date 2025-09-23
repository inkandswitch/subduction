use sedimentree_core::Blob;
use wasm_bindgen::prelude::*;

/// A wrapper around [`Blob`] to be used in JavaScript.
#[wasm_bindgen(js_name = Blob)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsBlob(Blob);

#[wasm_bindgen(js_class = Blob)]
impl JsBlob {
    /// Creates a new [`Blob`] from a byte array.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: Vec<u8>) -> Result<JsBlob, JsValue> {
        Ok(Blob::new(bytes).into())
    }

    /// Returns the byte representation of the [`Blob`].
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_slice().to_vec()
    }
}

impl From<Blob> for JsBlob {
    fn from(blob: Blob) -> Self {
        Self(blob)
    }
}

impl From<JsBlob> for Blob {
    fn from(blob: JsBlob) -> Self {
        blob.0
    }
}
