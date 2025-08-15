use sedimentree_sync_core::chunk::Chunk;
use wasm_bindgen::prelude::*;

use super::storage::key::JsStorageKey;

#[wasm_bindgen(js_name = "Chunk")]
pub struct JsChunk(Chunk);

#[wasm_bindgen(js_class = "Chunk")]
impl JsChunk {
    #[wasm_bindgen(getter, js_name = "key")]
    pub fn key(&self) -> JsStorageKey {
        self.0.key.clone().into()
    }

    #[wasm_bindgen(getter, js_name = "data")]
    pub fn data(&self) -> Vec<u8> {
        self.0.data.clone()
    }
}

impl From<Chunk> for JsChunk {
    fn from(chunk: Chunk) -> Self {
        JsChunk(chunk)
    }
}

impl From<JsChunk> for Chunk {
    fn from(js_chunk: JsChunk) -> Self {
        js_chunk.0
    }
}
