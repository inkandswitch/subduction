use sedimentree_core::Chunk;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = Chunk)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsChunk(Chunk);

impl From<Chunk> for JsChunk {
    fn from(chunk: Chunk) -> Self {
        Self(chunk)
    }
}

impl From<JsChunk> for Chunk {
    fn from(chunk: JsChunk) -> Self {
        chunk.0
    }
}
