//! Sedimentree [`Chunk`](sedimentree_core::Chunk).

use sedimentree_core::Chunk;
use subduction_core::subduction::request::ChunkRequested;
use wasm_bindgen::prelude::*;

/// A data chunk used in the Sedimentree system.
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

/// A request for a specific chunk in the Sedimentree system.
#[wasm_bindgen(js_name = ChunkRequested)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct JsChunkRequested(ChunkRequested);

impl From<ChunkRequested> for JsChunkRequested {
    fn from(req: ChunkRequested) -> Self {
        Self(req)
    }
}

impl From<JsChunkRequested> for ChunkRequested {
    fn from(req: JsChunkRequested) -> Self {
        req.0
    }
}

#[wasm_bindgen(inline_js = r#"
export function tryIntoChunkArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `JsChunk`.
    #[wasm_bindgen(js_name = tryIntoChunkArray, catch)]
    pub fn try_into_js_chunk_array(v: &JsValue) -> Result<Vec<JsChunk>, JsValue>;
}

pub(crate) struct JsChunksArray(pub(crate) Vec<JsChunk>);

impl TryFrom<&JsValue> for JsChunksArray {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(JsChunksArray(try_into_js_chunk_array(js_value)?))
    }
}
