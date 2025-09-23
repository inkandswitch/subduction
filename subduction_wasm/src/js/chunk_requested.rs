use subduction_core::subduction::request::ChunkRequested;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = ChunkRequested)]
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
