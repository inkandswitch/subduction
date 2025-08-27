use sedimentree_sync_core::payload::Payload;
use wasm_bindgen::prelude::*;

use super::storage::key::JsStorageKey;

#[wasm_bindgen(js_name = "Payload")]
pub struct JsPayload(Payload);

#[wasm_bindgen(js_class = "Payload")]
impl JsPayload {
    #[wasm_bindgen(getter, js_name = "key")]
    pub fn key(&self) -> JsStorageKey {
        self.0.key.clone().into()
    }

    #[wasm_bindgen(getter, js_name = "data")]
    pub fn data(&self) -> Vec<u8> {
        self.0.data.clone()
    }
}

impl From<Payload> for JsPayload {
    fn from(payload: Payload) -> Self {
        JsPayload(payload)
    }
}

impl From<JsPayload> for Payload {
    fn from(js_payload: JsPayload) -> Self {
        js_payload.0
    }
}
