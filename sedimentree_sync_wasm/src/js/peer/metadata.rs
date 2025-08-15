use wasm_bindgen::prelude::*;

use crate::js::storage::id::JsStorageId;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "PeerMetadata")]
    #[derive(Debug, Clone)]
    pub type JsPeerMetadata;

    #[wasm_bindgen(method, getter, js_name = storageId)]
    pub fn storage_id(this: &JsPeerMetadata) -> Option<JsStorageId>;

    #[wasm_bindgen(method, getter, js_name = isEphemeral)]
    pub fn is_ephemeral(this: &JsPeerMetadata) -> Option<bool>;
}
