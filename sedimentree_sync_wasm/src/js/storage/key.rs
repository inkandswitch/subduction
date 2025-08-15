use sedimentree_sync_core::storage::key::StorageKey;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "StorageKey")]
pub struct JsStorageKey(StorageKey);

#[wasm_bindgen(js_class = "StorageKey")]
impl JsStorageKey {
    #[wasm_bindgen(constructor)]
    pub fn new(parts: Vec<String>) -> Self {
        JsStorageKey(StorageKey::new(parts))
    }

    #[wasm_bindgen(getter, js_name = "segments")]
    pub fn segments(&self) -> Vec<String> {
        self.0.to_vec()
    }
}

impl From<StorageKey> for JsStorageKey {
    fn from(key: StorageKey) -> Self {
        JsStorageKey(key)
    }
}
