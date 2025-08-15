use sedimentree_sync_core::storage::id::StorageId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "StorageId")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsStorageId(StorageId);

#[wasm_bindgen(js_class = "StorageId")]
impl JsStorageId {
    #[wasm_bindgen(constructor)]
    pub fn new(id: &str) -> Self {
        JsStorageId(StorageId::new(id.to_string()))
    }

    #[wasm_bindgen(getter, js_name = "id")]
    pub fn id(&self) -> String {
        self.0.to_string()
    }
}

impl From<StorageId> for JsStorageId {
    fn from(id: StorageId) -> Self {
        JsStorageId(id)
    }
}

impl From<JsStorageId> for StorageId {
    fn from(js_id: JsStorageId) -> Self {
        js_id.0
    }
}
