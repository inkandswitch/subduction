use std::str::FromStr;

use sedimentree_core::DocumentId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "DocumentId")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsDocumentId(DocumentId);

#[wasm_bindgen(js_class = "DocumentId")]
impl JsDocumentId {
    #[wasm_bindgen(constructor)]
    pub fn new(id: &str) -> Self {
        JsDocumentId(FromStr::from_str(id).expect("FIXME"))
    }

    #[wasm_bindgen(getter, js_name = "id")]
    pub fn id(&self) -> String {
        self.0.to_string()
    }
}

impl From<DocumentId> for JsDocumentId {
    fn from(document_id: DocumentId) -> Self {
        JsDocumentId(document_id)
    }
}

impl From<JsDocumentId> for DocumentId {
    fn from(js_document_id: JsDocumentId) -> Self {
        js_document_id.0
    }
}
