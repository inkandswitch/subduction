use sedimentree_core::Digest;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = Digest)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsDigest(Digest);

#[wasm_bindgen(js_class = Digest)]
impl JsDigest {
    pub fn from_bytes(bytes: &[u8]) -> Result<JsDigest, JsValue> {
        let digest = Digest::from_bytes(bytes).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(JsDigest(digest))
    }

    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}

impl From<Digest> for JsDigest {
    fn from(digest: Digest) -> Self {
        Self(digest)
    }
}

impl From<JsDigest> for Digest {
    fn from(digest: JsDigest) -> Self {
        digest.0
    }
}
