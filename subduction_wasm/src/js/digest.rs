//! Hash digests.

use sedimentree_core::Digest;
use wasm_bindgen::prelude::*;

// FIXME remove Copy from all bindgen classes that aren't scalar newtypes under the hood

/// A wrapper around [`sedimentree_core::Digest`] for use in JavaScript via wasm-bindgen.
#[wasm_bindgen(js_name = Digest)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsDigest(Digest);

#[wasm_bindgen(js_class = Digest)]
impl JsDigest {
    /// Creates a new digest from its byte representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<JsDigest, JsValue> {
        let digest = Digest::from_bytes(bytes).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(JsDigest(digest))
    }

    /// Returns the byte representation of the digest.
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

// FIXME
// #[wasm_bindgen(typescript_custom_section)]
// const _: &str = r#"
// export class Digest {}
// "#;

#[wasm_bindgen(inline_js = r#"
export function tryIntoDigest(v) { return v; }
"#)]

extern "C" {
    #[wasm_bindgen(js_name = tryIntoDigest, catch)]
    pub fn try_into_js_digest(v: &JsValue) -> Result<JsDigest, JsValue>;
}

impl TryFrom<&JsValue> for JsDigest {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        try_into_js_digest(js_value)
    }
}
