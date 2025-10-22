//! Hash digests.

use sedimentree_core::{blob::error::InvalidDigest, Digest};
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A wrapper around [`sedimentree_core::Digest`] for use in JavaScript via wasm-bindgen.
#[wasm_bindgen(js_name = Digest)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct JsDigest(Digest);

#[wasm_bindgen(js_class = Digest)]
impl JsDigest {
    /// Creates a new digest from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` error if the byte slice is not a valid digest.
    pub fn from_bytes(bytes: &[u8]) -> Result<JsDigest, JsValue> {
        let digest = Digest::from_bytes(bytes).map_err(JsInvalidDigest::from)?;
        Ok(JsDigest(digest))
    }

    /// Returns the byte representation of the digest.
    #[must_use]
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

/// An error indicating an invalid [`Digest`].
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct JsInvalidDigest(#[from] InvalidDigest);

impl From<JsInvalidDigest> for JsValue {
    fn from(err: JsInvalidDigest) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidDigest");
        err.into()
    }
}

#[wasm_bindgen(inline_js = r#"
export function tryIntoDigest(v) { return v; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into a `JsDigest`.
    #[wasm_bindgen(js_name = tryIntoDigest, catch)]
    pub fn try_into_js_digest(v: &JsValue) -> Result<JsDigest, JsValue>;
}

impl TryFrom<&JsValue> for JsDigest {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        try_into_js_digest(js_value)
    }
}
