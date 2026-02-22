//! Sedimentree [`Fragment`](sedimentree_core::Fragment).

use alloc::{string::ToString, vec::Vec};
use sedimentree_core::fragment::Fragment;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use crate::{
    digest::{JsDigest, WasmDigest},
    loose_commit::WasmBlobMeta,
    sedimentree_id::WasmSedimentreeId,
};

/// A data fragment used in the Sedimentree system.
#[derive(Debug, Clone, PartialEq, Eq)]
#[wasm_bindgen(js_name = Fragment)]
pub struct WasmFragment(Fragment);

#[wasm_refgen(js_ref = JsFragment)]
#[wasm_bindgen(js_class = Fragment)]
impl WasmFragment {
    /// Create a new fragment from the given sedimentree ID, head, boundary, checkpoints, and blob metadata.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub fn new(
        sedimentree_id: WasmSedimentreeId,
        head: WasmDigest,
        boundary: Vec<JsDigest>,
        checkpoints: Vec<JsDigest>,
        blob_meta: WasmBlobMeta,
    ) -> Self {
        let cps: Vec<_> = checkpoints
            .iter()
            .map(|d| WasmDigest::from(d).into())
            .collect();
        Fragment::new(
            sedimentree_id.into(),
            head.into(),
            boundary
                .iter()
                .map(|d| WasmDigest::from(d).into())
                .collect(),
            &cps,
            blob_meta.into(),
        )
        .into()
    }

    /// Get the head digest of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmDigest {
        self.0.head().into()
    }

    /// Get the boundary digests of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn boundary(&self) -> Vec<WasmDigest> {
        self.0.boundary().iter().copied().map(Into::into).collect()
    }

    /// Get the blob metadata of the fragment.
    #[must_use]
    #[wasm_bindgen(getter, js_name = blobMeta)]
    pub fn blob_meta(&self) -> WasmBlobMeta {
        self.0.summary().blob_meta().into()
    }
}

impl From<Fragment> for WasmFragment {
    fn from(fragment: Fragment) -> Self {
        Self(fragment)
    }
}

impl From<WasmFragment> for Fragment {
    fn from(fragment: WasmFragment) -> Self {
        fragment.0
    }
}

#[wasm_bindgen(js_name = FragmentsArray)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WasmFragmentsArray(pub(crate) Vec<WasmFragment>);

#[wasm_refgen(js_ref = JsFragmentsArray)]
#[wasm_bindgen(js_class = FragmentsArray)]
impl WasmFragmentsArray {}

impl TryFrom<&JsValue> for WasmFragmentsArray {
    type Error = WasmConvertJsValueToFragmentArrayError;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(WasmFragmentsArray(
            try_into_js_fragment_array(js_value).map_err(WasmConvertJsValueToFragmentArrayError)?,
        ))
    }
}

/// An error indicating that a `JsValue` could not be converted into a `Fragment` array.
#[derive(Debug, Error)]
#[error("unable to convert JsValue into Fragment array")]
pub struct WasmConvertJsValueToFragmentArrayError(JsValue);

impl From<WasmConvertJsValueToFragmentArrayError> for JsValue {
    fn from(err: WasmConvertJsValueToFragmentArrayError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("UnableToConvertFragmentArrayError");
        err.into()
    }
}

#[wasm_bindgen(inline_js = r#"
    export function tryIntoJsFragmentArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `WasmFragment`.
    #[wasm_bindgen(js_name = tryIntoJsFragmentArray, catch)]
    pub fn try_into_js_fragment_array(v: &JsValue) -> Result<Vec<WasmFragment>, JsValue>;
}
