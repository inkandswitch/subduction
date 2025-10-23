//! Sedimentree [`Fragment`](sedimentree_core::Fragment).

use sedimentree_core::Fragment;
use subduction_core::subduction::request::FragmentRequested;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A data fragment used in the Sedimentree system.
#[wasm_bindgen(js_name = Fragment)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmFragment(Fragment);

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

/// A request for a specific fragment in the Sedimentree system.
#[wasm_bindgen(js_name = FragmentRequested)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct WasmFragmentRequested(FragmentRequested);

impl From<FragmentRequested> for WasmFragmentRequested {
    fn from(req: FragmentRequested) -> Self {
        Self(req)
    }
}

impl From<WasmFragmentRequested> for FragmentRequested {
    fn from(req: WasmFragmentRequested) -> Self {
        req.0
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
