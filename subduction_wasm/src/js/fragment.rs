//! Sedimentree [`Fragment`](sedimentree_core::Fragment).

use sedimentree_core::Fragment;
use subduction_core::subduction::request::FragmentRequested;
use wasm_bindgen::prelude::*;

/// A data fragment used in the Sedimentree system.
#[wasm_bindgen(js_name = Fragment)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsFragment(Fragment);

impl From<Fragment> for JsFragment {
    fn from(fragment: Fragment) -> Self {
        Self(fragment)
    }
}

impl From<JsFragment> for Fragment {
    fn from(fragment: JsFragment) -> Self {
        fragment.0
    }
}

/// A request for a specific fragment in the Sedimentree system.
#[wasm_bindgen(js_name = FragmentRequested)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct JsFragmentRequested(FragmentRequested);

impl From<FragmentRequested> for JsFragmentRequested {
    fn from(req: FragmentRequested) -> Self {
        Self(req)
    }
}

impl From<JsFragmentRequested> for FragmentRequested {
    fn from(req: JsFragmentRequested) -> Self {
        req.0
    }
}

#[wasm_bindgen(inline_js = r#"
export function tryIntoFragmentArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `JsFragment`.
    #[wasm_bindgen(js_name = tryIntoFragmentArray, catch)]
    pub fn try_into_js_fragment_array(v: &JsValue) -> Result<Vec<JsFragment>, JsValue>;
}

pub(crate) struct JsFragmentsArray(pub(crate) Vec<JsFragment>);

impl TryFrom<&JsValue> for JsFragmentsArray {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(JsFragmentsArray(try_into_js_fragment_array(js_value)?))
    }
}
