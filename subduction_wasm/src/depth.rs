//! Wasm wrapper for `Depth`.

use sedimentree_core::depth::Depth;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A JavaScript wrapper around `Depth`.
#[wasm_bindgen(js_name = Depth)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmDepth(Depth);

impl From<Depth> for WasmDepth {
    fn from(depth: Depth) -> Self {
        WasmDepth(depth)
    }
}

impl From<WasmDepth> for Depth {
    fn from(js_depth: WasmDepth) -> Self {
        js_depth.0
    }
}

#[wasm_refgen(js_ref = JsDepth)]
#[wasm_bindgen(js_class = Depth)]
impl WasmDepth {
    /// Creates a new `WasmDepth` from a JavaScript value.
    ///
    /// # Errors
    ///
    /// Returns a `NotU32Error` if the JS value is not safely coercible to `u32`.
    #[wasm_bindgen(constructor)]
    pub fn new(js_value: &JsValue) -> Result<Self, NotU32Error> {
        let value = js_value
            .as_f64()
            .and_then(|f| {
                if f.is_finite() && f.fract() == 0.0 && 0.0 <= f && f <= (f64::from(u32::MAX)) {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    Some(f as u32)
                } else {
                    None
                }
            })
            .ok_or(NotU32Error)?;

        Ok(WasmDepth(Depth(value)))
    }

    /// The depth value as an integer.
    #[must_use]
    #[wasm_bindgen(getter)]
    #[allow(clippy::missing_const_for_fn)]
    pub fn value(&self) -> u32 {
        self.0 .0
    }

    /// Intrenal method for a hack crossing the JS bounary.
    #[must_use]
    #[wasm_bindgen(js_name = __subduction_castToDepth)]
    #[allow(clippy::missing_const_for_fn)]
    pub fn cast_to_depth(&self) -> WasmDepth {
        self.clone()
    }
}

/// Error indicating that a value is not a valid u32.
#[derive(Debug, Clone, Error)]
#[error("value is not a valid u32")]
#[allow(missing_copy_implementations)]
pub struct NotU32Error;

impl From<NotU32Error> for JsValue {
    fn from(err: NotU32Error) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("NotU32Error");
        err.into()
    }
}

#[wasm_bindgen]
extern "C" {
    /// An interface for functions from `JsDigest` to `WasmDepth` on the JS side of the boundary.
    #[wasm_bindgen(typescript_type = "(digest: Digest) => Depth")]
    pub type JsToDepth;
}

impl std::fmt::Debug for JsToDepth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsToDepth").finish()
    }
}
