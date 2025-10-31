//! Wasm bindings for `sedimentree_automerge`

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use std::collections::HashSet;

use base58::FromBase58;
use js_sys::{Array, Uint8Array};
use sedimentree_core::{
    blob::Digest,
    commit::{CommitStore, FragmentError, FragmentState},
};
use subduction_wasm::{digest::WasmDigest, subduction::WasmHashMetric};
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around a duck-typed `Automerge` instance.
#[wasm_bindgen(js_name = SedimentreeAutomerge)]
pub struct WasmSedimentreeAutomerge(JsAutomerge);

impl std::fmt::Debug for WasmSedimentreeAutomerge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WasmSedimentreeAutomerge").finish()
    }
}

#[wasm_bindgen(js_class = SedimentreeAutomerge)]
impl WasmSedimentreeAutomerge {
    /// Create a new `WasmSedimentreeAutomerge` instance.
    #[wasm_bindgen(constructor)]
    pub fn new(automerge: JsAutomerge) -> Self {
        Self(automerge)
    }

    #[wasm_bindgen(js_name = fragment)]
    pub fn js_fragment(
        &self,
        wasm_digest: &WasmDigest,
        hash_metric: &WasmHashMetric,
    ) -> Result<WasmFragmentState, WasmFragmentError> {
        let digest: Digest = wasm_digest.clone().into();
        self.fragment(digest, hash_metric)
            .map(WasmFragmentState)
            .map_err(WasmFragmentError)
    }
}

impl CommitStore<'static> for WasmSedimentreeAutomerge {
    type Node = HashSet<Digest>;
    type LookupError = WasmLookupError;

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError> {
        let js_change_hash: JsValue = digest.as_bytes().to_vec().into();
        let js_value_should_be_change_meta = self
            .0
            .get_change_meta_by_hash(js_change_hash)
            .map_err(WasmLookupError::ProblemCallingGetChangeMetaByHash)?;

        let _obj: &js_sys::Object = js_value_should_be_change_meta
            .dyn_ref()
            .ok_or_else(|| WasmLookupError::MetaShouldBeObject)?;

        let deps_val =
            js_sys::Reflect::get(&js_value_should_be_change_meta, &JsValue::from_str("deps"))
                .map_err(|_| WasmLookupError::NoDepsMethod)?;

        if !js_sys::Array::is_array(&deps_val) {
            return Err(WasmLookupError::DepsAreNotArray);
        }

        let deps_arr = js_sys::Array::from(&deps_val);
        let mut deps = Vec::with_capacity(deps_arr.length() as usize);

        for i in 0..deps_arr.length() {
            let item = deps_arr.get(i);

            // Handle both Uint8Array and Array<number>
            let bytes: Vec<u8> = if item.is_instance_of::<Uint8Array>() {
                Uint8Array::new(&item).to_vec()
            } else if Array::is_array(&item) {
                let a = Array::from(&item);
                let mut v = Vec::with_capacity(a.length() as usize);
                for i in 0..a.length() {
                    let raw = a.get(i);
                    let b = raw
                        .as_f64()
                        .ok_or_else(|| WasmLookupError::UnexpectedNonNumericValue(raw))?;
                    v.push(b as u8);
                }
                v
            } else {
                return Err(WasmLookupError::ExpectedHashNotByteArray(item));
            };

            if bytes.len() != 32 {
                return Err(WasmLookupError::InvalidHashLength(bytes));
            }

            let mut arr32 = [0u8; 32];
            arr32.copy_from_slice(&bytes);
            deps.push(automerge::ChangeHash(arr32));
        }

        Ok(Some(deps.into_iter().map(|h| Digest::from(h.0)).collect()))
    }
}

/// An error that can occur when looking up a change's metadata in Wasm.
#[derive(Debug, Error)]
pub enum WasmLookupError {
    /// An invalid hash length was encountered.
    #[error("invalid hash length: expected 32 bytes, got {0:?}")]
    InvalidHashLength(Vec<u8>),

    /// The expected hash was not a byte array.
    #[error("expected hash to be a byte array: got {0:?}")]
    ExpectedHashNotByteArray(JsValue),

    /// A non-numeric value was encountered where a numeric value was expected.
    #[error("expected numeric value: got {0:?}")]
    UnexpectedNonNumericValue(JsValue),

    /// Expected the `deps` field to be an array, but it wasn't.
    #[error("change metadata `deps` field is not an array")]
    DepsAreNotArray,

    /// The change metadata object is missing the `deps` method.
    #[error("object missing `deps` field")]
    NoDepsMethod,

    /// There was a problem calling `getChangeMetaByHash`.
    #[error("problem calling `getChangeMetaByHash`: {0:?}")]
    ProblemCallingGetChangeMetaByHash(JsValue),

    /// The value returned by `getChangeMetaByHash` should be an object but isn't.
    #[error("value returned by `getChangeMetaByHash` should be an object but isn't")]
    MetaShouldBeObject,
}

impl From<WasmLookupError> for JsValue {
    fn from(err: WasmLookupError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("LookupError");
        js_err.into()
    }
}

#[wasm_bindgen]
extern "C" {
    /// Duck-typed interface for `Automerge`.
    pub type JsAutomerge;

    /// Get change metadata by its hash.
    #[wasm_bindgen(method, js_name = getChangeMetaByHash, catch)]
    fn get_change_meta_by_hash(this: &JsAutomerge, hash: JsValue) -> Result<JsValue, JsValue>;
}

#[wasm_bindgen(js_name = FragmentState)]
pub struct WasmFragmentState(FragmentState<HashSet<Digest>>);

pub struct WasmFragmentError(FragmentError<'static, WasmSedimentreeAutomerge>);

impl From<WasmFragmentError> for JsValue {
    fn from(err: WasmFragmentError) -> Self {
        let js_err = js_sys::Error::new(&err.0.to_string());
        js_err.set_name("FragmentError");
        js_err.into()
    }
}

// FIXME err type
#[wasm_bindgen(js_name = digestOfBase58Id)]
pub fn digest_of_base58_id(b58_str: &str) -> Result<WasmDigest, JsValue> {
    let decoded = b58_str.from_base58().map_err(|e| {
        let js_err = js_sys::Error::new(&"base58 decoding error");
        js_err.set_name("Base58DecodeError");
        JsValue::from(js_err)
    })?;

    let raw: [u8; 32] = blake3::hash(&decoded).into();

    Ok(Digest::from(raw).into())
}

// FIXME what we actually want is base58docId -> sedimentreeId
// ...until we use [u8;32] docIDs. Maybe we just keep this as a backcompat?
