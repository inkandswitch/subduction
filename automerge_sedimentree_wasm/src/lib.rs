//! Wasm bindings for `automerge_sedimentree`

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)] // wasm_bindgen doens't like const

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod error;
pub mod fragment;

use alloc::{string::String, vec::Vec};
use sedimentree_core::collections::Set;

use base58::FromBase58;
use error::{WasmFragmentError, WasmFromBase58Error, WasmLookupError};
use fragment::{WasmFragmentState, WasmFragmentStateStore};
use js_sys::{Array, Uint8Array};
use sedimentree_core::{
    commit::CommitStore, crypto::digest::Digest, hex::decode_hex, loose_commit::LooseCommit,
};
use sedimentree_wasm::digest::{JsDigest, WasmDigest};
use subduction_wasm::subduction::WasmHashMetric;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around a duck-typed `Automerge` instance.
#[wasm_bindgen(js_name = SedimentreeAutomerge)]
pub struct WasmSedimentreeAutomerge(JsAutomerge);

#[wasm_bindgen(js_class = SedimentreeAutomerge)]
impl WasmSedimentreeAutomerge {
    /// Create a new `WasmSedimentreeAutomerge` instance.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // wasm_bindgen does not support const constructors
    pub fn new(automerge: JsAutomerge) -> Self {
        Self(automerge)
    }

    // NOTE `js_` prefix to avoid conflict
    // with CommitStore::fragment (trait method)
    /// Build the fragment state for a given head.
    ///
    /// # Errors
    ///
    /// Returns a `WasmFragmentError` if building the fragment state fails.
    #[wasm_bindgen(js_name = fragment)]
    pub fn js_fragment(
        &self,
        head: &WasmDigest,
        known_states: &WasmFragmentStateStore,
        hash_metric: &WasmHashMetric,
    ) -> Result<WasmFragmentState, WasmFragmentError> {
        Ok(self
            .fragment(head.clone().into(), &known_states.0, hash_metric)
            .map(WasmFragmentState)?)
    }

    // NOTE `js_` prefix to avoid conflict
    /// Build a fragment store starting from the given head digests.
    ///
    /// # Errors
    ///
    /// Returns a `WasmFragmentError` if building the fragment store fails.
    #[wasm_bindgen(js_name = buildFragmentStore)]
    pub fn js_build_fragment_store(
        &self,
        head_digests: Vec<JsDigest>,
        known_fragment_states: &mut WasmFragmentStateStore,
        strategy: &WasmHashMetric,
    ) -> Result<Vec<WasmFragmentState>, WasmFragmentError> {
        let heads: Vec<Digest<LooseCommit>> = head_digests
            .into_iter()
            .map(|js_digest| WasmDigest::from(&js_digest).into())
            .collect();

        let fresh = self
            .build_fragment_store(&heads, &mut known_fragment_states.0, strategy)?
            .into_iter()
            .cloned()
            .map(WasmFragmentState)
            .collect();

        Ok(fresh)
    }
}

impl core::fmt::Debug for WasmSedimentreeAutomerge {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("WasmSedimentreeAutomerge").finish()
    }
}

impl CommitStore<'static> for WasmSedimentreeAutomerge {
    type Node = Set<Digest<LooseCommit>>;
    type LookupError = WasmLookupError;

    fn lookup(&self, digest: Digest<LooseCommit>) -> Result<Option<Self::Node>, Self::LookupError> {
        let mut hexes = Vec::with_capacity(32);
        for byte in digest.as_bytes() {
            hexes.push(alloc::format!("{byte:02x}"));
        }
        let hash_hex = hexes.join("");

        let js_value_should_be_change_meta = self
            .0
            .get_change_meta_by_hash(hash_hex)
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
                    if !(0.0..=255.0).contains(&b) {
                        return Err(WasmLookupError::ByteValueOutOfRange(b));
                    }
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    v.push(b as u8);
                }
                v
            } else if item.is_string() {
                #[allow(clippy::expect_used)]
                let s = item.as_string().expect("just checked is_string");
                decode_hex(&s).ok_or(WasmLookupError::InvalidHexString)?
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

        Ok(Some(
            deps.into_iter().map(|h| Digest::from_bytes(h.0)).collect(),
        ))
    }
}

/// Compute the digest of a base58-encoded ID string.
///
/// # Errors
///
/// Returns a `WasmFromBase58Error` if the input string is not valid base58.
#[wasm_bindgen(js_name = digestOfBase58Id)]
pub fn digest_of_base58_id(b58_str: &str) -> Result<WasmDigest, WasmFromBase58Error> {
    let decoded = b58_str.from_base58()?;
    let raw: [u8; 32] = blake3::hash(&decoded).into();
    Ok(Digest::<LooseCommit>::from_bytes(raw).into())
}

#[wasm_bindgen]
extern "C" {
    /// JS interface for `Automerge`.
    pub type JsAutomerge;

    /// Get change metadata by its hash.
    #[wasm_bindgen(method, js_name = getChangeMetaByHash, catch)]
    fn get_change_meta_by_hash(this: &JsAutomerge, hash: String) -> Result<JsValue, JsValue>;
}
