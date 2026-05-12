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
    commit::{CommitStore, Parents},
    hex::decode_hex,
    loose_commit::id::CommitId,
};
use sedimentree_wasm::{
    alloc_cap::cap_array_length,
    commit_id::{JsCommitId, WasmCommitId},
};
use subduction_wasm::subduction::WasmHashMetric;
use wasm_bindgen::prelude::*;

/// Maximum reservation when pre-allocating a `Vec` for an Automerge
/// change's `deps` array.
///
/// Each entry in `deps` is the parent of an Automerge change. In real
/// workloads a single change has at most a handful of parents (one
/// per concurrent author). The 16384 cap exists only to neutralise an
/// adversarial / corrupted JS Automerge object reporting a fabricated
/// huge length — it would have to represent thousands of concurrent
/// authors to even approach this bound legitimately.
const MAX_DEPS_RESERVATION: usize = 16 * 1024;

/// Maximum reservation when pre-allocating a `Vec` for the inner
/// per-dep byte array (when a JS Automerge object encodes a `ChangeHash`
/// as `Array<number>` instead of `Uint8Array`).
///
/// A `ChangeHash` is exactly 32 bytes by Automerge specification. We
/// cap the reservation at 64 (2× headroom) — anything longer is
/// rejected by the subsequent `bytes.len() != 32` check anyway, but
/// pre-allocating 64 bytes per dep is cheap and obviously safe.
const MAX_INNER_HASH_RESERVATION: usize = 64;

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
    /// Returns [`WasmFragmentError::StoreBusy`] if the same
    /// `FragmentStateStore` is already locked by an in-flight fragment
    /// computation (e.g. a `getChangeMetaByHash` JS callback re-entered
    /// this method on the same store). Returns
    /// [`WasmFragmentError::Fragment`] if fragment construction itself
    /// fails.
    #[wasm_bindgen(js_name = fragment)]
    pub fn js_fragment(
        &self,
        head: &WasmCommitId,
        known_states: &WasmFragmentStateStore,
        hash_metric: &WasmHashMetric,
    ) -> Result<WasmFragmentState, WasmFragmentError> {
        let head_id = CommitId::from(head);
        // `try_borrow` rather than `borrow` so that a re-entrant JS
        // callback that attempts to mutate the same store returns a
        // typed error instead of panicking the Wasm module.
        let known = known_states
            .0
            .try_borrow()
            .map_err(|_| WasmFragmentError::StoreBusy)?;
        Ok(self
            .fragment(head_id, &known, hash_metric)
            .map(WasmFragmentState)?)
    }

    // NOTE `js_` prefix to avoid conflict
    /// Build a fragment store starting from the given head digests.
    ///
    /// # Errors
    ///
    /// Returns [`WasmFragmentError::StoreBusy`] if the same
    /// `FragmentStateStore` is already locked by an in-flight fragment
    /// computation. Returns [`WasmFragmentError::Fragment`] if
    /// fragment construction itself fails.
    #[wasm_bindgen(js_name = buildFragmentStore)]
    pub fn js_build_fragment_store(
        &self,
        head_ids: Vec<JsCommitId>,
        known_fragment_states: &WasmFragmentStateStore,
        strategy: &WasmHashMetric,
    ) -> Result<Vec<WasmFragmentState>, WasmFragmentError> {
        let heads: Vec<CommitId> = head_ids
            .into_iter()
            .map(|js_id| CommitId::from(WasmCommitId::from(&js_id)))
            .collect();

        // `try_borrow_mut` for the same reason as above. The borrow is
        // held for the duration of `build_fragment_store`, which calls
        // `CommitStore::lookup` (and therefore the user-supplied JS
        // `getChangeMetaByHash` callback) — re-entering through that
        // callback would otherwise panic.
        let mut known = known_fragment_states
            .0
            .try_borrow_mut()
            .map_err(|_| WasmFragmentError::StoreBusy)?;
        let fresh = self
            .build_fragment_store(&heads, &mut known, strategy)?
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

/// A Wasm-compatible [`Parents`] wrapper around a set of [`CommitId`]s.
#[derive(Debug, Clone)]
pub struct WasmParents(Set<CommitId>);

impl Parents for WasmParents {
    fn parents(&self) -> Set<CommitId> {
        self.0.clone()
    }
}

impl CommitStore<'static> for WasmSedimentreeAutomerge {
    type Node = WasmParents;
    type LookupError = WasmLookupError;

    fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
        let mut hexes = Vec::with_capacity(32);
        for byte in id.as_bytes() {
            hexes.push(alloc::format!("{byte:02x}"));
        }
        let hash_hex = hexes.join("");

        let js_value_should_be_change_meta = self
            .0
            .get_change_meta_by_hash(hash_hex)
            .map_err(WasmLookupError::ProblemCallingGetChangeMetaByHash)?;

        if js_value_should_be_change_meta.is_null() || js_value_should_be_change_meta.is_undefined()
        {
            return Ok(None);
        }

        let _obj: &js_sys::Object = js_value_should_be_change_meta
            .dyn_ref()
            .ok_or(WasmLookupError::MetaShouldBeObject)?;

        let deps_val =
            js_sys::Reflect::get(&js_value_should_be_change_meta, &JsValue::from_str("deps"))
                .map_err(|_| WasmLookupError::NoDepsMethod)?;

        if !js_sys::Array::is_array(&deps_val) {
            return Err(WasmLookupError::DepsAreNotArray);
        }

        let deps_arr = js_sys::Array::from(&deps_val);
        // Cap the pre-allocation against MAX_DEPS_RESERVATION; see
        // doc comment for rationale.
        let mut deps = Vec::with_capacity(cap_array_length(
            deps_arr.length() as usize,
            MAX_DEPS_RESERVATION,
        ));

        for i in 0..deps_arr.length() {
            let item = deps_arr.get(i);

            // Handle both Uint8Array and Array<number>
            let bytes: Vec<u8> = if item.is_instance_of::<Uint8Array>() {
                Uint8Array::new(&item).to_vec()
            } else if Array::is_array(&item) {
                let a = Array::from(&item);
                // Cap the inner-array pre-allocation. A valid
                // ChangeHash is exactly 32 bytes; the
                // `bytes.len() != 32` check below rejects anything
                // else, but we cap reservation defensively.
                let mut v = Vec::with_capacity(cap_array_length(
                    a.length() as usize,
                    MAX_INNER_HASH_RESERVATION,
                ));
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

        Ok(Some(WasmParents(
            deps.into_iter().map(|h| CommitId::new(h.0)).collect(),
        )))
    }
}

/// Compute the commit ID of a base58-encoded ID string.
///
/// # Errors
///
/// Returns a `WasmFromBase58Error` if the input string is not valid base58.
#[wasm_bindgen(js_name = commitIdOfBase58Id)]
pub fn commit_id_of_base58_id(b58_str: &str) -> Result<WasmCommitId, WasmFromBase58Error> {
    let decoded = b58_str.from_base58()?;
    let raw: [u8; 32] = blake3::hash(&decoded).into();
    Ok(WasmCommitId::from(CommitId::new(raw)))
}

#[wasm_bindgen]
extern "C" {
    /// JS interface for `Automerge`.
    pub type JsAutomerge;

    /// Get change metadata by its hash.
    #[wasm_bindgen(method, js_name = getChangeMetaByHash, catch)]
    fn get_change_meta_by_hash(this: &JsAutomerge, hash: String) -> Result<JsValue, JsValue>;
}
