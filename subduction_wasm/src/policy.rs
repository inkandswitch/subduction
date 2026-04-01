//! Pluggable policy types for Wasm.
//!
//! [`JsPolicy`] is a duck-typed JS-imported interface for connection and
//! storage authorization. Any JS object with the right method signatures
//! can serve as a policy — including Rust-exported types like a future
//! `WasmKeyhivePolicy`.
//!
//! [`JsEphemeralPolicy`](ephemeral::JsEphemeralPolicy) is a separate
//! interface for ephemeral message authorization (subscribe/publish).
//!
//! Both interfaces follow the same convention: throwing (or returning a
//! rejected promise) denies the operation; resolving allows it.

pub mod ephemeral;

use alloc::{collections::BTreeSet, format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::id::SedimentreeId;
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::peer_id::WasmPeerId;

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for connection and storage authorization.
    ///
    /// Any JS object with the required methods can be passed where a
    /// `JsPolicy` is expected. See the [module-level docs](self) for
    /// the required interface.
    #[wasm_bindgen(js_name = Policy, typescript_type = "Policy")]
    pub type JsPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeConnect)]
    fn js_authorize_connect(this: &JsPolicy, peer_id: WasmPeerId) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizeFetch)]
    fn js_authorize_fetch(
        this: &JsPolicy,
        peer_id: WasmPeerId,
        sedimentree_id: WasmSedimentreeId,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePut)]
    fn js_authorize_put(
        this: &JsPolicy,
        requestor: WasmPeerId,
        author: WasmPeerId,
        sedimentree_id: WasmSedimentreeId,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedFetch)]
    fn js_filter_authorized_fetch(
        this: &JsPolicy,
        peer_id: WasmPeerId,
        ids: Vec<WasmSedimentreeId>,
    ) -> Result<Promise, JsValue>;
}

// ── Open policy (JS glue) ───────────────────────────────────────────────

#[wasm_bindgen(inline_js = r#"
export function makeOpenPolicy() {
    return {
        authorizeConnect() { return Promise.resolve(); },
        authorizeFetch() { return Promise.resolve(); },
        authorizePut() { return Promise.resolve(); },
        filterAuthorizedFetch(_peer, ids) { return Promise.resolve(ids); },
    };
}
"#)]
extern "C" {
    /// Create an open (allow-all) policy JS object.
    #[wasm_bindgen(js_name = makeOpenPolicy)]
    pub fn make_open_policy() -> JsPolicy;
}

// ── Error type ──────────────────────────────────────────────────────────

/// Error returned when a JS policy denies an operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("policy denied: {reason}")]
pub struct JsPolicyDenied {
    reason: String,
}

impl JsPolicyDenied {
    fn from_js(val: &JsValue) -> Self {
        let reason = val
            .as_string()
            .or_else(|| {
                js_sys::Reflect::get(val, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
            })
            .unwrap_or_else(|| format!("{val:?}"));
        Self { reason }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

async fn await_void_promise(result: Result<Promise, JsValue>) -> Result<(), JsPolicyDenied> {
    let promise = result.map_err(|e| JsPolicyDenied::from_js(&e))?;
    JsFuture::from(promise)
        .await
        .map_err(|e| JsPolicyDenied::from_js(&e))?;
    Ok(())
}

// ── ConnectionPolicy ────────────────────────────────────────────────────

impl ConnectionPolicy<Local> for JsPolicy {
    type ConnectionDisallowed = JsPolicyDenied;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::ConnectionDisallowed>>
    {
        let result = self.js_authorize_connect(WasmPeerId::from(peer_id));
        async move { await_void_promise(result).await }.boxed_local()
    }
}

// ── StoragePolicy ───────────────────────────────────────────────────────

impl StoragePolicy<Local> for JsPolicy {
    type FetchDisallowed = JsPolicyDenied;
    type PutDisallowed = JsPolicyDenied;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::FetchDisallowed>> {
        let result = self.js_authorize_fetch(
            WasmPeerId::from(peer),
            WasmSedimentreeId::from(sedimentree_id),
        );
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PutDisallowed>> {
        let result = self.js_authorize_put(
            WasmPeerId::from(requestor),
            WasmPeerId::from(PeerId::from(*author.verifying_key())),
            WasmSedimentreeId::from(sedimentree_id),
        );
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<SedimentreeId>> {
        let wasm_ids: Vec<WasmSedimentreeId> =
            ids.iter().copied().map(WasmSedimentreeId::from).collect();
        let result = self.js_filter_authorized_fetch(WasmPeerId::from(peer), wasm_ids);
        async move {
            let Ok(promise) = result else {
                return Vec::new();
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return Vec::new();
            };

            let Ok(arr) = resolved.dyn_into::<js_sys::Array>() else {
                return Vec::new();
            };

            // Intersect with the original list to prevent a buggy/malicious
            // policy from expanding the authorized set.
            let allowed: BTreeSet<SedimentreeId> = ids.into_iter().collect();
            arr.iter()
                .filter_map(|item| {
                    // The returned items are WasmSedimentreeId objects;
                    // extract bytes via their toBytes() method.
                    let to_bytes = js_sys::Reflect::get(&item, &"toBytes".into()).ok()?;
                    let func: js_sys::Function = to_bytes.dyn_into().ok()?;
                    let bytes_val = func.call0(&item).ok()?;
                    let bytes: Uint8Array = bytes_val.dyn_into().ok()?;
                    let vec = bytes.to_vec();
                    let arr: [u8; 32] = vec.try_into().ok()?;
                    let id = SedimentreeId::new(arr);
                    allowed.contains(&id).then_some(id)
                })
                .collect()
        }
        .boxed_local()
    }
}
