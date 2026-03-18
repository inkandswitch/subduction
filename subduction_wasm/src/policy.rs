//! Pluggable policy types for Wasm.
//!
//! [`JsPolicy`] is a duck-typed JS-imported interface for connection and
//! storage authorization. Any JS object with the right method signatures
//! can serve as a policy — including Rust-exported types like a future
//! `WasmKeyhivePolicy`.
//!
//! [`JsEphemeralPolicy`] is a separate interface for ephemeral message
//! authorization (subscribe/publish).
//!
//! # JS interface
//!
//! A policy object must implement:
//!
//! ```js
//! {
//!   authorizeConnect(peerId: Uint8Array): Promise<void>,
//!   authorizeFetch(peerId: Uint8Array, sedimentreeId: Uint8Array): Promise<void>,
//!   authorizePut(requestor: Uint8Array, author: Uint8Array, sedimentreeId: Uint8Array): Promise<void>,
//!   filterAuthorizedFetch(peerId: Uint8Array, ids: Uint8Array[]): Promise<Uint8Array[]>,
//! }
//! ```
//!
//! Throwing (or returning a rejected promise) denies the operation.
//! Resolving allows it.

pub mod ephemeral;

use alloc::{format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::{Array, Promise, Uint8Array};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for connection and storage authorization.
    ///
    /// Any JS object with the required methods can be passed where a
    /// `JsPolicy` is expected. See the [module-level docs](self) for
    /// the required interface.
    #[wasm_bindgen(js_name = Policy)]
    pub type JsPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeConnect)]
    fn js_authorize_connect(this: &JsPolicy, peer_id: Uint8Array) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizeFetch)]
    fn js_authorize_fetch(
        this: &JsPolicy,
        peer_id: Uint8Array,
        sedimentree_id: Uint8Array,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePut)]
    fn js_authorize_put(
        this: &JsPolicy,
        requestor: Uint8Array,
        author: Uint8Array,
        sedimentree_id: Uint8Array,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedFetch)]
    fn js_filter_authorized_fetch(
        this: &JsPolicy,
        peer_id: Uint8Array,
        ids: Array,
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

fn peer_bytes(peer: PeerId) -> Uint8Array {
    Uint8Array::from(peer.as_bytes().as_slice())
}

fn sed_bytes(id: SedimentreeId) -> Uint8Array {
    Uint8Array::from(id.as_bytes().as_slice())
}

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
        let result = self.js_authorize_connect(peer_bytes(peer_id));
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
        let result = self.js_authorize_fetch(peer_bytes(peer), sed_bytes(sedimentree_id));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PutDisallowed>> {
        let result = self.js_authorize_put(
            peer_bytes(requestor),
            peer_bytes(author),
            sed_bytes(sedimentree_id),
        );
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<SedimentreeId>> {
        let js_ids = Array::new();
        for id in &ids {
            js_ids.push(&sed_bytes(*id));
        }
        let result = self.js_filter_authorized_fetch(peer_bytes(peer), js_ids);
        async move {
            let Ok(promise) = result else {
                return Vec::new();
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return Vec::new();
            };

            let Ok(arr) = resolved.dyn_into::<Array>() else {
                return Vec::new();
            };

            // Intersect with the original list to prevent a buggy/malicious
            // policy from expanding the authorized set.
            arr.iter()
                .filter_map(|item| {
                    let bytes: Uint8Array = item.dyn_into().ok()?;
                    let vec = bytes.to_vec();
                    let arr: [u8; 32] = vec.try_into().ok()?;
                    let id = SedimentreeId::new(arr);
                    ids.contains(&id).then_some(id)
                })
                .collect()
        }
        .boxed_local()
    }
}
