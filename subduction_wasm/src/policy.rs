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
use js_sys::Promise;
use sedimentree_core::id::SedimentreeId;
use sedimentree_wasm::pojo::{hex, unhex};
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

// ── TypeScript interface ────────────────────────────────────────────────

#[wasm_bindgen(typescript_custom_section)]
const TS_POLICY: &str = r#"
/**
 * Connection and storage authorization policy.
 *
 * Throwing (or returning a rejected promise) denies the operation.
 * Resolving allows it.
 */
export interface Policy {
    authorizeConnect(peerId: PeerId): Promise<void>;
    authorizeFetch(peerId: PeerId, sedimentreeId: SedimentreeId): Promise<void>;
    authorizePut(requestor: PeerId, author: PeerId, sedimentreeId: SedimentreeId): Promise<void>;
    filterAuthorizedFetch(peerId: PeerId, ids: SedimentreeId[]): Promise<SedimentreeId[]>;
}
"#;

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for connection and storage authorization.
    ///
    /// Must implement the `Policy` TypeScript interface emitted by this module.
    #[wasm_bindgen(js_name = Policy, typescript_type = "Policy")]
    pub type JsPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeConnect)]
    fn js_authorize_connect(this: &JsPolicy, peer_id: String) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizeFetch)]
    fn js_authorize_fetch(
        this: &JsPolicy,
        peer_id: String,
        sedimentree_id: String,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePut)]
    fn js_authorize_put(
        this: &JsPolicy,
        requestor: String,
        author: String,
        sedimentree_id: String,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedFetch)]
    fn js_filter_authorized_fetch(
        this: &JsPolicy,
        peer_id: String,
        ids: Vec<String>,
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
        let result = self.js_authorize_connect(hex(peer_id.as_bytes()));
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
        let result = self.js_authorize_fetch(hex(peer.as_bytes()), hex(sedimentree_id.as_bytes()));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PutDisallowed>> {
        let result = self.js_authorize_put(
            hex(requestor.as_bytes()),
            hex(author.verifying_key().as_bytes()),
            hex(sedimentree_id.as_bytes()),
        );
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<SedimentreeId>> {
        let js_ids = ids.iter().map(|id| hex(id.as_bytes())).collect();
        let result = self.js_filter_authorized_fetch(hex(peer.as_bytes()), js_ids);
        async move {
            let Ok(promise) = result else {
                return Vec::new();
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return Vec::new();
            };

            let Ok(arr) =
                sedimentree_wasm::pojo::array(&resolved, "filterAuthorizedFetch return value")
            else {
                return Vec::new();
            };

            // Intersect with the original list to prevent a buggy/malicious
            // policy from expanding the authorized set.
            let allowed: BTreeSet<SedimentreeId> = ids.into_iter().collect();
            arr.into_iter()
                .filter_map(|item| {
                    let id = SedimentreeId::new(unhex(&item, "filterAuthorizedFetch[]").ok()?);
                    allowed.contains(&id).then_some(id)
                })
                .collect()
        }
        .boxed_local()
    }
}
