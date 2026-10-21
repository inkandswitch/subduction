//! JS-pluggable ephemeral policy.
//!
//! [`JsEphemeralPolicy`] is a duck-typed JS-imported interface for
//! ephemeral message authorization (subscribe/publish).
//!
//! Throwing (or returning a rejected promise) denies the operation.
//! Resolving allows it.

use alloc::{collections::BTreeSet, format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::Promise;
use subduction_core::peer::id::PeerId;
use subduction_ephemeral::{policy::EphemeralPolicy, topic::Topic};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use sedimentree_wasm::pojo::{hex, unhex};

// ── TypeScript interface ────────────────────────────────────────────────

#[wasm_bindgen(typescript_custom_section)]
const TS_EPHEMERAL_POLICY: &str = r#"
/**
 * Ephemeral message authorization policy.
 *
 * Throwing (or returning a rejected promise) denies the operation.
 * Resolving allows it.
 */
export interface EphemeralPolicy {
    authorizeSubscribe(peerId: PeerId, topic: Topic): Promise<void>;
    authorizePublish(peerId: PeerId, topic: Topic): Promise<void>;
    filterAuthorizedSubscribers(topic: Topic, peers: PeerId[]): Promise<PeerId[]>;
}
"#;

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for ephemeral message authorization.
    ///
    /// Must implement the `EphemeralPolicy` TypeScript interface emitted by
    /// this module.
    #[wasm_bindgen(js_name = EphemeralPolicy, typescript_type = "EphemeralPolicy")]
    pub type JsEphemeralPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeSubscribe)]
    fn js_authorize_subscribe(
        this: &JsEphemeralPolicy,
        peer_id: String,
        topic: String,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePublish)]
    fn js_authorize_publish(
        this: &JsEphemeralPolicy,
        peer_id: String,
        topic: String,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedSubscribers)]
    fn js_filter_authorized_subscribers(
        this: &JsEphemeralPolicy,
        topic: String,
        peers: Vec<String>,
    ) -> Result<Promise, JsValue>;
}

impl Clone for JsEphemeralPolicy {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}

// ── Open policy (JS glue) ───────────────────────────────────────────────

#[wasm_bindgen(inline_js = r#"
export function makeOpenEphemeralPolicy() {
    return {
        authorizeSubscribe() { return Promise.resolve(); },
        authorizePublish() { return Promise.resolve(); },
        filterAuthorizedSubscribers(_topic, peers) { return Promise.resolve(peers); },
    };
}
"#)]
extern "C" {
    /// Create an open (allow-all) ephemeral policy JS object.
    #[wasm_bindgen(js_name = makeOpenEphemeralPolicy)]
    pub fn make_open_ephemeral_policy() -> JsEphemeralPolicy;
}

// ── Error type ──────────────────────────────────────────────────────────

/// Error returned when a JS ephemeral policy denies an operation.
#[derive(Debug, Clone, thiserror::Error)]
#[error("ephemeral operation denied: {reason}")]
pub struct JsEphemeralDenied {
    reason: String,
}

impl JsEphemeralDenied {
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

async fn await_void_promise(result: Result<Promise, JsValue>) -> Result<(), JsEphemeralDenied> {
    let promise = result.map_err(|e| JsEphemeralDenied::from_js(&e))?;
    JsFuture::from(promise)
        .await
        .map_err(|e| JsEphemeralDenied::from_js(&e))?;
    Ok(())
}

// ── EphemeralPolicy ─────────────────────────────────────────────────────

impl EphemeralPolicy<Local> for JsEphemeralPolicy {
    type SubscribeDisallowed = JsEphemeralDenied;
    type PublishDisallowed = JsEphemeralDenied;

    fn authorize_subscribe(
        &self,
        peer: PeerId,
        id: Topic,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::SubscribeDisallowed>> {
        let result = self.js_authorize_subscribe(hex(peer.as_bytes()), hex(id.as_bytes()));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        id: Topic,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
        let result = self.js_authorize_publish(hex(peer.as_bytes()), hex(id.as_bytes()));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_subscribers(
        &self,
        id: Topic,
        peers: Vec<PeerId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<PeerId>> {
        let js_peers = peers.iter().map(|peer| hex(peer.as_bytes())).collect();
        let result = self.js_filter_authorized_subscribers(hex(id.as_bytes()), js_peers);
        async move {
            let Ok(promise) = result else {
                return Vec::new();
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return Vec::new();
            };

            let Ok(arr) = sedimentree_wasm::pojo::array(
                &resolved,
                "filterAuthorizedSubscribers return value",
            ) else {
                return Vec::new();
            };

            // Intersect with the original list to prevent a buggy/malicious
            // policy from expanding the authorized set.
            let allowed: BTreeSet<PeerId> = peers.into_iter().collect();
            arr.into_iter()
                .filter_map(|item| {
                    let peer = PeerId::new(unhex(&item, "filterAuthorizedSubscribers[]").ok()?);
                    allowed.contains(&peer).then_some(peer)
                })
                .collect()
        }
        .boxed_local()
    }
}
