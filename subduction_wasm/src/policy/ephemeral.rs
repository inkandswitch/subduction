//! JS-pluggable ephemeral policy.
//!
//! [`JsEphemeralPolicy`] is a duck-typed JS-imported interface for
//! ephemeral message authorization (subscribe/publish), matching the
//! emitted `EphemeralPolicy` TypeScript interface.
//!
//! Throwing (or returning a rejected promise) denies the operation.
//! Resolving allows it.

use alloc::{collections::BTreeSet, format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::{Array, Promise, Uint8Array};
use subduction_core::peer::id::PeerId;
use subduction_ephemeral::{policy::EphemeralPolicy, topic::Topic};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

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
    /** Authorize a peer subscribing to ephemeral messages on a topic. */
    authorizeSubscribe(peerId: Uint8Array, topic: Uint8Array): Promise<void>;

    /** Authorize a peer publishing an ephemeral message to a topic. */
    authorizePublish(peerId: Uint8Array, topic: Uint8Array): Promise<void>;

    /**
     * Filter a list of subscriber peer IDs to only those currently
     * authorized for the given topic.
     *
     * The returned array must be a subset of `peers`; extra entries
     * are silently discarded.
     */
    filterAuthorizedSubscribers(topic: Uint8Array, peers: Uint8Array[]): Promise<Uint8Array[]>;
}
"#;

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for ephemeral message authorization.
    ///
    /// Any JS object with the required methods can be passed where a
    /// `JsEphemeralPolicy` is expected. See the TypeScript `EphemeralPolicy`
    /// interface for the required shape.
    #[wasm_bindgen(js_name = EphemeralPolicy, typescript_type = "EphemeralPolicy")]
    pub type JsEphemeralPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeSubscribe)]
    fn js_authorize_subscribe(
        this: &JsEphemeralPolicy,
        peer_id: Uint8Array,
        topic: Uint8Array,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePublish)]
    fn js_authorize_publish(
        this: &JsEphemeralPolicy,
        peer_id: Uint8Array,
        topic: Uint8Array,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedSubscribers)]
    fn js_filter_authorized_subscribers(
        this: &JsEphemeralPolicy,
        topic: Uint8Array,
        peers: Array,
    ) -> Result<Promise, JsValue>;
}

// wasm_bindgen extern types need a manual Clone impl.
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

fn peer_bytes(peer: PeerId) -> Uint8Array {
    Uint8Array::from(peer.as_bytes().as_slice())
}

fn topic_bytes(id: Topic) -> Uint8Array {
    Uint8Array::from(id.as_bytes().as_slice())
}

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
        let result = self.js_authorize_subscribe(peer_bytes(peer), topic_bytes(id));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        id: Topic,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
        let result = self.js_authorize_publish(peer_bytes(peer), topic_bytes(id));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_subscribers(
        &self,
        id: Topic,
        peers: Vec<PeerId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<PeerId>> {
        let js_peers = Array::new();
        for peer in &peers {
            js_peers.push(&peer_bytes(*peer));
        }
        let result = self.js_filter_authorized_subscribers(topic_bytes(id), js_peers);
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
            let allowed: BTreeSet<PeerId> = peers.into_iter().collect();
            arr.iter()
                .filter_map(|item| {
                    let bytes: Uint8Array = item.dyn_into().ok()?;
                    let vec = bytes.to_vec();
                    let arr: [u8; 32] = vec.try_into().ok()?;
                    let peer = PeerId::new(arr);
                    allowed.contains(&peer).then_some(peer)
                })
                .collect()
        }
        .boxed_local()
    }
}
