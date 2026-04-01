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
use js_sys::{Promise, Uint8Array};
use subduction_core::peer::id::PeerId;
use subduction_ephemeral::{policy::EphemeralPolicy, topic::Topic};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{peer_id::WasmPeerId, topic::WasmTopic};

// ── Duck-typed JS import ────────────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A duck-typed JS policy object for ephemeral message authorization.
    ///
    /// Any JS object with the required methods can be passed where a
    /// `JsEphemeralPolicy` is expected.
    #[wasm_bindgen(js_name = EphemeralPolicy, typescript_type = "EphemeralPolicy")]
    pub type JsEphemeralPolicy;

    #[wasm_bindgen(method, catch, js_name = authorizeSubscribe)]
    fn js_authorize_subscribe(
        this: &JsEphemeralPolicy,
        peer_id: WasmPeerId,
        topic: WasmTopic,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = authorizePublish)]
    fn js_authorize_publish(
        this: &JsEphemeralPolicy,
        peer_id: WasmPeerId,
        topic: WasmTopic,
    ) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = filterAuthorizedSubscribers)]
    fn js_filter_authorized_subscribers(
        this: &JsEphemeralPolicy,
        topic: WasmTopic,
        peers: Vec<WasmPeerId>,
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
        let result = self.js_authorize_subscribe(WasmPeerId::from(peer), WasmTopic::from(id));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        id: Topic,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
        let result = self.js_authorize_publish(WasmPeerId::from(peer), WasmTopic::from(id));
        async move { await_void_promise(result).await }.boxed_local()
    }

    fn filter_authorized_subscribers(
        &self,
        id: Topic,
        peers: Vec<PeerId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<PeerId>> {
        let wasm_peers: Vec<WasmPeerId> = peers.iter().copied().map(WasmPeerId::from).collect();
        let result = self.js_filter_authorized_subscribers(WasmTopic::from(id), wasm_peers);
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
            let allowed: BTreeSet<PeerId> = peers.into_iter().collect();
            arr.iter()
                .filter_map(|item| {
                    // The returned items are WasmPeerId objects;
                    // extract bytes via their toBytes() method.
                    let to_bytes = js_sys::Reflect::get(&item, &"toBytes".into()).ok()?;
                    let func: js_sys::Function = to_bytes.dyn_into().ok()?;
                    let bytes_val = func.call0(&item).ok()?;
                    let bytes: Uint8Array = bytes_val.dyn_into().ok()?;
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
