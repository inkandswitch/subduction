//! JS-pluggable ephemeral policy.
//!
//! Wraps a JS object that provides:
//! - `authorizeSubscribe(peerId: Uint8Array, id: Uint8Array): Promise<void>`
//! - `authorizePublish(peerId: Uint8Array, id: Uint8Array): Promise<void>`
//! - `filterAuthorizedSubscribers(id: Uint8Array, peers: Uint8Array[]): Promise<Uint8Array[]>`

use alloc::{format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::{Array, Promise, Uint8Array};
use sedimentree_core::id::SedimentreeId;
use subduction_core::peer::id::PeerId;
use subduction_ephemeral::policy::EphemeralPolicy;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// An ephemeral policy backed by a JS object.
///
/// The JS object must have:
///
/// ```js
/// {
///   authorizeSubscribe(peerId: Uint8Array, id: Uint8Array): Promise<void>,
///   authorizePublish(peerId: Uint8Array, id: Uint8Array): Promise<void>,
///   filterAuthorizedSubscribers(id: Uint8Array, peers: Uint8Array[]): Promise<Uint8Array[]>,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct JsEphemeralPolicy {
    inner: JsValue,
}

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

impl JsEphemeralPolicy {
    /// Wrap a JS object as an ephemeral policy.
    #[must_use]
    pub const fn new(inner: JsValue) -> Self {
        Self { inner }
    }
}

/// Call a JS method that returns `Promise<void>`, mapping rejection to an error.
async fn call_void_method(
    inner: &JsValue,
    method_name: &str,
    args: &[JsValue],
) -> Result<(), JsEphemeralDenied> {
    let method = js_sys::Reflect::get(inner, &method_name.into())
        .map_err(|e| JsEphemeralDenied::from_js(&e))?;
    let func: js_sys::Function = method.dyn_into().map_err(|_| JsEphemeralDenied {
        reason: alloc::format!("{method_name} is not a function"),
    })?;
    let js_args = Array::new();
    for arg in args {
        js_args.push(arg);
    }
    let result = func
        .apply(inner, &js_args)
        .map_err(|e| JsEphemeralDenied::from_js(&e))?;
    if let Ok(promise) = result.dyn_into::<Promise>() {
        JsFuture::from(promise)
            .await
            .map_err(|e| JsEphemeralDenied::from_js(&e))?;
    }
    Ok(())
}

impl EphemeralPolicy<Local> for JsEphemeralPolicy {
    type SubscribeDisallowed = JsEphemeralDenied;
    type PublishDisallowed = JsEphemeralDenied;

    fn authorize_subscribe(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::SubscribeDisallowed>> {
        let inner = self.inner.clone();
        async move {
            let peer_bytes = Uint8Array::from(peer.as_bytes().as_slice());
            let id_bytes = Uint8Array::from(id.as_bytes().as_slice());
            call_void_method(
                &inner,
                "authorizeSubscribe",
                &[peer_bytes.into(), id_bytes.into()],
            )
            .await
        }
        .boxed_local()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
        let inner = self.inner.clone();
        async move {
            let peer_bytes = Uint8Array::from(peer.as_bytes().as_slice());
            let id_bytes = Uint8Array::from(id.as_bytes().as_slice());
            call_void_method(
                &inner,
                "authorizePublish",
                &[peer_bytes.into(), id_bytes.into()],
            )
            .await
        }
        .boxed_local()
    }

    fn filter_authorized_subscribers(
        &self,
        id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<PeerId>> {
        let inner = self.inner.clone();
        async move {
            // Fail closed: if the JS policy errors, return empty (deny all)
            // rather than returning the original list (allow all).
            let Ok(method) = js_sys::Reflect::get(&inner, &"filterAuthorizedSubscribers".into())
            else {
                return Vec::new();
            };
            let Ok(func): Result<js_sys::Function, _> = method.dyn_into() else {
                return Vec::new();
            };

            let id_bytes = Uint8Array::from(id.as_bytes().as_slice());
            let js_peers = Array::new();
            for peer in &peers {
                js_peers.push(&Uint8Array::from(peer.as_bytes().as_slice()));
            }

            let Ok(result) = func.call2(&inner, &id_bytes, &js_peers) else {
                return Vec::new();
            };

            let Ok(promise): Result<Promise, _> = result.dyn_into() else {
                return Vec::new();
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return Vec::new();
            };

            let Ok(arr): Result<Array, _> = resolved.dyn_into() else {
                return Vec::new();
            };

            arr.iter()
                .filter_map(|item| {
                    let bytes: Uint8Array = item.dyn_into().ok()?;
                    let vec = bytes.to_vec();
                    let arr: [u8; 32] = vec.try_into().ok()?;
                    Some(PeerId::new(arr))
                })
                .collect()
        }
        .boxed_local()
    }
}
