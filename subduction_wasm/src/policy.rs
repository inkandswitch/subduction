//! Pluggable policy types for Wasm.
//!
//! [`JsPolicy`] wraps a single JS object and implements both
//! [`ConnectionPolicy<Local>`] and [`StoragePolicy<Local>`]. Any JS object
//! with the right method signatures can serve as a policy.
//!
//! [`JsEphemeralPolicy`] is a separate type for ephemeral message
//! authorization (subscribe/publish), since [`EphemeralPolicy`] is a
//! different trait consumed by [`EphemeralHandler`], not by [`Subduction`].
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
//!
//! [`EphemeralPolicy`]: subduction_ephemeral::policy::EphemeralPolicy
//! [`EphemeralHandler`]: subduction_ephemeral::handler::EphemeralHandler
//! [`Subduction`]: subduction_core::subduction::Subduction

pub mod ephemeral;

pub use ephemeral::JsEphemeralPolicy;

use alloc::{format, string::String, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use js_sys::{Array, Promise, Uint8Array};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

/// A connection + storage policy backed by a single JS object.
///
/// See the [module-level docs](self) for the required JS interface.
#[derive(Debug, Clone)]
pub struct JsPolicy {
    inner: JsValue,
}

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
                js_sys::Reflect::get(&val, &"message".into())
                    .ok()
                    .and_then(|v| v.as_string())
            })
            .unwrap_or_else(|| format!("{val:?}"));
        Self { reason }
    }
}

impl JsPolicy {
    /// Wrap a JS object as a policy.
    #[must_use]
    pub const fn new(inner: JsValue) -> Self {
        Self { inner }
    }

    /// Create an open (allow-all) policy.
    ///
    /// All authorization methods resolve immediately. Equivalent to
    /// [`OpenPolicy`](subduction_core::policy::open::OpenPolicy).
    #[must_use]
    pub fn open() -> Self {
        Self {
            inner: make_open_policy_object(),
        }
    }
}

// ── JS glue ─────────────────────────────────────────────────────────────

#[wasm_bindgen(inline_js = r#"
export function makeOpenPolicyObject() {
    return {
        authorizeConnect() { return Promise.resolve(); },
        authorizeFetch() { return Promise.resolve(); },
        authorizePut() { return Promise.resolve(); },
        filterAuthorizedFetch(_peer, ids) { return Promise.resolve(ids); },
    };
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = makeOpenPolicyObject)]
    fn make_open_policy_object() -> JsValue;
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Call a JS method that returns `Promise<void>`, mapping rejection to an error.
async fn call_void(
    inner: &JsValue,
    method_name: &str,
    args: &[JsValue],
) -> Result<(), JsPolicyDenied> {
    let method = js_sys::Reflect::get(inner, &method_name.into())
        .map_err(|e| JsPolicyDenied::from_js(&e))?;
    let func: js_sys::Function = method.dyn_into().map_err(|_| JsPolicyDenied {
        reason: format!("{method_name} is not a function"),
    })?;
    let js_args = Array::new();
    for arg in args {
        js_args.push(arg);
    }
    let result = func
        .apply(inner, &js_args)
        .map_err(|e| JsPolicyDenied::from_js(&e))?;
    if let Ok(promise) = result.dyn_into::<Promise>() {
        JsFuture::from(promise)
            .await
            .map_err(|e| JsPolicyDenied::from_js(&e))?;
    }
    Ok(())
}

fn peer_bytes(peer: PeerId) -> JsValue {
    Uint8Array::from(peer.as_bytes().as_slice()).into()
}

fn sed_bytes(id: SedimentreeId) -> JsValue {
    Uint8Array::from(id.as_bytes().as_slice()).into()
}

// ── ConnectionPolicy ────────────────────────────────────────────────────

impl ConnectionPolicy<Local> for JsPolicy {
    type ConnectionDisallowed = JsPolicyDenied;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::ConnectionDisallowed>>
    {
        let inner = self.inner.clone();
        async move { call_void(&inner, "authorizeConnect", &[peer_bytes(peer_id)]).await }
            .boxed_local()
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
        let inner = self.inner.clone();
        async move {
            call_void(
                &inner,
                "authorizeFetch",
                &[peer_bytes(peer), sed_bytes(sedimentree_id)],
            )
            .await
        }
        .boxed_local()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as future_form::FutureForm>::Future<'_, Result<(), Self::PutDisallowed>> {
        let inner = self.inner.clone();
        async move {
            call_void(
                &inner,
                "authorizePut",
                &[
                    peer_bytes(requestor),
                    peer_bytes(author),
                    sed_bytes(sedimentree_id),
                ],
            )
            .await
        }
        .boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Local as future_form::FutureForm>::Future<'_, Vec<SedimentreeId>> {
        let inner = self.inner.clone();
        async move {
            let Ok(method) = js_sys::Reflect::get(&inner, &"filterAuthorizedFetch".into()) else {
                return ids;
            };
            let Ok(func): Result<js_sys::Function, _> = method.dyn_into() else {
                return ids;
            };

            let js_ids = Array::new();
            for id in &ids {
                js_ids.push(&Uint8Array::from(id.as_bytes().as_slice()));
            }

            let Ok(result) = func.call2(&inner, &peer_bytes(peer), &js_ids) else {
                return ids;
            };

            let Ok(promise): Result<Promise, _> = result.dyn_into() else {
                return ids;
            };

            let Ok(resolved) = JsFuture::from(promise).await else {
                return ids;
            };

            let Ok(arr): Result<Array, _> = resolved.dyn_into() else {
                return ids;
            };

            arr.iter()
                .filter_map(|item| {
                    let bytes: Uint8Array = item.dyn_into().ok()?;
                    let vec = bytes.to_vec();
                    let arr: [u8; 32] = vec.try_into().ok()?;
                    Some(SedimentreeId::new(arr))
                })
                .collect()
        }
        .boxed_local()
    }
}
