//! Wasm [`RemoteHeadsObserver`] that forwards notifications to a JS callback.

use sedimentree_core::id::SedimentreeId;
use sedimentree_wasm::{digest::WasmDigest, sedimentree_id::WasmSedimentreeId};
use subduction_core::{
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
};
use wasm_bindgen::JsValue;

use crate::peer_id::WasmPeerId;

/// A [`RemoteHeadsObserver`] that optionally calls a JavaScript function.
///
/// When a callback is set, it is invoked with `(sedimentreeId, peerId, heads)`
/// where `heads` is an array of `Digest` objects.
#[derive(Clone, Default)]
pub struct JsRemoteHeadsObserver {
    callback: Option<js_sys::Function>,
}

impl JsRemoteHeadsObserver {
    /// Create a new observer with no callback (notifications are discarded).
    pub fn new() -> Self {
        Self { callback: None }
    }

    /// Create a new observer wrapping a JS callback function.
    pub fn with_callback(callback: js_sys::Function) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

impl core::fmt::Debug for JsRemoteHeadsObserver {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("JsRemoteHeadsObserver")
            .field("has_callback", &self.callback.is_some())
            .finish()
    }
}

impl RemoteHeadsObserver for JsRemoteHeadsObserver {
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        let Some(ref callback) = self.callback else {
            return;
        };
        let js_id = WasmSedimentreeId::from(id);
        let js_peer = WasmPeerId::from(peer);
        let js_heads = heads
            .heads
            .into_iter()
            .map(|d| JsValue::from(WasmDigest::from(d)))
            .collect::<js_sys::Array>();

        if let Err(e) = callback.call3(
            &JsValue::NULL,
            &JsValue::from(js_id),
            &JsValue::from(js_peer),
            &js_heads,
        ) {
            tracing::warn!("remote heads callback error: {e:?}");
        }
    }
}
