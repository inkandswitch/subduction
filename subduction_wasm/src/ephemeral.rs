//! Wasm ephemeral message observer that forwards events to a JS callback.

use sedimentree_core::id::SedimentreeId;
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;
use subduction_core::peer::id::PeerId;
use wasm_bindgen::JsValue;

use crate::peer_id::WasmPeerId;

/// Forwards inbound [`EphemeralEvent`]s to a JavaScript callback.
///
/// The JS callback is invoked with `(sedimentreeId, senderId, payload)` where
/// `payload` is a `Uint8Array`.
///
/// [`EphemeralEvent`]: subduction_ephemeral::config::EphemeralEvent
#[derive(Clone, Debug)]
pub struct JsEphemeralObserver {
    callback: js_sys::Function,
}

impl JsEphemeralObserver {
    /// Create a new observer wrapping a JS callback function.
    #[must_use]
    pub fn new(callback: js_sys::Function) -> Self {
        Self { callback }
    }

    /// Forward an ephemeral event to the JS callback.
    pub fn on_event(&self, id: SedimentreeId, sender: PeerId, payload: &[u8]) {
        let js_id = WasmSedimentreeId::from(id);
        let js_sender = WasmPeerId::from(sender);
        let js_payload = js_sys::Uint8Array::from(payload);

        if let Err(e) = self.callback.call3(
            &JsValue::NULL,
            &JsValue::from(js_id),
            &JsValue::from(js_sender),
            &js_payload,
        ) {
            tracing::warn!("ephemeral callback error: {e:?}");
        }
    }
}
