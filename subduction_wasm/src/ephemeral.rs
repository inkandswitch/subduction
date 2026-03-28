//! Wasm ephemeral message observer that forwards events to a JS callback.

use subduction_core::peer::id::PeerId;
use subduction_ephemeral::topic::Topic;
use wasm_bindgen::JsValue;

use crate::{peer_id::WasmPeerId, topic::WasmTopic};

/// Forwards inbound [`EphemeralEvent`]s to a JavaScript callback.
///
/// The JS callback is invoked with `(topic, senderId, payload)` where
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
    pub fn on_event(&self, topic: Topic, sender: PeerId, payload: &[u8]) {
        let js_topic = WasmTopic::from(topic);
        let js_sender = WasmPeerId::from(sender);
        let js_payload = js_sys::Uint8Array::from(payload);

        if let Err(e) = self.callback.call3(
            &JsValue::NULL,
            &JsValue::from(js_topic),
            &JsValue::from(js_sender),
            &js_payload,
        ) {
            tracing::warn!("ephemeral callback error: {e:?}");
        }
    }
}
