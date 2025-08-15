use sedimentree_sync_core::message::Message;
use wasm_bindgen::prelude::*;

use super::peer::id::JsPeerId;

#[wasm_bindgen(js_name = "Message")]
pub struct JsMessage(Message);

#[wasm_bindgen(js_class = "Message")]
impl JsMessage {
    #[wasm_bindgen(constructor)]
    pub fn new(action: &str, sender_id: JsPeerId, target_id: JsPeerId) -> Self {
        web_sys::console::log_1(
            &format!(
                "Creating Message with action: {}, senderId: {}, targetId: {}",
                action, sender_id, target_id
            )
            .into(),
        );

        Self(Message {
            action: action.to_string(),
            sender_id: sender_id.into(),
            target_id: target_id.into(),
        })
    }

    #[wasm_bindgen(getter, js_name = "type")]
    pub fn action(&self) -> String {
        self.0.action.clone()
    }

    #[wasm_bindgen(getter, js_name = "senderId")]
    pub fn sender_id(&self) -> String {
        self.0.sender_id.to_string()
    }

    #[wasm_bindgen(getter, js_name = "targetId")]
    pub fn target_id(&self) -> String {
        self.0.target_id.to_string()
    }
}
