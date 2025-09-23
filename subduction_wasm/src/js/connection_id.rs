use subduction_core::connection::id::ConnectionId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = ConnectionId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsConnectionId(ConnectionId);

impl From<ConnectionId> for JsConnectionId {
    fn from(id: ConnectionId) -> Self {
        Self(id)
    }
}

impl From<JsConnectionId> for ConnectionId {
    fn from(id: JsConnectionId) -> Self {
        id.0
    }
}
