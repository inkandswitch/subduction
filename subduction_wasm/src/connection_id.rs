//! Type safe connection ID.

use subduction_core::connection::id::ConnectionId;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around the Rust `ConnectionId` type.
#[wasm_bindgen(js_name = ConnectionId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmConnectionId(ConnectionId);

impl From<ConnectionId> for WasmConnectionId {
    fn from(id: ConnectionId) -> Self {
        Self(id)
    }
}

impl From<WasmConnectionId> for ConnectionId {
    fn from(id: WasmConnectionId) -> Self {
        id.0
    }
}
