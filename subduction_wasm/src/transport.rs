//! Structural JavaScript byte transport and its core trait implementations.
pub mod authenticated;
pub mod fetch_client;
pub mod longpoll;
pub mod message_port;
mod on_disconnect;
pub mod websocket;

use crate::error::WasmHandshakeError;
use alloc::{string::ToString, vec::Vec};
use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use js_sys::Promise;
pub(crate) use on_disconnect::OnDisconnect;
use subduction_core::{handshake::Handshake, transport::Transport};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

/// Default discovery service for local `MessageChannel` connections.
pub const DEFAULT_LOCAL_SERVICE_NAME: &str = "subduction:local";

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Transport {
    sendBytes(bytes: Uint8Array): Promise<void>;
    recvBytes(): Promise<Uint8Array>;
    disconnect(): Promise<void>;
    onDisconnect(callback: () => void): void;
}
"#;
#[wasm_bindgen]
extern "C" {
    /// Caller-provided byte transport service.
    #[wasm_bindgen(js_name = Transport, typescript_type = "Transport")]
    pub type JsTransport;
    #[wasm_bindgen(method, catch, js_name = sendBytes)]
    fn js_send_bytes(this: &JsTransport, bytes: &js_sys::Uint8Array) -> Result<Promise, JsValue>;
    #[wasm_bindgen(method, catch, js_name = recvBytes)]
    fn js_recv_bytes(this: &JsTransport) -> Result<Promise, JsValue>;
    #[wasm_bindgen(method, catch, js_name = disconnect)]
    fn js_disconnect(this: &JsTransport) -> Result<Promise, JsValue>;
    /// Install the node's disconnect observer.
    #[wasm_bindgen(method, js_name = onDisconnect)]
    pub fn js_on_disconnect(this: &JsTransport, callback: &js_sys::Function);
}
impl core::fmt::Debug for JsTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsTransport").finish()
    }
}
impl Clone for JsTransport {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}
impl PartialEq for JsTransport {
    fn eq(&self, other: &Self) -> bool {
        JsValue::from(self) == JsValue::from(other)
    }
}
impl Transport<Local> for JsTransport {
    type SendError = JsTransportError;
    type RecvError = JsTransportError;
    type DisconnectionError = JsTransportError;
    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let bytes = js_sys::Uint8Array::from(bytes);
        async move {
            JsFuture::from(self.js_send_bytes(&bytes).map_err(JsTransportError::Send)?)
                .await
                .map_err(JsTransportError::Send)?;
            Ok(())
        }
        .boxed_local()
    }
    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async move {
            let value = JsFuture::from(self.js_recv_bytes().map_err(JsTransportError::Recv)?)
                .await
                .map_err(JsTransportError::Recv)?;
            Ok(js_sys::Uint8Array::new(&value).to_vec())
        }
        .boxed_local()
    }
    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            JsFuture::from(self.js_disconnect().map_err(JsTransportError::Disconnect)?)
                .await
                .map_err(JsTransportError::Disconnect)?;
            Ok(())
        }
        .boxed_local()
    }
}
impl Handshake<Local> for JsTransport {
    type Error = WasmHandshakeError;
    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let array = js_sys::Uint8Array::from(bytes.as_slice());
        async move {
            JsFuture::from(
                self.js_send_bytes(&array)
                    .map_err(|e| WasmHandshakeError::Transport(e.into()))?,
            )
            .await
            .map_err(|e| WasmHandshakeError::Transport(e.into()))?;
            Ok(())
        }
        .boxed_local()
    }
    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        async move {
            let value = JsFuture::from(
                self.js_recv_bytes()
                    .map_err(|e| WasmHandshakeError::Transport(e.into()))?,
            )
            .await
            .map_err(|e| WasmHandshakeError::Transport(e.into()))?;
            let array: js_sys::Uint8Array = value
                .dyn_into()
                .map_err(|v| WasmHandshakeError::Transport(v.into()))?;
            Ok(array.to_vec())
        }
        .boxed_local()
    }
}
/// Failures from a JavaScript transport callback.
#[derive(Error, Debug, Clone)]
pub enum JsTransportError {
    /// Disconnect failed.
    #[error("Disconnect error")]
    Disconnect(JsValue),
    /// Send failed.
    #[error("Send error")]
    Send(JsValue),
    /// Receive failed.
    #[error("Recv error")]
    Recv(JsValue),
}
impl From<JsTransportError> for JsValue {
    fn from(err: JsTransportError) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name("TransportError");
        error.into()
    }
}
