//! JS [`Transport`] interface for Subduction.

pub mod authenticated;
pub mod fetch_client;
pub mod longpoll;
pub mod message;
pub mod message_port;
pub mod nonce;
pub mod websocket;

use alloc::{format, rc::Rc, string::ToString, vec::Vec};
use core::cell::RefCell;
use wasm_refgen::wasm_refgen;

use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use js_sys::{self, Promise};
use subduction_core::{
    connection::message::{BatchSyncRequest, BatchSyncResponse, RequestId},
    handshake::Handshake,
    transport::Transport,
};
use thiserror::Error;
use wasm_bindgen::{closure::Closure, prelude::*};
use wasm_bindgen_futures::JsFuture;

use crate::peer_id::WasmPeerId;

use self::nonce::WasmNonce;
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;

/// Shared, optional disconnect callback.
///
/// Stored as a [`Closure`] so it is properly dropped (not leaked) when the
/// connection is cleaned up or a new callback is registered.
#[derive(Debug, Default, Clone)]
pub(crate) struct OnDisconnect(
    #[allow(clippy::type_complexity)] Rc<RefCell<Option<Closure<dyn Fn()>>>>,
);

impl OnDisconnect {
    /// Store a disconnect callback.
    pub(crate) fn set(&self, closure: Closure<dyn Fn()>) {
        *self.0.borrow_mut() = Some(closure);
    }

    /// Take and invoke the callback (if any).
    ///
    /// The closure is consumed so it cannot fire twice, and the borrow is
    /// released before calling into JS to avoid re-entrant `RefCell` panics.
    pub(crate) fn take_and_fire(&self) {
        let closure = self.0.borrow_mut().take();
        if let Some(closure) = closure {
            let func: &js_sys::Function = closure.as_ref().unchecked_ref();
            if let Err(e) = func.call0(&JsValue::NULL) {
                tracing::error!("onDisconnect callback threw: {e:?}");
            }
        }
    }
}

/// Default service name for local (non-network) discovery handshakes.
///
/// Used by [`connectTransport`](crate::subduction::WasmSubduction::connect_transport)
/// and [`acceptTransport`](crate::subduction::WasmSubduction::accept_transport) when
/// no explicit service name is provided.
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
    /// Byte-oriented transport interface.
    #[wasm_bindgen(js_name = Transport, typescript_type = "Transport")]
    pub type JsTransport;

    /// Send raw bytes over the transport.
    #[wasm_bindgen(method, js_name = sendBytes)]
    fn js_send_bytes(this: &JsTransport, bytes: &js_sys::Uint8Array) -> Promise;

    /// Receive the next message frame as raw bytes.
    #[wasm_bindgen(method, js_name = recvBytes)]
    fn js_recv_bytes(this: &JsTransport) -> Promise;

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(method, js_name = disconnect)]
    fn js_disconnect(this: &JsTransport) -> Promise;

    /// Register a callback to be invoked when the transport disconnects.
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
        let uint8_array = js_sys::Uint8Array::from(bytes);
        async move {
            JsFuture::from(self.js_send_bytes(&uint8_array))
                .await
                .map_err(JsTransportError::Send)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async move {
            let js_value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(JsTransportError::Recv)?;
            let uint8_array = js_sys::Uint8Array::new(&js_value);
            Ok(uint8_array.to_vec())
        }
        .boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            JsFuture::from(self.js_disconnect())
                .await
                .map_err(JsTransportError::Disconnect)?;
            Ok(())
        }
        .boxed_local()
    }
}

impl Handshake<Local> for JsTransport {
    type Error = crate::error::WasmHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let array = js_sys::Uint8Array::from(bytes.as_slice());
        async move {
            JsFuture::from(self.js_send_bytes(&array))
                .await
                .map_err(|e| crate::error::WasmHandshakeError::WebSocket(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        async move {
            let value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(|e| crate::error::WasmHandshakeError::WebSocket(format!("{e:?}")))?;

            let array: js_sys::Uint8Array = value.dyn_into().map_err(|v| {
                crate::error::WasmHandshakeError::WebSocket(format!(
                    "expected Uint8Array, got {v:?}"
                ))
            })?;

            Ok(array.to_vec())
        }
        .boxed_local()
    }
}

// Request-response multiplexing is handled by `ManagedConnection`,
// which pairs the connection with a `Multiplexer` and routes responses
// in the `Subduction` listen loop.

/// Errors from the JS transport.
#[derive(Error, Debug, Clone)]
pub enum JsTransportError {
    /// An error that occurred while disconnecting.
    #[error("Disconnect error")]
    Disconnect(JsValue),

    /// An error that occurred while sending bytes.
    #[error("Send error")]
    Send(JsValue),

    /// An error that occurred while receiving bytes.
    #[error("Recv error")]
    Recv(JsValue),
}

impl From<JsTransportError> for JsValue {
    fn from(err: JsTransportError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("TransportError");
        js_err.into()
    }
}

/// Wasm wrapper for [`RequestId`].
#[wasm_bindgen(js_name = RequestId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmRequestId(RequestId);

#[wasm_refgen(js_ref = JsRequestId)]
#[wasm_bindgen(js_class = RequestId)]
impl WasmRequestId {
    /// Create a new [`RequestId`] from a requestor peer ID and nonce.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(requestor: &WasmPeerId, nonce: &WasmNonce) -> Self {
        RequestId {
            requestor: requestor.clone().into(),
            nonce: nonce.0,
        }
        .into()
    }

    /// The peer ID of the requestor.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn requestor(&self) -> WasmPeerId {
        self.0.requestor.into()
    }

    /// The request nonce.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn nonce(&self) -> WasmNonce {
        WasmNonce(self.0.nonce)
    }
}

impl From<RequestId> for WasmRequestId {
    fn from(id: RequestId) -> Self {
        Self(id)
    }
}

impl From<WasmRequestId> for RequestId {
    fn from(id: WasmRequestId) -> Self {
        id.0
    }
}

/// Wasm wrapper for [`BatchSyncRequest`].
#[wasm_bindgen(js_name = BatchSyncRequest)]
#[derive(Debug, Clone)]
pub struct WasmBatchSyncRequest(BatchSyncRequest);

#[wasm_bindgen(js_class = BatchSyncRequest)]
impl WasmBatchSyncRequest {
    /// The sedimentree ID this request corresponds to.
    #[must_use]
    pub fn id(&self) -> WasmSedimentreeId {
        self.0.id.into()
    }

    /// The request ID for this request.
    #[must_use]
    pub fn request_id(&self) -> WasmRequestId {
        self.0.req_id.into()
    }

    /// Whether this request subscribes to future updates.
    #[must_use]
    pub fn subscribe(&self) -> bool {
        self.0.subscribe
    }
}

impl From<BatchSyncRequest> for WasmBatchSyncRequest {
    fn from(req: BatchSyncRequest) -> Self {
        Self(req)
    }
}

impl From<WasmBatchSyncRequest> for BatchSyncRequest {
    fn from(req: WasmBatchSyncRequest) -> Self {
        req.0
    }
}

/// Wasm wrapper for [`BatchSyncResponse`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = BatchSyncResponse)]
pub struct WasmBatchSyncResponse(BatchSyncResponse);

#[wasm_bindgen(js_class = BatchSyncResponse)]
impl WasmBatchSyncResponse {
    /// The sedimentree ID this response corresponds to.
    #[must_use]
    pub fn id(&self) -> WasmSedimentreeId {
        self.0.id.into()
    }

    /// The request ID this response corresponds to.
    #[must_use]
    pub fn request_id(&self) -> WasmRequestId {
        self.0.req_id.into()
    }
}

#[wasm_refgen(js_ref = JsBatchSyncResponse)]
#[wasm_bindgen(js_class = BatchSyncResponse)]
impl WasmBatchSyncResponse {}

impl From<BatchSyncResponse> for WasmBatchSyncResponse {
    fn from(resp: BatchSyncResponse) -> Self {
        Self(resp)
    }
}

impl From<WasmBatchSyncResponse> for BatchSyncResponse {
    fn from(resp: WasmBatchSyncResponse) -> Self {
        resp.0
    }
}

pub use authenticated::WasmAuthenticatedTransport;
