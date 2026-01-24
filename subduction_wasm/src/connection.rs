//! JS [`Connection`] interface for Subduction.

mod handshake;
pub mod message;
pub mod nonce;
pub mod websocket;

use alloc::string::ToString;
use core::time::Duration;
use wasm_refgen::wasm_refgen;

use futures::{FutureExt, future::LocalBoxFuture};
use futures_kind::Local;
use js_sys::Promise;
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    peer::id::PeerId,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{peer_id::WasmPeerId, sedimentree_id::WasmSedimentreeId};

pub use message::{JsMessage, WasmMessage};
pub use nonce::{WasmNonce, WasmNonceError};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Connection {
    peerId(): PeerId;
    disconnect(): Promise<void>;
    send(message: Message): Promise<void>;
    recv(): Promise<Message>;
    nextRequestId(): Promise<RequestId>;
    call(request: BatchSyncRequest, timeoutMs: number | null): Promise<BatchSyncResponse>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed connection interface.
    #[wasm_bindgen(js_name = Connection, typescript_type = "Connection")]
    pub type JsConnection;

    /// Get the peer ID of the remote peer.
    #[wasm_bindgen(method, js_name = peerId)]
    fn js_peer_id(this: &JsConnection) -> WasmPeerId;

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(method, js_name = disconnect)]
    fn js_disconnect(this: &JsConnection) -> Promise;

    /// Send a message.
    #[wasm_bindgen(method, js_name = send)]
    fn js_send(this: &JsConnection, message: WasmMessage) -> Promise;

    /// Receive a message.
    #[wasm_bindgen(method, js_name = recv)]
    fn js_recv(this: &JsConnection) -> Promise;

    /// Get the next request ID.
    #[wasm_bindgen(method, js_name = nextRequestId)]
    fn js_next_request_id(this: &JsConnection) -> Promise;

    /// Make a synchronous call to the peer.
    #[wasm_bindgen(method, js_name = call)]
    fn js_call(
        this: &JsConnection,
        request: WasmBatchSyncRequest,
        timeout_ms: Option<f64>,
    ) -> Promise;
}

impl core::fmt::Debug for JsConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsConnection").finish()
    }
}

impl Clone for JsConnection {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}

impl PartialEq for JsConnection {
    fn eq(&self, other: &Self) -> bool {
        JsValue::from(self) == JsValue::from(other)
    }
}

impl Connection<Local> for JsConnection {
    type DisconnectionError = JsConnectionError;
    type SendError = JsConnectionError;
    type RecvError = JsConnectionError;
    type CallError = JsConnectionError;

    fn peer_id(&self) -> PeerId {
        self.js_peer_id().into()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            JsFuture::from(self.js_disconnect())
                .await
                .map_err(JsConnectionError::Disconnect)?;
            Ok(())
        }
        .boxed_local()
    }

    fn send(&self, message: &Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let wasm_msg = WasmMessage::from(message.clone());
        async move {
            JsFuture::from(self.js_send(wasm_msg))
                .await
                .map_err(JsConnectionError::Send)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async move {
            let js_value = JsFuture::from(self.js_recv())
                .await
                .map_err(JsConnectionError::Recv)?;
            let js_msg: JsMessage = JsCast::unchecked_into(js_value);
            let wasm_msg = WasmMessage::from(&js_msg);
            Ok(wasm_msg.into())
        }
        .boxed_local()
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        async move {
            #[allow(clippy::expect_used)]
            let js_value = JsFuture::from(self.js_next_request_id())
                .await
                .expect("nextRequestId should not fail");
            let js_req_id: JsRequestId = js_value.unchecked_into();
            let wasm_req_id = WasmRequestId::from(&js_req_id);
            wasm_req_id.into()
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            let wasm_req = WasmBatchSyncRequest::from(req);
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            let timeout_ms = timeout.map(|d| d.as_millis() as f64);
            let js_value = JsFuture::from(self.js_call(wasm_req, timeout_ms))
                .await
                .map_err(JsConnectionError::Call)?;
            let js_resp: JsBatchSyncResponse = JsCast::unchecked_into(js_value);
            let wasm_resp = WasmBatchSyncResponse::from(&js_resp);
            Ok(wasm_resp.into())
        }
        .boxed_local()
    }
}

/// Errors from the JS connection.
#[derive(Error, Debug, Clone)]
pub enum JsConnectionError {
    /// An error that occurred while disconnecting.
    #[error("Disconnect error")]
    Disconnect(JsValue),

    /// An error that occurred while sending a message.
    #[error("Send error")]
    Send(JsValue),

    /// An error that occurred while receiving a message.
    #[error("Recv error")]
    Recv(JsValue),

    /// An error that occurred during a synchronous call.
    #[error("Call error")]
    Call(JsValue),
}

impl From<JsConnectionError> for JsValue {
    fn from(err: JsConnectionError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ConnectionError");
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
