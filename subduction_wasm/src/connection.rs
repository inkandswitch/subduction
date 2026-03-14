//! JS [`Connection`] interface for Subduction.

pub(crate) mod handshake;

pub mod fetch_client;
pub mod longpoll;
pub mod message;
pub mod nonce;
pub mod transport;
pub mod websocket;

use alloc::string::ToString;
use core::time::Duration;
use wasm_refgen::wasm_refgen;

use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use js_sys::{self, Promise};
use subduction_core::{
    connection::{
        Connection,
        authenticated::Authenticated,
        handshake::{self as hs, audience::Audience},
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    connection::{
        message::{JsMessage, WasmMessage},
        nonce::WasmNonce,
        transport::IdentifiedConnection,
    },
    error::WasmHandshakeError,
    peer_id::WasmPeerId,
    signer::JsSigner,
};
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Connection {
    disconnect(): Promise<void>;
    send(message: Message): Promise<void>;
    recv(): Promise<Message>;
    nextRequestId(): Promise<RequestId>;
    call(request: BatchSyncRequest, timeoutMs: number | null): Promise<BatchSyncResponse>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// Connection interface.
    #[wasm_bindgen(js_name = Connection, typescript_type = "Connection")]
    pub type JsConnection;

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
            let js_msg: JsMessage =
                js_value
                    .dyn_into()
                    .map_err(|value| JsConnectionError::UnexpectedJsType {
                        expected: "Message",
                        value,
                    })?;
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
                .expect("JsConnection.nextRequestId() promise rejected");
            #[allow(clippy::expect_used)]
            let js_req_id: JsRequestId = js_value
                .dyn_into()
                .expect("JsConnection.nextRequestId() did not return a RequestId");
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
            let js_resp: JsBatchSyncResponse =
                js_value
                    .dyn_into()
                    .map_err(|value| JsConnectionError::UnexpectedJsType {
                        expected: "BatchSyncResponse",
                        value,
                    })?;
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

    /// A JS value could not be cast to the expected type.
    #[error("JS type cast failed: expected {expected}, got {value:?}")]
    UnexpectedJsType {
        /// The type name that was expected.
        expected: &'static str,
        /// The actual JS value received.
        value: JsValue,
    },
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

/// A transport-erased authenticated connection.
///
/// Wraps an [`Authenticated<IdentifiedConnection>`] and is the common type
/// accepted by [`onboard`](crate::subduction::WasmSubduction::onboard) and
/// [`addConnection`](crate::subduction::WasmSubduction::add_connection).
///
/// # Construction
///
/// There are three ways to obtain an `AuthenticatedConnection`:
///
/// 1. **Custom transport** — implement [`HandshakeConnection`](handshake::JsHandshakeConnection)
///    (extends `Connection` with `sendBytes`/`recvBytes`) and call [`setup`](Self::setup):
///
///    ```js
///    const auth = await AuthenticatedConnection.setup(myConn, signer, peerId);
///    ```
///
/// 2. **From WebSocket** — authenticate via [`SubductionWebSocket`] then convert:
///
///    ```js
///    const wsAuth = await SubductionWebSocket.tryConnect(url, signer, peerId, timeout);
///    const auth = wsAuth.toConnection();
///    ```
///
/// 3. **From HTTP long-poll** — same pattern via [`SubductionLongPoll`]:
///
///    ```js
///    const lpAuth = await SubductionLongPoll.tryConnect(url, signer, peerId, timeout);
///    const auth = lpAuth.toConnection();
///    ```
#[wasm_bindgen(js_name = AuthenticatedConnection)]
#[derive(Debug)]
pub struct WasmAuthenticatedConnection {
    inner: Authenticated<IdentifiedConnection, Local>,
}

impl WasmAuthenticatedConnection {
    /// Access the inner `Authenticated` connection.
    pub(crate) fn inner(&self) -> &Authenticated<IdentifiedConnection, Local> {
        &self.inner
    }

    /// Construct from an `Authenticated<IdentifiedConnection>`.
    ///
    /// Used by [`WasmAuthenticatedWebSocket::to_connection`] and
    /// [`WasmAuthenticatedLongPoll::to_connection`] to wrap transport-specific
    /// authenticated connections.
    pub(crate) fn from_identified(inner: Authenticated<IdentifiedConnection, Local>) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = AuthenticatedConnection)]
impl WasmAuthenticatedConnection {
    /// Run the Subduction handshake over a custom transport, producing an
    /// authenticated connection.
    ///
    /// The `connection` object must implement both `HandshakeConnection`
    /// (for the handshake phase) and `Connection` (for post-handshake
    /// communication). The same object is used for both phases.
    ///
    /// # Arguments
    ///
    /// * `connection` - A `HandshakeConnection` (extends `Connection`)
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn setup(
        connection: handshake::JsHandshakeConnection,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
    ) -> Result<WasmAuthenticatedConnection, WasmHandshakeError> {
        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, peer_id) = hs::initiate::<Local, _, _, _, _>(
            connection,
            |hs_conn, peer_id| {
                let transport: JsConnection = wasm_bindgen::JsValue::from(hs_conn).unchecked_into();
                (IdentifiedConnection::new(transport, peer_id), peer_id)
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Handshake complete: authenticated peer {peer_id}");

        Ok(Self {
            inner: authenticated,
        })
    }

    /// Accept an incoming handshake over a custom transport (responder side).
    ///
    /// This is the counterpart to [`setup`](Self::setup). The initiator calls
    /// `setup`, and the responder calls `accept` over the same underlying channel.
    ///
    /// # Arguments
    ///
    /// * `connection` - A `HandshakeConnection` (extends `Connection`)
    /// * `signer` - The responder's signer for authentication
    /// * `max_drift_seconds` - Maximum acceptable clock drift in seconds (default: 600)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn accept(
        connection: handshake::JsHandshakeConnection,
        signer: &JsSigner,
        max_drift_seconds: Option<u32>,
    ) -> Result<WasmAuthenticatedConnection, WasmHandshakeError> {
        use core::time::Duration;
        use subduction_core::connection::nonce_cache::NonceCache;

        let our_peer_id = signer.verifying_key().into();
        let nonce_cache = NonceCache::default();
        let max_drift = Duration::from_secs(u64::from(max_drift_seconds.unwrap_or(600)));

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);

        let (authenticated, peer_id) = hs::respond::<Local, _, _, _, _>(
            connection,
            |hs_conn, peer_id| {
                let transport: JsConnection = wasm_bindgen::JsValue::from(hs_conn).unchecked_into();
                (IdentifiedConnection::new(transport, peer_id), peer_id)
            },
            signer,
            &nonce_cache,
            our_peer_id,
            None,
            now,
            max_drift,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Handshake complete (responder): authenticated peer {peer_id}");

        Ok(Self {
            inner: authenticated,
        })
    }

    /// The verified peer identity.
    #[must_use]
    #[wasm_bindgen(getter, js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        self.inner.peer_id().into()
    }
}
