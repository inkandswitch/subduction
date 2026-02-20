//! JS [`WebSocket`] connection implementation for Subduction.

use alloc::{
    boxed::Box,
    rc::Rc,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::{
    cell::RefCell,
    convert::Infallible,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use sedimentree_core::collections::Map;

use async_lock::Mutex;
use future_form::Local;
use futures::{
    FutureExt,
    channel::oneshot::{self, Canceled},
    future::LocalBoxFuture,
};
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    peer::id::PeerId,
};
use thiserror::Error;
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    BinaryType, Event, MessageEvent, Url, WebSocket,
    js_sys::{self, Promise},
};

use super::{
    WasmBatchSyncRequest, WasmBatchSyncResponse, WasmRequestId, handshake::WasmWebSocketHandshake,
    message::WasmMessage,
};
use crate::{error::WasmHandshakeError, peer_id::WasmPeerId, signer::JsSigner};
use subduction_core::connection::{
    authenticated::Authenticated,
    handshake::{self, Audience},
};

/// A WebSocket connection with internal wiring for [`Subduction`] message handling.
#[wasm_bindgen(js_name = SubductionWebSocket)]
#[derive(Debug, Clone)]
pub struct WasmWebSocket {
    peer_id: PeerId,
    timeout_ms: u32,

    request_id_counter: Arc<AtomicU64>,
    socket: WebSocket,

    pending: Arc<Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: async_channel::Receiver<Message>,
}

impl WasmWebSocket {
    /// Synchronous setup for an already-open WebSocket.
    ///
    /// This sets up message handlers and returns a `WasmWebSocket`.
    /// The WebSocket MUST be in OPEN state.
    fn setup_open_socket(ws: WebSocket, peer_id: PeerId, timeout_ms: u32) -> Self {
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Message>(64);

        let pending = Arc::new(Mutex::new(Map::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let closure_pending = pending.clone();

        let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            tracing::debug!("WS message event received");
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                tracing::debug!("WS received {} bytes", bytes.len());
                match minicbor::decode::<Message>(&bytes) {
                    Ok(msg) => {
                        tracing::trace!("WS message decoded: {} bytes", bytes.len());
                        let inner_pending = closure_pending.clone();
                        let inner_inbound_writer = inbound_writer.clone();

                        wasm_bindgen_futures::spawn_local(async move {
                            match msg {
                                Message::BatchSyncResponse(resp) => {
                                    tracing::debug!(
                                        "BatchSyncResponse received: {:?}",
                                        resp.result
                                    );
                                    let req_id = resp.req_id;
                                    let removed = { inner_pending.lock().await.remove(&req_id) };
                                    if let Some(waiting) = removed {
                                        tracing::trace!("dispatching to waiter {:?}", req_id);
                                        let result = waiting.send(resp);
                                        debug_assert!(result.is_ok());
                                        if result.is_err() {
                                            tracing::error!(
                                                "oneshot channel closed before sending response for req_id {:?}",
                                                req_id
                                            );
                                        }
                                    } else {
                                        tracing::trace!(
                                            "dispatching to inbound channel {:?}",
                                            resp.req_id
                                        );
                                        if let Err(e) = inner_inbound_writer
                                            .clone()
                                            .send(Message::BatchSyncResponse(resp))
                                            .await
                                        {
                                            tracing::error!("failed to send inbound message: {e}");
                                        }
                                    }
                                }
                                other @ (Message::LooseCommit { .. }
                                | Message::Fragment { .. }
                                | Message::BlobsRequest { .. }
                                | Message::BlobsResponse { .. }
                                | Message::BatchSyncRequest(_)
                                | Message::RemoveSubscriptions(_)
                                | Message::DataRequestRejected(_)) => {
                                    tracing::debug!("other message type received");
                                    if let Err(e) = inner_inbound_writer.clone().send(other).await {
                                        tracing::error!("failed to send inbound message: {e}");
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(
                            "CBOR decode failed for {} byte message: {:?}. First 64 bytes: {:?}",
                            bytes.len(),
                            e,
                            bytes.get(..64).unwrap_or(&bytes)
                        );
                    }
                }
            } else {
                tracing::error!(
                    "unexpected message event (not ArrayBuffer): {:?}",
                    event.data()
                );
            }
        });

        let onclose = Closure::<dyn FnMut(_)>::new(move |event: Event| {
            tracing::warn!("WebSocket connection closed: {:?}", event);
        });

        ws.set_binary_type(BinaryType::Arraybuffer);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        onmessage.forget();
        onclose.forget();

        Self {
            peer_id,
            timeout_ms,
            request_id_counter: Arc::new(AtomicU64::new(0)),
            socket: ws,
            pending,
            inbound_reader,
        }
    }
}

#[wasm_bindgen(js_class = SubductionWebSocket)]
impl WasmWebSocket {
    /// Authenticate an existing WebSocket via handshake.
    ///
    /// This performs the Subduction handshake protocol over the provided WebSocket
    /// to establish mutual identity. The WebSocket can be in CONNECTING or OPEN state.
    ///
    /// # Arguments
    ///
    /// * `ws` - An existing WebSocket (CONNECTING or OPEN)
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake fails (signature invalid, wrong peer, etc.)
    #[wasm_bindgen]
    pub async fn setup(
        ws: &WebSocket,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedConnectionError> {
        use subduction_core::timestamp::TimestampSeconds;
        use subduction_crypto::nonce::Nonce;

        // Ensure WebSocket is ready
        let ws = Self::wait_for_open(ws.clone()).await?;

        let ws_for_setup = ws.clone();
        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            WasmWebSocketHandshake::new(ws),
            move |_handshake_transport, peer_id| {
                (
                    Self::setup_open_socket(ws_for_setup, peer_id, timeout_milliseconds),
                    (),
                )
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedConnectionError::Handshake)?;

        tracing::info!(
            "Handshake complete: authenticated peer {}",
            authenticated.peer_id()
        );

        Ok(WasmAuthenticatedWebSocket {
            inner: authenticated,
        })
    }

    /// Connect to a WebSocket server with mutual authentication via handshake.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The WebSocket connection could not be established
    /// - The handshake fails (signature invalid, wrong server, clock drift, etc.)
    #[wasm_bindgen(js_name = tryConnect)]
    pub async fn try_connect(
        address: &Url,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedConnectionError> {
        Self::connect_authenticated(address, signer, expected_peer_id, timeout_milliseconds)
            .await
            .map(|inner| WasmAuthenticatedWebSocket { inner })
    }

    /// Connect to a WebSocket server using discovery mode.
    ///
    /// This method performs a cryptographic handshake using a service name
    /// instead of a known peer ID. The server's peer ID is discovered during
    /// the handshake and returned.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `signer` - The client's signer for authentication
    /// * `timeout_milliseconds` - Request timeout in milliseconds. Defaults to 30000 (30s).
    /// * `service_name` - The service name for discovery (e.g., `localhost:8080`).
    ///   If omitted, the host is extracted from the URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The WebSocket connection could not be established
    /// - The handshake fails (signature invalid, clock drift, etc.)
    #[wasm_bindgen(js_name = tryDiscover)]
    pub async fn try_discover(
        address: &Url,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedConnectionError> {
        Self::connect_discover_authenticated(address, signer, timeout_milliseconds, service_name)
            .await
            .map(|inner| WasmAuthenticatedWebSocket { inner })
    }

    /// Connect and return an `Authenticated<WasmWebSocket, Local>`.
    ///
    /// This is used internally by `WasmSubduction::connect` to keep the
    /// `Authenticated` wrapper without exposing it through wasm-bindgen.
    pub(crate) async fn connect_authenticated(
        address: &Url,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<Authenticated<WasmWebSocket, Local>, WebSocketAuthenticatedConnectionError> {
        use subduction_core::timestamp::TimestampSeconds;
        use subduction_crypto::nonce::Nonce;

        let ws = WebSocket::new(&address.href())
            .map_err(WebSocketAuthenticatedConnectionError::SocketCreationFailed)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let ws = Self::wait_for_open(ws).await?;

        let ws_for_setup = ws.clone();
        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            WasmWebSocketHandshake::new(ws),
            move |_handshake_transport, peer_id| {
                (
                    Self::setup_open_socket(ws_for_setup, peer_id, timeout_milliseconds),
                    (),
                )
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedConnectionError::Handshake)?;

        tracing::info!(
            "Handshake complete: connected to server {}",
            authenticated.peer_id()
        );

        Ok(authenticated)
    }

    /// Connect using discovery and return an `Authenticated<WasmWebSocket, Local>`.
    ///
    /// This is used internally by `WasmSubduction::connect_discover` to keep the
    /// `Authenticated` wrapper without exposing it through wasm-bindgen.
    pub(crate) async fn connect_discover_authenticated(
        address: &Url,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<Authenticated<WasmWebSocket, Local>, WebSocketAuthenticatedConnectionError> {
        use subduction_core::timestamp::TimestampSeconds;
        use subduction_crypto::nonce::Nonce;

        let timeout_milliseconds = timeout_milliseconds.unwrap_or(30_000);
        let service_name = service_name.unwrap_or_else(|| address.host());

        let ws = WebSocket::new(&address.href())
            .map_err(WebSocketAuthenticatedConnectionError::SocketCreationFailed)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let ws = Self::wait_for_open(ws).await?;

        let ws_for_setup = ws.clone();
        let audience = Audience::discover(service_name.as_bytes());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            WasmWebSocketHandshake::new(ws),
            move |_handshake_transport, peer_id| {
                (
                    Self::setup_open_socket(ws_for_setup, peer_id, timeout_milliseconds),
                    (),
                )
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedConnectionError::Handshake)?;

        tracing::info!(
            "Discovery handshake complete: connected to server {}",
            authenticated.peer_id()
        );

        Ok(authenticated)
    }

    /// Wait for a WebSocket to reach OPEN state.
    async fn wait_for_open(
        ws: WebSocket,
    ) -> Result<WebSocket, WebSocketAuthenticatedConnectionError> {
        let (open_tx, open_rx) = oneshot::channel::<Result<(), String>>();
        let open_tx_cell = Rc::new(RefCell::new(Some(open_tx)));
        let open_tx_clone = open_tx_cell.clone();

        let onopen = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = open_tx_clone.borrow_mut().take() {
                drop(tx.send(Ok(())));
            }
        });

        let onerror_tx = open_tx_cell.clone();
        let onerror = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = onerror_tx.borrow_mut().take() {
                drop(tx.send(Err("WebSocket connection failed".into())));
            }
        });

        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        // Handle case where socket is already open
        if ws.ready_state() == WebSocket::OPEN
            && let Some(tx) = open_tx_cell.borrow_mut().take()
        {
            drop(tx.send(Ok(())));
        }

        // Wait for open or error
        open_rx
            .await
            .map_err(|_| WebSocketAuthenticatedConnectionError::Canceled)?
            .map_err(WebSocketAuthenticatedConnectionError::ConnectionFailed)?;

        // Clear temporary handlers
        ws.set_onopen(None);
        ws.set_onerror(None);
        drop(onopen);
        drop(onerror);

        Ok(ws)
    }

    /// Get the peer ID of the remote peer.
    #[must_use]
    #[wasm_bindgen(js_name = peerId)]
    pub fn wasm_peer_id(&self) -> WasmPeerId {
        self.peer_id.into()
    }

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(js_name = disconnect)]
    pub async fn wasm_disconnect(&self) {
        match self.disconnect().await {
            Ok(()) => (),
            Err(_infallible) => {}
        }
    }

    /// Send a message.
    ///
    /// # Errors
    ///
    /// Returns [`WasmSendError`] if the message could not be sent over the WebSocket.
    #[wasm_bindgen(js_name = send)]
    pub async fn wasm_send(&self, wasm_message: WasmMessage) -> Result<(), WasmSendError> {
        let msg: Message = wasm_message.into();
        self.send(&msg).await?;
        Ok(())
    }

    /// Receive a message.
    ///
    /// # Errors
    ///
    /// Returns [`ReadFromClosedChannel`] if the channel has been closed.
    #[wasm_bindgen(js_name = recv)]
    pub async fn wasm_recv(&self) -> Result<WasmMessage, ReadFromClosedChannel> {
        let msg = self.recv().await?;
        Ok(msg.into())
    }

    /// Get the next request ID.
    #[wasm_bindgen(js_name = nextRequestId)]
    pub async fn wasm_next_request_id(&self) -> WasmRequestId {
        self.next_request_id().await.into()
    }

    /// Make a synchronous call to the peer.
    ///
    /// # Errors
    ///
    /// Returns [`WasmCallError`] if the call fails or times out.
    #[wasm_bindgen(js_name = call)]
    pub async fn wasm_call(
        &self,
        request: WasmBatchSyncRequest,
        timeout_ms: Option<f64>,
    ) -> Result<WasmBatchSyncResponse, WasmCallError> {
        let optional_duration = timeout_ms.map(|f64_ms| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Duration::from_millis(f64_ms as u64)
        });
        self.call(request.into(), optional_duration)
            .await
            .map(Into::into)
            .map_err(Into::into)
    }
}

impl PartialEq for WasmWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.socket == other.socket
    }
}

impl Connection<Local> for WasmWebSocket {
    type SendError = SendError;
    type RecvError = ReadFromClosedChannel;
    type CallError = CallError;
    type DisconnectionError = Infallible;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        let counter = self.request_id_counter.clone();
        async move {
            let counter = counter.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("generated message id {:?}", counter);
            RequestId {
                requestor: self.peer_id,
                nonce: counter,
            }
        }
        .boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(&self, message: &Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let request_id = message.request_id();

        #[allow(clippy::expect_used)]
        let msg_bytes = minicbor::to_vec(message).expect("serialization should be infallible");

        async move {
            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(SendError::SocketSend)?;

            tracing::debug!("sent outbound message id {:?}", request_id);

            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("waiting for inbound message");
            let msg = self
                .inbound_reader
                .recv()
                .await
                .map_err(|_| ReadFromClosedChannel)?;
            tracing::info!("received inbound message id {:?}", msg.request_id());
            Ok(msg)
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            {
                self.pending.lock().await.insert(req_id, tx);
            }

            #[allow(clippy::expect_used)]
            let msg_bytes = minicbor::to_vec(Message::BatchSyncRequest(req))
                .expect("serialization should be infallible");

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(CallError::SocketSend)?;

            tracing::info!("sent request {:?}", req_id);

            let req_timeout =
                override_timeout.unwrap_or(Duration::from_millis(self.timeout_ms.into()));

            // await response with timeout & cleanup
            match timeout(req_timeout, rx).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    Err(CallError::ChannelCanceled)
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    Err(CallError::TimedOut)
                }
            }
        }
        .boxed_local()
    }
}

// NOTE: Reconnect is not implemented for WasmWebSocket because handshake
// requires the signer, which is not stored in the struct. Applications should
// handle reconnection by calling connect again.

#[derive(Debug)]
struct WasmTimeout {
    id: JsValue, // Numeric in browsers, special Timeout type in e.g. Deno.
    // Keep the closure alive so the timer can call it
    _closure: Option<Closure<dyn FnMut()>>,
}

impl WasmTimeout {
    /// Creates a Promise that resolves after `ms` and a handle you can cancel.
    fn new(ms: i32) -> (JsFuture, Self) {
        let mut out_id: JsValue = JsValue::UNDEFINED;
        let mut out_closure: Option<Closure<dyn FnMut()>> = None;

        let promise = Promise::new(&mut |resolve, reject| {
            // NOTE this `global` strategy looks ugly,
            // BUT it abstracts over both `window` and `worker` contexts.
            let global = js_sys::global();

            let js_value = match js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout")) {
                Ok(v) => v,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            let set_timeout = match js_value.dyn_into::<js_sys::Function>() {
                Ok(set_timeout) => set_timeout,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            let callback_reject = reject.clone();
            let callback = Closure::wrap(Box::new(move || match resolve.call0(&JsValue::NULL) {
                Err(e) => {
                    drop(callback_reject.call1(&JsValue::NULL, &e));
                }
                Ok(_val) => (),
            }) as Box<dyn FnMut()>);

            out_id = match set_timeout.call2(
                &global,
                callback.as_ref().unchecked_ref(),
                &JsValue::from(ms),
            ) {
                Ok(id) => id,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            out_closure = Some(callback);
        });

        (
            JsFuture::from(promise),
            WasmTimeout {
                id: out_id,
                _closure: out_closure,
            },
        )
    }

    /// Cancel the timer (prevents the callback from firing).
    fn cancel(self) {
        let global = js_sys::global();
        if let Ok(clear_timeout) = js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
            .and_then(JsCast::dyn_into::<js_sys::Function>)
            && let Err(e) = clear_timeout.call1(&global, &self.id)
        {
            tracing::error!("failed to clear timeout: {:?}", e);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    let ms = dur.as_millis().try_into().unwrap_or(i32::MAX);
    let (sleep, handle) = WasmTimeout::new(ms);
    match futures_util::future::select(fut, sleep).await {
        futures_util::future::Either::Left((val, _sleep_future)) => {
            handle.cancel();
            Ok(val)
        }
        futures_util::future::Either::Right((_done, _fut)) => Err(TimedOut),
    }
}

/// Problem while sending a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),
}

/// Attempted to read from a closed channel.
#[derive(Debug, Clone, Copy, Error)]
#[error("Attempted to read from closed channel")]
pub struct ReadFromClosedChannel;

impl From<ReadFromClosedChannel> for JsValue {
    fn from(err: ReadFromClosedChannel) -> JsValue {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ReadFromClosedChannel");
        js_err.into()
    }
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Clone, Error)]
pub enum CallError {
    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),

    /// Tried to read from a canceled channel.
    #[error("Channel canceled")]
    ChannelCanceled,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    TimedOut,
}

/// Problem while attempting to connect or reconnect the WebSocket.
#[derive(Debug, Clone, Error)]
pub enum WebSocketConnectionError {
    /// Problem creating the WebSocket.
    #[error("WebSocket creation failed: {0:?}")]
    SocketCreationFailed(JsValue),

    /// Problem creating the URL.
    #[error("invalid URL: {0:?}")]
    InvalidUrl(JsValue),

    /// WebSocket setup was canceled.
    #[error(transparent)]
    WasmSetupCanceled(#[from] WasmWebSocketSetupCanceled),
}

impl From<WebSocketConnectionError> for JsValue {
    fn from(err: WebSocketConnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WebSocketConnectionError");
        err.into()
    }
}

/// Error connecting to a WebSocket server with handshake authentication.
#[derive(Debug, Error)]
pub enum WebSocketAuthenticatedConnectionError {
    /// Problem creating the WebSocket.
    #[error("WebSocket creation failed: {0:?}")]
    SocketCreationFailed(JsValue),

    /// WebSocket connection failed.
    #[error("connection failed: {0}")]
    ConnectionFailed(alloc::string::String),

    /// Connection was canceled.
    #[error("connection canceled")]
    Canceled,

    /// Handshake failed.
    #[error("handshake failed: {0}")]
    Handshake(#[from] handshake::AuthenticateError<WasmHandshakeError>),

    /// WebSocket setup failed after handshake.
    #[error("setup failed: {0}")]
    Setup(#[from] WasmWebSocketSetupCanceled),
}

impl From<WebSocketAuthenticatedConnectionError> for JsValue {
    fn from(err: WebSocketAuthenticatedConnectionError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("WebSocketAuthenticatedConnectionError");
        js_err.into()
    }
}

/// WebSocket setup was canceled.
#[derive(Debug, Clone, Error)]
#[error("WebSocket setup was canceled")]
#[allow(missing_copy_implementations)]
pub struct WasmWebSocketSetupCanceled(Canceled);

impl From<Canceled> for WasmWebSocketSetupCanceled {
    fn from(err: Canceled) -> Self {
        Self(err)
    }
}

impl From<WasmWebSocketSetupCanceled> for JsValue {
    fn from(err: WasmWebSocketSetupCanceled) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CancelWebSocketSetup");
        js_err.into()
    }
}

/// An error that occurred while sending a message over a WebSocket.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmSendError(#[from] SendError);

impl From<WasmSendError> for JsValue {
    fn from(err: WasmSendError) -> JsValue {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("SendError");
        js_err.into()
    }
}

/// An error that occurred during a synchronous call over a WebSocket.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmCallError(#[from] CallError);

impl From<WasmCallError> for JsValue {
    fn from(err: WasmCallError) -> JsValue {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CallError");
        js_err.into()
    }
}

/// An authenticated WebSocket connection.
///
/// This wrapper proves that the connection has completed the Subduction handshake
/// and the peer identity has been cryptographically verified.
///
/// Obtain via [`SubductionWebSocket::setup`], [`SubductionWebSocket::tryConnect`],
/// or [`SubductionWebSocket::tryDiscover`].
#[wasm_bindgen(js_name = AuthenticatedWebSocket)]
#[derive(Debug)]
pub struct WasmAuthenticatedWebSocket {
    inner: Authenticated<WasmWebSocket, Local>,
}

impl WasmAuthenticatedWebSocket {
    /// Access the inner `Authenticated` connection.
    pub(crate) fn inner(&self) -> &Authenticated<WasmWebSocket, Local> {
        &self.inner
    }
}

#[wasm_bindgen(js_class = AuthenticatedWebSocket)]
impl WasmAuthenticatedWebSocket {
    /// The verified peer identity.
    #[must_use]
    #[wasm_bindgen(getter, js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        self.inner.peer_id().into()
    }
}
