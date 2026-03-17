//! JS [`WebSocket`] transport implementation for Subduction.
//!
//! [`WasmWebSocket`] exposes the byte-oriented `Transport` interface
//! (`sendBytes`/`recvBytes`/`disconnect`) over a browser `WebSocket`.

use alloc::{
    rc::Rc,
    string::{String, ToString},
    vec::Vec,
};
use core::cell::RefCell;

use future_form::Local;
use futures::{
    FutureExt,
    channel::oneshot::{self, Canceled},
    future::LocalBoxFuture,
};
use subduction_core::{timestamp::TimestampSeconds, transport::Transport};
use subduction_crypto::nonce::Nonce;
use thiserror::Error;
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use web_sys::{BinaryType, Event, MessageEvent, Url, WebSocket, js_sys};

use crate::{error::WasmHandshakeError, peer_id::WasmPeerId, signer::JsSigner};
use subduction_core::{
    authenticated::Authenticated,
    handshake::{self, audience::Audience},
};

/// A WebSocket transport exposing the byte-oriented `Transport` interface.
///
/// Raw bytes from the WebSocket's `onmessage` handler are buffered in an
/// `async_channel` and returned via `recvBytes`. No message decoding or
/// request-response routing happens here — that's handled by
/// [`MessageTransport`](subduction_core::transport::message::MessageTransport).
#[wasm_bindgen(js_name = SubductionWebSocket)]
#[derive(Debug, Clone)]
pub struct WasmWebSocket {
    socket: WebSocket,
    inbound_reader: async_channel::Receiver<Vec<u8>>,
}

impl WasmWebSocket {
    /// Set up the byte channel and `onmessage` handler on an already-open `WebSocket`.
    fn from_open_socket(ws: WebSocket) -> Self {
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Vec<u8>>(64);

        let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                tracing::debug!("WS received {} bytes", bytes.len());

                let writer = inbound_writer.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    if let Err(e) = writer.send(bytes).await {
                        tracing::error!("failed to send inbound bytes: {e}");
                    }
                });
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
            socket: ws,
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
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedTransportError> {
        // Ensure WebSocket is ready
        let ws = Self::wait_for_open(ws.clone()).await?;

        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            Self::from_open_socket(ws),
            move |ws_transport, _peer_id| (ws_transport, ()),
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedTransportError::Handshake)?;

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
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedTransportError> {
        Self::connect_authenticated(address, signer, expected_peer_id)
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
        service_name: Option<String>,
    ) -> Result<WasmAuthenticatedWebSocket, WebSocketAuthenticatedTransportError> {
        Self::connect_discover_authenticated(address, signer, service_name)
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
    ) -> Result<Authenticated<WasmWebSocket, Local>, WebSocketAuthenticatedTransportError> {
        let ws = WebSocket::new(&address.href())
            .map_err(WebSocketAuthenticatedTransportError::SocketCreationFailed)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let ws = Self::wait_for_open(ws).await?;

        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            Self::from_open_socket(ws),
            move |ws_transport, _peer_id| (ws_transport, ()),
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedTransportError::Handshake)?;

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
        service_name: Option<String>,
    ) -> Result<Authenticated<WasmWebSocket, Local>, WebSocketAuthenticatedTransportError> {
        let service_name = service_name.unwrap_or_else(|| address.host());

        let ws = WebSocket::new(&address.href())
            .map_err(WebSocketAuthenticatedTransportError::SocketCreationFailed)?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let ws = Self::wait_for_open(ws).await?;

        let audience = Audience::discover(service_name.as_bytes());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, ()) = handshake::initiate::<Local, _, _, _, _>(
            Self::from_open_socket(ws),
            move |ws_transport, _peer_id| (ws_transport, ()),
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(WebSocketAuthenticatedTransportError::Handshake)?;

        tracing::info!(
            "Discovery handshake complete: connected to server {}",
            authenticated.peer_id()
        );

        Ok(authenticated)
    }

    /// Wait for a WebSocket to reach OPEN state.
    async fn wait_for_open(
        ws: WebSocket,
    ) -> Result<WebSocket, WebSocketAuthenticatedTransportError> {
        let (open_tx, open_rx) = oneshot::channel::<Result<(), WebSocketTransportFailed>>();
        let open_tx_cell = Rc::new(RefCell::new(Some(open_tx)));
        let open_tx_clone = open_tx_cell.clone();

        let onopen = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = open_tx_clone.borrow_mut().take() {
                let _ = tx.send(Ok(()));
            }
        });

        let onerror_tx = open_tx_cell.clone();
        let onerror = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = onerror_tx.borrow_mut().take() {
                let _ = tx.send(Err(WebSocketTransportFailed));
            }
        });

        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        // Handle case where socket is already open
        if ws.ready_state() == WebSocket::OPEN
            && let Some(tx) = open_tx_cell.borrow_mut().take()
        {
            let _ = tx.send(Ok(()));
        }

        // Wait for open or error
        open_rx
            .await
            .map_err(|_| WebSocketAuthenticatedTransportError::Canceled)??;

        // Clear temporary handlers
        ws.set_onopen(None);
        ws.set_onerror(None);
        drop(onopen);
        drop(onerror);

        Ok(ws)
    }

    /// Send raw bytes over the WebSocket.
    ///
    /// # Errors
    ///
    /// Returns [`WasmSendError`] if the bytes could not be sent.
    #[wasm_bindgen(js_name = sendBytes)]
    pub async fn send_bytes(&self, bytes: &[u8]) -> Result<(), WasmSendError> {
        Transport::<Local>::send_bytes(self, bytes).await?;
        Ok(())
    }

    /// Receive the next message frame as raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ReadFromClosedChannel`] if the channel has been closed.
    #[wasm_bindgen(js_name = recvBytes)]
    pub async fn recv_bytes(&self) -> Result<js_sys::Uint8Array, ReadFromClosedChannel> {
        let bytes = Transport::<Local>::recv_bytes(self).await?;
        Ok(js_sys::Uint8Array::from(bytes.as_slice()))
    }

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(js_name = disconnect)]
    pub async fn wasm_disconnect(&self) {
        let _ = Transport::<Local>::disconnect(self).await;
    }
}

impl PartialEq for WasmWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.socket == other.socket
    }
}

impl Transport<Local> for WasmWebSocket {
    type SendError = SendError;
    type RecvError = ReadFromClosedChannel;
    type DisconnectionError = DisconnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let data = bytes.to_vec();
        async move {
            self.socket
                .send_with_u8_array(data.as_slice())
                .map_err(SendError::SocketSend)?;

            tracing::debug!("sent {} outbound bytes", data.len());
            Ok(())
        }
        .boxed_local()
    }

    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async {
            tracing::debug!("waiting for inbound bytes");
            let bytes = self
                .inbound_reader
                .recv()
                .await
                .map_err(|_| ReadFromClosedChannel)?;
            tracing::debug!("received {} inbound bytes", bytes.len());
            Ok(bytes)
        }
        .boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }
}

impl subduction_core::handshake::Handshake<Local> for WasmWebSocket {
    type Error = crate::error::WasmHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            self.socket.send_with_u8_array(&bytes).map_err(|e| {
                crate::error::WasmHandshakeError::WebSocket(alloc::format!("{e:?}"))
            })?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        async move {
            let bytes = self.inbound_reader.recv().await.map_err(|_| {
                crate::error::WasmHandshakeError::WebSocket("inbound channel closed".into())
            })?;
            Ok(bytes)
        }
        .boxed_local()
    }
}

// NOTE: Reconnect is not implemented for WasmWebSocket because handshake
// requires the signer, which is not stored in the struct. Applications should
// handle reconnection by calling connect again.

/// Problem while sending bytes.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),
}

/// Problem while disconnecting (infallible for WebSocket).
#[derive(Debug, Clone, Copy, Error)]
#[error("WebSocket disconnection error (should not occur)")]
pub struct DisconnectionError;

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

/// Error connecting to a WebSocket server with handshake authentication.
#[derive(Debug, Error)]
pub enum WebSocketAuthenticatedTransportError {
    /// Problem creating the WebSocket.
    #[error("WebSocket creation failed: {0:?}")]
    SocketCreationFailed(JsValue),

    /// WebSocket transport failed to open.
    #[error(transparent)]
    TransportFailed(#[from] WebSocketTransportFailed),

    /// Transport setup was canceled.
    #[error("transport setup canceled")]
    Canceled,

    /// Handshake failed.
    #[error("handshake failed: {0}")]
    Handshake(#[from] handshake::AuthenticateError<WasmHandshakeError>),

    /// WebSocket setup failed after handshake.
    #[error("setup failed: {0}")]
    Setup(#[from] WasmWebSocketSetupCanceled),
}

impl From<WebSocketAuthenticatedTransportError> for JsValue {
    fn from(err: WebSocketAuthenticatedTransportError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("WebSocketAuthenticatedTransportError");
        js_err.into()
    }
}

/// WebSocket transport failed during open.
#[derive(Debug, Clone, Copy, Error)]
#[error("WebSocket transport failed")]
pub struct WebSocketTransportFailed;

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

/// An authenticated WebSocket transport.
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

#[wasm_bindgen(js_class = AuthenticatedWebSocket)]
impl WasmAuthenticatedWebSocket {
    /// The verified peer identity.
    #[must_use]
    #[wasm_bindgen(getter, js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        self.inner.peer_id().into()
    }

    /// Convert to a transport-erased [`AuthenticatedTransport`](super::WasmAuthenticatedTransport).
    #[must_use]
    #[wasm_bindgen(js_name = toTransport)]
    pub fn to_transport(self) -> super::WasmAuthenticatedTransport {
        super::WasmAuthenticatedTransport::from_authenticated(self.inner.map(|ws| {
            let transport: super::JsTransport = wasm_bindgen::JsValue::from(ws).unchecked_into();
            super::make_transport(transport)
        }))
    }
}
