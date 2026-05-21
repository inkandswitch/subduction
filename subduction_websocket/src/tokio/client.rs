//! # Subduction [`WebSocket`] client for Tokio

use alloc::vec::Vec;

use crate::{
    DEFAULT_MAX_MESSAGE_SIZE,
    error::{DisconnectionError, RecvError, RunError, SendError},
    handshake::{WebSocketHandshake, WebSocketHandshakeError},
    sleep::TokioSleeper,
    websocket::{KeepAlive, KeepAliveTask, ListenerTask, SenderTask, WebSocket},
};
use async_tungstenite::tokio::{ConnectStream, connect_async_with_config};
use future_form::{FutureForm, Sendable};
use futures::{FutureExt, future::BoxFuture};

use subduction_core::{
    authenticated::Authenticated,
    connection::{Connection, Reconnect, message::SyncMessage},
    handshake::{self, AuthenticateError, audience::Audience},
    timestamp::TimestampSeconds,
    transport::Transport,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use tungstenite::{http::Uri, protocol::WebSocketConfig};

/// Error type for client connection.
#[derive(Debug, thiserror::Error)]
pub enum ClientConnectError {
    /// WebSocket connection error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Handshake failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] AuthenticateError<WebSocketHandshakeError>),
}

/// A Tokio-flavoured [`WebSocket`] client implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketClient<R: Signer<Sendable> + Clone> {
    address: Uri,
    signer: R,
    audience: Audience,
    socket: WebSocket<ConnectStream, Sendable>,
    /// Carried across reconnects via [`Reconnect`]. Defaults to
    /// [`KeepAlive::balanced`]; use [`Self::with_keepalive`] to
    /// override.
    keepalive: KeepAlive,
}

impl<R: Signer<Sendable> + Clone + Send + Sync> TokioWebSocketClient<R> {
    /// Create a new [`TokioWebSocketClient`] connection with
    /// [`KeepAlive::balanced`] settings.
    ///
    /// Performs the handshake protocol to authenticate both sides and
    /// spawns a keepalive task that pings the server every 30 s. The
    /// `keepalive_task` element of the returned tuple must be spawned
    /// for the keepalive to fire.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn new<'a>(
        address: Uri,
        signer: R,
        audience: Audience,
    ) -> Result<
        (
            Authenticated<Self, Sendable>,
            ListenerTask<'a>,
            SenderTask<'a>,
            KeepAliveTask<Sendable>,
        ),
        ClientConnectError,
    >
    where
        R: 'a,
    {
        Self::with_options(
            address,
            signer,
            audience,
            DEFAULT_MAX_MESSAGE_SIZE,
            KeepAlive::balanced(),
        )
        .await
    }

    /// Like [`Self::new`] but with a custom tungstenite `max_message_size`
    /// (and matching `max_frame_size`).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn with_max_message_size<'a>(
        address: Uri,
        signer: R,
        audience: Audience,
        max_message_size: usize,
    ) -> Result<
        (
            Authenticated<Self, Sendable>,
            ListenerTask<'a>,
            SenderTask<'a>,
            KeepAliveTask<Sendable>,
        ),
        ClientConnectError,
    >
    where
        R: 'a,
    {
        Self::with_options(
            address,
            signer,
            audience,
            max_message_size,
            KeepAlive::balanced(),
        )
        .await
    }

    /// Like [`Self::new`] but with an explicit [`KeepAlive`] config
    /// (mostly for tests that need aggressive timings).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn with_keepalive<'a>(
        address: Uri,
        signer: R,
        audience: Audience,
        keepalive: KeepAlive,
    ) -> Result<
        (
            Authenticated<Self, Sendable>,
            ListenerTask<'a>,
            SenderTask<'a>,
            KeepAliveTask<Sendable>,
        ),
        ClientConnectError,
    >
    where
        R: 'a,
    {
        Self::with_options(
            address,
            signer,
            audience,
            DEFAULT_MAX_MESSAGE_SIZE,
            keepalive,
        )
        .await
    }

    /// Full constructor exposing every option in one call.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn with_options<'a>(
        address: Uri,
        signer: R,
        audience: Audience,
        max_message_size: usize,
        keepalive: KeepAlive,
    ) -> Result<
        (
            Authenticated<Self, Sendable>,
            ListenerTask<'a>,
            SenderTask<'a>,
            KeepAliveTask<Sendable>,
        ),
        ClientConnectError,
    >
    where
        R: 'a,
    {
        tracing::info!("Connecting to WebSocket server at {address} (keepalive: true)");
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(max_message_size);
        ws_config.max_frame_size = Some(max_message_size);
        let (ws_stream, _resp) =
            connect_async_with_config(address.clone(), Some(ws_config)).await?;

        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let (authenticated, (sender_fut, keepalive_task)) =
            handshake::initiate::<Sendable, _, _, _, _>(
                WebSocketHandshake::new(ws_stream),
                |ws_handshake, peer_id| {
                    let (socket, sender_fut, keepalive_task) = WebSocket::new_with_keepalive(
                        ws_handshake.into_inner(),
                        peer_id,
                        keepalive,
                        TokioSleeper,
                    );
                    (socket, (Sendable::from_future(sender_fut), keepalive_task))
                },
                &signer,
                audience,
                now,
                nonce,
            )
            .await?;

        let server_id = authenticated.peer_id();
        tracing::info!("Handshake complete: connected to {server_id}");

        let socket = authenticated.inner().clone();
        let listener_socket = socket.clone();
        let listener = ListenerTask::new(async move { listener_socket.listen().await }.boxed());
        let sender = SenderTask::new(sender_fut);

        let authenticated_client = authenticated.map(|_socket| TokioWebSocketClient {
            address,
            signer,
            audience,
            socket,
            keepalive,
        });

        Ok((authenticated_client, listener, sender, keepalive_task))
    }

    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * the connection drops unexpectedly
    /// * a message could not be sent or received
    pub async fn listen(&self) -> Result<(), RunError> {
        self.socket.listen().await
    }
}

impl<R: Signer<Sendable> + Clone + Send + Sync> Transport<Sendable> for TokioWebSocketClient<R> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!("client sending {} bytes", bytes.len());
        Transport::<Sendable>::send_bytes(&self.socket, bytes)
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        let socket = self.socket.clone();
        async move {
            tracing::debug!("client waiting to receive bytes");
            Transport::<Sendable>::recv_bytes(&socket).await
        }
        .boxed()
    }
}

/// Bridging impl: delegates to [`Transport`] with encode/decode so that
/// [`Reconnect`] (which requires `Connection`) is satisfied. Downstream
/// code should prefer [`MessageTransport`] for new integrations.
///
/// [`MessageTransport`]: subduction_core::transport::message::MessageTransport
impl<R: Signer<Sendable> + Clone + Send + Sync> Connection<Sendable, SyncMessage>
    for TokioWebSocketClient<R>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Transport::<Sendable>::disconnect(self)
    }

    fn send(&self, message: &SyncMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let bytes = message.encode();
        let this = self.socket.clone();
        async move { Transport::<Sendable>::send_bytes(&this, &bytes).await }.boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        let socket = self.socket.clone();
        async move {
            loop {
                let bytes = Transport::<Sendable>::recv_bytes(&socket).await?;
                match SyncMessage::try_decode(&bytes) {
                    Ok(msg) => return Ok(msg),
                    Err(e) => {
                        tracing::warn!("failed to decode inbound bytes as SyncMessage: {e}");
                        // Skip non-decodable frames and keep reading
                    }
                }
            }
        }
        .boxed()
    }
}

impl<R: 'static + Signer<Sendable> + Clone + Send + Sync> Reconnect<Sendable, SyncMessage>
    for TokioWebSocketClient<R>
{
    type ReconnectionError = ClientConnectError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ReconnectionError>> {
        async move {
            // Build the new connection first so a failure leaves the
            // old one intact.
            let (authenticated, listener, sender, keepalive_task) =
                TokioWebSocketClient::<R>::with_options(
                    self.address.clone(),
                    self.signer.clone(),
                    self.audience,
                    DEFAULT_MAX_MESSAGE_SIZE,
                    self.keepalive,
                )
                .await?;

            // Step 1 of the `Reconnect` contract: close the old
            // connection's channels. Sender + keepalive exit cleanly;
            // the listener still leaks until the TCP socket closes
            // (FIXME: needs CancellationToken plumbing).
            self.socket.close_channels();

            // Extract the inner client from the Authenticated wrapper
            *self = authenticated.into_inner();
            tokio::spawn(async move {
                if let Err(e) = listener.await {
                    tracing::info!("WebSocket client listener disconnected after reconnect: {e:?}");
                }
            });
            tokio::spawn(async move {
                if let Err(e) = sender.await {
                    tracing::info!("WebSocket client sender disconnected after reconnect: {e:?}");
                }
            });
            tokio::spawn(async move {
                let outcome = keepalive_task.await;
                tracing::debug!(
                    ?outcome,
                    "WebSocket client keepalive exited after reconnect"
                );
            });

            Ok(())
        }
        .boxed()
    }

    fn should_retry(&self, error: &Self::ReconnectionError) -> bool {
        match error {
            // Network errors are generally retryable
            ClientConnectError::WebSocket(_) => true,

            // Handshake errors depend on the specific type
            ClientConnectError::Handshake(auth_err) => match auth_err {
                // Transport errors - check the underlying WebSocket error
                AuthenticateError::Transport(ws_err) => match ws_err {
                    WebSocketHandshakeError::WebSocket(_)
                    | WebSocketHandshakeError::ConnectionClosed => true,
                    WebSocketHandshakeError::UnexpectedMessageType(_) => false,
                },

                // Connection closed during handshake - retry
                AuthenticateError::ConnectionClosed => true,

                // Protocol violations or explicit rejection - don't retry
                AuthenticateError::Decode(_)
                | AuthenticateError::Handshake(_)
                | AuthenticateError::Rejected { .. }
                | AuthenticateError::UnexpectedMessage
                | AuthenticateError::ReflectedChallenge
                | AuthenticateError::ReflectionAttack
                | AuthenticateError::SimultaneousOpenPeerMismatch => false,
            },
        }
    }
}

impl<R: Signer<Sendable> + Clone + Send + Sync> PartialEq for TokioWebSocketClient<R> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id() == other.socket.peer_id()
    }
}
