//! # Subduction [`WebSocket`] client for Tokio

use crate::{
    DEFAULT_MAX_MESSAGE_SIZE,
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    handshake::{WebSocketHandshake, WebSocketHandshakeError},
    timeout::Timeout,
    websocket::{ChannelMessage, ListenerTask, SenderTask, WebSocket},
};
use async_tungstenite::tokio::{ConnectStream, connect_async_with_config};
use core::time::Duration;
use future_form::{FutureForm, Sendable};
use futures::{FutureExt, future::BoxFuture};
use subduction_core::{
    connection::{
        Connection, Reconnect, Roundtrip,
        authenticated::Authenticated,
        handshake::{self, AuthenticateError, audience::Audience},
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    peer::id::PeerId,
    timestamp::TimestampSeconds,
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
///
/// The `M` parameter is the channel message type (default: `SyncMessage`).
/// When using `WireMessage`, both sync and ephemeral traffic are multiplexed.
#[derive(Debug, Clone)]
pub struct TokioWebSocketClient<
    R: Signer<Sendable> + Clone,
    O: Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> {
    address: Uri,
    signer: R,
    audience: Audience,
    socket: WebSocket<ConnectStream, Sendable, O, M>,
}

impl<
    R: Signer<Sendable> + Clone + Send + Sync,
    O: Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> TokioWebSocketClient<R, O, M>
{
    /// Create a new [`TokioWebSocketClient`] connection.
    ///
    /// Performs the handshake protocol to authenticate both sides.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URI to connect to
    /// * `timeout` - Timeout strategy for requests
    /// * `default_time_limit` - Default timeout duration
    /// * `signer` - The client's signer for authentication
    /// * `audience` - The expected server identity ([`Audience::Known`] for a specific peer,
    ///   [`Audience::Discover`] for service discovery)
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - The authenticated client instance (wrapped in [`Authenticated`] to prove handshake completed)
    /// - A future for the listener task (receives incoming messages)
    /// - A future for the sender task (sends outgoing messages)
    ///
    /// Both futures should be spawned as background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    ///
    /// # Panics
    ///
    /// Panics if internal state is inconsistent after a successful handshake (should never happen).
    #[allow(clippy::expect_used)]
    pub async fn new<'a>(
        address: Uri,
        timeout: O,
        default_time_limit: Duration,
        signer: R,
        audience: Audience,
    ) -> Result<
        (
            Authenticated<Self, Sendable>,
            ListenerTask<'a, M>,
            SenderTask<'a, M>,
        ),
        ClientConnectError,
    >
    where
        O: 'a,
        R: 'a,
        M: 'a,
    {
        tracing::info!("Connecting to WebSocket server at {address}");
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(DEFAULT_MAX_MESSAGE_SIZE);
        let (ws_stream, _resp) =
            connect_async_with_config(address.clone(), Some(ws_config)).await?;

        // Perform handshake
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let timeout_clone = timeout.clone();
        let (authenticated, sender_fut) = handshake::initiate::<Sendable, _, _, _, _>(
            WebSocketHandshake::new(ws_stream),
            |ws_handshake, peer_id| {
                let (socket, sender_fut) = WebSocket::<_, _, O, M>::new(
                    ws_handshake.into_inner(),
                    timeout_clone,
                    default_time_limit,
                    peer_id,
                );
                (socket, Sendable::from_future(sender_fut))
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

        // Lift the Authenticated proof from WebSocket to TokioWebSocketClient
        let authenticated_client = authenticated.map(|_socket| TokioWebSocketClient {
            address,
            signer,
            audience,
            socket,
        });

        Ok((authenticated_client, listener, sender))
    }

    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * the connection drops unexpectedly
    /// * a message could not be sent or received
    /// * a message could not be parsed
    pub async fn listen(&self) -> Result<(), RunError<M>> {
        self.socket.listen().await
    }
}

impl<
    R: Signer<Sendable> + Clone + Send + Sync,
    O: Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> Connection<Sendable, SyncMessage> for TokioWebSocketClient<R, O, M>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        Connection::<Sendable, SyncMessage>::peer_id(&self.socket)
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: &SyncMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!("client sending message: {:?}", message);
        Connection::<Sendable, SyncMessage>::send(&self.socket, message)
    }

    fn recv(&self) -> BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        async {
            tracing::debug!("client waiting to receive message");
            Connection::<Sendable, SyncMessage>::recv(&self.socket).await
        }
        .boxed()
    }
}

impl<
    R: Signer<Sendable> + Clone + Send + Sync,
    O: Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse> for TokioWebSocketClient<R, O, M>
{
    type CallError = CallError;

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async {
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(
                &self.socket,
            )
            .await
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("client making call with request: {:?}", req);
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(
                &self.socket,
                req,
                override_timeout,
            )
            .await
        }
        .boxed()
    }
}

impl<
    R: 'static + Signer<Sendable> + Clone + Send + Sync,
    O: 'static + Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> Reconnect<Sendable, SyncMessage> for TokioWebSocketClient<R, O, M>
{
    type ReconnectionError = ClientConnectError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ReconnectionError>> {
        async move {
            let (authenticated, listener, sender) = TokioWebSocketClient::<R, O, M>::new(
                self.address.clone(),
                self.socket.timeout_strategy().clone(),
                self.socket.default_time_limit(),
                self.signer.clone(),
                self.audience,
            )
            .await?;

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
                | AuthenticateError::UnexpectedMessage => false,
            },
        }
    }
}

impl<
    R: Signer<Sendable> + Clone + Send + Sync,
    O: Timeout<Sendable> + Send + Sync,
    M: ChannelMessage,
> PartialEq for TokioWebSocketClient<R, O, M>
{
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id() == other.socket.peer_id()
    }
}
