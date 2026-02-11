//! # Subduction [`WebSocket`] client for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    handshake::{client_handshake, WebSocketHandshakeError},
    timeout::Timeout,
    websocket::{ListenerTask, SenderTask, WebSocket},
    MAX_MESSAGE_SIZE,
};
use async_tungstenite::tokio::{connect_async_with_config, ConnectStream};
use core::time::Duration;
use future_form::{FutureForm, Sendable};
use futures::{future::BoxFuture, FutureExt};
use subduction_core::{
    connection::{
        handshake::Audience,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    crypto::{nonce::Nonce, signer::Signer},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use tungstenite::{http::Uri, protocol::WebSocketConfig};

/// Error type for client connection.
#[derive(Debug, thiserror::Error)]
pub enum ClientConnectError {
    /// WebSocket connection error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Handshake failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] WebSocketHandshakeError),
}

/// A Tokio-flavoured [`WebSocket`] client implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketClient<
    R: Signer<Sendable> + Clone,
    O: Timeout<Sendable> + Clone + Send + Sync,
> {
    address: Uri,
    signer: R,
    audience: Audience,
    socket: WebSocket<ConnectStream, Sendable, O>,
}

impl<R: Signer<Sendable> + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
    TokioWebSocketClient<R, O>
{
    /// Create a new [`WebSocketClient`] connection.
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
    /// - The client instance
    /// - A future for the listener task (receives incoming messages)
    /// - A future for the sender task (sends outgoing messages)
    ///
    /// Both futures should be spawned as background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn new<'a>(
        address: Uri,
        timeout: O,
        default_time_limit: Duration,
        signer: R,
        audience: Audience,
    ) -> Result<(Self, ListenerTask<'a>, SenderTask<'a>), ClientConnectError>
    where
        O: 'a,
        R: 'a,
    {
        tracing::info!("Connecting to WebSocket server at {address}");
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
        let (mut ws_stream, _resp) =
            connect_async_with_config(address.clone(), Some(ws_config)).await?;

        // Perform handshake
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let handshake_result =
            client_handshake(&mut ws_stream, &signer, audience, now, nonce).await?;

        let server_id = handshake_result.server_id;
        tracing::info!("Handshake complete: connected to {server_id}");

        let (socket, outbound_rx) =
            WebSocket::<_, _, O>::new(ws_stream, timeout, default_time_limit, server_id);

        let listener_socket = socket.clone();
        let listener = ListenerTask::new(async move { listener_socket.listen().await }.boxed());
        let sender = SenderTask::new(Sendable::from_future(socket.sender_task(outbound_rx)));

        let client = TokioWebSocketClient {
            address,
            signer,
            audience,
            socket,
        };
        Ok((client, listener, sender))
    }

    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * the connection drops unexpectedly
    /// * a message could not be sent or received
    /// * a message could not be parsed
    pub async fn listen(&self) -> Result<(), RunError> {
        self.socket.listen().await
    }
}

impl<R: Signer<Sendable> + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
    Connection<Sendable> for TokioWebSocketClient<R, O>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        Connection::<Sendable>::peer_id(&self.socket)
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async { Connection::<Sendable>::next_request_id(&self.socket).await }.boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: &Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!("client sending message: {:?}", message);
        Connection::<Sendable>::send(&self.socket, message)
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("client waiting to receive message");
            Connection::<Sendable>::recv(&self.socket).await
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
            Connection::<Sendable>::call(&self.socket, req, override_timeout).await
        }
        .boxed()
    }
}

impl<
        R: 'static + Signer<Sendable> + Clone + Send + Sync,
        O: 'static + Timeout<Sendable> + Clone + Send + Sync,
    > Reconnect<Sendable> for TokioWebSocketClient<R, O>
{
    type ReconnectionError = ClientConnectError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ReconnectionError>> {
        async move {
            let (new_instance, listener, sender) = TokioWebSocketClient::new(
                self.address.clone(),
                self.socket.timeout_strategy().clone(),
                self.socket.default_time_limit(),
                self.signer.clone(),
                self.audience,
            )
            .await?;

            *self = new_instance;
            tokio::spawn(async move {
                if let Err(e) = listener.await {
                    tracing::error!("WebSocket client listener error after reconnect: {e:?}");
                }
            });
            tokio::spawn(async move {
                if let Err(e) = sender.await {
                    tracing::error!("WebSocket client sender error after reconnect: {e:?}");
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
            ClientConnectError::Handshake(handshake_err) => match handshake_err {
                // Network/transport errors - retry
                WebSocketHandshakeError::WebSocket(_)
                | WebSocketHandshakeError::ConnectionClosed => true,

                // Protocol violations or explicit rejection - don't retry
                WebSocketHandshakeError::UnexpectedMessageType(_)
                | WebSocketHandshakeError::DecodeError(_)
                | WebSocketHandshakeError::Handshake(_)
                | WebSocketHandshakeError::Rejected { .. } => false,
            },
        }
    }
}

impl<R: Signer<Sendable> + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
    PartialEq for TokioWebSocketClient<R, O>
{
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id() == other.socket.peer_id()
    }
}
