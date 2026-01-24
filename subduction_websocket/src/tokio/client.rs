//! # Subduction [`WebSocket`] client for Tokio

use crate::{
    MAX_MESSAGE_SIZE,
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    handshake::{WebSocketHandshakeError, client_handshake},
    timeout::Timeout,
    websocket::WebSocket,
};
use async_tungstenite::tokio::{ConnectStream, connect_async_with_config};
use core::time::Duration;
use futures::{FutureExt, future::BoxFuture};
use futures_kind::Sendable;
use subduction_core::{
    connection::{
        Connection, Reconnect,
        handshake::{Audience, Nonce},
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    crypto::signer::Signer,
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
pub struct TokioWebSocketClient<R: Signer + Clone, O: Timeout<Sendable> + Clone + Send + Sync> {
    address: Uri,
    signer: R,
    expected_peer_id: PeerId,
    socket: WebSocket<ConnectStream, Sendable, O>,
}

impl<R: Signer + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
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
    /// * `expected_peer_id` - The expected server peer ID
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or handshake fails.
    pub async fn new<'a>(
        address: Uri,
        timeout: O,
        default_time_limit: Duration,
        signer: R,
        expected_peer_id: PeerId,
    ) -> Result<(Self, BoxFuture<'a, Result<(), RunError>>), ClientConnectError>
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
        let audience = Audience::known(expected_peer_id);
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let handshake_result =
            client_handshake(&mut ws_stream, &signer, audience, now, nonce).await?;

        let server_id = handshake_result.server_id;
        tracing::info!("Handshake complete: connected to {server_id}");

        let socket = WebSocket::<_, _, O>::new(ws_stream, timeout, default_time_limit, server_id);
        let fut_socket = socket.clone();

        let socket_listener = async move { fut_socket.listen().await }.boxed();
        let client = TokioWebSocketClient {
            address,
            signer,
            expected_peer_id,
            socket,
        };
        Ok((client, socket_listener))
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

impl<R: Signer + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync>
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
    R: 'static + Signer + Clone + Send + Sync,
    O: 'static + Timeout<Sendable> + Clone + Send + Sync,
> Reconnect<Sendable> for TokioWebSocketClient<R, O>
{
    type ConnectError = ClientConnectError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ConnectError>> {
        async move {
            let (new_instance, new_fut) = TokioWebSocketClient::new(
                self.address.clone(),
                self.socket.timeout_strategy().clone(),
                self.socket.default_time_limit(),
                self.signer.clone(),
                self.expected_peer_id,
            )
            .await?;

            *self = new_instance;
            tokio::spawn(async move {
                if let Err(e) = new_fut.await {
                    tracing::error!("WebSocket client listener error after reconnect: {e:?}");
                }
            });

            Ok(())
        }
        .boxed()
    }
}

impl<R: Signer + Clone + Send + Sync, O: Timeout<Sendable> + Clone + Send + Sync> PartialEq
    for TokioWebSocketClient<R, O>
{
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id() == other.socket.peer_id()
    }
}
