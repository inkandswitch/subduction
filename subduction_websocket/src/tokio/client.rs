//! # Subduction [`WebSocket`] client for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    timeout::Timeout,
    websocket::WebSocket,
};
use async_tungstenite::tokio::{connect_async, ConnectStream};
use core::time::Duration;
use futures::{future::BoxFuture, FutureExt};
use futures_kind::Sendable;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use tungstenite::http::Uri;

/// A Tokio-flavoured [`WebSocket`] client implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketClient<O: Timeout<Sendable> + Clone + Send + Sync> {
    address: Uri,
    socket: WebSocket<ConnectStream, Sendable, O>,
}

impl<O: Timeout<Sendable> + Clone + Send + Sync> TokioWebSocketClient<O> {
    /// Create a new [`WebSocketClient`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established.
    pub async fn new<'a>(
        address: Uri,
        timeout: O,
        default_time_limit: Duration,
        peer_id: PeerId,
    ) -> Result<(Self, BoxFuture<'a, Result<(), RunError>>), tungstenite::Error>
    where
        O: 'a,
    {
        tracing::info!("Connecting to WebSocket server at {address}");
        let (ws_stream, _resp) = connect_async(address.clone()).await?;

        let socket = WebSocket::<_, _, O>::new(ws_stream, timeout, default_time_limit, peer_id);
        let fut_socket = socket.clone();

        let socket_listener = async move { fut_socket.listen().await }.boxed();
        let client = TokioWebSocketClient { address, socket };
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

impl<O: Timeout<Sendable> + Clone + Send + Sync> Connection<Sendable> for TokioWebSocketClient<O> {
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

impl<O: 'static + Timeout<Sendable> + Clone + Send + Sync> Reconnect<Sendable>
    for TokioWebSocketClient<O>
{
    type ConnectError = tungstenite::Error;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ConnectError>> {
        async move {
            let (new_instance, new_fut) = TokioWebSocketClient::new(
                self.address.clone(),
                self.socket.timeout_strategy().clone(),
                self.socket.default_time_limit(),
                self.socket.peer_id(),
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

impl<O: Timeout<Sendable> + Clone + Send + Sync> PartialEq for TokioWebSocketClient<O> {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id() == other.socket.peer_id()
    }
}
