//! # Subduction [`WebSocket`] client for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    tokio::start::Unstarted,
    websocket::WebSocket,
};
use async_tungstenite::tokio::{connect_async, ConnectStream};
use futures::{future::BoxFuture, FutureExt};
use sedimentree_core::future::Sendable;
use std::time::Duration;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use tokio::task::JoinHandle;
use tungstenite::http::Uri;

use super::start::Start;

/// A Tokio-flavoured [`WebSocket`] client implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketClient {
    address: Uri,
    socket: WebSocket<ConnectStream>,
}

impl TokioWebSocketClient {
    /// Create a new [`WebSocketClient`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established.
    pub async fn new(
        address: Uri,
        timeout: Duration,
        peer_id: PeerId,
    ) -> Result<Unstarted<Self>, tungstenite::Error> {
        tracing::info!("Connecting to WebSocket server at {address}");
        let (ws_stream, _resp) = connect_async(address.clone()).await?;
        Ok(Unstarted(TokioWebSocketClient {
            address,
            socket: WebSocket::<_>::new(ws_stream, timeout, peer_id),
        }))
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

impl Start for TokioWebSocketClient {
    fn start(&self) -> JoinHandle<Result<(), RunError>> {
        let inner = self.clone();
        tokio::spawn(async move { inner.socket.listen().await })
    }
}

impl Connection<Sendable> for TokioWebSocketClient {
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

    fn disconnect(&mut self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async {
            tracing::debug!("Client sending message: {:?}", message);
            Connection::<Sendable>::send(&self.socket, message).await
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Client waiting to receive message");
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
            tracing::debug!("Client making call with request: {:?}", req);
            Connection::<Sendable>::call(&self.socket, req, override_timeout).await
        }
        .boxed()
    }
}

impl Reconnect<Sendable> for TokioWebSocketClient {
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ConnectError>> {
        async move {
            *self = TokioWebSocketClient::new(
                self.address.clone(),
                self.socket.timeout,
                self.socket.peer_id,
            )
            .await?
            .start();

            Ok(())
        }
        .boxed()
    }

    fn run(&mut self) -> BoxFuture<'_, Result<(), Self::RunError>> {
        async move {
            loop {
                self.socket.listen().await?;
                self.reconnect().await?;
            }
        }
        .boxed()
    }
}

impl PartialEq for TokioWebSocketClient {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id == other.socket.peer_id
    }
}
