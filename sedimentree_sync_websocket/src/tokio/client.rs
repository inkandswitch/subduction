//! # Sedimentree Sync [`WebSocket`] client for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::tokio::{connect_async, ConnectStream};
use sedimentree_sync_core::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use std::time::Duration;
use tungstenite::http::Uri;

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
        conn_id: ConnectionId,
    ) -> Result<Self, tungstenite::Error> {
        tracing::info!("Connecting to WebSocket server at {address}");
        let (ws_stream, _resp) = connect_async(address.clone()).await?;
        Ok(TokioWebSocketClient {
            address,
            socket: WebSocket::<_>::new(ws_stream, timeout, peer_id, conn_id),
        })
    }
}

impl Connection for TokioWebSocketClient {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn connection_id(&self) -> ConnectionId {
        self.socket.connection_id()
    }

    fn peer_id(&self) -> PeerId {
        self.socket.peer_id()
    }

    async fn next_request_id(&self) -> RequestId {
        self.socket.next_request_id().await
    }

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        Ok(())
    }

    async fn send(&self, message: Message) -> Result<(), Self::SendError> {
        tracing::debug!("Client sending message: {:?}", message);
        self.socket.send(message).await
    }

    async fn recv(&self) -> Result<Message, Self::RecvError> {
        tracing::debug!("Client waiting to receive message");
        self.socket.recv().await
    }

    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
        tracing::debug!("Client making call with request: {:?}", req);
        self.socket.call(req, override_timeout).await
    }
}

impl Reconnect for TokioWebSocketClient {
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    async fn reconnect(&mut self) -> Result<(), Self::ConnectError> {
        *self = TokioWebSocketClient::new(
            self.address.clone(),
            self.socket.timeout,
            self.socket.peer_id,
            self.connection_id(),
        )
        .await?;

        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::RunError> {
        loop {
            self.socket.listen().await?;
            self.reconnect().await?;
        }
    }
}
