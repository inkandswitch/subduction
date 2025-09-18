//! # Sedimentree Sync WebSocket server for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::{
    tokio::{accept_async, TokioAdapter},
    WebSocketStream,
};
use core::net::SocketAddr;
use sedimentree_sync_core::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketServer {
    address: SocketAddr,
    socket: WebSocket<TokioAdapter<TcpStream>>,
}

impl TokioWebSocketServer {
    /// Create a new [`WebSocketServer`] connection from an accepted TCP stream.
    pub fn new(
        address: SocketAddr,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
        ws_stream: WebSocketStream<TokioAdapter<TcpStream>>,
    ) -> Self {
        let socket = WebSocket::<_>::new(ws_stream, timeout, peer_id, conn_id);
        tracing::info!("Accepting WebSocket connections at {address}");
        TokioWebSocketServer { address, socket }
    }

    /// Create a new [`WebSocketServer`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound,
    /// or if the connection could not be established.
    pub async fn setup(
        address: SocketAddr,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Result<Self, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {address}");
        let listener = TcpListener::bind(address).await?;
        let (tcp, _peer) = listener.accept().await?;
        let ws_stream = accept_async(tcp).await?;
        Ok(Self::new(address, timeout, peer_id, conn_id, ws_stream))
    }
}

impl Connection for TokioWebSocketServer {
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
        tracing::debug!("Server sending message: {:?}", message);
        self.socket.send(message).await
    }

    async fn recv(&self) -> Result<Message, Self::RecvError> {
        tracing::debug!("Server waiting to receive message");
        self.socket.recv().await
    }

    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
        tracing::debug!("Server making call with request: {:?}", req);
        self.socket.call(req, override_timeout).await
    }
}

impl Reconnect for TokioWebSocketServer {
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    async fn reconnect(&mut self) -> Result<(), Self::ConnectError> {
        *self = TokioWebSocketServer::setup(
            self.address,
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
