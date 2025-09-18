//! # Sedimentree Sync WebSocket server for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::tokio::{accept_async, TokioAdapter};
use core::net::SocketAddr;
use sedimentree_sync_core::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionId, Message, Reconnection,
        RequestId,
    },
    peer::id::PeerId,
};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

/// A WebSocketServer implementation for [`Connection`].
#[derive(Debug)]
pub struct TokioWebSocketServer {
    address: SocketAddr,
    socket: WebSocket<TokioAdapter<TcpStream>>,
}

impl TokioWebSocketServer {
    /// Create a new WebSocketServer connection.
    pub async fn new(
        address: SocketAddr,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Result<Self, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {address}");
        let listener = TcpListener::bind(address).await?;
        let (tcp, _peer) = listener.accept().await?;
        let ws_stream = accept_async(tcp).await?;
        let socket = WebSocket::<_>::new(ws_stream, timeout, peer_id, conn_id);
        Ok(TokioWebSocketServer { address, socket })
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

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: Message) -> Result<(), Self::SendError> {
        self.socket.send(message).await
    }

    #[tracing::instrument(skip(self))]
    async fn recv(&self) -> Result<Message, Self::RecvError> {
        self.socket.recv().await
    }

    #[tracing::instrument(skip(self, req), fields(req_id = ?req.req_id))]
    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
        self.socket.call(req, override_timeout).await
    }
}

impl Reconnection for TokioWebSocketServer {
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    async fn reconnect(&mut self) -> Result<(), Self::ConnectError> {
        *self = TokioWebSocketServer::new(
            self.address.clone(),
            self.socket.timeout,
            self.socket.peer_id,
            self.connection_id(),
        )
        .await?;

        Ok(())
    }

    async fn run(&self) -> Result<(), Self::RunError> {
        self.socket.run().await
    }
}
