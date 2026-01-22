//! Unified WebSocket connection type for both incoming and outgoing connections.

use crate::{
    error::{CallError, DisconnectionError, RecvError, SendError},
    timeout::Timeout,
    websocket::WebSocket,
};

use async_tungstenite::tokio::{ConnectStream, TokioAdapter};
use core::time::Duration;
use futures::future::BoxFuture;
use futures_kind::Sendable;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::id::PeerId,
};
use tokio::net::TcpStream;

/// A unified WebSocket connection that can hold either an incoming TCP connection
/// or an outgoing WebSocket connection.
///
/// This allows the server to use the same `Subduction` instance for both
/// accepting incoming connections and initiating outgoing connections to peers.
#[derive(Debug, Clone)]
pub enum UnifiedWebSocket<O: Timeout<Sendable> + Clone + Send + Sync> {
    /// An incoming TCP connection (from `accept`).
    Incoming(WebSocket<TokioAdapter<TcpStream>, Sendable, O>),
    /// An outgoing WebSocket connection (from `connect_async`).
    Outgoing(WebSocket<ConnectStream, Sendable, O>),
}

impl<O: Timeout<Sendable> + Clone + Send + Sync> UnifiedWebSocket<O> {
    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket connection fails.
    pub async fn listen(&self) -> Result<(), crate::error::RunError> {
        match self {
            UnifiedWebSocket::Incoming(ws) => ws.listen().await,
            UnifiedWebSocket::Outgoing(ws) => ws.listen().await,
        }
    }
}

impl<O: Timeout<Sendable> + Clone + Send + Sync> Connection<Sendable> for UnifiedWebSocket<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::peer_id(ws),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::peer_id(ws),
        }
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::next_request_id(ws),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::next_request_id(ws),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::disconnect(ws),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::disconnect(ws),
        }
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::send(ws, message),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::send(ws, message),
        }
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::recv(ws),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::recv(ws),
        }
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        match self {
            UnifiedWebSocket::Incoming(ws) => Connection::<Sendable>::call(ws, req, timeout),
            UnifiedWebSocket::Outgoing(ws) => Connection::<Sendable>::call(ws, req, timeout),
        }
    }
}

impl<O: Timeout<Sendable> + Clone + Send + Sync> PartialEq for UnifiedWebSocket<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (UnifiedWebSocket::Incoming(a), UnifiedWebSocket::Incoming(b)) => a == b,
            (UnifiedWebSocket::Outgoing(a), UnifiedWebSocket::Outgoing(b)) => a == b,
            _ => false,
        }
    }
}
