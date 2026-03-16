//! Unified WebSocket connection type for both accepted and dialed connections.

use alloc::vec::Vec;

use subduction_core::connection::timeout::Timeout;

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};

use async_tungstenite::tokio::{ConnectStream, TokioAdapter};
use core::time::Duration;
use future_form::Sendable;
use futures::future::BoxFuture;

use subduction_core::{
    connection::{
        Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId},
        transport::Transport,
    },
    peer::id::PeerId,
};
use tokio::net::TcpStream;

/// A unified WebSocket connection that can hold either an accepted or dialed connection.
///
/// This allows the server to use the same `Subduction` instance for both
/// accepting incoming connections and dialing outgoing connections to peers.
#[derive(Debug, Clone)]
pub enum UnifiedWebSocket<O: Timeout<Sendable> + Send + Sync> {
    /// A connection we accepted (peer connected to us).
    Accepted(WebSocket<TokioAdapter<TcpStream>, Sendable, O>),

    /// A connection we dialed (we connected to peer).
    Dialed(WebSocket<ConnectStream, Sendable, O>),
}

impl<O: Timeout<Sendable> + Send + Sync> UnifiedWebSocket<O> {
    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket connection fails.
    pub async fn listen(&self) -> Result<(), RunError> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => in_ws.listen().await,
            UnifiedWebSocket::Dialed(out_ws) => out_ws.listen().await,
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync> Transport<Sendable> for UnifiedWebSocket<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => Transport::<Sendable>::peer_id(in_ws),
            UnifiedWebSocket::Dialed(out_ws) => Transport::<Sendable>::peer_id(out_ws),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => Transport::<Sendable>::disconnect(in_ws),
            UnifiedWebSocket::Dialed(out_ws) => Transport::<Sendable>::disconnect(out_ws),
        }
    }

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => Transport::<Sendable>::send_bytes(in_ws, bytes),
            UnifiedWebSocket::Dialed(out_ws) => Transport::<Sendable>::send_bytes(out_ws, bytes),
        }
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => Transport::<Sendable>::recv_bytes(in_ws),
            UnifiedWebSocket::Dialed(out_ws) => Transport::<Sendable>::recv_bytes(out_ws),
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync> Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse>
    for UnifiedWebSocket<O>
{
    type CallError = CallError;

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(in_ws)
            }
            UnifiedWebSocket::Dialed(out_ws) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(out_ws)
            }
        }
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        match self {
            UnifiedWebSocket::Accepted(ws) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(ws, req, timeout)
            }
            UnifiedWebSocket::Dialed(ws) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(ws, req, timeout)
            }
        }
    }
}

/// [`PartialEq`] compares connections only within the same variant type.
///
/// An `Accepted` connection is never equal to a `Dialed` connection, even if they
/// represent a connection to the same peer. This is intentional: the same physical
/// peer connection cannot be both accepted and dialed simultaneously. However, this
/// means a server could have two separate connections to the same peer (one where
/// they connected to us, one where we connected to them). If this is undesirable,
/// deduplication should be handled at a higher level using peer IDs.
impl<O: Timeout<Sendable> + Send + Sync> PartialEq for UnifiedWebSocket<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (UnifiedWebSocket::Accepted(a), UnifiedWebSocket::Accepted(b)) => a == b,
            (UnifiedWebSocket::Dialed(a), UnifiedWebSocket::Dialed(b)) => a == b,
            _ => false,
        }
    }
}
