//! Unified WebSocket connection type for both accepted and dialed connections.

use crate::{
    error::{CallError, DisconnectionError, RecvError, SendError},
    timeout::Timeout,
    websocket::{ChannelMessage, WebSocket},
};

use async_tungstenite::tokio::{ConnectStream, TokioAdapter};
use core::time::Duration;
use future_form::Sendable;
use futures::future::BoxFuture;
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    peer::id::PeerId,
};
use tokio::net::TcpStream;

/// A unified WebSocket connection that can hold either an accepted or dialed connection.
///
/// This allows the server to use the same `Subduction` instance for both
/// accepting incoming connections and dialing outgoing connections to peers.
///
/// The `M` parameter is the channel message type — typically [`SyncMessage`].
#[derive(Debug, Clone)]
pub enum UnifiedWebSocket<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage> {
    /// A connection we accepted (peer connected to us).
    Accepted(WebSocket<TokioAdapter<TcpStream>, Sendable, O, M>),

    /// A connection we dialed (we connected to peer).
    Dialed(WebSocket<ConnectStream, Sendable, O, M>),
}

impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage> UnifiedWebSocket<O, M> {
    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket connection fails.
    pub async fn listen(&self) -> Result<(), crate::error::RunError<M>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => in_ws.listen().await,
            UnifiedWebSocket::Dialed(out_ws) => out_ws.listen().await,
        }
    }

    /// Send a raw wire message (bypasses `Connection<Sendable, SyncMessage>`).
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] if the outbound channel is closed.
    pub async fn send_wire(&self, msg: &M) -> Result<(), crate::error::SendError> {
        match self {
            UnifiedWebSocket::Accepted(ws) => ws.send_wire(msg).await,
            UnifiedWebSocket::Dialed(ws) => ws.send_wire(msg).await,
        }
    }

    /// Receive the next unfiltered wire message.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError`] if the inbound channel is closed.
    pub async fn recv_wire(&self) -> Result<M, crate::error::RecvError> {
        match self {
            UnifiedWebSocket::Accepted(ws) => ws.recv_wire().await,
            UnifiedWebSocket::Dialed(ws) => ws.recv_wire().await,
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage> Connection<Sendable, SyncMessage>
    for UnifiedWebSocket<O, M>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => {
                Connection::<Sendable, SyncMessage>::peer_id(in_ws)
            }
            UnifiedWebSocket::Dialed(out_ws) => {
                Connection::<Sendable, SyncMessage>::peer_id(out_ws)
            }
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => {
                Connection::<Sendable, SyncMessage>::disconnect(in_ws)
            }
            UnifiedWebSocket::Dialed(out_ws) => {
                Connection::<Sendable, SyncMessage>::disconnect(out_ws)
            }
        }
    }

    fn send(&self, message: &SyncMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => {
                Connection::<Sendable, SyncMessage>::send(in_ws, message)
            }
            UnifiedWebSocket::Dialed(out_ws) => {
                Connection::<Sendable, SyncMessage>::send(out_ws, message)
            }
        }
    }

    fn recv(&self) -> BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        match self {
            UnifiedWebSocket::Accepted(in_ws) => Connection::<Sendable, SyncMessage>::recv(in_ws),
            UnifiedWebSocket::Dialed(out_ws) => Connection::<Sendable, SyncMessage>::recv(out_ws),
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage>
    Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse> for UnifiedWebSocket<O, M>
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
impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage> PartialEq for UnifiedWebSocket<O, M> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (UnifiedWebSocket::Accepted(a), UnifiedWebSocket::Accepted(b)) => a == b,
            (UnifiedWebSocket::Dialed(a), UnifiedWebSocket::Dialed(b)) => a == b,
            _ => false,
        }
    }
}
