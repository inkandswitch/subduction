//! Unified transport enum for the CLI server.
//!
//! Wraps [`UnifiedWebSocket`], [`HttpLongPollConnection`], and
//! [`IrohConnection`] so the server can use a single [`Subduction`]
//! instance for all transport types.
//!
//! The inner connections are parameterized over [`CliWireMessage`] so
//! that sync, ephemeral, and keyhive traffic can be multiplexed on
//! every physical link. Two [`Connection`] impls are provided:
//!
//! - `Connection<Sendable, CliWireMessage>` — passthrough, used by
//!   the composed handler to send/receive any wire message variant.
//! - `Connection<Sendable, SyncMessage>` — filters to sync-only
//!   traffic, used by the core `Subduction` listen loop.

use core::time::Duration;

use future_form::Sendable;
use futures::future::BoxFuture;
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
        timeout::Timeout,
    },
    peer::id::PeerId,
};
use subduction_http_longpoll::connection::HttpLongPollConnection;
use subduction_iroh::connection::IrohConnection;
use subduction_websocket::tokio::unified::UnifiedWebSocket;

use crate::wire::CliWireMessage;

/// A unified connection covering all transport types the CLI server supports.
#[derive(Debug, Clone)]
pub(crate) enum UnifiedTransport<O: Timeout<Sendable> + Send + Sync> {
    /// WebSocket transport (accepted or dialed).
    WebSocket(UnifiedWebSocket<O, CliWireMessage>),

    /// HTTP long-poll transport.
    HttpLongPoll(HttpLongPollConnection<O, CliWireMessage>),

    /// Iroh QUIC transport.
    Iroh(IrohConnection<O, CliWireMessage>),
}

/// Error type for send operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(crate) enum TransportSendError {
    /// WebSocket send error.
    #[error(transparent)]
    WebSocket(#[from] subduction_websocket::error::SendError),

    /// HTTP long-poll send error.
    #[error(transparent)]
    HttpLongPoll(#[from] subduction_http_longpoll::error::SendError),

    /// Iroh send error.
    #[error(transparent)]
    Iroh(#[from] subduction_iroh::error::SendError),
}

/// Error type for recv operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(crate) enum TransportRecvError {
    /// WebSocket recv error.
    #[error(transparent)]
    WebSocket(#[from] subduction_websocket::error::RecvError),

    /// HTTP long-poll recv error.
    #[error(transparent)]
    HttpLongPoll(#[from] subduction_http_longpoll::error::RecvError),

    /// Iroh recv error.
    #[error(transparent)]
    Iroh(#[from] subduction_iroh::error::RecvError),
}

/// Error type for call operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(crate) enum TransportCallError {
    /// WebSocket call error.
    #[error(transparent)]
    WebSocket(#[from] subduction_websocket::error::CallError),

    /// HTTP long-poll call error.
    #[error(transparent)]
    HttpLongPoll(#[from] subduction_http_longpoll::error::CallError),

    /// Iroh call error.
    #[error(transparent)]
    Iroh(#[from] subduction_iroh::error::CallError),
}

/// Error type for disconnect operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub(crate) enum TransportDisconnectionError {
    /// WebSocket disconnection error.
    #[error(transparent)]
    WebSocket(#[from] subduction_websocket::error::DisconnectionError),

    /// HTTP long-poll disconnection error.
    #[error(transparent)]
    HttpLongPoll(#[from] subduction_http_longpoll::error::DisconnectionError),

    /// Iroh disconnection error.
    #[error(transparent)]
    Iroh(#[from] subduction_iroh::error::DisconnectionError),
}

// ── Connection<Sendable, CliWireMessage> — unfiltered wire access ───────
//
// Uses `send_wire` / `recv_wire` on WebSocket and Iroh, and
// `push_outbound` / `recv_inbound` on HTTP long-poll to bypass the
// `Connection<Sendable, SyncMessage>` filter layer.

impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable, CliWireMessage>
    for UnifiedTransport<O>
{
    type SendError = TransportSendError;
    type RecvError = TransportRecvError;
    type DisconnectionError = TransportDisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            Self::WebSocket(ws) => Connection::<Sendable, SyncMessage>::peer_id(ws),
            Self::HttpLongPoll(lp) => Connection::<Sendable, SyncMessage>::peer_id(lp),
            Self::Iroh(iroh) => Connection::<Sendable, SyncMessage>::peer_id(iroh),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(iroh)
                    .await
                    .map_err(Into::into)
            }),
        }
    }

    fn send(&self, message: &CliWireMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let msg = message.clone();
        match self {
            Self::WebSocket(ws) => {
                let ws = ws.clone();
                Box::pin(async move { ws.send_wire(&msg).await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                let lp = lp.clone();
                Box::pin(async move {
                    lp.push_outbound(msg)
                        .await
                        .map_err(|_| subduction_http_longpoll::error::SendError)
                        .map_err(Into::into)
                })
            }
            Self::Iroh(iroh) => {
                let iroh = iroh.clone();
                Box::pin(async move { iroh.send_wire(&msg).await.map_err(Into::into) })
            }
        }
    }

    fn recv(&self) -> BoxFuture<'_, Result<CliWireMessage, Self::RecvError>> {
        match self {
            Self::WebSocket(ws) => {
                let ws = ws.clone();
                Box::pin(async move { ws.recv_wire().await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                let lp = lp.clone();
                Box::pin(async move {
                    lp.recv_inbound()
                        .await
                        .map_err(|_| subduction_http_longpoll::error::RecvError)
                        .map_err(Into::into)
                })
            }
            Self::Iroh(iroh) => {
                let iroh = iroh.clone();
                Box::pin(async move { iroh.recv_wire().await.map_err(Into::into) })
            }
        }
    }
}

// ── Connection<Sendable, SyncMessage> — sync-only filter ───────────────

impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable, SyncMessage> for UnifiedTransport<O> {
    type SendError = TransportSendError;
    type RecvError = TransportRecvError;
    type DisconnectionError = TransportDisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            Self::WebSocket(ws) => Connection::<Sendable, SyncMessage>::peer_id(ws),
            Self::HttpLongPoll(lp) => Connection::<Sendable, SyncMessage>::peer_id(lp),
            Self::Iroh(iroh) => Connection::<Sendable, SyncMessage>::peer_id(iroh),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::disconnect(iroh)
                    .await
                    .map_err(Into::into)
            }),
        }
    }

    fn send(&self, message: &SyncMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Connection::<Sendable, SyncMessage>::send(ws, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                let fut = Connection::<Sendable, SyncMessage>::send(lp, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::Iroh(iroh) => {
                let fut = Connection::<Sendable, SyncMessage>::send(iroh, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
        }
    }

    fn recv(&self) -> BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::recv(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::recv(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Connection::<Sendable, SyncMessage>::recv(iroh)
                    .await
                    .map_err(Into::into)
            }),
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync> Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse>
    for UnifiedTransport<O>
{
    type CallError = TransportCallError;

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        match self {
            Self::WebSocket(ws) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(ws)
            }
            Self::HttpLongPoll(lp) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(lp)
            }
            Self::Iroh(iroh) => {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(iroh)
            }
        }
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async move {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(ws, req, timeout)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async move {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(lp, req, timeout)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async move {
                Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(iroh, req, timeout)
                    .await
                    .map_err(Into::into)
            }),
        }
    }
}

impl<O: Timeout<Sendable> + Send + Sync> PartialEq for UnifiedTransport<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::WebSocket(a), Self::WebSocket(b)) => a == b,
            (Self::HttpLongPoll(a), Self::HttpLongPoll(b)) => a == b,
            (Self::Iroh(a), Self::Iroh(b)) => a == b,
            _ => false,
        }
    }
}
