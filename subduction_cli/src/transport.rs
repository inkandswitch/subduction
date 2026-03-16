//! Unified transport enum for the CLI server.
//!
//! Wraps [`UnifiedWebSocket`], [`HttpLongPollConnection`], and
//! [`IrohConnection`] so the server can use a single [`Subduction`]
//! instance for all transport types.

use core::time::Duration;

use future_form::Sendable;
use futures::future::BoxFuture;
use subduction_core::connection::{
    Roundtrip,
    message::{BatchSyncRequest, BatchSyncResponse, RequestId},
    timeout::Timeout,
    transport::Transport,
};
use subduction_http_longpoll::connection::HttpLongPollConnection;
use subduction_iroh::connection::IrohConnection;
use subduction_websocket::tokio::unified::UnifiedWebSocket;

/// A unified connection covering all transport types the CLI server supports.
#[derive(Debug, Clone)]
pub(crate) enum UnifiedTransport<O: Timeout<Sendable> + Send + Sync> {
    /// WebSocket transport (accepted or dialed).
    WebSocket(UnifiedWebSocket<O>),

    /// HTTP long-poll transport.
    HttpLongPoll(HttpLongPollConnection<O>),

    /// Iroh QUIC transport.
    Iroh(IrohConnection<O>),
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

impl<O: Timeout<Sendable> + Send + Sync> Transport<Sendable> for UnifiedTransport<O> {
    type SendError = TransportSendError;
    type RecvError = TransportRecvError;
    type DisconnectionError = TransportDisconnectionError;

    fn peer_id(&self) -> subduction_core::peer::id::PeerId {
        match self {
            Self::WebSocket(ws) => Transport::<Sendable>::peer_id(ws),
            Self::HttpLongPoll(lp) => Transport::<Sendable>::peer_id(lp),
            Self::Iroh(iroh) => Transport::<Sendable>::peer_id(iroh),
        }
    }

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Transport::<Sendable>::send_bytes(ws, bytes);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                let fut = Transport::<Sendable>::send_bytes(lp, bytes);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::Iroh(iroh) => {
                let fut = Transport::<Sendable>::send_bytes(iroh, bytes);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
        }
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Transport::<Sendable>::recv_bytes(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Transport::<Sendable>::recv_bytes(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Transport::<Sendable>::recv_bytes(iroh)
                    .await
                    .map_err(Into::into)
            }),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Transport::<Sendable>::disconnect(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Transport::<Sendable>::disconnect(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Transport::<Sendable>::disconnect(iroh)
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
