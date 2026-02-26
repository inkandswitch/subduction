//! Unified transport enum for the CLI server.
//!
//! Wraps [`UnifiedWebSocket`], [`HttpLongPollConnection`], and
//! [`IrohConnection`] so the server can use a single [`Subduction`]
//! instance for all transport types.

use core::time::Duration;

use future_form::Sendable;
use futures::future::BoxFuture;
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        timeout::Timeout,
    },
    peer::id::PeerId,
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

impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable> for UnifiedTransport<O> {
    type SendError = TransportSendError;
    type RecvError = TransportRecvError;
    type CallError = TransportCallError;
    type DisconnectionError = TransportDisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            Self::WebSocket(ws) => Connection::<Sendable>::peer_id(ws),
            Self::HttpLongPoll(lp) => Connection::<Sendable>::peer_id(lp),
            Self::Iroh(iroh) => Connection::<Sendable>::peer_id(iroh),
        }
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        match self {
            Self::WebSocket(ws) => Connection::<Sendable>::next_request_id(ws),
            Self::HttpLongPoll(lp) => Connection::<Sendable>::next_request_id(lp),
            Self::Iroh(iroh) => Connection::<Sendable>::next_request_id(iroh),
        }
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            Self::WebSocket(ws) => Box::pin(async {
                Connection::<Sendable>::disconnect(ws)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async {
                Connection::<Sendable>::disconnect(lp)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async {
                Connection::<Sendable>::disconnect(iroh)
                    .await
                    .map_err(Into::into)
            }),
        }
    }

    fn send(&self, message: &Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Connection::<Sendable>::send(ws, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                let fut = Connection::<Sendable>::send(lp, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
            Self::Iroh(iroh) => {
                let fut = Connection::<Sendable>::send(iroh, message);
                Box::pin(async move { fut.await.map_err(Into::into) })
            }
        }
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        match self {
            Self::WebSocket(ws) => {
                Box::pin(async { Connection::<Sendable>::recv(ws).await.map_err(Into::into) })
            }
            Self::HttpLongPoll(lp) => {
                Box::pin(async { Connection::<Sendable>::recv(lp).await.map_err(Into::into) })
            }
            Self::Iroh(iroh) => {
                Box::pin(async { Connection::<Sendable>::recv(iroh).await.map_err(Into::into) })
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
                Connection::<Sendable>::call(ws, req, timeout)
                    .await
                    .map_err(Into::into)
            }),
            Self::HttpLongPoll(lp) => Box::pin(async move {
                Connection::<Sendable>::call(lp, req, timeout)
                    .await
                    .map_err(Into::into)
            }),
            Self::Iroh(iroh) => Box::pin(async move {
                Connection::<Sendable>::call(iroh, req, timeout)
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
