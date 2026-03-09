//! Unified transport enum for wasm.
//!
//! Wraps both [`WasmWebSocket`] and [`WasmLongPollConnection`] so a single
//! [`Subduction`] instance can accept connections from either transport.

use core::time::Duration;

use super::{longpoll::WasmLongPollConnection, websocket::WasmWebSocket};
use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    peer::id::PeerId,
};

/// A unified connection covering both WebSocket and HTTP long-poll transports.
#[derive(Debug, Clone)]
pub enum WasmUnifiedTransport {
    /// WebSocket transport.
    WebSocket(WasmWebSocket),

    /// HTTP long-poll transport.
    LongPoll(WasmLongPollConnection),
}

impl From<WasmWebSocket> for WasmUnifiedTransport {
    fn from(ws: WasmWebSocket) -> Self {
        Self::WebSocket(ws)
    }
}

impl From<WasmLongPollConnection> for WasmUnifiedTransport {
    fn from(lp: WasmLongPollConnection) -> Self {
        Self::LongPoll(lp)
    }
}

/// Error type for send operations across transports.
#[derive(Debug, thiserror::Error)]
pub enum TransportSendError {
    /// WebSocket send error.
    #[error(transparent)]
    WebSocket(#[from] super::websocket::SendError),

    /// HTTP long-poll send error.
    #[error(transparent)]
    LongPoll(#[from] subduction_http_longpoll::error::SendError),
}

/// Error type for recv operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum TransportRecvError {
    /// WebSocket recv error.
    #[error(transparent)]
    WebSocket(#[from] super::websocket::ReadFromClosedChannel),

    /// HTTP long-poll recv error.
    #[error(transparent)]
    LongPoll(#[from] subduction_http_longpoll::error::RecvError),
}

/// Error type for call operations across transports.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransportCallError {
    /// WebSocket call error.
    #[error("websocket call error: {0}")]
    WebSocket(super::websocket::CallError),

    /// HTTP long-poll call error.
    #[error(transparent)]
    LongPoll(#[from] subduction_http_longpoll::error::CallError),
}

impl From<super::websocket::CallError> for TransportCallError {
    fn from(e: super::websocket::CallError) -> Self {
        Self::WebSocket(e)
    }
}

/// Error type for disconnect operations across transports.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum TransportDisconnectionError {
    /// WebSocket disconnection error (infallible).
    #[error("websocket disconnection: {0}")]
    WebSocket(core::convert::Infallible),

    /// HTTP long-poll disconnection error.
    #[error(transparent)]
    LongPoll(#[from] subduction_http_longpoll::error::DisconnectionError),
}

impl Connection<Local, SyncMessage> for WasmUnifiedTransport {
    type SendError = TransportSendError;
    type RecvError = TransportRecvError;
    type DisconnectionError = TransportDisconnectionError;

    fn peer_id(&self) -> PeerId {
        match self {
            Self::WebSocket(ws) => Connection::<Local, SyncMessage>::peer_id(ws),
            Self::LongPoll(lp) => Connection::<Local, SyncMessage>::peer_id(lp),
        }
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Connection::<Local, SyncMessage>::disconnect(ws);
                async move { fut.await.map_err(TransportDisconnectionError::WebSocket) }
                    .boxed_local()
            }
            Self::LongPoll(lp) => {
                let fut = Connection::<Local, SyncMessage>::disconnect(lp);
                async move { fut.await.map_err(Into::into) }.boxed_local()
            }
        }
    }

    fn send(&self, message: &SyncMessage) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Connection::<Local, SyncMessage>::send(ws, message);
                async move { fut.await.map_err(Into::into) }.boxed_local()
            }
            Self::LongPoll(lp) => {
                let fut = Connection::<Local, SyncMessage>::send(lp, message);
                async move { fut.await.map_err(Into::into) }.boxed_local()
            }
        }
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        match self {
            Self::WebSocket(ws) => {
                let fut = Connection::<Local, SyncMessage>::recv(ws);
                async move { fut.await.map_err(Into::into) }.boxed_local()
            }
            Self::LongPoll(lp) => {
                let fut = Connection::<Local, SyncMessage>::recv(lp);
                async move { fut.await.map_err(Into::into) }.boxed_local()
            }
        }
    }
}

impl Roundtrip<Local, BatchSyncRequest, BatchSyncResponse> for WasmUnifiedTransport {
    type CallError = TransportCallError;

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        match self {
            Self::WebSocket(ws) => {
                Roundtrip::<Local, BatchSyncRequest, BatchSyncResponse>::next_request_id(ws)
            }
            Self::LongPoll(lp) => {
                Roundtrip::<Local, BatchSyncRequest, BatchSyncResponse>::next_request_id(lp)
            }
        }
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        match self {
            Self::WebSocket(ws) => async move {
                Roundtrip::<Local, BatchSyncRequest, BatchSyncResponse>::call(ws, req, timeout)
                    .await
                    .map_err(Into::into)
            }
            .boxed_local(),
            Self::LongPoll(lp) => async move {
                Roundtrip::<Local, BatchSyncRequest, BatchSyncResponse>::call(lp, req, timeout)
                    .await
                    .map_err(Into::into)
            }
            .boxed_local(),
        }
    }
}

impl PartialEq for WasmUnifiedTransport {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::WebSocket(a), Self::WebSocket(b)) => a == b,
            (Self::LongPoll(a), Self::LongPoll(b)) => a == b,
            _ => false,
        }
    }
}
