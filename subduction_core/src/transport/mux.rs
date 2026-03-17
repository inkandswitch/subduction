//! Request-response multiplexer over a [`Transport`](super::Transport).
//!
//! [`MuxTransport<T, O>`] wraps any transport and provides [`Roundtrip`]
//! via a [`Multiplexer`]. On the recv path, `BatchSyncResponse` messages
//! are intercepted and routed to pending `call()` callers instead of
//! being returned to the normal recv stream.

use alloc::sync::Arc;
use core::time::Duration;

use super::Transport;
use crate::{
    connection::{
        Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    multiplexer::Multiplexer,
    peer::id::PeerId,
    timeout::{TimedOut, Timeout},
};
use future_form::{FutureForm, Local, Sendable, future_form};

/// Request-response multiplexer over a [`Transport`].
///
/// Wraps any `Transport` and provides [`Roundtrip`] via a [`Multiplexer`].
/// On the recv path, `BatchSyncResponse` messages are intercepted and
/// routed to pending `call()` callers instead of being returned to the
/// normal recv stream.
///
/// # Usage
///
/// ```ignore
/// let ws: WebSocket = ...;
/// let mux = MuxTransport::new(ws, timeout, time_limit, peer_id);
/// let conn = MessageTransport::new(mux);
/// // conn implements Connection<K, M> + Roundtrip<K, BatchSyncRequest, BatchSyncResponse>
/// ```
///
/// Transport backends that don't need request-response (e.g., fire-and-forget
/// ephemeral relays) can skip `MuxTransport` and use `MessageTransport`
/// directly — `Roundtrip` remains a separate trait that consumers opt into.
#[derive(Debug)]
pub struct MuxTransport<T, O> {
    transport: T,
    multiplexer: Arc<Multiplexer>,
    timeout: O,
}

impl<T: Clone, O: Clone> Clone for MuxTransport<T, O> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            multiplexer: self.multiplexer.clone(),
            timeout: self.timeout.clone(),
        }
    }
}

impl<T: PartialEq, O> PartialEq for MuxTransport<T, O> {
    fn eq(&self, other: &Self) -> bool {
        self.transport == other.transport
    }
}

impl<T, O> MuxTransport<T, O> {
    /// Wrap a transport with request-response multiplexing.
    pub fn new(transport: T, timeout: O, default_time_limit: Duration, peer_id: PeerId) -> Self {
        Self {
            transport,
            multiplexer: Arc::new(Multiplexer::new(peer_id, default_time_limit)),
            timeout,
        }
    }

    /// Access the inner transport.
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// Access the multiplexer.
    pub fn multiplexer(&self) -> &Multiplexer {
        &self.multiplexer
    }

    /// Consume and return the inner transport.
    pub fn into_transport(self) -> T {
        self.transport
    }
}

#[future_form(
    Sendable where
        T: Transport<Sendable> + Send + Sync,
        T::SendError: Send + 'static,
        T::RecvError: Send + 'static,
        T::DisconnectionError: Send + 'static,
        O: Timeout<Sendable> + Send + Sync,
    Local where
        T: Transport<Local>,
        O: Timeout<Local>
)]
impl<K: FutureForm, T, O> Transport<K> for MuxTransport<T, O> {
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type DisconnectionError = T::DisconnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> K::Future<'_, Result<(), Self::SendError>> {
        self.transport.send_bytes(bytes)
    }

    fn recv_bytes(&self) -> K::Future<'_, Result<alloc::vec::Vec<u8>, Self::RecvError>> {
        K::from_future(async move {
            loop {
                let bytes = self.transport.recv_bytes().await?;

                // Intercept BatchSyncResponse and route to pending callers.
                if let Ok(SyncMessage::BatchSyncResponse(ref resp)) =
                    SyncMessage::try_decode(&bytes)
                    && self.multiplexer.resolve_pending(resp).await
                {
                    continue;
                }

                return Ok(bytes);
            }
        })
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        self.transport.disconnect()
    }
}

/// Error from a [`MuxTransport`] call.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MuxCallError<S: core::error::Error> {
    /// The transport failed to send the request.
    #[error("send error: {0}")]
    Send(S),

    /// The response channel was dropped (peer disconnected).
    #[error("response channel dropped")]
    ResponseDropped,

    /// The call timed out waiting for a response.
    #[error("call timed out")]
    Timeout,
}

impl<S: core::error::Error> MuxCallError<S> {
    /// Short error name suitable for JS `Error.name` or logging.
    #[must_use]
    pub const fn error_name(&self) -> &'static str {
        match self {
            Self::Send(_) => "CallSendError",
            Self::ResponseDropped => "CallResponseDropped",
            Self::Timeout => "CallTimeout",
        }
    }
}

#[future_form(
    Sendable where
        T: Transport<Sendable> + Send + Sync,
        T::SendError: Send + 'static,
        T::RecvError: Send + 'static,
        T::DisconnectionError: Send + 'static,
        O: Timeout<Sendable> + Send + Sync,
    Local where
        T: Transport<Local>,
        O: Timeout<Local>
)]
impl<K: FutureForm, T, O> Roundtrip<K, BatchSyncRequest, BatchSyncResponse> for MuxTransport<T, O> {
    type CallError = MuxCallError<T::SendError>;

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        K::from_future(async move { self.multiplexer.next_request_id() })
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        K::from_future(async move {
            let req_id = req.req_id;
            let rx = self.multiplexer.register_pending(req_id).await;

            let msg_bytes = SyncMessage::BatchSyncRequest(req).encode();
            self.transport
                .send_bytes(&msg_bytes)
                .await
                .map_err(MuxCallError::Send)?;

            let req_timeout = timeout.unwrap_or(self.multiplexer.default_time_limit());

            match self.timeout.timeout(req_timeout, K::from_future(rx)).await {
                Ok(Ok(resp)) => Ok(resp),
                Ok(Err(_)) => {
                    tracing::error!("request {req_id:?} response dropped");
                    Err(MuxCallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    tracing::error!("request {req_id:?} timed out");
                    self.multiplexer.cancel_pending(&req_id).await;
                    Err(MuxCallError::Timeout)
                }
            }
        })
    }
}
