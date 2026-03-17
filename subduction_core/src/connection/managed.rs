//! A connection paired with its request-response multiplexer.
//!
//! [`ManagedConnection`] bundles an [`Authenticated`] connection with a
//! [`Multiplexer`] and a [`Timeout`] strategy so that `Subduction` can
//! perform roundtrip calls without the transport knowing about multiplexing.
//!
//! # Architecture
//!
//! ```text
//! ManagedConnection<C, K, O>
//!   ├── Authenticated<C, K>   — the connection + verified peer identity
//!   ├── Arc<Multiplexer>      — pending map + request ID counter
//!   └── O                     — timeout strategy (e.g., TokioTimeout, JsTimeout)
//! ```
//!
//! The [`call`](ManagedConnection::call) method sends a
//! [`BatchSyncRequest`] and awaits the matching [`BatchSyncResponse`].
//! Responses are delivered by the `Subduction` listen loop calling
//! [`Multiplexer::resolve_pending`] when it receives a
//! [`BatchSyncResponse`] from the handler dispatch.

use alloc::sync::Arc;
use core::time::Duration;

use future_form::{FutureForm, Local, Sendable};
use futures::FutureExt;
use sedimentree_core::codec::{decode::Decode, encode::Encode};

use crate::{
    authenticated::Authenticated,
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    multiplexer::Multiplexer,
    peer::id::PeerId,
    timeout::{TimedOut, Timeout},
};

/// A connection paired with its request-response multiplexer and timeout.
#[derive(Debug)]
pub struct ManagedConnection<C: Clone, K: FutureForm, O> {
    authenticated: Authenticated<C, K>,
    multiplexer: Arc<Multiplexer>,
    timer: O,
}

impl<C: Clone, K: FutureForm, O: Clone> Clone for ManagedConnection<C, K, O> {
    fn clone(&self) -> Self {
        Self {
            authenticated: self.authenticated.clone(),
            multiplexer: self.multiplexer.clone(),
            timer: self.timer.clone(),
        }
    }
}

impl<C: Clone + PartialEq, K: FutureForm, O> PartialEq for ManagedConnection<C, K, O> {
    fn eq(&self, other: &Self) -> bool {
        self.authenticated == other.authenticated
    }
}

impl<C: Clone, K: FutureForm, O> ManagedConnection<C, K, O> {
    /// Create a new managed connection.
    pub const fn new(
        authenticated: Authenticated<C, K>,
        multiplexer: Arc<Multiplexer>,
        timer: O,
    ) -> Self {
        Self {
            authenticated,
            multiplexer,
            timer,
        }
    }

    /// The verified peer identity.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.authenticated.peer_id()
    }

    /// Access the authenticated connection.
    #[must_use]
    pub const fn authenticated(&self) -> &Authenticated<C, K> {
        &self.authenticated
    }

    /// Access the multiplexer.
    #[must_use]
    pub const fn multiplexer(&self) -> &Arc<Multiplexer> {
        &self.multiplexer
    }

    /// Consume the managed connection, returning the authenticated connection.
    #[must_use]
    pub fn into_authenticated(self) -> Authenticated<C, K> {
        self.authenticated
    }

    /// Generate the next request ID for this connection.
    pub fn next_request_id(&self) -> RequestId {
        self.multiplexer.next_request_id()
    }
}

/// Error from a roundtrip [`call`](ManagedConnection::call).
#[derive(Debug, Clone, thiserror::Error)]
pub enum CallError<S: core::error::Error> {
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

impl<S: core::error::Error> CallError<S> {
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

impl<C: Clone, O> ManagedConnection<C, Sendable, O>
where
    O: Timeout<Sendable> + Send + Sync,
{
    /// Send a [`BatchSyncRequest`] and await the matching [`BatchSyncResponse`].
    ///
    /// The request is wrapped in `SyncMessage::BatchSyncRequest`, converted to
    /// the handler's wire type `M` via `From<SyncMessage>`, and sent via
    /// `Connection::send`. The response arrives via the `Multiplexer`'s
    /// pending map, which is resolved by the `Subduction` listen loop.
    ///
    /// # Errors
    ///
    /// Returns [`CallError`] on send failure, dropped response, or timeout.
    pub fn call<M>(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'_, Result<BatchSyncResponse, CallError<C::SendError>>>
    where
        C: Connection<Sendable, M> + PartialEq + Send + Sync,
        C::SendError: Send + 'static,
        M: Encode + Decode + From<SyncMessage> + Send,
    {
        let time_limit = timeout.unwrap_or(self.multiplexer.default_time_limit());
        async move {
            let req_id = req.req_id;
            let rx = self.multiplexer.register_pending(req_id).await;

            let wire_msg: M = SyncMessage::BatchSyncRequest(req).into();
            Connection::<Sendable, M>::send(&self.authenticated, &wire_msg)
                .await
                .map_err(CallError::Send)?;

            match self.timer.timeout(time_limit, rx.boxed()).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(_)) => {
                    tracing::error!("request {req_id:?} response dropped");
                    Err(CallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    tracing::error!("request {req_id:?} timed out");
                    self.multiplexer.cancel_pending(&req_id).await;
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

impl<C: Clone, O> ManagedConnection<C, Local, O>
where
    O: Timeout<Local>,
{
    /// Send a [`BatchSyncRequest`] and await the matching [`BatchSyncResponse`].
    ///
    /// See the `Sendable` variant for full documentation.
    ///
    /// # Errors
    ///
    /// Returns [`CallError`] on send failure, dropped response, or timeout.
    pub fn call<M>(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::LocalBoxFuture<'_, Result<BatchSyncResponse, CallError<C::SendError>>>
    where
        C: Connection<Local, M> + PartialEq,
        M: Encode + Decode + From<SyncMessage>,
    {
        let time_limit = timeout.unwrap_or(self.multiplexer.default_time_limit());
        async move {
            let req_id = req.req_id;
            let rx = self.multiplexer.register_pending(req_id).await;

            let wire_msg: M = SyncMessage::BatchSyncRequest(req).into();
            Connection::<Local, M>::send(&self.authenticated, &wire_msg)
                .await
                .map_err(CallError::Send)?;

            match self.timer.timeout(time_limit, rx.boxed_local()).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(_)) => {
                    tracing::error!("request {req_id:?} response dropped");
                    Err(CallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    tracing::error!("request {req_id:?} timed out");
                    self.multiplexer.cancel_pending(&req_id).await;
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed_local()
    }
}
