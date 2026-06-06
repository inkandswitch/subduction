//! A connection paired with its request-response multiplexer.
//!
//! [`ManagedConnection`] bundles an [`Authenticated`] connection with a
//! [`Multiplexer`] and a [`Timeout`] strategy so that `Subduction` can
//! perform roundtrip calls without the transport knowing about multiplexing.
//!
//! # Architecture
//!
//! ```text
//! ManagedConnection<Conn, Async, Timer>
//!   ├── Authenticated<Conn, Async>   — the connection + verified peer identity
//!   ├── Arc<Multiplexer>             — pending map + request ID counter
//!   └── Timer                        — timeout strategy (e.g., TokioTimeout, JsTimeout)
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
pub struct ManagedConnection<Conn: Clone, Async: FutureForm, Timer> {
    authenticated: Authenticated<Conn, Async>,
    multiplexer: Arc<Multiplexer>,
    timer: Timer,
}

impl<Conn: Clone, Async: FutureForm, Timer: Clone> Clone for ManagedConnection<Conn, Async, Timer> {
    fn clone(&self) -> Self {
        Self {
            authenticated: self.authenticated.clone(),
            multiplexer: self.multiplexer.clone(),
            timer: self.timer.clone(),
        }
    }
}

impl<Conn: Clone + PartialEq, Async: FutureForm, Timer> PartialEq
    for ManagedConnection<Conn, Async, Timer>
{
    fn eq(&self, other: &Self) -> bool {
        self.authenticated == other.authenticated
    }
}

impl<Conn: Clone, Async: FutureForm, Timer> ManagedConnection<Conn, Async, Timer> {
    /// Create a new managed connection.
    pub const fn new(
        authenticated: Authenticated<Conn, Async>,
        multiplexer: Arc<Multiplexer>,
        timer: Timer,
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
    pub const fn authenticated(&self) -> &Authenticated<Conn, Async> {
        &self.authenticated
    }

    /// Access the multiplexer.
    #[must_use]
    pub const fn multiplexer(&self) -> &Arc<Multiplexer> {
        &self.multiplexer
    }

    /// Consume the managed connection, returning the authenticated connection.
    #[must_use]
    pub fn into_authenticated(self) -> Authenticated<Conn, Async> {
        self.authenticated
    }

    /// Generate the next request ID for this connection.
    pub fn next_request_id(&self) -> RequestId {
        self.multiplexer.next_request_id()
    }
}

/// RAII guard that removes a pending entry if the call future is dropped
/// before it resolves.
///
/// This is what makes a [`call`](ManagedConnection::call) **cancel-safe**:
/// dropping the future — because an outer deadline elapsed, a `select!`
/// arm lost, or a shutdown token fired — runs [`Drop`], which removes the
/// registered [`oneshot::Sender`](futures::channel::oneshot) so no entry
/// leaks in the multiplexer's pending map.
///
/// [`disarm`](Self::disarm) is called once the call resolves normally
/// (response received or channel dropped), so the guard does not
/// double-remove.
struct PendingGuard {
    multiplexer: Arc<Multiplexer>,
    req_id: RequestId,
    armed: bool,
}

impl PendingGuard {
    const fn new(multiplexer: Arc<Multiplexer>, req_id: RequestId) -> Self {
        Self {
            multiplexer,
            req_id,
            armed: true,
        }
    }

    /// Stop the guard from removing the entry on drop (the call resolved).
    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.armed {
            // Synchronous best-effort removal. On the rare `try_lock` miss
            // the straggler is reaped by the next `resolve_pending` /
            // `cancel_all_pending`; it can never be mismatched because
            // `RequestId`s are never reused. See
            // `Multiplexer::try_cancel_pending`.
            let _removed = self.multiplexer.try_cancel_pending(&self.req_id);
        }
    }
}

/// Error from a roundtrip [`call`](ManagedConnection::call).
#[derive(Debug, Clone, thiserror::Error)]
pub enum CallError<SendErr: core::error::Error> {
    /// The transport failed to send the request.
    #[error("send error: {0}")]
    Send(SendErr),

    /// The response channel was dropped (peer disconnected).
    #[error("response channel dropped")]
    ResponseDropped,

    /// The call timed out waiting for a response.
    #[error("call timed out")]
    Timeout,
}

impl<SendErr: core::error::Error> CallError<SendErr> {
    /// Short error name suitable for JS `Error.name` or logging.
    #[must_use]
    pub const fn error_name(&self) -> &'static str {
        match self {
            Self::Send(_) => "CallSendError",
            Self::ResponseDropped => "CallResponseDropped",
            Self::Timeout => "CallTimeout",
        }
    }

    /// Map the inner send-error type.
    pub fn map_send<OutErr: core::error::Error>(
        self,
        f: impl FnOnce(SendErr) -> OutErr,
    ) -> CallError<OutErr> {
        match self {
            Self::Send(e) => CallError::Send(f(e)),
            Self::ResponseDropped => CallError::ResponseDropped,
            Self::Timeout => CallError::Timeout,
        }
    }
}

/// Trait for performing roundtrip calls on a [`ManagedConnection`].
///
/// This trait bridges the `Sendable` and `Local` implementations of
/// [`ManagedConnection::call`], making the method available in code
/// generic over `Async: FutureForm`.
pub trait ManagedCall<Async: FutureForm, WireMsg>: Sized {
    /// The connection's send-error type.
    type SendError: core::error::Error;

    /// Send a [`BatchSyncRequest`] and await the matching [`BatchSyncResponse`].
    ///
    /// # Errors
    ///
    /// Returns [`CallError`] on send failure, dropped response, or timeout.
    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> Async::Future<'_, Result<BatchSyncResponse, CallError<Self::SendError>>>;
}

impl<Conn: Clone, Timer> ManagedConnection<Conn, Sendable, Timer>
where
    Timer: Timeout<Sendable> + Send + Sync,
{
    /// Send a [`BatchSyncRequest`] and await the matching [`BatchSyncResponse`].
    ///
    /// The request is wrapped in `SyncMessage::BatchSyncRequest`, converted to
    /// the handler's wire type `WireMsg` via `From<SyncMessage>`, and sent via
    /// `Connection::send`. The response arrives via the `Multiplexer`'s
    /// pending map, which is resolved by the `Subduction` listen loop.
    ///
    /// # Errors
    ///
    /// Returns [`CallError`] on send failure, dropped response, or timeout.
    pub fn call_inner<WireMsg>(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'_, Result<BatchSyncResponse, CallError<Conn::SendError>>>
    where
        Conn: Connection<Sendable, WireMsg> + PartialEq + Send + Sync,
        Conn::SendError: Send + 'static,
        WireMsg: Encode + Decode + From<SyncMessage> + Send,
    {
        let time_limit = timeout.unwrap_or(self.multiplexer.default_idle_timeout());
        async move {
            let req_id = req.req_id;
            let rx = self.multiplexer.register_pending(req_id).await;
            // Cancel-safe from here: if this future is dropped (outer
            // deadline / select / shutdown) before resolving, the guard
            // removes the pending entry on drop.
            let mut guard = PendingGuard::new(Arc::clone(&self.multiplexer), req_id);

            let wire_msg: WireMsg = SyncMessage::BatchSyncRequest(req).into();
            Connection::<Sendable, WireMsg>::send(&self.authenticated, &wire_msg)
                .await
                .map_err(CallError::Send)?;

            match self.timer.timeout(time_limit, rx.boxed()).await {
                Ok(Ok(resp)) => {
                    // Entry already removed by `resolve_pending`.
                    guard.disarm();
                    tracing::debug!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(_)) => {
                    // Sender dropped (e.g. `cancel_all_pending` on disconnect)
                    // already removed the entry.
                    guard.disarm();
                    tracing::debug!("request {req_id:?} response channel dropped");
                    Err(CallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    // Deadline elapsed with the entry still registered; let
                    // the armed guard remove it on drop.
                    tracing::warn!("request {req_id:?} timed out after {time_limit:?}");
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

impl<Conn, WireMsg, Timer> ManagedCall<Sendable, WireMsg>
    for ManagedConnection<Conn, Sendable, Timer>
where
    Conn: Connection<Sendable, WireMsg> + PartialEq + Clone + Send + Sync,
    Conn::SendError: Send + 'static,
    WireMsg: Encode + Decode + From<SyncMessage> + Send,
    Timer: Timeout<Sendable> + Send + Sync,
{
    type SendError = Conn::SendError;

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'_, Result<BatchSyncResponse, CallError<Self::SendError>>> {
        self.call_inner(req, timeout)
    }
}

impl<Conn: Clone, Timer> ManagedConnection<Conn, Local, Timer>
where
    Timer: Timeout<Local>,
{
    /// Send a [`BatchSyncRequest`] and await the matching [`BatchSyncResponse`].
    ///
    /// See the `Sendable` variant for full documentation.
    ///
    /// # Errors
    ///
    /// Returns [`CallError`] on send failure, dropped response, or timeout.
    pub fn call_inner<WireMsg>(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::LocalBoxFuture<'_, Result<BatchSyncResponse, CallError<Conn::SendError>>>
    where
        Conn: Connection<Local, WireMsg> + PartialEq,
        WireMsg: Encode + Decode + From<SyncMessage>,
    {
        let time_limit = timeout.unwrap_or(self.multiplexer.default_idle_timeout());
        async move {
            let req_id = req.req_id;
            let rx = self.multiplexer.register_pending(req_id).await;
            // Cancel-safe from here: see the `Sendable` variant.
            let mut guard = PendingGuard::new(Arc::clone(&self.multiplexer), req_id);

            let wire_msg: WireMsg = SyncMessage::BatchSyncRequest(req).into();
            Connection::<Local, WireMsg>::send(&self.authenticated, &wire_msg)
                .await
                .map_err(CallError::Send)?;

            match self.timer.timeout(time_limit, rx.boxed_local()).await {
                Ok(Ok(resp)) => {
                    guard.disarm();
                    tracing::debug!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(_)) => {
                    guard.disarm();
                    tracing::debug!("request {req_id:?} response channel dropped");
                    Err(CallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    tracing::warn!("request {req_id:?} timed out after {time_limit:?}");
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed_local()
    }
}

impl<Conn, WireMsg, Timer> ManagedCall<Local, WireMsg> for ManagedConnection<Conn, Local, Timer>
where
    Conn: Connection<Local, WireMsg> + PartialEq + Clone,
    WireMsg: Encode + Decode + From<SyncMessage>,
    Timer: Timeout<Local>,
{
    type SendError = Conn::SendError;

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> futures::future::LocalBoxFuture<'_, Result<BatchSyncResponse, CallError<Self::SendError>>>
    {
        self.call_inner(req, timeout)
    }
}
