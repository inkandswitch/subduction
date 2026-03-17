//! Request-response multiplexer.
//!
//! [`Multiplexer`] manages the pending-response map for correlating
//! outbound [`BatchSyncRequest`]s with inbound [`BatchSyncResponse`]s.
//!
//! The multiplexer manages:
//!
//! - `RequestId` generation (atomic counter seeded from the platform CSPRNG)
//! - A pending-response map (`RequestId` → oneshot sender)
//! - Response routing via [`resolve_pending`](Multiplexer::resolve_pending)
//!
//! Timeouts are the caller's responsibility (see [`ManagedConnection::call`]).
//!
//! [`BatchSyncRequest`]: crate::connection::message::BatchSyncRequest
//! [`BatchSyncResponse`]: crate::connection::message::BatchSyncResponse
//! [`ManagedConnection::call`]: crate::connection::managed::ManagedConnection::call

use core::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use futures::channel::oneshot;
use sedimentree_core::collections::Map;

use crate::{
    connection::message::{BatchSyncResponse, RequestId},
    peer::id::PeerId,
};

/// Generic request-response multiplexer.
///
/// Holds the pending-response map, request ID counter, and timeout
/// strategy. Transport backends embed this and delegate their
/// request-response impls to it.
#[derive(Debug)]
pub struct Multiplexer {
    peer_id: PeerId,
    req_id_counter: AtomicU64,
    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,
    default_time_limit: Duration,
}

impl Clone for Multiplexer {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            req_id_counter: AtomicU64::new(self.req_id_counter.load(Ordering::Relaxed)),
            pending: Mutex::new(Map::new()),
            default_time_limit: self.default_time_limit,
        }
    }
}

/// Error from a multiplexed call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CallError {
    /// The outbound channel was closed (transport shut down).
    #[error("outbound channel closed")]
    ChannelClosed,

    /// The oneshot response channel was dropped.
    #[error("response channel dropped")]
    ResponseDropped,

    /// The call timed out waiting for a response.
    #[error("call timed out")]
    Timeout,
}

impl Multiplexer {
    /// Create a new multiplexer.
    ///
    /// # Panics
    ///
    /// Panics if the platform's random number generator is unavailable.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn new(peer_id: PeerId, default_time_limit: Duration) -> Self {
        Self {
            peer_id,
            req_id_counter: AtomicU64::new({
                let mut buf = [0u8; 8];
                getrandom::getrandom(&mut buf).expect("RNG unavailable");
                u64::from_be_bytes(buf)
            }),
            pending: Mutex::new(Map::new()),
            default_time_limit,
        }
    }

    /// The remote peer's identity.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The default per-call time limit.
    pub const fn default_time_limit(&self) -> Duration {
        self.default_time_limit
    }

    /// Generate the next request ID.
    pub fn next_request_id(&self) -> RequestId {
        let counter = self.req_id_counter.fetch_add(1, Ordering::Relaxed);
        RequestId {
            requestor: self.peer_id,
            nonce: counter,
        }
    }

    /// Register a pending call and return the receiver.
    ///
    /// The caller should:
    /// 1. Call this to get a `(RequestId, oneshot::Receiver)`
    /// 2. Send the request bytes via the transport's outbound channel
    /// 3. Await the receiver (with timeout via [`wait_for_response`](Self::wait_for_response))
    pub async fn register_pending(
        &self,
        req_id: RequestId,
    ) -> oneshot::Receiver<BatchSyncResponse> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(req_id, tx);
        rx
    }

    /// Cancel a pending call (e.g., on timeout).
    pub async fn cancel_pending(&self, req_id: &RequestId) {
        self.pending.lock().await.remove(req_id);
    }

    /// Try to resolve a pending call with an inbound `BatchSyncResponse`.
    ///
    /// Returns `true` if the response matched a pending request and was
    /// delivered. Returns `false` if no matching pending request exists
    /// (the caller should forward the bytes to the normal recv path).
    pub async fn resolve_pending(&self, resp: &BatchSyncResponse) -> bool {
        let req_id = resp.req_id;
        let mut pending = self.pending.lock().await;
        if let Some(tx) = pending.remove(&req_id) {
            drop(tx.send(resp.clone()));
            tracing::debug!("routed BatchSyncResponse for {req_id:?} to pending caller");
            true
        } else {
            false
        }
    }
}
