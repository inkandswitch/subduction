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

use alloc::sync::Arc;

use crate::{
    connection::message::{BatchSyncResponse, RequestId},
    peer::id::PeerId,
};

/// Default per-call idle timeout.
///
/// This is an **idle** timeout, not a total-deadline: it is the maximum
/// tolerable gap between *completions* on a connection, not the total
/// lifetime of a single request. Each completion on the connection
/// re-arms it (see [`Multiplexer::completion_epoch`]), so a request
/// queued behind others that are completing stays patient and only fails
/// once the connection has been silent for this long.
///
/// Single source of truth: the builder default and the Wasm bindings both
/// reference this constant.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Generic request-response multiplexer.
///
/// Holds the pending-response map, request ID counter, idle-timeout
/// default, and a shared completion epoch. Transport backends embed this
/// and delegate their request-response impls to it.
///
/// # Progress-aware ("idle") timeouts
///
/// A multiplexed connection may carry many concurrent in-flight requests
/// (e.g. one per document in a large sync). Timing each request out on an
/// *absolute* deadline since-send is wrong: a request merely queued
/// behind others that are completing would false-fire even though the
/// connection is healthy and making progress.
///
/// Instead, completions advance a shared [`completion_epoch`]. A waiting
/// `call` snapshots the epoch and waits in `idle_timeout`-sized windows;
/// if the epoch advanced over a window, the connection made progress
/// (some request completed) and the caller re-arms rather than failing. A
/// request times out only after a full window passes with **no**
/// completion anywhere on the connection.
///
/// [`completion_epoch`]: Multiplexer::completion_epoch
#[derive(Debug)]
pub struct Multiplexer {
    peer_id: PeerId,
    req_id_counter: AtomicU64,
    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,
    default_idle_timeout: Duration,

    /// Monotonic count of completions (responses matched to a waiting
    /// caller) on this connection.
    ///
    /// Shared (`Arc`) across multiplexer clones so every concurrent caller
    /// observes the same progress signal. Bumped in
    /// [`resolve_pending`](Self::resolve_pending); read by waiting `call`s
    /// to decide whether the connection is alive. This is the
    /// "reset on progress" fuse: completions re-arm it, idle gaps burn it
    /// down.
    completion_epoch: Arc<AtomicU64>,
}

impl Clone for Multiplexer {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            req_id_counter: AtomicU64::new(self.req_id_counter.load(Ordering::Relaxed)),
            pending: Mutex::new(Map::new()),
            default_idle_timeout: self.default_idle_timeout,
            completion_epoch: Arc::clone(&self.completion_epoch),
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
    pub fn new(peer_id: PeerId, default_idle_timeout: Duration) -> Self {
        Self {
            peer_id,
            req_id_counter: AtomicU64::new({
                let mut buf = [0u8; 8];
                getrandom::getrandom(&mut buf).expect("RNG unavailable");
                u64::from_be_bytes(buf)
            }),
            pending: Mutex::new(Map::new()),
            default_idle_timeout,
            completion_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The remote peer's identity.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The default per-call idle timeout: the maximum tolerable gap
    /// between completions on this connection before a waiting call gives
    /// up. See [`DEFAULT_IDLE_TIMEOUT`] and the type-level docs.
    pub const fn default_idle_timeout(&self) -> Duration {
        self.default_idle_timeout
    }

    /// The current completion epoch: a monotonic count of responses
    /// matched to a waiting caller on this connection.
    ///
    /// A waiting `call` compares this across an idle window to decide
    /// whether the connection made progress (stay patient) or went silent
    /// (time out). The value itself is meaningless in isolation; only
    /// *changes* matter.
    #[must_use]
    pub fn completion_epoch(&self) -> u64 {
        self.completion_epoch.load(Ordering::Acquire)
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

    /// Cancel every pending call, dropping their response senders.
    ///
    /// Called when the connection backing this multiplexer is torn down
    /// (disconnect / removal). Dropping the [`oneshot::Sender`]s resolves
    /// any awaiting receiver with [`CallError::ResponseDropped`]
    /// immediately, rather than letting in-flight calls strand for the
    /// full per-call timeout. (The multiplexer can outlive its removal
    /// from the connection map because in-flight calls hold `Arc`
    /// clones of it.)
    pub async fn cancel_all_pending(&self) {
        let mut pending = self.pending.lock().await;
        let n = pending.len();
        pending.clear();
        tracing::debug!(
            "cancelled {n} pending call(s) on multiplexer for peer {:?}",
            self.peer_id
        );
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
            // Progress beat: a request completed on this connection. Bump
            // the shared epoch so any concurrently-waiting `call` observes
            // that the connection is alive and re-arms its idle window.
            // Only genuine completions count — `cancel_all_pending` (on
            // disconnect) deliberately does NOT bump, since a torn-down
            // connection is not making progress.
            self.completion_epoch.fetch_add(1, Ordering::Release);
            drop(tx.send(resp.clone()));
            tracing::debug!("routed BatchSyncResponse for {req_id:?} to pending caller");
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        connection::message::SyncResult, remote_heads::RemoteHeads,
    };
    use core::time::Duration;
    use sedimentree_core::id::SedimentreeId;

    fn test_mux() -> Multiplexer {
        Multiplexer::new(PeerId::new([1u8; 32]), Duration::from_secs(5))
    }

    fn response_for(req_id: RequestId) -> BatchSyncResponse {
        BatchSyncResponse {
            req_id,
            id: SedimentreeId::new([2u8; 32]),
            result: SyncResult::NotFound,
            responder_heads: RemoteHeads::default(),
        }
    }

    #[tokio::test]
    async fn completion_epoch_starts_at_zero() {
        let mux = test_mux();
        assert_eq!(mux.completion_epoch(), 0);
    }

    #[tokio::test]
    async fn resolve_pending_advances_completion_epoch() {
        let mux = test_mux();
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        let before = mux.completion_epoch();
        assert!(mux.resolve_pending(&response_for(req_id)).await);
        assert_eq!(
            mux.completion_epoch(),
            before + 1,
            "a delivered response must advance the completion epoch"
        );
    }

    #[tokio::test]
    async fn unmatched_resolve_does_not_advance_epoch() {
        let mux = test_mux();
        let unknown = mux.next_request_id();

        let before = mux.completion_epoch();
        assert!(!mux.resolve_pending(&response_for(unknown)).await);
        assert_eq!(
            mux.completion_epoch(),
            before,
            "a response with no pending caller is not progress"
        );
    }

    #[tokio::test]
    async fn cancel_all_pending_does_not_advance_epoch() {
        // A torn-down connection is not "making progress" — disconnect
        // teardown must not look like a completion to waiting callers.
        let mux = test_mux();
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        let before = mux.completion_epoch();
        mux.cancel_all_pending().await;
        assert_eq!(
            mux.completion_epoch(),
            before,
            "cancel_all_pending must not advance the completion epoch"
        );
    }

    #[tokio::test]
    async fn completion_epoch_is_shared_across_clones() {
        // The epoch is the cross-caller progress signal; clones (one per
        // concurrent caller path) must observe the same counter.
        let mux = test_mux();
        let clone = mux.clone();
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        assert!(mux.resolve_pending(&response_for(req_id)).await);
        assert_eq!(
            clone.completion_epoch(),
            mux.completion_epoch(),
            "a clone must observe completions resolved on the original"
        );
        assert!(clone.completion_epoch() >= 1);
    }

    #[tokio::test]
    async fn cancel_all_pending_drops_registered_senders() {
        let mux = test_mux();
        let req_id = mux.next_request_id();
        let rx = mux.register_pending(req_id).await;

        mux.cancel_all_pending().await;

        // Short timeout so a regression that leaves the sender alive fails
        // fast here instead of hanging the receiver for the full call limit.
        let resolved = tokio::time::timeout(Duration::from_millis(200), rx)
            .await
            .expect("receiver must resolve promptly; cancel_all_pending did not drop the sender");
        assert!(
            resolved.is_err(),
            "receiver must resolve as Canceled after cancel_all_pending"
        );
    }

    /// `cancel_all_pending` resolves every registered receiver to `Err`,
    /// for any number of pending requests (including zero).
    #[cfg(feature = "bolero")]
    #[test]
    fn cancel_all_pending_resolves_every_receiver() {
        bolero::check!().with_type::<u8>().for_each(|n| {
            // Keep per-iteration runtimes cheap; n in 0..=15.
            let n = usize::from(n % 16);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let mux = test_mux();
                let mut rxs = alloc::vec::Vec::with_capacity(n);
                for _ in 0..n {
                    let id = mux.next_request_id();
                    rxs.push(mux.register_pending(id).await);
                }

                mux.cancel_all_pending().await;

                for rx in rxs {
                    let resolved = tokio::time::timeout(Duration::from_millis(200), rx)
                        .await
                        .expect("every receiver must resolve promptly after cancel");
                    assert!(
                        resolved.is_err(),
                        "every receiver must resolve as Canceled after cancel_all_pending"
                    );
                }
            });
        });
    }

    /// `cancel_all_pending` is idempotent and does not poison the
    /// multiplexer: a request registered after the cancels stays pending
    /// and is itself cancellable.
    #[cfg(feature = "bolero")]
    #[test]
    fn cancel_all_pending_is_idempotent_and_does_not_poison() {
        bolero::check!().with_type::<u8>().for_each(|n| {
            let n = usize::from(n % 16);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let mux = test_mux();
                // Hold the receivers alive so the N senders stay registered
                // in the pending map until we cancel.
                let mut rxs = alloc::vec::Vec::with_capacity(n);
                for _ in 0..n {
                    let id = mux.next_request_id();
                    rxs.push(mux.register_pending(id).await);
                }

                // Double cancel must neither panic nor hang.
                mux.cancel_all_pending().await;
                mux.cancel_all_pending().await;
                drop(rxs);

                // A request registered AFTER the cancels must stay pending
                // (the cancels must not have poisoned the mux).
                let id = mux.next_request_id();
                let mut rx = mux.register_pending(id).await;
                let still_pending = tokio::time::timeout(Duration::from_millis(50), &mut rx).await;
                assert!(
                    still_pending.is_err(),
                    "a freshly-registered request must remain pending after prior cancels"
                );

                // The new request is itself cancellable.
                mux.cancel_all_pending().await;
                let resolved = tokio::time::timeout(Duration::from_millis(200), rx)
                    .await
                    .expect("post-cancel receiver must resolve promptly");
                assert!(
                    resolved.is_err(),
                    "the freshly-registered request must be cancellable in turn"
                );
            });
        });
    }

    /// Always-on smoke check (the bolero properties above are gated).
    #[tokio::test]
    async fn cancel_all_pending_on_empty_is_a_noop_and_leaves_mux_usable() {
        let mux = test_mux();
        mux.cancel_all_pending().await;

        // A freshly-registered request is not pre-cancelled.
        let id = mux.next_request_id();
        let mut rx = mux.register_pending(id).await;
        let still_pending = tokio::time::timeout(Duration::from_millis(50), &mut rx).await;
        assert!(
            still_pending.is_err(),
            "empty cancel must not poison the mux for future registrations"
        );
        drop(rx);
    }
}
