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

/// Default per-call deadline applied when a caller passes `timeout: None`.
///
/// This is a **caller-side** deadline, not a transport-layer fuse: the
/// multiplexer itself holds no clock. A blocking
/// [`call`](crate::connection::managed::ManagedConnection::call) wraps its
/// (cancel-safe) wait in this deadline unless the caller supplies an
/// explicit one. It exists so that calls are *bounded by default* —
/// matching the Erlang/OTP `GenServer.call` convention — even on transports
/// (e.g. HTTP long-poll) whose recv loop does not otherwise guarantee an
/// eventual disconnect on a byte-alive but protocol-silent peer.
///
/// Single source of truth: the builder default and the Wasm bindings both
/// reference this constant.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Generic request-response multiplexer.
///
/// Holds the pending-response map, request ID counter, and the default
/// per-call deadline. Transport backends embed this and delegate their
/// request-response impls to it.
///
/// # Timeouts are caller policy, not a transport fuse
///
/// The multiplexer holds no timer. A blocking
/// [`call`](crate::connection::managed::ManagedConnection::call) layers an
/// optional deadline over a **cancel-safe** wait: dropping the call future
/// (because a deadline elapsed, a `select!` lost, or a shutdown token
/// fired) removes the pending entry. Disconnect resolves any in-flight call
/// via [`cancel_all_pending`](Self::cancel_all_pending).
#[derive(Debug)]
pub struct Multiplexer {
    peer_id: PeerId,
    req_id_counter: AtomicU64,
    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,
    default_idle_timeout: Duration,
}

impl Clone for Multiplexer {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            req_id_counter: AtomicU64::new(self.req_id_counter.load(Ordering::Relaxed)),
            pending: Mutex::new(Map::new()),
            default_idle_timeout: self.default_idle_timeout,
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
        }
    }

    /// The remote peer's identity.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The default per-call deadline applied when a caller passes
    /// `timeout: None`. See [`DEFAULT_IDLE_TIMEOUT`].
    pub const fn default_idle_timeout(&self) -> Duration {
        self.default_idle_timeout
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

    /// Synchronously cancel a pending call without `await`ing the lock.
    ///
    /// Returns `true` if the entry was removed, `false` if the pending
    /// lock was momentarily contended (a `try_lock` miss) and the removal
    /// was skipped.
    ///
    /// This exists for [`Drop`]-based cancellation of a call future, where
    /// `async` cleanup is impossible. The common case (no contention)
    /// removes the entry immediately. On the rare miss the straggler
    /// [`oneshot::Sender`] is reaped by the next
    /// [`resolve_pending`](Self::resolve_pending) or
    /// [`cancel_all_pending`](Self::cancel_all_pending) (disconnect) — it
    /// can never be mismatched because [`RequestId`]s are never reused. If
    /// metrics ever show meaningful straggler accumulation on busy,
    /// never-disconnecting connections, upgrade this to a `Spawn`-backed
    /// deferred removal.
    pub fn try_cancel_pending(&self, req_id: &RequestId) -> bool {
        if let Some(mut pending) = self.pending.try_lock() {
            pending.remove(req_id);
            true
        } else {
            false
        }
    }

    /// Number of currently-registered pending calls.
    ///
    /// Primarily for tests asserting that cancellation leaves no leak.
    pub async fn pending_len(&self) -> usize {
        self.pending.lock().await.len()
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
    use core::time::Duration;

    fn test_mux() -> Multiplexer {
        Multiplexer::new(PeerId::new([1u8; 32]), Duration::from_secs(5))
    }

    #[tokio::test]
    async fn try_cancel_pending_removes_uncontended_entry() {
        let mux = test_mux();
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;
        assert_eq!(mux.pending_len().await, 1);

        assert!(
            mux.try_cancel_pending(&req_id),
            "uncontended try_cancel_pending must succeed"
        );
        assert_eq!(
            mux.pending_len().await,
            0,
            "try_cancel_pending must remove the entry on success"
        );
    }

    #[tokio::test]
    async fn try_cancel_pending_reports_miss_under_contention() {
        let mux = test_mux();
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        // Hold the pending lock so the synchronous try_lock cannot acquire it.
        let held = mux.pending.lock().await;
        assert!(
            !mux.try_cancel_pending(&req_id),
            "try_cancel_pending must report a miss while the lock is held"
        );
        drop(held);

        // The straggler is still present; a backstop (cancel_all_pending) reaps it.
        assert_eq!(mux.pending_len().await, 1);
        mux.cancel_all_pending().await;
        assert_eq!(mux.pending_len().await, 0);
    }

    #[tokio::test]
    async fn default_idle_timeout_is_reported() {
        let mux = Multiplexer::new(PeerId::new([7u8; 32]), Duration::from_secs(12));
        assert_eq!(mux.default_idle_timeout(), Duration::from_secs(12));
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
