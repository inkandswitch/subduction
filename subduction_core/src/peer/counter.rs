//! Per-peer monotonic message counter.
//!
//! [`PeerCounter`] provides a shared, lock-free counter per connected peer.
//! It is the canonical way to stamp outgoing messages with a monotonic
//! sequence number, ensuring receivers can detect out-of-order or stale
//! messages on non-TCP transports.
//!
//! The counter is shared across all handlers ([`SyncHandler`], ephemeral,
//! keyhive, etc.) and [`Subduction`] itself, so that every message to a
//! given peer draws from the same monotonic sequence.
//!
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//! [`Subduction`]: crate::subduction::Subduction

use alloc::sync::Arc;
use async_lock::Mutex;
use core::sync::atomic::{AtomicU64, Ordering};

use sedimentree_core::collections::Map;

use crate::peer::id::PeerId;

/// A shared, per-peer monotonic message counter.
///
/// Each peer's counter is an [`AtomicU64`] behind an [`Arc`], so counter
/// increments are lock-free once the per-peer entry exists. The outer
/// [`Mutex`] is only held briefly to insert new peers.
///
/// ```ignore
/// let counter = PeerCounter::default();
/// let seq = counter.next(peer_id).await;
/// ```
#[derive(Debug, Default, Clone)]
pub struct PeerCounter(Arc<Mutex<Map<PeerId, Arc<AtomicU64>>>>);

impl PeerCounter {
    /// Get the next counter value for a peer, incrementing atomically.
    ///
    /// The first call for a given peer returns 1. Subsequent calls return
    /// strictly increasing values. The counter is lock-free after the
    /// first call (only the map insertion requires the mutex).
    pub async fn next(&self, peer: PeerId) -> u64 {
        let counter = {
            let mut map = self.0.lock().await;
            map.entry(peer).or_default().clone()
        };
        counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Raise a peer's counter so the next [`next`](Self::next) returns a value
    /// strictly greater than `floor`, without ever lowering it.
    ///
    /// This is for callers whose identity is stable but whose in-memory counter
    /// can be reconstructed from scratch (e.g. a hibernatable Cloudflare Durable
    /// Object: it keeps a persisted `peer_id` but rebuilds the `PeerCounter`
    /// each time the isolate is re-created). Seeding the counter above every
    /// value handed out in prior lifetimes keeps the sequence monotonic across
    /// restarts, so receivers' staleness filters (drop `counter <= last seen`)
    /// don't blackhole post-hibernation updates.
    ///
    /// `floor` is clamped to `u64::MAX - 1`: [`next`](Self::next) increments
    /// with `fetch_add(1)`, so a counter seeded to `u64::MAX` would wrap to `0`
    /// on the next stamp and break monotonicity. `u64::MAX` is therefore not a
    /// representable stamp — the largest value `next` can return is `u64::MAX`,
    /// reached only by natural increment, never by seeding.
    pub async fn advance_to(&self, peer: PeerId, floor: u64) {
        let floor = floor.min(u64::MAX - 1);
        let counter = {
            let mut map = self.0.lock().await;
            map.entry(peer).or_default().clone()
        };
        counter.fetch_max(floor, Ordering::Relaxed);
    }

    /// Remove the counter for a peer that has fully disconnected.
    ///
    /// Call this from connection cleanup paths when a peer's last
    /// connection is removed. If the peer reconnects, a fresh counter
    /// starting at 1 will be created on the next [`next`](Self::next) call.
    pub async fn clear_peer(&self, peer: &PeerId) {
        self.0.lock().await.remove(peer);
    }

    /// Remove all per-peer counters.
    ///
    /// Call this when all connections are being torn down (e.g., `disconnect_all`).
    pub async fn clear_all(&self) {
        self.0.lock().await.clear();
    }

    /// The current counter value for a peer without incrementing it, or
    /// `None` if the peer has no counter entry (never stamped, or cleared).
    ///
    /// Test-only observability for asserting that teardown does not reset
    /// a still-connected peer's counter.
    #[cfg(any(feature = "test_utils", test))]
    pub async fn peek(&self, peer: &PeerId) -> Option<u64> {
        self.0
            .lock()
            .await
            .get(peer)
            .map(|c| c.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn advance_to_seeds_then_next_exceeds_floor() {
        let counter = PeerCounter::default();
        let peer = PeerId::new([7u8; 32]);

        counter.advance_to(peer, 1000).await;
        // The next stamp is strictly greater than the floor.
        assert_eq!(counter.next(peer).await, 1001);
        assert_eq!(counter.next(peer).await, 1002);
    }

    #[tokio::test]
    async fn advance_to_never_lowers_a_counter() {
        let counter = PeerCounter::default();
        let peer = PeerId::new([9u8; 32]);

        assert_eq!(counter.next(peer).await, 1);
        assert_eq!(counter.next(peer).await, 2);
        // A lower (or equal) floor must not rewind the counter.
        counter.advance_to(peer, 1).await;
        assert_eq!(counter.next(peer).await, 3);
        // A higher floor jumps it forward.
        counter.advance_to(peer, 500).await;
        assert_eq!(counter.next(peer).await, 501);
    }

    #[tokio::test]
    async fn advance_to_clamps_floor_below_wraparound() {
        let counter = PeerCounter::default();
        let peer = PeerId::new([11u8; 32]);

        // A floor of u64::MAX must not force the counter to the wrapping value:
        // `next` (fetch_add(1)) would otherwise return 0 and rewind the
        // sequence. Clamped, the next stamp is the largest representable value.
        counter.advance_to(peer, u64::MAX).await;
        assert_eq!(counter.next(peer).await, u64::MAX);
    }
}
