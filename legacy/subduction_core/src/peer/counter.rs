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
//! A peer's counter is never reset, even on full disconnect: receivers keep
//! a never-reset high-water mark (see `FilteredHeadsNotifier`), so a
//! restarted sequence would be dropped as stale. Embedders with a wall
//! clock should seed the counter ([`PeerCounter::with_seed`] +
//! `wall_clock_seed`) so a restarted *process* also resumes above previous
//! values. The default seed is zero: in-process monotonicity only.
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
/// Entries are never removed (the monotonicity contract outlives any
/// connection), so the map grows with distinct peers stamped per process
/// lifetime — ~100 bytes each. Deliberate trade-off; extreme peer churn
/// may eventually want era-based GC.
///
/// Clones share the same counters and seed.
///
/// ```ignore
/// let counter = PeerCounter::default();
/// let seq = counter.next(peer_id).await;
/// ```
#[derive(Debug, Clone)]
pub struct PeerCounter(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    counters: Mutex<Map<PeerId, Arc<AtomicU64>>>,
    seed: fn() -> u64,
}

impl PeerCounter {
    /// Create a counter whose fresh per-peer entries start from `seed()`,
    /// evaluated once per peer at its first stamp.
    ///
    /// Embedders with a wall clock should pass `wall_clock_seed` (or a
    /// `Date.now()`-based equivalent on Wasm) so a restarted process
    /// resumes above its previous sequence; see the module docs.
    #[must_use]
    pub fn with_seed(seed: fn() -> u64) -> Self {
        Self(Arc::new(Inner {
            counters: Mutex::new(Map::new()),
            seed,
        }))
    }

    /// Get the next counter value for a peer, incrementing atomically.
    ///
    /// Values are strictly increasing per peer for the life of the process.
    /// The counter is lock-free after the first call (only the map
    /// insertion requires the mutex).
    pub async fn next(&self, peer: PeerId) -> u64 {
        let counter = {
            let mut map = self.0.counters.lock().await;
            map.entry(peer)
                .or_insert_with(|| Arc::new(AtomicU64::new((self.0.seed)())))
                .clone()
        };
        counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The current counter value for a peer without incrementing it, or
    /// `None` if the peer has no counter entry (never stamped).
    ///
    /// Test-only observability for asserting that counters survive
    /// disconnects.
    #[cfg(any(feature = "test_utils", test))]
    pub async fn peek(&self, peer: &PeerId) -> Option<u64> {
        self.0
            .counters
            .lock()
            .await
            .get(peer)
            .map(|c| c.load(Ordering::Relaxed))
    }
}

impl Default for PeerCounter {
    fn default() -> Self {
        Self::with_seed(zero_seed)
    }
}

const fn zero_seed() -> u64 {
    0
}

/// Wall-clock counter seed: microseconds since the Unix epoch. Opt-in for
/// embedders via [`PeerCounter::with_seed`]; core never reads the clock.
///
/// Restart monotonicity holds unless the process sustained >1M msgs/sec to
/// one peer or the clock stepped backwards between runs. Not available on
/// `wasm32-unknown-unknown` (`SystemTime::now()` panics there); Wasm
/// embedders should seed in the same unit — `Date.now() * 1_000.0` — so a
/// peer migrating between embedders stays above its old high-water mark.
#[cfg(all(
    feature = "system_time",
    not(all(target_family = "wasm", target_os = "unknown"))
))]
#[cfg_attr(
    docsrs,
    doc(cfg(all(
        feature = "system_time",
        not(all(target_family = "wasm", target_os = "unknown"))
    )))
)]
#[must_use]
pub fn wall_clock_seed() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(since_epoch) => u64::try_from(since_epoch.as_micros()).unwrap_or(u64::MAX),
        Err(e) => {
            // Pre-epoch clock: silently degrading to unseeded would
            // reintroduce the failure this seed exists to prevent.
            tracing::warn!(
                error = %e,
                "system clock is before the Unix epoch; send counters are \
                 unseeded and may be dropped as stale by peers after restart"
            );
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fresh per-peer entries start from the injected seed; the first stamp
    /// is `seed + 1`.
    #[tokio::test]
    async fn fresh_counter_starts_from_injected_seed() {
        fn seed() -> u64 {
            1_000
        }

        let counter = PeerCounter::with_seed(seed);
        assert_eq!(counter.next(PeerId::new([1u8; 32])).await, 1_001);
        assert_eq!(counter.next(PeerId::new([1u8; 32])).await, 1_002);
        assert_eq!(counter.next(PeerId::new([2u8; 32])).await, 1_001);
    }

    /// The seed is evaluated once *per peer*, at that peer's first stamp —
    /// not once per counter. A constant seed can't distinguish the two, so
    /// this uses a counting seed: each fresh peer gets a strictly larger
    /// base, and a repeat stamp does not re-seed.
    #[tokio::test]
    async fn seed_is_evaluated_per_peer_at_first_stamp() {
        use core::sync::atomic::{AtomicU64, Ordering};

        static CALLS: AtomicU64 = AtomicU64::new(0);
        fn counting_seed() -> u64 {
            (CALLS.fetch_add(1, Ordering::Relaxed) + 1) * 1_000
        }

        let counter = PeerCounter::with_seed(counting_seed);

        assert_eq!(counter.next(PeerId::new([1u8; 32])).await, 1_001);
        assert_eq!(
            counter.next(PeerId::new([2u8; 32])).await,
            2_001,
            "a fresh peer must evaluate the seed anew"
        );
        assert_eq!(
            counter.next(PeerId::new([1u8; 32])).await,
            1_002,
            "a repeat stamp must not re-seed"
        );
    }
}
