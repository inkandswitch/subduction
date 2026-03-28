//! Time-bucketed nonce deduplication cache.
//!
//! Each `(sender, topic)` pair maintains two time buckets:
//! _current_ and _previous_. When a bucket's age exceeds the
//! configured window duration, it is rotated out. This ensures
//! that an attacker cannot flush nonces by volume — eviction is
//! solely time-driven.

use sedimentree_core::collections::{Map, Set};

use crate::topic::Topic;
use subduction_core::peer::id::PeerId;

/// A single time-bucketed set of nonces.
#[derive(Debug, Clone)]
struct NonceBucket {
    nonces: Set<u64>,
    rotated_at_ms: u64,
}

impl NonceBucket {
    fn new(now_ms: u64) -> Self {
        Self {
            nonces: Set::new(),
            rotated_at_ms: now_ms,
        }
    }
}

/// Time-bucketed nonce deduplication cache.
///
/// Maintains two buckets per `(sender, topic)` pair. On each check,
/// if the current bucket is older than `window_duration_ms`, the
/// previous bucket is discarded, current becomes previous, and a
/// fresh current is created.
///
/// A nonce is considered duplicate if it appears in _either_ bucket.
/// This gives each nonce a retention period of 1-2 window durations.
#[derive(Debug, Clone)]
pub struct NonceCache {
    windows: Map<(PeerId, Topic), [NonceBucket; 2]>,
    window_duration_ms: u64,
}

impl NonceCache {
    /// Create a new nonce cache with the given window duration.
    #[must_use]
    pub fn new(window_duration_ms: u64) -> Self {
        Self {
            windows: Map::new(),
            window_duration_ms,
        }
    }

    /// Check whether `nonce` has been seen for `(sender, topic)`.
    ///
    /// Returns `true` if the nonce is _new_ (not a duplicate) and
    /// inserts it into the current bucket. Returns `false` if the
    /// nonce was already seen (duplicate — should be dropped).
    ///
    /// Performs bucket rotation if the current bucket has expired.
    pub fn check_and_insert(
        &mut self,
        sender: PeerId,
        topic: Topic,
        nonce: u64,
        now_ms: u64,
    ) -> bool {
        let key = (sender, topic);
        let [current, previous] = self
            .windows
            .entry(key)
            .or_insert_with(|| [NonceBucket::new(now_ms), NonceBucket::new(now_ms)]);

        // Rotate if the current bucket has expired.
        if now_ms.saturating_sub(current.rotated_at_ms) > self.window_duration_ms {
            *previous = core::mem::replace(current, NonceBucket::new(now_ms));
        }

        // Check for duplicate in both buckets.
        if current.nonces.contains(&nonce) || previous.nonces.contains(&nonce) {
            return false;
        }

        current.nonces.insert(nonce);
        true
    }

    /// Remove all entries for a given peer (called on disconnect).
    pub fn remove_peer(&mut self, peer: PeerId) {
        self.windows.retain(|(p, _), _| *p != peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        PeerId::new([n; 32])
    }

    fn topic(n: u8) -> Topic {
        Topic::new([n; 32])
    }

    #[test]
    fn new_nonce_accepted() {
        let mut cache = NonceCache::new(30_000);
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
    }

    #[test]
    fn duplicate_nonce_rejected() {
        let mut cache = NonceCache::new(30_000);
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
        assert!(!cache.check_and_insert(peer(1), topic(1), 42, 1000));
    }

    #[test]
    fn same_nonce_different_sender_accepted() {
        let mut cache = NonceCache::new(30_000);
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
        assert!(cache.check_and_insert(peer(2), topic(1), 42, 1000));
    }

    #[test]
    fn same_nonce_different_topic_accepted() {
        let mut cache = NonceCache::new(30_000);
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
        assert!(cache.check_and_insert(peer(1), topic(2), 42, 1000));
    }

    #[test]
    fn nonce_survives_in_previous_bucket() {
        let mut cache = NonceCache::new(1000);
        // Insert at t=100
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 100));
        // Advance past one window — nonce rotates to previous bucket
        assert!(!cache.check_and_insert(peer(1), topic(1), 42, 1200));
    }

    #[test]
    fn nonce_evicted_after_two_windows() {
        let mut cache = NonceCache::new(1000);
        // Insert at t=100
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 100));
        // First rotation at t=1200 — nonce moves to previous
        assert!(cache.check_and_insert(peer(1), topic(1), 99, 1200));
        // Second rotation at t=2300 — previous (with nonce 42) is discarded
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 2300));
    }

    #[test]
    fn remove_peer_clears_entries() {
        let mut cache = NonceCache::new(30_000);
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
        assert!(cache.check_and_insert(peer(1), topic(2), 43, 1000));
        assert!(cache.check_and_insert(peer(2), topic(1), 44, 1000));

        cache.remove_peer(peer(1));

        // Peer 1 nonces are gone — re-insert succeeds
        assert!(cache.check_and_insert(peer(1), topic(1), 42, 1000));
        assert!(cache.check_and_insert(peer(1), topic(2), 43, 1000));
        // Peer 2 unaffected — duplicate still rejected
        assert!(!cache.check_and_insert(peer(2), topic(1), 44, 1000));
    }

    #[test]
    fn volume_cannot_flush_cache() {
        let mut cache = NonceCache::new(30_000);
        // Insert the target nonce
        assert!(cache.check_and_insert(peer(1), topic(1), 1, 1000));

        // Send 10,000 unique nonces — all at the same time (no rotation)
        for nonce in 2..10_002 {
            assert!(cache.check_and_insert(peer(1), topic(1), nonce, 1000));
        }

        // Target nonce is still tracked
        assert!(!cache.check_and_insert(peer(1), topic(1), 1, 1000));
    }
}
