//! Time-bucketed nonce deduplication cache.
//!
//! Each `(sender, topic)` pair maintains two time buckets:
//! _current_ and _previous_. When a bucket's age exceeds the
//! configured window duration, it is rotated out. This ensures
//! that an attacker cannot flush nonces by volume — eviction is
//! solely time-driven.

use core::time::Duration;

use sedimentree_core::collections::{Map, Set};
use subduction_core::{peer::id::PeerId, timestamp::TimestampSeconds};

use crate::topic::Topic;

/// A single time-bucketed set of nonces.
#[derive(Debug, Clone)]
struct NonceBucket {
    nonces: Set<u64>,
    rotated_at: TimestampSeconds,
}

impl NonceBucket {
    // Cannot be `const fn`: under the `std` feature, `Set` aliases to
    // `HashSet`, whose `::new()` is not `const`. (It would be `const`-able
    // under `no_std` where `Set` aliases to `BTreeSet`, but the lint fires
    // on the `std` build and would fail to compile if applied unconditionally.)
    #[allow(clippy::missing_const_for_fn)]
    fn new(now: TimestampSeconds) -> Self {
        Self {
            nonces: Set::new(),
            rotated_at: now,
        }
    }
}

/// Time-bucketed nonce deduplication cache.
///
/// Maintains two buckets per `(sender, topic)` pair. On each check,
/// if the current bucket is older than `window_duration`, the
/// previous bucket is discarded, current becomes previous, and a
/// fresh current is created.
///
/// A nonce is considered duplicate if it appears in _either_ bucket.
/// This gives each nonce a retention period of 1-2 window durations.
#[derive(Debug, Clone)]
pub struct EphemeralNonceCache {
    windows: Map<(PeerId, Topic), [NonceBucket; 2]>,
    window_duration: Duration,
}

impl EphemeralNonceCache {
    /// Create a new nonce cache with the given window duration.
    // Cannot be `const fn`: under the `std` feature, `Map` aliases to
    // `HashMap`, whose `::new()` is not `const`. (It would be `const`-able
    // under `no_std` where `Map` aliases to `BTreeMap`, but the lint fires
    // on the `std` build and would fail to compile if applied unconditionally.)
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(window_duration: Duration) -> Self {
        Self {
            windows: Map::new(),
            window_duration,
        }
    }

    /// Check whether `nonce` has been seen for `(sender, topic)` without
    /// inserting it.
    ///
    /// Returns `true` if the nonce was already seen, `false` if it's
    /// unknown (or no entry exists yet for this `(sender, topic)`).
    ///
    /// This is the read-only fast path used _before_ signature
    /// verification: cross-edge duplicates short-circuit here, avoiding
    /// an Ed25519 verify per wasted hop. Because no nonce is ever
    /// inserted by this call, an attacker who can't produce a valid
    /// signature cannot pollute the cache by sending messages with
    /// chosen `(issuer, topic, nonce)` triples. The cache is only
    /// mutated by [`check_and_insert`](Self::check_and_insert), which
    /// is gated behind successful verification at the call site.
    ///
    /// Bucket rotation _is_ performed for existing keys so that nonces
    /// older than the retention window stop counting as duplicates.
    /// Rotation only ever mutates entries that already exist; missing
    /// keys remain absent (no spurious allocation from probe traffic).
    pub fn contains(
        &mut self,
        sender: PeerId,
        topic: Topic,
        nonce: u64,
        now: TimestampSeconds,
    ) -> bool {
        let key = (sender, topic);
        // Only rotate / read existing entries — never create one on the
        // pre-verify path, since the caller has not yet authenticated
        // the `sender`/issuer field.
        let Some([current, previous]) = self.windows.get_mut(&key) else {
            return false;
        };

        rotate_buckets(current, previous, now, self.window_duration);

        current.nonces.contains(&nonce) || previous.nonces.contains(&nonce)
    }

    /// Check whether `nonce` has been seen for `(sender, topic)`.
    ///
    /// Returns `true` if the nonce is _new_ (not a duplicate) and
    /// inserts it into the current bucket. Returns `false` if the
    /// nonce was already seen (duplicate — should be dropped).
    ///
    /// Performs bucket rotation if the current bucket has expired.
    ///
    /// Use this _after_ verifying the signature so that an unverified
    /// `sender` cannot inject entries; the read-only
    /// [`contains`](Self::contains) is the pre-verify fast path.
    pub fn check_and_insert(
        &mut self,
        sender: PeerId,
        topic: Topic,
        nonce: u64,
        now: TimestampSeconds,
    ) -> bool {
        let key = (sender, topic);
        let [current, previous] = self
            .windows
            .entry(key)
            .or_insert_with(|| [NonceBucket::new(now), NonceBucket::new(now)]);

        rotate_buckets(current, previous, now, self.window_duration);

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

/// Rotate `current` and `previous` based on the current bucket's age.
/// Extracted so [`EphemeralNonceCache::contains`] and
/// [`EphemeralNonceCache::check_and_insert`] share identical rotation
/// semantics — divergence would cause one path to see stale nonces the
/// other doesn't.
fn rotate_buckets(
    current: &mut NonceBucket,
    previous: &mut NonceBucket,
    now: TimestampSeconds,
    window_duration: Duration,
) {
    let age = current.rotated_at.abs_diff(now);
    if age > window_duration.saturating_mul(2) {
        // Long idle: both buckets are stale — reset entirely.
        *current = NonceBucket::new(now);
        *previous = NonceBucket::new(now);
    } else if age > window_duration {
        // Normal rotation: current → previous, fresh current.
        *previous = core::mem::replace(current, NonceBucket::new(now));
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

    fn ts(secs: u64) -> TimestampSeconds {
        TimestampSeconds::new(secs)
    }

    #[test]
    fn new_nonce_accepted() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
    }

    #[test]
    fn duplicate_nonce_rejected() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(!cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
    }

    #[test]
    fn same_nonce_different_sender_accepted() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(cache.check_and_insert(peer(2), topic(1), 42, ts(1000)));
    }

    #[test]
    fn same_nonce_different_topic_accepted() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(cache.check_and_insert(peer(1), topic(2), 42, ts(1000)));
    }

    #[test]
    fn nonce_survives_in_previous_bucket() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(2));
        // Insert at t=0
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // Advance past one window (>2s) — nonce rotates to previous but still seen
        assert!(!cache.check_and_insert(peer(1), topic(1), 42, ts(3)));
    }

    #[test]
    fn nonce_evicted_after_two_windows() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(2));
        // Insert at t=0
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // First rotation at t=3 — nonce 42 moves to previous
        assert!(cache.check_and_insert(peer(1), topic(1), 99, ts(3)));
        // Second rotation at t=6 — previous (with nonce 42) is discarded
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(6)));
    }

    #[test]
    fn remove_peer_clears_entries() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(cache.check_and_insert(peer(1), topic(2), 43, ts(1000)));
        assert!(cache.check_and_insert(peer(2), topic(1), 44, ts(1000)));

        cache.remove_peer(peer(1));

        // Peer 1 nonces are gone — re-insert succeeds
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(cache.check_and_insert(peer(1), topic(2), 43, ts(1000)));
        // Peer 2 unaffected — duplicate still rejected
        assert!(!cache.check_and_insert(peer(2), topic(1), 44, ts(1000)));
    }

    #[test]
    fn volume_cannot_flush_cache() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        // Insert the target nonce
        assert!(cache.check_and_insert(peer(1), topic(1), 1, ts(1000)));

        // Send 10,000 unique nonces — all at the same time (no rotation)
        for nonce in 2..10_002 {
            assert!(cache.check_and_insert(peer(1), topic(1), nonce, ts(1000)));
        }

        // Target nonce is still tracked
        assert!(!cache.check_and_insert(peer(1), topic(1), 1, ts(1000)));
    }

    #[test]
    fn long_idle_resets_both_buckets() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(2));
        // Insert at t=0
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // Still duplicate at t=0
        assert!(!cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // Jump past 2 * window (>4s) — both buckets stale, nonce accepted again
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(5)));
    }

    // ── contains() (read-only fast-path) ────────────────────────────────

    #[test]
    fn contains_returns_false_for_unknown_key() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(!cache.contains(peer(1), topic(1), 42, ts(1000)));
    }

    #[test]
    fn contains_returns_true_after_insert() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
        assert!(cache.contains(peer(1), topic(1), 42, ts(1000)));
    }

    #[test]
    fn contains_does_not_insert() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        // contains on unknown key returns false…
        assert!(!cache.contains(peer(1), topic(1), 42, ts(1000)));
        // …and must NOT have created an entry: a subsequent
        // check_and_insert sees a fresh nonce and accepts it.
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(1000)));
    }

    #[test]
    fn contains_finds_nonce_in_previous_bucket_after_rotation() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(2));
        // Insert at t=0
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // contains at t=3 should rotate (current→previous, fresh current)
        // and still find 42 in the previous bucket.
        assert!(cache.contains(peer(1), topic(1), 42, ts(3)));
    }

    #[test]
    fn contains_drops_nonce_after_two_windows() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(2));
        assert!(cache.check_and_insert(peer(1), topic(1), 42, ts(0)));
        // Force one rotation by doing an unrelated insert at t=3.
        assert!(cache.check_and_insert(peer(1), topic(1), 99, ts(3)));
        // contains at t=6: rotates again, previous (with 42) is discarded.
        assert!(!cache.contains(peer(1), topic(1), 42, ts(6)));
    }

    #[test]
    fn contains_does_not_pollute_cache_with_probe_traffic() {
        let mut cache = EphemeralNonceCache::new(Duration::from_secs(30));
        // Hostile pre-verify probe of many (sender, topic, nonce) triples.
        // None of these should leave state behind: the cache only mutates
        // on a successful check_and_insert (which is post-verify).
        for n in 0..1000_u64 {
            assert!(!cache.contains(peer((n % 250) as u8), topic((n % 50) as u8), n, ts(1000)));
        }
        // The legitimate write path now sees a clean slate for every key
        // the prober touched.
        for n in 0..1000_u64 {
            assert!(cache.check_and_insert(
                peer((n % 250) as u8),
                topic((n % 50) as u8),
                n,
                ts(1000)
            ));
        }
    }
}
