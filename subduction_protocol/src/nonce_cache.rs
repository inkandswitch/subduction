//! Nonce tracking for handshake replay protection.
//!
//! Tracks `(PeerId, Nonce)` pairs from successful handshakes to prevent
//! replay attacks. Failed handshake attempts should _not_ call
//! [`NonceCache::try_claim`] to avoid denial-of-service via cache filling.
//!
//! The cache is plain machine state behind `&mut self`: the machine
//! serializes access by construction, so no lock is needed.
//!
//! Each claim records its owning edge. Re-claims from the same connection
//! (same or newer [`Generation`](crate::id::Generation)) are idempotent,
//! so a supervisor-restarted connection machine can safely retry a claim
//! without being mistaken for a replay. Claims from a _different_
//! connection — the actual replay/MITM case — are denied.
//!
//! # Design
//!
//! Uses time-based buckets to efficiently expire old nonces. When time advances,
//! old buckets are naturally overwritten — no explicit pruning required.
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬──────────┐
//! │ Bucket 0 │ Bucket 1 │ Bucket 2 │ Bucket 3 │
//! │  0-3 min │  3-6 min │  6-9 min │ 9-12 min │
//! └──────────┴──────────┴──────────┴──────────┘
//!      ↑
//!   rotates as time advances
//! ```
//!
//! With 4 buckets × 3 minutes = 12 minute window, this covers the 10 minute
//! [`MAX_PLAUSIBLE_DRIFT`](crate::handshake::MAX_PLAUSIBLE_DRIFT) with a 2 minute buffer.

use core::time::Duration;

use sedimentree_core::collections::Map;
use subduction_crypto::nonce::Nonce;

use crate::{edge::EdgeId, peer_id::PeerId, wall_clock::TimestampSeconds};

/// Default bucket duration (3 minutes).
// `Duration::from_mins` is not yet const-stable (rust-lang/rust#140881), so
// stay on `from_secs` until the MSRV catches up. The `unknown_lints` allow
// keeps older toolchains (pre-1.95) quiet about the unrecognized lint name.
#[allow(unknown_lints, clippy::duration_suboptimal_units)]
const DEFAULT_BUCKET_DURATION: Duration = Duration::from_secs(3 * 60);

/// Number of buckets.
const BUCKET_COUNT: usize = 4;

/// Cache of recently-seen nonces for replay protection.
///
/// Uses 4 time-based buckets with configurable duration. Lookup is O(4)
/// across buckets. Old entries expire naturally as buckets rotate with time.
#[derive(Debug)]
pub struct NonceCache {
    buckets: [Map<(PeerId, Nonce), EdgeId>; BUCKET_COUNT],

    /// Bucket number of the oldest valid bucket (ring buffer head).
    head: u64,

    bucket_duration_secs: u64,
}

impl Default for NonceCache {
    fn default() -> Self {
        Self::new(DEFAULT_BUCKET_DURATION)
    }
}

impl NonceCache {
    /// Create a new cache with the specified bucket duration.
    #[must_use]
    pub fn new(bucket_duration: Duration) -> Self {
        Self {
            buckets: core::array::from_fn(|_| Map::default()),
            head: 0,
            bucket_duration_secs: bucket_duration.as_secs(),
        }
    }

    /// Attempt to claim a nonce from a successful handshake on behalf of
    /// `claimant`.
    ///
    /// Returns `Ok(())` if the nonce is fresh and has been recorded, or if
    /// it was already claimed by the same connection at the same or an
    /// older generation — the idempotent-retry path for supervisor
    /// restarts.
    ///
    /// Only call this after signature verification succeeds — failed attempts
    /// must not fill the cache (denial-of-service vector).
    ///
    /// # Errors
    ///
    /// Returns [`NonceReused`] if this `(peer, nonce)` pair was already
    /// claimed by a different connection (replay), or by a _newer_
    /// incarnation of the same connection (stale claimant).
    pub fn try_claim(
        &mut self,
        claimant: EdgeId,
        peer: PeerId,
        nonce: Nonce,
        timestamp: TimestampSeconds,
    ) -> Result<(), NonceReused> {
        let key = (peer, nonce);
        let bucket_num = self.bucket_number(timestamp);

        self.advance_head(bucket_num);

        for bucket in &mut self.buckets {
            let Some(owner) = bucket.get_mut(&key) else {
                continue;
            };

            if owner.conn == claimant.conn && owner.generation <= claimant.generation {
                // Same connection, same or restarted machine: idempotent
                // re-claim. Track the newest incarnation.
                *owner = claimant;
                return Ok(());
            }

            return Err(NonceReused);
        }

        let idx = Self::bucket_index(bucket_num);
        #[allow(clippy::indexing_slicing)] // idx is always < BUCKET_COUNT (4)
        let _previous = self.buckets[idx].insert(key, claimant);

        Ok(())
    }

    const fn bucket_number(&self, ts: TimestampSeconds) -> u64 {
        ts.as_secs() / self.bucket_duration_secs
    }

    #[allow(clippy::cast_possible_truncation)] // BUCKET_COUNT is 4, so this is always < usize::MAX
    const fn bucket_index(bucket_num: u64) -> usize {
        (bucket_num % BUCKET_COUNT as u64) as usize
    }

    fn advance_head(&mut self, current_bucket: u64) {
        // Clear buckets that would be reused (they're now expired)
        let oldest_valid = current_bucket.saturating_sub(BUCKET_COUNT as u64 - 1);
        while self.head < oldest_valid {
            let idx = Self::bucket_index(self.head);
            #[allow(clippy::indexing_slicing)] // idx is always < BUCKET_COUNT (4)
            self.buckets[idx].clear();
            self.head += 1;
        }
    }
}

/// Error returned when a nonce has already been used by another edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("nonce has already been used")]
pub struct NonceReused;


#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ConnId, Generation};
    use testresult::TestResult;

    fn peer(id: u8) -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        PeerId::new(bytes)
    }

    fn edge(conn: u64, generation: u64) -> EdgeId {
        let mut r#gen = Generation::FIRST;
        for _ in 0..generation {
            r#gen = r#gen.next();
        }
        EdgeId {
            conn: ConnId::new(conn),
            generation: r#gen,
        }
    }

    #[test]
    fn replayed_nonce_rejected() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(edge(1, 0), peer(1), Nonce::from_u128(1), t)?;
        assert_eq!(
            cache.try_claim(edge(2, 0), peer(1), Nonce::from_u128(1), t),
            Err(NonceReused)
        );
        Ok(())
    }

    #[test]
    fn same_nonce_different_peer_allowed() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(edge(1, 0), peer(1), Nonce::from_u128(1), t)?;
        cache.try_claim(edge(2, 0), peer(2), Nonce::from_u128(1), t)?;
        Ok(())
    }

    #[test]
    fn nonce_tracked_across_active_buckets() -> TestResult {
        let mut cache = NonceCache::default();

        cache.try_claim(
            edge(1, 0),
            peer(1),
            Nonce::from_u128(1),
            TimestampSeconds::new(0),
        )?;

        // 6 minutes later: still within the 12-minute window.
        assert_eq!(
            cache.try_claim(
                edge(2, 0),
                peer(1),
                Nonce::from_u128(1),
                TimestampSeconds::new(360)
            ),
            Err(NonceReused)
        );
        Ok(())
    }

    #[test]
    fn nonce_expires_after_window() -> TestResult {
        let mut cache = NonceCache::default();

        cache.try_claim(
            edge(1, 0),
            peer(1),
            Nonce::from_u128(1),
            TimestampSeconds::new(0),
        )?;

        // 15 minutes later: bucket 0 has rotated out.
        cache.try_claim(
            edge(2, 0),
            peer(1),
            Nonce::from_u128(1),
            TimestampSeconds::new(900),
        )?;
        Ok(())
    }

    #[test]
    fn same_edge_reclaim_is_idempotent() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(edge(1, 0), peer(1), Nonce::from_u128(1), t)?;
        cache.try_claim(edge(1, 0), peer(1), Nonce::from_u128(1), t)?;
        Ok(())
    }

    #[test]
    fn restarted_machine_newer_generation_may_reclaim() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(edge(1, 0), peer(1), Nonce::from_u128(1), t)?;
        // Supervisor restart: same conn, bumped generation.
        cache.try_claim(edge(1, 1), peer(1), Nonce::from_u128(1), t)?;
        Ok(())
    }

    #[test]
    fn stale_incarnation_cannot_reclaim() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(edge(1, 2), peer(1), Nonce::from_u128(1), t)?;
        // An older incarnation of the same conn is stale, not a retry.
        assert_eq!(
            cache.try_claim(edge(1, 1), peer(1), Nonce::from_u128(1), t),
            Err(NonceReused)
        );
        Ok(())
    }
}
