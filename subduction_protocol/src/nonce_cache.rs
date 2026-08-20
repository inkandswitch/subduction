//! Nonce tracking for handshake replay protection.
//!
//! Tracks `(PeerId, Nonce)` pairs from successful handshakes to prevent
//! replay attacks. Failed handshake attempts should *not* call
//! [`NonceCache::try_claim`] to avoid denial-of-service via cache filling.
//!
//! Copied from `legacy/subduction_core/src/nonce_cache.rs` with the
//! `async_lock::Mutex` removed: the cache is plain machine state behind
//! `&mut self` (the machine serializes access by construction).
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

use sedimentree_core::collections::Set;
use subduction_crypto::nonce::Nonce;

use crate::{peer_id::PeerId, wall_clock::TimestampSeconds};

/// Default bucket duration (3 minutes).
// `Duration::from_mins` is not yet const-stable (rust-lang/rust#140881), so
// stay on `from_secs` until the MSRV catches up. The `unknown_lints` allow
// keeps older toolchains (pre-1.95) quiet about the unrecognized lint name.
#[allow(unknown_lints, clippy::duration_suboptimal_units)]
const DEFAULT_BUCKET_DURATION: Duration = Duration::from_secs(3 * 60);

/// Number of buckets.
const BUCKET_COUNT: usize = 4;

/// Error returned when a nonce has already been used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("nonce has already been used")]
pub struct NonceReused;

/// Cache of recently-seen nonces for replay protection.
///
/// Uses 4 time-based buckets with configurable duration. Lookup is O(4)
/// across buckets. Old entries expire naturally as buckets rotate with time.
#[derive(Debug)]
pub struct NonceCache {
    buckets: [Set<(PeerId, Nonce)>; BUCKET_COUNT],

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
            buckets: core::array::from_fn(|_| Set::default()),
            head: 0,
            bucket_duration_secs: bucket_duration.as_secs(),
        }
    }

    /// Attempt to claim a nonce from a successful handshake.
    ///
    /// Returns `Ok(())` if the nonce is fresh and has been recorded.
    ///
    /// Only call this after signature verification succeeds — failed attempts
    /// must not fill the cache (denial-of-service vector).
    ///
    /// # Errors
    ///
    /// Returns [`NonceReused`] if this `(peer, nonce)` pair was already seen.
    pub fn try_claim(
        &mut self,
        peer: PeerId,
        nonce: Nonce,
        timestamp: TimestampSeconds,
    ) -> Result<(), NonceReused> {
        let key = (peer, nonce);
        let bucket_num = self.bucket_number(timestamp);

        // Advance head if needed (clears old buckets)
        self.advance_head(bucket_num);

        // Check all active buckets
        for bucket in &self.buckets {
            if bucket.contains(&key) {
                return Err(NonceReused);
            }
        }

        // Insert into appropriate bucket
        let idx = Self::bucket_index(bucket_num);
        #[allow(clippy::indexing_slicing)] // idx is always < BUCKET_COUNT (4)
        self.buckets[idx].insert(key);

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

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

    fn peer(id: u8) -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        PeerId::new(bytes)
    }

    #[test]
    fn replayed_nonce_rejected() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(peer(1), Nonce::from_u128(1), t)?;
        assert_eq!(
            cache.try_claim(peer(1), Nonce::from_u128(1), t),
            Err(NonceReused)
        );
        Ok(())
    }

    #[test]
    fn same_nonce_different_peer_allowed() -> TestResult {
        let mut cache = NonceCache::default();
        let t = TimestampSeconds::new(100);

        cache.try_claim(peer(1), Nonce::from_u128(1), t)?;
        cache.try_claim(peer(2), Nonce::from_u128(1), t)?;
        Ok(())
    }

    #[test]
    fn nonce_tracked_across_active_buckets() -> TestResult {
        let mut cache = NonceCache::default();

        cache.try_claim(peer(1), Nonce::from_u128(1), TimestampSeconds::new(0))?;

        // 6 minutes later: still within the 12-minute window.
        assert_eq!(
            cache.try_claim(peer(1), Nonce::from_u128(1), TimestampSeconds::new(360)),
            Err(NonceReused)
        );
        Ok(())
    }

    #[test]
    fn nonce_expires_after_window() -> TestResult {
        let mut cache = NonceCache::default();

        cache.try_claim(peer(1), Nonce::from_u128(1), TimestampSeconds::new(0))?;

        // 15 minutes later: bucket 0 has rotated out.
        cache.try_claim(peer(1), Nonce::from_u128(1), TimestampSeconds::new(900))?;
        Ok(())
    }
}
