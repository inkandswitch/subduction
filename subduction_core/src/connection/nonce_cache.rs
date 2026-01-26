//! Nonce tracking for handshake replay protection.
//!
//! Tracks `(PeerId, Nonce)` pairs from successful handshakes to prevent
//! replay attacks. Failed handshake attempts should *not* call
//! [`NonceCache::try_claim`] to avoid denial-of-service via cache filling.
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
//! [`MAX_PLAUSIBLE_DRIFT`](super::handshake::MAX_PLAUSIBLE_DRIFT) with a 2 minute buffer.

use core::time::Duration;

use async_lock::Mutex;
use sedimentree_core::collections::Set;

use crate::{peer::id::PeerId, timestamp::TimestampSeconds};

pub use crate::crypto::nonce::Nonce;

/// Default bucket duration (3 minutes).
const DEFAULT_BUCKET_DURATION: Duration = Duration::from_secs(180);

/// Number of buckets.
const BUCKET_COUNT: usize = 4;

/// Error returned when a nonce has already been used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("nonce has already been used")]
pub struct NonceReused;

/// Cache of recently-seen nonces for replay protection.
///
/// Uses 4 time-based buckets with configurable duration. Lookup is O(4) across buckets.
/// Old entries expire naturally as buckets rotate with time.
#[derive(Debug)]
pub struct NonceCache {
    inner: Mutex<NonceCacheInner>,
    bucket_duration_secs: u64,
}

#[derive(Debug)]
struct NonceCacheInner {
    buckets: [Set<(PeerId, Nonce)>; BUCKET_COUNT],

    /// Bucket number of the oldest valid bucket (ring buffer head).
    head: u64,
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
            inner: Mutex::new(NonceCacheInner {
                buckets: core::array::from_fn(|_| Set::default()),
                head: 0,
            }),
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
    pub async fn try_claim(
        &self,
        peer: PeerId,
        nonce: Nonce,
        timestamp: TimestampSeconds,
    ) -> Result<(), NonceReused> {
        let key = (peer, nonce);
        let bucket_num = self.bucket_number(timestamp);
        let mut inner = self.inner.lock().await;

        // Advance head if needed (clears old buckets)
        Self::advance_head(&mut inner, bucket_num);

        // Check all active buckets
        for bucket in &inner.buckets {
            if bucket.contains(&key) {
                return Err(NonceReused);
            }
        }

        // Insert into appropriate bucket
        let idx = Self::bucket_index(bucket_num);
        #[allow(clippy::indexing_slicing)] // idx is always < BUCKET_COUNT (4)
        inner.buckets[idx].insert(key);

        Ok(())
    }

    const fn bucket_number(&self, ts: TimestampSeconds) -> u64 {
        ts.as_secs() / self.bucket_duration_secs
    }

    #[allow(clippy::cast_possible_truncation)] // BUCKET_COUNT is 4, so this is always < usize::MAX
    const fn bucket_index(bucket_num: u64) -> usize {
        (bucket_num % BUCKET_COUNT as u64) as usize
    }

    fn advance_head(inner: &mut NonceCacheInner, current_bucket: u64) {
        // Clear buckets that would be reused (they're now expired)
        let oldest_valid = current_bucket.saturating_sub(BUCKET_COUNT as u64 - 1);
        while inner.head < oldest_valid {
            let idx = Self::bucket_index(inner.head);
            #[allow(clippy::indexing_slicing)] // idx is always < BUCKET_COUNT (4)
            inner.buckets[idx].clear();
            inner.head += 1;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn cache() -> NonceCache {
        NonceCache::default()
    }

    fn peer(id: u8) -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        PeerId::new(bytes)
    }

    #[tokio::test]
    async fn fresh_nonce_succeeds() {
        let cache = cache();
        let now = TimestampSeconds::new(1000);

        let result = cache.try_claim(peer(1), Nonce::new(42), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn duplicate_nonce_fails() {
        let cache = cache();
        let now = TimestampSeconds::new(1000);

        cache
            .try_claim(peer(1), Nonce::new(42), now)
            .await
            .expect("first claim should succeed");

        let result = cache.try_claim(peer(1), Nonce::new(42), now).await;
        assert!(matches!(result, Err(NonceReused)));
    }

    #[tokio::test]
    async fn same_nonce_different_peer_succeeds() {
        let cache = cache();
        let now = TimestampSeconds::new(1000);

        cache
            .try_claim(peer(1), Nonce::new(42), now)
            .await
            .expect("first claim should succeed");

        let result = cache.try_claim(peer(2), Nonce::new(42), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn different_nonce_same_peer_succeeds() {
        let cache = cache();
        let now = TimestampSeconds::new(1000);

        cache
            .try_claim(peer(1), Nonce::new(1), now)
            .await
            .expect("first claim should succeed");

        let result = cache.try_claim(peer(1), Nonce::new(2), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn old_nonces_expire_with_time() {
        let cache = cache();
        let t0 = TimestampSeconds::new(0);
        let t_later = TimestampSeconds::new(15 * 60); // 15 minutes later (past 12 min window)

        // Claim a nonce at t0
        cache
            .try_claim(peer(1), Nonce::new(42), t0)
            .await
            .expect("first claim should succeed");

        // Same nonce still rejected immediately
        let result = cache.try_claim(peer(1), Nonce::new(42), t0).await;
        assert!(matches!(result, Err(NonceReused)));

        // After 15 minutes, the bucket has rotated and the nonce is claimable again
        let result = cache.try_claim(peer(1), Nonce::new(42), t_later).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bucket_isolation() {
        let cache = cache();

        // Insert into different time buckets (3 minute buckets)
        let t0 = TimestampSeconds::new(0);
        let t1 = TimestampSeconds::new(180); // 3 minutes
        let t2 = TimestampSeconds::new(360); // 6 minutes

        cache
            .try_claim(peer(1), Nonce::new(1), t0)
            .await
            .expect("bucket 0");
        cache
            .try_claim(peer(1), Nonce::new(2), t1)
            .await
            .expect("bucket 1");
        cache
            .try_claim(peer(1), Nonce::new(3), t2)
            .await
            .expect("bucket 2");

        // All should still be tracked (within 12 min window)
        assert!(matches!(
            cache.try_claim(peer(1), Nonce::new(1), t2).await,
            Err(NonceReused)
        ));
        assert!(matches!(
            cache.try_claim(peer(1), Nonce::new(2), t2).await,
            Err(NonceReused)
        ));
        assert!(matches!(
            cache.try_claim(peer(1), Nonce::new(3), t2).await,
            Err(NonceReused)
        ));
    }
}
