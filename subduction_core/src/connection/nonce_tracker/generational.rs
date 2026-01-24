//! Generational bucket-based nonce tracker.
//!
//! Uses time-based buckets to efficiently prune old nonces. When a bucket
//! expires, the entire bucket is cleared in O(1) rather than scanning entries.
//!
//! # Design
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬──────────┐
//! │ Bucket 0 │ Bucket 1 │ Bucket 2 │ Bucket 3 │
//! │  0-3 min │  3-6 min │  6-9 min │ 9-12 min │
//! └──────────┴──────────┴──────────┴──────────┘
//!      ↓
//!   cleared on prune()
//! ```
//!
//! With 4 buckets × 3 minutes = 12 minute window, this covers the 10 minute
//! [`MAX_PLAUSIBLE_DRIFT`](crate::connection::handshake::MAX_PLAUSIBLE_DRIFT)
//! with a 2 minute buffer.

use alloc::boxed::Box;
use core::time::Duration;

use async_lock::Mutex;
use futures_kind::Sendable;
use sedimentree_core::collections::Set;

use super::{Nonce, NonceReused, NonceTracker};
use crate::{peer::id::PeerId, timestamp::TimestampSeconds};

/// Default bucket duration for the recommended configuration.
pub const DEFAULT_BUCKET_DURATION: Duration = Duration::from_secs(180); // 3 min

/// Default number of buckets for the recommended configuration.
pub const DEFAULT_BUCKET_COUNT: usize = 4;

/// Generational bucket-based nonce tracker.
///
/// Uses `N` buckets with configurable duration. Lookup is O(N) across buckets.
/// Pruning clears expired buckets in O(expired_buckets).
#[derive(Debug)]
pub struct GenerationalNonceTracker<const N: usize> {
    inner: Mutex<GenerationalInner<N>>,
    bucket_duration_secs: u64,
}

#[derive(Debug)]
struct GenerationalInner<const N: usize> {
    buckets: [Set<(PeerId, Nonce)>; N],
    /// Bucket number of the oldest valid bucket.
    horizon: u64,
}

impl<const N: usize> GenerationalNonceTracker<N> {
    /// Create a new tracker with the specified bucket duration.
    pub fn new(bucket_duration: Duration) -> Self {
        Self {
            inner: Mutex::new(GenerationalInner {
                buckets: core::array::from_fn(|_| Set::default()),
                horizon: 0,
            }),
            bucket_duration_secs: bucket_duration.as_secs(),
        }
    }

    fn bucket_number(&self, ts: TimestampSeconds) -> u64 {
        ts.as_secs() / self.bucket_duration_secs
    }

    fn bucket_index(&self, bucket_num: u64) -> usize {
        (bucket_num % N as u64) as usize
    }
}

impl GenerationalNonceTracker<DEFAULT_BUCKET_COUNT> {
    /// Create a tracker with the default configuration (4 buckets × 3 minutes).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_BUCKET_DURATION)
    }
}

impl<const N: usize> NonceTracker<Sendable> for GenerationalNonceTracker<N> {
    fn try_claim(
        &self,
        peer: PeerId,
        nonce: Nonce,
        timestamp: TimestampSeconds,
    ) -> <Sendable as futures_kind::FutureKind>::Future<'_, Result<(), NonceReused>> {
        Box::pin(async move {
            let key = (peer, nonce);
            let mut inner = self.inner.lock().await;

            // Check all active buckets
            for bucket in &inner.buckets {
                if bucket.contains(&key) {
                    return Err(NonceReused);
                }
            }

            // Insert into appropriate bucket
            let bucket_num = self.bucket_number(timestamp);
            let idx = self.bucket_index(bucket_num);
            inner.buckets[idx].insert(key);

            Ok(())
        })
    }

    fn prune(
        &self,
        now: TimestampSeconds,
        max_age: Duration,
    ) -> <Sendable as futures_kind::FutureKind>::Future<'_, ()> {
        Box::pin(async move {
            let cutoff_ts = now.saturating_sub(max_age);
            let cutoff_bucket = self.bucket_number(cutoff_ts);

            let mut inner = self.inner.lock().await;

            // Clear buckets that are now expired
            while inner.horizon < cutoff_bucket {
                let idx = self.bucket_index(inner.horizon);
                inner.buckets[idx].clear();
                inner.horizon += 1;
            }
        })
    }
}

/// Type alias for the recommended configuration (4 buckets × 3 minutes).
///
/// Uses `HashSet` with std, `BTreeSet` without.
pub type DefaultNonceTracker = GenerationalNonceTracker<DEFAULT_BUCKET_COUNT>;

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> DefaultNonceTracker {
        DefaultNonceTracker::with_defaults()
    }

    fn peer(id: u8) -> PeerId {
        let mut bytes = [0u8; 32];
        bytes[0] = id;
        PeerId::new(bytes)
    }

    #[tokio::test]
    async fn fresh_nonce_succeeds() {
        let tracker = tracker();
        let now = TimestampSeconds::new(1000);

        let result = tracker.try_claim(peer(1), Nonce::new(42), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn duplicate_nonce_fails() {
        let tracker = tracker();
        let now = TimestampSeconds::new(1000);

        tracker
            .try_claim(peer(1), Nonce::new(42), now)
            .await
            .expect("first claim should succeed");

        let result = tracker.try_claim(peer(1), Nonce::new(42), now).await;
        assert!(matches!(result, Err(NonceReused)));
    }

    #[tokio::test]
    async fn same_nonce_different_peer_succeeds() {
        let tracker = tracker();
        let now = TimestampSeconds::new(1000);

        tracker
            .try_claim(peer(1), Nonce::new(42), now)
            .await
            .expect("first claim should succeed");

        let result = tracker.try_claim(peer(2), Nonce::new(42), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn different_nonce_same_peer_succeeds() {
        let tracker = tracker();
        let now = TimestampSeconds::new(1000);

        tracker
            .try_claim(peer(1), Nonce::new(1), now)
            .await
            .expect("first claim should succeed");

        let result = tracker.try_claim(peer(1), Nonce::new(2), now).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn prune_clears_old_buckets() {
        let tracker = tracker();
        let t0 = TimestampSeconds::new(0);
        let t_later = TimestampSeconds::new(15 * 60); // 15 minutes later

        // Claim a nonce at t0
        tracker
            .try_claim(peer(1), Nonce::new(42), t0)
            .await
            .expect("first claim should succeed");

        // Same nonce still rejected before prune
        let result = tracker.try_claim(peer(1), Nonce::new(42), t0).await;
        assert!(matches!(result, Err(NonceReused)));

        // Prune with max_age of 10 minutes, current time 15 minutes later
        // This should clear all buckets from before 5 minutes ago
        tracker.prune(t_later, Duration::from_secs(10 * 60)).await;

        // Now the nonce should be claimable again (old entry was pruned)
        let result = tracker.try_claim(peer(1), Nonce::new(42), t_later).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn bucket_isolation() {
        let tracker = tracker();

        // Insert into different time buckets (3 minute buckets)
        let t0 = TimestampSeconds::new(0);
        let t1 = TimestampSeconds::new(180); // 3 minutes
        let t2 = TimestampSeconds::new(360); // 6 minutes

        tracker
            .try_claim(peer(1), Nonce::new(1), t0)
            .await
            .expect("bucket 0");
        tracker
            .try_claim(peer(1), Nonce::new(2), t1)
            .await
            .expect("bucket 1");
        tracker
            .try_claim(peer(1), Nonce::new(3), t2)
            .await
            .expect("bucket 2");

        // All should still be tracked (no pruning yet)
        assert!(matches!(
            tracker.try_claim(peer(1), Nonce::new(1), t0).await,
            Err(NonceReused)
        ));
        assert!(matches!(
            tracker.try_claim(peer(1), Nonce::new(2), t1).await,
            Err(NonceReused)
        ));
        assert!(matches!(
            tracker.try_claim(peer(1), Nonce::new(3), t2).await,
            Err(NonceReused)
        ));
    }
}
