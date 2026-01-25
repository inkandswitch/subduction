//! A caching wrapper for policies that caches successful authorization results.
//!
//! The cache is keyed by `(peer_id, sedimentree_id, generation)`, so entries
//! automatically become stale when the generation changes (e.g., membership revocation).
//!
//! # Example
//!
//! ```ignore
//! use subduction_core::policy::{CachedPolicy, OpenPolicy};
//!
//! let policy = CachedPolicy::new(OpenPolicy);
//! ```
//!
//! # Caching Strategy
//!
//! - **Connection auth**: Not cached (no generation for invalidation)
//! - **Fetch auth**: Cached on success, keyed by `(peer, sedimentree_id, generation)`
//! - **Put auth**: Cached on success, keyed by `(requestor, author, sedimentree_id, generation)`
//!
//! Only successful authorizations are cached. Failures are not cached since
//! the error types may not be cloneable, and re-checking on failure is safer.

use alloc::{collections::BTreeSet, vec::Vec};

use async_lock::Mutex;
use futures_kind::{FutureKind, Local, Sendable};
use sedimentree_core::id::SedimentreeId;

use super::{ConnectionPolicy, Generation, StoragePolicy};
use crate::peer::id::PeerId;

/// A caching wrapper around a policy.
///
/// Caches successful authorization results to avoid repeated lookups.
/// Cache entries are keyed by generation, so they automatically become
/// stale when permissions change.
#[derive(Debug)]
pub struct CachedPolicy<P> {
    inner: P,
    fetch_cache: Mutex<BTreeSet<(PeerId, SedimentreeId, Generation)>>,
    put_cache: Mutex<BTreeSet<(PeerId, PeerId, SedimentreeId, Generation)>>,
}

impl<P> CachedPolicy<P> {
    /// Create a new caching policy wrapping the given inner policy.
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            fetch_cache: Mutex::new(BTreeSet::new()),
            put_cache: Mutex::new(BTreeSet::new()),
        }
    }

    /// Get a reference to the inner policy.
    #[must_use]
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    /// Clear all cached authorizations.
    pub async fn clear_cache(&self) {
        self.fetch_cache.lock().await.clear();
        self.put_cache.lock().await.clear();
    }

    /// Clear cached authorizations for a specific sedimentree.
    pub async fn clear_cache_for(&self, sedimentree_id: SedimentreeId) {
        self.fetch_cache
            .lock()
            .await
            .retain(|(_, id, _)| *id != sedimentree_id);
        self.put_cache
            .lock()
            .await
            .retain(|(_, _, id, _)| *id != sedimentree_id);
    }
}

impl<P: Default> Default for CachedPolicy<P> {
    fn default() -> Self {
        Self::new(P::default())
    }
}

impl<P: Clone> Clone for CachedPolicy<P> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone())
    }
}

// Connection auth is not cached (no generation for invalidation)
// Implement for Sendable
impl<P: ConnectionPolicy<Sendable>> ConnectionPolicy<Sendable> for CachedPolicy<P> {
    type ConnectionDisallowed = P::ConnectionDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        self.inner.authorize_connect(peer)
    }
}

// Implement for Local
impl<P: ConnectionPolicy<Local>> ConnectionPolicy<Local> for CachedPolicy<P> {
    type ConnectionDisallowed = P::ConnectionDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        self.inner.authorize_connect(peer)
    }
}

// StoragePolicy for Sendable
impl<P: StoragePolicy<Sendable> + Sync> StoragePolicy<Sendable> for CachedPolicy<P>
where
    P::FetchDisallowed: Send,
    P::PutDisallowed: Send,
{
    type FetchDisallowed = P::FetchDisallowed;
    type PutDisallowed = P::PutDisallowed;

    fn generation(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureKind>::Future<'_, Generation> {
        self.inner.generation(sedimentree_id)
    }

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::FetchDisallowed>> {
        Sendable::into_kind(async move {
            let generation = self.inner.generation(sedimentree_id).await;
            let cache_key = (peer, sedimentree_id, generation);

            if self.fetch_cache.lock().await.contains(&cache_key) {
                return Ok(());
            }

            let result = self.inner.authorize_fetch(peer, sedimentree_id).await;

            if result.is_ok() {
                self.fetch_cache.lock().await.insert(cache_key);
            }

            result
        })
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::PutDisallowed>> {
        Sendable::into_kind(async move {
            let generation = self.inner.generation(sedimentree_id).await;
            let cache_key = (requestor, author, sedimentree_id, generation);

            if self.put_cache.lock().await.contains(&cache_key) {
                return Ok(());
            }

            let result = self
                .inner
                .authorize_put(requestor, author, sedimentree_id)
                .await;

            if result.is_ok() {
                self.put_cache.lock().await.insert(cache_key);
            }

            result
        })
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Sendable as FutureKind>::Future<'_, Vec<SedimentreeId>> {
        self.inner.filter_authorized_fetch(peer, ids)
    }
}

// StoragePolicy for Local
impl<P: StoragePolicy<Local>> StoragePolicy<Local> for CachedPolicy<P> {
    type FetchDisallowed = P::FetchDisallowed;
    type PutDisallowed = P::PutDisallowed;

    fn generation(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureKind>::Future<'_, Generation> {
        self.inner.generation(sedimentree_id)
    }

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::FetchDisallowed>> {
        Local::into_kind(async move {
            let generation = self.inner.generation(sedimentree_id).await;
            let cache_key = (peer, sedimentree_id, generation);

            if self.fetch_cache.lock().await.contains(&cache_key) {
                return Ok(());
            }

            let result = self.inner.authorize_fetch(peer, sedimentree_id).await;

            if result.is_ok() {
                self.fetch_cache.lock().await.insert(cache_key);
            }

            result
        })
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::PutDisallowed>> {
        Local::into_kind(async move {
            let generation = self.inner.generation(sedimentree_id).await;
            let cache_key = (requestor, author, sedimentree_id, generation);

            if self.put_cache.lock().await.contains(&cache_key) {
                return Ok(());
            }

            let result = self
                .inner
                .authorize_put(requestor, author, sedimentree_id)
                .await;

            if result.is_ok() {
                self.put_cache.lock().await.insert(cache_key);
            }

            result
        })
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> <Local as FutureKind>::Future<'_, Vec<SedimentreeId>> {
        self.inner.filter_authorized_fetch(peer, ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::OpenPolicy;
    use core::sync::atomic::{AtomicUsize, Ordering};

    /// A policy that counts how many times each method is called.
    #[derive(Debug, Default)]
    struct CountingPolicy {
        fetch_count: AtomicUsize,
        put_count: AtomicUsize,
    }

    impl CountingPolicy {
        fn fetch_count(&self) -> usize {
            self.fetch_count.load(Ordering::SeqCst)
        }

        fn put_count(&self) -> usize {
            self.put_count.load(Ordering::SeqCst)
        }
    }

    #[futures_kind::kinds(Sendable, Local)]
    impl<K: FutureKind> ConnectionPolicy<K> for CountingPolicy {
        type ConnectionDisallowed = core::convert::Infallible;

        fn authorize_connect(
            &self,
            _peer: PeerId,
        ) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>> {
            K::into_kind(async { Ok(()) })
        }
    }

    #[futures_kind::kinds(Sendable, Local)]
    impl<K: FutureKind> StoragePolicy<K> for CountingPolicy {
        type FetchDisallowed = core::convert::Infallible;
        type PutDisallowed = core::convert::Infallible;

        fn generation(&self, _sedimentree_id: SedimentreeId) -> K::Future<'_, Generation> {
            K::into_kind(async { Generation::default() })
        }

        fn authorize_fetch(
            &self,
            _peer: PeerId,
            _sedimentree_id: SedimentreeId,
        ) -> K::Future<'_, Result<(), Self::FetchDisallowed>> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            K::into_kind(async { Ok(()) })
        }

        fn authorize_put(
            &self,
            _requestor: PeerId,
            _author: PeerId,
            _sedimentree_id: SedimentreeId,
        ) -> K::Future<'_, Result<(), Self::PutDisallowed>> {
            self.put_count.fetch_add(1, Ordering::SeqCst);
            K::into_kind(async { Ok(()) })
        }

        fn filter_authorized_fetch(
            &self,
            _peer: PeerId,
            ids: Vec<SedimentreeId>,
        ) -> K::Future<'_, Vec<SedimentreeId>> {
            // CountingPolicy allows everything
            K::into_kind(async { ids })
        }
    }

    #[test]
    fn test_wraps_open_policy() {
        let policy: CachedPolicy<OpenPolicy> = CachedPolicy::new(OpenPolicy);
        assert!(core::mem::size_of_val(&policy) > 0);
    }

    #[test]
    fn test_default() {
        let policy: CachedPolicy<OpenPolicy> = CachedPolicy::default();
        assert!(core::mem::size_of_val(&policy) > 0);
    }

    #[cfg(feature = "std")]
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn test_fetch_caching() {
            let counting = CountingPolicy::default();
            let policy: CachedPolicy<CountingPolicy> = CachedPolicy::new(counting);

            let peer = PeerId::new([1; 32]);
            let sed_id = SedimentreeId::new([2; 32]);

            // First call - cache miss
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().fetch_count(), 1);

            // Second call - cache hit
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().fetch_count(), 1); // Still 1, cached

            // Different peer - cache miss
            let other_peer = PeerId::new([3; 32]);
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, other_peer, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().fetch_count(), 2);
        }

        #[tokio::test]
        async fn test_put_caching() {
            let counting = CountingPolicy::default();
            let policy: CachedPolicy<CountingPolicy> = CachedPolicy::new(counting);

            let requestor = PeerId::new([1; 32]);
            let author = PeerId::new([2; 32]);
            let sed_id = SedimentreeId::new([3; 32]);

            // First call - cache miss
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_put(
                    &policy, requestor, author, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().put_count(), 1);

            // Second call - cache hit
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_put(
                    &policy, requestor, author, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().put_count(), 1); // Still 1, cached
        }

        #[tokio::test]
        async fn test_clear_cache() {
            let counting = CountingPolicy::default();
            let policy: CachedPolicy<CountingPolicy> = CachedPolicy::new(counting);

            let peer = PeerId::new([1; 32]);
            let sed_id = SedimentreeId::new([2; 32]);

            // Populate cache
            let _: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert_eq!(policy.inner().fetch_count(), 1);

            // Clear cache
            policy.clear_cache().await;

            // Should miss now
            let _: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert_eq!(policy.inner().fetch_count(), 2);
        }
    }
}
