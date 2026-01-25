//! A wrapper for policies.
//!
//! This type is kept for API compatibility but now simply delegates to the inner policy.
//! Authorization is checked per-forward via `filter_authorized_fetch`, so caching
//! is not needed for correctness.
//!
//! # Example
//!
//! ```ignore
//! use subduction_core::policy::{CachedPolicy, OpenPolicy};
//!
//! let policy = CachedPolicy::new(OpenPolicy);
//! ```

use alloc::vec::Vec;

use futures_kind::{FutureKind, Local, Sendable};
use sedimentree_core::id::SedimentreeId;

use super::{ConnectionPolicy, StoragePolicy};
use crate::peer::id::PeerId;

/// A wrapper around a policy.
///
/// This type is kept for API compatibility but now simply delegates
/// all operations to the inner policy.
#[derive(Debug)]
pub struct CachedPolicy<P> {
    inner: P,
}

impl<P> CachedPolicy<P> {
    /// Create a new policy wrapping the given inner policy.
    #[must_use]
    pub const fn new(inner: P) -> Self {
        Self { inner }
    }

    /// Get a reference to the inner policy.
    #[must_use]
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    /// No-op for API compatibility.
    pub async fn clear_cache(&self) {
        // No caching, nothing to clear
    }

    /// No-op for API compatibility.
    pub async fn clear_cache_for(&self, _sedimentree_id: SedimentreeId) {
        // No caching, nothing to clear
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

// ConnectionPolicy for Sendable
impl<P: ConnectionPolicy<Sendable>> ConnectionPolicy<Sendable> for CachedPolicy<P> {
    type ConnectionDisallowed = P::ConnectionDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        self.inner.authorize_connect(peer)
    }
}

// ConnectionPolicy for Local
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

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::FetchDisallowed>> {
        self.inner.authorize_fetch(peer, sedimentree_id)
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::PutDisallowed>> {
        self.inner.authorize_put(requestor, author, sedimentree_id)
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

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::FetchDisallowed>> {
        self.inner.authorize_fetch(peer, sedimentree_id)
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::PutDisallowed>> {
        self.inner.authorize_put(requestor, author, sedimentree_id)
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
        // Just verify it compiles and inner() returns the wrapped policy
        let _ = policy.inner();
    }

    #[test]
    fn test_default() {
        let policy: CachedPolicy<OpenPolicy> = CachedPolicy::default();
        // Just verify it compiles and inner() returns the wrapped policy
        let _ = policy.inner();
    }

    #[cfg(feature = "std")]
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn test_fetch_delegates() {
            let counting = CountingPolicy::default();
            let policy: CachedPolicy<CountingPolicy> = CachedPolicy::new(counting);

            let peer = PeerId::new([1; 32]);
            let sed_id = SedimentreeId::new([2; 32]);

            // First call
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().fetch_count(), 1);

            // Second call - now delegates directly (no caching)
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_fetch(
                    &policy, peer, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().fetch_count(), 2);
        }

        #[tokio::test]
        async fn test_put_delegates() {
            let counting = CountingPolicy::default();
            let policy: CachedPolicy<CountingPolicy> = CachedPolicy::new(counting);

            let requestor = PeerId::new([1; 32]);
            let author = PeerId::new([2; 32]);
            let sed_id = SedimentreeId::new([3; 32]);

            // First call
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_put(
                    &policy, requestor, author, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().put_count(), 1);

            // Second call - now delegates directly (no caching)
            let result: Result<(), _> =
                <CachedPolicy<CountingPolicy> as StoragePolicy<Sendable>>::authorize_put(
                    &policy, requestor, author, sed_id,
                )
                .await;
            assert!(result.is_ok());
            assert_eq!(policy.inner().put_count(), 2);
        }
    }
}
