//! A storage wrapper that records metrics for all operations.
//!
//! This module is only available when the `metrics` feature is enabled.

use core::future::Future;
use std::time::Instant;

use alloc::vec::Vec;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::verified_meta::VerifiedMeta;

use crate::{metrics, storage::traits::Storage};

/// A storage wrapper that records metrics for all operations.
///
/// This wrapper delegates all storage operations to an inner storage implementation
/// while recording metrics for each operation.
#[derive(Debug, Clone)]
pub struct MetricsStorage<Store> {
    inner: Store,
}

/// Record a storage operation's duration, and an error counter if it failed.
#[inline]
fn observe<T, E>(operation: &'static str, start: Instant, result: &Result<T, E>) {
    metrics::storage_operation_duration(operation, start.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::storage_operation_error(operation);
    }
}

impl<Store> MetricsStorage<Store> {
    /// Create a new `MetricsStorage` wrapper around the given storage.
    #[must_use]
    pub const fn new(inner: Store) -> Self {
        Self { inner }
    }

    /// Get a reference to the inner storage.
    #[must_use]
    pub const fn inner(&self) -> &Store {
        &self.inner
    }

    /// Get a mutable reference to the inner storage.
    #[must_use]
    pub const fn inner_mut(&mut self) -> &mut Store {
        &mut self.inner
    }

    /// Unwrap this wrapper and return the inner storage.
    #[must_use]
    pub fn into_inner(self) -> Store {
        self.inner
    }
}

/// Trait for refreshing scan-free storage gauges.
pub trait RefreshMetrics {
    /// The error type for storage operations.
    type Error;

    /// Refresh the cheap storage gauges from current state.
    ///
    /// Only the sedimentree-count gauge is refreshed here, sourced from the
    /// backend's in-memory id cache (`load_all_sedimentree_ids`) — an O(1)
    /// clone, **not** a directory scan.
    ///
    /// Commit and fragment volume are tracked as cumulative write/delete
    /// counters maintained incrementally on each operation (see the `Storage`
    /// impl below), so this method never walks per-tree storage. The previous
    /// implementation scanned every tree on every tick, which was O(trees) of
    /// `read_dir` syscalls per refresh and the dominant cost at scale.
    fn refresh_metrics(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Store: Storage<Sendable> + Send + Sync> RefreshMetrics for MetricsStorage<Store> {
    type Error = Store::Error;

    async fn refresh_metrics(&self) -> Result<(), Self::Error> {
        // Cheap: the FS backend returns a clone of its in-memory id cache.
        let sedimentree_count = Storage::<Sendable>::load_all_sedimentree_ids(&self.inner)
            .await?
            .len();
        metrics::set_storage_sedimentrees(sedimentree_count);

        tracing::trace!(sedimentrees = sedimentree_count, "refreshed storage gauges");
        Ok(())
    }
}

#[future_form(Sendable where Store: Storage<Sendable> + Send + Sync, Local where Store: Storage<Local>)]
impl<Async: FutureForm, Store> Storage<Async> for MetricsStorage<Store> {
    type Error = Store::Error;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_sedimentree_id(sedimentree_id).await;
            observe("save_sedimentree_id", start, &result);
            result
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_sedimentree_id(sedimentree_id).await;
            observe("delete_sedimentree_id", start, &result);
            result
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_all_sedimentree_ids().await;
            observe("load_all_sedimentree_ids", start, &result);
            result
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.contains_sedimentree_id(sedimentree_id).await;
            observe("contains_sedimentree_id", start, &result);
            result
        })
    }

    // ==================== Loose Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_loose_commit(sedimentree_id, verified).await;
            observe("save_loose_commit", start, &result);
            if result.is_ok() {
                metrics::storage_commit_written();
            }
            result
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.list_commit_ids(sedimentree_id).await;
            observe("list_commit_ids", start, &result);
            result
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_loose_commits(sedimentree_id).await;
            observe("load_loose_commits", start, &result);
            result
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .load_loose_commit(sedimentree_id, commit_id)
                .await;
            observe("load_loose_commit", start, &result);
            result
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .delete_loose_commit(sedimentree_id, commit_id)
                .await;
            observe("delete_loose_commit", start, &result);
            if result.is_ok() {
                metrics::storage_commit_deleted();
            }
            result
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_loose_commits(sedimentree_id).await;
            observe("delete_loose_commits", start, &result);
            result
        })
    }

    // ==================== Fragments (compound with blob) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_fragment(sedimentree_id, verified).await;
            observe("save_fragment", start, &result);
            if result.is_ok() {
                metrics::storage_fragment_written();
            }
            result
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .load_fragment(sedimentree_id, fragment_head)
                .await;
            observe("load_fragment", start, &result);
            result
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.list_fragment_ids(sedimentree_id).await;
            observe("list_fragment_ids", start, &result);
            result
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_fragments(sedimentree_id).await;
            observe("load_fragments", start, &result);
            result
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .delete_fragment(sedimentree_id, fragment_head)
                .await;
            observe("delete_fragment", start, &result);
            if result.is_ok() {
                metrics::storage_fragment_deleted();
            }
            result
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_fragments(sedimentree_id).await;
            observe("delete_fragments", start, &result);
            result
        })
    }

    // ==================== Batch Operations ====================

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Async::Future<'_, Result<usize, Self::Error>> {
        Async::from_future(async move {
            let n_commits = commits.len() as u64;
            let n_fragments = fragments.len() as u64;
            let start = Instant::now();
            let result = self
                .inner
                .save_batch(sedimentree_id, commits, fragments)
                .await;
            observe("save_batch", start, &result);
            if result.is_ok() {
                metrics::storage_commits_written(n_commits);
                metrics::storage_fragments_written(n_fragments);
            }
            result
        })
    }
}
