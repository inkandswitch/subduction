//! A storage wrapper that records metrics for all operations.
//!
//! This module is only available when the `metrics` feature is enabled.

use core::future::Future;
use std::{sync::Arc, time::Instant};

use alloc::vec::Vec;

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    blob::Blob, collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};

use crate::{
    metrics,
    storage::traits::{BatchResult, Storage},
};
use subduction_crypto::signed::Signed;

/// A storage wrapper that records metrics for all operations.
///
/// This wrapper delegates all storage operations to an inner storage implementation
/// while recording metrics for each operation.
#[derive(Debug, Clone)]
pub struct MetricsStorage<S> {
    inner: S,

    /// Track previously-seen sedimentree IDs to clean up stale gauges on refresh.
    previous_ids: Arc<Mutex<Set<SedimentreeId>>>,
}

impl<S> MetricsStorage<S> {
    /// Create a new `MetricsStorage` wrapper around the given storage.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            previous_ids: Arc::new(Mutex::new(Set::new())),
        }
    }

    /// Get a reference to the inner storage.
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner storage.
    #[must_use]
    pub const fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Unwrap this wrapper and return the inner storage.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }
}

/// Record metrics for a sedimentree's contents.
fn record_sedimentree_metrics(
    sedimentree_id: SedimentreeId,
    loose_commit_count: usize,
    fragment_count: usize,
) {
    let label = sedimentree_id.to_string();
    metrics::set_storage_loose_commits(label.clone(), loose_commit_count);
    metrics::set_storage_fragments(label, fragment_count);
}

/// Trait for refreshing metrics from storage state.
pub trait RefreshMetrics {
    /// The error type for storage operations.
    type Error;

    /// Refresh metrics gauges from current storage state.
    ///
    /// This queries storage to count existing sedimentrees, loose commits,
    /// and fragments, then sets the gauge values accordingly. Call this
    /// periodically to ensure metrics reflect the actual storage state.
    fn refresh_metrics(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<S> RefreshMetrics for MetricsStorage<S>
where
    S: Storage<Sendable> + Send + Sync,
{
    type Error = S::Error;

    async fn refresh_metrics(&self) -> Result<(), Self::Error> {
        let sedimentree_ids = Storage::<Sendable>::load_all_sedimentree_ids(&self.inner).await?;
        let sedimentree_count = sedimentree_ids.len();

        metrics::set_storage_sedimentrees(sedimentree_count);

        // Clean up gauges for deleted sedimentrees
        {
            let previous = self.previous_ids.lock().await;
            for old_id in previous.iter() {
                if !sedimentree_ids.contains(old_id) {
                    let label = old_id.to_string();
                    metrics::set_storage_loose_commits(label.clone(), 0);
                    metrics::set_storage_fragments(label, 0);
                }
            }
        }

        let mut total_loose_commits = 0;
        let mut total_fragments = 0;

        for sedimentree_id in &sedimentree_ids {
            // Use list_*_digests for efficient counting (no decoding needed)
            let commit_digests =
                Storage::<Sendable>::list_commit_digests(&self.inner, *sedimentree_id).await?;
            let fragment_digests =
                Storage::<Sendable>::list_fragment_digests(&self.inner, *sedimentree_id).await?;

            let commit_count = commit_digests.len();
            let fragment_count = fragment_digests.len();

            total_loose_commits += commit_count;
            total_fragments += fragment_count;

            record_sedimentree_metrics(*sedimentree_id, commit_count, fragment_count);
        }

        // Update previous IDs for next refresh
        *self.previous_ids.lock().await = sedimentree_ids;

        metrics::set_storage_loose_commits_total(total_loose_commits);
        metrics::set_storage_fragments_total(total_fragments);

        tracing::debug!(
            sedimentrees = sedimentree_count,
            loose_commits = total_loose_commits,
            fragments = total_fragments,
            "Refreshed storage metrics"
        );

        Ok(())
    }
}

#[future_form(Sendable where S: Storage<Sendable> + Send + Sync, Local where S: Storage<Local>)]
impl<K: FutureForm, S> Storage<K> for MetricsStorage<S> {
    type Error = S::Error;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "save_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn load_all_sedimentree_ids(&self) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_all_sedimentree_ids().await;
            metrics::storage_operation_duration(
                "load_all_sedimentree_ids",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    // ==================== Loose Commits (CAS) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> K::Future<'_, Result<Digest<LooseCommit>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .save_loose_commit(sedimentree_id, loose_commit)
                .await;
            metrics::storage_operation_duration("save_loose_commit", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<Signed<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_loose_commit(sedimentree_id, digest).await;
            metrics::storage_operation_duration("load_loose_commit", start.elapsed().as_secs_f64());
            result
        })
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.list_commit_digests(sedimentree_id).await;
            metrics::storage_operation_duration(
                "list_commit_digests",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "load_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_loose_commit(sedimentree_id, digest).await;
            metrics::storage_operation_duration(
                "delete_loose_commit",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    // ==================== Fragments (CAS) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> K::Future<'_, Result<Digest<Fragment>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_fragment(sedimentree_id, fragment).await;
            metrics::storage_operation_duration("save_fragment", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<Signed<Fragment>>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_fragment(sedimentree_id, digest).await;
            metrics::storage_operation_duration("load_fragment", start.elapsed().as_secs_f64());
            result
        })
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.list_fragment_digests(sedimentree_id).await;
            metrics::storage_operation_duration(
                "list_fragment_digests",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("load_fragments", start.elapsed().as_secs_f64());
            result
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_fragment(sedimentree_id, digest).await;
            metrics::storage_operation_duration("delete_fragment", start.elapsed().as_secs_f64());
            result
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("delete_fragments", start.elapsed().as_secs_f64());
            result
        })
    }

    // ==================== Blobs (per-sedimentree CAS) ====================

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.save_blob(sedimentree_id, blob).await;
            metrics::storage_operation_duration("save_blob", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<Option<Blob>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_blob(sedimentree_id, blob_digest).await;
            metrics::storage_operation_duration("load_blob", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_blobs(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digests: &[Digest<Blob>],
    ) -> K::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, Self::Error>> {
        let blob_digests = blob_digests.to_vec();
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.load_blobs(sedimentree_id, &blob_digests).await;
            metrics::storage_operation_duration("load_blobs", start.elapsed().as_secs_f64());
            result
        })
    }

    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self.inner.delete_blob(sedimentree_id, blob_digest).await;
            metrics::storage_operation_duration("delete_blob", start.elapsed().as_secs_f64());
            result
        })
    }

    // ==================== Convenience Methods ====================

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .save_commit_with_blob(sedimentree_id, commit, blob)
                .await;
            metrics::storage_operation_duration(
                "save_commit_with_blob",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .save_fragment_with_blob(sedimentree_id, fragment, blob)
                .await;
            metrics::storage_operation_duration(
                "save_fragment_with_blob",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> K::Future<'_, Result<BatchResult, Self::Error>> {
        K::from_future(async move {
            let start = Instant::now();
            let result = self
                .inner
                .save_batch(sedimentree_id, commits, fragments)
                .await;
            metrics::storage_operation_duration("save_batch", start.elapsed().as_secs_f64());
            result
        })
    }
}
