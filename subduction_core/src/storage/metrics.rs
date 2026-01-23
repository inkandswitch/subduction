//! A storage wrapper that records metrics for all operations.
//!
//! This module is only available when the `metrics` feature is enabled.

use core::future::Future;
use std::{sync::Arc, time::Instant};

use alloc::vec::Vec;

use async_lock::Mutex;
use futures_kind::{FutureKind, Local, Sendable};
use sedimentree_core::{
    blob::{Blob, Digest},
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    storage::Storage,
};

use crate::metrics;

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
    loose_commits: &[LooseCommit],
    fragments: &[Fragment],
) {
    let label = sedimentree_id.to_string();
    metrics::set_storage_loose_commits(label.clone(), loose_commits.len());
    metrics::set_storage_fragments(label, fragments.len());
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
    fn refresh_metrics(
        &self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
            let loose_commits =
                Storage::<Sendable>::load_loose_commits(&self.inner, *sedimentree_id).await?;
            let fragments =
                Storage::<Sendable>::load_fragments(&self.inner, *sedimentree_id).await?;

            total_loose_commits += loose_commits.len();
            total_fragments += fragments.len();

            record_sedimentree_metrics(*sedimentree_id, &loose_commits, &fragments);
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

#[futures_kind::kinds(Sendable where S: Storage<Sendable> + Send + Sync, Local where S: Storage<Local>)]
impl<K: FutureKind, S> Storage<K> for MetricsStorage<S> {
    type Error = S::Error;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
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
        K::into_kind(async move {
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
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.load_all_sedimentree_ids().await;
            metrics::storage_operation_duration(
                "load_all_sedimentree_ids",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self
                .inner
                .save_loose_commit(sedimentree_id, loose_commit)
                .await;
            metrics::storage_operation_duration("save_loose_commit", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.load_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "load_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.delete_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        })
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.save_fragment(sedimentree_id, fragment).await;
            metrics::storage_operation_duration("save_fragment", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Fragment>, Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.load_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("load_fragments", start.elapsed().as_secs_f64());
            result
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.delete_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("delete_fragments", start.elapsed().as_secs_f64());
            result
        })
    }

    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.save_blob(blob).await;
            metrics::storage_operation_duration("save_blob", start.elapsed().as_secs_f64());
            result
        })
    }

    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.load_blob(blob_digest).await;
            metrics::storage_operation_duration("load_blob", start.elapsed().as_secs_f64());
            result
        })
    }

    fn delete_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            let start = Instant::now();
            let result = self.inner.delete_blob(blob_digest).await;
            metrics::storage_operation_duration("delete_blob", start.elapsed().as_secs_f64());
            result
        })
    }
}
