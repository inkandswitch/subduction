//! A storage wrapper that records metrics for all operations.
//!
//! This module is only available when the `metrics` feature is enabled.

use alloc::vec::Vec;

use futures::{
    future::{BoxFuture, LocalBoxFuture},
    FutureExt,
};
use futures_kind::{Local, Sendable};
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
}

impl<S> MetricsStorage<S> {
    /// Create a new `MetricsStorage` wrapper around the given storage.
    #[must_use]
    pub const fn new(inner: S) -> Self {
        Self { inner }
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

impl<S> Storage<Local> for MetricsStorage<S>
where
    S: Storage<Local>,
{
    type Error = S::Error;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "save_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            if result.is_ok() {
                metrics::storage_sedimentree_added();
            }
            result
        }
        .boxed_local()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            if result.is_ok() {
                metrics::storage_sedimentree_removed();
            }
            result
        }
        .boxed_local()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_all_sedimentree_ids().await;
            metrics::storage_operation_duration(
                "load_all_sedimentree_ids",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed_local()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let label = sedimentree_id.to_string();
        async move {
            let start = std::time::Instant::now();
            let result = self
                .inner
                .save_loose_commit(sedimentree_id, loose_commit)
                .await;
            metrics::storage_operation_duration("save_loose_commit", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_loose_commit_saved(&label);
            }
            result
        }
        .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "load_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed_local()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed_local()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let label = sedimentree_id.to_string();
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_fragment(sedimentree_id, fragment).await;
            metrics::storage_operation_duration("save_fragment", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_fragment_saved(&label);
            }
            result
        }
        .boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("load_fragments", start.elapsed().as_secs_f64());
            result
        }
        .boxed_local()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("delete_fragments", start.elapsed().as_secs_f64());
            result
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_blob(blob).await;
            metrics::storage_operation_duration("save_blob", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_blob_added();
            }
            result
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_blob(blob_digest).await;
            metrics::storage_operation_duration("load_blob", start.elapsed().as_secs_f64());
            result
        }
        .boxed_local()
    }

    fn delete_blob(&self, blob_digest: Digest) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_blob(blob_digest).await;
            metrics::storage_operation_duration("delete_blob", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_blob_removed();
            }
            result
        }
        .boxed_local()
    }
}

impl<S> Storage<Sendable> for MetricsStorage<S>
where
    S: Storage<Sendable> + Send + Sync,
{
    type Error = S::Error;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "save_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            if result.is_ok() {
                metrics::storage_sedimentree_added();
            }
            result
        }
        .boxed()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_sedimentree_id(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_sedimentree_id",
                start.elapsed().as_secs_f64(),
            );
            if result.is_ok() {
                metrics::storage_sedimentree_removed();
            }
            result
        }
        .boxed()
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_all_sedimentree_ids().await;
            metrics::storage_operation_duration(
                "load_all_sedimentree_ids",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let label = sedimentree_id.to_string();
        async move {
            let start = std::time::Instant::now();
            let result = self
                .inner
                .save_loose_commit(sedimentree_id, loose_commit)
                .await;
            metrics::storage_operation_duration("save_loose_commit", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_loose_commit_saved(&label);
            }
            result
        }
        .boxed()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "load_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_loose_commits(sedimentree_id).await;
            metrics::storage_operation_duration(
                "delete_loose_commits",
                start.elapsed().as_secs_f64(),
            );
            result
        }
        .boxed()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        let label = sedimentree_id.to_string();
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_fragment(sedimentree_id, fragment).await;
            metrics::storage_operation_duration("save_fragment", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_fragment_saved(&label);
            }
            result
        }
        .boxed()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("load_fragments", start.elapsed().as_secs_f64());
            result
        }
        .boxed()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_fragments(sedimentree_id).await;
            metrics::storage_operation_duration("delete_fragments", start.elapsed().as_secs_f64());
            result
        }
        .boxed()
    }

    fn save_blob(&self, blob: Blob) -> BoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.save_blob(blob).await;
            metrics::storage_operation_duration("save_blob", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_blob_added();
            }
            result
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.load_blob(blob_digest).await;
            metrics::storage_operation_duration("load_blob", start.elapsed().as_secs_f64());
            result
        }
        .boxed()
    }

    fn delete_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let start = std::time::Instant::now();
            let result = self.inner.delete_blob(blob_digest).await;
            metrics::storage_operation_duration("delete_blob", start.elapsed().as_secs_f64());
            if result.is_ok() {
                metrics::storage_blob_removed();
            }
            result
        }
        .boxed()
    }
}
