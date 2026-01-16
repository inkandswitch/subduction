//! Storage abstraction for `Sedimentree` data.

use alloc::{
    string::String,
    sync::Arc,
    vec::Vec,
};

use crate::collections::{Map, Set};

use async_lock::Mutex;
use futures::{
    future::{BoxFuture, LocalBoxFuture},
    FutureExt,
};
use futures_kind::{FutureKind, Local, Sendable};

use crate::{blob::Blob, Digest, SedimentreeId};

use super::{Fragment, LooseCommit};

/// Abstraction over storage for `Sedimentree` data.
pub trait Storage<K: FutureKind + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Insert a sedimentree ID to know which sedimentrees have data stored.
    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Delete a sedimentree ID to know which sedimentrees have data stored.
    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Get all sedimentree IDs that have loose commits stored.
    fn load_all_sedimentree_ids(
        &self,
    ) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>>;

    /// Save a loose commit to storage.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all loose commits from storage.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>>;

    /// Delete all loose commits from storage.
    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a fragment to storage.
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all fragments from storage.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Fragment>, Self::Error>>;

    /// Delete all fragments from storage.
    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a blob to storage.
    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Load a blob from storage.
    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;

    /// Delete a blob from storage.
    fn delete_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<(), Self::Error>>;
}

/// Errors that can occur when loading tree data (commits or fragments)
#[derive(Debug, thiserror::Error)]
pub enum LoadTreeData {
    /// An error occurred in the storage subsystem itself.
    #[error("error from storage: {0}")]
    Storage(String),

    /// A blob is missing.
    #[error("missing blob: {0}")]
    MissingBlob(Digest),
}

/// An in-memory storage backend.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    fragments: Arc<Mutex<Map<SedimentreeId, Set<Fragment>>>>,
    commits: Arc<Mutex<Map<SedimentreeId, Set<LooseCommit>>>>,
    blobs: Arc<Mutex<Map<Digest, Blob>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage backend.
    #[must_use]
    pub fn new() -> Self {
        tracing::debug!("creating new in-memory storage");
        Self {
            ids: Arc::new(Mutex::new(Set::new())),
            fragments: Arc::new(Mutex::new(Map::new())),
            commits: Arc::new(Mutex::new(Map::new())),
            blobs: Arc::new(Mutex::new(Map::new())),
        }
    }
}

impl Storage<Local> for MemoryStorage {
    type Error = core::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: inserting sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        }
        .boxed_local()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: deleting sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed_local()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        tracing::debug!("[local] MemoryStorage: getting sedimentree_ids");
        async move {
            let ids = self.ids.lock().await.iter().copied().collect();
            Ok(ids)
        }
        .boxed_local()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: saving loose commit {:?} for sedimentree_id {:?}",
            loose_commit,
            sedimentree_id
        );
        async move {
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(loose_commit);
            Ok(())
        }
        .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: loading loose commits for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            let stored = {
                let locked = self.commits.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        }
        .boxed_local()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: deleting loose commits for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.commits.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed_local()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: saving fragment {:?} for sedimentree_id {:?}",
            fragment,
            sedimentree_id
        );
        async move {
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment);
            Ok(())
        }
        .boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: loading fragments for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            let stored = {
                let locked = self.fragments.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        }
        .boxed_local()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "[local] MemoryStorage: deleting fragments for sedimentree_id {:?}",
                sedimentree_id
            );
            self.fragments.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        tracing::debug!(
            "[local] MemoryStorage: saving blob with contents {:?}",
            blob.contents()
        );
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.entry(digest).or_insert(blob);
            Ok(digest)
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            tracing::debug!(
                "[local] MemoryStorage: loading blob with digest {:?}",
                blob_digest
            );
            Ok(self.blobs.lock().await.get(&blob_digest).cloned())
        }
        .boxed_local()
    }

    fn delete_blob(&self, blob_digest: Digest) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "[local] MemoryStorage: deleting blob with digest {:?}",
                blob_digest
            );
            self.blobs.lock().await.remove(&blob_digest);
            Ok(())
        }
        .boxed_local()
    }
}

impl Storage<Sendable> for MemoryStorage {
    type Error = core::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: inserting sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        }
        .boxed()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: deleting sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        tracing::debug!("[sendable] MemoryStorage: getting sedimentree_ids");
        async move { Ok(self.ids.lock().await.iter().copied().collect()) }.boxed()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: deleting loose commits for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            self.commits.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: saving loose commit {:?} for sedimentree_id {:?}",
            loose_commit,
            sedimentree_id
        );
        async move {
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(loose_commit);
            Ok(())
        }
        .boxed()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: loading loose commits for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            let stored = {
                let locked = self.commits.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "[sendable] MemoryStorage: deleting fragments for sedimentree_id {:?}",
                sedimentree_id
            );
            self.fragments.lock().await.remove(&sedimentree_id);
            Ok(())
        }
        .boxed()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: saving fragment {:?} for sedimentree_id {:?}",
            fragment,
            sedimentree_id
        );
        async move {
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment);
            Ok(())
        }
        .boxed()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: loading fragments for sedimentree_id {:?}",
            sedimentree_id
        );
        async move {
            let stored = {
                let locked = self.fragments.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn save_blob(&self, blob: Blob) -> BoxFuture<'_, Result<Digest, Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: saving blob with contents {:?}",
            blob.contents()
        );
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.entry(digest).or_insert(blob);
            Ok(digest)
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        tracing::debug!(
            "[sendable] MemoryStorage: loading blob with digest {:?}",
            blob_digest
        );
        async move { Ok(self.blobs.lock().await.get(&blob_digest).cloned()) }.boxed()
    }

    fn delete_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "[sendable] MemoryStorage: deleting blob with digest {:?}",
                blob_digest
            );
            self.blobs.lock().await.remove(&blob_digest);
            Ok(())
        }
        .boxed()
    }
}
