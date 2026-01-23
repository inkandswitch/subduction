//! Storage abstraction for `Sedimentree` data.

use alloc::{string::String, sync::Arc, vec::Vec};

use crate::collections::{Map, Set};

use async_lock::Mutex;
use futures_kind::FutureKind;

use crate::{
    blob::{Blob, Digest},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

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
    fn load_all_sedimentree_ids(&self) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>>;

    /// Save a loose commit to storage.
    // FIXME also include the blob so this can be done transactionsally
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

#[futures_kind::kinds(Sendable, Local)]
impl<K: FutureKind> Storage<K> for MemoryStorage {
    type Error = core::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_sedimentree_id");
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_sedimentree_id");
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        K::into_kind(async move {
            tracing::debug!("MemoryStorage::load_all_sedimentree_ids");
            Ok(self.ids.lock().await.iter().copied().collect())
        })
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, ?loose_commit, "MemoryStorage::save_loose_commit");
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(loose_commit);
            Ok(())
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_loose_commits");
            let stored = {
                let locked = self.commits.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_loose_commits");
            self.commits.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, ?fragment, "MemoryStorage::save_fragment");
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment);
            Ok(())
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Fragment>, Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_fragments");
            let stored = {
                let locked = self.fragments.lock().await;
                locked.get(&sedimentree_id).cloned()
            };
            if let Some(set) = stored {
                Ok(set.into_iter().collect())
            } else {
                Ok(Vec::new())
            }
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_fragments");
            self.fragments.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>> {
        K::into_kind(async move {
            let digest = Digest::hash(blob.contents());
            tracing::debug!(?digest, "MemoryStorage::save_blob");
            self.blobs.lock().await.entry(digest).or_insert(blob);
            Ok(digest)
        })
    }

    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?blob_digest, "MemoryStorage::load_blob");
            Ok(self.blobs.lock().await.get(&blob_digest).cloned())
        })
    }

    fn delete_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<(), Self::Error>> {
        K::into_kind(async move {
            tracing::debug!(?blob_digest, "MemoryStorage::delete_blob");
            self.blobs.lock().await.remove(&blob_digest);
            Ok(())
        })
    }
}
