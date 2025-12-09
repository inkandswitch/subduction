//! Storage abstraction for `Sedimentree` data.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{
    future::{BoxFuture, LocalBoxFuture},
    lock::Mutex,
    FutureExt,
};

use crate::{
    blob::Blob,
    future::{FutureKind, Local, Sendable},
    Digest, SedimentreeId,
};

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

    /// Get all sedimentree IDs that have loose commits stored.
    fn load_all_sedimentree_ids(
        &self,
    ) -> K::Future<'_, Result<HashSet<SedimentreeId>, Self::Error>>;

    /// Load all loose commits from storage.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>>;

    /// Save a loose commit to storage.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
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

    /// Save a blob to storage.
    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Load a blob from storage.
    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;
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
    ids: Arc<Mutex<HashSet<SedimentreeId>>>,
    fragments: Arc<Mutex<HashMap<SedimentreeId, HashSet<Fragment>>>>,
    commits: Arc<Mutex<HashMap<SedimentreeId, HashSet<LooseCommit>>>>,
    blobs: Arc<Mutex<HashMap<Digest, Blob>>>,
}

impl Storage<Local> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "MemoryStorage: inserting sedimentree_id {:?}",
                sedimentree_id
            );
            {
                self.ids.lock().await.insert(sedimentree_id);
            }
            Ok(())
        }
        .boxed_local()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<HashSet<SedimentreeId>, Self::Error>> {
        async move {
            tracing::debug!("MemoryStorage: getting sedimentree_ids");
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
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            tracing::debug!(
                "MemoryStorage: saving loose commit {:?} for sedimentree_id {:?}",
                loose_commit,
                sedimentree_id
            );
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
        async move {
            tracing::debug!(
                "MemoryStorage: loading loose commits for sedimentree_id {:?}",
                sedimentree_id
            );
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

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            tracing::debug!(
                "MemoryStorage: saving fragment {:?} for sedimentree_id {:?}",
                fragment,
                sedimentree_id
            );
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
        async move {
            tracing::debug!(
                "MemoryStorage: loading fragments for sedimentree_id {:?}",
                sedimentree_id
            );
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

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            tracing::debug!(
                "MemoryStorage: saving blob with contents {:?}",
                blob.contents()
            );
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
            tracing::debug!("MemoryStorage: loading blob with digest {:?}", blob_digest);
            Ok(self.blobs.lock().await.get(&blob_digest).cloned())
        }
        .boxed_local()
    }
}

impl Storage<Sendable> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!(
                "MemoryStorage: inserting sedimentree_id {:?}",
                sedimentree_id
            );
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        }
        .boxed()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> BoxFuture<'_, Result<HashSet<SedimentreeId>, Self::Error>> {
        async move {
            tracing::debug!("MemoryStorage: getting sedimentree_ids");
            let ids = self.ids.lock().await.iter().copied().collect();
            Ok(ids)
        }
        .boxed()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            tracing::debug!(
                "MemoryStorage: saving loose commit {:?} for sedimentree_id {:?}",
                loose_commit,
                sedimentree_id
            );
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

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            tracing::debug!(
                "MemoryStorage: saving fragment {:?} for sedimentree_id {:?}",
                fragment,
                sedimentree_id
            );
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
            "MemoryStorage: loading fragments for sedimentree_id {:?}",
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
        async move {
            tracing::debug!(
                "MemoryStorage: saving blob with contents {:?}",
                blob.contents()
            );
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.entry(digest).or_insert(blob);
            Ok(digest)
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            tracing::debug!("MemoryStorage: loading blob with digest {:?}", blob_digest);
            Ok(self.blobs.lock().await.get(&blob_digest).cloned())
        }
        .boxed()
    }
}
