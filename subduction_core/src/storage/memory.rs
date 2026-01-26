//! In-memory storage backend.

use alloc::{sync::Arc, vec::Vec};

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    blob::{Blob, Digest},
    collections::{Map, Set},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::traits::{BatchResult, Storage};
use crate::crypto::signed::Signed;

/// An in-memory storage backend.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    fragments: Arc<Mutex<Map<SedimentreeId, Set<Signed<Fragment>>>>>,
    commits: Arc<Mutex<Map<SedimentreeId, Set<Signed<LooseCommit>>>>>,
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

#[future_form(Sendable, Local)]
impl<K: FutureForm> Storage<K> for MemoryStorage {
    type Error = core::convert::Infallible;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_sedimentree_id");
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_sedimentree_id");
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(&self) -> K::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!("MemoryStorage::load_all_sedimentree_ids");
            Ok(self.ids.lock().await.iter().copied().collect())
        })
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "MemoryStorage::save_loose_commit"
            );
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
    ) -> K::Future<'_, Result<Vec<Signed<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
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
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_loose_commits");
            self.commits.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_fragment");
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
    ) -> K::Future<'_, Result<Vec<Signed<Fragment>>, Self::Error>> {
        K::from_future(async move {
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
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_fragments");
            self.fragments.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>> {
        K::from_future(async move {
            let digest = Digest::hash(blob.contents());
            tracing::debug!(?digest, "MemoryStorage::save_blob");
            self.blobs.lock().await.entry(digest).or_insert(blob);
            Ok(digest)
        })
    }

    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?blob_digest, "MemoryStorage::load_blob");
            Ok(self.blobs.lock().await.get(&blob_digest).cloned())
        })
    }

    fn delete_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?blob_digest, "MemoryStorage::delete_blob");
            self.blobs.lock().await.remove(&blob_digest);
            Ok(())
        })
    }

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "MemoryStorage::save_commit_with_blob"
            );
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.entry(digest).or_insert(blob);
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(commit);
            Ok(digest)
        })
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "MemoryStorage::save_fragment_with_blob"
            );
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.entry(digest).or_insert(blob);
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment);
            Ok(digest)
        })
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> K::Future<'_, Result<BatchResult, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                num_commits = commits.len(),
                num_fragments = fragments.len(),
                "MemoryStorage::save_batch"
            );

            let mut blob_digests = Vec::with_capacity(commits.len() + fragments.len());

            self.ids.lock().await.insert(sedimentree_id);

            for (commit, blob) in commits {
                let digest = Digest::hash(blob.contents());
                self.blobs.lock().await.entry(digest).or_insert(blob);
                self.commits
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(commit);
                blob_digests.push(digest);
            }

            for (fragment, blob) in fragments {
                let digest = Digest::hash(blob.contents());
                self.blobs.lock().await.entry(digest).or_insert(blob);
                self.fragments
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(fragment);
                blob_digests.push(digest);
            }

            Ok(BatchResult { blob_digests })
        })
    }
}
