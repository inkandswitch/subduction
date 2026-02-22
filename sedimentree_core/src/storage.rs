//! Storage abstraction for `Sedimentree` data.

use alloc::{sync::Arc, vec::Vec};
use core::convert::Infallible;

use async_lock::Mutex;
use future_form::{future_form, FutureForm, Local, Sendable};
use subduction_crypto::verified_signature::VerifiedSignature;

use crate::{
    blob::Blob,
    collections::{Map, Set},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

/// Abstraction over storage for `Sedimentree` data.
pub trait Storage<K: FutureForm + ?Sized> {
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

    /// Save a blob under a sedimentree, returning its digest.
    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

    /// Load a blob by its digest within a sedimentree.
    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;

    /// Delete a blob by its digest within a sedimentree.
    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    // ─────────────────────────────────────────────────────────────────────────
    // Compound operations
    //
    // These methods combine multiple storage operations into logical units.
    // Backends with transaction support (SQLite, Postgres, etc.) can wrap
    // these in BEGIN/COMMIT for atomic execution.
    //
    // Simple backends should execute operations sequentially (blob first,
    // then metadata). Partial failures may leave storage in an incomplete
    // state that can be recovered via re-sync. See `MemoryStorage` for the
    // reference implementation pattern.
    // ─────────────────────────────────────────────────────────────────────────

    /// Save a commit with its blob.
    ///
    /// Saves the blob first, then the commit metadata. This ordering ensures
    /// that referenced data exists before the reference. For transactional
    /// backends, wrap in a transaction for atomicity.
    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

    /// Save a fragment with its blob.
    ///
    /// Saves the blob first, then the fragment metadata. This ordering ensures
    /// that referenced data exists before the reference. For transactional
    /// backends, wrap in a transaction for atomicity.
    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>>;

    /// Save a batch of commits and fragments.
    ///
    /// Saves the sedimentree ID first, then commits (blob + metadata each),
    /// then fragments (blob + metadata each). For transactional backends,
    /// wrap in a transaction for atomicity.
    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> K::Future<'_, Result<BatchResult, Self::Error>>;
}

/// Result of a batch save operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult {
    /// Digests of saved blobs (commits first, then fragments).
    pub blob_digests: Vec<Digest<Blob>>,
}

/// An in-memory storage backend.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    fragments: Arc<Mutex<Map<SedimentreeId, Set<Fragment>>>>,
    commits: Arc<Mutex<Map<SedimentreeId, Set<LooseCommit>>>>,
    #[allow(clippy::type_complexity)]
    blobs: Arc<Mutex<Map<SedimentreeId, Map<Digest<Blob>, Blob>>>>,
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
    type Error = Infallible;

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
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?loose_commit,
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
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
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
        fragment: Fragment,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
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

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            let digest = Digest::hash_bytes(blob.contents());
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_blob");
            self.blobs
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(digest)
                .or_insert(blob);
            Ok(digest)
        })
    }

    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<Option<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?blob_digest, "MemoryStorage::load_blob");
            Ok(self
                .blobs
                .lock()
                .await
                .get(&sedimentree_id)
                .and_then(|blobs| blobs.get(&blob_digest).cloned()))
        })
    }

    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?blob_digest, "MemoryStorage::delete_blob");
            if let Some(blobs) = self.blobs.lock().await.get_mut(&sedimentree_id) {
                blobs.remove(&blob_digest);
            }
            Ok(())
        })
    }

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?commit,
                "MemoryStorage::save_commit_with_blob"
            );
            let digest = Digest::hash_bytes(blob.contents());
            self.blobs
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(digest)
                .or_insert(blob);
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
        fragment: Fragment,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?fragment,
                "MemoryStorage::save_fragment_with_blob"
            );
            let digest = Digest::hash_bytes(blob.contents());
            self.blobs
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(digest)
                .or_insert(blob);
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
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
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
                let digest = Digest::hash_bytes(blob.contents());
                self.blobs
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(digest)
                    .or_insert(blob);
                self.commits
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(commit);
                blob_digests.push(digest);
            }

            for (fragment, blob) in fragments {
                let digest = Digest::hash_bytes(blob.contents());
                self.blobs
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(digest)
                    .or_insert(blob);
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
