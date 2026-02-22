//! In-memory storage backend.

use alloc::{sync::Arc, vec::Vec};
use core::convert::Infallible;

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    blob::Blob,
    collections::{Map, Set},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::{batch_result::BatchResult, traits::Storage};
use subduction_crypto::signed::Signed;

/// An in-memory storage backend.
///
/// Commits and fragments are stored in content-addressed maps keyed by digest.
#[derive(Debug, Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    commits: Arc<Mutex<Map<SedimentreeId, Map<Digest<LooseCommit>, Signed<LooseCommit>>>>>,
    fragments: Arc<Mutex<Map<SedimentreeId, Map<Digest<Fragment>, Signed<Fragment>>>>>,
    blobs: Arc<Mutex<Map<SedimentreeId, Map<Digest<Blob>, Blob>>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage backend.
    #[must_use]
    pub fn new() -> Self {
        tracing::debug!("creating new in-memory storage");
        Self {
            ids: Arc::new(Mutex::new(Set::new())),
            commits: Arc::new(Mutex::new(Map::new())),
            fragments: Arc::new(Mutex::new(Map::new())),
            blobs: Arc::new(Mutex::new(Map::new())),
        }
    }

    /// Compute digest from a signed commit by decoding the payload.
    fn commit_digest(signed: &Signed<LooseCommit>) -> Option<Digest<LooseCommit>> {
        signed.try_decode_payload().ok().map(|c| c.digest())
    }

    /// Compute digest from a signed fragment by decoding the payload.
    fn fragment_digest(signed: &Signed<Fragment>) -> Option<Digest<Fragment>> {
        signed.try_decode_payload().ok().map(|f| f.digest())
    }
}

#[future_form(Sendable, Local)]
impl<K: FutureForm> Storage<K> for MemoryStorage {
    type Error = Infallible;

    // ==================== Sedimentree IDs ====================

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

    // ==================== Loose Commits (CAS) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> K::Future<'_, Result<Digest<LooseCommit>, Self::Error>> {
        K::from_future(async move {
            #[allow(clippy::expect_used)]
            let digest = Self::commit_digest(&loose_commit)
                .expect("signed commit should decode for digest computation");
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_loose_commit");
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(digest, loose_commit);
            Ok(digest)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<Signed<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::load_loose_commit");
            let locked = self.commits.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .and_then(|map| map.get(&digest).cloned()))
        })
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::list_commit_digests");
            let locked = self.commits.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.keys().copied().collect())
                .unwrap_or_default())
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_loose_commits");
            let locked = self.commits.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.iter().map(|(d, s)| (*d, s.clone())).collect())
                .unwrap_or_default())
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "MemoryStorage::delete_loose_commit"
            );
            if let Some(map) = self.commits.lock().await.get_mut(&sedimentree_id) {
                map.remove(&digest);
            }
            Ok(())
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

    // ==================== Fragments (CAS) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> K::Future<'_, Result<Digest<Fragment>, Self::Error>> {
        K::from_future(async move {
            #[allow(clippy::expect_used)]
            let digest = Self::fragment_digest(&fragment)
                .expect("signed fragment should decode for digest computation");
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_fragment");
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(digest, fragment);
            Ok(digest)
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<Signed<Fragment>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::load_fragment");
            let locked = self.fragments.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .and_then(|map| map.get(&digest).cloned()))
        })
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::list_fragment_digests");
            let locked = self.fragments.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.keys().copied().collect())
                .unwrap_or_default())
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_fragments");
            let locked = self.fragments.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.iter().map(|(d, s)| (*d, s.clone())).collect())
                .unwrap_or_default())
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::delete_fragment");
            if let Some(map) = self.fragments.lock().await.get_mut(&sedimentree_id) {
                map.remove(&digest);
            }
            Ok(())
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

    // ==================== Blobs (per-sedimentree CAS) ====================

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            let digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
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

    fn load_blobs(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digests: &[Digest<Blob>],
    ) -> K::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, Self::Error>> {
        let blob_digests = blob_digests.to_vec();
        K::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                count = blob_digests.len(),
                "MemoryStorage::load_blobs"
            );
            let all_blobs = self.blobs.lock().await;
            let tree_blobs = all_blobs.get(&sedimentree_id);
            Ok(blob_digests
                .into_iter()
                .filter_map(|digest| {
                    tree_blobs
                        .and_then(|blobs| blobs.get(&digest).map(|blob| (digest, blob.clone())))
                })
                .collect())
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

    // ==================== Convenience Methods ====================

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_commit_with_blob");
            let blob_digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
            self.blobs
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(blob_digest)
                .or_insert(blob);

            #[allow(clippy::expect_used)]
            let commit_digest = Self::commit_digest(&commit)
                .expect("signed commit should decode for digest computation");
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(commit_digest, commit);
            Ok(blob_digest)
        })
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_fragment_with_blob");
            let blob_digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
            self.blobs
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(blob_digest)
                .or_insert(blob);

            #[allow(clippy::expect_used)]
            let fragment_digest = Self::fragment_digest(&fragment)
                .expect("signed fragment should decode for digest computation");
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment_digest, fragment);
            Ok(blob_digest)
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

            self.ids.lock().await.insert(sedimentree_id);

            let mut commit_digests = Vec::with_capacity(commits.len());
            let mut fragment_digests = Vec::with_capacity(fragments.len());

            for (commit, blob) in commits {
                let blob_digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
                self.blobs
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(blob_digest)
                    .or_insert(blob);

                #[allow(clippy::expect_used)]
                let commit_digest = Self::commit_digest(&commit)
                    .expect("signed commit should decode for digest computation");
                self.commits
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(commit_digest, commit);
                commit_digests.push(commit_digest);
            }

            for (fragment, blob) in fragments {
                let blob_digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
                self.blobs
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(blob_digest)
                    .or_insert(blob);

                #[allow(clippy::expect_used)]
                let fragment_digest = Self::fragment_digest(&fragment)
                    .expect("signed fragment should decode for digest computation");
                self.fragments
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(fragment_digest, fragment);
                fragment_digests.push(fragment_digest);
            }

            Ok(BatchResult {
                commit_digests,
                fragment_digests,
            })
        })
    }
}
