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
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

use super::traits::Storage;

/// An in-memory storage backend.
///
/// Commits and fragments are stored in content-addressed maps keyed by digest.
/// Each commit/fragment is stored together with its blob as a `VerifiedMeta<T>`.
#[derive(Debug, Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    /// Commits stored as (Signed<LooseCommit>, Blob) pairs.
    commits: Arc<Mutex<Map<SedimentreeId, Map<Digest<LooseCommit>, (Signed<LooseCommit>, Blob)>>>>,
    /// Fragments stored as (Signed<Fragment>, Blob) pairs.
    fragments: Arc<Mutex<Map<SedimentreeId, Map<Digest<Fragment>, (Signed<Fragment>, Blob)>>>>,
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
        }
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

    // ==================== Loose Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let digest = verified.payload().digest();
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_loose_commit");

            let (signed, _payload, blob) = verified.into_full_parts();
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(digest, (signed, blob));
            Ok(())
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::load_loose_commit");
            let locked = self.commits.lock().await;
            Ok(locked.get(&sedimentree_id).and_then(|map| {
                map.get(&digest)
                    .map(|(signed, blob)| VerifiedMeta::from_trusted(signed.clone(), blob.clone()))
            }))
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
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_loose_commits");
            let locked = self.commits.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| {
                    map.values()
                        .map(|(signed, blob)| {
                            VerifiedMeta::from_trusted(signed.clone(), blob.clone())
                        })
                        .collect()
                })
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

    // ==================== Fragments (compound with blob) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            let digest = verified.payload().digest();
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_fragment");

            let (signed, _payload, blob) = verified.into_full_parts();
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(digest, (signed, blob));
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::load_fragment");
            let locked = self.fragments.lock().await;
            Ok(locked.get(&sedimentree_id).and_then(|map| {
                map.get(&digest)
                    .map(|(signed, blob)| VerifiedMeta::from_trusted(signed.clone(), blob.clone()))
            }))
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
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        K::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_fragments");
            let locked = self.fragments.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| {
                    map.values()
                        .map(|(signed, blob)| {
                            VerifiedMeta::from_trusted(signed.clone(), blob.clone())
                        })
                        .collect()
                })
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

    // ==================== Batch Operations ====================

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> K::Future<'_, Result<usize, Self::Error>> {
        K::from_future(async move {
            let num_commits = commits.len();
            let num_fragments = fragments.len();
            tracing::debug!(
                ?sedimentree_id,
                num_commits,
                num_fragments,
                "MemoryStorage::save_batch"
            );

            self.ids.lock().await.insert(sedimentree_id);

            for verified in commits {
                let digest = verified.payload().digest();
                let (signed, _payload, blob) = verified.into_full_parts();
                self.commits
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(digest, (signed, blob));
            }

            for verified in fragments {
                let digest = verified.payload().digest();
                let (signed, _payload, blob) = verified.into_full_parts();
                self.fragments
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .insert(digest, (signed, blob));
            }

            Ok(num_commits + num_fragments)
        })
    }
}
