//! In-memory storage backend.

use alloc::{sync::Arc, vec::Vec};

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    blob::Blob,
    collections::{Map, Set},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

use super::traits::Storage;

/// An in-memory storage backend.
///
/// Both commits and fragments are content-addressed internally,
/// keyed by [`Digest<LooseCommit>`] and [`Digest<Fragment>`] respectively.
/// Saving the same content twice is a no-op (CAS semantics).
///
/// Each entry caches the causal identity ([`CommitId`]) alongside the
/// signed payload and blob, so lookup/delete by identity is a simple
/// field comparison without re-decoding.
#[derive(Debug, Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,

    /// Commits: CAS key → (cached head [`CommitId`], signed payload, blob).
    commits: Arc<
        Mutex<Map<SedimentreeId, Map<Digest<LooseCommit>, (CommitId, Signed<LooseCommit>, Blob)>>>,
    >,

    /// Fragments: CAS key → (cached head [`CommitId`], signed payload, blob).
    fragments:
        Arc<Mutex<Map<SedimentreeId, Map<Digest<Fragment>, (CommitId, Signed<Fragment>, Blob)>>>>,
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

/// Failed to decode payload from stored data.
#[derive(Debug, Clone, Copy, Error)]
#[error(transparent)]
pub struct MemoryStorageError(#[from] sedimentree_core::codec::error::DecodeError);

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Storage<Async> for MemoryStorage {
    type Error = MemoryStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::save_sedimentree_id");
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::delete_sedimentree_id");
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!("MemoryStorage::load_all_sedimentree_ids");
            Ok(self.ids.lock().await.iter().copied().collect())
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::contains_sedimentree_id");
            Ok(self.ids.lock().await.contains(&sedimentree_id))
        })
    }

    // ==================== Loose Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let commit_id = verified.payload().head();
            let digest = Digest::hash(verified.payload());
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_loose_commit");

            let (signed, _payload, blob) = verified.into_full_parts();
            self.commits
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(digest)
                .or_insert((commit_id, signed, blob));
            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::list_commit_ids");
            let locked = self.commits.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.values().map(|(id, _, _)| *id).collect())
                .unwrap_or_default())
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_loose_commits");
            let locked = self.commits.lock().await;
            locked
                .get(&sedimentree_id)
                .map(|map| {
                    map.values()
                        .map(|(_, signed, blob)| {
                            VerifiedMeta::try_from_trusted(signed.clone(), blob.clone())
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()
                .map_err(MemoryStorageError::from)
                .map(Option::unwrap_or_default)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?commit_id,
                "MemoryStorage::load_loose_commit"
            );
            let locked = self.commits.lock().await;
            let Some(map) = locked.get(&sedimentree_id) else {
                return Ok(None);
            };
            for (id, signed, blob) in map.values() {
                if *id == commit_id {
                    return VerifiedMeta::try_from_trusted(signed.clone(), blob.clone())
                        .map(Some)
                        .map_err(MemoryStorageError::from);
                }
            }
            Ok(None)
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?commit_id,
                "MemoryStorage::delete_loose_commit"
            );
            if let Some(map) = self.commits.lock().await.get_mut(&sedimentree_id) {
                map.retain(|_, (id, _, _)| *id != commit_id);
            }
            Ok(())
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let fragment_head = verified.payload().head();
            let digest = Digest::hash(verified.payload());
            tracing::debug!(?sedimentree_id, ?digest, "MemoryStorage::save_fragment");

            let (signed, _payload, blob) = verified.into_full_parts();
            self.fragments
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .entry(digest)
                .or_insert((fragment_head, signed, blob));
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?fragment_head,
                "MemoryStorage::load_fragment"
            );
            let locked = self.fragments.lock().await;
            let Some(map) = locked.get(&sedimentree_id) else {
                return Ok(None);
            };
            for (id, signed, blob) in map.values() {
                if *id == fragment_head {
                    return VerifiedMeta::try_from_trusted(signed.clone(), blob.clone())
                        .map(Some)
                        .map_err(MemoryStorageError::from);
                }
            }
            Ok(None)
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::list_fragment_ids");
            let locked = self.fragments.lock().await;
            Ok(locked
                .get(&sedimentree_id)
                .map(|map| map.values().map(|(id, _, _)| *id).collect())
                .unwrap_or_default())
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::load_fragments");
            let locked = self.fragments.lock().await;
            locked
                .get(&sedimentree_id)
                .map(|map| {
                    map.values()
                        .map(|(_, signed, blob)| {
                            VerifiedMeta::try_from_trusted(signed.clone(), blob.clone())
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()
                .map_err(MemoryStorageError::from)
                .map(Option::unwrap_or_default)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?fragment_head,
                "MemoryStorage::delete_fragment"
            );
            if let Some(map) = self.fragments.lock().await.get_mut(&sedimentree_id) {
                map.retain(|_, (id, _, _)| *id != fragment_head);
            }
            Ok(())
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<usize, Self::Error>> {
        Async::from_future(async move {
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
                let commit_id = verified.payload().head();
                let digest = Digest::hash(verified.payload());
                let (signed, _payload, blob) = verified.into_full_parts();
                self.commits
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(digest)
                    .or_insert((commit_id, signed, blob));
            }

            for verified in fragments {
                let fragment_head = verified.payload().head();
                let digest = Digest::hash(verified.payload());
                let (signed, _payload, blob) = verified.into_full_parts();
                self.fragments
                    .lock()
                    .await
                    .entry(sedimentree_id)
                    .or_default()
                    .entry(digest)
                    .or_insert((fragment_head, signed, blob));
            }

            Ok(num_commits + num_fragments)
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn sid(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    /// `contains_sedimentree_id` reflects registration: false before, true
    /// after `save_sedimentree_id`, false again after `delete_sedimentree_id`,
    /// and always agrees with `load_all_sedimentree_ids` membership.
    #[tokio::test]
    async fn contains_tracks_registration_and_agrees_with_load_all() {
        let store = MemoryStorage::new();
        let id = sid(7);
        let other = sid(8);

        // Absent before registration.
        assert!(
            !Storage::<Sendable>::contains_sedimentree_id(&store, id)
                .await
                .expect("infallible"),
            "unregistered id must not be contained"
        );

        Storage::<Sendable>::save_sedimentree_id(&store, id)
            .await
            .expect("save");

        // Present after registration — even with no commits/fragments (the
        // registered-but-empty case the hydration path relies on).
        assert!(
            Storage::<Sendable>::contains_sedimentree_id(&store, id)
                .await
                .expect("infallible"),
            "registered id must be contained even when empty"
        );
        assert!(
            !Storage::<Sendable>::contains_sedimentree_id(&store, other)
                .await
                .expect("infallible"),
            "a different, unregistered id must not be contained"
        );

        // Agreement with the enumerating API.
        let all = Storage::<Sendable>::load_all_sedimentree_ids(&store)
            .await
            .expect("load all");
        assert!(all.contains(&id));
        assert!(!all.contains(&other));

        // Absent again after deletion.
        Storage::<Sendable>::delete_sedimentree_id(&store, id)
            .await
            .expect("delete");
        assert!(
            !Storage::<Sendable>::contains_sedimentree_id(&store, id)
                .await
                .expect("infallible"),
            "deleted id must no longer be contained"
        );
    }
}
