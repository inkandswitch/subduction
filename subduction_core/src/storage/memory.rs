//! In-memory storage backend.

use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::hash::{Hash, Hasher};

use async_lock::{Mutex, RwLock};
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    blob::Blob,
    codec::{decode::DecodeFields, encode::EncodeFields, schema::Schema},
    collections::{Map, Set},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use siphasher::sip::SipHasher24;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

use super::traits::Storage;

/// Number of lock shards for the commit and fragment stores.
///
/// Each [`SedimentreeId`] maps to one shard, so operations on distinct
/// documents (the common case — a node holds many sedimentrees) do not
/// contend. Per shard the lock is an [`RwLock`], so concurrent *reads* of the
/// same document (the sync hot path is read-dominated) proceed in parallel
/// while writes stay exclusive.
const STORAGE_SHARDS: usize = 256;

/// A `SedimentreeId`-sharded store: `STORAGE_SHARDS` independent
/// [`RwLock`]-guarded maps. Each inner map is keyed by `SedimentreeId` (a
/// single shard can hold several ids that hash to it) and then by the
/// content-addressed [`Digest`].
type ShardMap<D, T> = Map<SedimentreeId, Map<D, (CommitId, Signed<T>, Blob)>>;

#[derive(Debug)]
struct Sharded<D, T: Schema + EncodeFields + DecodeFields> {
    shards: Box<[RwLock<ShardMap<D, T>>]>,
}

impl<D: Ord + Hash, T: Schema + EncodeFields + DecodeFields> Sharded<D, T> {
    fn new() -> Self {
        Self {
            shards: (0..STORAGE_SHARDS)
                .map(|_| RwLock::new(Map::new()))
                .collect(),
        }
    }

    /// Select the shard for `id` via SipHash-2-4 + Lemire fast-range (matching
    /// [`ShardedMap`](crate::collections::sharded_map::ShardedMap)). A fixed
    /// zero key is fine: this store is in-memory and ephemeral, so no
    /// cross-process determinism is required.
    fn shard(&self, id: &SedimentreeId) -> &RwLock<ShardMap<D, T>> {
        let mut hasher = SipHasher24::new_with_keys(0, 0);
        id.hash(&mut hasher);
        let hash = hasher.finish();
        #[allow(clippy::cast_possible_truncation)]
        let idx = ((u128::from(hash) * (STORAGE_SHARDS as u128)) >> 64) as usize;
        #[allow(
            clippy::indexing_slicing,
            reason = "idx < STORAGE_SHARDS by construction"
        )]
        &self.shards[idx]
    }
}

/// An in-memory storage backend.
///
/// Both commits and fragments are content-addressed internally,
/// keyed by [`Digest<LooseCommit>`] and [`Digest<Fragment>`] respectively.
/// Saving the same content twice is a no-op (CAS semantics).
///
/// Each entry caches the causal identity ([`CommitId`]) alongside the
/// signed payload and blob, so lookup/delete by identity is a simple
/// field comparison without re-decoding.
///
/// The commit and fragment stores are [`SedimentreeId`]-sharded behind
/// per-shard [`RwLock`]s so concurrent syncs of different documents don't
/// contend and concurrent reads of the same document run in parallel. Loads
/// snapshot the raw stored entries under the lock and decode *after* releasing
/// it, keeping the critical section short.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    ids: Arc<Mutex<Set<SedimentreeId>>>,
    commits: Arc<Sharded<Digest<LooseCommit>, LooseCommit>>,
    fragments: Arc<Sharded<Digest<Fragment>, Fragment>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new in-memory storage backend.
    #[must_use]
    pub fn new() -> Self {
        tracing::debug!("creating new in-memory storage");
        Self {
            ids: Arc::new(Mutex::new(Set::new())),
            commits: Arc::new(Sharded::new()),
            fragments: Arc::new(Sharded::new()),
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
                .shard(&sedimentree_id)
                .write()
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
            let shard = self.commits.shard(&sedimentree_id).read().await;
            Ok(shard
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
            // Snapshot the raw stored entries under the read lock, then release
            // it before decoding — decode never serializes other readers.
            let raw: Vec<(Signed<LooseCommit>, Blob)> = {
                let shard = self.commits.shard(&sedimentree_id).read().await;
                shard.get(&sedimentree_id).map_or_else(Vec::new, |map| {
                    map.values()
                        .map(|(_, signed, blob)| (signed.clone(), blob.clone()))
                        .collect()
                })
            };
            raw.into_iter()
                .map(|(signed, blob)| VerifiedMeta::try_from_trusted(signed, blob))
                .collect::<Result<Vec<_>, _>>()
                .map_err(MemoryStorageError::from)
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
            // Snapshot the matching raw entry under the lock; decode after.
            let raw = {
                let shard = self.commits.shard(&sedimentree_id).read().await;
                shard.get(&sedimentree_id).and_then(|map| {
                    map.values().find_map(|(id, signed, blob)| {
                        (*id == commit_id).then(|| (signed.clone(), blob.clone()))
                    })
                })
            };
            raw.map(|(signed, blob)| VerifiedMeta::try_from_trusted(signed, blob))
                .transpose()
                .map_err(MemoryStorageError::from)
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
            if let Some(map) = self
                .commits
                .shard(&sedimentree_id)
                .write()
                .await
                .get_mut(&sedimentree_id)
            {
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
            self.commits
                .shard(&sedimentree_id)
                .write()
                .await
                .remove(&sedimentree_id);
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
                .shard(&sedimentree_id)
                .write()
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
            let raw = {
                let shard = self.fragments.shard(&sedimentree_id).read().await;
                shard.get(&sedimentree_id).and_then(|map| {
                    map.values().find_map(|(id, signed, blob)| {
                        (*id == fragment_head).then(|| (signed.clone(), blob.clone()))
                    })
                })
            };
            raw.map(|(signed, blob)| VerifiedMeta::try_from_trusted(signed, blob))
                .transpose()
                .map_err(MemoryStorageError::from)
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
            tracing::debug!(?sedimentree_id, "MemoryStorage::list_fragment_ids");
            let shard = self.fragments.shard(&sedimentree_id).read().await;
            Ok(shard
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
            // Snapshot under the read lock; decode after releasing it.
            let raw: Vec<(Signed<Fragment>, Blob)> = {
                let shard = self.fragments.shard(&sedimentree_id).read().await;
                shard.get(&sedimentree_id).map_or_else(Vec::new, |map| {
                    map.values()
                        .map(|(_, signed, blob)| (signed.clone(), blob.clone()))
                        .collect()
                })
            };
            raw.into_iter()
                .map(|(signed, blob)| VerifiedMeta::try_from_trusted(signed, blob))
                .collect::<Result<Vec<_>, _>>()
                .map_err(MemoryStorageError::from)
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
            if let Some(map) = self
                .fragments
                .shard(&sedimentree_id)
                .write()
                .await
                .get_mut(&sedimentree_id)
            {
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
            self.fragments
                .shard(&sedimentree_id)
                .write()
                .await
                .remove(&sedimentree_id);
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

            // Lock each shard once for the whole batch (not per item).
            {
                let mut shard = self.commits.shard(&sedimentree_id).write().await;
                let entry = shard.entry(sedimentree_id).or_default();
                for verified in commits {
                    let commit_id = verified.payload().head();
                    let digest = Digest::hash(verified.payload());
                    let (signed, _payload, blob) = verified.into_full_parts();
                    entry.entry(digest).or_insert((commit_id, signed, blob));
                }
            }

            {
                let mut shard = self.fragments.shard(&sedimentree_id).write().await;
                let entry = shard.entry(sedimentree_id).or_default();
                for verified in fragments {
                    let fragment_head = verified.payload().head();
                    let digest = Digest::hash(verified.payload());
                    let (signed, _payload, blob) = verified.into_full_parts();
                    entry.entry(digest).or_insert((fragment_head, signed, blob));
                }
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
