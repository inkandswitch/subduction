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
            tracing::trace!(?sedimentree_id, "MemoryStorage::save_sedimentree_id");
            self.ids.lock().await.insert(sedimentree_id);
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "MemoryStorage::delete_sedimentree_id");
            self.ids.lock().await.remove(&sedimentree_id);
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Async::from_future(async move {
            tracing::trace!("MemoryStorage::load_all_sedimentree_ids");
            Ok(self.ids.lock().await.iter().copied().collect())
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "MemoryStorage::contains_sedimentree_id");
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
            tracing::trace!(?sedimentree_id, ?digest, "MemoryStorage::save_loose_commit");

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
            tracing::trace!(?sedimentree_id, "MemoryStorage::list_commit_ids");
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
            tracing::trace!(?sedimentree_id, "MemoryStorage::load_loose_commits");
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
            tracing::trace!(
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
            tracing::trace!(
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
            tracing::trace!(?sedimentree_id, "MemoryStorage::delete_loose_commits");
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
            tracing::trace!(?sedimentree_id, ?digest, "MemoryStorage::save_fragment");

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
            tracing::trace!(
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
            tracing::trace!(?sedimentree_id, "MemoryStorage::list_fragment_ids");
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
            tracing::trace!(?sedimentree_id, "MemoryStorage::load_fragments");
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
            tracing::trace!(
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
            tracing::trace!(?sedimentree_id, "MemoryStorage::delete_fragments");
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
            tracing::trace!(
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

    /// Property tests for the `SedimentreeId`-sharded commit/fragment stores.
    ///
    /// These exercise shard selection (`Sharded::shard`), the
    /// decode-outside-lock load path, and the single-lock `save_batch`. The
    /// properties hold *regardless of how ids distribute across shards*, which
    /// is exactly the invariant a shard-keying or hashing bug would break.
    #[cfg(feature = "bolero")]
    mod proptests {
        use alloc::{
            collections::{BTreeMap, BTreeSet},
            vec,
        };

        use sedimentree_core::{blob::BlobMeta, collections::Set};
        use subduction_crypto::signer::memory::MemorySigner;

        use super::*;

        /// One commit to store, generated from arbitrary bytes. `id_seed`
        /// selects the document (and thus, via hashing, the shard); `data_seed`
        /// makes the commit content — and therefore its content-addressed digest
        /// and `CommitId` — distinct.
        #[derive(Debug, Clone, arbitrary::Arbitrary)]
        struct CommitSpec {
            id_seed: u16,
            data_seed: u16,
        }

        fn sed_id_from(seed: u16) -> SedimentreeId {
            let mut bytes = [0u8; 32];
            bytes[..2].copy_from_slice(&seed.to_le_bytes());
            SedimentreeId::new(bytes)
        }

        /// Build a verified commit whose identity is a function of `data_seed`
        /// alone, so two specs with the same `data_seed` produce byte-identical
        /// commits (exercising content-addressed deduplication).
        async fn make_commit(
            signer: &MemorySigner,
            sed_id: SedimentreeId,
            data_seed: u16,
        ) -> VerifiedMeta<LooseCommit> {
            let blob = Blob::new(data_seed.to_le_bytes().to_vec());
            let blob_meta = BlobMeta::new(&blob);
            let mut head_bytes = [0u8; 32];
            head_bytes[..2].copy_from_slice(&data_seed.to_le_bytes());
            let head = CommitId::new(head_bytes);
            let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
            let sealed = Signed::seal::<Sendable, _>(signer, commit).await;
            let verified = sealed
                .into_signed()
                .try_verify()
                .expect("freshly sealed commit verifies");
            VerifiedMeta::new(verified, blob).expect("blob matches commit meta")
        }

        /// The set of commit identities currently stored under `sed_id`.
        async fn loaded_ids(store: &MemoryStorage, sed_id: SedimentreeId) -> Set<CommitId> {
            Storage::<Sendable>::load_loose_commits(store, sed_id)
                .await
                .expect("load")
                .iter()
                .map(|v| v.payload().head())
                .collect()
        }

        fn runtime() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("current-thread runtime")
        }

        /// Roundtrip + grouping: after saving a batch of commits one-by-one,
        /// loading each document returns exactly the commits stored under it —
        /// no loss, no leakage between documents that hash to the same or
        /// different shards.
        #[test]
        fn prop_save_load_roundtrip_groups_by_document() {
            let rt = runtime();
            let signer = MemorySigner::from_bytes(&[7u8; 32]);

            bolero::check!()
                .with_arbitrary::<vec::Vec<CommitSpec>>()
                .for_each(|specs| {
                    rt.block_on(async {
                        let store = MemoryStorage::new();

                        // Expected: per-document set of commit identities. A
                        // document accumulates the distinct `data_seed`s saved
                        // under it (content-addressing dedups equal commits).
                        let mut expected: BTreeMap<SedimentreeId, Set<CommitId>> = BTreeMap::new();
                        for spec in specs {
                            let sed_id = sed_id_from(spec.id_seed);
                            let commit = make_commit(&signer, sed_id, spec.data_seed).await;
                            let head = commit.payload().head();
                            Storage::<Sendable>::save_loose_commit(&store, sed_id, commit)
                                .await
                                .expect("save");
                            expected.entry(sed_id).or_default().insert(head);
                        }

                        for (sed_id, want) in &expected {
                            assert_eq!(
                                &loaded_ids(&store, *sed_id).await,
                                want,
                                "loaded commits for {sed_id:?} must equal what was stored"
                            );
                        }
                    });
                });
        }

        /// Cross-document isolation: a commit stored under document A is never
        /// visible under a distinct document B, even when A and B land in the
        /// same shard.
        #[test]
        fn prop_documents_are_isolated() {
            let rt = runtime();
            let signer = MemorySigner::from_bytes(&[9u8; 32]);

            bolero::check!()
                .with_arbitrary::<(u16, u16, u16)>()
                .for_each(|(a_seed, b_seed, data_seed)| {
                    if a_seed == b_seed {
                        return;
                    }

                    rt.block_on(async {
                        let store = MemoryStorage::new();
                        let a = sed_id_from(*a_seed);
                        let b = sed_id_from(*b_seed);

                        let commit = make_commit(&signer, a, *data_seed).await;
                        Storage::<Sendable>::save_loose_commit(&store, a, commit)
                            .await
                            .expect("save");

                        assert!(
                            loaded_ids(&store, b).await.is_empty(),
                            "document {b:?} must not see commits saved under {a:?}"
                        );
                    });
                });
        }

        /// Oracle: `save_batch` and a sequence of `save_loose_commit` calls
        /// produce the same observable per-document load state. This pins the
        /// single-lock batch path to the per-item path it optimizes.
        #[test]
        fn prop_save_batch_matches_sequential_saves() {
            let rt = runtime();
            let signer = MemorySigner::from_bytes(&[11u8; 32]);

            bolero::check!()
                .with_arbitrary::<(u16, vec::Vec<u16>)>()
                .for_each(|(id_seed, data_seeds)| {
                    rt.block_on(async {
                        let sed_id = sed_id_from(*id_seed);

                        let sequential = MemoryStorage::new();
                        for &data_seed in data_seeds {
                            let commit = make_commit(&signer, sed_id, data_seed).await;
                            Storage::<Sendable>::save_loose_commit(&sequential, sed_id, commit)
                                .await
                                .expect("save");
                        }

                        let batched = MemoryStorage::new();
                        let mut commits = vec::Vec::new();
                        for &data_seed in data_seeds {
                            commits.push(make_commit(&signer, sed_id, data_seed).await);
                        }
                        Storage::<Sendable>::save_batch(&batched, sed_id, commits, vec::Vec::new())
                            .await
                            .expect("batch");

                        assert_eq!(
                            loaded_ids(&sequential, sed_id).await,
                            loaded_ids(&batched, sed_id).await,
                            "save_batch must agree with sequential saves for {sed_id:?}"
                        );
                    });
                });
        }

        /// CAS idempotence: saving the same content-addressed commit twice
        /// leaves exactly one entry.
        #[test]
        fn prop_duplicate_save_is_idempotent() {
            let rt = runtime();
            let signer = MemorySigner::from_bytes(&[13u8; 32]);

            bolero::check!()
                .with_arbitrary::<(u16, u16)>()
                .for_each(|(id_seed, data_seed)| {
                    rt.block_on(async {
                        let store = MemoryStorage::new();
                        let sed_id = sed_id_from(*id_seed);

                        for _ in 0..2 {
                            let commit = make_commit(&signer, sed_id, *data_seed).await;
                            Storage::<Sendable>::save_loose_commit(&store, sed_id, commit)
                                .await
                                .expect("save");
                        }

                        assert_eq!(
                            loaded_ids(&store, sed_id).await.len(),
                            1,
                            "saving identical commit twice must not duplicate"
                        );
                    });
                });
        }
    }
}
