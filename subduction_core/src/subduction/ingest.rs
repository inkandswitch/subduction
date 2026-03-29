//! Shared data-ingestion helpers used by both [`Subduction`] and [`SyncHandler`].
//!
//! These free functions contain the logic for storing commits, fragments,
//! and blobs locally and updating the in-memory [`Sedimentree`] cache.
//! Both `Subduction` and `SyncHandler` delegate to these functions through
//! thin `&self` wrappers, keeping the implementation in one place.
//!
//! [`Subduction`]: super::Subduction
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//! [`Sedimentree`]: sedimentree_core::sedimentree::Sedimentree

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Map, crypto::digest::Digest, depth::DepthMetric, fragment::Fragment,
    id::SedimentreeId, loose_commit::LooseCommit, sedimentree::Sedimentree,
};
use subduction_crypto::verified_meta::VerifiedMeta;

use crate::{
    connection::{Connection, message::SyncDiff},
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
};
use sedimentree_core::codec::{decode::Decode, encode::Encode};

use super::error::IoError;

/// Process an incoming batch sync response: verify and store all commits
/// and fragments from the diff.
///
/// Policy-rejected diffs are logged and silently ignored (returns `Ok(())`).
pub(crate) async fn recv_batch_sync_response<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, M>,
    M: Encode + Decode,
    P: StoragePolicy<F>,
    const N: usize,
>(
    sedimentrees: &ShardedMap<SedimentreeId, Sedimentree, N>,
    storage: &StoragePowerbox<S, P>,
    from: &PeerId,
    id: SedimentreeId,
    diff: SyncDiff,
) -> Result<(), IoError<F, S, C, M>> {
    tracing::info!(
        "received batch sync response for sedimentree {:?} from peer {:?} with {} missing commits and {} missing fragments",
        id,
        from,
        diff.missing_commits.len(),
        diff.missing_fragments.len()
    );

    let mut putter_cache: Map<PeerId, Putter<F, S>> = Map::new();

    for (signed_commit, blob) in diff.missing_commits {
        let verified = match signed_commit.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("batch sync commit signature verification failed: {e}");
                continue;
            }
        };

        let author = verified.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)]
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter::<F>(*from, author, id).await {
                Ok(p) => {
                    putter_cache.insert(author_id, p);
                }
                Err(e) => {
                    tracing::warn!(
                        "policy rejected commit from {from:?} (author {author:?}) for {id:?}: {e}"
                    );
                    continue;
                }
            }
        }
        let Some(putter) = putter_cache.get(&author_id) else {
            continue;
        };

        let verified_meta = match VerifiedMeta::new(verified, blob) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("batch sync commit blob mismatch: {e}");
                continue;
            }
        };

        insert_commit_locally(sedimentrees, putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;
    }

    for (signed_fragment, blob) in diff.missing_fragments {
        let verified = match signed_fragment.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("batch sync fragment signature verification failed: {e}");
                continue;
            }
        };

        let author = verified.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)] // async in insertion path
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter::<F>(*from, author, id).await {
                Ok(p) => {
                    putter_cache.insert(author_id, p);
                }
                Err(e) => {
                    tracing::warn!(
                        "policy rejected fragment from {from:?} (author {author:?}) for {id:?}: {e}"
                    );
                    continue;
                }
            }
        }

        let Some(putter) = putter_cache.get(&author_id) else {
            continue;
        };

        let verified_meta = match VerifiedMeta::new(verified, blob) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("batch sync fragment blob mismatch: {e}");
                continue;
            }
        };

        insert_fragment_locally(sedimentrees, putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;
    }

    Ok(())
}

/// Insert a verified commit into storage and the in-memory tree.
///
/// Persists to storage first (cancel-safe: idempotent CAS writes),
/// then updates the in-memory tree. Returns whether the commit was
/// newly added (`false` if already present).
pub(crate) async fn insert_commit_locally<F: FutureForm, S: Storage<F>, const N: usize>(
    sedimentrees: &ShardedMap<SedimentreeId, Sedimentree, N>,
    putter: &Putter<F, S>,
    verified_meta: VerifiedMeta<LooseCommit>,
) -> Result<bool, S::Error> {
    let id = putter.sedimentree_id();
    let commit = verified_meta.payload().clone();

    tracing::debug!("inserting commit {:?} locally", Digest::hash(&commit));

    putter.save_sedimentree_id().await?;
    putter.save_commit(verified_meta).await?;

    let was_added = sedimentrees
        .with_entry_or_default(id, |tree| tree.add_commit(commit))
        .await;

    Ok(was_added)
}

/// Insert a verified fragment into storage and the in-memory tree.
///
/// See [`insert_commit_locally`] for cancel-safety rationale.
pub(crate) async fn insert_fragment_locally<F: FutureForm, S: Storage<F>, const N: usize>(
    sedimentrees: &ShardedMap<SedimentreeId, Sedimentree, N>,
    putter: &Putter<F, S>,
    verified_meta: VerifiedMeta<Fragment>,
) -> Result<bool, S::Error> {
    let id = putter.sedimentree_id();
    let fragment = verified_meta.payload().clone();

    putter.save_sedimentree_id().await?;
    putter.save_fragment(verified_meta).await?;

    let was_added = sedimentrees
        .with_entry_or_default(id, |tree| tree.add_fragment(fragment))
        .await;

    Ok(was_added)
}

/// Re-minimize a sedimentree in the in-memory cache.
///
/// Prunes dominated fragments and loose commits covered by fragments,
/// keeping only the minimal covering set. Storage retains the full history.
pub(crate) async fn minimize_tree<M: DepthMetric, const N: usize>(
    sedimentrees: &ShardedMap<SedimentreeId, Sedimentree, N>,
    depth_metric: &M,
    id: SedimentreeId,
) {
    sedimentrees
        .with_entry(&id, |tree| {
            *tree = tree.minimize(depth_metric);
        })
        .await;
}

/// Look up a blob from local storage by its digest.
///
/// Searches through both loose commits and fragments for the given
/// sedimentree, returning the first blob whose digest matches.
pub(crate) async fn get_blob<F: FutureForm, S: Storage<F>, P: StoragePolicy<F>>(
    storage: &StoragePowerbox<S, P>,
    id: SedimentreeId,
    digest: Digest<Blob>,
) -> Result<Option<Blob>, S::Error> {
    let local_access = storage.hydration_access();

    for verified in local_access.load_loose_commits::<F>(id).await? {
        if verified.payload().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    for verified in local_access.load_fragments::<F>(id).await? {
        if verified.payload().summary().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    Ok(None)
}
