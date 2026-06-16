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

use alloc::vec::Vec;
use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob,
    collections::{Map, Set},
    crypto::digest::Digest,
    depth::DepthMetric,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_crypto::verified_meta::VerifiedMeta;

use crate::{
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::{Connection, message::SyncDiff},
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
};
use sedimentree_core::codec::{decode::Decode, encode::Encode};

use super::error::IoError;

/// Process an incoming batch sync response: verify and store all commits
/// and fragments from the diff.
///
/// Policy-rejected diffs are logged and silently ignored (returns `Ok(())`).
#[allow(clippy::too_many_lines)]
pub(crate) async fn recv_batch_sync_response<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
    Auth: StoragePolicy<Async>,
    const SHARDS: usize,
>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    storage: &StoragePowerbox<Store, Auth>,
    from: &PeerId,
    id: SedimentreeId,
    diff: SyncDiff,
) -> Result<(), IoError<Async, Store, Conn, WireMsg>> {
    tracing::info!(
        tree = ?id,
        peer = %from,
        missing_commits = diff.missing_commits.len(),
        missing_fragments = diff.missing_fragments.len(),
        "received batch sync response"
    );

    let mut putter_cache: Map<PeerId, Putter<Async, Store>> = Map::new();

    // Collect verified commits and fragments grouped by author,
    // so we can call save_batch once per author instead of once per item.
    let mut commits_by_author: Map<PeerId, Vec<VerifiedMeta<LooseCommit>>> = Map::new();
    let mut fragments_by_author: Map<PeerId, Vec<VerifiedMeta<Fragment>>> = Map::new();

    for (signed_commit, blob) in diff.missing_commits {
        let verified = match signed_commit.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "batch sync commit signature verification failed");
                continue;
            }
        };

        let verified_meta = match VerifiedMeta::new(verified, blob) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!(error = %e, "batch sync commit blob mismatch");
                continue;
            }
        };

        let author = verified_meta.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)]
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter::<Async>(*from, author, id).await {
                Ok(p) => {
                    putter_cache.insert(author_id, p);
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %from,
                        author = ?author,
                        tree = ?id,
                        error = %e,
                        "policy rejected commit"
                    );
                    continue;
                }
            }
        }
        if !putter_cache.contains_key(&author_id) {
            continue;
        }

        commits_by_author
            .entry(author_id)
            .or_default()
            .push(verified_meta);
    }

    for (signed_fragment, blob) in diff.missing_fragments {
        let verified = match signed_fragment.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "batch sync fragment signature verification failed");
                continue;
            }
        };

        let verified_meta = match VerifiedMeta::new(verified, blob) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!(error = %e, "batch sync fragment blob mismatch");
                continue;
            }
        };

        let author = verified_meta.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)]
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter::<Async>(*from, author, id).await {
                Ok(p) => {
                    putter_cache.insert(author_id, p);
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %from,
                        author = ?author,
                        tree = ?id,
                        error = %e,
                        "policy rejected fragment"
                    );
                    continue;
                }
            }
        }
        if !putter_cache.contains_key(&author_id) {
            continue;
        }

        fragments_by_author
            .entry(author_id)
            .or_default()
            .push(verified_meta);
    }

    // Flush each author's batch to storage in a single save_batch call,
    // then update the in-memory sedimentree for each item.
    let all_authors: Set<PeerId> = commits_by_author
        .keys()
        .chain(fragments_by_author.keys())
        .copied()
        .collect();

    for author_id in all_authors {
        let Some(putter) = putter_cache.get(&author_id) else {
            tracing::warn!(author = %author_id, "putter for author unexpectedly missing from cache");
            continue;
        };
        let commits = commits_by_author.remove(&author_id).unwrap_or_default();
        let fragments = fragments_by_author.remove(&author_id).unwrap_or_default();

        // Clone payloads for in-memory tree updates before moving into save_batch.
        let commit_payloads: Vec<LooseCommit> = commits
            .iter()
            .map(|v: &VerifiedMeta<LooseCommit>| v.payload().clone())
            .collect();
        let fragment_payloads: Vec<Fragment> = fragments
            .iter()
            .map(|v: &VerifiedMeta<Fragment>| v.payload().clone())
            .collect();

        putter
            .save_batch(commits, fragments)
            .await
            .map_err(IoError::Storage)?;

        let local_access = storage.hydration_access();
        for commit in commit_payloads {
            sedimentrees
                .with_entry_hydrated(
                    id,
                    || load_tree::<Async, _>(&local_access, id),
                    |tree| tree.add_commit(commit),
                )
                .await
                .map_err(IoError::Storage)?;
        }
        for fragment in fragment_payloads {
            sedimentrees
                .with_entry_hydrated(
                    id,
                    || load_tree::<Async, _>(&local_access, id),
                    |tree| tree.add_fragment(fragment),
                )
                .await
                .map_err(IoError::Storage)?;
        }
    }

    Ok(())
}

/// Insert a verified commit into storage and the in-memory tree.
///
/// Persists to storage first (cancel-safe: idempotent CAS writes),
/// then updates the in-memory tree. Returns whether the commit was
/// newly added (`false` if already present).
pub(crate) async fn insert_commit_locally<
    Async: FutureForm,
    Store: Storage<Async>,
    const SHARDS: usize,
>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    putter: &Putter<Async, Store>,
    verified_meta: VerifiedMeta<LooseCommit>,
) -> Result<bool, Store::Error> {
    let id = putter.sedimentree_id();
    let commit = verified_meta.payload().clone();
    let head = commit.head();

    tracing::debug!(digest = ?Digest::hash(&commit), "inserting commit locally");

    putter.save_sedimentree_id().await?;

    // Newness ("was this commit not already known?") is judged against the
    // tree state *before* persisting. Read it from the resident cache, or
    // hydrate-on-miss — both reflect pre-save state (we save below). This is
    // O(1) on the resident hot path (no storage scan) and leaves the tree
    // resident so the add is a cheap hit. A `None` (tree not in storage)
    // means a brand-new tree, so the commit is necessarily new.
    let was_added = sedimentrees
        .with_hydrated_ref(
            id,
            || load_tree_via_putter::<Async, _>(putter),
            |tree| !tree.has_loose_commit(head),
        )
        .await?
        .unwrap_or(true);

    // Persist before the in-RAM mutation (storage is the source of truth;
    // the map is a cache that re-hydrates from it).
    putter.save_commit(verified_meta).await?;

    // Apply to the in-RAM tree, hydrating on a miss in case it was evicted
    // between the read above and here. On that (rare) miss the loader reloads
    // post-save state — which already contains the commit — so `add_commit`
    // is a harmless no-op and the resident tree is the full, correct history.
    sedimentrees
        .with_entry_hydrated(
            id,
            || load_tree_via_putter::<Async, _>(putter),
            |tree| {
                tree.add_commit(commit);
            },
        )
        .await?;

    Ok(was_added)
}

/// Insert a verified fragment into storage and the in-memory tree.
///
/// See [`insert_commit_locally`] for cancel-safety rationale.
pub(crate) async fn insert_fragment_locally<
    Async: FutureForm,
    Store: Storage<Async>,
    const SHARDS: usize,
>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    putter: &Putter<Async, Store>,
    verified_meta: VerifiedMeta<Fragment>,
) -> Result<bool, Store::Error> {
    let id = putter.sedimentree_id();
    let fragment = verified_meta.payload().clone();
    let head = fragment.head();

    putter.save_sedimentree_id().await?;

    // Newness from pre-save tree state (resident or hydrated); see
    // `insert_commit_locally`.
    let was_added = sedimentrees
        .with_hydrated_ref(
            id,
            || load_tree_via_putter::<Async, _>(putter),
            |tree| !tree.has_fragment(head),
        )
        .await?
        .unwrap_or(true);

    putter.save_fragment(verified_meta).await?;

    sedimentrees
        .with_entry_hydrated(
            id,
            || load_tree_via_putter::<Async, _>(putter),
            |tree| {
                tree.add_fragment(fragment);
            },
        )
        .await?;

    Ok(was_added)
}

/// Re-minimize a sedimentree in the in-memory cache.
///
/// Prunes dominated fragments and loose commits covered by fragments,
/// keeping only the minimal covering set. Storage retains the full history.
pub(crate) async fn minimize_tree<Metric: DepthMetric, const SHARDS: usize>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    depth_metric: &Metric,
    id: SedimentreeId,
) {
    sedimentrees
        .with_entry(&id, |tree| {
            tree.ensure_minimized(depth_metric);
        })
        .await;
}

/// Get a sedimentree from the in-memory cache, hydrating it from durable
/// storage on a miss.
///
/// This is the single read entry point that makes the in-memory
/// [`BoundedShardedMap`] safe to evict: every reader that needs the *full*
/// tree state must go through here so an evicted (or never-resident) tree is
/// transparently reloaded from storage rather than silently seen as empty.
///
/// # Deadlock safety
///
/// Hydration loads from storage (an `.await` that, on Wasm, is an async
/// `IndexedDB` transaction) **without holding any shard lock**. Only after
/// the load completes does it briefly lock the shard to install the tree.
/// Never hold the shard mutex across the storage await.
///
/// # Concurrency
///
/// Concurrent misses for the same id each load independently (a bounded,
/// self-correcting "thundering herd"); the first to install wins and the
/// rest are dropped — correct because all loads read the same durable
/// source. Single-flight de-duplication is intentionally not implemented.
///
/// Returns `None` only when the tree does not exist. Existence is recorded
/// by the sedimentree-id index, *not* by having commits/fragments: a tree
/// may be registered while empty (e.g. an `add_sedimentree` of an empty
/// tree), in which case this returns `Some(empty tree)`. A miss with no
/// stored data therefore consults the id index to distinguish "registered
/// but empty" (→ `Some`) from "never stored" (→ `None`).
pub(crate) async fn get_or_hydrate<
    Async: FutureForm,
    Store: Storage<Async>,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    const SHARDS: usize,
>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    storage: &StoragePowerbox<Store, Auth>,
    depth_metric: &Metric,
    id: SedimentreeId,
) -> Result<Option<Sedimentree>, Store::Error> {
    // Fast path: resident hit (also records an LRU access). Minimize in place
    // first if dirty so callers that feed the wire (fingerprint summaries /
    // resolvers) always observe the minimal form.
    if let Some(tree) = sedimentrees
        .with_entry(&id, |tree| tree.minimized(depth_metric).clone())
        .await
    {
        tracing::trace!(tree = ?id, "sedimentree cache hit");
        #[cfg(feature = "metrics")]
        crate::metrics::sedimentree_cache_hit();
        return Ok(Some(tree));
    }

    // Miss: load full history from storage with NO shard lock held. This is the
    // single point where a miss is recorded — `heads_or_hydrate` falls through
    // to here on its own miss, so it must not also count one.
    #[cfg(feature = "metrics")]
    crate::metrics::sedimentree_cache_miss();
    tracing::debug!(tree = ?id, "sedimentree cache miss; hydrating from storage");
    let local_access = storage.hydration_access();
    // Metadata-only: hydration rebuilds the tree from payloads and never
    // needs blob bytes, so this skips blob I/O entirely (a per-item file
    // read on the external-blob backends).
    let loose_commits = local_access.load_loose_commit_metas::<Async>(id).await?;
    let fragments = local_access.load_fragment_metas::<Async>(id).await?;

    // Existence is recorded by the sedimentree-id index, not by having
    // commits/fragments: a tree can be registered while empty (e.g. an
    // `add_sedimentree` of an empty tree). If the tree has no data, a
    // single-key index lookup distinguishes "registered but empty" from
    // "nonexistent" — O(1), without enumerating every id.
    if loose_commits.is_empty() && fragments.is_empty() {
        if local_access.contains_sedimentree_id::<Async>(id).await? {
            tracing::trace!(tree = ?id, "sedimentree registered but empty");
            // Cache the empty tree so repeat reads are resident hits rather
            // than repeated storage lookups. Safe to install here (unlike the
            // sync-request path) because the id is confirmed in the index, so
            // this cannot fabricate existence for a never-stored id.
            sedimentrees
                .get_or_insert_with(id, || {
                    MinimizedSedimentree::already_minimal(Sedimentree::default())
                })
                .await;
            return Ok(Some(Sedimentree::default()));
        }
        tracing::trace!(tree = ?id, "sedimentree not found in storage");
        return Ok(None);
    }

    let hydrated = Sedimentree::new(fragments, loose_commits).minimize(depth_metric);

    // Install (or adopt a concurrently-installed value). Enforces the LRU
    // cap; the lock is only taken now, after the await above. The tree is
    // already minimal, so wrap it clean.
    sedimentrees
        .get_or_insert_with(id, || {
            MinimizedSedimentree::already_minimal(hydrated.clone())
        })
        .await;
    Ok(Some(hydrated))
}

/// Compute a sedimentree's heads, hydrating from storage on a cache miss.
///
/// Like [`get_or_hydrate`] but returns only the heads, computing them inside
/// the shard lock *without cloning the whole tree out* and without the
/// double-minimization that `get_or_hydrate(...).heads()` incurred (one
/// minimize in the cache, then a second inside [`Sedimentree::heads`]). On the
/// resident-hit fast path — taken on every newly-accepted commit/fragment —
/// this is a single dirty-gated minimize plus the head walk.
///
/// Returns an empty `Vec` for a nonexistent tree (heads are advisory).
pub(crate) async fn heads_or_hydrate<
    Async: FutureForm,
    Store: Storage<Async>,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    const SHARDS: usize,
>(
    sedimentrees: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    storage: &StoragePowerbox<Store, Auth>,
    depth_metric: &Metric,
    id: SedimentreeId,
) -> Result<Vec<CommitId>, Store::Error> {
    // Fast path: resident hit. Compute heads in place — minimize only if dirty,
    // no tree clone, no re-minimize.
    if let Some(heads) = sedimentrees
        .with_entry(&id, |tree| tree.heads(depth_metric))
        .await
    {
        #[cfg(feature = "metrics")]
        crate::metrics::sedimentree_cache_hit();
        return Ok(heads);
    }

    // Miss: hydrate (which installs into the cache), then read its heads. The
    // hydrated tree is already minimal, so this second call is a clean,
    // dirty-gated no-op minimize plus the head walk. `get_or_hydrate` records
    // the miss (don't double-count it here).
    match get_or_hydrate::<Async, Store, Auth, Metric, SHARDS>(
        sedimentrees,
        storage,
        depth_metric,
        id,
    )
    .await?
    {
        Some(tree) => Ok(tree.heads_assuming_minimal()),
        None => Ok(Vec::new()),
    }
}

/// Reconstruct a sedimentree's full history directly from storage.
///
/// Returns `Ok(None)` if storage holds no commits and no fragments for `id`
/// (a brand-new or empty tree); otherwise the rebuilt tree. Used as the
/// hydrate-on-miss loader for the write paths so a mutation applied to an
/// evicted tree starts from its complete durable state, not an empty
/// default.
///
/// The tree is **not** minimized here: the write paths re-minimize after
/// their mutation (or minimization happens lazily on read), so minimizing
/// in the loader would be redundant — which is why no [`DepthMetric`] is
/// needed.
pub(crate) async fn load_tree<Async: FutureForm, Store: Storage<Async>>(
    access: &crate::storage::local_access::LocalStorageAccess<Store>,
    id: SedimentreeId,
) -> Result<Option<MinimizedSedimentree>, Store::Error> {
    // Metadata-only: the rebuilt tree holds no blobs (see `get_or_hydrate`).
    let loose_commits = access.load_loose_commit_metas::<Async>(id).await?;
    let fragments = access.load_fragment_metas::<Async>(id).await?;

    if loose_commits.is_empty() && fragments.is_empty() {
        return Ok(None);
    }
    // Full history, not yet minimized: wrap dirty so the next read minimizes.
    Ok(Some(MinimizedSedimentree::new(Sedimentree::new(
        fragments,
        loose_commits,
    ))))
}

/// Like [`load_tree`], but for the local insert paths, which already hold a
/// [`Putter`] scoped to the tree. Reads through the putter's fetch
/// capability ([`Putter::as_fetcher`]) — put implies fetch — keeping the
/// `Putter` itself write-only.
async fn load_tree_via_putter<Async: FutureForm, Store: Storage<Async>>(
    putter: &Putter<Async, Store>,
) -> Result<Option<MinimizedSedimentree>, Store::Error> {
    let fetcher = putter.as_fetcher();
    // Metadata-only: the rebuilt tree holds no blobs (see `get_or_hydrate`).
    let loose_commits = fetcher.load_loose_commit_metas().await?;
    let fragments = fetcher.load_fragment_metas().await?;

    if loose_commits.is_empty() && fragments.is_empty() {
        return Ok(None);
    }
    // Full history, not yet minimized: wrap dirty so the next read minimizes.
    Ok(Some(MinimizedSedimentree::new(Sedimentree::new(
        fragments,
        loose_commits,
    ))))
}

/// Look up a blob from local storage by its digest.
///
/// Searches through both loose commits and fragments for the given
/// sedimentree, returning the first blob whose digest matches. Matching is
/// by blob *content digest*, so it returns the correct blob even under
/// Byzantine equivocation (two payloads sharing a head but carrying
/// different blobs).
pub(crate) async fn get_blob<
    Async: FutureForm,
    Store: Storage<Async>,
    Auth: StoragePolicy<Async>,
>(
    storage: &StoragePowerbox<Store, Auth>,
    id: SedimentreeId,
    digest: Digest<Blob>,
) -> Result<Option<Blob>, Store::Error> {
    let local_access = storage.hydration_access();

    for verified in local_access.load_loose_commits::<Async>(id).await? {
        if verified.payload().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    for verified in local_access.load_fragments::<Async>(id).await? {
        if verified.payload().summary().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    Ok(None)
}
