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

#[derive(Debug)]
#[expect(clippy::struct_field_names)]
pub(crate) struct IngestSummary<Rejection> {
    pub(crate) commit_ids: Vec<CommitId>,
    pub(crate) fragment_ids: Vec<CommitId>,
    pub(crate) rejected_commit_ids: Vec<(CommitId, Rejection)>,
    pub(crate) rejected_fragment_ids: Vec<(CommitId, Rejection)>,
}

impl<Rejection> IngestSummary<Rejection> {
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            commit_ids: Vec::new(),
            fragment_ids: Vec::new(),
            rejected_commit_ids: Vec::new(),
            rejected_fragment_ids: Vec::new(),
        }
    }
}

impl<Rejection> Default for IngestSummary<Rejection> {
    fn default() -> Self {
        Self::new()
    }
}

/// Process an incoming batch sync response: verify and store all commits
/// and fragments from the diff.
///
/// Each author's batch is persisted and applied to the resident sedimentree
/// inside one shard-lock hold, so a concurrent reader (e.g. the responder
/// building a diff) never observes the pre-apply tree after the save's change
/// notification has fired. See [`insert_commit_locally`] for the rationale.
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
) -> Result<IngestSummary<Auth::PutDisallowed>, IoError<Async, Store, Conn, WireMsg>> {
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
    let mut summary = IngestSummary::default();

    for (signed_commit, blob) in diff.missing_commits {
        let verified = match signed_commit.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "batch sync commit signature verification failed");
                #[cfg(feature = "metrics")]
                crate::metrics::sync_verify_failure("commit");
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

        let commit_id = verified_meta.payload().head();
        if verified_meta.payload().sedimentree_id() != id {
            tracing::warn!(
                expected = ?id,
                actual = ?verified_meta.payload().sedimentree_id(),
                commit_id = ?commit_id,
                "batch commit payload sedimentree_id does not match response id; rejecting"
            );
            continue;
        }
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
                    summary.rejected_commit_ids.push((commit_id, e));
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
                #[cfg(feature = "metrics")]
                crate::metrics::sync_verify_failure("fragment");
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

        let fragment_id = verified_meta.payload().head();
        if verified_meta.payload().sedimentree_id() != id {
            tracing::warn!(
                expected = ?id,
                actual = ?verified_meta.payload().sedimentree_id(),
                commit_id = ?fragment_id,
                "batch fragment payload sedimentree_id does not match response id; rejecting"
            );
            continue;
        }
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
                    summary.rejected_fragment_ids.push((fragment_id, e));
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

        // One shard-lock hold spans newness, persistence, and the in-RAM
        // apply for this author's batch; see `insert_commit_locally` for the
        // full rationale. `save_batch` is where storage emits the "part
        // changed" notification the sync task wakes on, so a reader arriving
        // while it is in flight must block here rather than be served the
        // pre-apply tree — a stale view that the already-fired notification
        // would never refresh. The guard is dropped before the next author,
        // keeping each hold to a single author's batch.
        //
        // Loader choice: previously the newness read used this putter loader
        // and the applies used `load_tree` via `storage.hydration_access()`.
        // The two are interchangeable on a miss — both clone the same
        // `Arc<Store>` (the powerbox hands the putter and the hydration access
        // the same backend) and call exactly `load_loose_commit_metas` +
        // `load_fragment_metas` for the putter's id, which is `id` here — so
        // unifying on the putter loader preserves miss-path behaviour and
        // matches the local-insert paths.
        //
        // Newness is judged against this pre-save tree. As in the previous
        // `with_hydrated_ref` form, a tree absent from storage hydrates to
        // `V::default()` — an empty tree — so every payload is new.
        //
        // Cancel safety: as in `insert_commit_locally`, a drop between
        // `save_batch` resolving and the apply leaves the batch durable with
        // a stale resident tree; the hydrate-on-miss path reloads post-save
        // state, so re-application is a no-op.
        let mut tree = sedimentrees
            .entry_guard_hydrated(id, || load_tree_via_putter::<Async, _>(putter))
            .await
            .map_err(IoError::Storage)?;

        let new_commit_ids: Vec<CommitId> = commit_payloads
            .iter()
            .map(LooseCommit::head)
            .filter(|head| !tree.has_loose_commit(*head))
            .collect();
        let new_fragment_ids: Vec<CommitId> = fragment_payloads
            .iter()
            .map(Fragment::head)
            .filter(|head| !tree.has_fragment(*head))
            .collect();

        putter
            .save_batch(commits, fragments)
            .await
            .map_err(IoError::Storage)?;

        summary.commit_ids.extend(new_commit_ids);
        summary.fragment_ids.extend(new_fragment_ids);

        for commit in commit_payloads {
            tree.add_commit(commit);
        }
        for fragment in fragment_payloads {
            tree.add_fragment(fragment);
        }
        drop(tree);
    }

    Ok(summary)
}

/// Insert a verified commit into storage and the in-memory tree.
///
/// Both the persistence and the in-memory apply happen inside a single
/// shard-lock hold, so a concurrent reader is never served the pre-apply
/// tree after the save's change notification has fired. Returns whether the
/// commit was newly added (`false` if already present).
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
    // Object membership: a commit is only valid for the sedimentree its own
    // payload names. The wire/session id is untrusted routing metadata; two
    // sedimentrees that share a fork base contain causally-connected commits,
    // so causal reachability must never be treated as membership.
    if commit.sedimentree_id() != id {
        tracing::warn!(
            expected = ?id,
            actual = ?commit.sedimentree_id(),
            commit_id = ?head,
            "commit payload sedimentree_id does not match its wire id; rejecting"
        );
        return Ok(false);
    }
    tracing::debug!(digest = ?Digest::hash(&commit), "inserting commit locally");

    // One shard-lock hold spans newness, persistence, and the in-RAM apply.
    //
    // Persisting emits the "part changed" notification the sync task wakes
    // on, so a reader arriving while the write is in flight (e.g. the
    // responder building a `ResponderDiff` via `with_entry`) must not be
    // served the pre-apply tree: holding the shard lock across `save_commit`
    // makes such a reader wait until the apply below lands, instead of
    // settling on a stale view that no further notification will refresh.
    //
    // Newness ("was this commit not already known?") is judged against the
    // pre-save tree state: the guard is the resident cache, or hydrates on a
    // miss, and either way reflects state before the save below. A tree not
    // in storage hydrates to `V::default()` — a brand-new tree — so the
    // commit is necessarily new. The resident hot path is O(1) (no storage
    // scan).
    //
    // Cancel safety: if this future is dropped after `save_commit` resolved
    // but before the apply, the commit is durable while the resident tree is
    // stale. That is benign — the hydrate-on-miss path, used whenever the
    // tree is not resident, reloads post-save state, so a later load already
    // contains the commit and its re-application is a no-op.
    let mut tree = sedimentrees
        .entry_guard_hydrated(id, || load_tree_via_putter::<Async, _>(putter))
        .await?;

    let was_added = !tree.has_loose_commit(head);

    // Persist before the in-RAM mutation: storage is the source of truth; the
    // map is a cache that re-hydrates from it.
    putter.save_commit(verified_meta).await?;

    tree.add_commit(commit);
    let frontier_after: Vec<CommitId> = tree.heads(&sedimentree_core::depth::CountLeadingZeroBytes);
    drop(tree);

    tracing::debug!(
        sedimentree_id = ?id,
        commit_id = ?head,
        was_added,
        frontier = ?frontier_after,
        "inserted local loose commit into resident sedimentree",
    );

    Ok(was_added)
}

/// Insert a verified fragment into storage and the in-memory tree.
///
/// See [`insert_commit_locally`] for the single-hold rationale and
/// cancel-safety note.
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
    // Object membership: see the comment in `insert_commit_locally`.
    if fragment.sedimentree_id() != id {
        tracing::warn!(
            expected = ?id,
            actual = ?fragment.sedimentree_id(),
            commit_id = ?head,
            "fragment payload sedimentree_id does not match its wire id; rejecting"
        );
        return Ok(false);
    }

    // One shard-lock hold spans newness, persistence, and the in-RAM apply;
    // see `insert_commit_locally` for the rationale and cancel-safety note.
    let mut tree = sedimentrees
        .entry_guard_hydrated(id, || load_tree_via_putter::<Async, _>(putter))
        .await?;

    let was_added = !tree.has_fragment(head);

    // Persist before the in-RAM mutation; see `insert_commit_locally`.
    putter.save_fragment(verified_meta).await?;

    tree.add_fragment(fragment);
    drop(tree);

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
    #[cfg(feature = "metrics")]
    let hydration = crate::metrics::HydrationGuard::new();
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
            #[cfg(feature = "metrics")]
            hydration.complete();
            return Ok(Some(Sedimentree::default()));
        }
        tracing::trace!(tree = ?id, "sedimentree not found in storage");
        // Not-found probes drop the guard without a duration sample.
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
    #[cfg(feature = "metrics")]
    hydration.complete();
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
    #[cfg(feature = "metrics")]
    let hydration = crate::metrics::HydrationGuard::new();
    // Metadata-only: the rebuilt tree holds no blobs (see `get_or_hydrate`).
    let loose_commits = access.load_loose_commit_metas::<Async>(id).await?;
    let fragments = access.load_fragment_metas::<Async>(id).await?;

    if loose_commits.is_empty() && fragments.is_empty() {
        // Not-found probes drop the guard without a duration sample.
        return Ok(None);
    }
    #[cfg(feature = "metrics")]
    hydration.complete();
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
    #[cfg(feature = "metrics")]
    let hydration = crate::metrics::HydrationGuard::new();
    let fetcher = putter.as_fetcher();
    // Metadata-only: the rebuilt tree holds no blobs (see `get_or_hydrate`).
    let loose_commits = fetcher.load_loose_commit_metas().await?;
    let fragments = fetcher.load_fragment_metas().await?;

    if loose_commits.is_empty() && fragments.is_empty() {
        // Not-found probes drop the guard without a duration sample.
        return Ok(None);
    }
    #[cfg(feature = "metrics")]
    hydration.complete();
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

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod tests {
    use alloc::{collections::BTreeSet, sync::Arc};

    use future_form::{FutureForm, Sendable};
    use futures::future::BoxFuture;
    use sedimentree_core::blob::verified::VerifiedBlobMeta;
    use subduction_crypto::signer::memory::MemorySigner;
    use tokio::sync::Notify;

    use crate::{
        connection::message::{RequestedData, SyncMessage},
        peer::id::PeerId,
        policy::open::OpenPolicy,
        storage::memory::MemoryStorage,
    };

    use super::*;

    /// Which storage write the double blocks on. The two gated methods are
    /// never both exercised by one test, so a single `save_entered` /
    /// `save_release` pair is unambiguous once the method is fixed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum GatedWrite {
        LooseCommit,
        Batch,
    }

    /// Wraps [`MemoryStorage`], gating one write method behind two test
    /// signals so a write can be held mid-save while a reader is attempted:
    /// `save_entered` fires once the gated save is reached, then the save
    /// blocks until `save_release` fires.
    #[derive(Debug, Clone)]
    pub(crate) struct GatedSaveStorage {
        inner: MemoryStorage,
        gated: GatedWrite,
        save_entered: Arc<Notify>,
        save_release: Arc<Notify>,
    }

    impl GatedSaveStorage {
        pub(crate) fn new(gated: GatedWrite) -> Self {
            Self {
                inner: MemoryStorage::new(),
                gated,
                save_entered: Arc::new(Notify::new()),
                save_release: Arc::new(Notify::new()),
            }
        }

        /// Fires once the gated save is reached. Wait on this before probing
        /// the reader, so the `save_batch`/`save_commit` critical section is
        /// known to be in flight.
        pub(crate) fn save_entered(&self) -> Arc<Notify> {
            Arc::clone(&self.save_entered)
        }

        /// Release the gated save once the reader probe has been asserted.
        pub(crate) fn save_release(&self) -> Arc<Notify> {
            Arc::clone(&self.save_release)
        }
    }

    impl Storage<Sendable> for GatedSaveStorage {
        type Error = <MemoryStorage as Storage<Sendable>>::Error;

        fn save_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::save_sedimentree_id(&self.inner, id)
        }

        fn delete_sedimentree_id(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::delete_sedimentree_id(&self.inner, id)
        }

        fn load_all_sedimentree_ids(
            &self,
        ) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
            Storage::<Sendable>::load_all_sedimentree_ids(&self.inner)
        }

        fn contains_sedimentree_id(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<bool, Self::Error>> {
            Storage::<Sendable>::contains_sedimentree_id(&self.inner, id)
        }

        /// Signal that the write has reached the save, then block until the
        /// test releases it. While blocked, the caller holds its shard guard
        /// across this await.
        fn save_loose_commit(
            &self,
            id: SedimentreeId,
            verified: VerifiedMeta<LooseCommit>,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Box::pin(async move {
                if self.gated == GatedWrite::LooseCommit {
                    self.save_entered.notify_one();
                    self.save_release.notified().await;
                }
                Storage::<Sendable>::save_loose_commit(&self.inner, id, verified).await
            })
        }

        fn list_commit_ids(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
            Storage::<Sendable>::list_commit_ids(&self.inner, id)
        }

        fn load_loose_commits(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
            Storage::<Sendable>::load_loose_commits(&self.inner, id)
        }

        fn load_loose_commit_metas(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
            Storage::<Sendable>::load_loose_commit_metas(&self.inner, id)
        }

        fn load_loose_commit(
            &self,
            id: SedimentreeId,
            commit_id: CommitId,
        ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
            Storage::<Sendable>::load_loose_commit(&self.inner, id, commit_id)
        }

        fn delete_loose_commit(
            &self,
            id: SedimentreeId,
            commit_id: CommitId,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::delete_loose_commit(&self.inner, id, commit_id)
        }

        fn delete_loose_commits(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::delete_loose_commits(&self.inner, id)
        }

        fn save_fragment(
            &self,
            id: SedimentreeId,
            verified: VerifiedMeta<Fragment>,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::save_fragment(&self.inner, id, verified)
        }

        fn load_fragment(
            &self,
            id: SedimentreeId,
            fragment_head: CommitId,
        ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
            Storage::<Sendable>::load_fragment(&self.inner, id, fragment_head)
        }

        fn list_fragment_ids(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
            Storage::<Sendable>::list_fragment_ids(&self.inner, id)
        }

        fn load_fragments(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
            Storage::<Sendable>::load_fragments(&self.inner, id)
        }

        fn load_fragment_metas(
            &self,
            id: SedimentreeId,
        ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
            Storage::<Sendable>::load_fragment_metas(&self.inner, id)
        }

        fn delete_fragment(
            &self,
            id: SedimentreeId,
            fragment_head: CommitId,
        ) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::delete_fragment(&self.inner, id, fragment_head)
        }

        fn delete_fragments(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
            Storage::<Sendable>::delete_fragments(&self.inner, id)
        }

        /// Batch-save gate: signal that the write has reached `save_batch`,
        /// then block until released. While blocked, the caller holds its
        /// shard guard across this await.
        fn save_batch(
            &self,
            id: SedimentreeId,
            commits: Vec<VerifiedMeta<LooseCommit>>,
            fragments: Vec<VerifiedMeta<Fragment>>,
        ) -> BoxFuture<'_, Result<usize, Self::Error>> {
            Box::pin(async move {
                if self.gated == GatedWrite::Batch {
                    self.save_entered.notify_one();
                    self.save_release.notified().await;
                }
                Storage::<Sendable>::save_batch(&self.inner, id, commits, fragments).await
            })
        }
    }

    /// Regression guard for the "persisted but not yet cached" window: a
    /// reader on the responder's cache path must not be served the pre-apply
    /// tree while a local write's `save_commit` is in flight.
    ///
    /// Writes persist to storage — which emits the change notification the
    /// sync task wakes on — before applying to the in-RAM tree. If those are
    /// not one shard-lock hold, a reader arriving in between observes the
    /// pre-apply tree, and because the notification already fired, no later
    /// sync round refreshes it: the peer settles on a stale view permanently.
    /// This test drives exactly that interleaving with a gated save and
    /// asserts the reader blocks until the apply lands.
    ///
    /// Against the pre-fix code (separate lock holds for the newness read,
    /// the save, and the apply) the first poll of `reader` completes with
    /// `Some(false)` and the `is_none` assertion below fails — the negative
    /// check for this guard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_cannot_observe_pre_apply_tree_during_save() {
        let id = SedimentreeId::new([0x5A; 32]);
        let head = CommitId::new([0xC0; 32]);
        let signer = MemorySigner::from_bytes(&[0x11; 32]);

        let sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree>> =
            Arc::new(BoundedShardedMap::with_key(0, 0));

        let gated = GatedSaveStorage::new(GatedWrite::LooseCommit);
        let entered = gated.save_entered();
        let release = gated.save_release();
        let storage = StoragePowerbox::new(gated, Arc::new(OpenPolicy));
        let putter = storage.local_putter::<Sendable>(id);

        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (id, head, BTreeSet::new()),
            VerifiedBlobMeta::new(Blob::new(alloc::vec![1u8; 16])),
        )
        .await;

        // Writer: the real local-insert path, held inside `save_commit`.
        let writer_maps = Arc::clone(&sedimentrees);
        let writer = tokio::spawn(async move {
            insert_commit_locally::<Sendable, _, 256>(&writer_maps, &putter, verified).await
        });

        // Wait until the writer is actually inside `save_commit`. Only after
        // this signal is it (once fixed) holding the shard lock.
        entered.notified().await;

        // Reader: the exact shard-locked path the sync responder uses
        // (`with_entry` on the same id). While the write is in flight it must
        // not acquire the shard, so a single poll is Pending.
        let reader_maps = Arc::clone(&sedimentrees);
        let mut reader = Box::pin(async move {
            reader_maps
                .with_entry(&id, |tree| tree.has_loose_commit(head))
                .await
        });
        assert!(
            futures::future::poll_immediate(reader.as_mut())
                .await
                .is_none(),
            "reader acquired the shard while save_commit was in flight, so it \
             observed the pre-apply tree — which the already-fired sync \
             notification will never refresh"
        );

        // Release the blocked save; the writer applies the commit and drops
        // the shard guard.
        release.notify_one();
        let was_added = writer
            .await
            .expect("writer task panicked")
            .expect("save succeeds");
        assert!(was_added, "the commit must be newly added");

        // The reader now acquires the shard and must observe the applied
        // commit.
        assert_eq!(
            reader.await,
            Some(true),
            "reader must observe the commit once the write has applied"
        );
    }

    /// Minimal [`Connection`] for the `recv_batch_sync_response` call site.
    ///
    /// That function is generic over the connection only for its error type
    /// and never touches it on the `Ok` path, so a no-op implementation keeps
    /// this test independent of the `test_utils` feature.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct NoopConnection;

    impl Connection<Sendable, SyncMessage> for NoopConnection {
        type DisconnectionError = core::fmt::Error;
        type SendError = core::fmt::Error;
        type RecvError = core::fmt::Error;

        fn disconnect(
            &self,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
            Sendable::from_future(async { Ok(()) })
        }

        fn send(
            &self,
            _message: &SyncMessage,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
            Sendable::from_future(async { Ok(()) })
        }

        fn recv(
            &self,
        ) -> <Sendable as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
            Sendable::from_future(async { Err(core::fmt::Error) })
        }
    }

    /// Regression guard for the same persisted-but-not-cached window in
    /// `recv_batch_sync_response`: a reader on the responder's cache path must
    /// not be served the pre-apply tree while an ingest's `save_batch` is in
    /// flight.
    ///
    /// Unlike the single-commit path, ingest persists a whole author's batch
    /// before applying it, so a missing lock hold silently drops the peer's
    /// data from the resident tree with no later round to refresh it. This
    /// test gates `save_batch` and asserts the reader blocks until the apply
    /// lands.
    ///
    /// Against the pre-fix body (newness read, `save_batch`, and per-item
    /// `with_entry_hydrated` applies each taking the shard lock separately) the
    /// first poll of `reader` completes with `Some(false)` and the `is_none`
    /// assertion below fails — the negative check for this guard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_cannot_observe_pre_apply_tree_during_save_batch() {
        let id = SedimentreeId::new([0x5B; 32]);
        let head = CommitId::new([0xC1; 32]);
        let signer = MemorySigner::from_bytes(&[0x22; 32]);
        let from = PeerId::new([0x42; 32]);

        let sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree>> =
            Arc::new(BoundedShardedMap::with_key(0, 0));

        let gated = GatedSaveStorage::new(GatedWrite::Batch);
        let entered = gated.save_entered();
        let release = gated.save_release();
        let storage = StoragePowerbox::new(gated, Arc::new(OpenPolicy));

        let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (id, head, BTreeSet::new()),
            VerifiedBlobMeta::new(Blob::new(alloc::vec![2u8; 16])),
        )
        .await;
        let (signed_commit, _, blob) = verified.into_full_parts();
        let diff = SyncDiff {
            missing_commits: vec![(signed_commit, blob)],
            missing_fragments: Vec::new(),
            requesting: RequestedData::default(),
        };

        // Writer: the real ingest path, held inside `save_batch`.
        let writer_maps = Arc::clone(&sedimentrees);
        let writer = tokio::spawn(async move {
            recv_batch_sync_response::<Sendable, _, NoopConnection, SyncMessage, OpenPolicy, 256>(
                &writer_maps,
                &storage,
                &from,
                id,
                diff,
            )
            .await
        });

        // Wait until the writer is actually inside `save_batch`; once fixed it
        // holds the shard lock across the save.
        entered.notified().await;

        // Reader: the exact shard-locked path the sync responder uses
        // (`with_entry` on the same id). While the batch save is in flight it
        // must not acquire the shard, so a single poll is Pending.
        let reader_maps = Arc::clone(&sedimentrees);
        let mut reader = Box::pin(async move {
            reader_maps
                .with_entry(&id, |tree| tree.has_loose_commit(head))
                .await
        });
        assert!(
            futures::future::poll_immediate(reader.as_mut())
                .await
                .is_none(),
            "reader acquired the shard while save_batch was in flight, so it \
             observed the pre-apply tree — which the already-fired sync \
             notification will never refresh"
        );

        // Release the blocked save; the writer applies the batch and drops
        // the shard guard.
        release.notify_one();
        let summary = writer
            .await
            .expect("writer task panicked")
            .expect("ingest succeeds");
        assert_eq!(
            summary.commit_ids,
            vec![head],
            "the ingested commit must be reported as newly added"
        );

        // The reader now acquires the shard and must observe the applied
        // commit.
        assert_eq!(
            reader.await,
            Some(true),
            "reader must observe the commit once the ingest has applied"
        );
    }
}
