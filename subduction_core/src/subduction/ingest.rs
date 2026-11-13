//! Shared data-ingestion helpers used by both [`Subduction`] and [`SyncHandler`].
//!
//! These free functions contain the logic for storing commits, fragments,
//! and blobs locally, and for reading each tree's minimized [`Sedimentree`]
//! through the node's [minimized tree store](crate::minimized_tree_store).
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
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

use crate::{
    connection::{Connection, message::SyncDiff},
    minimized_tree_store::{Lookup, MinimizedTreeStore},
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    storage::{powerbox::StoragePowerbox, putter::Putter, state::cache::Resident, traits::Storage},
};
use sedimentree_core::codec::{decode::Decode, encode::Encode};

use super::error::IoError;

/// Items a batch sync response added to the local tree, in wire form, so
/// the caller can push them on to its own subscribers (see
/// [`peers::build_pushes`](super::peers::build_pushes)).
#[derive(Debug, Default)]
pub(crate) struct Ingested {
    /// Private initial-load readiness, including idempotently written items.
    /// Public sync statistics still count offers, not acceptance.
    pub(crate) stored_content: bool,
    pub(crate) commits: Vec<(Signed<LooseCommit>, Blob)>,
    pub(crate) fragments: Vec<(Signed<Fragment>, Blob)>,
}

impl Ingested {
    pub(crate) fn commit(signed: Signed<LooseCommit>, blob: Blob) -> Self {
        Self {
            commits: alloc::vec![(signed, blob)],
            fragments: Vec::new(),
            stored_content: true,
        }
    }

    pub(crate) fn fragment(signed: Signed<Fragment>, blob: Blob) -> Self {
        Self {
            commits: Vec::new(),
            fragments: alloc::vec![(signed, blob)],
            stored_content: true,
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.commits.is_empty() && self.fragments.is_empty()
    }
}

/// Process an incoming batch sync response: verify and store all commits
/// and fragments from the diff. Returns the items not already present in the
/// minimized local tree; the rest are written idempotently but not reported,
/// so they are not re-pushed.
///
/// Policy-rejected items are logged and skipped.
#[allow(clippy::too_many_lines)]
pub(crate) async fn recv_batch_sync_response<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
    Auth: StoragePolicy<Async>,
    Trees: MinimizedTreeStore<Async>,
    Metric: DepthMetric,
>(
    sedimentrees: &Trees,
    depth_metric: &Metric,
    storage: &StoragePowerbox<Store, Auth, Async>,
    from: &PeerId,
    id: SedimentreeId,
    diff: SyncDiff,
) -> Result<Ingested, IoError<Async, Store, Conn, WireMsg>> {
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

        let author = verified_meta.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)]
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter(*from, author, id).await {
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

        let author = verified_meta.verified_author();
        let author_id = PeerId::from(*author.verifying_key());

        #[allow(clippy::map_entry)]
        if !putter_cache.contains_key(&author_id) {
            match storage.get_putter(*from, author, id).await {
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

    let mut ingested = Ingested::default();
    for author_id in all_authors {
        let Some(putter) = putter_cache.get(&author_id) else {
            tracing::warn!(author = %author_id, "putter for author unexpectedly missing from cache");
            continue;
        };
        let commits = commits_by_author.remove(&author_id).unwrap_or_default();
        let fragments = fragments_by_author.remove(&author_id).unwrap_or_default();

        // Newness is judged against the tree *before* persisting, as in
        // `insert_commit_locally`: a hydrate-on-miss after `save_batch` would
        // load the just-written items and report nothing as new. A `None`
        // tree (absent from storage) means everything is new.
        //
        // `seen_*` start as the heads the tree already has; each accepted item
        // is added as we go, so `Set::insert` returns `false` for a head that
        // is in the tree *or* appeared earlier in this response.
        let (mut seen_commits, mut seen_fragments): (Set<CommitId>, Set<CommitId>) =
            read_minimized(sedimentrees, depth_metric, &putter.as_fetcher(), |tree| {
                (
                    commits
                        .iter()
                        .map(|v| v.payload().head())
                        .filter(|h| tree.has_loose_commit(*h))
                        .collect(),
                    fragments
                        .iter()
                        .map(|v| v.payload().head())
                        .filter(|h| tree.has_fragment(*h))
                        .collect(),
                )
            })
            .await
            .map_err(IoError::Storage)?
            .unwrap_or_default();

        ingested.commits.extend(
            commits
                .iter()
                .filter(|v| seen_commits.insert(v.payload().head()))
                .map(|v| (v.signed().clone(), v.blob().clone())),
        );
        ingested.fragments.extend(
            fragments
                .iter()
                .filter(|v| seen_fragments.insert(v.payload().head()))
                .map(|v| (v.signed().clone(), v.blob().clone())),
        );
        putter
            .save_batch(commits, fragments)
            .await
            .map_err(IoError::from)?;
        ingested.stored_content = true;
    }

    Ok(ingested)
}

/// Insert a verified commit into storage and the in-memory tree.
///
/// Persists to storage first (cancel-safe: idempotent CAS writes),
/// then updates the in-memory tree. Returns whether the commit was
/// newly added (`false` if already present).
pub(crate) async fn insert_commit_locally<
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Metric: DepthMetric,
>(
    sedimentrees: &Trees,
    depth_metric: &Metric,
    putter: &Putter<Async, Store>,
    verified_meta: VerifiedMeta<LooseCommit>,
) -> Result<bool, crate::storage::mutation::MutationError<Store::Error>> {
    let commit = verified_meta.payload().clone();
    let head = commit.head();

    tracing::debug!(digest = ?Digest::hash(&commit), "inserting commit locally");

    // Newness ("was this commit not already known?") is judged against the
    // tree state *before* persisting. Read it from the resident cache, or
    // hydrate-on-miss — both reflect pre-save state (we save below). This is
    // O(1) on the resident hot path (no storage scan) and leaves the tree
    // resident so the add is a cheap hit. A `None` (tree not in storage)
    // means a brand-new tree, so the commit is necessarily new.
    let was_added = read_minimized(sedimentrees, depth_metric, &putter.as_fetcher(), |tree| {
        !tree.has_loose_commit(head)
    })
    .await?
    .unwrap_or(true);

    putter.save_commit(verified_meta).await?;

    Ok(was_added)
}

/// Insert a verified fragment into storage and the in-memory tree.
///
/// See [`insert_commit_locally`] for cancel-safety rationale.
pub(crate) async fn insert_fragment_locally<
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Metric: DepthMetric,
>(
    sedimentrees: &Trees,
    depth_metric: &Metric,
    putter: &Putter<Async, Store>,
    verified_meta: VerifiedMeta<Fragment>,
) -> Result<bool, crate::storage::mutation::MutationError<Store::Error>> {
    let fragment = verified_meta.payload().clone();
    let head = fragment.head();

    // Newness from pre-save tree state (resident or hydrated); see
    // `insert_commit_locally`.
    let was_added = read_minimized(sedimentrees, depth_metric, &putter.as_fetcher(), |tree| {
        !tree.has_fragment(head)
    })
    .await?
    .unwrap_or(true);

    putter.save_fragment(verified_meta).await?;

    Ok(was_added)
}

/// Re-minimize a sedimentree in the in-memory cache.
///
/// Prunes dominated fragments and loose commits covered by fragments,
/// keeping only the minimal covering set. Storage retains the full history.
pub(crate) async fn minimize_tree<
    Async: FutureForm,
    Trees: MinimizedTreeStore<Async>,
    Metric: DepthMetric,
>(
    sedimentrees: &Trees,
    depth_metric: &Metric,
    id: SedimentreeId,
) {
    // Best effort: a miss or store error leaves nothing to minimize.
    if let Ok(Lookup::Hit(mut tree)) = sedimentrees.get(id).await {
        tree.ensure_minimized(depth_metric);
    }
}

/// Get a sedimentree from the in-memory cache, hydrating it from durable
/// storage on a miss.
///
/// This is the single read entry point that makes the in-memory
/// [`MinimizedTreeStore`] safe to evict: every reader that needs the *full*
/// tree state must go through here so an evicted (or never-resident) tree is
/// transparently reloaded from storage rather than silently seen as empty.
///
/// # Deadlock safety
///
/// Hydration loads from storage (an `.await` that, on Wasm, is an async
/// `IndexedDB` transaction) **without holding the tree's store lock** or any
/// store entry. Freshly loaded trees are also minimized outside the lock.
/// Only then does it briefly take the store lock to populate the store. If a
/// mutation overlapped the load, it retries, and the next lookup hits any tree
/// another reader populated meanwhile. Never hold a store entry across the
/// storage await.
///
/// # Concurrency
///
/// Concurrent misses for the same id each load independently (a bounded,
/// self-correcting "thundering herd"); the first to populate wins and the
/// rest are kept out — correct because all loads read the same durable
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
    Trees: MinimizedTreeStore<Async>,
>(
    sedimentrees: &Trees,
    storage: &StoragePowerbox<Store, Auth, Async>,
    depth_metric: &Metric,
    id: SedimentreeId,
) -> Result<Option<Sedimentree>, Store::Error> {
    // Minimize a resident tree in place first if dirty, so callers that feed
    // the wire (fingerprint summaries / resolvers) always see the minimal form.
    read_cached_prepared(
        sedimentrees,
        &storage.local_fetcher(id),
        Count::Recorded,
        |tree| tree.ensure_minimized(depth_metric),
        |tree| tree.minimized(depth_metric).clone(),
    )
    .await
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
/// Returns `None` for a nonexistent tree, with the same existence rule as
/// [`get_or_hydrate`]: a registered-but-empty tree yields `Some(vec![])`.
pub(crate) async fn heads_or_hydrate<
    Async: FutureForm,
    Store: Storage<Async>,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    Trees: MinimizedTreeStore<Async>,
>(
    sedimentrees: &Trees,
    storage: &StoragePowerbox<Store, Auth, Async>,
    depth_metric: &Metric,
    id: SedimentreeId,
) -> Result<Option<Vec<CommitId>>, Store::Error> {
    // Compute heads in place: minimize only if dirty, no tree clone, and no
    // second minimize inside `Sedimentree::heads`.
    read_cached_prepared(
        sedimentrees,
        &storage.local_fetcher(id),
        Count::Recorded,
        |tree| tree.ensure_minimized(depth_metric),
        |tree| tree.heads(depth_metric),
    )
    .await
}

/// Reconstruct a sedimentree's full history directly from storage.
///
/// Returns `Ok(None)` if storage holds no commits and no fragments for `id`
/// (a brand-new or empty tree); otherwise the rebuilt tree.
///
/// The tree is **not** minimized here: membership checks can use the full
/// history without a [`DepthMetric`]. Readers needing the minimal form prepare
/// the loaded tree before populating the store (see [`get_or_hydrate`]).
pub(crate) async fn load_tree<Async: FutureForm, Store: Storage<Async>>(
    access: &crate::storage::fetcher::Fetcher<Async, Store>,
) -> Result<Option<MinimizedSedimentree>, Store::Error> {
    #[cfg(feature = "metrics")]
    let hydration = crate::metrics::HydrationGuard::new();
    // Metadata-only: the rebuilt tree holds no blobs (see `get_or_hydrate`).
    let loose_commits = access.load_loose_commit_metas().await?;
    let fragments = access.load_fragment_metas().await?;

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

#[cfg(test)]
/// Read a tree from the store, loading it from storage if needed.
/// Before populating the store with the loaded tree, check that no writes or
/// deletes overlapped
/// the read; otherwise retry. This prevents an old read from putting stale data
/// back into the store after another operation cleared it.
/// No store or revision locks are held while waiting for storage.
pub(crate) async fn read_cached<Async, Store, Trees, Inspect, R>(
    cache: &Trees,
    fetcher: &crate::storage::fetcher::Fetcher<Async, Store>,
    inspect: Inspect,
) -> Result<Option<R>, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Inspect: FnMut(&mut MinimizedSedimentree) -> R,
{
    read_cached_prepared(cache, fetcher, Count::Silent, |_| (), inspect).await
}

/// Read a tree for a write path's newness check, minimizing a freshly loaded
/// tree before it populates the store, so the store never holds a dirty tree.
async fn read_minimized<Async, Store, Trees, Metric, Inspect, R>(
    cache: &Trees,
    depth_metric: &Metric,
    fetcher: &crate::storage::fetcher::Fetcher<Async, Store>,
    inspect: Inspect,
) -> Result<Option<R>, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Metric: DepthMetric,
    Inspect: FnMut(&mut MinimizedSedimentree) -> R,
{
    read_cached_prepared(
        cache,
        fetcher,
        Count::Silent,
        |tree| tree.ensure_minimized(depth_metric),
        inspect,
    )
    .await
}

/// Prepare each freshly loaded tree before populating the store with it.
/// Preparation may be expensive (e.g. minimization) and is repeated on retries.
/// Revision validation still happens afterward, under the tree's store lock.
/// `prepare` does not run on resident entries, so `inspect` must handle dirty
/// ones. Both may run more than once.
async fn read_cached_prepared<Async, Store, Trees, Prepare, Inspect, R>(
    cache: &Trees,
    fetcher: &crate::storage::fetcher::Fetcher<Async, Store>,
    count: Count,
    prepare: Prepare,
    mut inspect: Inspect,
) -> Result<Option<R>, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Prepare: Fn(&mut MinimizedSedimentree),
    Inspect: FnMut(&mut MinimizedSedimentree) -> R,
{
    let coordination = fetcher.coordination();
    loop {
        let seen = match coordination.lookup(cache).await {
            Resident::Hit(mut tree) => {
                count.hit(fetcher.sedimentree_id());
                return Ok(Some(inspect(&mut tree)));
            }
            Resident::Miss(seen) => seen,
        };
        count.miss(fetcher.sedimentree_id());
        let read = coordination.prepare_cache_read(cache, seen).await;
        let mut loaded = load_tree(fetcher).await?;
        if loaded.is_none() && fetcher.contains_sedimentree_id().await? {
            loaded = Some(MinimizedSedimentree::default());
        }
        if let Some(tree) = loaded.as_mut() {
            prepare(tree);
        }
        if let Some(result) = read.populate(loaded, &mut inspect).await {
            return Ok(result);
        }
        crate::spawn::yield_once().await;
    }
}

/// Whether a read counts toward the store hit/miss metrics. Only the node's
/// tree reads do; the newness checks on the write path do not.
#[derive(Debug, Clone, Copy)]
enum Count {
    Recorded,
    Silent,
}

impl Count {
    fn hit(self, id: SedimentreeId) {
        if matches!(self, Self::Recorded) {
            tracing::trace!(tree = ?id, "minimized tree store hit");
            #[cfg(feature = "metrics")]
            crate::metrics::sedimentree_cache_hit();
        }
    }

    fn miss(self, id: SedimentreeId) {
        if matches!(self, Self::Recorded) {
            tracing::trace!(tree = ?id, "minimized tree store miss");
            #[cfg(feature = "metrics")]
            crate::metrics::sedimentree_cache_miss();
        }
    }
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
    storage: &StoragePowerbox<Store, Auth, Async>,
    id: SedimentreeId,
    digest: Digest<Blob>,
) -> Result<Option<Blob>, Store::Error> {
    let local_access = storage.local_fetcher(id);

    for verified in local_access.load_loose_commits().await? {
        if verified.payload().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    for verified in local_access.load_fragments().await? {
        if verified.payload().summary().blob_meta().digest() == digest {
            return Ok(Some(verified.blob().clone()));
        }
    }

    Ok(None)
}
