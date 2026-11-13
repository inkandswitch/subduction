//! Generic conformance checks for [`MinimizedTreeStore`] implementations.
//!
//! The store [contract](super#the-contract) describes what a store does over
//! time and across nodes, which the type system cannot check. A store that
//! breaks it compiles and mostly works, then hides commits that are in
//! storage. Call these checks from every implementation's tests:
//!
//! | Store | Checks |
//! |-------|--------|
//! | Any [`MinimizedTreeStore`] | The `assert_*` functions that take one handle |
//! | A [`SharedMinimizedTreeStore`] | Also the `assert_*` functions that take two handles, and [`check_ops`] |
//!
//! ```ignore
//! let store = MyStore::connect(url).await?;
//! conformance::assert_remove_drops_the_tree::<Sendable, _>(&store, fresh_id()).await;
//!
//! // Two handles to one backing store, as two nodes would hold them.
//! let (a, b) = (MyStore::connect(url).await?, MyStore::connect(url).await?);
//! conformance::assert_a_populate_after_a_remove_is_stale::<Sendable, _>(&a, &b, fresh_id()).await;
//!
//! // Generated operation sequences, checked against a reference model.
//! bolero::check!()
//!     .with_arbitrary::<Vec<conformance::Op>>()
//!     .for_each(|ops| runtime.block_on(conformance::check_ops::<Sendable, _>(&a, &b, fresh_ids(), ops)));
//! ```
//!
//! Each check needs tree ids the store has never seen. On a networked store,
//! generate fresh ids per run rather than reusing fixed ones.
//!
//! # What these checks cannot see
//!
//! - _Dropping is always allowed._ A store may drop any entry at any time, so
//!   a check that finds a tree absent passes. A store that keeps nothing,
//!   such as [`NoopMinimizedTreeStore`](super::noop::NoopMinimizedTreeStore),
//!   passes every check.
//! - _A spurious [`Populate::Stale`] is allowed._ It only costs a storage
//!   read, so the checks reject `Populated` where it is wrong, never `Stale`.
//! - _Interleavings are sequential._ Operations run one after another
//!   across the two handles. Races inside one operation need the
//!   store's own concurrency tests.
//! - _Reclamation is store-specific._ The checks cannot force a store to
//!   forget a marker, so test that the store's reclamation keeps generations
//!   from repeating separately.
//!
//! # Panics
//!
//! Every check panics when the store breaks the contract or returns an
//! error.

#![allow(clippy::panic)]

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec,
    vec::Vec,
};

use future_form::FutureForm;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

use super::{Lookup, MinimizedTreeStore, Populate, SharedMinimizedTreeStore};

/// Assert that a tree served after a populate holds what was populated.
///
/// # Panics
///
/// Panics if `id` is resident before the check, if a hit holds anything
/// else, or on a store error.
pub async fn assert_a_populated_tree_is_served_as_populated<Async, Store>(
    store: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    let seen = miss(store, id, "an untouched tree").await;
    populate(store, id, &[1, 2], seen).await;
    assert_resident_is(store, id, &[1, 2], "after a populate").await;
}

/// Assert that a merge reaches a resident tree.
///
/// # Panics
///
/// Panics if `id` is resident before the check, if a hit after the merge
/// lacks the merged commit, or on a store error.
pub async fn assert_a_merge_reaches_a_resident_tree<Async, Store>(store: &Store, id: SedimentreeId)
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    let seen = miss(store, id, "an untouched tree").await;
    populate(store, id, &[1], seen).await;
    merge(store, id, 2).await;
    assert_resident_is(store, id, &[1, 2], "after a merge into a resident tree").await;
}

/// Assert that a merge on a missing tree leaves it missing.
///
/// Storage holds the whole tree, and the update is only part of it. A store
/// that kept the update as the tree would hide everything else.
///
/// # Panics
///
/// Panics if `id` is resident before or after the merge, or on a store
/// error.
pub async fn assert_a_merge_on_a_miss_leaves_the_tree_absent<Async, Store>(
    store: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    drop(miss(store, id, "an untouched tree").await);
    merge(store, id, 1).await;
    drop(miss(store, id, "a merge on a missing tree").await);
}

/// Assert that `remove` drops a resident tree.
///
/// # Panics
///
/// Panics if `id` is resident before the check or after the remove, or on a
/// store error.
pub async fn assert_remove_drops_the_tree<Async, Store>(store: &Store, id: SedimentreeId)
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    let seen = miss(store, id, "an untouched tree").await;
    populate(store, id, &[1], seen).await;
    remove(store, id).await;
    drop(miss(store, id, "a removed tree").await);
}

/// Assert that a merge on a missing tree rejects a populate from an older
/// miss (rule 2).
///
/// `a` and `b` are two handles to one backing store.
///
/// # Panics
///
/// Panics if the populate is kept, if `id` is resident before the check, or
/// on a store error.
pub async fn assert_a_populate_after_a_merge_on_a_miss_is_stale<Async, Store>(
    a: &Store,
    b: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    let seen = miss(a, id, "an untouched tree").await;
    merge(b, id, 1).await;
    assert_stale(a, id, &[], seen, "a merge on the missing tree").await;
    drop(miss(a, id, "a tree whose only populate was stale").await);
}

/// Assert that a remove rejects a populate from an older miss (rule 2).
///
/// `a` and `b` are two handles to one backing store.
///
/// # Panics
///
/// Panics if the populate is kept, if `id` is resident before the check, or
/// on a store error.
pub async fn assert_a_populate_after_a_remove_is_stale<Async, Store>(
    a: &Store,
    b: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    let seen = miss(a, id, "an untouched tree").await;
    remove(b, id).await;
    assert_stale(a, id, &[1], seen, "a remove").await;
}

/// Assert that a merge into a tree another node populated rejects a populate
/// from a miss before it (rule 1). Keeping that populate would lose the merge.
///
/// `a` and `b` are two handles to one backing store.
///
/// # Panics
///
/// Panics if the older populate is kept, if a hit lacks the merge, if `id`
/// is resident before the check, or on a store error.
pub async fn assert_a_populate_after_a_merge_into_a_resident_tree_is_stale<Async, Store>(
    a: &Store,
    b: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    let a_seen = miss(a, id, "an untouched tree").await;
    let b_seen = miss(b, id, "an untouched tree").await;
    populate(b, id, &[1], b_seen).await;
    merge(b, id, 2).await;
    assert_stale(a, id, &[1], a_seen, "another node's merge").await;
    assert_resident_is(a, id, &[1, 2], "after a rejected populate").await;
}

/// Assert that a tree's generation does not come back after it is removed
/// and populated again (rule 3).
///
/// `a` and `b` are two handles to one backing store.
///
/// # Panics
///
/// Panics if a populate from before the churn is kept, if `id` is resident
/// before the check, or on a store error.
pub async fn assert_generations_do_not_repeat<Async, Store>(a: &Store, b: &Store, id: SedimentreeId)
where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    const ROUNDS: u16 = 8;
    let seen = miss(a, id, "an untouched tree").await;
    for n in 0..ROUNDS {
        let b_seen = miss(b, id, "a removed tree").await;
        populate(b, id, &[n], b_seen).await;
        remove(b, id).await;
    }
    assert_stale(a, id, &[], seen, "repeated populates and removes").await;
}

/// Assert that a merge through one handle is visible through another.
///
/// `a` and `b` are two handles to one backing store.
///
/// # Panics
///
/// Panics if a hit through either handle lacks the merge, if `id` is
/// resident before the check, or on a store error.
pub async fn assert_a_merge_is_visible_to_every_handle<Async, Store>(
    a: &Store,
    b: &Store,
    id: SedimentreeId,
) where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    let seen = miss(a, id, "an untouched tree").await;
    populate(a, id, &[1], seen).await;
    merge(b, id, 2).await;
    assert_resident_is(a, id, &[1, 2], "through the populating handle").await;
    assert_resident_is(b, id, &[1, 2], "through the merging handle").await;
}

/// The number of trees [`check_ops`] works on.
pub const TREES: usize = 3;

/// One store operation in a sequence for [`check_ops`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Op {
    /// Look a tree up; a miss is remembered for a later populate.
    Get(Handle, Tree),

    /// Populate a tree from the handle's last miss, with the commits storage
    /// held at that miss. Skipped if the handle has no unused miss.
    Populate(Handle, Tree),

    /// Write one new commit to the tree, then merge it into the store.
    Merge(Handle, Tree),

    /// Remove the tree.
    Remove(Handle, Tree),
}

/// Which of the two handles an [`Op`] goes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Handle {
    /// The first handle.
    A,

    /// The second handle.
    B,
}

/// Which of the [`TREES`] trees an [`Op`] acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Tree {
    /// The first tree.
    First,

    /// The second tree.
    Second,

    /// The third tree.
    Third,
}

/// Run `ops` against a shared store and check each result against a model of
/// storage.
///
/// The model holds what storage contains for each tree and counts writes.
/// After every operation it checks:
///
/// - a hit holds exactly what storage holds, so a hit is never stale;
/// - a tree that was removed, or never populated, is not a hit;
/// - a populate is kept only if no write happened since its miss.
///
/// `a` and `b` are two handles to one backing store, and `ids` are trees it
/// has never seen.
///
/// # Panics
///
/// Panics on the first operation that breaks the contract, or on a store
/// error.
pub async fn check_ops<Async, Store>(a: &Store, b: &Store, ids: [SedimentreeId; TREES], ops: &[Op])
where
    Async: FutureForm,
    Store: SharedMinimizedTreeStore<Async>,
{
    let mut trees: BTreeMap<Tree, TreeModel> = BTreeMap::new();
    let mut misses: BTreeMap<(Handle, Tree), PendingMiss<Store::Generation>> = BTreeMap::new();

    for (step, &op) in ops.iter().enumerate() {
        let (handle, tree) = match op {
            Op::Get(handle, tree)
            | Op::Populate(handle, tree)
            | Op::Merge(handle, tree)
            | Op::Remove(handle, tree) => (handle, tree),
        };
        let store = match handle {
            Handle::A => a,
            Handle::B => b,
        };
        let id = tree_id(ids, tree);
        let model = trees.entry(tree).or_default();

        match op {
            Op::Get(..) => match get(store, id).await {
                Lookup::Hit(entry) => {
                    assert!(
                        model.may_be_resident,
                        "step {step} ({op:?}): a hit on a tree that was removed or never populated"
                    );
                    assert_eq!(
                        commits_in(&entry),
                        commit_ids(&model.storage),
                        "step {step} ({op:?}): a hit differs from storage"
                    );
                }
                Lookup::Miss(generation) => {
                    misses.insert(
                        (handle, tree),
                        PendingMiss {
                            generation,
                            writes: model.writes,
                            storage: model.storage.clone(),
                        },
                    );
                }
            },

            Op::Populate(..) => {
                let Some(pending) = misses.remove(&(handle, tree)) else {
                    continue;
                };
                let rebuilt: Vec<u16> = pending.storage.iter().copied().collect();
                if populate(store, id, &rebuilt, pending.generation).await == Populate::Populated {
                    assert_eq!(
                        model.writes, pending.writes,
                        "step {step} ({op:?}): a populate was kept after a write since its miss"
                    );
                    model.may_be_resident = true;
                }
            }

            Op::Merge(..) => {
                let commit = model.next_commit;
                model.next_commit = model.next_commit.wrapping_add(1);
                model.storage.insert(commit);
                model.writes += 1;
                merge(store, id, commit).await;
            }

            Op::Remove(..) => {
                model.writes += 1;
                model.may_be_resident = false;
                remove(store, id).await;
            }
        }
    }
}

/// What [`check_ops`] knows about one tree.
#[derive(Debug, Default)]
struct TreeModel {
    /// The commits storage holds.
    storage: BTreeSet<u16>,

    /// Writes so far; a populate is current only if none happened since its
    /// miss.
    writes: u64,

    /// False after a remove, until a populate is kept.
    may_be_resident: bool,

    next_commit: u16,
}

/// A miss a handle may later populate from.
#[derive(Debug)]
struct PendingMiss<Generation> {
    generation: Generation,
    writes: u64,
    storage: BTreeSet<u16>,
}

const fn tree_id(ids: [SedimentreeId; TREES], tree: Tree) -> SedimentreeId {
    let [first, second, third] = ids;
    match tree {
        Tree::First => first,
        Tree::Second => second,
        Tree::Third => third,
    }
}

const fn commit_id(n: u16) -> CommitId {
    let mut bytes = [0xC0; 32];
    let [low, high] = n.to_le_bytes();
    bytes[0] = low;
    bytes[1] = high;
    CommitId::new(bytes)
}

fn commit_ids(commits: &BTreeSet<u16>) -> BTreeSet<CommitId> {
    commits.iter().copied().map(commit_id).collect()
}

fn sedimentree(id: SedimentreeId, commits: &[u16]) -> Sedimentree {
    let commits = commits
        .iter()
        .map(|&n| {
            let blob = Blob::new(n.to_le_bytes().to_vec());
            LooseCommit::new(id, commit_id(n), BTreeSet::new(), BlobMeta::new(&blob))
                .unwrap_or_else(|error| panic!("a commit with no parents is valid: {error}"))
        })
        .collect();
    Sedimentree::new(vec![], commits)
}

fn commits_in(tree: &MinimizedSedimentree) -> BTreeSet<CommitId> {
    tree.tree().loose_commits().map(LooseCommit::head).collect()
}

async fn get<Async, Store>(
    store: &Store,
    id: SedimentreeId,
) -> Lookup<Store::Entry, Store::Generation>
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    store
        .get(id)
        .await
        .unwrap_or_else(|error| panic!("get failed: {error}"))
}

async fn miss<Async, Store>(store: &Store, id: SedimentreeId, context: &str) -> Store::Generation
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    match get(store, id).await {
        Lookup::Miss(generation) => generation,
        Lookup::Hit(_) => panic!("{context} must miss"),
    }
}

async fn populate<Async, Store>(
    store: &Store,
    id: SedimentreeId,
    commits: &[u16],
    seen: Store::Generation,
) -> Populate
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    store
        .populate(
            id,
            MinimizedSedimentree::new(sedimentree(id, commits)),
            seen,
        )
        .await
        .unwrap_or_else(|error| panic!("populate failed: {error}"))
}

async fn merge<Async, Store>(store: &Store, id: SedimentreeId, commit: u16)
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    store
        .merge(id, sedimentree(id, &[commit]))
        .await
        .unwrap_or_else(|error| panic!("merge failed: {error}"));
}

async fn remove<Async, Store>(store: &Store, id: SedimentreeId)
where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    store
        .remove(id)
        .await
        .unwrap_or_else(|error| panic!("remove failed: {error}"));
}

async fn assert_stale<Async, Store>(
    store: &Store,
    id: SedimentreeId,
    commits: &[u16],
    seen: Store::Generation,
    since: &str,
) where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    assert_eq!(
        populate(store, id, commits, seen).await,
        Populate::Stale,
        "a populate from a miss before {since} must be stale"
    );
}

/// Passes if the tree is absent: a store may drop any entry at any time.
async fn assert_resident_is<Async, Store>(
    store: &Store,
    id: SedimentreeId,
    commits: &[u16],
    context: &str,
) where
    Async: FutureForm,
    Store: MinimizedTreeStore<Async>,
{
    if let Lookup::Hit(entry) = get(store, id).await {
        let expected: BTreeSet<u16> = commits.iter().copied().collect();
        assert_eq!(
            commits_in(&entry),
            commit_ids(&expected),
            "{context}, a hit must hold exactly the stored commits"
        );
    }
}
