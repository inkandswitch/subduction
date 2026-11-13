//! Where a node keeps the minimized form of each sedimentree it serves.
//!
//! # What this is for
//!
//! [`Storage`] holds a tree's source data: its loose commits and fragments.
//! Serving sync requires the tree's _minimized_ form, which takes a full
//! storage read and a minimization pass to build. A [`MinimizedTreeStore`]
//! keeps those built trees so that hot reads skip both.
//!
//! Three stores ship with the crate:
//!
//! - [`memory::MemoryMinimizedTreeStore`] keeps trees in this process's
//!   memory, for one node. It is the default.
//! - [`shared::SharedMemoryMinimizedTreeStore`] keeps them in memory for
//!   several nodes in one process, following every rule for a shared store
//!   below. Start from it when writing a networked store.
//! - [`noop::NoopMinimizedTreeStore`] keeps nothing.
//!
//! Implement the trait to keep trees elsewhere: a shared store in front of
//! many nodes, or a key-value service.
//!
//! # The contract
//!
//! _Everything here is derived data._ Storage is the source of truth. An
//! implementation may drop any entry at any time, and the node rebuilds it
//! from storage on the next read. Never treat a store as durable, and never
//! write to one except through this trait.
//!
//! The node calls four operations:
//!
//! | Operation | When | What it must do |
//! |-----------|------|-----------------|
//! | [`get`](MinimizedTreeStore::get) | Every read | Check out the resident tree, or report a miss with the tree's current [generation] |
//! | [`populate`](MinimizedTreeStore::populate) | After a miss, once the tree is rebuilt from storage | Keep the tree, unless its generation moved since the miss |
//! | [`merge`](MinimizedTreeStore::merge) | After a successful write | Merge the new data into a resident tree, and change the generation |
//! | [`remove`](MinimizedTreeStore::remove) | After a failed write or a deletion, and to clear a bypassed tree | Drop the tree, and change the generation |
//!
//! [generation]: MinimizedTreeStore::Generation
//!
//! # Generations
//!
//! Between a reader's storage read and its `populate`, another writer can
//! change the tree. Populating the store with the reader's copy would then
//! hide that write. A generation lets the store detect this and reject the
//! populate:
//!
//! ```text
//! node A                                   store                 node B
//! get → Miss(g0) ──────────────────────────▶ (absent, g0)
//! read storage (old)
//!                                                                write a commit
//!                                            (absent, g1) ◀───── merge on a miss: g0 → g1
//! populate(old, seen: g0) ─────────────────▶ Stale (g1 ≠ g0): not kept
//! ```
//!
//! Within one process, the node's own coordination already orders these
//! steps, so a store that only one node uses can set
//! `type Generation = ()` and always report [`Populate::Populated`].
//! [`memory::MemoryMinimizedTreeStore`] and [`noop::NoopMinimizedTreeStore`]
//! do.
//!
//! # Sharing a store between nodes
//!
//! > [!WARNING]
//! > A store that several nodes write to concurrently must enforce the
//! > rules below itself. If it does not, it _loses updates and hides
//! > commits_ that are in storage, and nothing in the node can detect it.
//!
//! A store that meets these rules says so by implementing
//! [`SharedMinimizedTreeStore`].
//!
//! 1. **`populate` is a compare-and-set.** Checking `seen` against the
//!    current generation and writing the tree must happen as one atomic
//!    step.
//! 2. **`merge` and `remove` change the generation even on a miss.** Leave
//!    a marker with no tree, so an in-flight reader's populate collides with
//!    it.
//! 3. **A generation never repeats for a tree.** If the store forgets a
//!    marker, the tree's generation must not return to a value an in-flight
//!    reader may hold.
//! 4. **All four operations are serialized per tree.** A read-modify-write
//!    `merge` racing a `remove` can otherwise bring back a tree that storage
//!    no longer matches.
//!
//! An entry must never be written back: a `Hit` carries no generation, so
//! a write-back can overwrite another node's merge. Store calls must time
//! out, since the node holds a per-tree lock across store maintenance.
//!
//! [`shared::SharedMemoryMinimizedTreeStore`] meets all of these, including
//! safe reclamation of markers. The [design notes] cover the full rule set,
//! how to reclaim markers without reopening the race, mapping onto networked
//! stores, and layering a fast local store in front of a shared one.
//!
//! A per-node store over storage that _other_ nodes write to never sees
//! their writes, so it goes stale. Nodes sharing storage must share a store.
//!
//! [design notes]: https://github.com/inkandswitch/subduction/blob/main/design/minimized_tree_store.md
//!
//! # Errors
//!
//! A store error never fails a node operation; the store is only an
//! optimization. A failed `get` is a miss, and the node reads storage
//! without populating. A failed `populate` is ignored. A failed `merge` or
//! `remove` could leave a stale tree behind, so the node stops using the
//! store for that tree until a later `remove` succeeds.
//!
//! [`Storage`]: crate::storage::traits::Storage

pub mod memory;
pub mod noop;
pub mod shared;

use core::{error::Error, ops::DerefMut};

use future_form::FutureForm;
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};

/// Keeps the minimized form of each tree a node serves.
///
/// See the [module documentation](self) for the contract every
/// implementation must uphold, especially when several nodes share one.
#[allow(clippy::type_complexity)]
pub trait MinimizedTreeStore<Async: FutureForm>: Clone {
    /// The store's error type.
    type Error: Error + 'static;

    /// A per-tree token that changes whenever the tree's stored state does.
    ///
    /// The store compares it; the node only carries it from a miss to the
    /// matching [`populate`](Self::populate). A store used by one node can
    /// make this `()`.
    ///
    /// Not `Clone`, so the node can use each generation only once: it cannot
    /// keep one from an old miss and populate against it later.
    type Generation: 'static;

    /// A checked-out resident tree, owned rather than borrowed from the store.
    ///
    /// The node may minimize the tree in place through this handle. That
    /// changes its form, never its content. A store that shares trees
    /// between nodes must not write the result back: a [`Lookup::Hit`]
    /// carries no generation, so a write-back can overwrite another node's
    /// merge. The node never holds an entry across an `.await`.
    type Entry: DerefMut<Target = MinimizedSedimentree> + 'static;

    /// Check out the tree for `id`, or report a miss with its current
    /// generation for a later [`populate`](Self::populate).
    fn get(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<Self::Entry, Self::Generation>, Self::Error>>;

    /// Keep a tree rebuilt from storage after a miss.
    ///
    /// Returns [`Populate::Stale`] if the tree's generation moved since
    /// `seen`. The node then serves its read without keeping it; the next
    /// read rebuilds the tree. If a tree is already resident
    /// and the generation still matches, keep the resident one and return
    /// [`Populate::Populated`].
    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: Self::Generation,
    ) -> Async::Future<'_, Result<Populate, Self::Error>>;

    /// Merge newly persisted data into a resident tree.
    ///
    /// A miss leaves the tree absent, since the next read rebuilds it from
    /// storage, which already holds the data. Either way, change the
    /// generation.
    fn merge(
        &self,
        id: SedimentreeId,
        update: Sedimentree,
    ) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Drop the tree for `id`, if resident, and change its generation.
    fn remove(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Self::Error>>;
}

/// A [`MinimizedTreeStore`] that several nodes can share.
///
/// Implementing this promises that the store meets every rule in [Sharing a
/// store between nodes](self#sharing-a-store-between-nodes). The compiler
/// cannot check that promise.
pub trait SharedMinimizedTreeStore<Async: FutureForm>: MinimizedTreeStore<Async> {}

/// The result of [`MinimizedTreeStore::get`].
#[derive(Debug)]
pub enum Lookup<Entry, Generation> {
    /// The tree is resident.
    Hit(Entry),

    /// The tree is not resident; populate a rebuilt copy against this
    /// generation.
    Miss(Generation),
}

/// The result of [`MinimizedTreeStore::populate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Populate {
    /// The generation still matched; the rebuilt tree is current.
    Populated,

    /// The generation moved since the miss; the tree was not kept.
    Stale,
}
