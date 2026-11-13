//! Where a node keeps the minimized form of each sedimentree it serves.
//!
//! # What this is for
//!
//! [`Storage`] holds a tree's source data: its loose commits and fragments.
//! Serving sync requires the tree's _minimized_ form, which takes a full
//! storage read and a minimization pass to build. A [`MinimizedTreeStore`]
//! keeps those built trees so that hot reads skip both.
//!
//! The stock [`memory::MemoryMinimizedTreeStore`] keeps them in this
//! process's memory. Implement the trait to keep them elsewhere: a shared
//! store in front of many nodes, a key-value service, or nothing at all
//! ([`noop::NoopMinimizedTreeStore`]).
//!
//! # The contract
//!
//! **Everything here is derived data.** Storage is the source of truth. An
//! implementation may drop any entry at any time, and the node rebuilds it
//! from storage on the next read. Never treat a store as durable, and never
//! write to one except through this trait.
//!
//! The node calls four operations:
//!
//! | Operation | When | What it must do |
//! |-----------|------|-----------------|
//! | [`get`](MinimizedTreeStore::get) | Every read | Check out the resident tree, or report a miss with the tree's current [generation] |
//! | [`install`](MinimizedTreeStore::install) | After a miss, once the tree is rebuilt from storage | Keep the tree, unless its generation moved since the miss |
//! | [`merge`](MinimizedTreeStore::merge) | After a successful write | Merge the new data into a resident tree, and change the generation |
//! | [`remove`](MinimizedTreeStore::remove) | After a failed write or a deletion | Drop the tree, and change the generation |
//!
//! [generation]: MinimizedTreeStore::Generation
//!
//! # Generations
//!
//! Between a reader's storage read and its `install`, another writer can
//! change the tree. Installing the reader's copy would then hide that write.
//! A generation lets the store detect this and reject the install:
//!
//! ```text
//! node A                                   store                 node B
//! get → Miss(g0) ──────────────────────────▶ (absent, g0)
//! read storage (old)
//!                                                                write a commit
//!                                            (absent, g1) ◀───── merge on a miss: g0 → g1
//! install(old, seen: g0) ──────────────────▶ Stale (g1 ≠ g0) → re-read, retry
//! ```
//!
//! Within one process, the node's own coordination already orders these
//! steps, so a store that only one node uses can set
//! `type Generation = ()` and always report [`Install::Current`]. Both stock
//! stores do.
//!
//! # Sharing a store between nodes
//!
//! > [!WARNING]
//! > A store that several nodes write to concurrently must enforce the
//! > rules below itself. If it does not, it **loses updates and hides
//! > commits** that are in storage, and nothing in the node can detect it.
//!
//! 1. **`install` is a compare-and-set.** Checking `seen` against the
//!    current generation and writing the tree must happen as one atomic
//!    step.
//! 2. **`merge` and `remove` change the generation even on a miss.** Leave
//!    a marker with no tree, so an in-flight reader's install collides with
//!    it.
//! 3. **A generation never repeats for a tree.** If the store forgets a
//!    marker, the tree's generation must not return to a value an in-flight
//!    reader may hold. Random 128-bit tokens with a store-wide epoch bumped
//!    on marker eviction work, as do markers that outlive any hydration.
//! 4. **`merge` is atomic against other `merge`s.** Merging is a set union,
//!    so a retry loop is trivially correct.
//!
//! These rules need an atomic conditional write. Durable Objects provide one
//! for free, since each is single-threaded. Redis provides one through a Lua
//! script or `WATCH`/`MULTI`. **Cloudflare KV does not**: it has no
//! conditional writes and is eventually consistent, so it is unsafe as a
//! writable shared store.
//!
//! A per-node in-memory store over storage that _other_ nodes write to never
//! sees their writes, so it goes stale. Several nodes sharing storage must
//! also share a store, or accept that staleness.
//!
//! # Layering
//!
//! A fast per-node store in front of a shared one composes as a single
//! implementation of this trait, using the shared store's generation. Its
//! local copies go stale when other nodes write, so it must refresh them
//! by one of:
//!
//! - checking the shared generation on each hit (one small round trip,
//!   never stale);
//! - invalidating on a push from the shared store (stale for the push
//!   delay);
//! - expiring entries after a time-to-live (stale for up to the TTL).
//!
//! # Errors
//!
//! A store error never fails a node operation; the store is only an
//! optimization. A failed `get` is a miss, and the node reads storage
//! without installing. A failed `install` is ignored. A failed `merge` or
//! `remove` could leave a stale tree behind, so the node stops using the
//! store for that tree until a later `remove` succeeds.
//!
//! [`Storage`]: crate::storage::traits::Storage

pub mod memory;
pub mod noop;

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
    /// Only compared for equality, never interpreted by the node. A store
    /// used by one node can make this `()`.
    type Generation: Clone + 'static;

    /// A checked-out resident tree, owned rather than borrowed from the store.
    ///
    /// The node may minimize the tree in place through this handle. That
    /// changes its form, never its content, so writing the result back is
    /// optional. The node never holds an entry across an `.await`.
    type Entry: DerefMut<Target = MinimizedSedimentree> + 'static;

    /// Check out the tree for `id`, or report a miss with its current
    /// generation for a later [`install`](Self::install).
    fn get(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<Self::Entry, Self::Generation>, Self::Error>>;

    /// Keep a tree rebuilt from storage after a miss.
    ///
    /// Returns [`Install::Stale`] if the tree's generation moved since
    /// `seen`, so the node re-reads storage. If a tree is already resident
    /// and the generation still matches, keep the resident one and return
    /// [`Install::Current`].
    fn install(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: Self::Generation,
    ) -> Async::Future<'_, Result<Install, Self::Error>>;

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

/// The result of [`MinimizedTreeStore::get`].
#[derive(Debug)]
pub enum Lookup<Entry, Generation> {
    /// The tree is resident.
    Hit(Entry),

    /// The tree is not resident; install a rebuilt copy against this
    /// generation.
    Miss(Generation),
}

/// The result of [`MinimizedTreeStore::install`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Install {
    /// The generation still matched; the rebuilt tree is current.
    Current,

    /// The generation moved since the miss; re-read storage and retry.
    Stale,
}
