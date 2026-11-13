//! A bounded store that several nodes in one process can share.
//!
//! # Why this exists
//!
//! [`MemoryMinimizedTreeStore`](super::memory::MemoryMinimizedTreeStore)
//! relies on its single node to order populates against writes, so it cannot
//! be shared. This store follows every rule a shared store must, so that:
//!
//! - nodes sharing storage can share it and stay coherent, which a per-node
//!   store cannot;
//! - implementers of a networked store have a complete template to copy,
//!   including safe reclamation;
//! - tests can run several nodes against one store.
//!
//! # How it meets the contract
//!
//! Each tree has a _slot_ holding its [`Residency`] and its generation, behind
//! its own lock. A slot whose tree is not resident is a _marker_: it records
//! that the tree changed, so a populate based on an older read is rejected.
//! The [module rules](super#sharing-a-store-between-nodes) map onto it as
//! follows:
//!
//! | Rule | Here |
//! |------|------|
//! | `populate` is a compare-and-set | It compares `seen` and writes the tree while holding the slot lock |
//! | `merge` and `remove` change the generation even on a miss | They create a marker if no slot exists, then bump its generation |
//! | Generations never repeat | Every generation comes from one store-wide counter |
//! | Operations are serialized per tree | Every operation, including reads through [`SharedEntry`], holds the slot lock |
//!
//! # Reclaiming slots
//!
//! A tree with no slot reports the store's _absent generation_. Deleting a
//! slot naively would be unsafe: the tree would read as absent again, and a
//! reader holding the absent generation from before the slot existed could
//! populate over the write that created it.
//!
//! So [`with_capacity`](SharedMemoryMinimizedTreeStore::with_capacity) bounds
//! the number of slots, resident or marker. When a new slot pushes the count
//! over the bound, a sweep deletes the least-recently-used slots down to a
//! low-water mark, then replaces the absent generation with a fresh one. Every
//! reader that read the old absent generation now gets [`Populate::Stale`] and
//! re-reads storage. One bump covers the whole sweep, so the cost to in-flight
//! readers is paid once per sweep, not once per slot.
//!
//! The sweep only deletes slots it can lock without waiting, and marks them
//! retired while holding both the slot and the slot table. An operation that
//! was already waiting for a retired slot sees the mark and starts over from
//! the table, so no write can land in a deleted slot.
//!
//! In-place minimization through [`SharedEntry`] is safe here only because the
//! entry holds the slot lock. A networked store's entry is an owned copy, and
//! must not be written back.
//!
//! The [design notes] explain the reclamation scheme and how to carry this
//! layout onto networked stores.
//!
//! [design notes]: https://github.com/inkandswitch/subduction/blob/main/design/minimized_tree_store.md

use alloc::{sync::Arc, vec::Vec};
use core::{
    convert::Infallible,
    fmt,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, Ordering},
};

use async_lock::{Mutex as AsyncMutex, MutexGuardArc};
use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    collections::Map,
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use spin::Mutex as SpinMutex;

use super::{Lookup, MinimizedTreeStore, Populate, SharedMinimizedTreeStore};

/// Whether a slot holds its tree.
#[derive(Debug)]
pub enum Residency {
    /// The tree changed at this slot's generation but is not resident; the
    /// next read rebuilds it from storage.
    Marker,

    /// The tree is resident.
    Resident(MinimizedSedimentree),
}

/// One tree's residency and generation, guarded together.
#[derive(Debug)]
struct Slot {
    generation: u64,
    residency: Residency,
    /// Set by a sweep that deleted this slot from the table. An operation that
    /// finds it set must start over from the table.
    retired: bool,
    last_used: u64,
}

type SharedSlot = Arc<AsyncMutex<Slot>>;

#[derive(Debug)]
struct Inner {
    slots: SpinMutex<Table>,
    /// The last generation issued. Starts at `0`, the initial absent generation.
    issued: AtomicU64,
    clock: AtomicU64,
    capacity: Option<usize>,
}

/// The slot table and the generation reported for trees with no slot.
/// Changed together, under one lock.
#[derive(Debug, Default)]
struct Table {
    slots: Map<SedimentreeId, SharedSlot>,
    absent: u64,
}

/// A bounded store several nodes in one process can share; clones share the
/// same trees. See the [module documentation](self) for how it meets the
/// shared store rules and reclaims memory.
#[derive(Debug, Clone)]
pub struct SharedMemoryMinimizedTreeStore {
    inner: Arc<Inner>,
}

impl SharedMemoryMinimizedTreeStore {
    /// An unbounded store: slots are never reclaimed.
    #[must_use]
    pub fn new() -> Self {
        Self::build(None)
    }

    /// A store holding at most `max_slots` slots, resident trees and markers
    /// together. Exceeding it sweeps the least-recently-used slots down to
    /// about 90% of the bound.
    ///
    /// # Panics
    ///
    /// Panics if `max_slots` is zero.
    #[must_use]
    pub fn with_capacity(max_slots: usize) -> Self {
        assert!(max_slots > 0, "a store needs room for at least one slot");
        Self::build(Some(max_slots))
    }

    fn build(capacity: Option<usize>) -> Self {
        Self {
            inner: Arc::new(Inner {
                slots: SpinMutex::new(Table::default()),
                issued: AtomicU64::new(0),
                clock: AtomicU64::new(0),
                capacity,
            }),
        }
    }

    /// A copy of the resident tree for `id`, if any.
    pub async fn get_cloned(&self, id: SedimentreeId) -> Option<MinimizedSedimentree> {
        let slot = self.lock_existing(id).await?;
        match &slot.residency {
            Residency::Resident(tree) => Some(tree.clone()),
            Residency::Marker => None,
        }
    }

    /// The number of slots, resident trees and markers together.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.inner.slots.lock().slots.len()
    }

    fn next_generation(&self) -> u64 {
        self.inner.issued.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn now(&self) -> u64 {
        self.inner.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Lock the live slot for `id`, if one exists.
    async fn lock_existing(&self, id: SedimentreeId) -> Option<MutexGuardArc<Slot>> {
        loop {
            let slot = self.inner.slots.lock().slots.get(&id).cloned()?;
            let guard = slot.lock_arc().await;
            if !guard.retired {
                return Some(guard);
            }
        }
    }

    /// Lock the live slot for `id`, creating a marker if none exists.
    async fn lock_or_create(&self, id: SedimentreeId) -> MutexGuardArc<Slot> {
        loop {
            let slot = {
                let mut table = self.inner.slots.lock();
                if let Some(slot) = table.slots.get(&id) {
                    slot.clone()
                } else {
                    let slot = self.new_slot(table.absent, Residency::Marker);
                    table.slots.insert(id, slot.clone());
                    self.sweep_if_full(&mut table, id);
                    slot
                }
            };
            let guard = slot.lock_arc().await;
            if !guard.retired {
                return guard;
            }
        }
    }

    fn new_slot(&self, generation: u64, residency: Residency) -> SharedSlot {
        Arc::new(AsyncMutex::new(Slot {
            generation,
            residency,
            retired: false,
            last_used: self.now(),
        }))
    }

    /// Delete least-recently-used slots once the table exceeds its bound,
    /// sparing `keep`, then retire the old absent generation. Runs under the
    /// table lock and never waits for a slot.
    fn sweep_if_full(&self, table: &mut Table, keep: SedimentreeId) {
        let Some(capacity) = self.inner.capacity else {
            return;
        };
        if table.slots.len() <= capacity {
            return;
        }
        let low_water = capacity - capacity / 10;
        let mut candidates: Vec<(u64, SedimentreeId)> = table
            .slots
            .iter()
            .filter(|(id, _)| **id != keep)
            .filter_map(|(id, slot)| slot.try_lock().map(|slot| (slot.last_used, *id)))
            .collect();
        candidates.sort_unstable();
        let mut deleted = false;
        for (_, id) in candidates {
            if table.slots.len() <= low_water {
                break;
            }
            let Some(slot) = table.slots.get(&id).cloned() else {
                continue;
            };
            // Busy since the scan: skip it rather than wait under the table lock.
            let Some(mut guard) = slot.try_lock() else {
                continue;
            };
            guard.retired = true;
            drop(guard);
            table.slots.remove(&id);
            deleted = true;
        }
        if deleted {
            table.absent = self.next_generation();
        }
    }
}

impl Default for SharedMemoryMinimizedTreeStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A resident tree checked out of a [`SharedMemoryMinimizedTreeStore`],
/// holding its slot lock until dropped.
pub struct SharedEntry {
    slot: MutexGuardArc<Slot>,
}

impl Deref for SharedEntry {
    type Target = MinimizedSedimentree;

    #[allow(
        clippy::panic,
        reason = "an entry is only handed out for a resident tree, and the slot stays locked"
    )]
    fn deref(&self) -> &MinimizedSedimentree {
        match &self.slot.residency {
            Residency::Resident(tree) => tree,
            Residency::Marker => panic!("checked-out tree is resident"),
        }
    }
}

impl DerefMut for SharedEntry {
    #[allow(
        clippy::panic,
        reason = "an entry is only handed out for a resident tree, and the slot stays locked"
    )]
    fn deref_mut(&mut self) -> &mut MinimizedSedimentree {
        match &mut self.slot.residency {
            Residency::Resident(tree) => tree,
            Residency::Marker => panic!("checked-out tree is resident"),
        }
    }
}

impl fmt::Debug for SharedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedEntry").finish_non_exhaustive()
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> MinimizedTreeStore<Async> for SharedMemoryMinimizedTreeStore {
    type Error = Infallible;
    type Generation = u64;
    type Entry = SharedEntry;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<SharedEntry, u64>, Infallible>> {
        Async::from_future(async move {
            loop {
                let slot = {
                    let table = self.inner.slots.lock();
                    match table.slots.get(&id) {
                        Some(slot) => slot.clone(),
                        None => return Ok(Lookup::Miss(table.absent)),
                    }
                };
                let mut slot = slot.lock_arc().await;
                if slot.retired {
                    continue;
                }
                slot.last_used = self.now();
                return Ok(match slot.residency {
                    Residency::Resident(_) => Lookup::Hit(SharedEntry { slot }),
                    Residency::Marker => Lookup::Miss(slot.generation),
                });
            }
        })
    }

    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: u64,
    ) -> Async::Future<'_, Result<Populate, Infallible>> {
        Async::from_future(async move {
            loop {
                let slot = {
                    let mut table = self.inner.slots.lock();
                    if let Some(slot) = table.slots.get(&id) {
                        slot.clone()
                    } else {
                        // Check and create under the table lock, so no sweep
                        // can retire the absent generation in between.
                        if seen != table.absent {
                            return Ok(Populate::Stale);
                        }
                        let slot = self.new_slot(seen, Residency::Resident(tree));
                        table.slots.insert(id, slot);
                        self.sweep_if_full(&mut table, id);
                        return Ok(Populate::Populated);
                    }
                };
                let mut slot = slot.lock_arc().await;
                if slot.retired {
                    continue;
                }
                if slot.generation != seen {
                    return Ok(Populate::Stale);
                }
                slot.last_used = self.now();
                // A tree already resident at this generation came from an
                // equally current read; keep it.
                if matches!(slot.residency, Residency::Marker) {
                    slot.residency = Residency::Resident(tree);
                }
                return Ok(Populate::Populated);
            }
        })
    }

    fn merge(
        &self,
        id: SedimentreeId,
        update: Sedimentree,
    ) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async move {
            let mut slot = self.lock_or_create(id).await;
            slot.generation = self.next_generation();
            slot.last_used = self.now();
            if let Residency::Resident(tree) = &mut slot.residency {
                tree.merge(update);
            }
            Ok(())
        })
    }

    fn remove(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Infallible>> {
        Async::from_future(async move {
            let mut slot = self.lock_or_create(id).await;
            slot.generation = self.next_generation();
            slot.last_used = self.now();
            slot.residency = Residency::Marker;
            Ok(())
        })
    }
}
#[future_form(Sendable, Local)]
impl<Async: FutureForm> SharedMinimizedTreeStore<Async> for SharedMemoryMinimizedTreeStore {}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    //! Store-specific behaviour: reclamation, capacity and the absent
    //! generation. The shared-store rules themselves are checked by the
    //! conformance suite in `tests/minimized_tree_store_conformance.rs`.

    use super::*;

    const ID: SedimentreeId = SedimentreeId::new([0x51; 32]);

    type Store = SharedMemoryMinimizedTreeStore;

    async fn miss(store: &Store) -> u64 {
        match MinimizedTreeStore::<Sendable>::get(store, ID).await {
            Ok(Lookup::Miss(generation)) => generation,
            Ok(Lookup::Hit(_)) => panic!("expected a miss"),
            Err(never) => match never {},
        }
    }

    async fn populate(store: &Store, seen: u64) -> Populate {
        MinimizedTreeStore::<Sendable>::populate(store, ID, MinimizedSedimentree::default(), seen)
            .await
            .unwrap_or_else(|never| match never {})
    }

    async fn merge(store: &Store) {
        MinimizedTreeStore::<Sendable>::merge(store, ID, Sedimentree::default())
            .await
            .unwrap_or_else(|never| match never {});
    }

    async fn remove(store: &Store) {
        MinimizedTreeStore::<Sendable>::remove(store, ID)
            .await
            .unwrap_or_else(|never| match never {});
    }

    fn tree_id(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    async fn merge_at(store: &Store, id: SedimentreeId) {
        MinimizedTreeStore::<Sendable>::merge(store, id, Sedimentree::default())
            .await
            .unwrap_or_else(|never| match never {});
    }

    #[tokio::test]
    async fn a_reader_from_before_a_reclaimed_slot_existed_cannot_populate() {
        let store = Store::with_capacity(10);
        let seen = miss(&store).await;
        // A write creates a marker for ID, then other trees push it out.
        merge(&store).await;
        for n in 100..120 {
            merge_at(&store, tree_id(n)).await;
        }
        assert!(store.get_cloned(ID).await.is_none());
        assert_eq!(
            populate(&store, seen).await,
            Populate::Stale,
            "ID's slot was reclaimed, but the write it recorded must still reject the old read"
        );
        // A fresh read populates normally.
        let current = miss(&store).await;
        assert_eq!(populate(&store, current).await, Populate::Populated);
    }

    #[tokio::test]
    async fn the_slot_count_stays_within_capacity() {
        let store = Store::with_capacity(10);
        for n in 0..200 {
            merge_at(&store, tree_id(n)).await;
            assert!(store.slot_count() <= 10, "{} slots", store.slot_count());
        }
    }

    #[tokio::test]
    async fn sweeps_reclaim_the_least_recently_used_slots() {
        let store = Store::with_capacity(10);
        let seen = miss(&store).await;
        assert_eq!(populate(&store, seen).await, Populate::Populated);
        for n in 100..109 {
            merge_at(&store, tree_id(n)).await;
            // Keep ID recently used while the store fills.
            assert!(matches!(
                MinimizedTreeStore::<Sendable>::get(&store, ID).await,
                Ok(Lookup::Hit(_))
            ));
        }
        merge_at(&store, tree_id(200)).await;
        assert!(
            store.get_cloned(ID).await.is_some(),
            "recently used tree survives the sweep"
        );
    }

    #[tokio::test]
    async fn a_sweep_skips_a_slot_that_is_in_use() {
        let store = Store::with_capacity(2);
        let seen = miss(&store).await;
        assert_eq!(populate(&store, seen).await, Populate::Populated);
        let Ok(Lookup::Hit(entry)) = MinimizedTreeStore::<Sendable>::get(&store, ID).await else {
            panic!("resident");
        };
        // ID is the least recently used, but its entry holds the slot lock.
        merge_at(&store, tree_id(1)).await;
        merge_at(&store, tree_id(2)).await;
        drop(entry);
        assert!(
            store.get_cloned(ID).await.is_some(),
            "a checked-out slot is never deleted"
        );
    }

    #[tokio::test]
    async fn a_merge_on_a_marker_rejects_an_older_populate() {
        let store = Store::new();
        remove(&store).await;
        let seen = miss(&store).await;
        merge(&store).await;
        assert_eq!(populate(&store, seen).await, Populate::Stale);
    }

    #[tokio::test]
    async fn a_current_populate_is_kept_and_a_second_keeps_the_first() {
        let store = Store::new();
        let seen = miss(&store).await;
        assert_eq!(populate(&store, seen).await, Populate::Populated);
        assert!(store.get_cloned(ID).await.is_some());
        assert_eq!(populate(&store, seen).await, Populate::Populated);
    }

    #[tokio::test]
    async fn generations_never_repeat_across_trees() {
        let store = Store::new();
        let other = SedimentreeId::new([0x52; 32]);
        merge(&store).await;
        MinimizedTreeStore::<Sendable>::merge(&store, other, Sedimentree::default())
            .await
            .unwrap_or_else(|never| match never {});
        let first = miss(&store).await;
        let Ok(Lookup::Miss(second)) = MinimizedTreeStore::<Sendable>::get(&store, other).await
        else {
            panic!("expected a miss");
        };
        assert_ne!(first, second);
        assert_ne!(first, 0);
    }
}
