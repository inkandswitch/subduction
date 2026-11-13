//! The node's handling of a store that fails or reports newer generations.
//!
//! The stock stores never do either, so these paths need a test double: a
//! single-node store with real generations and switchable failures.

use super::*;
use crate::{
    collections::bounded_sharded_map::Checkout,
    minimized_tree_store::{Lookup, Populate},
    storage::state::cache::Resident,
};
use futures::future::BoxFuture;

use core::{
    convert::Infallible,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

#[derive(Debug, thiserror::Error)]
#[error("injected minimized tree store failure")]
struct Injected;

#[derive(Debug, Default)]
struct Switches {
    generation: AtomicU64,
    fail_get: AtomicBool,
    fail_populate: AtomicBool,
    fail_merge: AtomicBool,
    fail_remove: AtomicBool,
    gets: AtomicUsize,
    populates: AtomicUsize,
    removes: AtomicUsize,
}

/// A store with one generation counter for every tree, bumped by `merge` and
/// `remove` and compared by `populate`. Each check-then-write is not atomic,
/// so this is only a faithful shared store for sequential tests.
#[derive(Debug, Clone, Default)]
struct FlakyStore {
    inner: Cache,
    switches: Arc<Switches>,
}

impl FlakyStore {
    fn set(&self, switch: impl Fn(&Switches) -> &AtomicBool, on: bool) {
        switch(&self.switches).store(on, Ordering::SeqCst);
    }

    fn count(&self, counter: impl Fn(&Switches) -> &AtomicUsize) -> usize {
        counter(&self.switches).load(Ordering::SeqCst)
    }

    fn bump(&self) {
        self.switches.generation.fetch_add(1, Ordering::SeqCst);
    }

    async fn resident_commits(&self) -> Option<usize> {
        self.inner
            .get_cloned(ID)
            .await
            .map(|tree| tree.loose_commits().count())
    }
}

fn never<T>(never: Infallible) -> T {
    match never {}
}

impl MinimizedTreeStore<Sendable> for FlakyStore {
    type Error = Injected;
    type Generation = u64;
    type Entry = Checkout<SedimentreeId, MinimizedSedimentree>;

    fn get(&self, id: SedimentreeId) -> BoxFuture<'_, Result<Lookup<Self::Entry, u64>, Injected>> {
        Sendable::from_future(async move {
            self.switches.gets.fetch_add(1, Ordering::SeqCst);
            if self.switches.fail_get.load(Ordering::SeqCst) {
                return Err(Injected);
            }
            Ok(
                match MinimizedTreeStore::<Sendable>::get(&self.inner, id)
                    .await
                    .unwrap_or_else(never)
                {
                    Lookup::Hit(entry) => Lookup::Hit(entry),
                    Lookup::Miss(()) => {
                        Lookup::Miss(self.switches.generation.load(Ordering::SeqCst))
                    }
                },
            )
        })
    }

    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: u64,
    ) -> BoxFuture<'_, Result<Populate, Injected>> {
        Sendable::from_future(async move {
            self.switches.populates.fetch_add(1, Ordering::SeqCst);
            if self.switches.fail_populate.load(Ordering::SeqCst) {
                return Err(Injected);
            }
            if seen != self.switches.generation.load(Ordering::SeqCst) {
                return Ok(Populate::Stale);
            }
            MinimizedTreeStore::<Sendable>::populate(&self.inner, id, tree, ())
                .await
                .unwrap_or_else(never);
            Ok(Populate::Populated)
        })
    }

    fn merge(&self, id: SedimentreeId, update: Sedimentree) -> BoxFuture<'_, Result<(), Injected>> {
        Sendable::from_future(async move {
            if self.switches.fail_merge.load(Ordering::SeqCst) {
                return Err(Injected);
            }
            self.bump();
            MinimizedTreeStore::<Sendable>::merge(&self.inner, id, update)
                .await
                .unwrap_or_else(never);
            Ok(())
        })
    }

    fn remove(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Injected>> {
        Sendable::from_future(async move {
            self.switches.removes.fetch_add(1, Ordering::SeqCst);
            if self.switches.fail_remove.load(Ordering::SeqCst) {
                return Err(Injected);
            }
            self.bump();
            MinimizedTreeStore::<Sendable>::remove(&self.inner, id)
                .await
                .unwrap_or_else(never);
            Ok(())
        })
    }
}

fn flaky_node() -> (StoragePowerbox<MemoryStorage, OpenPolicy>, FlakyStore) {
    use crate::subduction::StorageTasks;
    let store = FlakyStore::default();
    let storage =
        <Sendable as StorageTasks<MemoryStorage, OpenPolicy, FlakyStore>>::storage_powerbox(
            MemoryStorage::new(),
            Arc::new(OpenPolicy),
            store.clone(),
            TokioSpawn,
        );
    (storage, store)
}

async fn commit_count(
    store: &FlakyStore,
    storage: &StoragePowerbox<MemoryStorage, OpenPolicy>,
) -> Option<usize> {
    ingest::read_cached(store, &storage.local_fetcher(ID), |tree| {
        tree.loose_commits().count()
    })
    .await
    .expect("storage read")
}

#[tokio::test]
async fn a_write_from_another_node_between_miss_and_populate_is_not_hidden() {
    let (storage, store) = flaky_node();
    let fetcher = storage.local_fetcher(ID);
    storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("seed");

    let coordination = fetcher.coordination();
    let Resident::Miss(seen) = coordination.lookup(&store).await else {
        panic!("seeding a write does not make the tree resident");
    };
    let read = coordination.prepare_cache_read(&store, seen).await;
    let old = ingest::load_tree(&fetcher).await.expect("scan");

    // Another node writes to the shared storage and maintains the shared
    // store, bypassing this node's local coordination entirely.
    let incoming = item(2).await;
    let update = Sedimentree::new(Vec::new(), vec![incoming.payload().clone()]);
    persist(&fetcher, incoming).await.expect("remote write");
    MinimizedTreeStore::<Sendable>::merge(&store, ID, update)
        .await
        .expect("remote store maintenance");

    assert_eq!(
        read.populate(old, &mut |tree| tree.loose_commits().count())
            .await,
        Some(Some(1)),
        "the old read is served, as a hit a moment earlier would have been"
    );
    assert_eq!(
        store.resident_commits().await,
        None,
        "the generation moved, so the old read must not populate the store"
    );
    assert_eq!(
        commit_count(&store, &storage).await,
        Some(2),
        "the next read rebuilds from storage"
    );
    assert_eq!(store.resident_commits().await, Some(2));
}

#[tokio::test]
async fn a_stale_populate_serves_the_read_without_storing_it() {
    let (storage, store) = flaky_node();
    storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("seed");

    let coordination = storage.local_fetcher(ID).coordination().clone();
    let Resident::Miss(Some(seen)) = coordination.lookup(&store).await else {
        panic!("miss with a generation");
    };
    store.bump();
    let read = coordination.prepare_cache_read(&store, Some(seen)).await;
    assert_eq!(
        read.populate(Some(MinimizedSedimentree::default()), &mut |_| ())
            .await,
        Some(Some(())),
        "a stale populate serves the read instead of retrying"
    );
    assert_eq!(store.count(|s| &s.populates), 1, "no retry");
    assert_eq!(store.resident_commits().await, None, "nothing stored");
    assert_eq!(commit_count(&store, &storage).await, Some(1));
    assert_eq!(store.count(|s| &s.populates), 2, "the next read populates");
    assert_eq!(store.resident_commits().await, Some(1));
}

#[tokio::test]
async fn a_failed_lookup_reads_storage_without_populating() {
    let (storage, store) = flaky_node();
    storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("seed");

    store.set(|s| &s.fail_get, true);
    assert_eq!(commit_count(&store, &storage).await, Some(1));
    assert_eq!(
        store.count(|s| &s.populates),
        0,
        "no generation to populate against"
    );
    assert_eq!(store.resident_commits().await, None);

    store.set(|s| &s.fail_get, false);
    assert_eq!(commit_count(&store, &storage).await, Some(1));
    assert_eq!(store.resident_commits().await, Some(1));
}

#[tokio::test]
async fn a_failed_populate_still_serves_the_read() {
    let (storage, store) = flaky_node();
    storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("seed");

    store.set(|s| &s.fail_populate, true);
    assert_eq!(commit_count(&store, &storage).await, Some(1));
    assert_eq!(store.count(|s| &s.populates), 1);
    assert_eq!(store.resident_commits().await, None);
}

#[tokio::test]
async fn a_failed_merge_bypasses_the_store_until_a_remove_succeeds() {
    let (storage, store) = flaky_node();
    let putter = storage.local_putter(ID);
    putter.save_commit(item(1).await).await.expect("seed");
    assert_eq!(commit_count(&store, &storage).await, Some(1));
    assert_eq!(store.resident_commits().await, Some(1), "warmed");

    // The write persists, but its store maintenance fails, leaving the
    // resident tree stale.
    store.set(|s| &s.fail_merge, true);
    store.set(|s| &s.fail_remove, true);
    putter.save_commit(item(2).await).await.expect("write");
    assert_eq!(
        store.resident_commits().await,
        Some(1),
        "stale entry remains"
    );

    // While the remove keeps failing, reads skip the stale entry entirely.
    let gets = store.count(|s| &s.gets);
    let removes = store.count(|s| &s.removes);
    assert_eq!(commit_count(&store, &storage).await, Some(2));
    assert_eq!(
        store.count(|s| &s.gets),
        gets,
        "a bypassed tree is not looked up"
    );
    assert!(
        store.count(|s| &s.removes) > removes,
        "the remove is retried"
    );

    // Once the remove succeeds, the store is back in use.
    store.set(|s| &s.fail_remove, false);
    store.set(|s| &s.fail_merge, false);
    assert_eq!(commit_count(&store, &storage).await, Some(2));
    assert_eq!(
        store.resident_commits().await,
        Some(2),
        "populated again after healing"
    );
    assert!(store.count(|s| &s.gets) > gets, "lookups resume");
}
