//! Exercise the completion boundary with controlled storage effects and scans.
#![allow(clippy::expect_used, clippy::panic)]

mod hydration;
mod store_failures;

use crate::{
    connection::test_utils::{InstantTimeout, TokioSpawn, test_signer},
    effects::config::Config,
    minimized_tree_store::{MinimizedTreeStore, memory::MemoryMinimizedTreeStore},
    policy::open::OpenPolicy,
    storage::{
        fetcher::Fetcher,
        memory::{MemoryStorage, MemoryStorageError},
        powerbox::StoragePowerbox,
        traits::Storage,
    },
    subduction::ingest,
    test_utils::probe_storage::ProbeStorage,
};
use alloc::{boxed::Box, collections::BTreeSet, sync::Arc, vec, vec::Vec};
use core::future::Future;
use future_form::{FutureForm, Sendable};
use futures::{FutureExt, StreamExt};
use sedimentree_core::{
    blob::Blob,
    codec::error::{DecodeError, InvalidEnumTag},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_crypto::{
    signed::Signed, signer::memory::MemorySigner, verified_meta::VerifiedMeta,
};

// Primitive regressions admit inline; ownership tests below exercise the real
// StorageTasks submission boundary, including cancellation and spawner rejection.
async fn complete<Async, Store, Trees, Work, R>(
    cache: &Trees,
    fetcher: &Fetcher<Async, Store>,
    work: Work,
    update: Option<Sedimentree>,
) -> Result<R, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Trees: MinimizedTreeStore<Async>,
    Work: Future<Output = Result<R, Store::Error>>,
{
    fetcher
        .coordination()
        .track_mutation()
        .persist::<Async, _, _, _, _>(cache, work, update, None)
        .await
}

type Cache = MemoryMinimizedTreeStore<1>;
type Cfg = Config<MemoryStorage, OpenPolicy, MemorySigner, InstantTimeout, TokioSpawn, Cache>;

type WatchedNode = crate::subduction::Subduction<
    'static,
    Sendable,
    Cfg,
    crate::connection::test_utils::MockConnection,
    crate::handler::sync::SyncHandler<
        Sendable,
        Cfg,
        crate::connection::test_utils::MockConnection,
        CountLeadingZeroBytes,
    >,
    CountLeadingZeroBytes,
>;
fn watched_node(backend: MemoryStorage) -> Arc<WatchedNode> {
    use crate::{
        connection::test_utils::{InstantTimeout, MockConnection, TokioSpawn},
        subduction::builder::SubductionBuilder,
    };
    let (node, _handler, _listener, _manager) = SubductionBuilder::new()
        .minimized_tree_store(Cache::new())
        .signer(test_signer())
        .storage(backend, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConnection>();
    node
}
async fn observed_event(stream: &mut crate::stored::StoredUpdates) -> crate::stored::StoredEvent {
    tokio::time::timeout(core::time::Duration::from_secs(2), stream.next())
        .await
        .expect("stream wakes")
        .expect("ongoing stream")
        .expect("event")
}

#[tokio::test]
async fn watching_waits_for_cancelled_mutations_to_settle_then_publishes_or_recovers() {
    use crate::stored::{StoredEvent, StoredItem};
    for (fail, resident) in [(false, false), (false, true), (true, false), (true, true)] {
        let backend = MemoryStorage::new();
        let node = watched_node(backend.clone());
        let putter = node.storage.local_putter(ID);
        putter
            .save_commit(item(1).await)
            .await
            .expect("stored seed");
        if resident {
            assert!(node.get_commits(ID).await.is_some());
        }
        assert_eq!(
            node.minimized_tree_store.get_cloned(ID).await.is_some(),
            resident
        );
        let mut stream = node.load_and_watch(ID);
        assert!(matches!(
            observed_event(&mut stream).await,
            StoredEvent::Item(_)
        ));
        assert!(matches!(
            observed_event(&mut stream).await,
            StoredEvent::StorageChecked
        ));
        let (started, release) = backend.gate_next_commit();
        let mut waiter = Box::pin(putter.save_commit(item(2).await));
        assert!(waiter.as_mut().now_or_never().is_none());
        started
            .recv()
            .await
            .expect("backend effect before settlement");
        assert!(
            stream.next().now_or_never().is_none(),
            "no publication before settlement"
        );
        drop(waiter);
        release
            .send(if fail { Err(failure()) } else { Ok(()) })
            .await
            .expect("settle");
        if fail {
            assert!(matches!(
                observed_event(&mut stream).await,
                StoredEvent::RescanRequired
            ));
            let mut count = 0;
            loop {
                match observed_event(&mut stream).await {
                    StoredEvent::Item(_) => count += 1,
                    StoredEvent::StorageChecked => break,
                    StoredEvent::PeerSynced { .. } | StoredEvent::RescanRequired => {
                        panic!("ordered recovery")
                    }
                }
            }
            assert_eq!(
                count, 2,
                "recover partial effects, not just successful calls"
            );
            assert!(node.minimized_tree_store.get_cloned(ID).await.is_none());
        } else {
            let StoredEvent::Item(StoredItem::Commit(v)) = observed_event(&mut stream).await else {
                panic!("settled item")
            };
            assert_eq!(v.payload().head(), CommitId::new([2; 32]));
            let cached = node.minimized_tree_store.get_cloned(ID).await;
            if resident {
                assert_eq!(cached.expect("resident").loose_commits().count(), 2);
            } else {
                assert!(cached.is_none(), "publication must not populate the cache");
            }
        }
    }
}

#[tokio::test]
async fn catch_up_waits_for_active_mutation_even_when_backend_bytes_are_already_visible() {
    use crate::stored::StoredEvent;
    let backend = MemoryStorage::new();
    let node = watched_node(backend.clone());
    let (started, release) = backend.gate_next_commit();
    let putter = node.storage.local_putter(ID);
    let mut waiter = Box::pin(putter.save_commit(item(1).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("unsettled effect");
    let mut stream = node.load_and_watch(ID);
    assert!(stream.next().now_or_never().is_none());
    drop(waiter);
    release.send(Ok(())).await.expect("settle");
    assert!(matches!(
        observed_event(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(
        observed_event(&mut stream).await,
        StoredEvent::StorageChecked
    ));
}

#[tokio::test]
async fn scan_started_before_a_write_retries_instead_of_certifying_an_empty_view() {
    use crate::stored::StoredEvent;
    let backend = MemoryStorage::new();
    let node = watched_node(backend.clone());
    let (started, release) = backend.gate_next_scan();
    let mut stream = node.load_and_watch(ID);
    started.recv().await.expect("old empty scan captured");
    node.storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("concurrent mutation");
    release.send(Ok(())).await.expect("return old scan");
    assert!(
        matches!(observed_event(&mut stream).await, StoredEvent::Item(_)),
        "the old scan must retry before StorageChecked"
    );
    assert!(matches!(
        observed_event(&mut stream).await,
        StoredEvent::StorageChecked
    ));
}

#[tokio::test]
async fn failed_snapshot_is_terminal_for_its_watch_not_mutation_admission() {
    use crate::stored::LoadError;
    let backend = MemoryStorage::new();
    let node = watched_node(backend.clone());
    let (started, release) = backend.gate_next_scan();
    let mut stream = node.load_and_watch(ID);
    started.recv().await.expect("scan captured");
    release.send(Err(failure())).await.expect("read fails");
    assert!(matches!(
        stream.next().await,
        Some(Err(LoadError::Operation(_)))
    ));
    assert!(stream.next().await.is_none());
    node.storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("node still admits writes");
}

#[tokio::test]
async fn aborted_watch_driver_wakes_pending_read_and_does_not_end_mutation_admission() {
    use crate::stored::{LoadError, StoredEvent};
    let node = watched_node(MemoryStorage::new());
    let mut stream = node.load_and_watch(ID);
    assert!(matches!(
        observed_event(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    stream.abort_handle().abort();
    assert!(matches!(stream.next().await, Some(Err(LoadError::Closed))));
    node.storage
        .local_putter(ID)
        .save_commit(item(1).await)
        .await
        .expect("other work still admitted");
}

const ID: SedimentreeId = SedimentreeId::new([83; 32]);

async fn item(n: u8) -> VerifiedMeta<LooseCommit> {
    let blob = Blob::new(vec![n]);
    let commit = LooseCommit::new(ID, CommitId::new([n; 32]), BTreeSet::new(), blob.meta())
        .expect("metadata");
    VerifiedMeta::new(
        Signed::seal::<Sendable, _>(&test_signer(), commit).await,
        blob,
    )
    .expect("blob")
}
fn failure() -> MemoryStorageError {
    DecodeError::from(InvalidEnumTag {
        tag: 255,
        type_name: "injected partial mutation",
    })
    .into()
}
/// Persist a fixture and explicitly warm its cache for resident-entry tests.
async fn save(cache: &Cache, storage: &StoragePowerbox<MemoryStorage, OpenPolicy>, n: u8) {
    let fetcher = storage.local_fetcher(ID);
    let item = item(n).await;
    let update = Sedimentree::new(Vec::new(), vec![item.payload().clone()]);
    complete(cache, &fetcher, persist(&fetcher, item), Some(update))
        .await
        .expect("save");
    ingest::read_cached(cache, &fetcher, |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
}

#[tokio::test]
async fn successful_cold_write_does_not_read_storage_or_populate_cache() {
    let backend = ProbeStorage::default();
    let (storage, cache) = managed(backend.clone(), TokioSpawn);
    let fetcher = storage.local_fetcher(ID);
    let incoming = item(1).await;
    let update = Sedimentree::new(Vec::new(), vec![incoming.payload().clone()]);
    backend.fail(ID);

    complete(&cache, &fetcher, persist(&fetcher, incoming), Some(update))
        .await
        .expect("a successful write must not depend on storage reads");
    assert!(cache.get_cloned(ID).await.is_none());
    assert!(
        fetcher
            .coordination()
            .prepare_read()
            .now_or_never()
            .is_some(),
        "write has finished"
    );

    backend.recover(ID);
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("first read loads persisted data"),
        Some(1)
    );
}

#[tokio::test]
async fn successful_resident_write_updates_cache_without_reading_storage() {
    let backend = ProbeStorage::default();
    let (storage, cache) = managed(backend.clone(), TokioSpawn);
    let fetcher = storage.local_fetcher(ID);
    persist(&fetcher, item(1).await)
        .await
        .expect("seed storage");
    ingest::read_cached(&cache, &fetcher, |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
    let incoming = item(2).await;
    let update = Sedimentree::new(Vec::new(), vec![incoming.payload().clone()]);
    backend.fail(ID);

    complete(&cache, &fetcher, persist(&fetcher, incoming), Some(update))
        .await
        .expect("resident update needs no storage reads");
    assert_eq!(
        cache
            .get_cloned(ID)
            .await
            .expect("still resident")
            .loose_commits()
            .count(),
        2
    );
    assert!(
        fetcher
            .coordination()
            .prepare_read()
            .now_or_never()
            .is_some(),
        "write has finished"
    );
}

#[tokio::test]
async fn eviction_during_write_does_not_trigger_repopulation() {
    let cache = Cache::with_capacity(1);
    let backend = ProbeStorage::default();
    let storage = managed_with_cache(backend.clone(), cache.clone(), TokioSpawn);
    let fetcher = storage.local_fetcher(ID);
    persist(&fetcher, item(1).await)
        .await
        .expect("seed storage");
    ingest::read_cached(&cache, &fetcher, |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
    let incoming = item(2).await;
    let update = Sedimentree::new(Vec::new(), vec![incoming.payload().clone()]);
    let other_id = SedimentreeId::new([84; 32]);

    complete(
        &cache,
        &fetcher,
        async {
            // Force eviction after the mutation starts, before it completes.
            cache
                .insert_if_absent(other_id, MinimizedSedimentree::default())
                .await;
            assert!(cache.get_cloned(ID).await.is_none());
            backend.fail(ID);
            persist(&fetcher, incoming).await
        },
        Some(update),
    )
    .await
    .expect("eviction must not turn a successful write into a read failure");
    assert!(cache.get_cloned(ID).await.is_none());
    assert!(cache.get_cloned(other_id).await.is_some());
    assert!(
        fetcher
            .coordination()
            .prepare_read()
            .now_or_never()
            .is_some(),
        "write has finished"
    );

    backend.recover(ID);
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("reload full history"),
        Some(2)
    );
}

async fn persist<Store: Storage<Sendable>>(
    fetcher: &Fetcher<Sendable, Store>,
    item: VerifiedMeta<LooseCommit>,
) -> Result<(), Store::Error> {
    Store::save_loose_commit(fetcher.storage(), ID, item).await
}

#[tokio::test]
async fn partial_persistence_invalidates_a_resident_tree() {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    let incoming = item(2).await;
    let result = complete(
        &cache,
        &fetcher,
        async {
            persist(&fetcher, incoming).await?;
            Err::<(), _>(failure())
        },
        Some(Sedimentree::default()),
    )
    .await;
    assert!(result.is_err());
    let rebuilt = ingest::get_or_hydrate::<Sendable, _, _, _, Cache>(
        &cache,
        &storage,
        &CountLeadingZeroBytes,
        ID,
    )
    .await
    .expect("rehydrate")
    .expect("exists");
    assert_eq!(
        rebuilt.loose_commits().count(),
        2,
        "resident reads must recover partial persistence"
    );
}

#[tokio::test]
async fn partial_deletion_does_not_leave_phantom_cached_items() {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    assert!(
        complete(
            &cache,
            &fetcher,
            async {
                <MemoryStorage as Storage<Sendable>>::delete_loose_commits(fetcher.storage(), ID)
                    .await?;
                Err::<(), _>(failure())
            },
            None
        )
        .await
        .is_err()
    );
    let rebuilt = ingest::get_or_hydrate::<Sendable, _, _, _, Cache>(
        &cache,
        &storage,
        &CountLeadingZeroBytes,
        ID,
    )
    .await
    .expect("rehydrate")
    .expect("still registered");
    assert_eq!(rebuilt.loose_commits().count(), 0);
}

#[tokio::test]
async fn pre_failure_scan_cannot_reinstall_after_invalidation() {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    let read = fetcher
        .coordination()
        .prepare_cache_read::<Sendable, _>(&cache, Some(()))
        .await;
    let old = ingest::load_tree(&fetcher).await.expect("scan");
    assert!(
        complete(
            &cache,
            &fetcher,
            async {
                persist(&fetcher, item(2).await).await?;
                Err::<(), _>(failure())
            },
            Some(Sedimentree::default())
        )
        .await
        .is_err()
    );
    let stale = read.install(old, &mut |_| ()).await;
    assert!(stale.is_none(), "stale scan must retry, not reinstall");
    assert!(cache.get_cloned(ID).await.is_none());
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("read"),
        Some(2)
    );
}

#[tokio::test]
async fn scan_overlapped_by_a_completed_write_retries_into_the_resident_entry() {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    let read = fetcher
        .coordination()
        .prepare_cache_read::<Sendable, _>(&cache, Some(()))
        .await;
    let old = ingest::load_tree(&fetcher).await.expect("scan");
    save(&cache, &storage, 2).await;
    let overlapped = read
        .install(old, &mut |tree| tree.loose_commits().count())
        .await;
    assert_eq!(
        overlapped, None,
        "an overlapped scan must not install its old copy"
    );
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("read"),
        Some(2),
        "the retry sees the resident value, not the old loaded copy"
    );
}

#[tokio::test]
async fn cold_readers_all_wake_after_an_unsettled_mutation() {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    let fetcher = storage.local_fetcher(ID);
    let (started, begin) = futures::channel::oneshot::channel();
    let (release, released) = futures::channel::oneshot::channel();
    let writing = {
        let cache = cache.clone();
        let fetcher = fetcher.clone();
        tokio::spawn(async move {
            let incoming = item(1).await;
            let update = Sedimentree::new(Vec::new(), vec![incoming.payload().clone()]);
            complete(
                &cache,
                &fetcher,
                async {
                    started.send(()).expect("reader");
                    released.await.expect("release");
                    persist(&fetcher, incoming).await
                },
                Some(update),
            )
            .await
            .expect("save");
        })
    };
    begin.await.expect("started");
    let read = ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count());
    let other = ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count());
    futures::pin_mut!(read, other);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(other.as_mut().now_or_never().is_none());
    release.send(()).expect("writer");
    let (read, other) = futures::join!(read, other);
    assert_eq!(read.expect("first read"), Some(1));
    assert_eq!(other.expect("second read"), Some(1));
    writing.await.expect("writer joins");
}

fn managed<Store, Sp>(backend: Store, spawner: Sp) -> (StoragePowerbox<Store, OpenPolicy>, Cache)
where
    Store: Storage<Sendable> + core::fmt::Debug + Send + Sync + 'static,
    Store::Error: Send + 'static,
    Sp: crate::spawn::Spawn<Sendable> + Clone + Send + Sync + 'static,
{
    let cache = Cache::new();
    (managed_with_cache(backend, cache.clone(), spawner), cache)
}

fn managed_with_cache<Store, Sp>(
    backend: Store,
    cache: Cache,
    spawner: Sp,
) -> StoragePowerbox<Store, OpenPolicy>
where
    Store: Storage<Sendable> + core::fmt::Debug + Send + Sync + 'static,
    Store::Error: Send + 'static,
    Sp: crate::spawn::Spawn<Sendable> + Clone + Send + Sync + 'static,
{
    use crate::subduction::StorageTasks;
    <Sendable as StorageTasks<Store, OpenPolicy, Cache>>::storage_powerbox(
        backend,
        Arc::new(OpenPolicy),
        cache,
        spawner,
    )
}
async fn settled(fetcher: &Fetcher<Sendable, MemoryStorage>) {
    let _attempt = fetcher.coordination().prepare_read().await;
}

#[tokio::test]
async fn cancelled_waiter_does_not_abandon_resident_cache_completion() {
    use crate::connection::test_utils::TokioSpawn;
    let backend = MemoryStorage::new();
    let (storage, cache) = managed(backend.clone(), TokioSpawn);
    let putter = storage.local_putter(ID);
    putter
        .save_commit(item(1).await)
        .await
        .expect("seed resident");
    ingest::read_cached(&cache, &putter.as_fetcher(), |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
    let (started, release) = backend.gate_next_commit();
    let mut waiter = Box::pin(putter.save_commit(item(2).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("backend pending settlement");
    drop(waiter);
    release.send(Ok(())).await.expect("release");
    settled(&putter.as_fetcher()).await;
    assert_eq!(
        cache
            .get_cloned(ID)
            .await
            .expect("resident")
            .loose_commits()
            .count(),
        2
    );
}

#[tokio::test]
async fn cancelled_waiter_still_invalidates_after_late_partial_failure() {
    use crate::connection::test_utils::TokioSpawn;
    let backend = MemoryStorage::new();
    let (storage, cache) = managed(backend.clone(), TokioSpawn);
    let putter = storage.local_putter(ID);
    putter
        .save_commit(item(1).await)
        .await
        .expect("seed resident");
    ingest::read_cached(&cache, &putter.as_fetcher(), |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
    let (started, release) = backend.gate_next_commit();
    let mut waiter = Box::pin(putter.save_commit(item(2).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("backend pending settlement");
    drop(waiter);
    release.send(Err(failure())).await.expect("release failure");
    let fetcher = putter.as_fetcher();
    settled(&fetcher).await;
    assert!(
        cache.get_cloned(ID).await.is_none(),
        "repair must not rely on eviction"
    );
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("read"),
        Some(2)
    );
}

#[tokio::test]
async fn unpolled_or_closed_mutation_never_reaches_storage() {
    use crate::{connection::test_utils::TokioSpawn, storage::mutation::MutationError};
    let (storage, _) = managed(MemoryStorage::new(), TokioSpawn);
    let putter = storage.local_putter(ID);
    drop(putter.save_commit(item(1).await));
    assert!(
        !putter
            .as_fetcher()
            .contains_sedimentree_id()
            .await
            .expect("not admitted")
    );
    storage.state.close();
    assert!(matches!(
        putter.save_commit(item(1).await).await,
        Err(MutationError::Closed)
    ));
    assert!(
        !putter
            .as_fetcher()
            .contains_sedimentree_id()
            .await
            .expect("closed")
    );
}

#[tokio::test]
async fn rejected_submission_returns_interrupted_and_closes_admission() {
    use crate::{connection::test_utils::TestSpawn, storage::mutation::MutationError};
    let (storage, _) = managed(MemoryStorage::new(), TestSpawn);
    let putter = storage.local_putter(ID);
    assert!(matches!(
        putter.save_commit(item(1).await).await,
        Err(MutationError::Closed)
    ));
    assert!(storage.state.closed());
    assert!(
        !putter
            .as_fetcher()
            .contains_sedimentree_id()
            .await
            .expect("rejected")
    );
}

#[derive(Clone, Default)]
struct CapturingSpawn(Arc<spin::Mutex<Vec<futures::future::AbortHandle>>>);
impl crate::spawn::Spawn<Sendable> for CapturingSpawn {
    fn spawn(
        &self,
        future: <Sendable as FutureForm>::Future<'static, ()>,
    ) -> futures::future::AbortHandle {
        use crate::connection::test_utils::TokioSpawn;
        let handle = crate::spawn::Spawn::<Sendable>::spawn(&TokioSpawn, future);
        self.0.lock().push(handle.clone());
        handle
    }
}
#[tokio::test]
async fn forced_driver_loss_ends_validity_not_backend_effects() {
    use crate::storage::mutation::MutationError;
    let backend = MemoryStorage::new();
    let (started, _release) = backend.gate_next_commit();
    let spawner = CapturingSpawn::default();
    let (storage, _) = managed(backend, spawner.clone());
    let putter = storage.local_putter(ID);
    let mut waiter = Box::pin(putter.save_commit(item(1).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("effect started");
    spawner.0.lock().first().expect("tracked").abort();
    assert!(matches!(waiter.await, Err(MutationError::Closed)));
    assert!(storage.state.closed());
    assert!(
        putter
            .as_fetcher()
            .contains_sedimentree_id()
            .await
            .expect("no rollback")
    );
}

#[tokio::test]
async fn cloned_powerboxes_manage_capabilities_from_the_first_write() {
    let spawner = CapturingSpawn::default();
    let (storage, cache) = managed(MemoryStorage::new(), spawner.clone());
    let clone = storage.clone();
    let putter = clone.local_putter(ID);
    drop(clone);
    putter
        .save_commit(item(1).await)
        .await
        .expect("first write");
    assert_eq!(spawner.0.lock().len(), 1, "first write uses the runner");
    ingest::read_cached(&cache, &putter.as_fetcher(), |_| ())
        .await
        .expect("warm cache")
        .expect("stored tree");
    storage
        .local_putter(ID)
        .save_commit(item(2).await)
        .await
        .expect("second write");
    assert_eq!(spawner.0.lock().len(), 2);
    assert_eq!(
        cache
            .get_cloned(ID)
            .await
            .expect("resident")
            .loose_commits()
            .count(),
        2
    );

    storage.state.close();
    assert!(matches!(
        putter.save_commit(item(3).await).await,
        Err(crate::storage::mutation::MutationError::Closed)
    ));
    assert_eq!(spawner.0.lock().len(), 2, "clones share shutdown state");
}

#[tokio::test]
async fn local_execution_owns_completion_without_a_network_listener() {
    use crate::{connection::test_utils::TokioSpawn, subduction::StorageTasks};
    use future_form::Local;
    tokio::task::LocalSet::new()
        .run_until(async {
            let backend = MemoryStorage::new();
            let (started, release) = backend.gate_next_commit();
            let cache = Cache::new();
            let storage =
                <Local as StorageTasks<MemoryStorage, OpenPolicy, Cache>>::storage_powerbox(
                    backend,
                    Arc::new(OpenPolicy),
                    cache.clone(),
                    TokioSpawn,
                );
            let putter = storage.local_putter(ID);
            let mut waiter = Box::pin(putter.save_commit(item(1).await));
            assert!(waiter.as_mut().now_or_never().is_none());
            started.recv().await.expect("local effect");
            drop(waiter);
            release.send(Ok(())).await.expect("release");
            let fetcher = putter.as_fetcher();
            let _attempt = fetcher.coordination().prepare_read().await;
            assert!(
                cache.get_cloned(ID).await.is_none(),
                "cold write stays uncached"
            );
            assert!(!storage.state.closed(), "owned task completed normally");
            assert_eq!(
                ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
                    .await
                    .expect("read after owned local completion"),
                Some(1)
            );
        })
        .await;
}
