//! Exercise the completion boundary with controlled storage effects and scans.
#![allow(clippy::expect_used, clippy::panic)]

use super::*;
use crate::{
    connection::test_utils::test_signer,
    policy::open::OpenPolicy,
    storage::{
        memory::{MemoryStorage, MemoryStorageError},
        powerbox::StoragePowerbox,
    },
};
use alloc::{boxed::Box, collections::BTreeSet, sync::Arc, vec, vec::Vec};
use future_form::Sendable;
use futures::FutureExt;
use sedimentree_core::{
    blob::Blob,
    codec::error::{DecodeError, InvalidEnumTag},
    depth::CountLeadingZeroBytes,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

// Primitive regressions admit inline; ownership tests below exercise the real
// NodeTasks submission boundary, including cancellation and spawner rejection.
async fn complete<Async, Store, Work, R, const SHARDS: usize>(
    cache: &BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>,
    fetcher: &Fetcher<Async, Store>,
    work: Work,
    update: Option<Sedimentree>,
) -> Result<R, Store::Error>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Work: Future<Output = Result<R, Store::Error>>,
{
    super::complete(cache, fetcher, work, update, fetcher.state.begin()).await
}

type Cache = BoundedShardedMap<SedimentreeId, MinimizedSedimentree, 1>;
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
async fn save(cache: &Cache, storage: &StoragePowerbox<MemoryStorage, OpenPolicy>, n: u8) {
    let fetcher = storage.local_fetcher(ID);
    let item = item(n).await;
    let update = Sedimentree::new(Vec::new(), vec![item.payload().clone()]);
    complete(cache, &fetcher, persist(&fetcher, item), Some(update))
        .await
        .expect("save");
}

async fn persist(
    fetcher: &Fetcher<Sendable, MemoryStorage>,
    item: VerifiedMeta<LooseCommit>,
) -> Result<(), MemoryStorageError> {
    <MemoryStorage as Storage<Sendable>>::save_loose_commit(fetcher.storage(), ID, item).await
}

#[tokio::test]
async fn partial_persistence_invalidates_a_resident_tree() {
    let cache = Cache::new();
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
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
    let rebuilt = ingest::get_or_hydrate::<Sendable, _, _, _, 1>(
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
    let cache = Cache::new();
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
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
    let rebuilt = ingest::get_or_hydrate::<Sendable, _, _, _, 1>(
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
    let cache = Cache::new();
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    let revision = fetcher.state.scan_start().expect("stable");
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
    let stale = cache
        .install_checked(ID, old, || fetcher.state.scan_current(revision), |_| ())
        .await;
    assert!(stale.is_none(), "stale scan must retry, not reinstall");
    assert!(cache.get_cloned(&ID).await.is_none());
    assert_eq!(
        ingest::read_cached(&cache, &fetcher, |tree| tree.loose_commits().count())
            .await
            .expect("read"),
        Some(2)
    );
}

#[tokio::test]
async fn scan_adopts_a_concurrently_completed_cache_entry() {
    let cache = Cache::new();
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    save(&cache, &storage, 1).await;
    let fetcher = storage.local_fetcher(ID);
    let revision = fetcher.state.scan_start().expect("stable");
    let old = ingest::load_tree(&fetcher).await.expect("scan");
    save(&cache, &storage, 2).await;
    let adopted = cache
        .install_checked(
            ID,
            old,
            || fetcher.state.scan_current(revision),
            |tree| tree.loose_commits().count(),
        )
        .await;
    assert_eq!(
        adopted,
        Some(Some(2)),
        "use the resident value, not the old loaded copy"
    );
}

#[tokio::test]
async fn cold_readers_all_wake_after_an_unsettled_mutation() {
    let cache = Arc::new(Cache::new());
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
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

fn managed<Sp: crate::spawn::Spawn<Sendable> + Clone + Send + Sync + 'static>(
    backend: MemoryStorage,
    spawner: Sp,
) -> (StoragePowerbox<MemoryStorage, OpenPolicy>, Arc<Cache>) {
    use crate::{
        connection::test_utils::{MockConnection, TokioSpawn},
        handler::sync::SyncHandler,
        subduction::NodeTasks,
    };
    use subduction_crypto::signer::memory::MemorySigner;
    type Hdl = SyncHandler<
        Sendable,
        MemoryStorage,
        MockConnection,
        OpenPolicy,
        CountLeadingZeroBytes,
        TokioSpawn,
        1,
    >;
    let storage = StoragePowerbox::new(backend, Arc::new(OpenPolicy));
    let cache = Arc::new(Cache::new());
    <Sendable as NodeTasks<
        'static,
        MemoryStorage,
        MockConnection,
        crate::connection::message::SyncMessage,
        Hdl,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        1,
    >>::install_mutation_runner(&storage, cache.clone(), spawner);
    (storage, cache)
}
async fn settled(fetcher: &Fetcher<Sendable, MemoryStorage>) {
    loop {
        let listener = fetcher.state.listen();
        if fetcher.state.scan_start().is_some() {
            return;
        }
        listener.await;
    }
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
    let (started, release) = backend.gate_next_commit();
    let mut waiter = Box::pin(putter.save_commit(item(2).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("backend pending settlement");
    drop(waiter);
    release.send(Ok(())).await.expect("release");
    settled(&putter.as_fetcher()).await;
    assert_eq!(
        cache
            .get_cloned(&ID)
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
    let (started, release) = backend.gate_next_commit();
    let mut waiter = Box::pin(putter.save_commit(item(2).await));
    assert!(waiter.as_mut().now_or_never().is_none());
    started.recv().await.expect("backend pending settlement");
    drop(waiter);
    release.send(Err(failure())).await.expect("release failure");
    let fetcher = putter.as_fetcher();
    settled(&fetcher).await;
    assert!(
        cache.get_cloned(&ID).await.is_none(),
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
async fn standalone_and_previously_minted_capabilities_have_explicit_execution() {
    use crate::{
        connection::test_utils::{MockConnection, TokioSpawn},
        handler::sync::SyncHandler,
        subduction::NodeTasks,
    };
    use subduction_crypto::signer::memory::MemorySigner;
    type Hdl = SyncHandler<
        Sendable,
        MemoryStorage,
        MockConnection,
        OpenPolicy,
        CountLeadingZeroBytes,
        TokioSpawn,
        1,
    >;
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    let putter = storage.local_putter(ID);
    putter
        .save_commit(item(1).await)
        .await
        .expect("standalone inline");
    let cache = Arc::new(Cache::new());
    <Sendable as NodeTasks<
        'static,
        MemoryStorage,
        MockConnection,
        crate::connection::message::SyncMessage,
        Hdl,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        1,
    >>::install_mutation_runner(&storage, cache.clone(), TokioSpawn);
    putter
        .save_commit(item(2).await)
        .await
        .expect("existing capability now managed");
    assert_eq!(
        cache
            .get_cloned(&ID)
            .await
            .expect("cache completion")
            .loose_commits()
            .count(),
        2
    );
}

#[tokio::test]
async fn local_execution_owns_completion_without_a_network_listener() {
    use crate::{
        connection::test_utils::{ChannelMockConnection, TokioSpawn},
        handler::sync::SyncHandler,
        subduction::NodeTasks,
    };
    use future_form::Local;
    use subduction_crypto::signer::memory::MemorySigner;
    type Conn = ChannelMockConnection<crate::connection::message::SyncMessage>;
    type Hdl =
        SyncHandler<Local, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn, 1>;
    tokio::task::LocalSet::new()
        .run_until(async {
            let backend = MemoryStorage::new();
            let (started, release) = backend.gate_next_commit();
            let storage = StoragePowerbox::<_, _, Local>::new(backend, Arc::new(OpenPolicy));
            let cache = Arc::new(Cache::new());
            <Local as NodeTasks<
                'static,
                MemoryStorage,
                Conn,
                crate::connection::message::SyncMessage,
                Hdl,
                OpenPolicy,
                MemorySigner,
                CountLeadingZeroBytes,
                1,
            >>::install_mutation_runner(&storage, cache.clone(), TokioSpawn);
            let putter = storage.local_putter(ID);
            let mut waiter = Box::pin(putter.save_commit(item(1).await));
            assert!(waiter.as_mut().now_or_never().is_none());
            started.recv().await.expect("local effect");
            drop(waiter);
            release.send(Ok(())).await.expect("release");
            let fetcher = putter.as_fetcher();
            loop {
                let listener = fetcher.state.listen();
                if fetcher.state.scan_start().is_some() {
                    break;
                }
                listener.await;
            }
            assert_eq!(
                cache
                    .get_cloned(&ID)
                    .await
                    .expect("owned local completion")
                    .loose_commits()
                    .count(),
                1
            );
        })
        .await;
}
