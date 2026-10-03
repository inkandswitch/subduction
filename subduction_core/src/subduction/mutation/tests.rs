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
use alloc::{collections::BTreeSet, sync::Arc, vec, vec::Vec};
use future_form::Sendable;
use futures::FutureExt;
use sedimentree_core::{
    blob::Blob,
    codec::error::{DecodeError, InvalidEnumTag},
    depth::CountLeadingZeroBytes,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

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
    let putter = storage.local_putter::<Sendable>(ID);
    let item = item(n).await;
    let update = Sedimentree::new(Vec::new(), vec![item.payload().clone()]);
    complete(
        cache,
        &putter.as_fetcher(),
        putter.save_commit(item),
        Some(update),
    )
    .await
    .expect("save");
}

#[tokio::test]
async fn partial_persistence_invalidates_a_resident_tree() {
    let cache = Cache::new();
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    save(&cache, &storage, 1).await;
    let putter = storage.local_putter::<Sendable>(ID);
    let incoming = item(2).await;
    let result = complete(
        &cache,
        &putter.as_fetcher(),
        async {
            putter.save_commit(incoming).await?;
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
    let destroyer = storage.local_destroyer::<Sendable>(ID);
    let fetcher = storage.local_fetcher::<Sendable>(ID);
    assert!(
        complete(
            &cache,
            &fetcher,
            async {
                destroyer.delete_loose_commits().await?;
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
    let fetcher = storage.local_fetcher::<Sendable>(ID);
    let revision = fetcher.state.scan_start().expect("stable");
    let old = ingest::load_tree(&fetcher).await.expect("scan");
    let putter = storage.local_putter::<Sendable>(ID);
    assert!(
        complete(
            &cache,
            &fetcher,
            async {
                putter.save_commit(item(2).await).await?;
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
    let fetcher = storage.local_fetcher::<Sendable>(ID);
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
    let fetcher = storage.local_fetcher::<Sendable>(ID);
    let (started, begin) = futures::channel::oneshot::channel();
    let (release, released) = futures::channel::oneshot::channel();
    let writing = {
        let cache = cache.clone();
        let putter = storage.local_putter::<Sendable>(ID);
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
                    putter.save_commit(incoming).await
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
