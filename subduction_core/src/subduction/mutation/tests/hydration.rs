//! Cold minimization must release the shard without weakening cache handoff.

use super::*;
use core::cell::Cell;
use sedimentree_core::{
    depth::{Depth, DepthMetric},
    fragment::Fragment,
};

struct InspectDepth<F>(F);

impl<F: Fn()> DepthMetric for InspectDepth<F> {
    fn to_depth(&self, id: CommitId) -> Depth {
        (self.0)();
        CountLeadingZeroBytes.to_depth(id)
    }
}

/// A cold tree with a fragment covering a loose commit. Minimization must
/// actually consult the metric and remove the covered commit.
async fn cold_tree() -> (StoragePowerbox<MemoryStorage, OpenPolicy>, Cache) {
    let (storage, cache) = managed(MemoryStorage::new(), TokioSpawn);
    // Leading zero bytes make this commit a fragment-range boundary.
    let commit = item(0).await;
    let blob = Blob::new(vec![3]);
    let fragment = Fragment::new(
        ID,
        commit.payload().head(),
        BTreeSet::new(),
        &[],
        blob.meta(),
    )
    .expect("fragment metadata");
    let fragment = VerifiedMeta::new(
        Signed::seal::<Sendable, _>(&test_signer(), fragment).await,
        blob,
    )
    .expect("fragment blob");
    storage
        .local_putter(ID)
        .save_batch(vec![commit], vec![fragment])
        .await
        .expect("seed cold storage");
    assert!(cache.get_cloned(ID).await.is_none());
    (storage, cache)
}

#[tokio::test]
async fn cold_minimization_leaves_other_trees_in_the_shard_accessible() {
    let (storage, cache) = cold_tree().await;
    let other = SedimentreeId::new([84; 32]);
    cache
        .insert_if_absent(other, MinimizedSedimentree::default())
        .await;
    let calls = Cell::new(0);
    let metric = InspectDepth(|| {
        calls.set(calls.get() + 1);
        // Cache has exactly one shard. Polling this access must complete
        // immediately, even while the loaded tree is being minimized.
        assert!(
            cache
                .get_cloned(other)
                .now_or_never()
                .expect("cold minimization must not hold the shard lock")
                .is_some()
        );
    });
    let tree = ingest::get_or_hydrate(&cache, &storage, &metric, ID)
        .await
        .expect("hydrate")
        .expect("exists");
    assert!(calls.get() > 0, "fixture must exercise minimization");
    assert_eq!(tree.fragments().count(), 1);
    assert_eq!(tree.loose_commits().count(), 0);
    assert!(!cache.get_cloned(ID).await.expect("installed").is_dirty());

    let before = calls.get();
    let resident = ingest::get_or_hydrate(&cache, &storage, &metric, ID)
        .await
        .expect("resident read")
        .expect("exists");
    assert_eq!(resident, tree);
    assert_eq!(calls.get(), before, "installed tree is already minimal");
}

#[tokio::test]
async fn mutation_during_cold_minimization_forces_a_reload() {
    let (storage, cache) = cold_tree().await;
    let fetcher = storage.local_fetcher(ID);
    let incoming = item(2).await;
    let first = Cell::new(true);
    let metric = InspectDepth(|| {
        if first.replace(false) {
            // Deterministically interleave a partial write and invalidation
            // between the scan and installation, while minimization runs.
            let result = complete(
                &cache,
                &fetcher,
                async {
                    persist(&fetcher, incoming.clone()).await?;
                    Err::<(), _>(failure())
                },
                Some(Sedimentree::default()),
            )
            .now_or_never()
            .expect("mutation must not wait on the cache lock");
            assert!(result.is_err());
        }
    });
    let tree = ingest::get_or_hydrate(&cache, &storage, &metric, ID)
        .await
        .expect("hydrate after retry")
        .expect("exists");
    assert!(!first.get(), "fixture must exercise minimization");
    assert!(
        tree.has_loose_commit(incoming.payload().head()),
        "must reload the newer storage state"
    );
    assert_eq!(
        tree.loose_commits().count(),
        1,
        "covered commit stays minimized"
    );
    let resident = cache.get_cloned(ID).await.expect("installed");
    assert_eq!(resident.tree(), &tree);
    assert!(!resident.is_dirty());
}

#[tokio::test]
async fn cold_minimization_adopts_and_minimizes_a_newer_resident_entry() {
    let (storage, cache) = cold_tree().await;
    let fetcher = storage.local_fetcher(ID);
    let incoming = item(2).await;
    let first = Cell::new(true);
    let calls = Cell::new(0);
    let metric = InspectDepth(|| {
        calls.set(calls.get() + 1);
        if first.replace(false) {
            complete(
                &cache,
                &fetcher,
                persist(&fetcher, incoming.clone()),
                Some(Sedimentree::new(
                    Vec::new(),
                    vec![incoming.payload().clone()],
                )),
            )
            .now_or_never()
            .expect("mutation must not wait on the cache lock")
            .expect("write");
            // Another reader installs the newer full history, still dirty.
            let newer = ingest::load_tree(&fetcher)
                .now_or_never()
                .expect("in-memory scan is ready")
                .expect("scan")
                .expect("exists");
            assert!(newer.is_dirty());
            cache
                .insert_if_absent(ID, newer)
                .now_or_never()
                .expect("concurrent installation must not wait on the cache lock");
        }
    });
    let tree = ingest::get_or_hydrate(&cache, &storage, &metric, ID)
        .await
        .expect("adopt resident entry")
        .expect("exists");
    assert!(
        calls.get() >= 2,
        "both loaded and adopted dirty trees need minimization"
    );
    assert!(
        tree.has_loose_commit(incoming.payload().head()),
        "adopt newer entry, not the old scan"
    );
    assert_eq!(
        tree.loose_commits().count(),
        1,
        "adopted entry must be minimized too"
    );
    let resident = cache.get_cloned(ID).await.expect("resident");
    assert_eq!(resident.tree(), &tree);
    assert!(!resident.is_dirty());
}
