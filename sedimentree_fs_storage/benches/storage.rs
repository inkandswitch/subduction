//! Filesystem storage benchmarks.
//!
//! Measures the CAS disk storage backend across the operations the sync protocol exercises
//! most: single-item saves, batched saves, hot-cache loads, and full-tree hydration.
//!
//! ## Why these?
//!
//! | Bench                         | Protocol hot path                                         |
//! |-------------------------------|-----------------------------------------------------------|
//! | `save_loose_commit/single`    | Every committed change                                    |
//! | `save_fragment/single`        | Every fragment boundary                                   |
//! | `save_batch`                  | Post-PR-#118 batch path; matters for Wasm parity (saveBatchAll) |
//! | `load_loose_commit`           | Every fetch request                                       |
//! | `load_all_sedimentree_ids`    | Server startup                                            |
//! | `concurrent_save_commits`     | Many-peer write contention                                |
//!
//! All benches use a fresh `tempfile::TempDir` per iteration to avoid OS page cache effects.
//! Results include filesystem syscall overhead (open/write/fsync/rename).
//!
//! Run with:
//! ```sh
//! cargo bench -p sedimentree_fs_storage --bench storage
//! ```

#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{collections::BTreeSet, sync::Arc};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use future_form::Sendable;
use futures::executor::block_on;
use sedimentree_core::{
    blob::verified::VerifiedBlobMeta,
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    test_utils::{
        blob_from_seed, commit_id_from_seed, commit_id_from_seed_with_leading_zeros,
        sedimentree_id_from_seed,
    },
};
use sedimentree_fs_storage::{FsStorage, FsStorageError};
use subduction_bench_support::{
    fixtures::tempdir::TempRoot, harness::criterion::default_criterion,
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{
    signer::memory::MemorySigner, test_utils::signer_from_seed, verified_meta::VerifiedMeta,
};

// ============================================================================
// Fixture helpers
// ============================================================================

/// Seal a `LooseCommit` together with a random blob into a `VerifiedMeta<LooseCommit>` ready
/// for storage.
fn seal_commit(
    signer: &MemorySigner,
    sedimentree_id: SedimentreeId,
    seed: u64,
    blob_size: usize,
) -> VerifiedMeta<LooseCommit> {
    let blob = blob_from_seed(seed, blob_size);
    let verified_blob = VerifiedBlobMeta::new(blob);
    let head = commit_id_from_seed(seed);
    let parents: BTreeSet<CommitId> = BTreeSet::new();
    let args = (sedimentree_id, head, parents);

    block_on(VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
        signer,
        args,
        verified_blob,
    ))
}

/// Seal a `Fragment` together with a random blob into a `VerifiedMeta<Fragment>` ready for
/// storage.
fn seal_fragment(
    signer: &MemorySigner,
    sedimentree_id: SedimentreeId,
    seed: u64,
    blob_size: usize,
) -> VerifiedMeta<Fragment> {
    let blob = blob_from_seed(seed, blob_size);
    let verified_blob = VerifiedBlobMeta::new(blob);

    // Fragment with a depth-1 head (realistic post-#122 boundary).
    let head = commit_id_from_seed_with_leading_zeros(1, seed);
    let boundary: BTreeSet<CommitId> = (0..2)
        .map(|i| commit_id_from_seed_with_leading_zeros(1, seed.wrapping_add(i + 10)))
        .collect();
    let checkpoints: Vec<CommitId> = (0..5)
        .map(|i| commit_id_from_seed(seed.wrapping_add(i + 100)))
        .collect();
    let args = (sedimentree_id, head, boundary, checkpoints);

    block_on(VerifiedMeta::<Fragment>::seal::<Sendable, _>(
        signer,
        args,
        verified_blob,
    ))
}

/// Build a fresh `FsStorage` + temp root. The `TempRoot` must be kept alive for the duration
/// of the storage handle — it's dropped with it.
fn fresh_storage() -> (FsStorage, TempRoot) {
    let root = TempRoot::new().expect("temp dir");
    let storage = FsStorage::new(root.path()).expect("fs storage");
    (storage, root)
}

/// Single-threaded Tokio runtime for benches with no concurrency requirement.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ---------------------------------------------------------------------------
// Trait-dispatch helpers
// ---------------------------------------------------------------------------
//
// `FsStorage` implements both `Storage<Sendable>` and `Storage<Local>`. Calling methods
// directly on the struct forces the compiler to disambiguate at every call site. These async
// wrappers pin `Sendable` once, keeping the bench bodies readable.

async fn save_id(storage: &FsStorage, id: SedimentreeId) -> Result<(), FsStorageError> {
    <FsStorage as Storage<Sendable>>::save_sedimentree_id(storage, id).await
}

async fn save_commit(
    storage: &FsStorage,
    id: SedimentreeId,
    verified: VerifiedMeta<LooseCommit>,
) -> Result<(), FsStorageError> {
    <FsStorage as Storage<Sendable>>::save_loose_commit(storage, id, verified).await
}

async fn save_frag(
    storage: &FsStorage,
    id: SedimentreeId,
    verified: VerifiedMeta<Fragment>,
) -> Result<(), FsStorageError> {
    <FsStorage as Storage<Sendable>>::save_fragment(storage, id, verified).await
}

async fn save_batch_both(
    storage: &FsStorage,
    id: SedimentreeId,
    commits: Vec<VerifiedMeta<LooseCommit>>,
    fragments: Vec<VerifiedMeta<Fragment>>,
) -> Result<usize, FsStorageError> {
    <FsStorage as Storage<Sendable>>::save_batch(storage, id, commits, fragments).await
}

async fn load_commits(
    storage: &FsStorage,
    id: SedimentreeId,
) -> Result<Vec<VerifiedMeta<LooseCommit>>, FsStorageError> {
    <FsStorage as Storage<Sendable>>::load_loose_commits(storage, id).await
}

async fn load_ids(storage: &FsStorage) -> Result<Set<SedimentreeId>, FsStorageError> {
    <FsStorage as Storage<Sendable>>::load_all_sedimentree_ids(storage).await
}

// ============================================================================
// Single-item save benches
// ============================================================================

fn bench_save_loose_commit(c: &mut Criterion) {
    let rt = runtime();
    let signer = signer_from_seed(0);
    let sed_id = sedimentree_id_from_seed(0);

    let mut group = c.benchmark_group("fs/save_loose_commit");

    for &blob_size in &[64usize, 4 * 1024, 256 * 1024] {
        group.throughput(Throughput::Bytes(blob_size as u64));
        group.bench_with_input(
            BenchmarkId::new("blob_size", blob_size),
            &blob_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let (storage, root) = fresh_storage();
                        // Prime sedimentree dirs so first save doesn't include mkdir cost.
                        rt.block_on(save_id(&storage, sed_id)).expect("save id");
                        let commit = seal_commit(&signer, sed_id, 42, size);
                        (storage, root, commit)
                    },
                    |(storage, root, commit)| {
                        rt.block_on(save_commit(&storage, sed_id, commit))
                            .expect("save commit");
                        drop(root);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_save_fragment(c: &mut Criterion) {
    let rt = runtime();
    let signer = signer_from_seed(0);
    let sed_id = sedimentree_id_from_seed(0);

    let mut group = c.benchmark_group("fs/save_fragment");

    for &blob_size in &[4 * 1024usize, 256 * 1024, 4 * 1024 * 1024] {
        group.throughput(Throughput::Bytes(blob_size as u64));
        group.bench_with_input(
            BenchmarkId::new("blob_size", blob_size),
            &blob_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let (storage, root) = fresh_storage();
                        rt.block_on(save_id(&storage, sed_id)).expect("save id");
                        let fragment = seal_fragment(&signer, sed_id, 42, size);
                        (storage, root, fragment)
                    },
                    |(storage, root, fragment)| {
                        rt.block_on(save_frag(&storage, sed_id, fragment))
                            .expect("save fragment");
                        drop(root);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Batch save — the saveBatchAll path
// ============================================================================

fn bench_save_batch(c: &mut Criterion) {
    let rt = runtime();
    let signer = signer_from_seed(0);
    let sed_id = sedimentree_id_from_seed(0);

    let mut group = c.benchmark_group("fs/save_batch");

    for &count in &[1usize, 10, 100] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("commits", count), &count, |b, &n| {
            b.iter_batched(
                || {
                    let (storage, root) = fresh_storage();
                    rt.block_on(save_id(&storage, sed_id)).expect("save id");
                    let items: Vec<VerifiedMeta<LooseCommit>> = (0..n)
                        .map(|i| seal_commit(&signer, sed_id, i as u64, 256))
                        .collect();
                    (storage, root, items)
                },
                |(storage, root, items)| {
                    rt.block_on(save_batch_both(&storage, sed_id, items, Vec::new()))
                        .expect("save batch");
                    drop(root);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Load benches
// ============================================================================

fn bench_load_loose_commits(c: &mut Criterion) {
    let rt = runtime();
    let signer = signer_from_seed(0);
    let sed_id = sedimentree_id_from_seed(0);

    let mut group = c.benchmark_group("fs/load_loose_commits");

    for &count in &[1usize, 10, 100, 1_000] {
        // Build one storage instance per tier and keep it alive for the bench: load is
        // read-only, so no need to rebuild each iter.
        let (storage, root) = fresh_storage();
        rt.block_on(save_id(&storage, sed_id)).expect("save id");

        let items: Vec<VerifiedMeta<LooseCommit>> = (0..count)
            .map(|i| seal_commit(&signer, sed_id, i as u64, 256))
            .collect();
        rt.block_on(save_batch_both(&storage, sed_id, items, Vec::new()))
            .expect("populate");

        let storage = Arc::new(storage);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &storage,
            |b, storage| {
                b.iter(|| rt.block_on(load_commits(storage, sed_id)).expect("load"));
            },
        );

        drop(storage);
        drop(root);
    }

    group.finish();
}

fn bench_load_all_sedimentree_ids(c: &mut Criterion) {
    let rt = runtime();
    let signer = signer_from_seed(0);

    let mut group = c.benchmark_group("fs/load_all_sedimentree_ids");

    for &count in &[1usize, 10, 100, 1_000] {
        let (storage, root) = fresh_storage();

        // Populate `count` sedimentrees, each with one commit — shape matches what `hydrate`
        // sees on a real server with many docs.
        for i in 0..count {
            let sed_id = sedimentree_id_from_seed(i as u64);
            rt.block_on(save_id(&storage, sed_id)).expect("save id");
            let commit = seal_commit(&signer, sed_id, i as u64, 64);
            rt.block_on(save_commit(&storage, sed_id, commit))
                .expect("save commit");
        }

        let storage = Arc::new(storage);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &storage,
            |b, storage| {
                b.iter(|| rt.block_on(load_ids(storage)).expect("list ids"));
            },
        );

        drop(storage);
        drop(root);
    }

    group.finish();
}

// ============================================================================
// Concurrent contention
// ============================================================================

/// N concurrent tasks each saving their own commit into the same sedimentree. Exercises the
/// storage lock path (`ids_cache` mutex, directory creation races, CAS skip paths).
fn bench_concurrent_save_commits(c: &mut Criterion) {
    let signer = signer_from_seed(0);
    let sed_id = sedimentree_id_from_seed(0);

    let mut group = c.benchmark_group("fs/concurrent_save_commits");
    group.sample_size(20);

    for &concurrency in &[1usize, 4, 16, 64] {
        group.throughput(Throughput::Elements(concurrency as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(concurrency),
            &concurrency,
            |b, &n| {
                // Multi-threaded runtime — concurrent IO is the point.
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n.clamp(2, 8))
                    .enable_all()
                    .build()
                    .expect("mt runtime");

                b.iter_batched(
                    || {
                        let (storage, root) = fresh_storage();
                        rt.block_on(save_id(&storage, sed_id)).expect("save id");
                        let items: Vec<_> = (0..n)
                            .map(|i| seal_commit(&signer, sed_id, i as u64, 256))
                            .collect();
                        (Arc::new(storage), root, items)
                    },
                    |(storage, root, items)| {
                        rt.block_on(async {
                            let mut handles = Vec::with_capacity(items.len());
                            for commit in items {
                                let storage = Arc::clone(&storage);
                                handles.push(tokio::spawn(async move {
                                    save_commit(&storage, sed_id, commit).await.expect("save");
                                }));
                            }
                            for h in handles {
                                h.await.expect("task");
                            }
                        });
                        drop(root);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_save_loose_commit,
        bench_save_fragment,
        bench_save_batch,
        bench_load_loose_commits,
        bench_load_all_sedimentree_ids,
        bench_concurrent_save_commits,
}
criterion_main!(benches);
