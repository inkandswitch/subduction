//! Minimized-tree hot paths through the public node API.
//!
//! Uses only the builder's default store and methods that exist both before
//! and after the store became an effect, so the same file measures either
//! side of that change:
//!
//! - `hot_heads`: heads of a resident tree, the per-commit hot path.
//! - `hot_commits`: every commit of a resident tree (clones the tree).
//! - `write_resident`: add one commit to a resident tree of a fixed size
//!   (newness read, persist, merge, minimize).
//! - `cold_heads`: heads of a tree that is only in storage (full hydration).
//! - `contended_heads`: concurrent heads reads of one tree from many tasks.
//!
//! Run:
//! ```text
//! cargo bench -p subduction_core --bench tree_store
//! ```

#![allow(clippy::expect_used, missing_docs)]

use std::{collections::BTreeSet, sync::Arc};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use future_form::Sendable;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    connection::test_utils::{InstantTimeout, MockConnection, TokioSpawn},
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::signer::memory::MemorySigner;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

const ID: SedimentreeId = SedimentreeId::new([0xB7; 32]);
const SIZES: [u32; 2] = [100, 1_000];

fn head(n: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    bytes[4] = 0x01;
    CommitId::new(bytes)
}

fn blob(n: u32) -> Blob {
    Blob::new(n.to_le_bytes().to_vec())
}

fn runtime() -> Runtime {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

/// A node over `storage`, built from the builder's defaults. The listener and
/// manager are not needed for local reads and writes, so they are dropped.
macro_rules! node {
    ($storage:expr) => {{
        // Annotated so the builder's defaults fix every parameter.
        let builder: SubductionBuilder = SubductionBuilder::new();
        let (node, _handler, _listener, _manager) = builder
            .signer(MemorySigner::from_bytes(&[7; 32]))
            .storage($storage, Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();
        node
    }};
}

/// A linear chain of `commits` commits, in one batch.
fn chain(commits: u32) -> Vec<(CommitId, BTreeSet<CommitId>, Blob)> {
    (0..commits)
        .map(|n| {
            let parents = if n == 0 {
                BTreeSet::new()
            } else {
                BTreeSet::from([head(n - 1)])
            };
            (head(n), parents, blob(n))
        })
        .collect()
}

/// Storage holding a linear chain of `commits` commits for `ID`.
fn seeded_storage(rt: &Runtime, commits: u32) -> MemoryStorage {
    let storage = MemoryStorage::new();
    let node = node!(storage.clone());
    rt.block_on(node.store_commits_batch(ID, chain(commits)))
        .expect("seed");
    storage
}

fn bench_hot(c: &mut Criterion) {
    let rt = runtime();
    let mut heads = c.benchmark_group("hot_heads");
    for size in SIZES {
        let node = node!(seeded_storage(&rt, size));
        rt.block_on(node.get_heads(ID)).expect("warm");
        heads.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(node.get_heads(ID)).expect("heads"));
        });
    }
    heads.finish();

    let mut commits = c.benchmark_group("hot_commits");
    for size in SIZES {
        let node = node!(seeded_storage(&rt, size));
        rt.block_on(node.get_heads(ID)).expect("warm");
        commits.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| rt.block_on(node.get_commits(ID)).expect("commits"));
        });
    }
    commits.finish();
}

fn bench_write(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("write_resident");
    // Each sample writes once into a fresh tree of exactly `size` commits, so
    // every run measures the same tree size. Seeding is outside the timing.
    group.sample_size(30);
    for size in SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let node = node!(seeded_storage(&rt, size));
                    rt.block_on(node.get_heads(ID)).expect("warm");
                    node
                },
                |node| {
                    rt.block_on(node.store_commit(
                        ID,
                        head(size),
                        BTreeSet::from([head(size - 1)]),
                        blob(size),
                    ))
                    .expect("write");
                    // Returned so the node is dropped outside the timing.
                    node
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_cold(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("cold_heads");
    for size in SIZES {
        let storage = seeded_storage(&rt, size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || node!(storage.clone()),
                |node| {
                    rt.block_on(node.get_heads(ID)).expect("hydrate");
                    // Returned so the node is dropped outside the timing.
                    node
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_contended(c: &mut Criterion) {
    const TASKS: usize = 8;
    const READS_PER_TASK: usize = 64;
    let rt = runtime();
    let mut group = c.benchmark_group("contended_heads");
    let node = node!(seeded_storage(&rt, 1_000));
    rt.block_on(node.get_heads(ID)).expect("warm");
    group.bench_function(BenchmarkId::new("tasks", TASKS), |b| {
        b.iter(|| {
            rt.block_on(async {
                let readers: Vec<_> = (0..TASKS)
                    .map(|_| {
                        let node = node.clone();
                        tokio::spawn(async move {
                            for _ in 0..READS_PER_TASK {
                                node.get_heads(ID).await.expect("heads");
                            }
                        })
                    })
                    .collect();
                for reader in readers {
                    reader.await.expect("reader");
                }
            });
        });
    });
    group.finish();
}

criterion_group!(benches, bench_hot, bench_write, bench_cold, bench_contended);
criterion_main!(benches);
