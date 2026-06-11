//! Backend shoot-out: `FsStorage` vs `RedbStorage` behind the same
//! `Storage<Sendable>` trait.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p sedimentree_redb_storage --bench backends
//! ```
//!
//! Groups (each parameterized by backend):
//!
//! - `load/count/{fs,redb}/N`: bulk `load_loose_commits` scaling, 256 B blobs.
//! - `load/blob_size/{fs,redb}/S`: byte-volume sensitivity at 1k commits.
//! - `save/single/{fs,redb}`: one durable `save_loose_commit`.
//! - `save/batch/{fs,redb}/1000`: one durable 1k-commit `save_batch`.
//!
//! A size-on-disk table (apparent + allocated bytes, plus compacted redb)
//! is printed to stderr before the timing runs.
//!
//! Same caveat as the `sedimentree_fs_storage` bench: reads are measured
//! with a warm page cache. Cold-cache deltas (clustered B+tree pages vs.
//! ~3 random I/Os per item) are expected to favor redb much more strongly
//! than the warm numbers shown here.

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

use std::{
    collections::BTreeSet,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_fs_storage::FsStorage;
use sedimentree_redb_storage::RedbStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use tokio::runtime::Runtime;

const TREE: [u8; 32] = [0xAB; 32];

/// Global commit sequence so every sealed commit is unique (CAS would
/// otherwise no-op repeat saves).
static SEQ: AtomicU64 = AtomicU64::new(0);

fn test_signer() -> MemorySigner {
    MemorySigner::from_bytes(&[42u8; 32])
}

/// Seal one unique commit with a `blob_size`-byte blob.
fn seal_commit(rt: &Runtime, id: SedimentreeId, blob_size: usize) -> VerifiedMeta<LooseCommit> {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);

    let mut head_bytes = [0u8; 32];
    head_bytes[..8].copy_from_slice(&seq.to_be_bytes());
    let head = CommitId::new(head_bytes);

    let mut blob_data = vec![0u8; blob_size.max(8)];
    blob_data[..8].copy_from_slice(&seq.to_be_bytes());

    let verified_blob = VerifiedBlobMeta::new(Blob::new(blob_data));
    rt.block_on(VerifiedMeta::seal::<Sendable, _>(
        &test_signer(),
        (id, head, BTreeSet::new()),
        verified_blob,
    ))
}

/// Populate `storage` with `n` unique commits.
fn populate<S: Storage<Sendable>>(rt: &Runtime, storage: &S, n: usize, blob_size: usize) {
    let id = SedimentreeId::new(TREE);
    rt.block_on(storage.save_sedimentree_id(id))
        .unwrap_or_else(|e| panic!("save sedimentree id: {e}"));

    // Populate through save_batch in chunks: orders of magnitude faster for
    // the durable FS backend, and identical end-state for both.
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let commits: Vec<_> = chunk
            .iter()
            .map(|_| seal_commit(rt, id, blob_size))
            .collect();
        rt.block_on(storage.save_batch(id, commits, Vec::new()))
            .unwrap_or_else(|e| panic!("save batch: {e}"));
    }
}

/// One backend under test: a constructor and a path to measure for size.
struct Backend<S> {
    name: &'static str,
    make: fn(&Path) -> S,
}

fn make_fs(dir: &Path) -> FsStorage {
    FsStorage::new(dir.to_path_buf()).expect("create FsStorage")
}

fn make_redb(dir: &Path) -> RedbStorage {
    RedbStorage::new(dir.join("data.redb")).expect("create RedbStorage")
}

const FS: Backend<FsStorage> = Backend {
    name: "fs",
    make: make_fs,
};
const REDB: Backend<RedbStorage> = Backend {
    name: "redb",
    make: make_redb,
};

/// Recursive (apparent bytes, allocated bytes) under `path`.
fn disk_usage(path: &Path) -> (u64, u64) {
    fn alloc_of(meta: &std::fs::Metadata) -> u64 {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            meta.blocks() * 512
        }
        #[cfg(not(unix))]
        {
            meta.len()
        }
    }

    let Ok(meta) = std::fs::metadata(path) else {
        return (0, 0);
    };

    if meta.is_file() {
        return (meta.len(), alloc_of(&meta));
    }

    let mut apparent = 0;
    let mut allocated = alloc_of(&meta);
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let (a, b) = disk_usage(&entry.path());
            apparent += a;
            allocated += b;
        }
    }

    (apparent, allocated)
}

fn human(bytes: u64) -> String {
    match bytes {
        b if b >= 1 << 30 => format!("{:.2} GiB", b as f64 / (1u64 << 30) as f64),
        b if b >= 1 << 20 => format!("{:.2} MiB", b as f64 / (1u64 << 20) as f64),
        b if b >= 1 << 10 => format!("{:.2} KiB", b as f64 / (1u64 << 10) as f64),
        b => format!("{b} B"),
    }
}

/// Print the size-on-disk comparison table to stderr.
fn report_sizes(rt: &Runtime) {
    eprintln!("\nsize on disk (apparent / allocated):");
    eprintln!(
        "┌──────────────────────┬──────────────────────────┬──────────────────────────────────────────┐"
    );
    eprintln!(
        "│ dataset              │ fs                       │ redb (uncompacted → compacted)           │"
    );
    eprintln!(
        "├──────────────────────┼──────────────────────────┼──────────────────────────────────────────┤"
    );

    for (n, blob_size, label) in [
        (10_000, 256, "10k × 256 B  "),
        (1_000, 65_536, "1k × 64 KiB  "),
    ] {
        let fs_dir = tempfile::tempdir().expect("tempdir");
        populate(rt, &make_fs(fs_dir.path()), n, blob_size);
        let (fs_app, fs_alloc) = disk_usage(fs_dir.path());

        let redb_dir = tempfile::tempdir().expect("tempdir");
        let redb_path = redb_dir.path().join("data.redb");
        populate(rt, &make_redb(redb_dir.path()), n, blob_size);
        let (redb_app, redb_alloc) = disk_usage(&redb_path);

        // Compact a copy of the database for the steady-state floor.
        let mut db = redb::Database::create(&redb_path).expect("reopen redb");
        db.compact().expect("compact redb");
        drop(db);
        let (_, redb_compacted) = disk_usage(&redb_path);

        eprintln!(
            "│ {label}        │ {:>11} / {:>10} │ {:>10} / {:>10} → {:>10} │",
            human(fs_app),
            human(fs_alloc),
            human(redb_app),
            human(redb_alloc),
            human(redb_compacted),
        );
    }

    eprintln!(
        "└──────────────────────┴──────────────────────────┴──────────────────────────────────────────┘"
    );
}

/// Bulk-load scaling over commit count.
fn bench_load_count<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    let mut group = c.benchmark_group("load/count");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    for n in [10usize, 100, 1_000, 10_000] {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        populate(rt, &storage, n, 256);

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new(backend.name, n), |b| {
            b.iter(|| {
                let commits = rt
                    .block_on(storage.load_loose_commits(id))
                    .unwrap_or_else(|e| panic!("load loose commits: {e}"));
                assert_eq!(commits.len(), n);
                commits
            });
        });
    }

    group.finish();
}

/// Byte-volume sensitivity at a fixed commit count.
fn bench_load_blob_size<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    const N: usize = 1_000;

    let mut group = c.benchmark_group("load/blob_size");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    for blob_size in [64usize, 1024, 65_536] {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        populate(rt, &storage, N, blob_size);

        group.throughput(Throughput::Bytes((N * blob_size) as u64));
        group.bench_function(BenchmarkId::new(backend.name, blob_size), |b| {
            b.iter(|| {
                let commits = rt
                    .block_on(storage.load_loose_commits(id))
                    .unwrap_or_else(|e| panic!("load loose commits: {e}"));
                assert_eq!(commits.len(), N);
                commits
            });
        });
    }

    group.finish();
}

/// Durable single-commit and 1k-batch save latency.
fn bench_save<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    const BATCH: usize = 1_000;

    let mut group = c.benchmark_group("save");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = (backend.make)(dir.path());
    rt.block_on(storage.save_sedimentree_id(id))
        .unwrap_or_else(|e| panic!("save sedimentree id: {e}"));

    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("single", backend.name), |b| {
        b.iter_batched(
            || seal_commit(rt, id, 256),
            |verified| {
                rt.block_on(storage.save_loose_commit(id, verified))
                    .unwrap_or_else(|e| panic!("save loose commit: {e}"));
            },
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function(
        BenchmarkId::new("batch", format!("{}/{BATCH}", backend.name)),
        |b| {
            b.iter_batched(
                || {
                    (0..BATCH)
                        .map(|_| seal_commit(rt, id, 256))
                        .collect::<Vec<_>>()
                },
                |commits| {
                    rt.block_on(storage.save_batch(id, commits, Vec::new()))
                        .unwrap_or_else(|e| panic!("save batch: {e}"));
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.finish();
}

fn all_benches(c: &mut Criterion) {
    let rt = Runtime::new().expect("create tokio runtime");

    report_sizes(&rt);

    bench_load_count(c, &rt, &FS);
    bench_load_count(c, &rt, &REDB);
    bench_load_blob_size(c, &rt, &FS);
    bench_load_blob_size(c, &rt, &REDB);
    bench_save(c, &rt, &FS);
    bench_save(c, &rt, &REDB);
}

criterion_group!(benches, all_benches);
criterion_main!(benches);
