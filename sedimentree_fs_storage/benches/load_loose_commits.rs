//! Benchmark: `FsStorage::load_loose_commits` throughput.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p sedimentree_fs_storage --bench load_loose_commits
//! ```
//!
//! This method is on the server's hot path: `recv_batch_sync_request`
//! reloads every commit (and fragment) for a tree from disk on each sync
//! request, and hydration does a full scan per tree. The current layout
//! costs ~`3N + 1` sequential filesystem ops to load `N` commits:
//!
//! ```text
//! commits/                       ← 1 readdir (listing)
//! └── {commit_id_hex}/           ← + per commit: 1 readdir (find the pair)
//!     ├── {digest}.meta          ←               + 1 open/read/close
//!     └── {digest}.blob          ←               + 1 open/read/close
//! ```
//!
//! Four groups answer "is the bottleneck the filesystem or elsewhere?":
//!
//! - `load_loose_commits/count`: end-to-end scaling over commit count.
//! - `load_loose_commits/blob_size`: bytes vs. metadata-ops sensitivity at
//!   a fixed commit count.
//! - `phases/*`: isolates a *sequential* raw filesystem read
//!   (`fs_read_only` — a sequential read baseline) from decoding
//!   (`decode_only`), against the same `end_to_end` call (which fans reads
//!   across blocking tasks, so it may beat the sequential baseline).
//! - `save/*`: write-path cost with full durability (temp files fsynced
//!   before rename, directories fsynced after) — single saves pay the
//!   fsyncs per item; `save_batch` amortizes the directory fsyncs.
//!
//! # Caveat: warm page cache
//!
//! Criterion re-reads the same files every iteration, so these numbers
//! measure syscall/decode overhead with a warm page cache. Cold-cache
//! numbers (one disk seek per inode) are strictly worse; to approximate
//! them, run once with caches dropped between samples:
//!
//! ```text
//! sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
//! ```

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::cast_possible_truncation
)]

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{
    signed::Signed, signer::memory::MemorySigner, verified_meta::VerifiedMeta,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// A populated store ready to be loaded from.
struct Populated {
    /// Owns the on-disk tree; dropped (and deleted) at end of scope.
    _dir: TempDir,
    storage: FsStorage,
    id: SedimentreeId,
}

/// Create an `FsStorage` in a tempdir holding `n` loose commits with
/// `blob_size`-byte blobs.
fn populate(rt: &Runtime, n: usize, blob_size: usize) -> Populated {
    let dir = tempfile::tempdir().expect("create tempdir");
    let storage = FsStorage::new(dir.path().to_path_buf()).expect("create FsStorage");
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let id = SedimentreeId::new([0xAB; 32]);

    rt.block_on(async {
        Storage::<Sendable>::save_sedimentree_id(&storage, id)
            .await
            .expect("save sedimentree id");

        for i in 0..n {
            let mut head_bytes = [0u8; 32];
            head_bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let head = CommitId::new(head_bytes);

            // Vary blob contents so digests differ across commits.
            let mut blob_data = vec![0u8; blob_size.max(8)];
            blob_data[..8].copy_from_slice(&(i as u64).to_be_bytes());

            let verified_blob = VerifiedBlobMeta::new(Blob::new(blob_data));
            let verified: VerifiedMeta<LooseCommit> = VerifiedMeta::seal::<Sendable, _>(
                &signer,
                (id, head, BTreeSet::new()),
                verified_blob,
            )
            .await;

            Storage::<Sendable>::save_loose_commit(&storage, id, verified)
                .await
                .expect("save loose commit");
        }
    });

    Populated {
        _dir: dir,
        storage,
        id,
    }
}

/// Locate the single `commits/` directory under `root` without depending on
/// `FsStorage` path internals: `trees/{bucket}/{leaf}/commits`.
fn find_commits_dir(root: &Path) -> PathBuf {
    let trees = root.join("trees");
    let bucket = std::fs::read_dir(&trees)
        .expect("read trees dir")
        .next()
        .expect("one bucket")
        .expect("bucket entry")
        .path();
    let leaf = std::fs::read_dir(&bucket)
        .expect("read bucket dir")
        .next()
        .expect("one leaf")
        .expect("leaf entry")
        .path();
    leaf.join("commits")
}

/// The raw-I/O phase: walk every `{commit_id}/` subdirectory of
/// `commits_dir` and read its `.meta` + `.blob` byte pair, mirroring the
/// crate-private `read_all_compound_sync`.
fn read_all_pairs_sync(commits_dir: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();

    for entry in std::fs::read_dir(commits_dir).expect("read commits dir") {
        let id_dir = entry.expect("dir entry").path();

        for file in std::fs::read_dir(&id_dir).expect("read commit id dir") {
            let file = file.expect("file entry");
            let name = file.file_name().into_string().expect("utf8 file name");

            if let Some(stem) = name.strip_suffix(".meta") {
                let meta = std::fs::read(id_dir.join(&name)).expect("read .meta");
                let blob = std::fs::read(id_dir.join(format!("{stem}.blob"))).expect("read .blob");
                out.push((meta, blob));
                break;
            }
        }
    }

    out
}

/// Decode a pre-read `.meta` + `.blob` pair, mirroring the decode phase of
/// `load_loose_commits`.
fn decode_pair(meta: Vec<u8>, blob: Vec<u8>) -> VerifiedMeta<LooseCommit> {
    let signed = Signed::try_decode(&meta).expect("decode Signed<LooseCommit>");
    VerifiedMeta::try_from_trusted(signed, Blob::new(blob)).expect("reconstruct VerifiedMeta")
}

/// End-to-end scaling over commit count at a small, realistic blob size.
fn bench_count(c: &mut Criterion) {
    let rt = Runtime::new().expect("create tokio runtime");
    let mut group = c.benchmark_group("load_loose_commits/count");
    group.sample_size(10);

    for n in [10usize, 100, 1_000, 10_000] {
        let populated = populate(&rt, n, 256);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::from_parameter(n), &populated, |b, p| {
            b.iter(|| {
                let commits = rt
                    .block_on(Storage::<Sendable>::load_loose_commits(&p.storage, p.id))
                    .expect("load loose commits");
                assert_eq!(commits.len(), n);
                commits
            });
        });
    }

    group.finish();
}

/// Bytes vs. metadata-ops: vary blob size at a fixed commit count.
fn bench_blob_size(c: &mut Criterion) {
    const N: usize = 1_000;

    let rt = Runtime::new().expect("create tokio runtime");
    let mut group = c.benchmark_group("load_loose_commits/blob_size");
    group.sample_size(10);

    for blob_size in [64usize, 1024, 65_536] {
        let populated = populate(&rt, N, blob_size);
        group.throughput(Throughput::Bytes((N * blob_size) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(blob_size),
            &populated,
            |b, p| {
                b.iter(|| {
                    let commits = rt
                        .block_on(Storage::<Sendable>::load_loose_commits(&p.storage, p.id))
                        .expect("load loose commits");
                    assert_eq!(commits.len(), N);
                    commits
                });
            },
        );
    }

    group.finish();
}

/// Phase isolation at a fixed shape: raw filesystem reads vs. decode vs.
/// the real end-to-end call. `fs_read_only + decode_only` should roughly
/// add up to `end_to_end`; whichever dominates is the bottleneck.
fn bench_phases(c: &mut Criterion) {
    const N: usize = 1_000;

    let rt = Runtime::new().expect("create tokio runtime");
    let mut group = c.benchmark_group("phases");
    group.sample_size(10);

    let populated = populate(&rt, N, 256);
    let commits_dir = find_commits_dir(populated.storage.root());
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function(BenchmarkId::new("fs_read_only", N), |b| {
        b.iter(|| {
            let pairs = read_all_pairs_sync(&commits_dir);
            assert_eq!(pairs.len(), N);
            pairs
        });
    });

    let pairs = read_all_pairs_sync(&commits_dir);
    group.bench_function(BenchmarkId::new("decode_only", N), |b| {
        b.iter_batched(
            || pairs.clone(),
            |pairs| {
                pairs
                    .into_iter()
                    .map(|(meta, blob)| decode_pair(meta, blob))
                    .collect::<Vec<_>>()
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(BenchmarkId::new("end_to_end", N), |b| {
        b.iter(|| {
            let commits = rt
                .block_on(Storage::<Sendable>::load_loose_commits(
                    &populated.storage,
                    populated.id,
                ))
                .expect("load loose commits");
            assert_eq!(commits.len(), N);
            commits
        });
    });

    group.finish();
}

/// Build a fresh, unique `VerifiedMeta<LooseCommit>` (unique head + blob so
/// repeated saves don't no-op on the CAS check).
fn seal_unique_commit(
    rt: &Runtime,
    signer: &MemorySigner,
    id: SedimentreeId,
    seq: u64,
    blob_size: usize,
) -> VerifiedMeta<LooseCommit> {
    let mut head_bytes = [0u8; 32];
    head_bytes[..8].copy_from_slice(&seq.to_be_bytes());
    let head = CommitId::new(head_bytes);

    let mut blob_data = vec![0u8; blob_size.max(8)];
    blob_data[..8].copy_from_slice(&seq.to_be_bytes());

    let verified_blob = VerifiedBlobMeta::new(Blob::new(blob_data));
    rt.block_on(VerifiedMeta::seal::<Sendable, _>(
        signer,
        (id, head, BTreeSet::new()),
        verified_blob,
    ))
}

/// Write-path cost with full durability: per-item saves vs the
/// dir-fsync-amortized batch path.
fn bench_save(c: &mut Criterion) {
    const BATCH: usize = 1_000;

    let rt = Runtime::new().expect("create tokio runtime");
    let mut group = c.benchmark_group("save");
    group.sample_size(10);

    let dir = tempfile::tempdir().expect("create tempdir");
    let storage = FsStorage::new(dir.path().to_path_buf()).expect("create FsStorage");
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let id = SedimentreeId::new([0xCD; 32]);
    rt.block_on(Storage::<Sendable>::save_sedimentree_id(&storage, id))
        .expect("save sedimentree id");

    let seq = std::sync::atomic::AtomicU64::new(0);
    let next = || seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    group.throughput(Throughput::Elements(1));
    group.bench_function("save_loose_commit", |b| {
        b.iter_batched(
            || seal_unique_commit(&rt, &signer, id, next(), 256),
            |verified| {
                rt.block_on(Storage::<Sendable>::save_loose_commit(
                    &storage, id, verified,
                ))
                .expect("save loose commit");
            },
            BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function(BenchmarkId::new("save_batch", BATCH), |b| {
        b.iter_batched(
            || {
                (0..BATCH)
                    .map(|_| seal_unique_commit(&rt, &signer, id, next(), 256))
                    .collect::<Vec<_>>()
            },
            |commits| {
                rt.block_on(Storage::<Sendable>::save_batch(
                    &storage,
                    id,
                    commits,
                    Vec::new(),
                ))
                .expect("save batch");
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_count,
    bench_blob_size,
    bench_phases,
    bench_save
);
criterion_main!(benches);
