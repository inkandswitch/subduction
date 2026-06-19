//! Backend shoot-out: `FsStorage` vs hybrid `RedbStorage` vs inline-only
//! `RedbStorage`, behind the same `Storage<Sendable>` trait.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p subduction_redb_storage --bench backends
//! ```
//!
//! Set `SUBDUCTION_BENCH_CI_SLIM=1` for a slim sweep (drops the 10k-commit
//! rows and the 64 KiB blob row) suitable for resource-constrained runners
//! or a quick local smoke.
//!
//! # Backends
//!
//! | id            | description                                             |
//! |---------------|---------------------------------------------------------|
//! | `fs`          | `FsStorage`: one dir + `.meta`/`.blob` files per item   |
//! | `redb`        | `RedbStorage`, default threshold: large blobs on FS CAS |
//! | `redb-inline` | `RedbStorage`, threshold = `usize::MAX`: everything in the B+tree |
//!
//! # Groups
//!
//! - `load/count/{backend}/N`: bulk `load_loose_commits` scaling, 256 B blobs.
//! - `load/blob_size/{backend}/S`: byte-volume sensitivity at 1k commits.
//! - `save/single/{backend}`: one durable `save_loose_commit`.
//! - `save/batch/{backend}/1000`: one durable 1k-commit `save_batch`.
//!
//! A size-on-disk table (apparent + allocated bytes, plus the compacted
//! floor for the redb variants) is printed to stderr before the timing runs
//! and written to `target/criterion/backend_sizes.txt` so it survives in CI
//! artifacts alongside the criterion results.
//!
//! # Comparing results: within-run only
//!
//! All backends run in a *single invocation* so that machine conditions
//! (background load, thermal state, page-cache pressure) cancel out.
//! Compare backends within one run; treat criterion's `change:` lines —
//! which compare against the *previous invocation's* saved baseline — with
//! suspicion: between-run drift on this workload has been observed at the
//! same magnitude as real 1.5–2x effects. To evaluate a code change, run
//! the full suite before and after and compare the *ratios between
//! backends*, not absolute times.
//!
//! Reads are measured with a warm page cache. Cold-cache deltas (clustered
//! B+tree pages vs. ~3 random I/Os per item) are expected to favor redb
//! much more strongly than the warm numbers shown here.
//!
//! The 64 KiB blob row is deliberately redb-inline's *worst case*: redb's
//! buddy allocator rounds each value up to the next power of two, and a
//! 64 KiB blob plus ~245 B of record overhead lands just past the 64 Ki
//! boundary → 128 KiB allocated per value (2x internal fragmentation that
//! `compact()` cannot reclaim — see `tests/size_probe.rs`). Arbitrary-sized
//! real-world blobs average ~25–33% buddy waste instead.

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::missing_const_for_fn
)]

use std::{
    collections::BTreeSet,
    fmt::Write as _,
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
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use subduction_redb_storage::RedbStorage;
use tokio::runtime::Runtime;

const TREE: [u8; 32] = [0xAB; 32];

/// Global commit sequence so every sealed commit is unique (CAS would
/// otherwise no-op repeat saves).
static SEQ: AtomicU64 = AtomicU64::new(0);

/// Whether the slim sweep was requested (see module docs).
fn ci_slim() -> bool {
    std::env::var("SUBDUCTION_BENCH_CI_SLIM").is_ok_and(|v| !v.is_empty())
}

/// Commit-count sweep, set to the production per-tree commit percentiles
/// (p50=2, p99=48, p99.9=1310, max=27444 across 190.6k trees, 2026-06): most
/// documents are tiny, with a heavy tail of large ones.
fn count_sweep() -> &'static [usize] {
    if ci_slim() {
        &[2, 48, 1_310]
    } else {
        &[2, 48, 1_310, 27_444]
    }
}

/// Blob-size sweep, set to the production blob-size percentiles (p50=187 B,
/// p90=639 B, p99=44 KB, external-mean 169 KB across 1.71M blobs, 2026-06).
/// 97.9% of blobs are inline (≤16 KiB); the multi-MB external tail is measured
/// separately by `large_external` (it would need a tiny `N` here).
fn blob_size_sweep() -> &'static [usize] {
    if ci_slim() {
        &[187, 44_233]
    } else {
        &[187, 639, 44_233, 169_259]
    }
}

/// Number of trees in the production-replica corpus (`SUBDUCTION_BENCH_PROD_REPLICA=1`).
/// Override with `SUBDUCTION_BENCH_REPLICA_TREES`. The footprint *ratio* is
/// scale-invariant, so a few thousand trees suffice to show it.
fn replica_trees() -> usize {
    std::env::var("SUBDUCTION_BENCH_REPLICA_TREES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(if ci_slim() { 1_000 } else { 5_000 })
}

/// Run only the production-replica group (footprint + id-enumeration + load).
fn replica_only() -> bool {
    std::env::var("SUBDUCTION_BENCH_PROD_REPLICA").is_ok_and(|v| !v.is_empty())
}

/// Datasets for the size-on-disk table: `(items, blob_size, label)`.
fn size_datasets() -> &'static [(usize, usize, &'static str)] {
    if ci_slim() {
        &[(1_000, 256, "1k × 256 B"), (1_000, 49_152, "1k × 48 KiB")]
    } else {
        &[
            (10_000, 256, "10k × 256 B"),
            (1_000, 49_152, "1k × 48 KiB"),
            (1_000, 65_536, "1k × 64 KiB"),
        ]
    }
}

/// Concurrency degrees (number of trees written simultaneously) for the
/// `concurrent_writes` gate. The total item count is held *constant* across
/// the sweep, so each step trades batch size for writer concurrency.
fn concurrency_sweep() -> &'static [usize] {
    if ci_slim() {
        &[1, 16, 64]
    } else {
        &[1, 4, 16, 64, 256]
    }
}

/// Run only the `concurrent_writes` gate (skip the size table and every
/// other group). Lets the production-readiness gate be measured on its own
/// without paying for the full shoot-out's populate phases.
fn concurrency_only() -> bool {
    std::env::var("SUBDUCTION_BENCH_CONCURRENCY_ONLY").is_ok_and(|v| !v.is_empty())
}

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

/// Populate `storage` with `n` unique commits, returning their ids (in
/// insertion order) for benches that read items back individually.
fn populate<S: Storage<Sendable>>(
    rt: &Runtime,
    storage: &S,
    n: usize,
    blob_size: usize,
) -> Vec<CommitId> {
    let id = SedimentreeId::new(TREE);
    rt.block_on(storage.save_sedimentree_id(id))
        .unwrap_or_else(|e| panic!("save sedimentree id: {e}"));

    let mut ids = Vec::with_capacity(n);

    // Populate through save_batch in chunks: orders of magnitude faster for
    // the durable FS backend, and identical end-state for all backends.
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let commits: Vec<_> = chunk
            .iter()
            .map(|_| seal_commit(rt, id, blob_size))
            .collect();
        ids.extend(commits.iter().map(|vm| vm.payload().head()));
        rt.block_on(storage.save_batch(id, commits, Vec::new()))
            .unwrap_or_else(|e| panic!("save batch: {e}"));
    }

    ids
}

/// One backend under test.
struct Backend<S> {
    /// Criterion id component (also the size-table label).
    name: &'static str,
    /// Construct the backend rooted at the given (temp) directory.
    make: fn(&Path) -> S,
    /// Whether the size table should also report a post-`compact()` floor
    /// (redb variants only).
    compactable: bool,
}

fn make_fs(dir: &Path) -> FsStorage {
    FsStorage::new(dir.to_path_buf()).expect("create FsStorage")
}

fn make_redb(dir: &Path) -> RedbStorage {
    RedbStorage::new(dir).expect("create RedbStorage")
}

fn make_redb_inline(dir: &Path) -> RedbStorage {
    RedbStorage::with_inline_threshold(dir, usize::MAX).expect("create inline RedbStorage")
}

const FS: Backend<FsStorage> = Backend {
    name: "fs",
    make: make_fs,
    compactable: false,
};

const REDB: Backend<RedbStorage> = Backend {
    name: "redb",
    make: make_redb,
    compactable: true,
};

const REDB_INLINE: Backend<RedbStorage> = Backend {
    name: "redb-inline",
    make: make_redb_inline,
    compactable: true,
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

/// One measured row of the size table.
struct SizeRow {
    dataset: &'static str,
    backend: &'static str,
    apparent: u64,
    allocated: u64,
    compacted: Option<u64>,
}

/// Populate a fresh store and measure its footprint (and, for redb
/// variants, the post-compaction floor).
fn measure_size<S: Storage<Sendable>>(
    rt: &Runtime,
    backend: &Backend<S>,
    n: usize,
    blob_size: usize,
    dataset: &'static str,
) -> SizeRow {
    let dir = tempfile::tempdir().expect("tempdir");

    // Inner scope: the storage handle (and its database lock, for redb
    // variants) must drop before the compaction reopen below.
    {
        let storage = (backend.make)(dir.path());
        populate(rt, &storage, n, blob_size);
    }

    let (apparent, allocated) = disk_usage(dir.path());

    let compacted = backend.compactable.then(|| {
        let db_path = dir.path().join(subduction_redb_storage::DB_FILE_NAME);
        let mut db = redb::Database::create(&db_path).expect("reopen redb for compaction");
        db.compact().expect("compact redb");
        drop(db);
        disk_usage(dir.path()).1
    });

    SizeRow {
        dataset,
        backend: backend.name,
        apparent,
        allocated,
        compacted,
    }
}

/// Render, print (stderr), and persist the size-on-disk table.
fn report_sizes(rt: &Runtime) {
    let mut rows = Vec::new();
    for &(n, blob_size, dataset) in size_datasets() {
        rows.push(measure_size(rt, &FS, n, blob_size, dataset));
        rows.push(measure_size(rt, &REDB, n, blob_size, dataset));
        rows.push(measure_size(rt, &REDB_INLINE, n, blob_size, dataset));
    }

    let mut table = String::new();
    let _ = writeln!(table, "size on disk:");
    let _ = writeln!(
        table,
        "┌──────────────┬─────────────┬─────────────┬─────────────┬─────────────┐"
    );
    let _ = writeln!(
        table,
        "│ dataset      │ backend     │ apparent    │ allocated   │ compacted   │"
    );
    let _ = writeln!(
        table,
        "├──────────────┼─────────────┼─────────────┼─────────────┼─────────────┤"
    );

    for row in &rows {
        let compacted = row.compacted.map_or_else(|| "—".to_owned(), human);
        let _ = writeln!(
            table,
            "│ {:<12} │ {:<11} │ {:>11} │ {:>11} │ {:>11} │",
            row.dataset,
            row.backend,
            human(row.apparent),
            human(row.allocated),
            compacted,
        );
    }

    let _ = writeln!(
        table,
        "└──────────────┴─────────────┴─────────────┴─────────────┴─────────────┘"
    );

    eprintln!("\n{table}");

    // Persist next to the criterion results so CI artifact uploads of
    // `target/criterion/` capture it.
    let out_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/criterion");
    if std::fs::create_dir_all(&out_dir).is_ok() {
        let path = out_dir.join("backend_sizes.txt");
        if let Err(e) = std::fs::write(&path, &table) {
            eprintln!("warning: could not write {}: {e}", path.display());
        }
    }
}

/// Bulk-load scaling over commit count.
fn bench_load_count<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    let mut group = c.benchmark_group("load/count");
    let id = SedimentreeId::new(TREE);

    for &n in count_sweep() {
        // Cheap sub-ms cases afford tighter confidence intervals.
        group.sample_size(if n <= 100 { 50 } else { 10 });

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

    for &blob_size in blob_size_sweep() {
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

/// Missing-set sizes for the point-read sweep.
const POINT_READ_SWEEP: &[usize] = &[32, 128, 256, 512, 1024];

/// Concurrent point reads per chunk — mirrors the sync responder's
/// `POINT_READ_CHUNK` so the measured cost matches the handler's access
/// pattern.
const POINT_READ_CHUNK: usize = 32;

/// Chunked-concurrent point reads of `m` items from an `n`-item tree.
///
/// This is the *other half* of the responder's crossover trade-off: the
/// `load/count` group measures the bulk scan (cost ∝ tree size `n`), this
/// group measures targeted point reads (cost ∝ missing set `m`). The
/// crossover for "peer is missing `m` of `n` items" sits where the two
/// curves intersect — compare within one run.
fn bench_point_reads<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    let mut group = c.benchmark_group("load/point_reads");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    for &n in count_sweep() {
        if n < POINT_READ_SWEEP[0] {
            continue;
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        let all_ids = populate(rt, &storage, n, 256);

        for &m in POINT_READ_SWEEP.iter().filter(|&&m| m <= n) {
            // Evenly spaced ids so the sample isn't biased toward one
            // region of the key space / directory.
            let wanted: Vec<CommitId> = all_ids
                .iter()
                .step_by((n / m).max(1))
                .take(m)
                .copied()
                .collect();
            assert_eq!(wanted.len(), m);

            group.throughput(Throughput::Elements(m as u64));
            group.bench_function(BenchmarkId::new(format!("{}/{n}", backend.name), m), |b| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut out = Vec::with_capacity(m);
                        for chunk in wanted.chunks(POINT_READ_CHUNK) {
                            let results = futures::future::join_all(
                                chunk
                                    .iter()
                                    .map(|commit_id| storage.load_loose_commit(id, *commit_id)),
                            )
                            .await;

                            for result in results {
                                out.push(
                                    result
                                        .unwrap_or_else(|e| panic!("point read: {e}"))
                                        .expect("populated commit must exist"),
                                );
                            }
                        }
                        assert_eq!(out.len(), m);
                        out
                    })
                });
            });
        }
    }

    group.finish();
}

/// Fetch `wanted` items as the responder's point-read branch does:
/// concurrent `load_loose_commit` calls in chunks of 32.
async fn fetch_via_point_reads<S: Storage<Sendable>>(
    storage: &S,
    id: SedimentreeId,
    wanted: &[CommitId],
) -> usize {
    let mut fetched = 0;
    for chunk in wanted.chunks(POINT_READ_CHUNK) {
        let results = futures::future::join_all(
            chunk
                .iter()
                .map(|commit_id| storage.load_loose_commit(id, *commit_id)),
        )
        .await;

        for result in results {
            let verified = result
                .unwrap_or_else(|e| panic!("point read: {e}"))
                .expect("populated commit must exist");
            let (_signed, _, _blob) = verified.into_full_parts();
            fetched += 1;
        }
    }
    fetched
}

/// Fetch `wanted` items as the responder's bulk-scan branch does:
/// one `load_loose_commits` scan filtered to the wanted set.
async fn fetch_via_scan<S: Storage<Sendable>>(
    storage: &S,
    id: SedimentreeId,
    wanted: &std::collections::BTreeSet<CommitId>,
) -> usize {
    let mut fetched = 0;
    for vm in storage
        .load_loose_commits(id)
        .await
        .unwrap_or_else(|e| panic!("bulk scan: {e}"))
    {
        if wanted.contains(&vm.payload().head()) {
            let (_signed, _, _blob) = vm.into_full_parts();
            fetched += 1;
        }
    }
    fetched
}

/// End-to-end validation of the responder's crossover rule: measure both
/// "no crossover" strategies on identical data, across the missing-set
/// sweep.
///
/// - `point`: what the fast path did before the crossover (always point
///   reads) — degrades linearly with the missing set.
/// - `scan`: what the responder did before the fast path (always bulk
///   scan + filter) — pays the whole tree regardless of the diff.
///
/// The rule itself (`missing > max(total/4, 32)`, sync.rs) is *not* benched
/// as a third row: below the crossover it executes the `point` code path
/// byte-for-byte, above it the `scan` path, so a separate row can only
/// re-measure one of these plus inter-row machine drift (which on this
/// hardware reaches ~2–3x and initially produced misleading "rule beats
/// its own branch" artifacts). The rule's cost is therefore *by
/// construction* `min` of these two rows at each `m`; that it picks the
/// right branch is pinned by the responder integration tests
/// (`responder_cache_fast_path.rs`).
fn bench_crossover<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    let mut group = c.benchmark_group("crossover");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    let tree_sizes: &[usize] = if ci_slim() {
        &[1_000]
    } else {
        &[1_000, 10_000]
    };

    for &n in tree_sizes {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        let all_ids = populate(rt, &storage, n, 256);

        // Sample around the rule boundary (total/4): below, at-ish, above,
        // and the full cold clone.
        for m in [n / 32, n / 8, n / 4, n / 2, n] {
            let wanted_vec: Vec<CommitId> = all_ids
                .iter()
                .step_by((n / m).max(1))
                .take(m)
                .copied()
                .collect();
            assert_eq!(wanted_vec.len(), m);
            let wanted_set: std::collections::BTreeSet<CommitId> =
                wanted_vec.iter().copied().collect();

            group.throughput(Throughput::Elements(m as u64));

            group.bench_function(
                BenchmarkId::new(format!("point/{}/{n}", backend.name), m),
                |b| {
                    b.iter(|| {
                        let fetched = rt.block_on(fetch_via_point_reads(&storage, id, &wanted_vec));
                        assert_eq!(fetched, m);
                    });
                },
            );

            group.bench_function(
                BenchmarkId::new(format!("scan/{}/{n}", backend.name), m),
                |b| {
                    b.iter(|| {
                        let fetched = rt.block_on(fetch_via_scan(&storage, id, &wanted_set));
                        assert_eq!(fetched, m);
                    });
                },
            );
        }
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

    // Realistic small batches: the median document is 2 commits and sync
    // deltas are tiny, so redb's one-fsync-per-transaction win matters here
    // more than at the 1000-batch above.
    for &batch in &[1usize, 2, 8, 32] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(
            BenchmarkId::new("smallbatch", format!("{}/{batch}", backend.name)),
            |b| {
                b.iter_batched(
                    || {
                        (0..batch)
                            .map(|_| seal_commit(rt, id, 256))
                            .collect::<Vec<_>>()
                    },
                    |commits| {
                        rt.block_on(storage.save_batch(id, commits, Vec::new()))
                            .unwrap_or_else(|e| panic!("small batch save: {e}"));
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

/// Hydration read cost: the full `load_loose_commits` (reads every blob)
/// vs the metadata-only `load_loose_commit_metas` (skips blob bytes — no
/// inline copy on redb, no external/`.blob` file read on either backend).
///
/// Hydration rebuilds the in-memory tree from payloads and discards the
/// blobs, so the delta here is pure wasted I/O avoided. The win is
/// negligible for tiny blobs and large for production-sized external blobs.
fn bench_hydrate_metas<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    const N: usize = 1_000;

    let mut group = c.benchmark_group("hydrate");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    // 256 B: blobs inline/tiny (win should be marginal). 192 KiB: the
    // production external-blob average (the case the optimization targets).
    for &blob_size in &[256usize, 196_608] {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        populate(rt, &storage, N, blob_size);

        group.throughput(Throughput::Elements(N as u64));
        group.bench_function(
            BenchmarkId::new(format!("full/{}", backend.name), blob_size),
            |b| {
                b.iter(|| {
                    // Match the hydration path: full load, then extract
                    // payloads and drop blobs (what `load_tree` does today).
                    let payloads: Vec<LooseCommit> = rt
                        .block_on(storage.load_loose_commits(id))
                        .unwrap_or_else(|e| panic!("full load: {e}"))
                        .into_iter()
                        .map(|vm| vm.into_full_parts().1)
                        .collect();
                    assert_eq!(payloads.len(), N);
                });
            },
        );
        group.bench_function(
            BenchmarkId::new(format!("metas/{}", backend.name), blob_size),
            |b| {
                b.iter(|| {
                    let metas = rt
                        .block_on(storage.load_loose_commit_metas(id))
                        .unwrap_or_else(|e| panic!("metas load: {e}"));
                    assert_eq!(metas.len(), N);
                });
            },
        );
    }

    group.finish();
}

/// Aggregate durable-write throughput as write concurrency rises — the gate
/// for replacing the parallel-friendly `FsStorage` with redb.
///
/// The production server writes many *distinct* trees at once: post-#220 the
/// listener spawns up to `MAX_INFLIGHT_DISPATCH` dispatch tasks, and each
/// peer typically syncs a different document. `FsStorage` writes land in
/// independent directories and parallelize across blocking threads; redb
/// funnels *every* write through a single writer with one fsync per
/// transaction. This group holds the total item count fixed and splits it
/// across `T` trees written concurrently, so the sweep isolates the cost of
/// concurrency itself: an ideal parallel backend stays flat (or improves)
/// as `T` grows, while a fully serialized one pays one fsync per tree.
///
/// Untimed setup seals all commits and builds a fresh store; the timed body
/// spawns one `save_batch` task per tree and joins them on the multi-thread
/// runtime.
fn bench_concurrent_writes<S: Storage<Sendable> + Clone + Send + Sync + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    /// Total commits persisted per iteration, fixed across the concurrency
    /// sweep. 256 B blobs — the dominant production commit size (p50 178 B).
    const TOTAL: usize = 1_024;
    const BLOB: usize = 256;

    let mut group = c.benchmark_group("concurrent_writes");
    group.sample_size(10);

    for &trees in concurrency_sweep() {
        let per_tree = (TOTAL / trees).max(1);
        let actual_total = per_tree * trees;

        group.throughput(Throughput::Elements(actual_total as u64));
        group.bench_function(BenchmarkId::new(backend.name, trees), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().expect("tempdir");
                    let storage = (backend.make)(dir.path());
                    let work: Vec<(SedimentreeId, Vec<VerifiedMeta<LooseCommit>>)> = (0..trees)
                        .map(|ti| {
                            let mut id_bytes = [0u8; 32];
                            id_bytes[..8].copy_from_slice(&(ti as u64).to_be_bytes());
                            let id = SedimentreeId::new(id_bytes);
                            let commits =
                                (0..per_tree).map(|_| seal_commit(rt, id, BLOB)).collect();
                            (id, commits)
                        })
                        .collect();
                    (dir, storage, work)
                },
                |(dir, storage, work)| {
                    rt.block_on(async {
                        let mut handles = Vec::with_capacity(work.len());
                        for (id, commits) in work {
                            let storage = storage.clone();
                            handles.push(tokio::spawn(async move {
                                storage
                                    .save_batch(id, commits, Vec::new())
                                    .await
                                    .unwrap_or_else(|e| panic!("save batch: {e}"));
                            }));
                        }
                        for handle in handles {
                            handle.await.expect("save task panicked");
                        }
                    });
                    // Returned so criterion drops the store (and cleans the
                    // tempdir) *outside* the timed region.
                    (dir, storage)
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ── Production-replica corpus ─────────────────────────────────────────

/// Deterministic xorshift64* PRNG — reproducible replica generation without a
/// `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform `f64` in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Inverse-CDF breakpoints `(cumulative_probability, value)` for the production
/// blob-size distribution (1.71M blobs, 2026-06). 97.9% inline (≤16 KiB), but
/// the >16 KiB tail holds 88% of the bytes.
const BLOB_CDF: &[(f64, f64)] = &[
    (0.0, 34.0),
    (0.10, 125.0),
    (0.25, 151.0),
    (0.50, 187.0),
    (0.75, 271.0),
    (0.90, 639.0),
    (0.95, 3465.0),
    (0.99, 44_233.0),
    (0.999, 653_060.0),
    (0.9999, 3_952_602.0),
    (1.0, 27_297_996.0),
];

/// Inverse-CDF breakpoints for production commits-per-tree (190.6k trees).
const TREE_CDF: &[(f64, f64)] = &[
    (0.0, 1.0),
    (0.50, 2.0),
    (0.75, 2.0),
    (0.90, 3.0),
    (0.99, 48.0),
    (0.999, 1_310.0),
    (0.9999, 9_218.0),
    (1.0, 27_444.0),
];

/// Sample from a piecewise-linear inverse CDF given `u` in `[0, 1)`.
fn sample_cdf(cdf: &[(f64, f64)], u: f64) -> f64 {
    for win in cdf.windows(2) {
        let (p0, v0) = win[0];
        let (p1, v1) = win[1];
        if u < p1 {
            let t = if p1 > p0 { (u - p0) / (p1 - p0) } else { 0.0 };
            return v0 + t * (v1 - v0);
        }
    }
    cdf[cdf.len() - 1].1
}

fn sample_blob_size(rng: &mut Rng) -> usize {
    (sample_cdf(BLOB_CDF, rng.unit()).round() as usize).max(34)
}

fn sample_tree_size(rng: &mut Rng) -> usize {
    (sample_cdf(TREE_CDF, rng.unit()).round() as usize).max(1)
}

/// Populate `storage` with `n_trees` documents whose per-tree commit counts and
/// per-commit blob sizes are drawn from the production distributions (one
/// durable batch per tree). Returns the tree ids.
fn populate_replica<S: Storage<Sendable>>(
    rt: &Runtime,
    storage: &S,
    n_trees: usize,
    seed: u64,
) -> Vec<SedimentreeId> {
    let mut rng = Rng::new(seed);
    let mut ids = Vec::with_capacity(n_trees);

    for ti in 0..n_trees {
        let mut id_bytes = [0u8; 32];
        id_bytes[..8].copy_from_slice(&(ti as u64).to_be_bytes());
        let id = SedimentreeId::new(id_bytes);

        let commits: Vec<_> = (0..sample_tree_size(&mut rng))
            .map(|_| seal_commit(rt, id, sample_blob_size(&mut rng)))
            .collect();
        rt.block_on(storage.save_batch(id, commits, Vec::new()))
            .unwrap_or_else(|e| panic!("replica save_batch: {e}"));
        ids.push(id);
    }

    ids
}

/// Measure one backend's footprint over the production-replica corpus.
fn measure_replica_size<S: Storage<Sendable>>(
    rt: &Runtime,
    backend: &Backend<S>,
    n_trees: usize,
) -> SizeRow {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let storage = (backend.make)(dir.path());
        populate_replica(rt, &storage, n_trees, 0xC0FF_EE00);
    }
    let (apparent, allocated) = disk_usage(dir.path());
    let compacted = backend.compactable.then(|| {
        let db_path = dir.path().join(subduction_redb_storage::DB_FILE_NAME);
        let mut db = redb::Database::create(&db_path).expect("reopen redb for compaction");
        db.compact().expect("compact redb");
        drop(db);
        disk_usage(dir.path()).1
    });
    SizeRow {
        dataset: "prod-replica",
        backend: backend.name,
        apparent,
        allocated,
        compacted,
    }
}

/// Print the production-replica footprint for all three backends — the
/// headline disk-footprint comparison on a realistic blob/tree mix.
fn report_replica_size(rt: &Runtime, n_trees: usize) {
    eprintln!("\nproduction-replica footprint ({n_trees} trees, prod blob/tree distributions):");
    for row in [
        measure_replica_size(rt, &FS, n_trees),
        measure_replica_size(rt, &REDB, n_trees),
        measure_replica_size(rt, &REDB_INLINE, n_trees),
    ] {
        let compacted = row.compacted.map_or_else(|| "—".to_owned(), human);
        eprintln!(
            "  {:<11}  apparent {:>11}  allocated {:>11}  compacted {:>11}",
            row.backend,
            human(row.apparent),
            human(row.allocated),
            compacted,
        );
    }
}

/// Id enumeration (`load_all_sedimentree_ids`) and an aggregate load over the
/// production-replica corpus. The enumeration cost is the metrics-refresh /
/// startup scan at realistic scale (redb `TREES` B+tree scan vs FS two-level
/// dir walk); the load samples the realistic tiny-tree-dominated read mix.
fn bench_replica_reads<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
    n_trees: usize,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = (backend.make)(dir.path());
    let ids = populate_replica(rt, &storage, n_trees, 0xC0FF_EE00);

    let mut group = c.benchmark_group("replica");
    group.sample_size(10);

    group.throughput(Throughput::Elements(n_trees as u64));
    group.bench_function(BenchmarkId::new("enumerate_ids", backend.name), |b| {
        b.iter(|| {
            let got = rt
                .block_on(storage.load_all_sedimentree_ids())
                .unwrap_or_else(|e| panic!("load_all_sedimentree_ids: {e}"));
            assert_eq!(got.len(), n_trees);
            got
        });
    });

    // Aggregate load over an evenly-spaced sample of trees (the realistic,
    // mostly-tiny read mix).
    let sample: Vec<_> = ids
        .iter()
        .step_by((ids.len() / 200).max(1))
        .take(200)
        .copied()
        .collect();
    group.throughput(Throughput::Elements(sample.len() as u64));
    group.bench_function(BenchmarkId::new("load_sample", backend.name), |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut total = 0;
                for &id in &sample {
                    total += storage
                        .load_loose_commits(id)
                        .await
                        .unwrap_or_else(|e| panic!("replica load: {e}"))
                        .len();
                }
                total
            })
        });
    });

    group.finish();
}

// ── Cold-cache reads ──────────────────────────────────────────────────

/// Drop the OS page cache for every file under `path` (`POSIX_FADV_DONTNEED`).
/// Clean pages only — callers must have synced first (the durable saves do).
#[cfg(unix)]
fn evict_cache(path: &Path) {
    use std::os::unix::io::AsRawFd as _;

    use nix::fcntl::{PosixFadviseAdvice, posix_fadvise};

    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return;
    };
    if meta.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                evict_cache(&entry.path());
            }
        }
    } else if meta.is_file()
        && let Ok(file) = std::fs::File::open(path)
    {
        let _ = posix_fadvise(
            file.as_raw_fd(),
            0,
            0,
            PosixFadviseAdvice::POSIX_FADV_DONTNEED,
        );
    }
}

/// Cold-cache bulk load at the production tail tree sizes (p99=48, p99.9=1310,
/// max=27444 commits). Each timed iteration opens the store fresh (cold
/// in-process cache) after evicting the OS page cache, so it reads from disk —
/// the cold path the warm `load/count` group understates and where redb's
/// clustered B+tree scan beats FS's per-item random I/O most. The open+drop
/// lifecycle is inside the timed region (the realistic cold-access cost).
#[cfg(unix)]
fn bench_cold_reads<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    let mut group = c.benchmark_group("load/cold");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    let sizes: &[usize] = if ci_slim() {
        &[48, 1_310]
    } else {
        &[48, 1_310, 27_444]
    };
    for &n in sizes {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        {
            let storage = (backend.make)(&path);
            populate(rt, &storage, n, 256);
        }

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new(backend.name, n), |b| {
            b.iter_batched(
                || evict_cache(&path),
                |()| {
                    let storage = (backend.make)(&path);
                    let commits = rt
                        .block_on(storage.load_loose_commits(id))
                        .unwrap_or_else(|e| panic!("cold load: {e}"));
                    assert_eq!(commits.len(), n);
                    commits
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ── Concurrent reads ──────────────────────────────────────────────────

/// Aggregate read throughput as read concurrency rises — the production
/// profile (~99% of storage ops are reads; the responder fans out to many
/// concurrent dispatches). `T` distinct trees at the realistic p99 size (48
/// commits) are loaded concurrently: redb MVCC readers vs FS parallel reads.
fn bench_concurrent_reads<S: Storage<Sendable> + Clone + Send + Sync + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    const PER_TREE: usize = 48; // production p99 commits/tree

    let mut group = c.benchmark_group("concurrent_reads");
    group.sample_size(10);

    for &readers in concurrency_sweep() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        let ids: Vec<SedimentreeId> = (0..readers)
            .map(|ti| {
                let mut id_bytes = [0u8; 32];
                id_bytes[..8].copy_from_slice(&(ti as u64).to_be_bytes());
                let id = SedimentreeId::new(id_bytes);
                let commits = (0..PER_TREE).map(|_| seal_commit(rt, id, 256)).collect();
                rt.block_on(storage.save_batch(id, commits, Vec::new()))
                    .unwrap_or_else(|e| panic!("populate tree: {e}"));
                id
            })
            .collect();

        group.throughput(Throughput::Elements((readers * PER_TREE) as u64));
        group.bench_function(BenchmarkId::new(backend.name, readers), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(ids.len());
                    for &id in &ids {
                        let storage = storage.clone();
                        handles.push(tokio::spawn(async move {
                            storage
                                .load_loose_commits(id)
                                .await
                                .unwrap_or_else(|e| panic!("concurrent read: {e}"))
                                .len()
                        }));
                    }
                    let mut total = 0;
                    for handle in handles {
                        total += handle.await.expect("read task panicked");
                    }
                    total
                })
            });
        });
    }

    group.finish();
}

// ── Large external blobs ──────────────────────────────────────────────

/// Save + load at the external-blob tail (the 2% of blobs holding 88% of the
/// bytes: ext-mean 169 KB, ext-p99 2.2 MB). Small `N` keeps the multi-MB sizes
/// tractable. `redb-inline` is included to show its buddy-allocator pathology
/// at MB scale — the hybrid streams these as flat files instead.
fn bench_large_external<S: Storage<Sendable> + 'static>(
    c: &mut Criterion,
    rt: &Runtime,
    backend: &Backend<S>,
) {
    const N: usize = 8;

    let mut group = c.benchmark_group("large_external");
    group.sample_size(10);
    let id = SedimentreeId::new(TREE);

    let sizes: &[usize] = if ci_slim() {
        &[169_259]
    } else {
        &[169_259, 2_230_481]
    };
    for &blob in sizes {
        group.throughput(Throughput::Bytes((N * blob) as u64));
        group.bench_function(
            BenchmarkId::new(format!("save/{}", backend.name), blob),
            |b| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().expect("tempdir");
                        let storage = (backend.make)(dir.path());
                        rt.block_on(storage.save_sedimentree_id(id))
                            .unwrap_or_else(|e| panic!("save id: {e}"));
                        let commits: Vec<_> = (0..N).map(|_| seal_commit(rt, id, blob)).collect();
                        (dir, storage, commits)
                    },
                    |(dir, storage, commits)| {
                        rt.block_on(storage.save_batch(id, commits, Vec::new()))
                            .unwrap_or_else(|e| panic!("save batch: {e}"));
                        (dir, storage)
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // Load (warm): the read-back/streaming cost.
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = (backend.make)(dir.path());
        populate(rt, &storage, N, blob);
        group.bench_function(
            BenchmarkId::new(format!("load/{}", backend.name), blob),
            |b| {
                b.iter(|| {
                    let commits = rt
                        .block_on(storage.load_loose_commits(id))
                        .unwrap_or_else(|e| panic!("load: {e}"));
                    assert_eq!(commits.len(), N);
                    commits
                });
            },
        );
    }

    group.finish();
}

fn all_benches(c: &mut Criterion) {
    let rt = Runtime::new().expect("create tokio runtime");

    // Production-readiness gate: measure concurrent durable writes in
    // isolation when requested (skips the size table + read groups, which
    // otherwise pay expensive populate phases before any timing).
    if concurrency_only() {
        bench_concurrent_writes(c, &rt, &FS);
        bench_concurrent_writes(c, &rt, &REDB);
        bench_concurrent_writes(c, &rt, &REDB_INLINE);
        return;
    }

    // Production-replica group (footprint + id enumeration + load) on its own:
    // it populates thousands of trees from the prod distributions (minutes),
    // so it's opt-in rather than part of the default shoot-out.
    if replica_only() {
        let n = replica_trees();
        report_replica_size(&rt, n);
        bench_replica_reads(c, &rt, &FS, n);
        bench_replica_reads(c, &rt, &REDB, n);
        return;
    }

    report_sizes(&rt);

    bench_hydrate_metas(c, &rt, &FS);
    bench_hydrate_metas(c, &rt, &REDB);

    bench_load_count(c, &rt, &FS);
    bench_load_count(c, &rt, &REDB);
    bench_load_count(c, &rt, &REDB_INLINE);
    bench_load_blob_size(c, &rt, &FS);
    bench_load_blob_size(c, &rt, &REDB);
    bench_load_blob_size(c, &rt, &REDB_INLINE);
    // Point reads: fs + redb only — redb-inline shares redb's exact code
    // path for the 256 B records used here.
    bench_point_reads(c, &rt, &FS);
    bench_point_reads(c, &rt, &REDB);
    bench_crossover(c, &rt, &FS);
    bench_crossover(c, &rt, &REDB);
    bench_save(c, &rt, &FS);
    bench_save(c, &rt, &REDB);
    bench_save(c, &rt, &REDB_INLINE);

    // Cold-cache reads (Unix only — uses POSIX_FADV_DONTNEED to evict).
    #[cfg(unix)]
    {
        bench_cold_reads(c, &rt, &FS);
        bench_cold_reads(c, &rt, &REDB);
    }

    bench_concurrent_reads(c, &rt, &FS);
    bench_concurrent_reads(c, &rt, &REDB);

    bench_large_external(c, &rt, &FS);
    bench_large_external(c, &rt, &REDB);
    bench_large_external(c, &rt, &REDB_INLINE);

    bench_concurrent_writes(c, &rt, &FS);
    bench_concurrent_writes(c, &rt, &REDB);
    bench_concurrent_writes(c, &rt, &REDB_INLINE);
}

criterion_group!(benches, all_benches);
criterion_main!(benches);
