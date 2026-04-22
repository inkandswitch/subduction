//! End-to-end Automerge ingest benchmarks.
//!
//! Fills the gap left by `egwalker.rs`, which measures `build_fragment_store` (the "cheap"
//! metadata phase) but not the full `ingest_automerge` pipeline that includes the expensive
//! `get_changes` byte-extraction phase flagged in `ingest.rs:192` as _"the main bottleneck"_.
//!
//! ## What this bench measures
//!
//! | Bench                        | Measures                                                   |
//! |------------------------------|------------------------------------------------------------|
//! | `get_changes_meta`           | Cheap DAG traversal (baseline for `get_changes` delta)     |
//! | `get_changes`                | Expensive: reconstructs raw bytes for all changes          |
//! | `ingest_automerge`           | Full pipeline: metadata + fragment decomposition + blobs   |
//! | `ingest_automerge_par` (rayon)| Same but with parallel fragment compression               |
//! | `indexed_build_fragment_store`| Indexed access pattern from `IndexedSedimentreeAutomerge` |
//!
//! Each is run across the standard egwalker test vectors (A1..S3). The delta between
//! `get_changes_meta` and `get_changes` is the precise cost of byte extraction on each doc —
//! useful for deciding whether caching the bytes is worth the complexity.

#![allow(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::hint::black_box;

use automerge_sedimentree::{indexed::IndexedSedimentreeAutomerge, ingest::ingest_automerge};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sedimentree_core::{
    collections::Map,
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_bench_support::{
    fixtures::egwalker::{STANDARD, TestVector},
    harness::criterion::default_criterion,
};

/// The sedimentree id under which we ingest each vector — arbitrary but stable.
const fn bench_sedimentree_id() -> SedimentreeId {
    SedimentreeId::new([0xab; 32])
}

/// Benchmark `get_changes_meta`: the cheap DAG-metadata traversal. This is Phase 1 of the
/// pipeline and is the component the `IndexedSedimentreeAutomerge` is built from.
fn bench_get_changes_meta(c: &mut Criterion) {
    let mut group = c.benchmark_group("automerge/get_changes_meta");

    for vector in STANDARD {
        let doc = vector.load_automerge().expect("load vector");

        group.throughput(Throughput::Bytes(vector.bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(vector.name), &doc, |b, doc| {
            b.iter(|| {
                let meta = black_box(doc).get_changes_meta(&[]);
                black_box(meta)
            });
        });
    }

    group.finish();
}

/// Benchmark `get_changes`: Phase 2 byte extraction. Per `ingest.rs:192`, this is _"the main
/// bottleneck"_ and its cost is a direct target for future optimisation (caching, streaming,
/// incremental ingest).
fn bench_get_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("automerge/get_changes");
    // Longer measurement window — this bench has higher variance on large vectors.
    group.sample_size(20);

    for vector in STANDARD {
        let doc = vector.load_automerge().expect("load vector");

        group.throughput(Throughput::Bytes(vector.bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(vector.name), &doc, |b, doc| {
            b.iter(|| {
                let changes = black_box(doc).get_changes(&[]);
                black_box(changes)
            });
        });
    }

    group.finish();
}

/// Full ingest pipeline: metadata → fragment-store → byte extraction → blob compression.
///
/// This is what applications actually pay to onboard an Automerge document into Subduction.
/// The delta vs `get_changes + get_changes_meta` is the cost of our own decomposition work
/// (fragment boundary detection, blob grouping).
fn bench_ingest_automerge(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_automerge");
    group.sample_size(15);

    for vector in STANDARD {
        let doc = vector.load_automerge().expect("load vector");
        let id = bench_sedimentree_id();

        group.throughput(Throughput::Bytes(vector.bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(vector.name),
            &(doc, id),
            |b, (doc, id)| {
                b.iter(|| {
                    let result = ingest_automerge(black_box(doc), black_box(*id)).expect("ingest");
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Parallel ingest pipeline. Only compiled when the `rayon` feature of `automerge_sedimentree`
/// is on. Compares per-vector against the sequential ingest — the speed-up ceiling is the
/// fraction of time spent in fragment compression (which parallelises) vs `get_changes` (which
/// does not).
#[cfg(feature = "rayon")]
fn bench_ingest_automerge_par(c: &mut Criterion) {
    use automerge_sedimentree::ingest::ingest_automerge_par;

    let mut group = c.benchmark_group("ingest_automerge_par");
    group.sample_size(15);

    for vector in STANDARD {
        let doc = vector.load_automerge().expect("load vector");
        let id = bench_sedimentree_id();

        group.throughput(Throughput::Bytes(vector.bytes.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(vector.name),
            &(doc, id),
            |b, (doc, id)| {
                b.iter(|| {
                    let result =
                        ingest_automerge_par(black_box(doc), black_box(*id)).expect("ingest");
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Isolate the indexed-lookup build of the fragment store. This is already partially covered
/// by `egwalker.rs::bench_build_fragment_store`, but here we run it against the same full-doc
/// input as the other `ingest_*` benches so the time comparison is apples-to-apples.
fn bench_indexed_build_fragment_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_build_fragment_store");

    for vector in STANDARD {
        let doc = vector.load_automerge().expect("load vector");
        let metadata = doc.get_changes_meta(&[]);
        let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
        let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();

        group.throughput(Throughput::Elements(metadata.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(vector.name),
            &(store, heads),
            |b, (store, heads)| {
                b.iter(|| {
                    let mut known: Map<CommitId, FragmentState<_>> = Map::new();
                    let fresh = black_box(store)
                        .build_fragment_store(black_box(heads), &mut known, &CountLeadingZeroBytes)
                        .expect("build_fragment_store");
                    black_box((fresh.len(), known.len()))
                });
            },
        );
    }

    group.finish();
}

/// Report one-time per-vector metadata so baseline captures can record how the test-vectors'
/// sizes relate to the wall-clock numbers above.
///
/// This is not a bench in the timing sense — it just logs info from a single-iteration run.
/// Criterion will still include it in the output under the `info` group.
#[allow(dead_code)]
fn report_vector_stats() {
    for vector in STANDARD {
        if let Ok(doc) = vector.load_automerge() {
            let meta = doc.get_changes_meta(&[]);
            let heads = doc.get_heads();

            eprintln!(
                "[ingest bench] {:>3}: compressed={:>7} bytes, changes={:>6}, heads={:>3}",
                vector.name,
                vector.bytes.len(),
                meta.len(),
                heads.len(),
            );
        }
    }

    // Avoid unused-import warnings when this helper isn't invoked.
    let _ = TestVector {
        name: "",
        bytes: &[],
    };
}

#[cfg(feature = "rayon")]
criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_get_changes_meta,
        bench_get_changes,
        bench_ingest_automerge,
        bench_ingest_automerge_par,
        bench_indexed_build_fragment_store,
}

#[cfg(not(feature = "rayon"))]
criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_get_changes_meta,
        bench_get_changes,
        bench_ingest_automerge,
        bench_indexed_build_fragment_store,
}

criterion_main!(benches);
