//! Benchmarks for sedimentree fragment strategy using egwalker paper test vectors.
//!
//! These benchmarks use real Automerge documents from the egwalker paper to measure
//! sedimentree's fragment strategy performance on realistic workloads.

#![allow(clippy::expect_used)]
//!
//! Test vectors:
//! - A1/A2: Automerge editing traces
//! - C1/C2: Collaborative editing traces
//! - S1/S2/S3: Sequential editing traces
//!
//! Run with: `cargo bench -p automerge_sedimentree`
//!
//! Note: Full fragment building is very expensive for large documents.
//! These benchmarks focus on document loading and sedimentree operations using
//! synthetic fragments scaled to match real document characteristics.

#![allow(missing_docs, unreachable_pub)]

use std::{collections::BTreeSet, hint::black_box, num::NonZero};

use automerge::Automerge;
use automerge_sedimentree::indexed::{IndexedSedimentreeAutomerge, OwnedParents};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_pprof::criterion::{Output, PProfProfiler};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use sedimentree_core::{
    blob::BlobMeta,
    collections::Map,
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    crypto::{digest::Digest, fingerprint::FingerprintSeed},
    depth::{CountTrailingZerosInBase, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::Sedimentree,
};

/// Test vector metadata.
struct TestVector {
    name: &'static str,
    bytes: &'static [u8],
}

macro_rules! include_test_vector {
    ($name:literal) => {
        TestVector {
            name: $name,
            bytes: include_bytes!(concat!("../test-vectors/", $name, ".am")),
        }
    };
}

/// All test vectors from the egwalker paper.
/// Using compressed versions for faster load times.
static TEST_VECTORS: &[TestVector] = &[
    include_test_vector!("A1"),
    include_test_vector!("A2"),
    include_test_vector!("C1"),
    include_test_vector!("C2"),
    include_test_vector!("S1"),
    include_test_vector!("S2"),
    include_test_vector!("S3"),
];

/// Load an Automerge document from bytes.
fn load_automerge(bytes: &[u8]) -> Automerge {
    #[allow(clippy::expect_used)]
    Automerge::load(bytes).expect("failed to load automerge document")
}

/// Generate a random digest with specified leading zero bytes.
#[allow(clippy::indexing_slicing)]
fn random_commit_id_with_depth(rng: &mut SmallRng, depth: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);

    // Set leading zeros based on depth
    // SAFETY: zeros is clamped to 31, so all indices are valid for a 32-byte array
    let zeros = depth.min(31) as usize;
    bytes[..zeros].fill(0);
    if zeros < 32 {
        // Ensure first non-zero byte is actually non-zero
        if bytes[zeros] == 0 {
            bytes[zeros] = 1;
        }
    }

    CommitId::new(bytes)
}

/// Generate synthetic fragments matching the expected distribution for a document.
///
/// ## Post-#122 distribution
///
/// Under `CountLeadingZeroBytes`, any commit with nonzero depth is a fragment boundary
/// (PR #122 removed the `MAX_STRATA_DEPTH = 2` gate). For uniformly-distributed commit ids
/// the probability of depth `d` is `256⁻ᵈ · (1 - 256⁻¹)`, so the realistic fragment
/// distribution is:
///
/// | Depth | P(depth = d)      | Share of fragments |
/// |-------|-------------------|--------------------|
/// | 1     | ≈ 255/256         | ~99.6%             |
/// | 2     | ≈ 255/256²        | ~0.39%             |
/// | 3+    | < 256⁻³           | negligible         |
///
/// Fragment count is estimated as `changes / 256` (average distance between depth-1
/// commits). Pre-#122 this bench modelled ~255/256 of fragments at **depth 0** — which
/// post-#122 aren't fragment boundaries at all. This was off-distribution and is now fixed.
fn generate_synthetic_fragments(change_count: usize, seed: u64) -> Vec<Fragment> {
    let metric = CountLeadingZeroBytes;
    let mut rng = SmallRng::seed_from_u64(seed);

    // Use a deterministic SedimentreeId for benchmarks
    #[allow(clippy::cast_possible_truncation)]
    let sedimentree_id = SedimentreeId::new([seed as u8; 32]);

    // Estimate fragment count (roughly one fragment per 256 commits on average)
    let fragment_count = (change_count / 256).max(1);

    let mut fragments = Vec::with_capacity(fragment_count);

    for _ in 0..fragment_count {
        // Realistic post-#122 distribution:
        //   depth 1 with ≈ 255/256
        //   depth 2 with ≈ 1/256
        //   depth 3+ vanishingly rare (cap at 4)
        let depth: u32 = {
            let mut d = 1_u32;
            while d < 4 {
                let extend: u8 = rng.r#gen();
                if extend == 0 {
                    d += 1;
                } else {
                    break;
                }
            }
            d
        };

        let head = random_commit_id_with_depth(&mut rng, depth);

        // Generate 1-3 boundary commits at same depth (boundary must be at-or-above depth
        // of head, and same-depth is the natural common case).
        let boundary_count = rng.gen_range(1..=3);
        let boundary: BTreeSet<_> = (0..boundary_count)
            .map(|_| random_commit_id_with_depth(&mut rng, depth))
            .collect();

        // Generate checkpoints (commits at higher depths within this fragment). Note
        // these are checkpoints, not fragment heads; depth-0 checkpoints are fine.
        let checkpoint_count = rng.gen_range(0..=10);
        let checkpoints: Vec<_> = (0..checkpoint_count)
            .map(|_| random_commit_id_with_depth(&mut rng, depth.saturating_sub(1)))
            .collect();

        // Blob size: average commit is ~100 bytes, fragment covers ~256 commits
        let blob_size = 256 * 100;
        let blob_digest = Digest::force_from_bytes({
            let mut b = [0u8; 32];
            rng.fill(&mut b);
            b
        });
        let blob_meta = BlobMeta::from_digest_size(blob_digest, blob_size);

        fragments.push(Fragment::new(
            sedimentree_id,
            head,
            boundary,
            &checkpoints,
            blob_meta,
        ));

        // Verify depth is as expected
        debug_assert_eq!(metric.to_depth(head).0, depth);
    }

    fragments
}

/// Generate a sedimentree matching document characteristics.
fn generate_sedimentree_for_doc(change_count: usize, seed: u64) -> Sedimentree {
    let fragments = generate_synthetic_fragments(change_count, seed);
    Sedimentree::new(fragments, vec![])
}

/// Depth metric types for benchmarking.
#[derive(Clone, Copy)]
enum MetricType {
    LeadingZeros,
    Base10,
}

impl MetricType {
    /// Expected fragment rate: 1 in N commits becomes a fragment boundary.
    /// - `LeadingZeros`: 1/256 chance per byte
    /// - `Base10`: 1/10 chance per trailing zero
    const fn fragment_rate(self) -> usize {
        match self {
            MetricType::LeadingZeros => 256,
            MetricType::Base10 => 10,
        }
    }
}

/// Generate synthetic fragments for a specific depth metric.
fn generate_fragments_for_metric(
    change_count: usize,
    metric_type: MetricType,
    seed: u64,
) -> Vec<Fragment> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let fragment_rate = metric_type.fragment_rate();

    // Use a deterministic SedimentreeId for benchmarks
    #[allow(clippy::cast_possible_truncation)]
    let sedimentree_id = SedimentreeId::new([seed as u8; 32]);

    // Estimate fragment count based on metric's probability distribution
    let fragment_count = (change_count / fragment_rate).max(1);

    let mut fragments = Vec::with_capacity(fragment_count);

    for _ in 0..fragment_count {
        // Distribute depths based on metric type
        let depth = match metric_type {
            MetricType::LeadingZeros => {
                // P(depth=d) ~ 1/256^d
                let r: u8 = rng.gen_range(0..=255);
                if r == 0 {
                    let r2: u8 = rng.gen_range(0..=255);
                    if r2 == 0 {
                        2
                    } else {
                        1
                    }
                } else {
                    0
                }
            }
            MetricType::Base10 => {
                // P(depth=d) ~ 1/10^d
                let r: u8 = rng.gen_range(0..10);
                if r == 0 {
                    let r2: u8 = rng.gen_range(0..10);
                    if r2 == 0 {
                        2
                    } else {
                        1
                    }
                } else {
                    0
                }
            }
        };

        let head = random_commit_id_with_depth(&mut rng, depth);

        let boundary_count = rng.gen_range(1..=3);
        let boundary: BTreeSet<_> = (0..boundary_count)
            .map(|_| random_commit_id_with_depth(&mut rng, depth))
            .collect();

        let checkpoint_count = rng.gen_range(0..=10);
        let checkpoints: Vec<_> = (0..checkpoint_count)
            .map(|_| random_commit_id_with_depth(&mut rng, depth + 1))
            .collect();

        // Blob size scales with fragment rate (larger fragments for sparser metrics)
        let blob_size = (fragment_rate * 100) as u64;
        let blob_digest = Digest::force_from_bytes({
            let mut b = [0u8; 32];
            rng.fill(&mut b);
            b
        });
        let blob_meta = BlobMeta::from_digest_size(blob_digest, blob_size);

        fragments.push(Fragment::new(
            sedimentree_id,
            head,
            boundary,
            &checkpoints,
            blob_meta,
        ));
    }

    fragments
}

/// Generate loose commits that don't fit into fragments.
fn generate_loose_commits(count: usize, seed: u64) -> Vec<LooseCommit> {
    let mut rng = SmallRng::seed_from_u64(seed);

    // Use a deterministic SedimentreeId for benchmarks
    #[allow(clippy::cast_possible_truncation)]
    let sedimentree_id = SedimentreeId::new([seed as u8; 32]);

    (0..count)
        .map(|_| {
            let parent_count = rng.gen_range(0..=2);
            let parents: BTreeSet<CommitId> = (0..parent_count)
                .map(|_| random_commit_id_with_depth(&mut rng, 0))
                .collect();
            let head = random_commit_id_with_depth(&mut rng, 0);
            let blob_digest = Digest::force_from_bytes({
                let mut b = [0u8; 32];
                rng.fill(&mut b);
                b
            });
            let blob_meta = BlobMeta::from_digest_size(blob_digest, 100);
            LooseCommit::new(sedimentree_id, head, parents, blob_meta)
        })
        .collect()
}

/// Generate a sedimentree for a specific metric type.
fn generate_sedimentree_for_metric(
    change_count: usize,
    metric_type: MetricType,
    seed: u64,
) -> Sedimentree {
    let fragments = generate_fragments_for_metric(change_count, metric_type, seed);
    let fragment_rate = metric_type.fragment_rate();

    // Loose commits are those between fragment boundaries
    // On average, fragment_rate - 1 loose commits per fragment
    let loose_count = (change_count % fragment_rate).min(change_count / 4);
    let loose_commits = generate_loose_commits(loose_count, seed + 1000);

    Sedimentree::new(fragments, loose_commits)
}

/// Benchmark minimizing sedimentrees scaled to document sizes.
fn bench_minimize(c: &mut Criterion) {
    let mut group = c.benchmark_group("minimize");
    let metric = CountLeadingZeroBytes;

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);
        let fragment_count = tree.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(BenchmarkId::from_parameter(tv.name), &tree, |b, tree| {
            b.iter(|| tree.minimize(black_box(&metric)));
        });
    }

    group.finish();
}

/// Benchmark creating fingerprint summaries of sedimentrees.
fn bench_fingerprint_summarize(c: &mut Criterion) {
    let mut group = c.benchmark_group("fingerprint_summarize");
    let seed = FingerprintSeed::new(42, 43);

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);
        let fragment_count = tree.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(BenchmarkId::from_parameter(tv.name), &tree, |b, tree| {
            b.iter(|| tree.fingerprint_summarize(black_box(&seed)));
        });
    }

    group.finish();
}

/// Benchmark `diff_remote_fingerprints` (comparing against a fingerprint summary).
fn bench_diff_remote_fingerprints(c: &mut Criterion) {
    let mut group = c.benchmark_group("diff_remote_fingerprints");
    let seed = FingerprintSeed::new(42, 43);

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);
        let summary = tree.fingerprint_summarize(&seed);
        let fragment_count = tree.fragments().count() as u64;

        // Diff against empty summary
        let empty_summary = Sedimentree::default().fingerprint_summarize(&seed);

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(
            BenchmarkId::new("vs_empty", tv.name),
            &(tree.clone(), empty_summary),
            |b, (tree, summary)| {
                b.iter(|| tree.diff_remote_fingerprints(black_box(summary)));
            },
        );

        // Diff against self summary
        group.bench_with_input(
            BenchmarkId::new("vs_self", tv.name),
            &(tree, summary),
            |b, (tree, summary)| {
                b.iter(|| tree.diff_remote_fingerprints(black_box(summary)));
            },
        );
    }

    group.finish();
}

/// Benchmark computing heads of sedimentrees.
fn bench_heads(c: &mut Criterion) {
    let mut group = c.benchmark_group("heads");
    let metric = CountLeadingZeroBytes;

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);
        let fragment_count = tree.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(BenchmarkId::from_parameter(tv.name), &tree, |b, tree| {
            b.iter(|| tree.heads(black_box(&metric)));
        });
    }

    group.finish();
}

/// Benchmark diffing two sedimentrees (simulating sync).
fn bench_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("diff");

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);

        // Diff against empty tree (worst case - everything is different)
        let empty_tree = Sedimentree::default();
        let fragment_count = tree.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(
            BenchmarkId::new("vs_empty", tv.name),
            &(tree.clone(), empty_tree),
            |b, (full, empty)| {
                b.iter(|| full.diff(black_box(empty)));
            },
        );

        // Diff against self (best case - nothing different)
        group.bench_with_input(BenchmarkId::new("vs_self", tv.name), &tree, |b, tree| {
            b.iter(|| tree.diff(black_box(tree)));
        });

        // Diff against partial tree (50% overlap)
        let partial_tree = generate_sedimentree_for_doc(change_count / 2, 99);
        group.bench_with_input(
            BenchmarkId::new("vs_partial", tv.name),
            &(tree.clone(), partial_tree),
            |b, (full, partial)| {
                b.iter(|| full.diff(black_box(partial)));
            },
        );
    }

    group.finish();
}

/// Benchmark minimal hash computation.
fn bench_minimal_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("minimal_hash");
    let metric = CountLeadingZeroBytes;

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree = generate_sedimentree_for_doc(change_count, 42);
        let fragment_count = tree.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count));
        group.bench_with_input(BenchmarkId::from_parameter(tv.name), &tree, |b, tree| {
            b.iter(|| tree.minimal_hash(black_box(&metric)));
        });
    }

    group.finish();
}

/// Benchmark merging two sedimentrees.
fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge");

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();
        let tree1 = generate_sedimentree_for_doc(change_count, 42);
        let tree2 = generate_sedimentree_for_doc(change_count, 99);
        let fragment_count = tree1.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count * 2));
        group.bench_with_input(
            BenchmarkId::from_parameter(tv.name),
            &(tree1, tree2),
            |b, (t1, t2)| {
                b.iter_batched(
                    || (t1.clone(), t2.clone()),
                    |(mut merged, other)| {
                        merged.merge(black_box(other));
                        merged
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark minimize with different depth metrics.
fn bench_minimize_by_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("minimize_by_metric");

    let leading_zeros = CountLeadingZeroBytes;
    #[allow(clippy::expect_used)]
    let base10 = CountTrailingZerosInBase::new(NonZero::new(10).expect("10 is non-zero"));

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();

        // LeadingZeros metric
        let tree_lz = generate_sedimentree_for_metric(change_count, MetricType::LeadingZeros, 42);
        let fragment_count_lz = tree_lz.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_lz));
        group.bench_with_input(
            BenchmarkId::new("leading_zeros", tv.name),
            &tree_lz,
            |b, tree| {
                b.iter(|| tree.minimize(black_box(&leading_zeros)));
            },
        );

        // Base10 metric
        let tree_b10 = generate_sedimentree_for_metric(change_count, MetricType::Base10, 42);
        let fragment_count_b10 = tree_b10.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_b10));
        group.bench_with_input(BenchmarkId::new("base10", tv.name), &tree_b10, |b, tree| {
            b.iter(|| tree.minimize(black_box(&base10)));
        });
    }

    group.finish();
}

/// Benchmark heads computation with different depth metrics.
fn bench_heads_by_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("heads_by_metric");

    let leading_zeros = CountLeadingZeroBytes;
    #[allow(clippy::expect_used)]
    let base10 = CountTrailingZerosInBase::new(NonZero::new(10).expect("10 is non-zero"));

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();

        // LeadingZeros metric
        let tree_lz = generate_sedimentree_for_metric(change_count, MetricType::LeadingZeros, 42);
        let fragment_count_lz = tree_lz.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_lz));
        group.bench_with_input(
            BenchmarkId::new("leading_zeros", tv.name),
            &tree_lz,
            |b, tree| {
                b.iter(|| tree.heads(black_box(&leading_zeros)));
            },
        );

        // Base10 metric
        let tree_b10 = generate_sedimentree_for_metric(change_count, MetricType::Base10, 42);
        let fragment_count_b10 = tree_b10.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_b10));
        group.bench_with_input(BenchmarkId::new("base10", tv.name), &tree_b10, |b, tree| {
            b.iter(|| tree.heads(black_box(&base10)));
        });
    }

    group.finish();
}

/// Benchmark `minimal_hash` computation with different depth metrics.
fn bench_minimal_hash_by_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("minimal_hash_by_metric");

    let leading_zeros = CountLeadingZeroBytes;
    #[allow(clippy::expect_used)]
    let base10 = CountTrailingZerosInBase::new(NonZero::new(10).expect("10 is non-zero"));

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let change_count = doc.get_changes(&[]).len();

        // LeadingZeros metric
        let tree_lz = generate_sedimentree_for_metric(change_count, MetricType::LeadingZeros, 42);
        let fragment_count_lz = tree_lz.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_lz));
        group.bench_with_input(
            BenchmarkId::new("leading_zeros", tv.name),
            &tree_lz,
            |b, tree| {
                b.iter(|| tree.minimal_hash(black_box(&leading_zeros)));
            },
        );

        // Base10 metric
        let tree_b10 = generate_sedimentree_for_metric(change_count, MetricType::Base10, 42);
        let fragment_count_b10 = tree_b10.fragments().count() as u64;

        group.throughput(Throughput::Elements(fragment_count_b10));
        group.bench_with_input(BenchmarkId::new("base10", tv.name), &tree_b10, |b, tree| {
            b.iter(|| tree.minimal_hash(black_box(&base10)));
        });
    }

    group.finish();
}

/// Benchmark `build_fragment_store` — the real automerge→sedimentree
/// decomposition pipeline using `get_changes_meta` + `from_metadata`.
fn bench_build_fragment_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_fragment_store");

    for tv in TEST_VECTORS {
        let doc = load_automerge(tv.bytes);
        let metadata = doc.get_changes_meta(&[]);
        let change_count = metadata.len() as u64;

        // Pre-build the index outside the bench loop (we're benchmarking
        // the fragment building, not the indexing).
        let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
        let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();

        group.throughput(Throughput::Elements(change_count));
        group.bench_with_input(
            BenchmarkId::from_parameter(tv.name),
            &(store, heads),
            |b, (store, heads)| {
                b.iter(|| {
                    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();
                    store
                        .build_fragment_store(black_box(heads), &mut known, &CountLeadingZeroBytes)
                        .expect("build_fragment_store");
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(None)));
    targets =
        bench_build_fragment_store,
        bench_minimize,
        bench_minimize_by_metric,
        bench_fingerprint_summarize,
        bench_diff_remote_fingerprints,
        bench_heads,
        bench_heads_by_metric,
        bench_diff,
        bench_minimal_hash,
        bench_minimal_hash_by_metric,
        bench_merge,
}

criterion_main!(benches);
