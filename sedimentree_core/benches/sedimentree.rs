//! Benchmarks for sedimentree_core operations.
//!
//! Run with: `cargo bench -p sedimentree_core`

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use sedimentree_core::{
    blob::{BlobMeta, Digest},
    commit::CountLeadingZeroBytes,
    depth::DepthMetric,
    Fragment, LooseCommit, Sedimentree,
};

// ============================================================================
// Synthetic Data Generators
// ============================================================================

/// Generate a deterministic digest from a seed.
fn digest_from_seed(seed: u64) -> Digest {
    let mut bytes = [0u8; 32];
    let mut rng = StdRng::seed_from_u64(seed);
    rng.fill(&mut bytes);
    Digest::from(bytes)
}

/// Generate a digest with a specific number of leading zero bytes.
fn digest_with_leading_zeros(zeros: usize, seed: u64) -> Digest {
    let mut bytes = [0u8; 32];
    let mut rng = StdRng::seed_from_u64(seed);
    rng.fill(&mut bytes[zeros..]);
    // First `zeros` bytes are already 0
    Digest::from(bytes)
}

/// Generate synthetic blob metadata.
fn synthetic_blob_meta(seed: u64, size: u64) -> BlobMeta {
    BlobMeta::from_digest_size(digest_from_seed(seed), size)
}

/// Generate a synthetic loose commit with the given number of parents.
fn synthetic_commit(seed: u64, parents: Vec<Digest>) -> LooseCommit {
    let digest = digest_from_seed(seed);
    let blob_meta = synthetic_blob_meta(seed.wrapping_add(1000000), 1024);
    LooseCommit::new(digest, parents, blob_meta)
}

/// Generate a synthetic fragment.
fn synthetic_fragment(
    head_seed: u64,
    boundary_count: usize,
    checkpoint_count: usize,
    leading_zeros: usize,
) -> Fragment {
    let head = digest_with_leading_zeros(leading_zeros, head_seed);
    let boundary: Vec<Digest> = (0..boundary_count)
        .map(|i| digest_with_leading_zeros(leading_zeros, head_seed + 100 + i as u64))
        .collect();
    let checkpoints: Vec<Digest> = (0..checkpoint_count)
        .map(|i| digest_from_seed(head_seed + 200 + i as u64))
        .collect();
    let blob_meta = synthetic_blob_meta(head_seed + 300, 4096);
    Fragment::new(head, boundary, checkpoints, blob_meta)
}

/// Generate a linear chain of commits (each has one parent).
fn linear_commit_chain(count: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(count);
    let mut prev_digest = None;

    for i in 0..count {
        let parents = prev_digest.map(|d| vec![d]).unwrap_or_default();
        let commit = synthetic_commit(base_seed + i as u64, parents);
        prev_digest = Some(commit.digest());
        commits.push(commit);
    }
    commits
}

/// Generate a DAG with merge commits (some commits have multiple parents).
fn merge_heavy_dag(count: usize, merge_frequency: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(count);
    let mut recent_digests: Vec<Digest> = Vec::new();

    for i in 0..count {
        let parents = if i == 0 {
            vec![]
        } else if i % merge_frequency == 0 && recent_digests.len() >= 2 {
            // Merge commit: take last two
            vec![
                recent_digests[recent_digests.len() - 1],
                recent_digests[recent_digests.len() - 2],
            ]
        } else if !recent_digests.is_empty() {
            vec![recent_digests[recent_digests.len() - 1]]
        } else {
            vec![]
        };

        let commit = synthetic_commit(base_seed + i as u64, parents);
        recent_digests.push(commit.digest());
        if recent_digests.len() > 10 {
            recent_digests.remove(0);
        }
        commits.push(commit);
    }
    commits
}

/// Generate a Sedimentree with specified fragment and commit counts.
fn synthetic_sedimentree(
    fragment_count: usize,
    commit_count: usize,
    base_seed: u64,
) -> Sedimentree {
    let fragments: Vec<Fragment> = (0..fragment_count)
        .map(|i| {
            let leading_zeros = (i % 3).min(2); // Vary depth 0, 1, 2
            synthetic_fragment(base_seed + i as u64 * 1000, 2, 5, leading_zeros)
        })
        .collect();

    let commits = linear_commit_chain(commit_count, base_seed + 500000);

    Sedimentree::new(fragments, commits)
}

/// Generate two Sedimentrees with some overlap for diff benchmarks.
fn overlapping_sedimentrees(
    shared_fragments: usize,
    unique_fragments_each: usize,
    shared_commits: usize,
    unique_commits_each: usize,
    base_seed: u64,
) -> (Sedimentree, Sedimentree) {
    // Shared data
    let shared_frags: Vec<Fragment> = (0..shared_fragments)
        .map(|i| synthetic_fragment(base_seed + i as u64 * 1000, 2, 5, i % 3))
        .collect();
    let shared_commits_vec = linear_commit_chain(shared_commits, base_seed + 100000);

    // Tree A unique data
    let a_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
        .map(|i| synthetic_fragment(base_seed + 200000 + i as u64 * 1000, 2, 5, i % 3))
        .collect();
    let a_unique_commits = linear_commit_chain(unique_commits_each, base_seed + 300000);

    // Tree B unique data
    let b_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
        .map(|i| synthetic_fragment(base_seed + 400000 + i as u64 * 1000, 2, 5, i % 3))
        .collect();
    let b_unique_commits = linear_commit_chain(unique_commits_each, base_seed + 500000);

    let mut a_frags = shared_frags.clone();
    a_frags.extend(a_unique_frags);
    let mut a_commits = shared_commits_vec.clone();
    a_commits.extend(a_unique_commits);

    let mut b_frags = shared_frags;
    b_frags.extend(b_unique_frags);
    let mut b_commits = shared_commits_vec;
    b_commits.extend(b_unique_commits);

    (
        Sedimentree::new(a_frags, a_commits),
        Sedimentree::new(b_frags, b_commits),
    )
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_digest_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("digest_hashing");

    for size in [64, 256, 1024, 4096, 16384, 65536] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| Digest::hash(black_box(data)));
        });
    }

    group.finish();
}

fn bench_depth_metric(c: &mut Criterion) {
    let mut group = c.benchmark_group("depth_metric");
    let metric = CountLeadingZeroBytes;

    // Test with various leading zero counts
    for zeros in 0..=4 {
        let digest = digest_with_leading_zeros(zeros, 12345);
        group.bench_with_input(
            BenchmarkId::new("leading_zeros", zeros),
            &digest,
            |b, digest| {
                b.iter(|| metric.to_depth(black_box(*digest)));
            },
        );
    }

    group.finish();
}

fn bench_sedimentree_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_construction");

    for (frag_count, commit_count) in [(10, 10), (100, 100), (1000, 1000)] {
        let fragments: Vec<Fragment> = (0..frag_count)
            .map(|i| synthetic_fragment(i as u64 * 1000, 2, 5, (i % 3).min(2)))
            .collect();
        let commits = linear_commit_chain(commit_count, 500000);

        group.bench_with_input(
            BenchmarkId::new("new", format!("{}f_{}c", frag_count, commit_count)),
            &(fragments.clone(), commits.clone()),
            |b, (f, c)| {
                b.iter(|| Sedimentree::new(black_box(f.clone()), black_box(c.clone())));
            },
        );
    }

    group.finish();
}

fn bench_sedimentree_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_merge");

    for size in [10, 100, 1000] {
        let tree1 = synthetic_sedimentree(size, size, 1);
        let tree2 = synthetic_sedimentree(size, size, 1000000);

        group.bench_with_input(
            BenchmarkId::new("disjoint", size),
            &(tree1.clone(), tree2.clone()),
            |b, (t1, t2)| {
                b.iter(|| {
                    let mut merged = t1.clone();
                    merged.merge(black_box(t2.clone()));
                    merged
                });
            },
        );
    }

    // Overlapping merge
    for size in [10, 100, 500] {
        let (tree1, tree2) = overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);

        group.bench_with_input(
            BenchmarkId::new("overlapping_50pct", size),
            &(tree1.clone(), tree2.clone()),
            |b, (t1, t2)| {
                b.iter(|| {
                    let mut merged = t1.clone();
                    merged.merge(black_box(t2.clone()));
                    merged
                });
            },
        );
    }

    group.finish();
}

fn bench_sedimentree_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_diff");

    // Disjoint trees
    for size in [10, 100, 1000] {
        let tree1 = synthetic_sedimentree(size, size, 1);
        let tree2 = synthetic_sedimentree(size, size, 1000000);

        group.bench_with_input(
            BenchmarkId::new("disjoint", size),
            &(tree1.clone(), tree2.clone()),
            |b, (t1, t2)| {
                b.iter(|| t1.diff(black_box(t2)));
            },
        );
    }

    // Overlapping trees
    for size in [10, 100, 500] {
        let (tree1, tree2) = overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);

        group.bench_with_input(
            BenchmarkId::new("overlapping_50pct", size),
            &(tree1.clone(), tree2.clone()),
            |b, (t1, t2)| {
                b.iter(|| t1.diff(black_box(t2)));
            },
        );
    }

    // Identical trees
    for size in [10, 100, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);

        group.bench_with_input(BenchmarkId::new("identical", size), &tree, |b, t| {
            b.iter(|| t.diff(black_box(t)));
        });
    }

    group.finish();
}

fn bench_sedimentree_diff_remote(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_diff_remote");
    let metric = CountLeadingZeroBytes;

    for size in [10, 100, 500] {
        let (local, remote) = overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);
        let remote_summary = remote.summarize();

        group.bench_with_input(
            BenchmarkId::new("overlapping_50pct", size),
            &(local.clone(), remote_summary.clone()),
            |b, (local, summary)| {
                b.iter(|| local.diff_remote(black_box(summary), &metric));
            },
        );
    }

    group.finish();
}

fn bench_sedimentree_minimize(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_minimize");
    let metric = CountLeadingZeroBytes;

    for size in [10, 100, 500, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);

        group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
            b.iter(|| t.minimize(black_box(&metric)));
        });
    }

    group.finish();
}

fn bench_sedimentree_heads(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_heads");
    let metric = CountLeadingZeroBytes;

    for size in [10, 100, 500, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);

        group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
            b.iter(|| t.heads(black_box(&metric)));
        });
    }

    group.finish();
}

fn bench_sedimentree_summarize(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_summarize");

    for size in [10, 100, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);

        group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
            b.iter(|| t.summarize());
        });
    }

    group.finish();
}

fn bench_sedimentree_add_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("sedimentree_add");

    // Benchmark add_commit
    for size in [10, 100, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);
        let new_commit = synthetic_commit(999999, vec![]);

        group.bench_with_input(
            BenchmarkId::new("add_commit", size),
            &(tree.clone(), new_commit.clone()),
            |b, (t, c)| {
                b.iter(|| {
                    let mut tree = t.clone();
                    tree.add_commit(black_box(c.clone()))
                });
            },
        );
    }

    // Benchmark add_fragment
    for size in [10, 100, 1000] {
        let tree = synthetic_sedimentree(size, size, 1);
        let new_fragment = synthetic_fragment(999999, 2, 5, 1);

        group.bench_with_input(
            BenchmarkId::new("add_fragment", size),
            &(tree.clone(), new_fragment.clone()),
            |b, (t, f)| {
                b.iter(|| {
                    let mut tree = t.clone();
                    tree.add_fragment(black_box(f.clone()))
                });
            },
        );
    }

    group.finish();
}

fn bench_fragment_supports(c: &mut Criterion) {
    let mut group = c.benchmark_group("fragment_supports");
    let metric = CountLeadingZeroBytes;

    // Create fragments at different depths
    for depth in 0..=2 {
        let fragment = synthetic_fragment(1000, 3, 10, depth);

        // Test against fragments at same depth
        let other_fragment = synthetic_fragment(2000, 3, 10, depth);
        let other_summary = other_fragment.summary();

        group.bench_with_input(
            BenchmarkId::new("same_depth", depth),
            &(fragment.clone(), other_summary.clone()),
            |b, (f, s)| {
                b.iter(|| f.supports(black_box(s), &metric));
            },
        );
    }

    group.finish();
}

fn bench_minimal_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("minimal_hash");
    let metric = CountLeadingZeroBytes;

    for size in [10, 100, 500] {
        let tree = synthetic_sedimentree(size, size, 1);

        group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
            b.iter(|| t.minimal_hash(black_box(&metric)));
        });
    }

    group.finish();
}

fn bench_merge_heavy_dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_heavy_dag");
    let metric = CountLeadingZeroBytes;

    // Compare linear vs merge-heavy DAGs
    for size in [100, 500, 1000] {
        // Linear DAG
        let linear_commits = linear_commit_chain(size, 1);
        let linear_tree = Sedimentree::new(vec![], linear_commits);

        group.bench_with_input(
            BenchmarkId::new("linear_minimize", size),
            &linear_tree,
            |b, t| {
                b.iter(|| t.minimize(black_box(&metric)));
            },
        );

        // Merge-heavy DAG (merge every 5 commits)
        let merge_commits = merge_heavy_dag(size, 5, 1);
        let merge_tree = Sedimentree::new(vec![], merge_commits);

        group.bench_with_input(
            BenchmarkId::new("merge_heavy_minimize", size),
            &merge_tree,
            |b, t| {
                b.iter(|| t.minimize(black_box(&metric)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_digest_hashing,
    bench_depth_metric,
    bench_sedimentree_construction,
    bench_sedimentree_merge,
    bench_sedimentree_diff,
    bench_sedimentree_diff_remote,
    bench_sedimentree_minimize,
    bench_sedimentree_heads,
    bench_sedimentree_summarize,
    bench_sedimentree_add_operations,
    bench_fragment_supports,
    bench_minimal_hash,
    bench_merge_heavy_dag,
);

criterion_main!(benches);
