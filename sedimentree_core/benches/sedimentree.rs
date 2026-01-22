//! Benchmarks for `sedimentree_core` operations.
//!
//! Run with: `cargo bench -p sedimentree_core`
//!
//! ## Benchmark Philosophy
//!
//! | Goal                     | Approach |
//! |--------------------------|----------|
//! | Isolate algorithm cost   | Use `iter_batched` to separate setup (cloning) from measurement |
//! | Test realistic scenarios | Include both typical and worst-case workloads |
//! | Enable scaling analysis  | Use `Throughput::Elements` to understand O(n) behavior |
//! | Document expectations    | Each benchmark group states what it measures and expected complexity |

#![allow(missing_docs, unreachable_pub)]

use criterion::{criterion_group, criterion_main};

mod generators {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use sedimentree_core::{
        blob::{BlobMeta, Digest},
        fragment::Fragment,
        loose_commit::LooseCommit,
        sedimentree::Sedimentree,
    };

    /// Generate a deterministic digest from a seed.
    pub(super) fn digest_from_seed(seed: u64) -> Digest {
        let mut bytes = [0u8; 32];
        let mut rng = StdRng::seed_from_u64(seed);
        rng.fill(&mut bytes);
        Digest::from(bytes)
    }

    /// Generate a digest with a specific number of leading zero bytes.
    /// The byte immediately after the zeros is guaranteed to be non-zero
    /// to ensure exact depth control.
    pub(super) fn digest_with_leading_zeros(zeros: usize, seed: u64) -> Digest {
        let mut bytes = [0u8; 32];
        let mut rng = StdRng::seed_from_u64(seed);
        rng.fill(
            #[allow(clippy::expect_used)]
            bytes
                .get_mut(zeros..)
                .expect("should have slice with leading zeros"),
        );
        // Ensure the first non-zero byte is actually non-zero for precise depth control
        #[allow(clippy::indexing_slicing)]
        if zeros < 32 && bytes[zeros] == 0 {
            bytes[zeros] = 1;
        }
        Digest::from(bytes)
    }

    /// Generate synthetic blob metadata.
    pub(super) fn synthetic_blob_meta(seed: u64, size: u64) -> BlobMeta {
        BlobMeta::from_digest_size(digest_from_seed(seed), size)
    }

    /// Generate a synthetic loose commit with the given number of parents.
    pub(super) fn synthetic_commit(seed: u64, parents: Vec<Digest>) -> LooseCommit {
        let digest = digest_from_seed(seed);
        let blob_meta = synthetic_blob_meta(seed.wrapping_add(1_000_000), 1024);
        LooseCommit::new(digest, parents, blob_meta)
    }

    /// Generate a synthetic fragment with configurable complexity.
    pub(super) fn synthetic_fragment(
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
    pub(super) fn linear_commit_chain(count: usize, base_seed: u64) -> Vec<LooseCommit> {
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
    #[allow(clippy::indexing_slicing)]
    pub(super) fn merge_heavy_dag(
        count: usize,
        merge_frequency: usize,
        base_seed: u64,
    ) -> Vec<LooseCommit> {
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

    /// Generate a wide DAG with many independent heads (simulates many concurrent writers).
    pub(super) fn wide_dag(
        width: usize,
        depth_per_branch: usize,
        base_seed: u64,
    ) -> Vec<LooseCommit> {
        let mut commits = Vec::with_capacity(width * depth_per_branch);

        for branch in 0..width {
            let branch_seed = base_seed + (branch as u64 * 10_000);
            let mut prev_digest = None;

            for i in 0..depth_per_branch {
                let parents = prev_digest.map(|d| vec![d]).unwrap_or_default();
                let commit = synthetic_commit(branch_seed + i as u64, parents);
                prev_digest = Some(commit.digest());
                commits.push(commit);
            }
        }
        commits
    }

    /// Generate a Sedimentree with specified fragment and commit counts.
    pub(super) fn synthetic_sedimentree(
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

        let commits = linear_commit_chain(commit_count, base_seed + 500_000);

        Sedimentree::new(fragments, commits)
    }

    /// Generate a Sedimentree with variable fragment complexity.
    pub(super) fn synthetic_sedimentree_varied(
        fragment_count: usize,
        commit_count: usize,
        boundaries_per_fragment: usize,
        checkpoints_per_fragment: usize,
        base_seed: u64,
    ) -> Sedimentree {
        let fragments: Vec<Fragment> = (0..fragment_count)
            .map(|i| {
                let leading_zeros = (i % 3).min(2);
                synthetic_fragment(
                    base_seed + i as u64 * 1000,
                    boundaries_per_fragment,
                    checkpoints_per_fragment,
                    leading_zeros,
                )
            })
            .collect();

        let commits = linear_commit_chain(commit_count, base_seed + 500_000);

        Sedimentree::new(fragments, commits)
    }

    /// Generate two Sedimentrees with some overlap for diff benchmarks.
    pub(super) fn overlapping_sedimentrees(
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
        let shared_commits_vec = linear_commit_chain(shared_commits, base_seed + 100_000);

        // Tree A unique data
        let a_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
            .map(|i| synthetic_fragment(base_seed + 200_000 + i as u64 * 1000, 2, 5, i % 3))
            .collect();
        let a_unique_commits = linear_commit_chain(unique_commits_each, base_seed + 300_000);

        // Tree B unique data
        let b_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
            .map(|i| synthetic_fragment(base_seed + 400_000 + i as u64 * 1000, 2, 5, i % 3))
            .collect();
        let b_unique_commits = linear_commit_chain(unique_commits_each, base_seed + 500_000);

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
}

mod digest {
    use std::hint::black_box;

    use criterion::{BenchmarkId, Criterion, Throughput};
    use sedimentree_core::{blob::Digest, commit::CountLeadingZeroBytes, depth::DepthMetric};

    use super::generators::{digest_from_seed, digest_with_leading_zeros};

    /// Benchmark BLAKE3 digest hashing.
    ///
    /// **Intent**: Measure raw hashing throughput at various payload sizes.
    /// This is a foundational operation used throughout the system.
    ///
    /// **Expected complexity**: O(n) where n is input size.
    /// BLAKE3 is designed for high throughput; expect ~1GB/s on modern CPUs.
    pub fn bench_digest_hashing(c: &mut Criterion) {
        let mut group = c.benchmark_group("digest_hashing");

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        for size in [32, 64, 256, 1024, 4096, 16384, 65536] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
                b.iter(|| Digest::hash(black_box(data)));
            });
        }

        group.finish();
    }

    /// Benchmark depth metric calculation (counting leading zero bytes).
    ///
    /// **Intent**: Measure the cost of determining a digest's stratum depth.
    /// This is called frequently during diff/merge operations.
    ///
    /// **Expected complexity**: O(1) - simple byte scan, typically finds non-zero quickly.
    pub fn bench_depth_metric(c: &mut Criterion) {
        let mut group = c.benchmark_group("depth_metric");
        let metric = CountLeadingZeroBytes;

        // Test with various leading zero counts including worst-case
        for zeros in [0, 1, 2, 4, 8, 16] {
            let digest = digest_with_leading_zeros(zeros.min(31), 12345);
            group.bench_with_input(
                BenchmarkId::new("leading_zeros", zeros),
                &digest,
                |b, digest| {
                    b.iter(|| metric.to_depth(black_box(*digest)));
                },
            );
        }

        // Random digest for average-case
        group.bench_function("random_digest", |b| {
            let digest = digest_from_seed(99999);
            b.iter(|| metric.to_depth(black_box(digest)));
        });

        group.finish();
    }
}

mod sedimentree {
    use std::hint::black_box;

    use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
    use sedimentree_core::{
        commit::CountLeadingZeroBytes, fragment::Fragment, loose_commit::LooseCommit,
        sedimentree::Sedimentree,
    };

    use super::generators::{
        linear_commit_chain, overlapping_sedimentrees, synthetic_commit, synthetic_fragment,
        synthetic_sedimentree, synthetic_sedimentree_varied,
    };

    /// Benchmark Sedimentree construction from fragments and commits.
    ///
    /// **Intent**: Measure the cost of building a new Sedimentree from raw parts.
    /// This occurs during hydration from storage and after sync operations.
    ///
    /// **Expected complexity**: O(n) where n = fragments + commits.
    pub fn bench_construction(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_construction");

        for (frag_count, commit_count) in [(10, 10), (100, 100), (1000, 1000), (5000, 5000)] {
            let total = frag_count + commit_count;
            group.throughput(Throughput::Elements(total as u64));

            group.bench_with_input(
                BenchmarkId::new("new", format!("{frag_count}f_{commit_count}c")),
                &(frag_count, commit_count),
                |b, &(fc, cc)| {
                    b.iter_batched(
                        || {
                            let fragments: Vec<Fragment> = (0..fc)
                                .map(|i| synthetic_fragment(i as u64 * 1000, 2, 5, (i % 3).min(2)))
                                .collect();
                            let commits = linear_commit_chain(cc, 500_000);
                            (fragments, commits)
                        },
                        |(f, c)| Sedimentree::new(black_box(f), black_box(c)),
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark merging two Sedimentrees.
    ///
    /// **Intent**: Measure the cost of combining two trees (e.g., after receiving remote data).
    /// Uses `iter_batched` to isolate merge cost from cloning cost.
    ///
    /// **Expected complexity**: O(n + m) where n, m are the sizes of the two trees.
    /// Set operations on fragments and commits should be roughly linear.
    pub fn bench_merge(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_merge");

        // Disjoint trees (no overlap - worst case for output size)
        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let tree1 = synthetic_sedimentree(size, size, 1);
            let tree2 = synthetic_sedimentree(size, size, 1_000_000);

            group.bench_with_input(
                BenchmarkId::new("disjoint", size),
                &(tree1, tree2),
                |b, (t1, t2)| {
                    b.iter_batched(
                        || (t1.clone(), t2.clone()),
                        |(mut merged, other)| {
                            merged.merge(black_box(other));
                            merged
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        // 50% overlapping trees (typical sync scenario)
        for size in [10, 100, 500, 2000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let (tree1, tree2) =
                overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);

            group.bench_with_input(
                BenchmarkId::new("overlapping_50pct", size),
                &(tree1, tree2),
                |b, (t1, t2)| {
                    b.iter_batched(
                        || (t1.clone(), t2.clone()),
                        |(mut merged, other)| {
                            merged.merge(black_box(other));
                            merged
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        // Identical trees (best case - no new data)
        for size in [100, 1000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::new("identical", size), &tree, |b, t| {
                b.iter_batched(
                    || (t.clone(), t.clone()),
                    |(mut merged, other)| {
                        merged.merge(black_box(other));
                        merged
                    },
                    BatchSize::SmallInput,
                );
            });
        }

        group.finish();
    }

    /// Benchmark computing diffs between two local Sedimentrees.
    ///
    /// **Intent**: Measure the cost of finding what data one tree has that another lacks.
    /// This is the core operation for determining what to sync.
    ///
    /// **Expected complexity**: O(n + m) for set difference operations.
    pub fn bench_diff(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_diff");

        // Disjoint trees (maximum diff size)
        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let tree1 = synthetic_sedimentree(size, size, 1);
            let tree2 = synthetic_sedimentree(size, size, 1_000_000);

            group.bench_with_input(
                BenchmarkId::new("disjoint", size),
                &(tree1, tree2),
                |b, (t1, t2)| {
                    b.iter(|| t1.diff(black_box(t2)));
                },
            );
        }

        // Overlapping trees (typical scenario)
        for size in [10, 100, 500, 2000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let (tree1, tree2) =
                overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);

            group.bench_with_input(
                BenchmarkId::new("overlapping_50pct", size),
                &(tree1, tree2),
                |b, (t1, t2)| {
                    b.iter(|| t1.diff(black_box(t2)));
                },
            );
        }

        // Identical trees (best case - empty diff)
        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64 * 2));

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::new("identical", size), &tree, |b, t| {
                b.iter(|| t.diff(black_box(t)));
            });
        }

        group.finish();
    }

    /// Benchmark computing diff against a remote summary (metadata-only diff).
    ///
    /// **Intent**: Measure the cost of the primary sync operation - determining what
    /// we have that a remote peer lacks, using only their summary (no full tree).
    ///
    /// **Expected complexity**: O(n) where n is local tree size, since we scan local
    /// data against the summary.
    pub fn bench_diff_remote(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_diff_remote");
        let metric = CountLeadingZeroBytes;

        for size in [10, 100, 500, 2000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            let (local, remote) =
                overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);
            let remote_summary = remote.summarize();

            group.bench_with_input(
                BenchmarkId::new("overlapping_50pct", size),
                &(local, remote_summary),
                |b, (local, summary)| {
                    b.iter(|| local.diff_remote(black_box(summary)));
                },
            );
        }

        // Completely missing remote (empty summary)
        for size in [100, 1000] {
            group.throughput(Throughput::Elements(size as u64));

            let local = synthetic_sedimentree(size, size, 1);
            let empty_remote = Sedimentree::new(vec![], vec![]);
            let empty_summary = empty_remote.summarize();

            group.bench_with_input(
                BenchmarkId::new("vs_empty_remote", size),
                &(local, empty_summary),
                |b, (local, summary)| {
                    b.iter(|| local.diff_remote(black_box(summary)));
                },
            );
        }

        group.finish();
    }

    /// Benchmark minimizing a Sedimentree (removing redundant data).
    ///
    /// **Intent**: Measure the cost of compacting a tree by removing fragments/commits
    /// that are subsumed by others at higher strata.
    ///
    /// **Expected complexity**: Depends on DAG structure. Linear DAGs should be O(n),
    /// merge-heavy DAGs may be O(n log n) or worse due to traversal.
    pub fn bench_minimize(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_minimize");
        let metric = CountLeadingZeroBytes;

        // Standard linear DAG
        for size in [10, 100, 500, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::new("linear_dag", size), &tree, |b, t| {
                b.iter(|| t.minimize(black_box(&metric)));
            });
        }

        // Merge-heavy DAG (merge every 5 commits)
        for size in [100, 500, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            let commits = super::generators::merge_heavy_dag(size, 5, 1);
            let tree = Sedimentree::new(vec![], commits);

            group.bench_with_input(BenchmarkId::new("merge_heavy_dag", size), &tree, |b, t| {
                b.iter(|| t.minimize(black_box(&metric)));
            });
        }

        // Wide DAG (many independent heads - stress test)
        for (width, depth) in [(10, 100), (50, 20), (100, 10)] {
            let total = width * depth;
            group.throughput(Throughput::Elements(total as u64));

            let commits = super::generators::wide_dag(width, depth, 1);
            let tree = Sedimentree::new(vec![], commits);

            group.bench_with_input(
                BenchmarkId::new("wide_dag", format!("{width}w_{depth}d")),
                &tree,
                |b, t| {
                    b.iter(|| t.minimize(black_box(&metric)));
                },
            );
        }

        group.finish();
    }

    /// Benchmark finding head commits in a Sedimentree.
    ///
    /// **Intent**: Measure the cost of identifying all commits with no children.
    /// This is used to determine the current "tips" of the commit DAG.
    ///
    /// **Expected complexity**: O(n) for linear DAGs (one head), potentially higher
    /// for wide DAGs with many heads.
    pub fn bench_heads(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_heads");
        let metric = CountLeadingZeroBytes;

        // Linear DAG (single head)
        for size in [10, 100, 500, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::new("linear_dag", size), &tree, |b, t| {
                b.iter(|| t.heads(black_box(&metric)));
            });
        }

        // Wide DAG (many heads)
        for (width, depth) in [(10, 100), (50, 20), (100, 10)] {
            let total = width * depth;
            group.throughput(Throughput::Elements(total as u64));

            let commits = super::generators::wide_dag(width, depth, 1);
            let tree = Sedimentree::new(vec![], commits);

            group.bench_with_input(
                BenchmarkId::new("wide_dag", format!("{width}heads")),
                &tree,
                |b, t| {
                    b.iter(|| t.heads(black_box(&metric)));
                },
            );
        }

        group.finish();
    }

    /// Benchmark creating a summary of a Sedimentree for remote sync.
    ///
    /// **Intent**: Measure the cost of producing a compact representation of tree
    /// contents that can be sent to peers for diff calculation.
    ///
    /// **Expected complexity**: O(n) - should scan all fragments and commits once.
    pub fn bench_summarize(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_summarize");

        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64 * 2)); // fragments + commits

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
                b.iter(|| t.summarize());
            });
        }

        // Tree with many checkpoints per fragment (stress metadata size)
        for size in [100, 500] {
            let tree = synthetic_sedimentree_varied(size, size, 5, 50, 1);
            group.throughput(Throughput::Elements(size as u64 * 2));

            group.bench_with_input(BenchmarkId::new("many_checkpoints", size), &tree, |b, t| {
                b.iter(|| t.summarize());
            });
        }

        group.finish();
    }

    /// Benchmark adding individual commits and fragments to a Sedimentree.
    ///
    /// **Intent**: Measure incremental update cost (e.g., as new data arrives during sync).
    /// Uses `iter_batched` to isolate add cost from tree cloning.
    ///
    /// **Expected complexity**: O(1) amortized for adding to a set-based structure,
    /// but may vary based on internal representation.
    pub fn bench_add_operations(c: &mut Criterion) {
        let mut group = c.benchmark_group("sedimentree_add");

        // Adding a single commit to trees of various sizes
        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(1));

            let tree = synthetic_sedimentree(size, size, 1);
            let new_commit = synthetic_commit(999_999, vec![]);

            group.bench_with_input(
                BenchmarkId::new("add_commit", size),
                &(tree, new_commit),
                |b, (t, c)| {
                    b.iter_batched(
                        || (t.clone(), c.clone()),
                        |(mut tree, commit)| {
                            tree.add_commit(black_box(commit));
                            tree
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        // Adding a single fragment to trees of various sizes
        for size in [10, 100, 1000, 5000] {
            group.throughput(Throughput::Elements(1));

            let tree = synthetic_sedimentree(size, size, 1);
            let new_fragment = synthetic_fragment(999_999, 2, 5, 1);

            group.bench_with_input(
                BenchmarkId::new("add_fragment", size),
                &(tree, new_fragment),
                |b, (t, f)| {
                    b.iter_batched(
                        || (t.clone(), f.clone()),
                        |(mut tree, fragment)| {
                            tree.add_fragment(black_box(fragment));
                            tree
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        // Batch adding multiple commits (simulates receiving sync response)
        #[allow(clippy::cast_sign_loss)]
        for batch_size in [10, 50, 100] {
            group.throughput(Throughput::Elements(batch_size as u64));

            let tree = synthetic_sedimentree(100, 100, 1);
            let new_commits: Vec<LooseCommit> = (0..batch_size)
                .map(|i| synthetic_commit(900_000 + i as u64, vec![]))
                .collect();

            group.bench_with_input(
                BenchmarkId::new("add_commits_batch", batch_size),
                &(tree, new_commits),
                |b, (t, commits)| {
                    b.iter_batched(
                        || (t.clone(), commits.clone()),
                        |(mut tree, commits)| {
                            for c in commits {
                                tree.add_commit(black_box(c));
                            }
                            tree
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }

        group.finish();
    }

    /// Benchmark computing minimal hash of a Sedimentree.
    ///
    /// **Intent**: Measure the cost of computing a compact hash representing the
    /// tree's minimized state. Used for quick equality/change detection.
    ///
    /// **Expected complexity**: O(n) - needs to minimize then hash resulting fragments.
    pub fn bench_minimal_hash(c: &mut Criterion) {
        let mut group = c.benchmark_group("minimal_hash");
        let metric = CountLeadingZeroBytes;

        for size in [10, 100, 500, 2000] {
            group.throughput(Throughput::Elements(size as u64));

            let tree = synthetic_sedimentree(size, size, 1);

            group.bench_with_input(BenchmarkId::from_parameter(size), &tree, |b, t| {
                b.iter(|| t.minimal_hash(black_box(&metric)));
            });
        }

        group.finish();
    }
}

mod fragment {
    use std::hint::black_box;

    use criterion::{BenchmarkId, Criterion};
    use sedimentree_core::commit::CountLeadingZeroBytes;

    use super::generators::synthetic_fragment;

    /// Benchmark fragment support checking.
    ///
    /// **Intent**: Measure the cost of determining if a fragment "supports" another
    /// (i.e., covers its commit range). Used during diff and minimize operations.
    ///
    /// **Expected complexity**: O(checkpoints) - needs to scan checkpoint sets.
    pub fn bench_supports(c: &mut Criterion) {
        let mut group = c.benchmark_group("fragment_supports");
        let metric = CountLeadingZeroBytes;

        // Same depth comparison
        for depth in 0..=2 {
            let fragment = synthetic_fragment(1000, 3, 10, depth);
            let other_fragment = synthetic_fragment(2000, 3, 10, depth);
            let other_summary = other_fragment.summary();

            group.bench_with_input(
                BenchmarkId::new("same_depth", depth),
                &(fragment, other_summary),
                |b, (f, s)| {
                    b.iter(|| f.supports(black_box(s), &metric));
                },
            );
        }

        // Cross-depth comparison (shallower checking deeper)
        for (frag_depth, other_depth) in [(0, 2), (1, 2), (2, 0)] {
            let fragment = synthetic_fragment(1000, 3, 10, frag_depth);
            let other_fragment = synthetic_fragment(2000, 3, 10, other_depth);
            let other_summary = other_fragment.summary();

            group.bench_with_input(
                BenchmarkId::new("cross_depth", format!("{frag_depth}_vs_{other_depth}")),
                &(fragment, other_summary),
                |b, (f, s)| {
                    b.iter(|| f.supports(black_box(s), &metric));
                },
            );
        }

        // Fragment with many checkpoints (stress test)
        for checkpoint_count in [10, 50, 100] {
            let fragment = synthetic_fragment(1000, 3, checkpoint_count, 1);
            let other_fragment = synthetic_fragment(2000, 3, checkpoint_count, 1);
            let other_summary = other_fragment.summary();

            group.bench_with_input(
                BenchmarkId::new("many_checkpoints", checkpoint_count),
                &(fragment, other_summary),
                |b, (f, s)| {
                    b.iter(|| f.supports(black_box(s), &metric));
                },
            );
        }

        group.finish();
    }
}

mod topology {
    use std::hint::black_box;

    use criterion::{BenchmarkId, Criterion, Throughput};
    use sedimentree_core::{commit::CountLeadingZeroBytes, sedimentree::Sedimentree};

    use super::generators::{linear_commit_chain, merge_heavy_dag};

    /// Benchmark comparing linear vs merge-heavy DAG performance.
    ///
    /// **Intent**: Understand how DAG topology affects algorithm performance.
    /// Real-world usage may have varying merge patterns; this helps identify
    /// pathological cases.
    ///
    /// **Expected complexity**: Linear DAGs should be faster due to simpler traversal.
    pub fn bench_dag_topology_comparison(c: &mut Criterion) {
        let mut group = c.benchmark_group("dag_topology");
        let metric = CountLeadingZeroBytes;

        for size in [100, 500, 1000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            // Linear DAG
            let linear_commits = linear_commit_chain(size, 1);
            let linear_tree = Sedimentree::new(vec![], linear_commits);

            group.bench_with_input(
                BenchmarkId::new("linear/minimize", size),
                &linear_tree,
                |b, t| {
                    b.iter(|| t.minimize(black_box(&metric)));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("linear/heads", size),
                &linear_tree,
                |b, t| {
                    b.iter(|| t.heads(black_box(&metric)));
                },
            );

            // Merge-heavy DAG (merge every 5 commits)
            let merge_commits = merge_heavy_dag(size, 5, 1);
            let merge_tree = Sedimentree::new(vec![], merge_commits);

            group.bench_with_input(
                BenchmarkId::new("merge_heavy/minimize", size),
                &merge_tree,
                |b, t| {
                    b.iter(|| t.minimize(black_box(&metric)));
                },
            );

            group.bench_with_input(
                BenchmarkId::new("merge_heavy/heads", size),
                &merge_tree,
                |b, t| {
                    b.iter(|| t.heads(black_box(&metric)));
                },
            );

            // Very frequent merges (merge every 2 commits - stress test)
            let frequent_merge_commits = merge_heavy_dag(size, 2, 1);
            let frequent_merge_tree = Sedimentree::new(vec![], frequent_merge_commits);

            group.bench_with_input(
                BenchmarkId::new("very_merge_heavy/minimize", size),
                &frequent_merge_tree,
                |b, t| {
                    b.iter(|| t.minimize(black_box(&metric)));
                },
            );
        }

        group.finish();
    }

    /// Benchmark deep commit chains (stress test for traversal algorithms).
    ///
    /// **Intent**: Test performance on very long linear histories, which may occur
    /// in long-running documents with many small edits.
    ///
    /// **Expected complexity**: Should remain O(n), but constant factors matter at scale.
    pub fn bench_deep_chains(c: &mut Criterion) {
        let mut group = c.benchmark_group("deep_chains");
        let metric = CountLeadingZeroBytes;

        // Measurement time needs to be longer for large chains
        group.measurement_time(std::time::Duration::from_secs(10));

        for size in [1000, 5000, 10000, 20000] {
            group.throughput(Throughput::Elements(size as u64));

            let commits = linear_commit_chain(size, 1);
            let tree = Sedimentree::new(vec![], commits);

            group.bench_with_input(BenchmarkId::new("minimize", size), &tree, |b, t| {
                b.iter(|| t.minimize(black_box(&metric)));
            });

            group.bench_with_input(BenchmarkId::new("heads", size), &tree, |b, t| {
                b.iter(|| t.heads(black_box(&metric)));
            });

            group.bench_with_input(BenchmarkId::new("summarize", size), &tree, |b, t| {
                b.iter(|| t.summarize());
            });
        }

        // Diff between two deep chains (one subset of other)
        for size in [1000, 5000] {
            group.throughput(Throughput::Elements(size as u64));

            let commits1 = linear_commit_chain(size, 1);
            let commits2 = linear_commit_chain(size / 2, 1); // Half the commits

            let tree1 = Sedimentree::new(vec![], commits1);
            let tree2 = Sedimentree::new(vec![], commits2);

            group.bench_with_input(
                BenchmarkId::new("diff_subset", size),
                &(tree1, tree2),
                |b, (t1, t2)| {
                    b.iter(|| t1.diff(black_box(t2)));
                },
            );
        }

        group.finish();
    }
}

criterion_group!(
    benches,
    digest::bench_digest_hashing,
    digest::bench_depth_metric,
    sedimentree::bench_construction,
    sedimentree::bench_merge,
    sedimentree::bench_diff,
    sedimentree::bench_diff_remote,
    sedimentree::bench_minimize,
    sedimentree::bench_heads,
    sedimentree::bench_summarize,
    sedimentree::bench_add_operations,
    sedimentree::bench_minimal_hash,
    fragment::bench_supports,
    topology::bench_dag_topology_comparison,
    topology::bench_deep_chains,
);

criterion_main!(benches);
