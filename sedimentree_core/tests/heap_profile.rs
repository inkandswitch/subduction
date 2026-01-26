//! Heap allocation profiling for sedimentree operations.
//!
//! Run with: `cargo test --package sedimentree_core --test heap_profile --features std -- --nocapture`
//!
//! This will output allocation statistics for key operations.

#![cfg(feature = "std")]
#![allow(missing_docs)]

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    digest::Digest,
    fragment::Fragment,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
};

use rand::{Rng, SeedableRng, rngs::SmallRng};

mod generators {
    use super::*;

    pub(crate) fn digest_from_seed(seed: u64) -> Digest<LooseCommit> {
        let mut bytes = [0u8; 32];
        let mut rng = SmallRng::seed_from_u64(seed);
        rng.fill(&mut bytes);
        Digest::from_bytes(bytes)
    }

    fn blob_digest_from_seed(seed: u64) -> Digest<Blob> {
        let mut bytes = [0u8; 32];
        let mut rng = SmallRng::seed_from_u64(seed);
        rng.fill(&mut bytes);
        Digest::from_bytes(bytes)
    }

    fn digest_with_leading_zeros(zeros: usize, seed: u64) -> Digest<LooseCommit> {
        let mut bytes = [0u8; 32];
        let mut rng = SmallRng::seed_from_u64(seed);
        #[allow(clippy::indexing_slicing)]
        rng.fill(&mut bytes[zeros..]);
        #[allow(clippy::indexing_slicing)]
        if zeros < 32 && bytes[zeros] == 0 {
            bytes[zeros] = 1;
        }
        Digest::from_bytes(bytes)
    }

    fn synthetic_blob_meta(seed: u64, size: u64) -> BlobMeta {
        BlobMeta::from_digest_size(blob_digest_from_seed(seed), size)
    }

    fn synthetic_commit(seed: u64, parents: Vec<Digest<LooseCommit>>) -> LooseCommit {
        let digest = digest_from_seed(seed);
        let blob_meta = synthetic_blob_meta(seed.wrapping_add(1_000_000), 1024);
        LooseCommit::new(digest, parents, blob_meta)
    }

    fn synthetic_fragment(
        head_seed: u64,
        boundary_count: usize,
        checkpoint_count: usize,
        leading_zeros: usize,
    ) -> Fragment {
        let head = digest_with_leading_zeros(leading_zeros, head_seed);
        let boundary: Vec<Digest<LooseCommit>> = (0..boundary_count)
            .map(|i| digest_with_leading_zeros(leading_zeros, head_seed + 100 + i as u64))
            .collect();
        let checkpoints: Vec<Digest<LooseCommit>> = (0..checkpoint_count)
            .map(|i| digest_from_seed(head_seed + 200 + i as u64))
            .collect();
        let blob_meta = synthetic_blob_meta(head_seed + 300, 4096);
        Fragment::new(head, boundary, checkpoints, blob_meta)
    }

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

    pub(crate) fn synthetic_sedimentree(
        fragment_count: usize,
        commit_count: usize,
        base_seed: u64,
    ) -> Sedimentree {
        let fragments: Vec<Fragment> = (0..fragment_count)
            .map(|i| {
                let leading_zeros = (i % 3).min(2);
                synthetic_fragment(base_seed + i as u64 * 1000, 2, 5, leading_zeros)
            })
            .collect();

        let commits = linear_commit_chain(commit_count, base_seed + 500_000);
        Sedimentree::new(fragments, commits)
    }

    pub(crate) fn overlapping_sedimentrees(
        shared_fragments: usize,
        unique_fragments_each: usize,
        shared_commits: usize,
        unique_commits_each: usize,
        base_seed: u64,
    ) -> (Sedimentree, Sedimentree) {
        let shared_frags: Vec<Fragment> = (0..shared_fragments)
            .map(|i| synthetic_fragment(base_seed + i as u64 * 1000, 2, 5, i % 3))
            .collect();
        let shared_commits_vec = linear_commit_chain(shared_commits, base_seed + 100_000);

        let a_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
            .map(|i| synthetic_fragment(base_seed + 200_000 + i as u64 * 1000, 2, 5, i % 3))
            .collect();
        let a_unique_commits = linear_commit_chain(unique_commits_each, base_seed + 300_000);

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

use generators::{overlapping_sedimentrees, synthetic_sedimentree};

fn measure_allocs<F, R>(f: F) -> (u64, u64, R)
where
    F: FnOnce() -> R,
{
    let before = dhat::HeapStats::get();
    let result = f();
    let after = dhat::HeapStats::get();
    (
        after.total_blocks - before.total_blocks,
        after.total_bytes - before.total_bytes,
        result,
    )
}

#[test]
fn heap_profile_all() {
    let output_path = "dhat-heap.json";

    let _profiler = dhat::Profiler::builder().file_name(output_path).build();

    println!("\n");
    println!("================================================================================");
    println!("                         HEAP ALLOCATION PROFILE");
    println!("================================================================================\n");

    // diff() scenarios

    println!("--- diff() on disjoint trees (100 fragments, 100 commits each) ---");
    let tree1 = synthetic_sedimentree(100, 100, 1);
    let tree2 = synthetic_sedimentree(100, 100, 1_000_000);
    let (blocks, bytes, _) = measure_allocs(|| tree1.diff(&tree2));
    println!("  Allocations: {blocks} blocks, {bytes} bytes\n");

    println!("--- diff() on 50% overlapping trees (100 fragments, 100 commits each) ---");
    let (tree1, tree2) = overlapping_sedimentrees(50, 50, 50, 50, 42);
    let (blocks, bytes, _) = measure_allocs(|| tree1.diff(&tree2));
    println!("  Allocations: {blocks} blocks, {bytes} bytes\n");

    println!("--- diff() on identical trees (100 fragments, 100 commits) ---");
    let tree = synthetic_sedimentree(100, 100, 1);
    let (blocks, bytes, _) = measure_allocs(|| tree.diff(&tree));
    println!("  Allocations: {blocks} blocks, {bytes} bytes\n");

    // diff_remote() scenarios

    println!("--- diff_remote() on 50% overlapping (100 fragments, 100 commits) ---");
    let (local, remote) = overlapping_sedimentrees(50, 50, 50, 50, 42);
    let remote_summary = remote.summarize();
    let (blocks, bytes, _) = measure_allocs(|| local.diff_remote(&remote_summary));
    println!("  Allocations: {blocks} blocks, {bytes} bytes\n");

    // Scaling analysis

    println!("--- diff() allocation scaling (disjoint trees) ---");
    println!("  {:>6}  {:>8}  {:>10}", "Size", "Blocks", "Bytes");
    println!("  {:->6}  {:->8}  {:->10}", "", "", "");

    for size in [10, 50, 100, 500, 1000] {
        let t1 = synthetic_sedimentree(size, size, 1);
        let t2 = synthetic_sedimentree(size, size, 1_000_000);
        let (blocks, bytes, _) = measure_allocs(|| t1.diff(&t2));
        println!("  {size:>6}  {blocks:>8}  {bytes:>10}");
    }

    println!("\n--- diff_remote() allocation scaling (50% overlap) ---");
    println!("  {:>6}  {:>8}  {:>10}", "Size", "Blocks", "Bytes");
    println!("  {:->6}  {:->8}  {:->10}", "", "", "");

    for size in [10, 50, 100, 500, 1000] {
        let (local, remote) = overlapping_sedimentrees(size / 2, size / 2, size / 2, size / 2, 42);
        let summary = remote.summarize();
        let (blocks, bytes, _) = measure_allocs(|| local.diff_remote(&summary));
        println!("  {size:>6}  {blocks:>8}  {bytes:>10}");
    }

    println!("\n================================================================================");
    println!("  Heap profile saved to: sedimentree_core/{output_path}");
    println!("  View with: https://nnethercote.github.io/dh_view/dh_view.html");
    println!("================================================================================\n");
}
