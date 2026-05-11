//! Single-threaded (`Local` future form) stress tests.
//!
//! Exercises the same code paths as the wasm bindings — `Local` is the
//! future form used by the wasm crates — without standing up a full wasm
//! runtime. If a deadlock, leak, or scaling cliff manifests in these
//! tests, the wasm path will hit it too.
//!
//! Each test is wrapped in [`tokio::task::LocalSet::run_until`] because
//! `Local`-based components rely on `tokio::task::spawn_local`, which in
//! turn requires a `LocalSet` context.
//!
//! Scenarios:
//!
//! - `many_docs_per_peer` — one client, many sedimentrees (fan-out).
//!   Models an automerge-repo-style workload where a single peer has a
//!   large open-document set.
//! - `long_lived_doc` — one client, one sedimentree with many commits.
//!   Models a mature document that has accumulated history over time.
//!
//! These are *correctness-under-load* tests: success means "no deadlock,
//! no panic, no data corruption, completes within the wall-clock budget."
//! Numerical bottleneck-hunting lives in the matching benchmarks.

#![allow(clippy::expect_used)]

use std::time::Duration;

use subduction_core::peer::id::PeerId;

#[path = "common.rs"]
mod common;

use common::{
    SETTLE, connect_local_pair, make_local_node, populate_linear_chain_local, sed_id, signer,
};

/// Many sedimentrees, single chain per tree.
///
/// Bottleneck candidates: per-tree shard-lock contention, sync's
/// `FuturesUnordered` fan-out (covered by `concurrent_sync.rs` for
/// correctness; this test catches scaling regressions).
#[tokio::test(flavor = "current_thread")]
async fn many_docs_per_peer_local() {
    const NUM_DOCS: u32 = 200;
    const COMMITS_PER_DOC: u32 = 3;
    const BLOB_SIZE: usize = 64;
    const SYNC_TIMEOUT: Duration = Duration::from_secs(30);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let alice = make_local_node(1);
            let bob = make_local_node(2);

            connect_local_pair(&alice, 1, &bob, 2)
                .await
                .expect("connect");

            // Populate Alice with NUM_DOCS distinct sedimentrees, each
            // holding a linear chain.
            for doc_seed in 0..NUM_DOCS {
                populate_linear_chain_local(
                    &alice,
                    sed_id(doc_seed),
                    doc_seed,
                    COMMITS_PER_DOC,
                    BLOB_SIZE,
                )
                .await
                .expect("populate");
            }

            tokio::time::sleep(SETTLE).await;

            // Sync everything via the all-trees variant.
            let bob_peer_id = PeerId::from(signer(2).verifying_key());
            let result = alice
                .full_sync_with_peer(&bob_peer_id, true, Some(SYNC_TIMEOUT))
                .await;
            assert!(result.0, "full_sync should report success");
            assert!(
                result.2.is_empty(),
                "no per-conn call errors expected: {:?}",
                result.2
            );
            assert!(
                result.3.is_empty(),
                "no per-tree IO errors expected: {:?}",
                result.3
            );

            // Let post-response broadcast settle.
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Verify every document propagated.
            for doc_seed in 0..NUM_DOCS {
                let id = sed_id(doc_seed);
                let bob_commits = bob
                    .get_commits(id)
                    .await
                    .unwrap_or_else(|| panic!("bob missing tree for doc_seed={doc_seed}"));
                assert_eq!(
                    bob_commits.len() as u32,
                    COMMITS_PER_DOC,
                    "bob should have all commits for doc_seed={doc_seed}"
                );
            }
        })
        .await;
}

/// One sedimentree, many commits.
///
/// Bottleneck candidates: `Sedimentree::minimize` running on every write
/// (`O(|M|² × avg_boundary)` with current implementation), in-memory tree
/// growth, and the per-message broadcast cost on hot-write paths.
///
/// This test ran in ~78 s release-mode locally with `NUM_COMMITS = 5_000`
/// during initial development — that wall-clock is itself a finding worth
/// optimizing. Keep the counts here within the CI test-timeout budget;
/// the bottleneck-hunting bench in `subduction_core/benches/stress.rs`
/// sweeps a wider parameter range.
#[tokio::test(flavor = "current_thread")]
async fn long_lived_doc_local() {
    const NUM_COMMITS: u32 = 1_000;
    const BLOB_SIZE: usize = 32;
    const SYNC_TIMEOUT: Duration = Duration::from_secs(60);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let alice = make_local_node(11);
            let bob = make_local_node(12);

            connect_local_pair(&alice, 11, &bob, 12)
                .await
                .expect("connect");

            let id = sed_id(0);
            populate_linear_chain_local(&alice, id, 0, NUM_COMMITS, BLOB_SIZE)
                .await
                .expect("populate");

            tokio::time::sleep(SETTLE).await;

            let bob_peer_id = PeerId::from(signer(12).verifying_key());
            let result = alice
                .full_sync_with_peer(&bob_peer_id, true, Some(SYNC_TIMEOUT))
                .await;
            assert!(result.0, "full_sync should report success");
            assert!(
                result.2.is_empty(),
                "no per-conn call errors expected: {:?}",
                result.2
            );
            assert!(
                result.3.is_empty(),
                "no per-tree IO errors expected: {:?}",
                result.3
            );

            // Let post-response settle for a longer doc.
            tokio::time::sleep(Duration::from_millis(500)).await;

            let bob_commits = bob.get_commits(id).await.expect("bob has tree");
            assert_eq!(
                bob_commits.len() as u32,
                NUM_COMMITS,
                "bob should have every commit"
            );
        })
        .await;
}
