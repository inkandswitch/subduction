//! Wasm-target protocol microbenchmarks for the *single-threaded* hot paths
//! browsers actually walk through.
//!
//! Mirrors a subset of `subduction_core/benches/stress.rs` but constrained
//! to inner-types (`Sedimentree`, `LooseCommit`, signing, fingerprints).
//! The full `Subduction` end-to-end sync bench can't run here today
//! because `Authenticated::new_for_test` is gated behind the
//! `subduction_core/test_utils` feature, which in turn pulls in `tokio`,
//! which doesn't link to `wasm32-unknown-unknown`.
//!
//! ## Why these benches exist
//!
//! Native multi-threaded benches showed:
//! - 95 % of `full_sync_with_peer` time at 900 docs is **per-commit data
//!   transfer** (verify + apply + persist), not protocol round-trips
//! - per-commit cost steady-state ~150 µs **on a 4-thread tokio runtime**
//!
//! Browsers can't parallelize that work — the wasm thread is the only one
//! doing it. So the per-commit cost there sets the *visible* startup
//! time when the JS side hydrates many docs from storage and re-syncs.
//!
//! Hypotheses these benches are designed to falsify:
//!
//! - **WH1 (`sedimentree.add_commit` + minimize is expensive enough to
//!   matter on wasm)**: if `Sedimentree::add_commit` is followed by
//!   minimize on every write, the cumulative cost over N commits could
//!   dominate startup.
//! - **WH2 (`heads()` triggers minimize internally on every call)**: in
//!   the native data, calling `.heads()` after every write doubled the
//!   minimize work. Worth measuring under wasm.
//! - **WH3 (`fingerprint_resolver` scales linearly with tree size)**:
//!   built once per `sync_with_peer`, so per-doc N=900 cost = 900 ×
//!   resolver-build cost. Even 100 µs/build = 90 ms.
//!
//! ## Running
//!
//! ```sh
//! wasm-pack test --release --node subduction_wasm \
//!   --test perf_protocol -- --nocapture
//! ```
//!
//! Each `#[wasm_bindgen_test]` emits one JSON-formatted summary line per
//! parameter point. Pipe through `grep '"bench"'` to extract.

#![cfg(target_family = "wasm")]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs,
    unused_must_use
)]

use std::{collections::BTreeSet, hint::black_box};

use future_form::Sendable;
use sedimentree_core::{
    commit::CountLeadingZeroBytes,
    crypto::fingerprint::FingerprintSeed,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
    test_utils::{linear_chain, synthetic_commit},
};
use subduction_bench_support::harness::wasm::{Config, bench, bench_async};
use subduction_crypto::{
    signed::Signed, test_utils::signer_from_seed, verified_signature::VerifiedSignature,
};
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_node_experimental);

// ============================================================================
// Per-commit pipeline cost
// ============================================================================
//
// The inner loop of receiving N commits from a peer is, per commit:
//   1. `Signed::try_verify` (Ed25519 verify + decode)  — ~190 µs (perf_crypto)
//   2. `Sedimentree::add_commit`                        — measured below
//   3. ... + a periodic `minimize` somewhere upstream
//
// Below isolates step 2 at varying cumulative tree sizes so we can see if
// add_commit itself is O(1) or if its cost grows with the tree.

/// `Sedimentree::add_commit` repeated N times into a starting-empty tree.
///
/// Reports the **total** wall-clock for inserting `N` commits. Divide by
/// `N` to get per-commit cost. Compare across N to see whether
/// `add_commit` is constant-time or grows with tree size.
fn bench_add_commit_at_size(size: u32) {
    let commits: Vec<LooseCommit> = linear_chain(size as usize, 0xA1CE);
    let summary = bench(
        &alloc::format!("wasm/sedimentree/add_commit_x{size}"),
        Config {
            warmup_iters: 2,
            measurement_iters: 10,
        },
        || {
            let mut tree = Sedimentree::default();
            for commit in &commits {
                let added = tree.add_commit(black_box(commit.clone()));
                black_box(added);
            }
            black_box(tree);
        },
    );
    summary.report();
}

#[wasm_bindgen_test]
fn add_commit_x10() {
    bench_add_commit_at_size(10);
}
#[wasm_bindgen_test]
fn add_commit_x100() {
    bench_add_commit_at_size(100);
}
#[wasm_bindgen_test]
fn add_commit_x500() {
    bench_add_commit_at_size(500);
}
#[wasm_bindgen_test]
fn add_commit_x1000() {
    bench_add_commit_at_size(1000);
}

// ============================================================================
// Sedimentree::minimize at varying tree sizes
// ============================================================================
//
// `minimize` is documented as O(|M|² × avg_boundary). At browser scale
// (one thread, no parallelism) this can dominate the per-write hot path.
// The native data showed `long_lived_doc/5000` taking ~78s on a single
// thread. Wasm should be similar order or slower.

/// Cost of a single `minimize` call on a tree pre-populated with `size`
/// linear-chain commits.
fn bench_minimize_at_size(size: u32) {
    let commits = linear_chain(size as usize, 0xB1D);
    let mut tree = Sedimentree::default();
    for commit in &commits {
        tree.add_commit(commit.clone());
    }
    let depth = CountLeadingZeroBytes;
    let summary = bench(
        &alloc::format!("wasm/sedimentree/minimize_at_{size}"),
        Config {
            warmup_iters: 2,
            measurement_iters: 10,
        },
        || {
            let minimized = tree.minimize(black_box(&depth));
            black_box(minimized);
        },
    );
    summary.report();
}

#[wasm_bindgen_test]
fn minimize_at_10() {
    bench_minimize_at_size(10);
}
#[wasm_bindgen_test]
fn minimize_at_100() {
    bench_minimize_at_size(100);
}
#[wasm_bindgen_test]
fn minimize_at_500() {
    bench_minimize_at_size(500);
}
#[wasm_bindgen_test]
fn minimize_at_1000() {
    bench_minimize_at_size(1000);
}

// ============================================================================
// `heads()` cost (which calls minimize internally)
// ============================================================================
//
// Tests **WH2**: native investigation flagged that `.heads()` triggers
// `minimize` inside, so an outer code path that already minimized and
// then immediately called `.heads()` would do the work twice. On wasm
// the doubled cost matters more.

/// `Sedimentree::heads` on a tree of `size` linear commits.
fn bench_heads_at_size(size: u32) {
    let commits = linear_chain(size as usize, 0xC0DE);
    let mut tree = Sedimentree::default();
    for commit in &commits {
        tree.add_commit(commit.clone());
    }
    let depth = CountLeadingZeroBytes;
    let summary = bench(
        &alloc::format!("wasm/sedimentree/heads_at_{size}"),
        Config {
            warmup_iters: 2,
            measurement_iters: 10,
        },
        || {
            let heads = tree.heads(black_box(&depth));
            black_box(heads);
        },
    );
    summary.report();
}

#[wasm_bindgen_test]
fn heads_at_100() {
    bench_heads_at_size(100);
}
#[wasm_bindgen_test]
fn heads_at_1000() {
    bench_heads_at_size(1000);
}

// ============================================================================
// `fingerprint_resolver` build cost
// ============================================================================
//
// Tests **WH3**: built once per `sync_with_peer` call, so an N-doc
// `full_sync_with_peer` builds N resolvers. If each resolver is
// proportional to tree-size that's the dominant per-doc cost on wasm.

/// `Sedimentree::fingerprint_resolver` on a tree of `size` linear commits.
fn bench_resolver_at_size(size: u32) {
    let commits = linear_chain(size as usize, 0xF1AE);
    let mut tree = Sedimentree::default();
    for commit in &commits {
        tree.add_commit(commit.clone());
    }
    let seed = FingerprintSeed::random();
    let summary = bench(
        &alloc::format!("wasm/sedimentree/fingerprint_resolver_at_{size}"),
        Config {
            warmup_iters: 2,
            measurement_iters: 10,
        },
        || {
            let resolver = tree.fingerprint_resolver(black_box(&seed));
            black_box(resolver);
        },
    );
    summary.report();
}

#[wasm_bindgen_test]
fn resolver_at_3() {
    bench_resolver_at_size(3);
}
#[wasm_bindgen_test]
fn resolver_at_30() {
    bench_resolver_at_size(30);
}
#[wasm_bindgen_test]
fn resolver_at_300() {
    bench_resolver_at_size(300);
}

// ============================================================================
// Full per-commit ingestion pipeline (verify → add_commit)
// ============================================================================
//
// What a peer's `recv_commit` actually does when a single commit arrives:
//   1. Pre-signed `Signed<LooseCommit>` (already encoded; comes off the
//      wire). We start with that.
//   2. `try_verify` — Ed25519 verify + decode.
//   3. `Sedimentree::add_commit` on the existing in-memory tree.
//
// Cost of step 1 is "free" in this bench (pre-built outside the timed
// region). Step 2 alone is measured by perf_crypto (~190 µs at
// release). Step 3 is `add_commit`. Together they bound per-commit
// inbound cost on wasm. This bench measures their sum so we can see
// the realistic wall-clock floor.

/// Verify-then-add on N pre-signed commits, accumulating into one
/// growing tree. Equivalent to "receive N commits in a row from a
/// trusted peer."
async fn bench_verify_and_add_pipeline(n: u32) {
    let signer = signer_from_seed(0);
    let commits: Vec<LooseCommit> = linear_chain(n as usize, 0xD09);

    // Pre-seal all commits outside the timed region.
    let mut sealed: Vec<Signed<LooseCommit>> = Vec::with_capacity(n as usize);
    for commit in &commits {
        let v: VerifiedSignature<LooseCommit> =
            Signed::seal::<Sendable, _>(&signer, commit.clone()).await;
        sealed.push(v.into_signed());
    }

    let summary = bench_async(
        &alloc::format!("wasm/pipeline/verify_and_add_x{n}"),
        Config {
            warmup_iters: 1,
            measurement_iters: 10,
        },
        || {
            let sealed = sealed.clone();
            async move {
                let mut tree = Sedimentree::default();
                for signed in sealed {
                    let verified = signed
                        .try_verify()
                        .expect("seeded signed commits always verify");
                    let _ = tree.add_commit(verified.into_payload());
                }
                black_box(tree);
            }
        },
    )
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn verify_and_add_x10() {
    bench_verify_and_add_pipeline(10).await;
}
#[wasm_bindgen_test]
async fn verify_and_add_x100() {
    bench_verify_and_add_pipeline(100).await;
}
#[wasm_bindgen_test]
async fn verify_and_add_x500() {
    bench_verify_and_add_pipeline(500).await;
}

// ============================================================================
// Many sedimentrees: per-doc cost of cold `add_commit` chains
// ============================================================================
//
// Approximates an automerge-repo-style startup: open N docs, each gets
// 3 commits added. No sync, no peers — just local building. This is the
// "best case" — what would the wall-clock be if storage and JS↔wasm
// crossings were free?

/// Build N sedimentrees, 3 commits each. Wall-clock dominated by N ×
/// 3 × `add_commit` cost.
fn bench_many_docs_local_only(num_docs: u32) {
    // Pre-build commit lists outside the timed region.
    let chains: Vec<Vec<LooseCommit>> = (0..num_docs)
        .map(|d| linear_chain(3, 0x1000 + u64::from(d)))
        .collect();

    let summary = bench(
        &alloc::format!("wasm/many_docs_local/{num_docs}"),
        Config {
            warmup_iters: 1,
            measurement_iters: 10,
        },
        || {
            let mut trees: Vec<Sedimentree> = Vec::with_capacity(num_docs as usize);
            for chain in &chains {
                let mut tree = Sedimentree::default();
                for commit in chain {
                    tree.add_commit(commit.clone());
                }
                trees.push(tree);
            }
            black_box(trees);
        },
    );
    summary.report();
}

#[wasm_bindgen_test]
fn many_docs_local_50() {
    bench_many_docs_local_only(50);
}
#[wasm_bindgen_test]
fn many_docs_local_200() {
    bench_many_docs_local_only(200);
}
#[wasm_bindgen_test]
fn many_docs_local_900() {
    bench_many_docs_local_only(900);
}
#[wasm_bindgen_test]
fn many_docs_local_1500() {
    bench_many_docs_local_only(1500);
}

// ============================================================================
// Glue: silence false-positive unused warnings
// ============================================================================

extern crate alloc;

// The harness `Summary::report()` uses `console::log_1` — flag suppression
// for `unused_must_use` keeps the file honest about side-effecting calls.
#[allow(dead_code)]
fn _silence_synthetic_commit_warning() {
    drop(synthetic_commit(0, BTreeSet::new()));
}
