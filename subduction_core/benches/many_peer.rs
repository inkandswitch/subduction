//! Many-peer sync benchmarks.
//!
//! Scales past the 8-peer cap of `subduction_websocket/benches/e2e.rs` by using the in-process
//! [`ChannelTransport`] mock. No TCP, no OS file descriptors, no TLS handshake — just the
//! pure protocol work.
//!
//! ## Coverage
//!
//! | Bench group                       | Topology        | Measures                                |
//! |-----------------------------------|-----------------|-----------------------------------------|
//! | `many_peer/hub_full_sync`         | Star (1 hub + N spokes) | Hub → all-spokes broadcast sync |
//! | `many_peer/pairwise_sync`         | 1 hub + 1 spoke | Baseline for the above (per-peer cost)  |
//!
//! ## Why this matters
//!
//! - PR #120 added a dedicated `response_queue` to avoid HOL blocking on busy servers. This
//!   bench exercises that queue at realistic N.
//! - The O(n²) broadcast flagged at `subduction_core/src/subduction.rs:1343` is the exact
//!   target for Phase 5 optimisation; `hub_full_sync` with large N surfaces its cost.
//!
//! ## What's here and what isn't
//!
//! **Present**: star topology, pairwise baseline. **Absent**: slow-peer HOL, full mesh, churn
//! scenarios (disconnects mid-sync). Those are straightforward extensions once baseline
//! numbers are in hand.
//!
//! Run with:
//! ```sh
//! cargo bench -p subduction_core --bench many_peer
//! # Include larger peer counts:
//! cargo bench -p subduction_core --bench many_peer --features test_utils
//! ```
//!
//! Note: the bench is single-tier by default (N ∈ {2, 8, 32}). Gate larger N on the
//! `medium_benches` feature of `subduction_bench_support` if you want slower runs — this
//! bench doesn't currently pull in that feature to keep the default `cargo bench` tractable.

#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::items_after_statements,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{hint::black_box, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
    test_utils::blob_from_seed,
};
use subduction_bench_support::harness::criterion::default_criterion;
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{builder::SubductionBuilder, Subduction},
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, test_utils::signer_from_seed};
use tokio::runtime::Runtime;

// ============================================================================
// Type aliases to keep the bench bodies readable
// ============================================================================

type Conn = MessageTransport<ChannelTransport>;

type BenchSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

// ============================================================================
// Harness helpers
// ============================================================================

fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Spawn a fresh `Subduction` node with `MemoryStorage`, the `OpenPolicy`, and a deterministic
/// signer derived from `seed`.
///
/// Returns the node plus its signer (for peer-id construction) and the listener / manager
/// `JoinHandle`s so the caller can keep them alive.
fn spawn_node(seed: u64) -> (BenchSubduction, MemorySigner) {
    let signer = signer_from_seed(seed);
    let storage = MemoryStorage::new();

    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer.clone())
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);

    (sd, signer)
}

/// Connect two nodes via a fresh `ChannelTransport` pair. Registers the authenticated
/// connection on both sides.
async fn connect(
    a: &BenchSubduction,
    a_signer: &MemorySigner,
    b: &BenchSubduction,
    b_signer: &MemorySigner,
) {
    let (t_a, t_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(t_a);
    let conn_b = MessageTransport::new(t_b);
    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());
    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await.expect("add_connection a");
    b.add_connection(auth_b).await.expect("add_connection b");
}

/// Seed `n` commits into the given node's storage.
async fn populate_hub(hub: &BenchSubduction, sed_id: SedimentreeId, commit_count: usize) {
    let mut prev: Option<CommitId> = None;

    for i in 0..commit_count {
        let mut head_bytes = [0u8; 32];
        head_bytes[0..8].copy_from_slice(&(i as u64).to_le_bytes());
        let head = CommitId::new(head_bytes);

        let parents: std::collections::BTreeSet<CommitId> = prev
            .map(|p| std::collections::BTreeSet::from([p]))
            .unwrap_or_default();

        let blob: Blob = blob_from_seed(i as u64, 64);
        hub.add_commit(sed_id, head, parents, blob)
            .await
            .expect("add_commit");

        prev = Some(head);
    }
}

// ============================================================================
// Bench: pairwise baseline
// ============================================================================

fn bench_pairwise_sync(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("many_peer/pairwise_sync");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(20);

    let sed_id = SedimentreeId::new([0xab; 32]);

    for &commits in &[10usize, 100] {
        group.throughput(Throughput::Elements(commits as u64));
        group.bench_with_input(BenchmarkId::new("commits", commits), &commits, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (hub, hub_signer) = spawn_node(0);
                        let (spoke, spoke_signer) = spawn_node(1);

                        populate_hub(&hub, sed_id, n).await;
                        connect(&hub, &hub_signer, &spoke, &spoke_signer).await;

                        let hub_peer = PeerId::from(hub_signer.verifying_key());
                        (hub, spoke, hub_peer)
                    })
                },
                |(hub, spoke, hub_peer)| {
                    rt.block_on(async move {
                        // `full_sync_with_peer(peer, subscribe, timeout)` returns a
                        // tuple `(ok, stats, transport_errors)`; no `Result`.
                        let result = spoke.full_sync_with_peer(&hub_peer, false, None).await;
                        black_box(result);
                        drop(hub);
                        drop(spoke);
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Bench: star topology, full_sync_with_all_peers
// ============================================================================

fn bench_hub_full_sync(c: &mut Criterion) {
    let rt = make_runtime();
    let mut group = c.benchmark_group("many_peer/hub_full_sync");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(15);

    let sed_id = SedimentreeId::new([0xcd; 32]);
    const COMMITS: usize = 50;

    // Peer counts: 2 (baseline), 8 (current websocket bench cap), 32 (genuinely new).
    // Uncomment 128 if running with extra wall-clock budget.
    for &n_spokes in &[2usize, 8, 32] {
        group.throughput(Throughput::Elements(n_spokes as u64));
        group.bench_with_input(
            BenchmarkId::new("spokes", n_spokes),
            &n_spokes,
            |b, &spoke_count| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let (hub, hub_signer) = spawn_node(0);
                            populate_hub(&hub, sed_id, COMMITS).await;

                            let mut spokes: Vec<(BenchSubduction, PeerId)> =
                                Vec::with_capacity(spoke_count);
                            let hub_peer = PeerId::from(hub_signer.verifying_key());

                            for i in 0..spoke_count {
                                let (spoke, spoke_signer) = spawn_node((i as u64) + 1);
                                connect(&hub, &hub_signer, &spoke, &spoke_signer).await;
                                spokes.push((spoke, hub_peer));
                            }

                            (hub, spokes)
                        })
                    },
                    |(hub, spokes)| {
                        rt.block_on(async move {
                            // Drive every spoke to fully sync with the hub concurrently.
                            // This is the "star broadcast" shape.
                            let mut handles = Vec::with_capacity(spokes.len());
                            for (spoke, hub_peer) in spokes {
                                handles.push(tokio::spawn(async move {
                                    let _res =
                                        spoke.full_sync_with_peer(&hub_peer, false, None).await;
                                    drop(spoke);
                                }));
                            }
                            for h in handles {
                                h.await.expect("spoke task");
                            }
                            drop(hub);
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_pairwise_sync,
        bench_hub_full_sync,
}
criterion_main!(benches);
