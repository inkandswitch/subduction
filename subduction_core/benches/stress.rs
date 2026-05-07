//! Scaling-curve benchmarks for the stress scenarios in
//! `subduction_core/tests/stress_*.rs`.
//!
//! These benches sweep the parameter space (commit count, peer count,
//! doc count) so the resulting Criterion plots show the scaling shape —
//! linear, quadratic, jagged. They're for finding bottlenecks, not for
//! gating CI on absolute numbers.
//!
//! Run with:
//!
//! ```sh
//! cargo bench --package subduction_core --features std --bench stress -- --noplot
//! ```

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use criterion_pprof::criterion::{Output, PProfProfiler};
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob,
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;

type Conn = MessageTransport<ChannelTransport>;

type SendableNode = Arc<
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

const SYNC_TIMEOUT: Duration = Duration::from_secs(60);

// ─── Helpers (inlined; mirror tests/common.rs) ──────────────────────────────

fn signer(seed: u32) -> MemorySigner {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    MemorySigner::from_bytes(&bytes)
}

fn sed_id(seed: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    SedimentreeId::new(bytes)
}

fn commit_id(doc_seed: u32, seq: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&doc_seed.to_be_bytes());
    bytes[4..8].copy_from_slice(&seq.to_be_bytes());
    CommitId::new(bytes)
}

fn make_blob(seed: u32, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let s = seed.to_be_bytes();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = s[i % 4].wrapping_add(u8::try_from(i % 256).unwrap_or(0));
    }
    Blob::new(data)
}

fn make_node(seed: u32) -> SendableNode {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &SendableNode,
    a_seed: u32,
    b: &SendableNode,
    b_seed: u32,
) {
    let (transport_a, transport_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);
    let peer_a = PeerId::from(signer(a_seed).verifying_key());
    let peer_b = PeerId::from(signer(b_seed).verifying_key());
    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);
    a.add_connection(auth_a).await.expect("add_connection a");
    b.add_connection(auth_b).await.expect("add_connection b");
}

async fn populate_chain(
    node: &SendableNode,
    id: SedimentreeId,
    doc_seed: u32,
    count: u32,
    blob_size: usize,
) {
    let mut parents = BTreeSet::new();
    for seq in 0..count {
        let head = commit_id(doc_seed, seq);
        node.add_commit(
            id,
            head,
            parents.clone(),
            make_blob(doc_seed.wrapping_add(seq), blob_size),
        )
        .await
        .expect("add_commit");
        parents = BTreeSet::from([head]);
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ─── Bench: long-lived doc (commit-count scaling) ───────────────────────────

/// Sync time scaling vs. commits in a single sedimentree.
///
/// Bottleneck shape: as commit count grows, expect `Sedimentree::minimize`
/// dominance (`O(|M|² × avg_boundary)` per write) plus the fingerprint
/// diff cost on the responder side.
fn bench_long_lived_doc(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/long_lived_doc");

    for &count in &[100u32, 500, 1_000, 2_000] {
        group.throughput(Throughput::Elements(u64::from(count)));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |b, &count| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let alice = make_node(1);
                            let bob = make_node(2);
                            connect_pair(&alice, 1, &bob, 2).await;
                            populate_chain(&alice, sed_id(0), 0, count, 32).await;
                            (alice, bob)
                        })
                    },
                    |(alice, _bob)| {
                        rt.block_on(async {
                            let bob_id = PeerId::from(signer(2).verifying_key());
                            let r = alice
                                .full_sync_with_peer(&bob_id, true, Some(SYNC_TIMEOUT))
                                .await;
                            assert!(r.0, "sync");
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

// ─── Bench: many docs per peer (fan-out scaling) ────────────────────────────

/// Sync time scaling vs. document count, single peer pair.
///
/// Bottleneck shape: as N grows, expect `FuturesUnordered` fan-out to
/// flatten the curve initially (concurrent sync per doc), then per-doc
/// shard-lock contention to kick in.
fn bench_many_docs(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/many_docs");

    for &num_docs in &[10u32, 50, 200, 500] {
        group.throughput(Throughput::Elements(u64::from(num_docs)));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_docs),
            &num_docs,
            |b, &num_docs| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let alice = make_node(11);
                            let bob = make_node(12);
                            connect_pair(&alice, 11, &bob, 12).await;
                            for doc in 0..num_docs {
                                populate_chain(&alice, sed_id(doc), doc, 3, 32).await;
                            }
                            (alice, bob)
                        })
                    },
                    |(alice, _bob)| {
                        rt.block_on(async {
                            let bob_id = PeerId::from(signer(12).verifying_key());
                            let r = alice
                                .full_sync_with_peer(&bob_id, true, Some(SYNC_TIMEOUT))
                                .await;
                            assert!(r.0, "sync");
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

// ─── Bench: many peers fan-in (concurrent peer scaling) ─────────────────────

/// Sync time scaling vs. number of clients fanning into one server.
///
/// Bottleneck shape: as client count grows, expect server-side
/// connection-pool contention plus the multiplexer table to surface as
/// the dominant cost. Each client owns a unique sedimentree.
fn bench_fan_in(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/fan_in");

    for &num_clients in &[4u32, 16, 64] {
        group.throughput(Throughput::Elements(u64::from(num_clients)));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            &num_clients,
            |b, &num_clients| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let server = make_node(0);
                            let mut clients = Vec::with_capacity(num_clients as usize);
                            for client_idx in 0..num_clients {
                                let seed = 1 + client_idx;
                                let client = make_node(seed);
                                connect_pair(&client, seed, &server, 0).await;
                                populate_chain(&client, sed_id(client_idx), client_idx, 3, 32)
                                    .await;
                                clients.push(client);
                            }
                            (server, clients)
                        })
                    },
                    |(_server, clients)| {
                        rt.block_on(async {
                            let server_id = PeerId::from(signer(0).verifying_key());
                            let mut handles = Vec::new();
                            for client in clients {
                                let id = server_id;
                                handles.push(tokio::spawn(async move {
                                    let r = client
                                        .full_sync_with_peer(&id, true, Some(SYNC_TIMEOUT))
                                        .await;
                                    assert!(r.0, "sync");
                                }));
                            }
                            for h in handles {
                                h.await.expect("client task");
                            }
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

// ─── Bench: blob-size scaling on long-lived doc ─────────────────────────────

/// Sync time scaling vs. blob size, with a fixed commit count.
///
/// Bottleneck shape: linear in (commits × blob_size) for raw transfer;
/// any super-linear behavior here points at copy / verify-per-blob
/// overheads that don't scale linearly.
fn bench_blob_size_scaling(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/blob_size");
    const COMMIT_COUNT: u32 = 100;

    for &(label, size) in &[
        ("32B", 32usize),
        ("1KB", 1_024),
        ("32KB", 32 * 1_024),
        ("256KB", 256 * 1_024),
    ] {
        group.throughput(Throughput::Bytes((COMMIT_COUNT as u64) * size as u64));
        group.bench_with_input(BenchmarkId::new("blob", label), &size, |b, &size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let alice = make_node(21);
                        let bob = make_node(22);
                        connect_pair(&alice, 21, &bob, 22).await;
                        populate_chain(&alice, sed_id(0), 0, COMMIT_COUNT, size).await;
                        (alice, bob)
                    })
                },
                |(alice, _bob)| {
                    rt.block_on(async {
                        let bob_id = PeerId::from(signer(22).verifying_key());
                        let r = alice
                            .full_sync_with_peer(&bob_id, true, Some(SYNC_TIMEOUT))
                            .await;
                        assert!(r.0, "sync");
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ─── Driver ─────────────────────────────────────────────────────────────────

/// Criterion config tuned for stress benches.
///
/// Stress benches are slow per iteration (each iter spins up two full
/// `Subduction` nodes). Following the e2e bench pattern (PR #153), we
/// cap sample size and warm-up so the suite fits within sane CI budgets
/// while still surfacing the shape of the scaling curve.
fn stress_criterion() -> Criterion {
    Criterion::default()
        .with_profiler(PProfProfiler::new(997, Output::Flamegraph(None)))
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
}

criterion_group! {
    name = benches;
    config = stress_criterion();
    targets =
        bench_long_lived_doc,
        bench_many_docs,
        bench_fan_in,
        bench_blob_size_scaling,
}

criterion_main!(benches);
