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
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::items_after_statements,
    clippy::panic,
    clippy::unreadable_literal,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use criterion_pprof::criterion::{Output, PProfProfiler};
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
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

/// Low-entropy ramp pattern. Each byte is `s[i%4] + (i%256)`, producing a
/// repeating 64-byte signature plus a slow ramp — compresses to ~3% of
/// input under gzip / zstd.
///
/// Used by every existing bench because protocol-cost benches don't care
/// about blob entropy. The high-entropy counterpart below exists so the
/// `blob_size_scaling` bench can answer "would wire-level compression
/// help?" measurably.
fn make_blob(seed: u32, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let s = seed.to_be_bytes();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = s[i % 4].wrapping_add(u8::try_from(i % 256).unwrap_or(0));
    }
    Blob::new(data)
}

/// High-entropy pseudo-random bytes from a SplitMix-style mixer keyed on
/// `seed` and `size`. Approximates the byte distribution of
/// already-compressed payloads (encrypted CRDT changes, image bytes,
/// audio frames) — gzip/zstd shrinks this to ~99% of input size, i.e.
/// transport-layer compression is a net loss for this content.
///
/// Used by the `_random` arms of `bench_blob_size_scaling` so we can
/// compare throughput against the low-entropy ramp pattern at the same
/// byte counts. The ratio answers "is the existing transport saturating
/// because of bytes, or because of per-byte processing?"
fn make_blob_random(seed: u32, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let mut state = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for byte in &mut data {
        // SplitMix64 next.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        *byte = (z & 0xFF) as u8;
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

async fn connect_pair(a: &SendableNode, a_seed: u32, b: &SendableNode, b_seed: u32) {
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
    let workers = std::env::var("STRESS_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
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
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
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
        });
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

    // Real-world startup workloads in downstream apps (e.g.
    // automerge-repo) routinely involve 900+ documents being synced after
    // hydration. Extend the parameter sweep to that scale plus headroom
    // so the curve shape stays visible past typical-use sizes.
    for &num_docs in &[1u32, 10, 50, 200, 500, 900, 1500, 3000, 6000] {
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
///
/// Two content variants per size:
///
/// - **`ramp`** (low entropy, default `make_blob`): compresses to ~3%
///   of input under gzip/zstd.
/// - **`random`** (high entropy, `make_blob_random`): compresses to
///   ~99% of input — i.e. uncompressible.
///
/// Comparing `ramp` and `random` throughput at matched size answers a
/// concrete question: would transport-layer wire compression help?
///
/// On loopback (this bench's environment) the answer is "no" regardless
/// — the channel transport has no wire I/O cost, so compression's CPU
/// overhead would be net-negative. The signal lives in the *gap*
/// between `ramp` and `random`: if the throughput is identical, per-byte
/// overhead (memcpy, hashing, encode) dominates; compression helps only
/// when wire bandwidth is the bottleneck (real WebSocket/iroh, not
/// in-process). To measure that, see `subduction_websocket/benches/e2e.rs`.
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
        for &(content_label, high_entropy) in &[("ramp", false), ("random", true)] {
            group.throughput(Throughput::Bytes((COMMIT_COUNT as u64) * size as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("blob_{content_label}"), label),
                &size,
                |b, &size| {
                    b.iter_batched(
                        || {
                            rt.block_on(async {
                                let alice = make_node(21);
                                let responder = make_node(22);
                                connect_pair(&alice, 21, &responder, 22).await;
                                let mut parents = BTreeSet::new();
                                for seq in 0..COMMIT_COUNT {
                                    let head = commit_id(0, seq);
                                    let payload = if high_entropy {
                                        make_blob_random(seq, size)
                                    } else {
                                        make_blob(seq, size)
                                    };
                                    alice
                                        .add_commit(sed_id(0), head, parents.clone(), payload)
                                        .await
                                        .expect("add_commit");
                                    parents = BTreeSet::from([head]);
                                }
                                (alice, responder)
                            })
                        },
                        |(alice, _responder)| {
                            rt.block_on(async {
                                let responder_id = PeerId::from(signer(22).verifying_key());
                                let r = alice
                                    .full_sync_with_peer(
                                        &responder_id,
                                        true,
                                        Some(SYNC_TIMEOUT),
                                    )
                                    .await;
                                assert!(r.0, "sync");
                            });
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }

    group.finish();
}

// ─── Driver ─────────────────────────────────────────────────────────────────

// ─── Diagnostic benches: decompose `many_docs` to localize the bottleneck ───
//
// `bench_many_docs` measures end-to-end `full_sync_with_peer` against N
// populated trees. The benches in this section pull apart the per-doc
// cost so we can see *where* time goes, not just how much.
//
// Hypotheses these benches are designed to falsify:
//
// - **H1 (multiplexer contention)**: every per-doc `sync_with_peer` shares
//   the same `Arc<Multiplexer>` and grabs its `pending` mutex twice. At
//   N=900 that's 1800 sequential acquisitions, which would Amdahl-cap
//   multi-thread scaling.
// - **H2 (channel transport saturation)**: a single `ChannelTransport`
//   pair carries every message. With N concurrent round-trips the channel
//   queue saturates, forcing serial drain.
// - **H3 (per-doc protocol fixed cost)**: each `sync_with_peer` does
//   non-parallelizable work (encode/decode, state-machine traversal). If
//   this dominates, mux/transport optimization is moot.
// - **H4 (data transfer dominates)**: most of the per-doc time is the
//   responder's data send-back (`LooseCommit` messages with blobs), not
//   the request/response round-trip.
//
// Initial measured curve (4 worker tokio runtime, in-process channel,
// 32-byte blobs, 3 commits/doc) — recorded here to anchor regressions:
//
// | N    | many_docs (full)   | already_synced     | sequential_already |
// |------|--------------------|--------------------|--------------------|
// |   10 | 6.08 ms (~608 µs)  | n/m                | 2.67 ms (~267 µs)  |
// |   50 | 27.0 ms (~540 µs)  | 3.31 ms (~66 µs)   | 3.66 ms (~73 µs)   |
// |  200 | 117  ms (~585 µs)  | n/m                | 7.76 ms (~39 µs)   |
// |  500 | 156  ms (~312 µs)  | 11.1 ms (~22 µs)   | 13.79 ms (~28 µs)  |
// |  900 | 226  ms (~251 µs)  | 20.9 ms (~23 µs)   | 21.6 ms (~24 µs)   |
// | 1500 | 331  ms (~221 µs)  | 49.3 ms (~33 µs)   | n/m                |
//
// Reading: at 900 docs already-synced sync is ~21 ms total (~23 µs/doc),
// vs. ~226 ms total (~250 µs/doc) for the full data-transfer case.
// **Data transfer accounts for ~95% of `bench_many_docs`'s wall time** —
// i.e. **H4 is the dominant cost**, not H1/H2/H3. The sequential vs
// concurrent ratio at N=900 (21.6 vs 20.9 ms) shows `FuturesUnordered`
// adds neither benefit nor cost in the protocol-only regime.

/// `full_sync_with_peer` against N trees that **already exist on both
/// nodes**, so the diff is empty.
///
/// Tests **H4**: difference between `bench_many_docs` (with data) and
/// this (no data) measures how much time goes to data transfer vs.
/// protocol round-trip + bookkeeping.
fn bench_many_docs_already_synced(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/many_docs_already_synced");

    for &num_docs in &[1u32, 10, 50, 200, 500, 900, 1500] {
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
                            // Populate **both** sides with the same data so
                            // the subsequent `full_sync_with_peer` has no
                            // diff to apply.
                            for doc in 0..num_docs {
                                let id = sed_id(doc);
                                populate_chain(&alice, id, doc, 3, 32).await;
                                populate_chain(&bob, id, doc, 3, 32).await;
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

/// Sync N already-synced trees **sequentially** (one at a time via N
/// `sync_with_peer` calls), not concurrently via `full_sync_with_peer`.
///
/// Tests **H1 + H2 jointly**: `full_sync_with_peer` uses `FuturesUnordered`
/// to drive all per-tree calls concurrently, hitting any multiplexer or
/// channel contention. A pure sequential equivalent avoids both. Compare
/// medians with `bench_many_docs_already_synced` at matched N — if
/// concurrent is much faster, parallelism is paying off; if comparable,
/// concurrency adds neither benefit nor cost; if concurrent is slower,
/// contention dominates parallelism.
fn bench_many_docs_sequential(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/many_docs_sequential");

    for &num_docs in &[10u32, 50, 200, 500, 900] {
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
                                let id = sed_id(doc);
                                populate_chain(&alice, id, doc, 3, 32).await;
                                populate_chain(&bob, id, doc, 3, 32).await;
                            }
                            (alice, bob)
                        })
                    },
                    |(alice, _bob)| {
                        rt.block_on(async {
                            let bob_id = PeerId::from(signer(12).verifying_key());
                            for doc in 0..num_docs {
                                let r = alice
                                    .sync_with_peer(&bob_id, sed_id(doc), false, Some(SYNC_TIMEOUT))
                                    .await
                                    .expect("sync_with_peer");
                                assert!(r.0, "sync");
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

/// One sedimentree that's already-synced, repeat `sync_with_peer` N
/// times **back-to-back**.
///
/// Tests **H3**: this is the absolute floor for `sync_with_peer`
/// latency. (median - first_sync_overhead) / N is the steady-state
/// per-call protocol cost on a single connection with no concurrent
/// contention. If it matches the per-doc cost in
/// `bench_many_docs_already_synced`, the concurrency in
/// `full_sync_with_peer` adds nothing measurable.
fn bench_repeated_single_doc_sync(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/repeated_single_doc_sync");

    for &repeats in &[1u32, 10, 50, 200, 500] {
        group.throughput(Throughput::Elements(u64::from(repeats)));
        group.bench_with_input(
            BenchmarkId::from_parameter(repeats),
            &repeats,
            |b, &repeats| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let alice = make_node(11);
                            let bob = make_node(12);
                            connect_pair(&alice, 11, &bob, 12).await;
                            populate_chain(&alice, sed_id(0), 0, 3, 32).await;
                            populate_chain(&bob, sed_id(0), 0, 3, 32).await;
                            (alice, bob)
                        })
                    },
                    |(alice, _bob)| {
                        rt.block_on(async {
                            let bob_id = PeerId::from(signer(12).verifying_key());
                            for _ in 0..repeats {
                                let r = alice
                                    .sync_with_peer(&bob_id, sed_id(0), false, Some(SYNC_TIMEOUT))
                                    .await
                                    .expect("sync_with_peer");
                                assert!(r.0, "sync");
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

/// Vary commits-per-doc at fixed doc count.
///
/// Tests **H4 decomposition**: with H4 confirmed (data transfer
/// dominates), the slope of (sync_time vs. commits-per-doc) reveals the
/// per-commit transfer cost separate from any per-doc fixed cost.
///
/// Compare with `bench_many_docs_already_synced/200` at the same N to
/// isolate transfer cost from protocol cost.
///
/// Initial measurement (N=200 docs, 32-byte blobs):
///
/// | commits/doc | total median | per-commit |
/// |-------------|--------------|------------|
/// |   1         |  37 ms       | ~184 µs    |
/// |   3         |  94 ms       | ~157 µs    |
/// |  10         | 405 ms       | ~203 µs    |
/// |  30         | 835 ms       | ~139 µs    |
/// | 100         |   2.1 s      | ~106 µs    |
///
/// Per-commit cost is roughly stable at ~150 µs, suggesting commit
/// transfer + verify + apply cost is well-amortized and not dominated
/// by per-doc setup.
fn bench_commits_per_doc(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("stress/commits_per_doc");
    const NUM_DOCS: u32 = 200;

    for &commits in &[1u32, 3, 10, 30, 100] {
        group.throughput(Throughput::Elements(u64::from(NUM_DOCS * commits)));
        group.bench_with_input(BenchmarkId::from_parameter(commits), &commits, |b, &commits| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let alice = make_node(11);
                        let bob = make_node(12);
                        connect_pair(&alice, 11, &bob, 12).await;
                        for doc in 0..NUM_DOCS {
                            populate_chain(&alice, sed_id(doc), doc, commits, 32).await;
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
        });
    }

    group.finish();
}

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
        bench_many_docs_already_synced,
        bench_many_docs_sequential,
        bench_repeated_single_doc_sync,
        bench_commits_per_doc,
        bench_fan_in,
        bench_blob_size_scaling,
}

criterion_main!(benches);
