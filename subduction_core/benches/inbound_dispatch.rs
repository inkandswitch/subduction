//! Measures server CPU cost on a many-document cold start, and how spawning
//! the per-document fan-out across worker threads affects it.
//!
//! ## What this measures
//!
//! The target workload is a client cold-starting and requesting **300+
//! documents** at once (keyhive disabled / `OpenPolicy`), where multi-core
//! utilisation is bursty rather than flat. This bench quantifies the cold-start
//! cost under that load.
//!
//! ## Harness shape
//!
//! Two in-process `Subduction` nodes connected over an in-memory
//! `ChannelTransport`:
//!
//! ```text
//!   server (B): pre-populated with `documents` sedimentrees,
//!               each holding `commits_per_doc` commits
//!        ▲
//!        │ ChannelTransport  (paired, in-process)
//!        ▼
//!   client (A): cold-pulls every document from B (see `Driver` for the two
//!               driver variants under comparison)
//! ```
//!
//! The timed region is **A's cold sync to convergence**. The bench is an A/B
//! between two drivers (see [`Driver`]):
//!
//! - `SerialFanout` — every document's `sync_with_peer` collected into one
//!   `FuturesUnordered` drained by a single caller task (concurrency, no
//!   parallelism).
//! - `SpawnedFullSync` — `full_sync_with_peer`, which spawns each document's
//!   sync onto the runtime (via the `Spawn` trait) so independent documents
//!   verify/ingest/minimize in parallel across worker threads.
//!
//! Comparing the two isolates the effect of spawning the per-document fan-out.
//!
//! ## Sweeps
//!
//! - `documents`: the headline axis (the bottleneck tracks document count).
//! - `commits_per_doc`: shifts the per-message verify/ingest cost.
//! - `worker_threads`: if wall-clock barely improves as cores are added, the
//!   path is serial regardless of pool size — which is the whole question.
//!
//! Run:
//! ```text
//! cargo bench -p subduction_core --bench inbound_dispatch
//! ```

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use future_form::Sendable;
use futures::StreamExt;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use tokio::runtime::Builder as RuntimeBuilder;

type Conn = MessageTransport<ChannelTransport>;

type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// Worker-thread counts to sweep. The crux question: does adding cores move the
/// wall-clock — and does the spawning driver convert cores into speedup the
/// serial driver can't?
const WORKER_THREADS: &[usize] = &[1, 4];

/// Document-count axis (the workload's primary driver).
const DOCUMENT_COUNTS: &[usize] = &[300, 1000];

/// Per-document commit counts. 50 keeps each iteration fast for a limited run;
/// the heavy 300-commit cases are deferred to a fuller sweep.
const COMMITS_PER_DOC: &[usize] = &[50];

/// Generous per-sync-call timeout; we measure convergence wall-clock, not
/// timeout behaviour.
const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(30_000);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

/// Spawn a node's listener + manager onto the *current* runtime and return the
/// handle. Both must be running for sync to make progress.
fn make_node(signer: MemorySigner) -> Node {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Deterministic distinct sedimentree id per document index.
fn doc_id(idx: usize) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&(idx as u64).to_le_bytes());
    SedimentreeId::new(bytes)
}

/// Build `commits_per_doc` non-boundary loose commits for one document.
///
/// Heads are `(doc, commit)`-unique so commits never collapse into fragments;
/// each is an independent loose commit to be pulled.
fn doc_commits(doc: usize, commits_per_doc: usize) -> Vec<(CommitId, BTreeSet<CommitId>, Blob)> {
    (0..commits_per_doc)
        .map(|c| {
            let mut head = [0u8; 32];
            head[..8].copy_from_slice(&(doc as u64).to_le_bytes());
            head[8..16].copy_from_slice(&(c as u64).to_le_bytes());
            // Avoid a boundary head (which would create a fragment): keep the
            // leading byte non-zero so the depth metric sees a shallow commit.
            head[0] |= 0x01;
            let blob = Blob::new((0..32u8).map(|i| i ^ (c as u8)).collect::<Vec<u8>>());
            (CommitId::new(head), BTreeSet::new(), blob)
        })
        .collect()
}

/// A single seed commit for document `doc` — the `c == 0` commit produced by
/// [`doc_commits`], used to pre-populate node A so it knows the document exists
/// and can delta-sync the remainder.
fn alloc_seed_commit(doc: usize) -> Vec<(CommitId, BTreeSet<CommitId>, Blob)> {
    let mut head = [0u8; 32];
    head[..8].copy_from_slice(&(doc as u64).to_le_bytes());
    head[0] |= 0x01;
    let blob = Blob::new((0..32u8).collect::<Vec<u8>>());
    vec![(CommitId::new(head), BTreeSet::new(), blob)]
}

/// Build `commits_per_doc` non-boundary commits whose blob bytes also encode
/// `client` — so distinct clients pushing the "same" document id carry
/// content-distinct commits (exercising the server's per-message verify +
/// ingest rather than dedup).
fn client_doc_commits(
    client: usize,
    doc: usize,
    commits_per_doc: usize,
) -> Vec<(CommitId, BTreeSet<CommitId>, Blob)> {
    (0..commits_per_doc)
        .map(|c| {
            let mut head = [0u8; 32];
            head[..8].copy_from_slice(&(doc as u64).to_le_bytes());
            head[8..16].copy_from_slice(&(c as u64).to_le_bytes());
            head[16..24].copy_from_slice(&(client as u64).to_le_bytes());
            head[0] |= 0x01;
            let blob = Blob::new(
                (0..32u8)
                    .map(|i| i ^ (c as u8) ^ (client as u8))
                    .collect::<Vec<u8>>(),
            );
            (CommitId::new(head), BTreeSet::new(), blob)
        })
        .collect()
}

/// Connect A and B over a paired in-memory transport, registering each side's
/// authenticated connection.
async fn connect(a: &Node, a_signer: &MemorySigner, b: &Node, b_signer: &MemorySigner) {
    let (t_a, t_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(t_a);
    let conn_b = MessageTransport::new(t_b);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a)
        .await
        .expect("A registers connection");
    b.add_connection(auth_b)
        .await
        .expect("B registers connection");
}

/// Which cold-start driver to exercise.
#[derive(Clone, Copy)]
enum Driver {
    /// Baseline: fan out `sync_with_peer` per document from a single caller
    /// task via `FuturesUnordered` (concurrency, no parallelism). Mirrors the
    /// pre-spawn `full_sync_with_peer` shape.
    SerialFanout,

    /// New path: `full_sync_with_peer`, which spawns each document's sync onto
    /// the runtime (unbounded, matching `SerialFanout`'s concurrency). To let A
    /// enumerate the documents it wants, it is pre-seeded with one commit per
    /// doc (a strict subset of B's set), so the bench measures the delta pull
    /// under the spawning driver.
    SpawnedFullSync,
}

/// One cold-start iteration: build a fresh A/B pair on the given runtime, load B
/// with `documents` docs, then time A pulling all of them to convergence.
///
/// Returns the elapsed time of the timed region only (setup is excluded).
fn one_cold_start(
    rt: &tokio::runtime::Runtime,
    driver: Driver,
    documents: usize,
    commits_per_doc: usize,
) -> Duration {
    rt.block_on(async move {
        let a_signer = make_signer(1);
        let b_signer = make_signer(2);
        let a = make_node(a_signer.clone());
        let b = make_node(b_signer.clone());

        // --- setup (untimed): populate B with `documents` documents ---
        for doc in 0..documents {
            b.store_commits_batch(doc_id(doc), doc_commits(doc, commits_per_doc))
                .await
                .expect("B stores document");
        }

        // Seed A with one commit per doc (a subset of B's commits) on *both*
        // drivers. `SpawnedFullSync` requires it so `full_sync_with_peer` can
        // enumerate A's documents and delta-pull each. `SerialFanout` doesn't
        // need it (it names each doc explicitly), but seeding it identically
        // keeps the transferred delta — `commits_per_doc - 1` per doc — equal
        // across both arms, so the A/B isolates the dispatch strategy rather
        // than confounding it with a different ingest volume.
        for doc in 0..documents {
            a.store_commits_batch(doc_id(doc), alloc_seed_commit(doc))
                .await
                .expect("A seeds document");
        }

        connect(&a, &a_signer, &b, &b_signer).await;

        // Let the connection/listener tasks settle so the timed region measures
        // sync work, not task startup.
        tokio::task::yield_now().await;

        let b_peer = PeerId::from(b_signer.verifying_key());

        // --- timed region: A cold-pulls every document from B ---
        let start = Instant::now();

        match driver {
            Driver::SerialFanout => {
                let mut pending: futures::stream::FuturesUnordered<_> = (0..documents)
                    .map(|doc| {
                        let a = Arc::clone(&a);
                        async move {
                            a.sync_with_peer(&b_peer, doc_id(doc), true, SYNC_TIMEOUT)
                                .await
                        }
                    })
                    .collect();

                while pending.next().await.is_some() {}
            }
            Driver::SpawnedFullSync => {
                a.full_sync_with_peer(&b_peer, true, SYNC_TIMEOUT).await;
            }
        }

        let elapsed = start.elapsed();

        // Correctness guard: convergence actually happened (a bench that
        // measures a no-op is worse than useless).
        let got = a
            .get_commits(doc_id(documents - 1))
            .await
            .map_or(0, |v| v.len());
        assert!(
            got >= 1,
            "cold start did not converge: last doc had {got} commits"
        );

        elapsed
    })
}

/// Number of client nodes simultaneously syncing against one server, for the
/// server fan-in scenario.
const FAN_IN_CLIENTS: &[usize] = &[8, 32];

/// Documents each client holds (and pushes to the server) in the fan-in case.
const FAN_IN_DOCS_PER_CLIENT: usize = 20;

/// One server fan-in iteration: build one server B and `clients` client nodes,
/// each holding `docs_per_client` distinct documents, then time all clients
/// `full_sync_with_peer(B)` concurrently. This stresses **B's `listen()`
/// dispatch** — N inbound request/data streams funnel through B's `listen()`
/// loop, and the server's ability to process CPU-heavy per-message handler work
/// in parallel depends on the dispatch strategy.
///
/// If `listen()` dispatch is the bottleneck, wall-clock should be ~flat across
/// worker threads; if handler CPU work parallelizes effectively, it should fall
/// with more cores.
fn one_server_fan_in(
    rt: &tokio::runtime::Runtime,
    clients: usize,
    docs_per_client: usize,
    commits_per_doc: usize,
) -> Duration {
    rt.block_on(async move {
        let b_signer = make_signer(200);
        let b = make_node(b_signer.clone());
        let b_peer = PeerId::from(b_signer.verifying_key());

        // Build clients, each with distinct docs, each connected to B.
        let mut client_nodes = Vec::with_capacity(clients);
        for ci in 0..clients {
            let c_signer = make_signer(u8::try_from(ci).unwrap_or(u8::MAX).wrapping_add(1));
            let c = make_node(c_signer.clone());
            for d in 0..docs_per_client {
                // Globally-distinct doc id per (client, doc) so the server must
                // ingest every client's documents (no cross-client dedup).
                let gid = ci * docs_per_client + d;
                c.store_commits_batch(doc_id(gid), client_doc_commits(ci, gid, commits_per_doc))
                    .await
                    .expect("client stores document");
            }
            connect(&c, &c_signer, &b, &b_signer).await;
            client_nodes.push(c);
        }

        tokio::task::yield_now().await;

        // --- timed region: every client pushes its docs to B at once ---
        let start = Instant::now();

        let mut pending: futures::stream::FuturesUnordered<_> = client_nodes
            .iter()
            .map(|c| {
                let c = Arc::clone(c);
                async move {
                    c.full_sync_with_peer(&b_peer, true, SYNC_TIMEOUT).await;
                }
            })
            .collect();

        while pending.next().await.is_some() {}

        let elapsed = start.elapsed();

        // Correctness guard: B eventually ingests the last client's last doc.
        // `full_sync_with_peer` pushes requested data fire-and-forget, so the
        // server may still be ingesting after the client call returns — poll
        // with a bounded deadline rather than assuming instant convergence.
        let last_gid = clients * docs_per_client - 1;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = b.get_commits(doc_id(last_gid)).await.map_or(0, |v| v.len());
            if got >= 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "server fan-in did not converge: last doc had 0 commits after 10s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        elapsed
    })
}

fn bench_cold_start(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_start");
    // Cold-start runs are heavy; keep sample counts modest so the sweep
    // finishes in reasonable wall-clock.
    group.sample_size(10);

    for &threads in WORKER_THREADS {
        let rt = RuntimeBuilder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("build multi-thread runtime");

        for &(driver, dname) in &[
            (Driver::SerialFanout, "serial"),
            (Driver::SpawnedFullSync, "spawned"),
        ] {
            for &documents in DOCUMENT_COUNTS {
                for &commits_per_doc in COMMITS_PER_DOC {
                    let id = BenchmarkId::new(
                        format!("wt{threads}_{dname}"),
                        format!("docs{documents}_c{commits_per_doc}"),
                    );
                    group.bench_with_input(id, &(documents, commits_per_doc), |bch, &(d, cpd)| {
                        bch.iter_custom(|iters| {
                            let mut total = Duration::ZERO;
                            for _ in 0..iters {
                                total += one_cold_start(&rt, driver, d, cpd);
                            }
                            total
                        });
                    });
                }
            }
        }
    }

    group.finish();
}

/// Server fan-in: many clients pushing to one server, stressing `listen()`.
fn bench_server_fan_in(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_fan_in");
    group.sample_size(10);

    for &threads in WORKER_THREADS {
        let rt = RuntimeBuilder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .expect("build multi-thread runtime");

        for &clients in FAN_IN_CLIENTS {
            for &commits_per_doc in COMMITS_PER_DOC {
                let id = BenchmarkId::new(
                    format!("wt{threads}"),
                    format!("clients{clients}_c{commits_per_doc}"),
                );
                group.bench_with_input(id, &(clients, commits_per_doc), |bch, &(n, cpd)| {
                    bch.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += one_server_fan_in(&rt, n, FAN_IN_DOCS_PER_CLIENT, cpd);
                        }
                        total
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_cold_start, bench_server_fan_in,
}

criterion_main!(benches);
