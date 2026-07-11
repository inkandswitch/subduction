//! Repro for <https://github.com/inkandswitch/subduction/issues/268>:
//! `get_all_heads` re-hydrates the entire collection from durable storage
//! once the resident-tree LRU no longer covers it, so every call costs
//! O(collection) storage reads — and callers that use it to answer
//! periodic collection-state queries starve sync with it.
//!
//! Two real `Subduction` engines are connected over an in-memory
//! `ChannelTransport`, with `max_resident_trees` bounded (as servers run).
//! Peer A adds one single-commit sedimentree at a time via
//! `add_built_batch` — the Automerge wasm adapter's `addBatch` path — in
//! fixed-size cohorts. After each cohort the test calls
//! `b.get_all_heads()` once and counts the **storage operations** the
//! sweep performed, via the crate's own [`MetricsStorage`] wrapper and a
//! [`DebuggingRecorder`] (deterministic — no wall-clock assertions).
//!
//! Measured shape on current `main` (cap = 256):
//!
//! - while the collection fits the resident cache the sweep is ~free
//!   (a single `load_all_sedimentree_ids`);
//! - past the cap it hydrates ~every tree — ~2 loads per tree per call —
//!   because sweeping N > cap trees through the LRU also evicts
//!   everything as it goes (thrash), so *every* subsequent sweep pays
//!   full re-hydration again.
//!
//! Production impact (Cloudflare Durable Object, one collection per DO,
//! instrumented): the identical sweep went from 2 storage calls at 789
//! trees to **8,586 synchronous `SQLite` reads ≈ 6–8 s of CPU per call** at
//! 2,401 trees (`WASM_DEFAULT_MAX_RESIDENT_TREES = 1_024`), with clients
//! issuing one collection-state query every ~10 s each. Pump passes spent
//! 6–8 s serving a single sweep while dispatching **zero** sync frames and
//! the inbound queue backed up monotonically — sync starves and collections
//! freeze at ~2,400 documents. Per-document sync rounds are O(1) throughout
//! (the flat `add_ms_per_doc` control column) — rounds are not the problem;
//! the sweep is.
//!
//! The test is `#[ignore]`d because it fails by design (it asserts that a
//! sweep does sub-linear storage work) until `get_all_heads` — or an
//! alternative collection-state API — is incremental (cached heads map,
//! `changed_since` cursor, or similar). Run it with:
//!
//! ```sh
//! cargo test -p subduction_core --features metrics \
//!   --test repro_268_get_all_heads_scaling -- --ignored --nocapture
//! ```

#![cfg(feature = "metrics")]
#![allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use std::{
    collections::BTreeMap,
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use future_form::Sendable;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    metrics::names::STORAGE_OPERATION_DURATION_SECONDS,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::{memory::MemoryStorage, metrics::MetricsStorage},
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type Store = MetricsStorage<MemoryStorage>;

type TestSyncHandler =
    SyncHandler<Sendable, Store, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        Store,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// Total documents pushed through the connected pair.
const TOTAL_DOCS: u32 = 1_200;
/// Documents per cohort; one sweep is measured after each cohort.
const COHORT: u32 = 200;
/// Generous per-round deadline so rounds are never errored by the harness.
const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(30_000);
/// Resident-tree cache bound, set below `TOTAL_DOCS` the way servers run
/// (the wasm bridge defaults to 1,024; the effective floor here is one tree
/// per shard — 256 by default — hence 256).
const MAX_RESIDENT_TREES: usize = 256;
/// A fixed (incremental/cached) sweep should cost far fewer storage reads
/// than one per tree; fail once a sweep reads more than `trees / 4` times.
const MAX_SWEEP_LOADS_FRACTION: u64 = 4;

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(
            MetricsStorage::new(MemoryStorage::new()),
            Arc::new(OpenPolicy),
        )
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .max_resident_trees(MAX_RESIDENT_TREES)
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult {
    let (transport_a, transport_b) = ChannelTransport::pair();

    let conn_a = MessageTransport::new(transport_a);
    let conn_b = MessageTransport::new(transport_b);

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok(())
}

/// Unique per-document sedimentree id (one sedimentree per document, the way
/// the Automerge adapter maps `DocumentId`s).
fn doc_sed_id(index: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&index.to_be_bytes());
    bytes[4] = 0xd0;
    SedimentreeId::new(bytes)
}

/// One small unsigned `(LooseCommit, Blob)` pair, mirroring what the wasm
/// adapter hands to `addBatch` for a freshly created document.
fn make_commit_pair(sed_id: SedimentreeId, index: u32) -> (LooseCommit, Blob) {
    let data: Vec<u8> = index
        .to_be_bytes()
        .iter()
        .cycle()
        .take(64)
        .copied()
        .collect();
    let blob = Blob::new(data);
    let blob_meta = BlobMeta::new(&blob);
    let mut head_bytes = [0u8; 32];
    head_bytes[..4].copy_from_slice(&index.to_be_bytes());
    head_bytes[4] = 0xc0;
    let commit = LooseCommit::new(
        sed_id,
        CommitId::new(head_bytes),
        BTreeSet::new(),
        blob_meta,
    );
    (commit, blob)
}

/// Storage-operation counts (per `operation` label) recorded by
/// [`MetricsStorage`] since the previous snapshot — [`Snapshotter::snapshot`]
/// drains the debugging recorder, so each call returns one window.
fn storage_op_counts(snapshotter: &Snapshotter) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for (key, _unit, _desc, value) in snapshotter.snapshot().into_vec() {
        let (kind, key) = key.into_parts();
        let _ = kind;
        if key.name() != STORAGE_OPERATION_DURATION_SECONDS {
            continue;
        }
        let operation = key
            .labels()
            .find(|label| label.key() == "operation")
            .map_or_else(|| "unknown".to_owned(), |label| label.value().to_owned());
        if let DebugValue::Histogram(samples) = value {
            *counts.entry(operation).or_insert(0) += samples.len() as u64;
        }
    }
    counts
}

/// Push `TOTAL_DOCS` single-commit documents from A to a connected B in
/// cohorts of `COHORT`, then count the storage operations one
/// `b.get_all_heads()` sweep performs at each collection size. Fails
/// (demonstrating the issue) when a sweep's storage reads exceed
/// `trees / MAX_SWEEP_LOADS_FRACTION` — i.e. while every sweep past the
/// resident cap re-hydrates the whole collection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "repro for issue #268: fails by design until get_all_heads (or a collection-state alternative) is incremental"]
async fn get_all_heads_rehydrates_whole_collection_past_resident_cap() -> TestResult {
    // Global (all-thread) recorder: the sweep runs on tokio worker threads.
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());
    connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let cohort_count = TOTAL_DOCS / COHORT;
    let mut sweep_loads: Vec<u64> = Vec::with_capacity(cohort_count as usize);
    let mut trees_at: Vec<u64> = Vec::with_capacity(cohort_count as usize);

    println!("cohort  trees  add_ms_per_doc  sweep_storage_ops  sweep_ms  ops_by_kind");
    for cohort in 0..cohort_count {
        // Control: per-document add + awaited sync round (the wasm adapter's
        // `addBatch` path). Stays flat — rounds are O(1) in collection size.
        let start = Instant::now();
        for offset in 0..COHORT {
            let index = cohort * COHORT + offset;
            let sed_id = doc_sed_id(index);
            let (commit, blob) = make_commit_pair(sed_id, index);
            a.add_built_batch(sed_id, vec![(commit, blob)], Vec::new(), SYNC_TIMEOUT)
                .await?;
        }
        let add_ms_per_doc =
            (start.elapsed().as_secs_f64() * 1_000.0 / f64::from(COHORT) * 100.0).round() / 100.0;

        // Barrier: wait for B's trailing ingest to converge so the sweep
        // below observes (and pays for) the full collection.
        let expected_trees = usize::try_from((cohort + 1) * COHORT).expect("tree count fits usize");
        let barrier_deadline = Instant::now() + Duration::from_secs(10);
        while b
            .get_commits(doc_sed_id((cohort + 1) * COHORT - 1))
            .await
            .is_none()
        {
            assert!(
                Instant::now() < barrier_deadline,
                "peer B did not receive the cohort's last document within 10s"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The measurement: one collection-state sweep, counted in storage
        // operations (deterministic; wall time is reported but not asserted).
        // The first call drains everything the add/converge phase recorded,
        // so the post-sweep snapshot holds exactly the sweep's window.
        let _ = storage_op_counts(&snapshotter);
        let sweep_start = Instant::now();
        let heads = b.get_all_heads().await;
        let sweep_ms = (sweep_start.elapsed().as_secs_f64() * 10_000.0).round() / 10.0;
        let by_op = storage_op_counts(&snapshotter);
        assert_eq!(heads.len(), expected_trees, "sweep saw all trees");

        let total_ops: u64 = by_op.values().sum();
        sweep_loads.push(total_ops);
        trees_at.push(u64::from((cohort + 1) * COHORT));
        let by_op_str = by_op
            .iter()
            .map(|(op, n)| format!("{op}:{n}"))
            .collect::<Vec<_>>()
            .join(" ");
        println!(
            "{:>6}  {:>5}  {:>14.2}  {:>17}  {:>8.1}  {}",
            cohort + 1,
            expected_trees,
            add_ms_per_doc,
            total_ops,
            sweep_ms,
            by_op_str,
        );
    }

    // The cliff, stated as data: while the collection fits the resident
    // cache a sweep is ~free; past it, every sweep re-hydrates ~everything.
    let first = sweep_loads[0];
    let last = *sweep_loads.last().expect("at least one cohort");
    let last_trees = *trees_at.last().expect("at least one cohort");
    println!(
        "sweep storage ops: {first} at {} trees -> {last} at {last_trees} trees",
        trees_at[0],
    );

    let per_tree_tenths = last * 10 / last_trees.max(1);
    assert!(
        last <= last_trees / MAX_SWEEP_LOADS_FRACTION,
        "one get_all_heads sweep performed {last} storage operations over a \
         {last_trees}-tree collection (~{}.{} per tree; first cohort: {first}). \
         Past the resident-tree cap every sweep re-hydrates the whole collection \
         through a thrashing LRU, so callers that answer periodic collection-state \
         queries with it starve sync — see issue #268.",
        per_tree_tenths / 10,
        per_tree_tenths % 10,
    );

    Ok(())
}
