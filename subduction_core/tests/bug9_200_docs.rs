//! Bug 9 — direct, minimal repro per the bug report:
//! "Sometimes fully in sync peers have heads divergence, happens
//! rarely (if you try to repro, create few hundred docs)."
//!
//! This is the literal scenario: two `Subduction` peers, 200
//! documents, compare `get_all_heads()` outputs after sync.
//!
//! Kept deliberately small and free of the wider harness machinery
//! (no PRNG, no relay topology, no concurrent authors) so a failure
//! here is unambiguous and can be inspected with a normal debugger.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::doc_markdown
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;
type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;
type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
    >,
>;

const NUM_DOCS: usize = 200;

fn make_node(seed: u8) -> TestSubduction {
    let signer = MemorySigner::from_bytes(&[seed; 32]);
    let (sd, _handler, listener, manager, broadcast_seed) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    tokio::spawn(
        sd.clone()
            .run_broadcast_worker_until_aborted(broadcast_seed),
    );
    sd
}

async fn connect(a: &TestSubduction, b: &TestSubduction) -> TestResult {
    let (t_a, t_b) = ChannelTransport::pair();
    let conn_a = MessageTransport::new(t_a);
    let conn_b = MessageTransport::new(t_b);
    let peer_a = a.peer_id();
    let peer_b = b.peer_id();
    a.add_connection(Authenticated::<Conn, Sendable>::new_for_test(conn_a, peer_b))
        .await?;
    b.add_connection(Authenticated::<Conn, Sendable>::new_for_test(conn_b, peer_a))
        .await?;
    Ok(())
}

fn make_commit(sed_id: SedimentreeId, doc_index: usize) -> (LooseCommit, Blob) {
    let payload: Vec<u8> = (0..32u8)
        .map(|b| b.wrapping_add(u8::try_from(doc_index & 0xff).unwrap_or(0)))
        .collect();
    let blob = Blob::new(payload);
    let blob_meta = BlobMeta::new(&blob);
    let mut head = [0u8; 32];
    head[0..8].copy_from_slice(&u64::try_from(doc_index).unwrap_or(u64::MAX).to_be_bytes());
    let commit = LooseCommit::new(
        sed_id,
        CommitId::new(head),
        BTreeSet::new(),
        blob_meta,
    );
    (commit, blob)
}

/// `get_all_heads()` → canonical, order-independent shape:
/// `SedimentreeId` → sorted set of head `CommitId`s.
async fn heads_map(sd: &TestSubduction) -> BTreeMap<SedimentreeId, BTreeSet<CommitId>> {
    sd.get_all_heads()
        .await
        .into_iter()
        .map(|(id, heads)| (id, heads.into_iter().collect()))
        .collect()
}

fn doc_ids(n: usize) -> Vec<SedimentreeId> {
    (0..n)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[0..8].copy_from_slice(&u64::try_from(i).unwrap_or(u64::MAX).to_be_bytes());
            SedimentreeId::new(bytes)
        })
        .collect()
}

/// Pretty-print a small diff so a regression has signal to debug from.
fn assert_heads_agree(
    a: &BTreeMap<SedimentreeId, BTreeSet<CommitId>>,
    b: &BTreeMap<SedimentreeId, BTreeSet<CommitId>>,
    label: &str,
) {
    if a == b {
        return;
    }
    let mut diffs = 0usize;
    for id in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
        let av = a.get(id);
        let bv = b.get(id);
        if av != bv {
            diffs += 1;
            if diffs <= 10 {
                eprintln!("[{label}] DIVERGE doc={id:?}\n  A: {av:?}\n  B: {bv:?}");
            }
        }
    }
    eprintln!("[{label}] total diverging docs: {diffs}");
    panic!("[{label}] A and B disagree on get_all_heads ({diffs} diverging docs)");
}

/// Repeats the 200-doc / 2-peer / get_all_heads scenario 10 times.
/// Each iteration creates a fresh pair of peers, writes 200 docs
/// from A, syncs, and checks that both peers' `get_all_heads()`
/// outputs are **equal as sets** — i.e. for every document, both
/// peers know about it and agree on its set of heads. Order of
/// entries in the returned `Vec` is ignored (it's HashMap iteration
/// order under the hood and not part of the contract).
#[tokio::test(flavor = "current_thread")]
async fn two_peers_200_docs_get_all_heads_match_10x() -> TestResult {
    const ITERATIONS: usize = 10;
    let mut failures: Vec<usize> = Vec::new();

    for iter in 1..=ITERATIONS {
        let a = make_node(u8::try_from((iter * 2) & 0xff).unwrap_or(1));
        let b = make_node(u8::try_from((iter * 2 + 1) & 0xff).unwrap_or(2));
        connect(&a, &b).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let sed_ids = doc_ids(NUM_DOCS);
        for (i, sed_id) in sed_ids.iter().enumerate() {
            let (commit, blob) = make_commit(*sed_id, i);
            a.add_built_batch(*sed_id, vec![(commit, blob)], Vec::new())
                .await?;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        a.full_sync_with_all_peers(Some(Duration::from_secs(5)))
            .await;
        b.full_sync_with_all_peers(Some(Duration::from_secs(5)))
            .await;

        let a_heads = heads_map(&a).await;
        let b_heads = heads_map(&b).await;
        let matches = a_heads == b_heads;

        println!(
            "iter {iter:>2}/{ITERATIONS}: a={a_len:>3} b={b_len:>3} match={matches}",
            a_len = a_heads.len(),
            b_len = b_heads.len(),
        );

        if matches {
            assert_eq!(a_heads.len(), NUM_DOCS, "iter {iter}: A should have {NUM_DOCS} docs");
            assert_eq!(b_heads.len(), NUM_DOCS, "iter {iter}: B should have {NUM_DOCS} docs");
        } else {
            failures.push(iter);
            let mut shown = 0usize;
            for id in a_heads.keys().chain(b_heads.keys()).collect::<BTreeSet<_>>() {
                let av = a_heads.get(id);
                let bv = b_heads.get(id);
                if av != bv && shown < 5 {
                    eprintln!(
                        "  iter {iter} DIVERGE doc={id:?}\n    A: {av:?}\n    B: {bv:?}"
                    );
                    shown += 1;
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {ITERATIONS} iterations had heads divergence: {failures:?}",
        failures.len()
    );

    Ok(())
}

/// Variant 1: writes come only from A. Broadcast worker + a trailing
/// `full_sync_with_all_peers` round.
#[tokio::test(flavor = "current_thread")]
async fn two_peers_200_docs_a_writes_then_sync() -> TestResult {
    let a = make_node(1);
    let b = make_node(2);
    connect(&a, &b).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_ids = doc_ids(NUM_DOCS);
    for (i, sed_id) in sed_ids.iter().enumerate() {
        let (commit, blob) = make_commit(*sed_id, i);
        a.add_built_batch(*sed_id, vec![(commit, blob)], Vec::new())
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    a.full_sync_with_all_peers(Some(Duration::from_secs(5)))
        .await;
    b.full_sync_with_all_peers(Some(Duration::from_secs(5)))
        .await;

    let a_heads = heads_map(&a).await;
    let b_heads = heads_map(&b).await;
    assert_eq!(a_heads.len(), NUM_DOCS);
    assert_heads_agree(&a_heads, &b_heads, "a_writes_then_sync");
    Ok(())
}

/// Variant 2: A and B both write (different doc id ranges), only the
/// background broadcast worker propagates — no explicit
/// `full_sync_with_all_peers` round. This is the closest to the
/// "fully in sync peers" wording in the bug report: no manual
/// reconciliation, just trust the protocol.
#[tokio::test(flavor = "current_thread")]
async fn two_peers_200_docs_both_write_broadcast_only() -> TestResult {
    let a = make_node(1);
    let b = make_node(2);
    connect(&a, &b).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_ids = doc_ids(NUM_DOCS);
    for (i, sed_id) in sed_ids.iter().enumerate() {
        let (commit, blob) = make_commit(*sed_id, i);
        // Interleave: even idx → A writes, odd idx → B writes.
        let writer = if i % 2 == 0 { &a } else { &b };
        writer
            .add_built_batch(*sed_id, vec![(commit, blob)], Vec::new())
            .await?;
    }

    // Give the broadcast worker time to drain its bounded queue.
    // Several short pauses let the listener+ingest pipeline catch up.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let a_heads = heads_map(&a).await;
    let b_heads = heads_map(&b).await;
    assert_heads_agree(&a_heads, &b_heads, "both_write_broadcast_only");
    Ok(())
}

/// Variant 3: bursty writes followed by a tight `get_all_heads` poll
/// loop. Captures the case where a caller reads heads immediately
/// after writing and expects the peer to have caught up "soon".
#[tokio::test(flavor = "current_thread")]
async fn two_peers_200_docs_burst_then_poll_heads() -> TestResult {
    let a = make_node(1);
    let b = make_node(2);
    connect(&a, &b).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_ids = doc_ids(NUM_DOCS);
    // Burst all writes with no awaits between them beyond what
    // `add_built_batch` itself yields.
    for (i, sed_id) in sed_ids.iter().enumerate() {
        let (commit, blob) = make_commit(*sed_id, i);
        a.add_built_batch(*sed_id, vec![(commit, blob)], Vec::new())
            .await?;
    }

    // Poll heads until convergence or timeout.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let a_heads = heads_map(&a).await;
        let b_heads = heads_map(&b).await;
        if a_heads == b_heads && a_heads.len() == NUM_DOCS {
            break;
        }
        if std::time::Instant::now() >= deadline {
            assert_heads_agree(&a_heads, &b_heads, "burst_then_poll_heads");
            panic!("didn't converge in 10s but heads matched? unreachable");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}
