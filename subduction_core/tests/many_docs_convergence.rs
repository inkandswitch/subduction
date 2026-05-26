//! Bug 9 stress harness: with many documents synced across multiple
//! peers, fully in-sync peers should agree on every sedimentree's
//! heads. The bug report says heads sometimes diverge — to reproduce
//! we churn N docs across 2-node and 3-node (A↔R↔B relay) topologies
//! with interleaved writes from both ends, then assert convergence.
//!
//! ## Repro strategy
//!
//! - Seeded PRNG. The seed is printed at test start so a failing CI
//!   run is reproducible.
//! - Each doc gets writes from *both* nodes spawned **concurrently**
//!   via `tokio::spawn`, so commits race the broadcast worker. Each
//!   author keeps its own parent chain so the resulting tree usually
//!   has two heads — that's exactly the shape that exercises
//!   ancestry pruning during sync.
//! - After all writes settle (via repeated `full_sync_with_all_peers`
//!   rounds with bounded backoff), the test asserts that all nodes'
//!   `get_all_heads()` agree.
//!
//! ## Status
//!
//! At time of writing, this harness has not (yet) reproduced the
//! reported divergence even with concurrent forking writers, 200+
//! docs, and 8 settle rounds. It is left `#[ignore]`-gated so the
//! suite is fast by default; run with
//!
//! ```text
//! cargo test --test many_docs_convergence -- --ignored
//! ```
//!
//! Tuning knobs (all read at runtime from env vars):
//!
//! - `BUG9_SEED=<u64>` — pin the PRNG seed
//! - `BUG9_DOCS=<usize>` — number of sedimentrees (default 200)
//! - `BUG9_WRITES=<usize>` — writes per doc per author (default 3)
//! - `BUG9_SETTLE_ROUNDS=<usize>` — full-sync passes after writes (default 8)
//! - `BUG9_AUTHORS_PER_DOC=<1|2>` — 1 = single chain, 2 = forked heads (default 2)

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use future_form::Sendable;
use rand::{Rng, SeedableRng, rngs::StdRng};
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
    peer::id::PeerId,
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

const SYNC_TIMEOUT: Option<Duration> = Some(Duration::from_millis(500));
const SETTLE_PAUSE: Duration = Duration::from_millis(50);
const DEFAULT_NUM_DOCS: usize = 200;
const DEFAULT_WRITES_PER_DOC: usize = 3;
const DEFAULT_NUM_SETTLE_ROUNDS: usize = 8;
const DEFAULT_AUTHORS_PER_DOC: usize = 2;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Knobs {
    num_docs: usize,
    writes_per_doc: usize,
    settle_rounds: usize,
    authors_per_doc: usize,
}

impl Knobs {
    fn from_env() -> Self {
        Self {
            num_docs: env_usize("BUG9_DOCS", DEFAULT_NUM_DOCS),
            writes_per_doc: env_usize("BUG9_WRITES", DEFAULT_WRITES_PER_DOC),
            settle_rounds: env_usize("BUG9_SETTLE_ROUNDS", DEFAULT_NUM_SETTLE_ROUNDS),
            authors_per_doc: env_usize("BUG9_AUTHORS_PER_DOC", DEFAULT_AUTHORS_PER_DOC)
                .clamp(1, 2),
        }
    }
}

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _handler, listener, manager, mut broadcast_seed) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);

    let abort_reg = broadcast_seed
        .take_abort_registration()
        .expect("broadcast worker abort registration consumed twice");
    let sd_for_worker = sd.clone();
    tokio::spawn(async move {
        let worker = sd_for_worker.run_broadcast_worker(broadcast_seed);
        let _ = futures::future::Abortable::new(worker, abort_reg).await;
    });
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

fn make_sed_id(rng: &mut StdRng) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    SedimentreeId::new(bytes)
}

fn choose_seed() -> u64 {
    if let Ok(s) = std::env::var("BUG9_SEED")
        && let Ok(parsed) = s.parse::<u64>()
    {
        return parsed;
    }
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).expect("RNG");
    u64::from_be_bytes(buf)
}

/// Hash a node's view of the world: `SedimentreeId` → sorted heads.
async fn snapshot_heads(sd: &TestSubduction) -> BTreeMap<SedimentreeId, BTreeSet<CommitId>> {
    sd.get_all_heads()
        .await
        .into_iter()
        .map(|(id, heads)| (id, heads.into_iter().collect()))
        .collect()
}

async fn settle(nodes: &[&TestSubduction], rounds: usize) {
    for _ in 0..rounds {
        for n in nodes {
            n.full_sync_with_all_peers(SYNC_TIMEOUT).await;
        }
        tokio::time::sleep(SETTLE_PAUSE).await;
    }
}

/// Spawn `authors_per_doc` writer tasks per doc, each producing
/// `writes_per_doc` commits along its own parent chain. With
/// `authors_per_doc == 2` the resulting tree has two concurrent
/// branches per doc — the shape most likely to expose pruning bugs.
async fn run_concurrent_writers(
    knobs: &Knobs,
    sed_ids: &[SedimentreeId],
    authors: &[TestSubduction],
    rng_seed: u64,
) -> TestResult {
    use std::sync::atomic::{AtomicU64, Ordering};
    let salt = AtomicU64::new(rng_seed);

    let mut joinset = tokio::task::JoinSet::new();
    for (author_idx, sd) in authors.iter().enumerate() {
        for sed_id in sed_ids {
            let sd = sd.clone();
            let sed_id = *sed_id;
            // Each task gets a deterministic-but-distinct seed so the
            // run reproduces across reruns of the same outer seed.
            let task_seed = salt.fetch_add(1, Ordering::Relaxed);
            let writes = knobs.writes_per_doc;
            let local_author_idx = author_idx;
            joinset.spawn(async move {
                let mut local_rng = StdRng::seed_from_u64(task_seed);
                let mut prev_head: Option<CommitId> = None;
                for write_idx in 0..writes {
                    let parents: BTreeSet<CommitId> = prev_head.into_iter().collect();
                    let (commit, blob) =
                        make_commit_pair_deterministic(sed_id, parents, &mut local_rng, local_author_idx, write_idx);
                    let head = commit.head();
                    sd.add_built_batch(sed_id, vec![(commit, blob)], Vec::new())
                        .await
                        .expect("add_built_batch");
                    prev_head = Some(head);
                }
            });
        }
    }
    while let Some(res) = joinset.join_next().await {
        res.expect("writer task panicked");
    }
    Ok(())
}

fn make_commit_pair_deterministic(
    sed_id: SedimentreeId,
    parents: BTreeSet<CommitId>,
    rng: &mut StdRng,
    author_idx: usize,
    write_idx: usize,
) -> (LooseCommit, Blob) {
    // Mix author/write idx into the head so concurrent authors of
    // the same doc cannot accidentally collide.
    let mut data = vec![0u8; 64];
    rng.fill(&mut data[..]);
    data.extend_from_slice(&u64::try_from(author_idx).unwrap_or(u64::MAX).to_be_bytes());
    data.extend_from_slice(&u64::try_from(write_idx).unwrap_or(u64::MAX).to_be_bytes());
    let blob = Blob::new(data);
    let blob_meta = BlobMeta::new(&blob);
    let mut head_bytes = [0u8; 32];
    rng.fill(&mut head_bytes);
    head_bytes[0] ^= u8::try_from(author_idx & 0xff).unwrap_or(0);
    head_bytes[1] ^= u8::try_from(write_idx & 0xff).unwrap_or(0);
    let head = CommitId::new(head_bytes);
    let commit = LooseCommit::new(sed_id, head, parents, blob_meta);
    (commit, blob)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress harness; run explicitly with --ignored"]
async fn two_node_many_docs_converge_on_heads() -> TestResult {
    let seed = choose_seed();
    let knobs = Knobs::from_env();
    println!(
        "BUG9_SEED={seed} docs={} writes={} authors={} rounds={}",
        knobs.num_docs, knobs.writes_per_doc, knobs.authors_per_doc, knobs.settle_rounds
    );
    let mut rng = StdRng::seed_from_u64(seed);

    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());
    connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(SETTLE_PAUSE).await;

    let sed_ids: Vec<SedimentreeId> = (0..knobs.num_docs)
        .map(|_| make_sed_id(&mut rng))
        .collect();

    let authors: Vec<TestSubduction> = if knobs.authors_per_doc == 2 {
        vec![a.clone(), b.clone()]
    } else {
        vec![a.clone()]
    };
    run_concurrent_writers(&knobs, &sed_ids, &authors, seed).await?;

    settle(&[&a, &b], knobs.settle_rounds).await;

    let a_heads = snapshot_heads(&a).await;
    let b_heads = snapshot_heads(&b).await;
    if a_heads != b_heads {
        report_divergence("A", &a_heads, "B", &b_heads);
    }
    assert_eq!(
        a_heads, b_heads,
        "A and B disagree on heads after settle (seed={seed})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress harness; run explicitly with --ignored"]
async fn relay_topology_many_docs_converge_on_heads() -> TestResult {
    let seed = choose_seed();
    let knobs = Knobs::from_env();
    println!(
        "BUG9_SEED={seed} docs={} writes={} authors={} rounds={}",
        knobs.num_docs, knobs.writes_per_doc, knobs.authors_per_doc, knobs.settle_rounds
    );
    let mut rng = StdRng::seed_from_u64(seed);

    let a_signer = make_signer(10);
    let r_signer = make_signer(20);
    let b_signer = make_signer(30);
    let a = make_node(a_signer.clone());
    let r = make_node(r_signer.clone());
    let b = make_node(b_signer.clone());
    connect_pair(&a, &a_signer, &r, &r_signer).await?;
    connect_pair(&r, &r_signer, &b, &b_signer).await?;
    tokio::time::sleep(SETTLE_PAUSE).await;

    let sed_ids: Vec<SedimentreeId> = (0..knobs.num_docs)
        .map(|_| make_sed_id(&mut rng))
        .collect();

    // Always have both endpoints write to maximize forking through R.
    let authors: Vec<TestSubduction> = if knobs.authors_per_doc == 2 {
        vec![a.clone(), b.clone()]
    } else {
        vec![a.clone()]
    };
    run_concurrent_writers(&knobs, &sed_ids, &authors, seed).await?;

    settle(&[&a, &r, &b], knobs.settle_rounds).await;

    let a_heads = snapshot_heads(&a).await;
    let r_heads = snapshot_heads(&r).await;
    let b_heads = snapshot_heads(&b).await;

    if a_heads != r_heads {
        report_divergence("A", &a_heads, "R", &r_heads);
    }
    if r_heads != b_heads {
        report_divergence("R", &r_heads, "B", &b_heads);
    }
    assert_eq!(
        a_heads, r_heads,
        "A and R disagree on heads after settle (seed={seed})"
    );
    assert_eq!(
        r_heads, b_heads,
        "R and B disagree on heads after settle (seed={seed})"
    );

    Ok(())
}

/// Print a small diff of two heads-maps to make divergence
/// debuggable when this harness eventually trips.
fn report_divergence(
    name_l: &str,
    l: &BTreeMap<SedimentreeId, BTreeSet<CommitId>>,
    name_r: &str,
    r: &BTreeMap<SedimentreeId, BTreeSet<CommitId>>,
) {
    let mut diff_count = 0usize;
    for id in l.keys().chain(r.keys()).collect::<BTreeSet<_>>() {
        let lh = l.get(id);
        let rh = r.get(id);
        if lh != rh {
            diff_count += 1;
            if diff_count <= 10 {
                eprintln!(
                    "DIVERGE doc={id:?}\n  {name_l}: {lh:?}\n  {name_r}: {rh:?}"
                );
            }
        }
    }
    eprintln!("DIVERGE total diverging docs: {diff_count}");
}
