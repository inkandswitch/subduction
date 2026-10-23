//! Sync-efficiency budget for the Subduction protocol, end to end.
//!
//! Real `Subduction` nodes talk over in-process `ChannelTransport` pairs
//! wrapped in a recording transport that decodes every frame it sends, so
//! tests can count frames, bytes, heads and the items inside each
//! `BatchSyncResponse`. Histories come from
//! `sedimentree_core::test_utils::sync_budget` (deterministic ids, blobs and
//! signing keys, so byte counts repeat exactly run to run).
//!
//! Each scenario has
//! * a **ratchet** against `tests/sync_budget_baselines.txt` (exact: fails
//!   when a number gets worse, and when it gets better without the baseline
//!   being re-recorded; `UPDATE_SYNC_BASELINES=1` rewrites it). Every
//!   ratchet scenario first checks that the receiver converged, so a lower
//!   number can only mean less redundancy, never less data;
//! * where a known bug breaks the ideal, a **target** test marked
//!   `#[should_panic(expected = "KNOWN BUG <id>:")]` that starts failing
//!   ("did not panic as expected") once the bug is fixed;
//! * where the ideal already holds, a plain **invariant** that guards it.
//!
//! Known causes referenced here:
//! * **C1**: every local save runs a full sync round whose request carries
//!   a fingerprint of every item in the (minimized) tree. The automerge-repo
//!   half (calling a round per save instead of pushing the delta with
//!   `add_commit`) is pinned by the JS suite; the target here can only flip
//!   on a Subduction-side change that makes a round between already-in-sync
//!   peers cost `O(change)`.
//! * **C2**: coverage pruning is broken for automerge-repo's layout
//!   (fragment heads never stored loose), so trees never minimize, and the
//!   commit before each fragment head shows up as a spurious head.
//! * **C13**: `BatchSyncRequest`/`Response` (and `RemoveSubscriptions`)
//!   item counts are `u16` and wrap at 65,536.
//! * **INGEST**: `insert_commit_locally` judges newness with
//!   `has_loose_commit` on the *minimized* tree, so a re-received commit
//!   that a fragment already covers counts as new and is fanned out again.
//!   (It is no longer re-inserted as a stray: the tree remembers the items
//!   minimization dropped.)
//! * **ECHO** (new): a responder asks the requester to send a commit it
//!   already holds inside a fragment, because the set it diffs against is
//!   its minimized tree (loose commits + fragment heads only).
//!
//! Waiting: after each operation the tests wait for the wire to go quiet
//! (no new frame for [`QUIET_FOR`]). That bounds settling only; nothing
//! asserts on latency.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::Duration,
};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
    test_utils::sync_budget::{self, FRAGMENT_PERIOD, History, Layout, SID, known_bug},
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::{
            BatchSyncRequest, BatchSyncResponse, RemoveSubscriptions, RequestId, RequestedData,
            SyncDiff, SyncMessage, SyncResult,
        },
        test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

/// History sizes for end-to-end scenarios (every commit is signed and
/// verified, so these stay smaller than the tree-level suite's). `N` has
/// 1 fragment + a 44-commit tail; `10 * N` has 11 fragments + 184.
const N: usize = 300;

/// Same 44-commit tail as `N`, ten more fragments. Comparing the two
/// isolates growth with fragment count from growth with tail length.
const N_SAME_TAIL: usize = N + 10 * FRAGMENT_PERIOD;

/// Branchy history (merges, depth-2 fragments with checkpoints) for the
/// end-to-end working-path guards.
const BRANCHY_N: usize = 2_100;

/// Ratchet `metric` against this crate's `tests/sync_budget_baselines.txt`.
#[track_caller]
fn ratchet(metric: &str, current: u64) {
    sync_budget::ratchet(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/sync_budget_baselines.txt"
        ),
        metric,
        current,
    );
}

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(10_000);

/// The wire is considered settled after this long with no new frame.
const QUIET_FOR: Duration = Duration::from_millis(60);

/// Give up waiting for quiet after this long (a livelock is a failure).
const SETTLE_DEADLINE: Duration = Duration::from_secs(20);

const fn layout_name(layout: Layout) -> &'static str {
    match layout {
        Layout::AutomergeRepo => "am_layout",
        Layout::Full => "full_layout",
    }
}

// ════════════════════════════════════════════════════════════════════════
// Recording transport
// ════════════════════════════════════════════════════════════════════════

/// One decoded frame, as sent on a labelled link (`"alice->server"`).
#[derive(Debug, Clone)]
struct Frame {
    link: String,
    variant: &'static str,
    bytes: u64,
    /// Heads the frame carries: `responder_heads` of a `BatchSyncResponse`,
    /// `sender_heads` of a push, `heads` of a `HeadsUpdate`.
    heads: usize,
    /// For `BatchSyncResponse`: heads of the loose commits it carries.
    response_commits: Vec<CommitId>,
    /// For `BatchSyncResponse`: heads of the fragments it carries.
    response_fragments: Vec<CommitId>,
    /// For `BatchSyncResponse`: fingerprints it asks the requester to send.
    response_requested: usize,
    /// For `BatchSyncRequest`: fingerprints in the summary.
    request_fingerprints: usize,
}

#[derive(Debug, Clone, Default)]
struct Wire(Arc<Mutex<Vec<Frame>>>);

impl Wire {
    fn record(&self, link: &str, raw: &[u8]) {
        let msg = SyncMessage::try_decode(raw).expect("every frame on the wire decodes");
        let mut frame = Frame {
            link: link.to_owned(),
            variant: msg.variant_name(),
            bytes: raw.len() as u64,
            heads: 0,
            response_commits: Vec::new(),
            response_fragments: Vec::new(),
            response_requested: 0,
            request_fingerprints: 0,
        };
        match &msg {
            SyncMessage::BatchSyncResponse(BatchSyncResponse {
                result,
                responder_heads,
                ..
            }) => {
                frame.heads = responder_heads.heads.len();
                if let SyncResult::Ok(diff) = result {
                    frame.response_commits = diff
                        .missing_commits
                        .iter()
                        .map(|(s, _)| s.try_decode_trusted_payload().expect("commit").head())
                        .collect();
                    frame.response_fragments = diff
                        .missing_fragments
                        .iter()
                        .map(|(s, _)| s.try_decode_trusted_payload().expect("fragment").head())
                        .collect();
                    frame.response_requested = diff.requesting.commit_fingerprints.len()
                        + diff.requesting.fragment_fingerprints.len();
                }
            }
            SyncMessage::BatchSyncRequest(req) => {
                frame.request_fingerprints = req.fingerprint_summary.commit_fingerprints().len()
                    + req.fingerprint_summary.fragment_fingerprints().len();
            }
            SyncMessage::LooseCommit { sender_heads, .. }
            | SyncMessage::Fragment { sender_heads, .. } => {
                frame.heads = sender_heads.heads.len();
            }
            SyncMessage::HeadsUpdate { heads, .. } => frame.heads = heads.heads.len(),
            SyncMessage::RemoveSubscriptions(_) | SyncMessage::DataRequestRejected(_) => {}
        }
        self.0.lock().expect("wire lock").push(frame);
    }

    fn len(&self) -> usize {
        self.0.lock().expect("wire lock").len()
    }

    /// Take everything recorded so far, leaving the record empty.
    fn take(&self) -> Recorded {
        Recorded(std::mem::take(&mut *self.0.lock().expect("wire lock")))
    }

    /// Wait until no frame has been sent for [`QUIET_FOR`].
    async fn settle(&self) {
        let deadline = tokio::time::Instant::now() + SETTLE_DEADLINE;
        let step = Duration::from_millis(10);
        let mut seen = self.len();
        let mut quiet = Duration::ZERO;
        while quiet < QUIET_FOR {
            assert!(
                tokio::time::Instant::now() < deadline,
                "wire never went quiet ({} frames so far)",
                self.len()
            );
            tokio::time::sleep(step).await;
            let now = self.len();
            if now == seen {
                quiet += step;
            } else {
                seen = now;
                quiet = Duration::ZERO;
            }
        }
    }
}

/// Frames recorded during one phase of a scenario.
#[derive(Debug)]
struct Recorded(Vec<Frame>);

impl Recorded {
    fn on<'a>(&'a self, link: &'a str) -> impl Iterator<Item = &'a Frame> + 'a {
        self.0.iter().filter(move |f| f.link == link)
    }

    const fn frames(&self) -> usize {
        self.0.len()
    }

    fn bytes(&self) -> u64 {
        self.0.iter().map(|f| f.bytes).sum()
    }

    fn bytes_on(&self, link: &str) -> u64 {
        self.on(link).map(|f| f.bytes).sum()
    }

    fn frames_of(&self, link: &str, variant: &str) -> usize {
        self.on(link).filter(|f| f.variant == variant).count()
    }

    fn responses_on<'a>(&'a self, link: &'a str) -> impl Iterator<Item = &'a Frame> + 'a {
        self.on(link).filter(|f| f.variant == "BatchSyncResponse")
    }

    fn requests_on<'a>(&'a self, link: &'a str) -> impl Iterator<Item = &'a Frame> + 'a {
        self.on(link).filter(|f| f.variant == "BatchSyncRequest")
    }

    /// One line per (link, variant) for failure messages / `--nocapture`.
    fn breakdown(&self) -> String {
        let mut by: BTreeMap<(&str, &str), (u64, u64)> = BTreeMap::new();
        for f in &self.0 {
            let e = by.entry((&f.link, f.variant)).or_default();
            e.0 += 1;
            e.1 += f.bytes;
        }
        let mut out = String::new();
        for ((l, v), (n, b)) in &by {
            writeln!(out, "  {l:<16} {v:<18} n={n:<5} bytes={b}").expect("write to String");
        }
        out
    }
}

/// `ChannelTransport` that records (and decodes) every frame it sends.
#[derive(Clone, Debug)]
struct Recording {
    inner: ChannelTransport,
    link: Arc<str>,
    wire: Wire,
}

impl PartialEq for Recording {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Transport<Sendable> for Recording {
    type SendError = <ChannelTransport as Transport<Sendable>>::SendError;
    type RecvError = <ChannelTransport as Transport<Sendable>>::RecvError;
    type DisconnectionError = <ChannelTransport as Transport<Sendable>>::DisconnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        self.wire.record(&self.link, bytes);
        self.inner.send_bytes(bytes)
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        self.inner.recv_bytes()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }
}

// ════════════════════════════════════════════════════════════════════════
// Nodes
// ════════════════════════════════════════════════════════════════════════

type Conn = MessageTransport<Recording>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

struct Peer {
    name: &'static str,
    signer: MemorySigner,
    node: Node,
}

impl Peer {
    fn new(name: &'static str, seed: u8) -> Self {
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let (node, _handler, listener, manager) = SubductionBuilder::new()
            .signer(signer.clone())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, Conn>();
        tokio::spawn(listener);
        tokio::spawn(manager);
        Self { name, signer, node }
    }

    fn id(&self) -> PeerId {
        PeerId::from(self.signer.verifying_key())
    }

    /// Store `history` under `layout` without touching the network.
    async fn seed(&self, history: &History, layout: Layout) {
        self.node
            .store_built_batch(SID, history.loose(layout), history.fragments.clone())
            .await
            .expect("store_built_batch");
    }

    /// Dial `other` (this side is the dialer).
    async fn connect(&self, other: &Peer, wire: &Wire) {
        let (t_self, t_other) = ChannelTransport::pair();
        let conn_self = MessageTransport::new(Recording {
            inner: t_self,
            link: format!("{}->{}", self.name, other.name).into(),
            wire: wire.clone(),
        });
        let conn_other = MessageTransport::new(Recording {
            inner: t_other,
            link: format!("{}->{}", other.name, self.name).into(),
            wire: wire.clone(),
        });
        let auth_self: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(conn_self, other.id(), Direction::Dialed);
        let auth_other: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(conn_other, self.id(), Direction::Accepted);
        self.node.add_connection(auth_self).await.expect("connect");
        other
            .node
            .add_connection(auth_other)
            .await
            .expect("connect");
    }

    /// One sync round with `other`, subscribing, then wait for the wire.
    async fn sync_with(&self, other: &Peer, wire: &Wire) {
        let (ok, _stats, errs) = self
            .node
            .sync_with_peer(&other.id(), SID, true, SYNC_TIMEOUT)
            .await
            .expect("sync_with_peer");
        assert!(
            ok && errs.is_empty(),
            "{} failed to sync with {}",
            self.name,
            other.name
        );
        wire.settle().await;
    }

    /// Author commit `commit` the incremental way: store it and push it to
    /// subscribers (`Subduction::add_commit`), then wait for the wire.
    async fn add_commit(&self, (commit, blob): (LooseCommit, Blob), wire: &Wire) {
        self.node
            .add_commit(SID, commit.head(), commit.parents().clone(), blob)
            .await
            .expect("add_commit");
        wire.settle().await;
    }

    async fn loose_ids(&self) -> BTreeSet<CommitId> {
        self.node
            .get_commits(SID)
            .await
            .unwrap_or_default()
            .iter()
            .map(LooseCommit::head)
            .collect()
    }

    async fn fragment_heads(&self) -> BTreeSet<CommitId> {
        self.node
            .get_fragments(SID)
            .await
            .unwrap_or_default()
            .iter()
            .map(Fragment::head)
            .collect()
    }

    async fn heads(&self) -> BTreeSet<CommitId> {
        self.node
            .get_heads(SID)
            .await
            .expect("get_heads")
            .unwrap_or_default()
            .into_iter()
            .collect()
    }
}

/// Nothing lost: `peer` holds every fragment `reference` holds, every tail
/// commit, and the same heads. In the full layout (where minimize works)
/// its loose commits are exactly the tail.
async fn assert_converged(peer: &Peer, reference: &Peer, history: &History, layout: Layout) {
    let what = format!("{} vs {} ({layout:?})", peer.name, reference.name);
    assert_eq!(
        peer.fragment_heads().await,
        reference.fragment_heads().await,
        "{what}: fragments differ"
    );
    let loose = peer.loose_ids().await;
    if layout == Layout::Full {
        assert_eq!(
            loose,
            history.tail(),
            "{what}: loose commits are not the tail"
        );
    } else {
        assert!(loose.is_superset(&history.tail()), "{what}: tail missing");
    }
    assert_eq!(
        peer.heads().await,
        reference.heads().await,
        "{what}: heads differ"
    );
}

// ════════════════════════════════════════════════════════════════════════
// Scenario: cold sync (empty client subscribes to a server holding history)
// ════════════════════════════════════════════════════════════════════════

struct ColdSync {
    recorded: Recorded,
    /// Loose commits in the response whose payload a fragment in the same
    /// response already carries.
    redundant: usize,
    /// Payload actually needed: every kept fragment blob + every tail blob.
    ideal_payload: u64,
}

async fn cold_sync(history: &History, layout: Layout) -> ColdSync {
    let wire = Wire::default();
    let server = Peer::new("server", 1);
    let bob = Peer::new("bob", 3);
    server.seed(history, layout).await;
    bob.connect(&server, &wire).await;
    wire.settle().await;
    drop(wire.take());

    bob.sync_with(&server, &wire).await;
    let recorded = wire.take();
    assert_converged(&bob, &server, history, layout).await;

    let (commits, fragments): (Vec<CommitId>, Vec<CommitId>) = recorded
        .responses_on("server->bob")
        .fold((Vec::new(), Vec::new()), |(mut c, mut f), r| {
            c.extend(&r.response_commits);
            f.extend(&r.response_fragments);
            (c, f)
        });
    let redundant = history.redundant(&commits, &fragments).len();

    let tail = history.tail();
    let top = history.top_fragments();
    let ideal_payload = history
        .fragments
        .iter()
        .filter(|(f, _)| top.contains(&f.head()))
        .map(|(_, b)| b.as_slice().len() as u64)
        .chain(
            history
                .commits
                .iter()
                .filter(|(c, _)| tail.contains(&c.head()))
                .map(|(_, b)| b.as_slice().len() as u64),
        )
        .sum();

    ColdSync {
        recorded,
        redundant,
        ideal_payload,
    }
}

/// Ideal: a cold fetch ships each fragment once and only the tail as loose
/// commits, never a commit that a fragment in the same response carries.
/// Holds for the full layout today, linear and branchy; guard.
#[tokio::test]
async fn full_layout_cold_sync_ships_no_covered_commits() {
    for history in [History::linear(10 * N), History::branchy(BRANCHY_N)] {
        let cold = cold_sync(&history, Layout::Full).await;
        assert_eq!(
            cold.redundant,
            0,
            "{:?}: cold sync shipped covered commits twice:\n{}",
            history.shape,
            cold.recorded.breakdown()
        );
    }
}

/// Ideal: as above, for the layout automerge-repo stores. Today the server's
/// tree never minimizes (C2), so the response carries every change both as
/// a loose commit and inside its fragment.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C2:")]
async fn am_layout_cold_sync_ships_no_covered_commits() {
    let history = History::linear(10 * N);
    let cold = cold_sync(&history, Layout::AutomergeRepo).await;
    known_bug(
        "C2",
        cold.redundant == 0,
        &format!(
            "cold sync of {} commits shipped {} commits both loose and inside a fragment; \
             {} wire bytes for {} payload bytes",
            history.len(),
            cold.redundant,
            cold.recorded.bytes(),
            cold.ideal_payload
        ),
    );
}

/// Ratchet: cold-sync wire bytes, frames, redundant commits, and
/// wire/payload overhead (x100; C5 lives here) at `N` and `10 * N`, for both
/// layouts (the full layout pins drift on the working path, e.g. envelope
/// growth).
#[tokio::test]
async fn cold_sync_ratchet() {
    for layout in [Layout::AutomergeRepo, Layout::Full] {
        for n in [N, 10 * N] {
            let history = History::linear(n);
            let cold = cold_sync(&history, layout).await;
            eprintln!("cold sync {layout:?} n={n}:\n{}", cold.recorded.breakdown());
            let m =
                |what: &str| format!("subduction.{}.cold_sync.{what}.n{n}", layout_name(layout));
            ratchet(&m("bytes"), cold.recorded.bytes());
            ratchet(&m("frames"), cold.recorded.frames() as u64);
            ratchet(&m("redundant_loose"), cold.redundant as u64);
            ratchet(
                &m("wire_per_payload_x100"),
                cold.recorded.bytes() * 100 / cold.ideal_payload,
            );
        }
    }
}

// ════════════════════════════════════════════════════════════════════════
// Scenario: steady state (in-sync rounds and one incremental commit)
// ════════════════════════════════════════════════════════════════════════

/// Server, alice (author) and bob (subscriber), all holding `history` and
/// all subscribed. Returns with the wire quiet and the record empty.
async fn steady_state(history: &History, layout: Layout) -> (Wire, Peer, Peer, Peer) {
    let wire = Wire::default();
    let server = Peer::new("server", 1);
    let alice = Peer::new("alice", 2);
    let bob = Peer::new("bob", 3);
    for p in [&server, &alice, &bob] {
        p.seed(history, layout).await;
    }
    alice.connect(&server, &wire).await;
    bob.connect(&server, &wire).await;
    alice.sync_with(&server, &wire).await;
    bob.sync_with(&server, &wire).await;
    drop(wire.take());
    (wire, server, alice, bob)
}

struct InSync {
    request_bytes: u64,
    request_fingerprints: usize,
    response_bytes: u64,
    response_heads: usize,
    response_items: usize,
    /// Every frame on every link during the round.
    recorded: Recorded,
}

async fn in_sync_round(history: &History, layout: Layout) -> InSync {
    let (wire, server, alice, bob) = steady_state(history, layout).await;
    alice.sync_with(&server, &wire).await;
    let recorded = wire.take();
    assert_converged(&alice, &server, history, layout).await;
    assert_converged(&bob, &server, history, layout).await;
    let req: Vec<&Frame> = recorded.requests_on("alice->server").collect();
    let resp: Vec<&Frame> = recorded.responses_on("server->alice").collect();
    assert_eq!(
        (req.len(), resp.len()),
        (1, 1),
        "one round:\n{}",
        recorded.breakdown()
    );
    InSync {
        request_bytes: req[0].bytes,
        request_fingerprints: req[0].request_fingerprints,
        response_bytes: resp[0].bytes,
        response_heads: resp[0].heads,
        response_items: resp[0].response_commits.len()
            + resp[0].response_fragments.len()
            + resp[0].response_requested,
        recorded,
    }
}

/// Ideal: a sync round between peers that are already in sync is exactly
/// one request and one empty response: no data either way, nothing asked
/// for, and no other frame on any link (no pushes, no `HeadsUpdate`, no
/// second round). Holds today in both layouts; guard.
#[tokio::test]
async fn in_sync_round_is_one_empty_exchange() {
    for layout in [Layout::Full, Layout::AutomergeRepo] {
        for n in [N, 10 * N] {
            let r = in_sync_round(&History::linear(n), layout).await;
            assert_eq!(
                r.response_items, 0,
                "{layout:?} n={n}: in-sync response carried items"
            );
            assert_eq!(
                r.recorded.frames(),
                2,
                "{layout:?} n={n}: in-sync round sent more than request + response:\n{}",
                r.recorded.breakdown()
            );
        }
    }
}

/// Ideal: the in-sync response lists exactly one head (a linear history
/// has one) and so costs the same whatever the history length. Holds for
/// the full layout; guard.
#[tokio::test]
async fn full_layout_in_sync_response_lists_one_head() {
    let small = in_sync_round(&History::linear(N), Layout::Full).await;
    let large = in_sync_round(&History::linear(10 * N), Layout::Full).await;
    assert_eq!((small.response_heads, large.response_heads), (1, 1));
    assert_eq!(small.response_bytes, large.response_bytes);
}

/// Ideal: as above, for automerge-repo's layout. Today the response's
/// `responder_heads` lists one spurious head per fragment: the commit just
/// before each fragment head is referenced by no loose commit (its child is
/// stored only as a fragment, C2) and fragments don't list their members,
/// so `heads_assuming_minimal` reports it as a head. Every response, push
/// and `HeadsUpdate` carries those 32-byte ids.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C2:")]
async fn am_layout_in_sync_response_lists_one_head() {
    let history = History::linear(10 * N);
    let r = in_sync_round(&history, Layout::AutomergeRepo).await;
    known_bug(
        "C2",
        r.response_heads == 1,
        &format!(
            "in-sync response for {} commits / {} fragments lists {} heads ({} bytes)",
            history.len(),
            history.fragments.len(),
            r.response_heads,
            r.response_bytes
        ),
    );
}

/// Ratchet: in-sync round request/response bytes, plus total bytes and
/// frames on every link, both layouts.
#[tokio::test]
async fn in_sync_round_ratchet() {
    for layout in [Layout::AutomergeRepo, Layout::Full] {
        for n in [N, 10 * N] {
            let r = in_sync_round(&History::linear(n), layout).await;
            let m = |what: &str| {
                format!(
                    "subduction.{}.in_sync_round.{what}.n{n}",
                    layout_name(layout)
                )
            };
            ratchet(&m("request_bytes"), r.request_bytes);
            ratchet(&m("response_bytes"), r.response_bytes);
            ratchet(&m("total_bytes"), r.recorded.bytes());
            ratchet(&m("frames"), r.recorded.frames() as u64);
        }
    }
}

/// Wire size of a `BatchSyncRequest` carrying `items` fingerprints:
/// envelope (9) + id (32), requestor (32), nonce (8), subscribe (1),
/// seed (16), two u16 counts (4) + 8 per fingerprint.
const fn request_bytes_for(items: usize) -> u64 {
    (9 + 93 + 8 * items) as u64
}

/// Ideal: the request of an in-sync round costs `O(fragments + tail)`:
/// one fingerprint per tail commit and two per fragment. Full layout
/// achieves exactly that today; guard.
#[tokio::test]
async fn full_layout_in_sync_request_is_fragments_plus_tail() {
    let history = History::linear(10 * N);
    let ideal = history.tail().len() + 2 * history.top_fragments().len();
    let r = in_sync_round(&history, Layout::Full).await;
    assert_eq!(r.request_fingerprints, ideal);
    assert_eq!(r.request_bytes, request_bytes_for(ideal));
}

/// Ideal: as above for automerge-repo's layout. Today the request carries
/// one fingerprint per commit in history (C2 keeps every loose commit), and
/// C1 sends one such request per throttled local save.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C1/C2:")]
async fn am_layout_in_sync_request_is_fragments_plus_tail() {
    let history = History::linear(10 * N);
    let ideal = history.tail().len() + 2 * history.top_fragments().len();
    let r = in_sync_round(&history, Layout::AutomergeRepo).await;
    known_bug(
        "C1/C2",
        r.request_fingerprints <= ideal,
        &format!(
            "in-sync request for {} commits carries {} fingerprints ({} bytes); ideal {ideal} \
             ({} bytes)",
            history.len(),
            r.request_fingerprints,
            r.request_bytes,
            request_bytes_for(ideal)
        ),
    );
}

struct Incremental {
    /// Everything alice put on the wire for one new commit.
    upload: u64,
    /// Everything on the server<->bob link (the fan-out to a subscriber).
    fanout: u64,
    /// Everything on every link.
    total: u64,
    recorded: Recorded,
}

/// How alice publishes one new commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publish {
    /// automerge-repo today (C1): a durable `store_*` write followed by a
    /// full `sync_with_peer` round.
    StoreThenSync,
    /// `Subduction::add_commit`: store and push the delta to subscribers.
    AddCommit,
}

/// Alice authors one commit and the server fans it out to bob.
async fn incremental_commit(history: &History, layout: Layout, how: Publish) -> Incremental {
    let (wire, server, alice, bob) = steady_state(history, layout).await;
    let next = history.next_commit();
    match how {
        Publish::StoreThenSync => {
            alice
                .node
                .store_built_batch(SID, vec![next.clone()], Vec::new())
                .await
                .expect("store new commit");
            alice.sync_with(&server, &wire).await;
        }
        Publish::AddCommit => alice.add_commit(next.clone(), &wire).await,
    }
    let recorded = wire.take();

    for p in [&alice, &bob] {
        assert!(
            p.loose_ids().await.contains(&next.0.head()),
            "sanity: {} must hold the new commit",
            p.name
        );
        assert_eq!(
            p.heads().await,
            server.heads().await,
            "sanity: {} converged with the server",
            p.name
        );
    }
    assert!(
        server.heads().await.contains(&next.0.head()),
        "sanity: the new commit is a head"
    );

    Incremental {
        upload: recorded.bytes_on("alice->server"),
        fanout: recorded.bytes_on("server->bob") + recorded.bytes_on("bob->server"),
        total: recorded.bytes(),
        recorded,
    }
}

/// Ideal: pushing one new commit to a subscriber costs the same whatever
/// the history length (the push carries the commit and the sender's
/// heads). Holds for the full layout; guard.
#[tokio::test]
async fn full_layout_incremental_fanout_is_history_independent() {
    let small = incremental_commit(&History::linear(N), Layout::Full, Publish::StoreThenSync).await;
    let large = incremental_commit(
        &History::linear(10 * N),
        Layout::Full,
        Publish::StoreThenSync,
    )
    .await;
    assert_eq!(
        small.fanout,
        large.fanout,
        "fan-out bytes grew with history:\nN:\n{}10N:\n{}",
        small.recorded.breakdown(),
        large.recorded.breakdown()
    );
}

/// Ideal: as above, for automerge-repo's layout. Today the pushed
/// `LooseCommit`'s `sender_heads` and the subscriber's `HeadsUpdate` ack
/// both carry one spurious head per fragment (see
/// `am_layout_in_sync_response_lists_one_head`), so fan-out grows with
/// history.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C2:")]
async fn am_layout_incremental_fanout_is_history_independent() {
    let small = incremental_commit(
        &History::linear(N),
        Layout::AutomergeRepo,
        Publish::StoreThenSync,
    )
    .await;
    let large = incremental_commit(
        &History::linear(10 * N),
        Layout::AutomergeRepo,
        Publish::StoreThenSync,
    )
    .await;
    known_bug(
        "C2",
        small.fanout == large.fanout,
        &format!(
            "fan-out of one commit: {} B at {N} commits, {} B at {} commits (spurious heads)",
            small.fanout,
            large.fanout,
            10 * N
        ),
    );
}

/// Ideal: publishing one commit through `add_commit` (the delta push) costs
/// the author the same whatever the history length. Holds for the full
/// layout; guards the path automerge-repo should use instead of a round per
/// save (C1).
#[tokio::test]
async fn full_layout_add_commit_upload_is_history_independent() {
    let small = incremental_commit(&History::linear(N), Layout::Full, Publish::AddCommit).await;
    let large =
        incremental_commit(&History::linear(10 * N), Layout::Full, Publish::AddCommit).await;
    assert_eq!(
        (small.upload, small.fanout),
        (large.upload, large.fanout),
        "add_commit upload/fan-out grew with history:\nN:\n{}10N:\n{}",
        small.recorded.breakdown(),
        large.recorded.breakdown()
    );
}

/// Ideal: a full sync round run to publish one commit between peers that
/// were already in sync costs `O(change)`, not `O(fragments + tail)`.
///
/// Isolated from C2 (full layout, where minimize works) and from tail
/// length (`N` and `N_SAME_TAIL` have the same 44-commit tail; the larger
/// has ten more fragments). Today the request re-sends two fingerprints per
/// fragment, so the upload grows with history (C1, Subduction side). This
/// flips only if Subduction makes such a round incremental; the
/// automerge-repo half of C1 (use `add_commit`, guarded above) is pinned by
/// the JS suite.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C1:")]
async fn full_layout_sync_round_upload_is_history_independent() {
    let small = incremental_commit(&History::linear(N), Layout::Full, Publish::StoreThenSync).await;
    let large = incremental_commit(
        &History::linear(N_SAME_TAIL),
        Layout::Full,
        Publish::StoreThenSync,
    )
    .await;
    known_bug(
        "C1",
        large.upload <= small.upload,
        &format!(
            "store + sync_with_peer upload for one {}-byte change: {} B with {} fragments, {} B \
             with {} fragments (same tail)",
            sync_budget::COMMIT_BLOB_LEN,
            small.upload,
            N / FRAGMENT_PERIOD,
            large.upload,
            N_SAME_TAIL / FRAGMENT_PERIOD
        ),
    );
}

/// Ratchet: bytes for one incremental commit (upload, fan-out, total) at
/// `N` and `10 * N`, for both layouts and both ways of publishing, plus the
/// automerge-repo pattern's upload growth factor (x100).
#[tokio::test]
async fn incremental_commit_ratchet() {
    for layout in [Layout::AutomergeRepo, Layout::Full] {
        for how in [Publish::StoreThenSync, Publish::AddCommit] {
            let mut uploads = Vec::new();
            let path = match how {
                Publish::StoreThenSync => "incremental_commit",
                Publish::AddCommit => "add_commit",
            };
            for n in [N, 10 * N] {
                let inc = incremental_commit(&History::linear(n), layout, how).await;
                eprintln!("{path} {layout:?} n={n}:\n{}", inc.recorded.breakdown());
                for (what, v) in [
                    ("upload", inc.upload),
                    ("fanout", inc.fanout),
                    ("total", inc.total),
                ] {
                    ratchet(
                        &format!(
                            "subduction.{}.{path}.{what}_bytes.n{n}",
                            layout_name(layout)
                        ),
                        v,
                    );
                }
                uploads.push(inc.upload);
            }
            if layout == Layout::AutomergeRepo && how == Publish::StoreThenSync {
                ratchet(
                    "subduction.am_layout.incremental_commit.upload_growth_10x_history_x100",
                    uploads[1] * 100 / uploads[0],
                );
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════
// Scenario: a commit the server already holds is received again
// ════════════════════════════════════════════════════════════════════════

/// Server holds the full layout (so minimize really prunes members); bob is
/// subscribed.
async fn covered_server(history: &History) -> (Wire, Peer, Peer) {
    let wire = Wire::default();
    let server = Peer::new("server", 1);
    let bob = Peer::new("bob", 3);
    server.seed(history, Layout::Full).await;
    assert_eq!(
        server.loose_ids().await,
        history.tail(),
        "precondition: the server's tree is minimized (covered commits pruned)"
    );
    bob.connect(&server, &wire).await;
    bob.sync_with(&server, &wire).await;
    (wire, server, bob)
}

/// A commit in the middle of the first fragment.
const COVERED: usize = 5;

struct PushedCovered {
    /// `LooseCommit` frames the server fans out to bob.
    rebroadcast: usize,
    /// Loose commits the server holds beyond the tail afterwards.
    stray: usize,
    recorded: Recorded,
}

/// INGEST in isolation: carol (in sync, subscribed) pushes, via
/// `add_commit`, a commit that a server fragment already covers. No sync
/// round is involved, so the responder's diff (ECHO) plays no part.
async fn push_covered_commit() -> PushedCovered {
    let history = History::linear(N);
    // `_bob` (not `_`) keeps the subscriber connected until the end.
    let (wire, server, _bob) = covered_server(&history).await;
    let carol = Peer::new("carol", 4);
    carol.connect(&server, &wire).await;
    carol.sync_with(&server, &wire).await;
    drop(wire.take());

    let covered = history.commit(COVERED);
    assert!(history.covered().contains(&covered.0.head()));
    carol.add_commit(covered, &wire).await;
    let recorded = wire.take();
    assert_eq!(
        recorded.frames_of("carol->server", "LooseCommit"),
        1,
        "precondition: carol pushed the commit:\n{}",
        recorded.breakdown()
    );

    let loose = server.loose_ids().await;
    assert!(loose.is_superset(&history.tail()), "server lost its tail");
    PushedCovered {
        rebroadcast: recorded.frames_of("server->bob", "LooseCommit"),
        stray: loose.len() - history.tail().len(),
        recorded,
    }
}

/// Ideal: a commit the server already holds inside a fragment is not new,
/// so receiving it again must not be fanned out to subscribers. Today
/// `insert_commit_locally` checks `has_loose_commit` against the minimized
/// tree, where covered commits were pruned, so the commit is reported as
/// new and broadcast (INGEST). Once C2 is fixed for automerge-repo's
/// layout, this is what turns re-sends into fan-out storms.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG INGEST:")]
async fn covered_commit_push_is_not_rebroadcast() {
    let r = push_covered_commit().await;
    known_bug(
        "INGEST",
        r.rebroadcast == 0,
        &format!(
            "server re-broadcast {} already-covered commit(s) to its subscriber:\n{}",
            r.rebroadcast,
            r.recorded.breakdown()
        ),
    );
}

/// Ideal: receiving a covered commit leaves the server's minimized tree
/// unchanged. Re-added, the commit would be a stray loose commit that
/// `minimize` cannot prune (no loose path to its fragment's head), riding
/// in every later summary and cold sync. The tree remembers what it holds,
/// so the commit is not re-added; guard.
#[tokio::test]
async fn covered_commit_push_leaves_no_stray_loose_commit() {
    let r = push_covered_commit().await;
    assert_eq!(
        r.stray, 0,
        "server keeps {} stray covered loose commit(s) after the push",
        r.stray
    );
}

/// ECHO in isolation: carol holds only a covered commit and runs a sync
/// round with the server.
async fn sync_covered_commit() -> Recorded {
    let history = History::linear(N);
    let (wire, server, _bob) = covered_server(&history).await;
    let carol = Peer::new("carol", 4);
    carol
        .node
        .store_built_batch(SID, vec![history.commit(COVERED)], Vec::new())
        .await
        .expect("store covered commit");
    carol.connect(&server, &wire).await;
    wire.settle().await;
    drop(wire.take());

    carol.sync_with(&server, &wire).await;
    wire.take()
}

/// Ideal: the server does not ask carol for a commit it already holds
/// inside a fragment. Today it does (ECHO): the set it diffs against is
/// its minimized tree, which holds loose commits and fragment heads but
/// not fragment members, so carol's fingerprint looks unknown. (The upload
/// then trips INGEST too; that is measured separately above.)
#[tokio::test]
#[should_panic(expected = "KNOWN BUG ECHO:")]
async fn covered_commit_fingerprint_is_not_requested() {
    let recorded = sync_covered_commit().await;
    let requested: usize = recorded
        .responses_on("server->carol")
        .map(|r| r.response_requested)
        .sum();
    known_bug(
        "ECHO",
        requested == 0,
        &format!(
            "server asked carol to upload {requested} commit(s) it already holds in a \
             fragment:\n{}",
            recorded.breakdown()
        ),
    );
}

/// Ratchet: frames spent on a covered commit. Push path (INGEST):
/// re-broadcast frames and stray loose commits left on the server. Sync
/// path (ECHO, then INGEST): carol's uploaded frames and the re-broadcast.
/// Ideal 0 for all four.
#[tokio::test]
async fn covered_commit_ratchet() {
    let pushed = push_covered_commit().await;
    ratchet(
        "subduction.full_layout.covered_push.rebroadcast_loose_frames",
        pushed.rebroadcast as u64,
    );
    ratchet(
        "subduction.full_layout.covered_push.server_stray_loose",
        pushed.stray as u64,
    );
    let synced = sync_covered_commit().await;
    ratchet(
        "subduction.full_layout.covered_resend.uploaded_loose_frames",
        synced.frames_of("carol->server", "LooseCommit") as u64,
    );
    ratchet(
        "subduction.full_layout.covered_resend.rebroadcast_loose_frames",
        synced.frames_of("server->bob", "LooseCommit") as u64,
    );
}

/// Ideal: a commit pushed twice reaches each subscriber once; the second
/// copy is recognized as known and not fanned out again. Holds today (the
/// commit is loose on the server); guard. Overlapping rounds (C7) are the
/// real-world way duplicates arrive.
#[tokio::test]
async fn duplicate_push_is_not_rebroadcast() {
    let history = History::linear(N);
    let (wire, server, alice, bob) = steady_state(&history, Layout::Full).await;
    let next = history.next_commit();
    alice.add_commit(next.clone(), &wire).await;
    alice.add_commit(next, &wire).await;
    let recorded = wire.take();
    assert_eq!(
        recorded.frames_of("alice->server", "LooseCommit"),
        2,
        "precondition: alice pushed twice:\n{}",
        recorded.breakdown()
    );
    assert_eq!(
        recorded.frames_of("server->bob", "LooseCommit"),
        1,
        "the duplicate was fanned out again:\n{}",
        recorded.breakdown()
    );
    assert_eq!(bob.heads().await, server.heads().await);
}

// ════════════════════════════════════════════════════════════════════════
// Codec: item counts must not wrap at u16::MAX + 1
// ════════════════════════════════════════════════════════════════════════

fn fingerprints(n: usize) -> BTreeSet<Fingerprint<CommitId>> {
    (0..n as u64).map(Fingerprint::from_u64).collect()
}

const fn req_id() -> RequestId {
    RequestId {
        requestor: PeerId::new([7; 32]),
        nonce: 1,
    }
}

fn request_with(commit_fps: usize, fragment_fps: usize) -> SyncMessage {
    SyncMessage::BatchSyncRequest(BatchSyncRequest {
        id: SID,
        req_id: req_id(),
        fingerprint_summary: FingerprintSummary::new(
            FingerprintSeed::new(1, 2),
            fingerprints(commit_fps),
            fingerprints(fragment_fps),
        ),
        subscribe: true,
    })
}

const fn response_with(diff: SyncDiff) -> SyncMessage {
    SyncMessage::BatchSyncResponse(BatchSyncResponse {
        req_id: req_id(),
        id: SID,
        result: SyncResult::Ok(diff),
        responder_heads: RemoteHeads {
            counter: 0,
            heads: Vec::new(),
        },
    })
}

fn response_requesting(commit_fps: usize) -> SyncMessage {
    response_with(SyncDiff {
        missing_commits: Vec::new(),
        missing_fragments: Vec::new(),
        requesting: RequestedData {
            commit_fingerprints: fingerprints(commit_fps).into_iter().collect(),
            fragment_fingerprints: Vec::new(),
        },
    })
}

/// A response carrying `n` copies of one tiny signed commit.
async fn response_with_commits(n: usize) -> SyncMessage {
    let blob = Blob::new(vec![1]);
    let commit = LooseCommit::new(
        SID,
        CommitId::new([0xC1; 32]),
        BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(&MemorySigner::from_bytes(&[9; 32]), commit)
        .await
        .into_signed();
    response_with(SyncDiff {
        missing_commits: vec![(signed, blob); n],
        missing_fragments: Vec::new(),
        requesting: RequestedData {
            commit_fingerprints: Vec::new(),
            fragment_fingerprints: Vec::new(),
        },
    })
}

/// A response carrying `n` copies of one tiny signed fragment.
async fn response_with_fragments(n: usize) -> SyncMessage {
    let blob = Blob::new(vec![1]);
    let fragment = Fragment::new(
        SID,
        CommitId::new([0; 32]),
        BTreeSet::new(),
        &[],
        BlobMeta::new(&blob),
    );
    let signed = Signed::seal::<Sendable, _>(&MemorySigner::from_bytes(&[9; 32]), fragment)
        .await
        .into_signed();
    response_with(SyncDiff {
        missing_commits: Vec::new(),
        missing_fragments: vec![(signed, blob); n],
        requesting: RequestedData {
            commit_fingerprints: Vec::new(),
            fragment_fingerprints: Vec::new(),
        },
    })
}

fn remove_subscriptions(n: usize) -> SyncMessage {
    SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
        ids: (0..n as u64)
            .map(|i| {
                let mut b = [0u8; 32];
                b[..8].copy_from_slice(&i.to_le_bytes());
                SedimentreeId::new(b)
            })
            .collect(),
    })
}

/// A message survives the codec if it round-trips exactly or is rejected;
/// decoding to a *different* message is silent corruption.
fn never_silently_altered(msg: &SyncMessage) -> bool {
    SyncMessage::try_decode(&msg.encode()).map_or(true, |decoded| decoded == *msg)
}

/// At the largest count a `u16` holds, every counted field round-trips;
/// guard.
#[tokio::test]
async fn sync_messages_with_65535_items_round_trip() {
    for msg in [
        request_with(65_535, 0),
        request_with(0, 65_535),
        response_requesting(65_535),
        response_with_commits(65_535).await,
        response_with_fragments(65_535).await,
        remove_subscriptions(65_535),
    ] {
        let decoded = SyncMessage::try_decode(&msg.encode()).expect("decodes");
        assert!(decoded == msg, "{} did not round-trip", msg.variant_name());
    }
}

/// Ideal: a `BatchSyncRequest` summarizing 65,536+ items round-trips (or
/// fails loudly). Today the `u16` count wraps to 0 and the receiver decodes
/// a different summary without error (C13).
#[test]
#[should_panic(expected = "KNOWN BUG C13:")]
fn batch_sync_request_with_65536_fingerprints_never_wraps() {
    let msg = request_with(65_536, 1);
    known_bug(
        "C13",
        never_silently_altered(&msg),
        "BatchSyncRequest with 65,536 commit fingerprints decoded to a different summary",
    );
}

/// Ideal: as above for the fingerprints a `BatchSyncResponse` requests back.
#[test]
#[should_panic(expected = "KNOWN BUG C13:")]
fn batch_sync_response_with_65536_requested_never_wraps() {
    let msg = response_requesting(65_536);
    known_bug(
        "C13",
        never_silently_altered(&msg),
        "BatchSyncResponse requesting 65,536 commits decoded to a different request",
    );
}

/// What decoding an encoded message gives back, for failure messages.
fn decode_outcome(msg: &SyncMessage) -> String {
    match SyncMessage::try_decode(&msg.encode()) {
        Ok(decoded) if decoded == *msg => "round-trips".to_owned(),
        Ok(_) => "decodes to a DIFFERENT message".to_owned(),
        Err(e) => format!("is rejected ({e})"),
    }
}

/// A response carrying 65,536+ commits or fragments is at least never
/// silently corrupted today: the wrapped count leaves the payload bytes
/// where the decoder expects the responder heads, and it rejects them.
/// Guard, so the C13 targets below cannot hide a change to silent
/// corruption (which they would still count as "not fixed").
#[tokio::test]
async fn batch_sync_response_with_65536_items_is_not_silently_altered() {
    for msg in [
        response_with_commits(65_536).await,
        response_with_fragments(65_536).await,
    ] {
        assert!(never_silently_altered(&msg), "{}", decode_outcome(&msg));
    }
}

/// Ideal: a `BatchSyncResponse` carrying 65,536+ commits round-trips. That
/// is a cold sync of a doc with that many loose commits, which C2 makes
/// realistic. Today the `u16` count wraps and the receiver rejects the
/// response, so such a doc cannot be cold-synced at all (C13).
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C13:")]
async fn batch_sync_response_with_65536_commits_round_trips() {
    let msg = response_with_commits(65_536).await;
    known_bug(
        "C13",
        SyncMessage::try_decode(&msg.encode()).is_ok_and(|d| d == msg),
        &format!(
            "BatchSyncResponse carrying 65,536 commits {}",
            decode_outcome(&msg)
        ),
    );
}

/// Ideal: as above for the fragments a `BatchSyncResponse` carries.
#[tokio::test]
#[should_panic(expected = "KNOWN BUG C13:")]
async fn batch_sync_response_with_65536_fragments_round_trips() {
    let msg = response_with_fragments(65_536).await;
    known_bug(
        "C13",
        SyncMessage::try_decode(&msg.encode()).is_ok_and(|d| d == msg),
        &format!(
            "BatchSyncResponse carrying 65,536 fragments {}",
            decode_outcome(&msg)
        ),
    );
}

/// Ideal: as above for `RemoveSubscriptions`.
#[test]
#[should_panic(expected = "KNOWN BUG C13:")]
fn remove_subscriptions_with_65536_ids_never_wraps() {
    let msg = remove_subscriptions(65_536);
    known_bug(
        "C13",
        never_silently_altered(&msg),
        "RemoveSubscriptions with 65,536 ids decoded to a different message",
    );
}
