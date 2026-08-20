//! Phase 2 acceptance: two machines converge to the same tree via one
//! batch sync — handshake, fingerprint diff, bidirectional data return —
//! with a synchronous in-test driver (crypto + in-memory storage), no
//! async runtime anywhere.

use std::collections::BTreeMap;

use ed25519_dalek::{Signer as _, SigningKey};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    codec::decode::DecodeFields as _,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_protocol::{
    command::{Command, NewCommit},
    effect::{AppEvent, CryptoOp, CryptoResult, Effect, SignatureCheck, SyncStatus},
    event::{Direction, Event},
    handshake::audience::Audience,
    id::ConnId,
    machine::{Config, Machine, Now},
    peer_id::PeerId,
    storage::{IngestRejection, ItemKind, StorageOp, StorageResult},
    timestamp::Timestamp,
    wall_clock::TimestampSeconds,
};

const CONN: ConnId = ConnId::new(1);

const fn now() -> Now {
    Now {
        monotonic: Timestamp::from_millis(0),
        wall: TimestampSeconds::new(1_700_000_000),
    }
}

type CommitStore = BTreeMap<CommitId, (Signed<LooseCommit>, Blob)>;
type FragmentStore = BTreeMap<CommitId, (Signed<Fragment>, Blob)>;

/// In-memory compound store: (commits, fragments) per tree.
#[derive(Default)]
struct Store {
    trees: BTreeMap<SedimentreeId, (CommitStore, FragmentStore)>,
}

fn verify_signed_commit(
    signed: &Signed<LooseCommit>,
    blob: &Blob,
) -> Result<LooseCommit, IngestRejection> {
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&signed.issuer().to_bytes()) else {
        return Err(IngestRejection::BadSignature);
    };
    if vk
        .verify_strict(signed.payload_bytes(), signed.signature())
        .is_err()
    {
        return Err(IngestRejection::BadSignature);
    }
    let Ok((commit, _)) = LooseCommit::try_decode_fields(signed.fields_bytes()) else {
        return Err(IngestRejection::BadSignature);
    };
    if BlobMeta::new(blob) != *commit.blob_meta() {
        return Err(IngestRejection::BlobMismatch);
    }
    Ok(commit)
}

/// One peer: machine + synchronous driver (signer, store, mailboxes).
struct TestPeer {
    machine: Machine,
    signing_key: SigningKey,
    signer: MemorySigner,
    store: Store,
    outbox: Vec<(ConnId, Vec<u8>)>,
    app: Vec<AppEvent>,
}

impl TestPeer {
    fn new(seed: u8) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(signing_key.verifying_key());
        Self {
            machine: Machine::new(Config::new(local_peer, [seed.wrapping_add(100); 32])),
            signing_key,
            signer,
            store: Store::default(),
            outbox: Vec::new(),
            app: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.signing_key.verifying_key())
    }

    fn feed(&mut self, event: Event) {
        let _outcome = self.machine.handle(now(), event);
        self.run_effects();
    }

    fn run_effects(&mut self) {
        for _ in 0..256 {
            let Some(effect) = self.machine.poll_effect() else {
                return;
            };
            match effect {
                Effect::SendMessage { conn, bytes } => self.outbox.push((conn, bytes)),
                Effect::Disconnect { .. } => {}
                Effect::App(event) => self.app.push(event),
                Effect::Crypto { ticket, op } => {
                    let result = match op {
                        CryptoOp::Sign { payload } => CryptoResult::Signed {
                            signature: self.signing_key.sign(&payload).to_bytes(),
                        },
                        CryptoOp::Verify(item) => {
                            let ok = ed25519_dalek::VerifyingKey::from_bytes(&item.verifying_key)
                                .is_ok_and(|vk| {
                                    vk.verify_strict(
                                        &item.payload,
                                        &ed25519_dalek::Signature::from_bytes(&item.signature),
                                    )
                                    .is_ok()
                                });
                            CryptoResult::Verified(if ok {
                                SignatureCheck::Valid
                            } else {
                                SignatureCheck::Invalid
                            })
                        }
                        CryptoOp::VerifyBatch(_) => unimplemented!("not used in this test"),
                    };
                    let _outcome = self
                        .machine
                        .handle(now(), Event::CryptoDone { ticket, result });
                }
                Effect::Storage { ticket, op } => {
                    let result = self.run_storage(op);
                    let _outcome = self
                        .machine
                        .handle(now(), Event::StorageDone { ticket, result });
                }
            }
        }
        // 256 iterations without quiescing means a feedback loop.
        assert!(
            self.machine.poll_effect().is_none(),
            "effect loop did not quiesce"
        );
    }

    fn seal_local(
        &mut self,
        tree: SedimentreeId,
        commits: &[subduction_protocol::command::NewCommit],
        fragments: &[subduction_protocol::command::NewFragment],
    ) -> StorageResult {
        let sealed: Vec<Signed<LooseCommit>> = commits
            .iter()
            .map(|new| {
                let commit =
                    LooseCommit::new(tree, new.head, new.parents.clone(), BlobMeta::new(&new.blob));
                let signed =
                    futures::executor::block_on(Signed::seal::<Sendable, _>(&self.signer, commit))
                        .into_signed();
                let slot = self.store.trees.entry(tree).or_default();
                slot.0.insert(new.head, (signed.clone(), new.blob.clone()));
                signed
            })
            .collect();
        let sealed_fragments: Vec<Signed<Fragment>> = fragments
            .iter()
            .map(|new| {
                let fragment = Fragment::new(
                    tree,
                    new.head,
                    new.boundary.clone(),
                    &new.checkpoints,
                    BlobMeta::new(&new.blob),
                );
                let signed = futures::executor::block_on(Signed::seal::<Sendable, _>(
                    &self.signer,
                    fragment,
                ))
                .into_signed();
                let slot = self.store.trees.entry(tree).or_default();
                slot.1.insert(new.head, (signed.clone(), new.blob.clone()));
                signed
            })
            .collect();
        StorageResult::LocallyIngested {
            commits: sealed,
            fragments: sealed_fragments,
        }
    }

    fn run_storage(&mut self, op: StorageOp) -> StorageResult {
        match op {
            StorageOp::IngestLocal {
                tree,
                commits,
                fragments,
            } => self.seal_local(tree, &commits, &fragments),

            StorageOp::Ingest {
                tree,
                commits,
                fragments,
                ..
            } => {
                let mut stored = 0u32;
                let mut rejected = Vec::new();
                for (signed, blob) in fragments {
                    // Signature checks mirror the commit path; details are
                    // exercised there.
                    let Ok((fragment, _)) = Fragment::try_decode_fields(signed.fields_bytes())
                    else {
                        continue;
                    };
                    let slot = self.store.trees.entry(tree).or_default();
                    slot.1.insert(fragment.head(), (signed, blob));
                    stored += 1;
                }
                for (index, (signed, blob)) in commits.into_iter().enumerate() {
                    match verify_signed_commit(&signed, &blob) {
                        Ok(commit) => {
                            let slot = self.store.trees.entry(tree).or_default();
                            slot.0.insert(commit.head(), (signed, blob));
                            stored += 1;
                        }
                        Err(reason) => {
                            #[allow(clippy::cast_possible_truncation)]
                            rejected.push((ItemKind::Commit, index as u32, reason));
                        }
                    }
                }
                StorageResult::Ingested { stored, rejected }
            }

            StorageOp::FetchItems {
                tree,
                commit_ids,
                fragment_heads,
                ..
            } => {
                let Some(slot) = self.store.trees.get(&tree) else {
                    return StorageResult::UnknownTree;
                };
                let commits = commit_ids
                    .iter()
                    .filter_map(|id| slot.0.get(id).cloned())
                    .collect();
                let fragments = fragment_heads
                    .iter()
                    .filter_map(|id| slot.1.get(id).cloned())
                    .collect();
                StorageResult::Fetched { commits, fragments }
            }

            StorageOp::DeleteTree { tree, .. } => {
                self.store.trees.remove(&tree);
                StorageResult::TreeDeleted
            }
        }
    }

    fn add_commit(&mut self, tree: SedimentreeId, head: u8) {
        self.feed(Event::Command(Command::AddCommits {
            tree,
            commits: vec![NewCommit {
                head: CommitId::new([head; 32]),
                parents: std::collections::BTreeSet::new(),
                blob: Blob::new(vec![head; 16]),
            }],
        }));
    }

    fn add_fragment(&mut self, tree: SedimentreeId, head: u8, boundary: &[u8]) {
        self.feed(Event::Command(Command::AddFragments {
            tree,
            fragments: vec![subduction_protocol::command::NewFragment {
                head: CommitId::new([head; 32]),
                boundary: boundary.iter().map(|b| CommitId::new([*b; 32])).collect(),
                checkpoints: vec![],
                blob: Blob::new(vec![head; 24]),
            }],
        }));
    }

    fn stored_fragment_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.store
            .trees
            .get(&tree)
            .map(|slot| slot.1.keys().copied().collect())
            .unwrap_or_default()
    }

    fn stored_commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.store
            .trees
            .get(&tree)
            .map(|slot| slot.0.keys().copied().collect())
            .unwrap_or_default()
    }
}

/// Shuttle wire messages until both sides quiesce.
fn pump(alice: &mut TestPeer, bob: &mut TestPeer) {
    for _ in 0..64 {
        let a_out: Vec<_> = alice.outbox.drain(..).collect();
        let b_out: Vec<_> = bob.outbox.drain(..).collect();
        if a_out.is_empty() && b_out.is_empty() {
            return;
        }
        for (_conn, bytes) in a_out {
            bob.feed(Event::MessageReceived { conn: CONN, bytes });
        }
        for (_conn, bytes) in b_out {
            alice.feed(Event::MessageReceived { conn: CONN, bytes });
        }
    }
    assert!(
        alice.outbox.is_empty() && bob.outbox.is_empty(),
        "message pump did not quiesce"
    );
}

fn handshake(alice: &mut TestPeer, bob: &mut TestPeer) {
    let bob_id = bob.peer_id();
    alice.feed(Event::Connected {
        conn: CONN,
        direction: Direction::Outbound,
        audience: Some(Audience::known(bob_id)),
    });
    bob.feed(Event::Connected {
        conn: CONN,
        direction: Direction::Inbound,
        audience: None,
    });
    pump(alice, bob);
    assert!(
        alice
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { .. })),
        "handshake must complete"
    );
}

#[test]
fn divergent_trees_converge_via_one_sync() {
    let tree = SedimentreeId::new([7u8; 32]);
    let mut alice = TestPeer::new(1);
    let mut bob = TestPeer::new(2);
    handshake(&mut alice, &mut bob);

    // Divergent local histories.
    alice.add_commit(tree, 0xA1);
    alice.add_commit(tree, 0xA2);
    bob.add_commit(tree, 0xB1);

    // Bob initiates one batch sync.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    // Bob's request concluded successfully.
    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::Completed, .. } if *t == tree
    )));

    // Both stores hold all three commits.
    let mut expected: Vec<CommitId> = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xB1; 32]),
    ];
    expected.sort();
    assert_eq!(alice.stored_commit_ids(tree), expected, "alice converged");
    assert_eq!(bob.stored_commit_ids(tree), expected, "bob converged");

    // Both resident trees agree on heads (as sets; ordering is
    // traversal-dependent).
    let mut alice_heads = alice.machine.tree_heads(tree).unwrap_or_default();
    let mut bob_heads = bob.machine.tree_heads(tree).unwrap_or_default();
    alice_heads.sort();
    bob_heads.sort();
    assert_eq!(alice_heads, bob_heads, "resident heads converged");
    assert!(!alice_heads.is_empty());

    // Both sides saw the remote data arrive.
    assert!(alice
        .app
        .iter()
        .any(|e| matches!(e, AppEvent::TreeUpdated { tree: t, .. } if *t == tree)));
    assert!(bob
        .app
        .iter()
        .any(|e| matches!(e, AppEvent::TreeUpdated { tree: t, .. } if *t == tree)));
}

#[test]
fn syncing_a_tree_we_lack_pulls_everything() {
    let tree = SedimentreeId::new([8u8; 32]);
    let mut alice = TestPeer::new(3);
    let mut bob = TestPeer::new(4);
    handshake(&mut alice, &mut bob);

    alice.add_commit(tree, 0xA1);

    // Bob has nothing; his summary advertises nothing.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    assert_eq!(
        bob.stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])],
        "bob received alice's commit"
    );
    assert_eq!(
        bob.machine.tree_heads(tree),
        Some(vec![CommitId::new([0xA1; 32])])
    );
}

#[test]
fn sync_of_unknown_tree_reports_not_found() {
    let tree = SedimentreeId::new([9u8; 32]);
    let mut alice = TestPeer::new(5);
    let mut bob = TestPeer::new(6);
    handshake(&mut alice, &mut bob);

    // Neither side has the tree.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::NotFound, .. } if *t == tree
    )));
}

#[test]
fn sync_request_times_out_without_response() {
    let tree = SedimentreeId::new([10u8; 32]);
    let mut alice = TestPeer::new(7);
    let mut bob = TestPeer::new(8);
    handshake(&mut alice, &mut bob);

    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    // Discard Bob's request instead of delivering it.
    bob.outbox.clear();
    assert_eq!(bob.machine.pending_sync_requests(), 1);
    assert!(
        bob.machine.poll_timeout().is_some(),
        "request deadline armed"
    );

    // Past the deadline.
    let late = Now {
        monotonic: Timestamp::from_millis(0)
            .saturating_add(Config::DEFAULT_SYNC_TIMEOUT)
            .saturating_add(core::time::Duration::from_secs(1)),
        wall: TimestampSeconds::new(1_700_000_000 + 61),
    };
    let _outcome = bob.machine.handle(late, Event::Wake);
    bob.run_effects();

    assert_eq!(bob.machine.pending_sync_requests(), 0);
    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::TimedOut, .. } if *t == tree
    )));
}

/// Pump between one hub (on two connections) and two spokes.
fn pump3(hub: &mut TestPeer, spoke_b: &mut TestPeer, spoke_c: &mut TestPeer) {
    let (conn_b, conn_c) = (ConnId::new(1), ConnId::new(2));
    for _ in 0..64 {
        let hub_out: Vec<_> = hub.outbox.drain(..).collect();
        let b_out: Vec<_> = spoke_b.outbox.drain(..).collect();
        let c_out: Vec<_> = spoke_c.outbox.drain(..).collect();
        if hub_out.is_empty() && b_out.is_empty() && c_out.is_empty() {
            return;
        }
        for (conn, bytes) in hub_out {
            if conn == conn_b {
                spoke_b.feed(Event::MessageReceived { conn: CONN, bytes });
            } else {
                spoke_c.feed(Event::MessageReceived { conn: CONN, bytes });
            }
        }
        for (_conn, bytes) in b_out {
            hub.feed(Event::MessageReceived {
                conn: conn_b,
                bytes,
            });
        }
        for (_conn, bytes) in c_out {
            hub.feed(Event::MessageReceived {
                conn: conn_c,
                bytes,
            });
        }
    }
    assert!(
        hub.outbox.is_empty() && spoke_b.outbox.is_empty() && spoke_c.outbox.is_empty(),
        "three-way pump did not quiesce"
    );
}

#[test]
fn subscriber_receives_pushes_for_local_commits() {
    let tree = SedimentreeId::new([11u8; 32]);
    let mut alice = TestPeer::new(9);
    let mut bob = TestPeer::new(10);
    handshake(&mut alice, &mut bob);

    alice.add_commit(tree, 0xA1);

    // Bob syncs WITH subscription.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }));
    pump(&mut alice, &mut bob);
    assert_eq!(bob.stored_commit_ids(tree), vec![CommitId::new([0xA1; 32])]);

    // Alice authors a NEW commit after the sync: it must be pushed.
    alice.add_commit(tree, 0xA2);
    pump(&mut alice, &mut bob);

    let mut expected = vec![CommitId::new([0xA1; 32]), CommitId::new([0xA2; 32])];
    expected.sort();
    assert_eq!(bob.stored_commit_ids(tree), expected, "push arrived");
    assert!(alice.machine.stats().subscription_pushes >= 1);
}

#[test]
fn remote_ingest_is_forwarded_to_other_subscribers() {
    let tree = SedimentreeId::new([12u8; 32]);
    let (conn_b, conn_c) = (ConnId::new(1), ConnId::new(2));
    let mut hub = TestPeer::new(11); // Alice, connected to both
    let mut bob = TestPeer::new(12);
    let mut carol = TestPeer::new(13);

    // Handshake hub↔bob on conn 1 and hub↔carol on conn 2 (hub inbound).
    for (spoke, conn) in [(&mut bob, conn_b), (&mut carol, conn_c)] {
        spoke.feed(Event::Connected {
            conn: CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(hub.peer_id())),
        });
        hub.feed(Event::Connected {
            conn,
            direction: Direction::Inbound,
            audience: None,
        });
    }
    pump3(&mut hub, &mut bob, &mut carol);

    // The hub holds the tree (mutual subscription only forms on an Ok
    // response — legacy semantics; a NotFound sync does not subscribe).
    hub.add_commit(tree, 0xA1);

    // Both spokes subscribe to the tree.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }));
    carol.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }));
    pump3(&mut hub, &mut bob, &mut carol);

    // Carol authors a commit and pushes it to the hub (she subscribed, so
    // the hub is in her subscriber set via the mutual subscription).
    carol.add_commit(tree, 0xC1);
    pump3(&mut hub, &mut bob, &mut carol);

    // The hub ingested it and forwarded to Bob — but never back to Carol.
    let mut expected = vec![CommitId::new([0xA1; 32]), CommitId::new([0xC1; 32])];
    expected.sort();
    assert_eq!(hub.stored_commit_ids(tree), expected, "hub ingested");
    assert_eq!(bob.stored_commit_ids(tree), expected, "bob got the forward");
    assert_eq!(carol.stored_commit_ids(tree), expected, "carol has her own");
}

/// Full triangle: A↔B, A↔C, B↔C, with per-peer conn maps.
/// Conn numbering on each peer: conn 1 and conn 2 to the two others.
struct Triangle {
    peers: [TestPeer; 3],
    /// `links[i] = [(conn_on_i, j, conn_on_j); 2]`
    links: [[(ConnId, usize, ConnId); 2]; 3],
}

// Fixed [_; 3] arrays with link indices constructed in `new` — every
// index is bounded by construction, and get().expect() would only trade
// one lint for another.
#[allow(clippy::indexing_slicing)]
impl Triangle {
    fn new(seeds: [u8; 3]) -> Self {
        let peers = [
            TestPeer::new(seeds[0]),
            TestPeer::new(seeds[1]),
            TestPeer::new(seeds[2]),
        ];
        let c1 = ConnId::new(1);
        let c2 = ConnId::new(2);
        Self {
            peers,
            links: [
                [(c1, 1, c1), (c2, 2, c1)], // A: conn1↔B.conn1, conn2↔C.conn1
                [(c1, 0, c1), (c2, 2, c2)], // B: conn1↔A.conn1, conn2↔C.conn2
                [(c1, 0, c2), (c2, 1, c2)], // C: conn1↔A.conn2, conn2↔B.conn2
            ],
        }
    }

    fn connect_all(&mut self) {
        // Lower index dials the higher one on each link.
        for i in 0..3 {
            for (local_conn, j, remote_conn) in self.links[i] {
                if i < j {
                    let peer_id = self.peers[j].peer_id();
                    self.peers[i].feed(Event::Connected {
                        conn: local_conn,
                        direction: Direction::Outbound,
                        audience: Some(Audience::known(peer_id)),
                    });
                    self.peers[j].feed(Event::Connected {
                        conn: remote_conn,
                        direction: Direction::Inbound,
                        audience: None,
                    });
                }
            }
        }
        self.pump();
    }

    fn pump(&mut self) {
        for _ in 0..128 {
            let mut moved = false;
            for i in 0..3 {
                let out: Vec<_> = self.peers[i].outbox.drain(..).collect();
                for (conn, bytes) in out {
                    moved = true;
                    let Some((_lc, j, remote_conn)) =
                        self.links[i].iter().copied().find(|(lc, _, _)| *lc == conn)
                    else {
                        unreachable!("every conn maps to a link by construction");
                    };
                    self.peers[j].feed(Event::MessageReceived {
                        conn: remote_conn,
                        bytes,
                    });
                }
            }
            if !moved {
                return;
            }
        }
        // 128 rounds without quiescing means an echo loop.
        assert!(
            self.peers.iter().all(|p| p.outbox.is_empty()),
            "triangle pump did not quiesce (thrash loop)"
        );
    }
}

/// The #281 regression class: a fully connected, mutually subscribed
/// triangle must damp — a new commit is pushed and forwarded, but
/// non-fresh copies are never re-forwarded, so traffic quiesces.
#[test]
#[allow(clippy::indexing_slicing)] // fixed [_; 3] topology, bounded indices
fn mutually_subscribed_triangle_damps() {
    let tree = SedimentreeId::new([13u8; 32]);
    let mut tri = Triangle::new([21, 22, 23]);
    tri.connect_all();

    // Seed the tree at A so every subscribe-sync gets an Ok response.
    tri.peers[0].add_commit(tree, 0x01);
    tri.pump();

    // Everyone subscribes to everyone (B→A, C→A, then B↔C both ways).
    for (i, conn) in [(1, 1), (2, 1), (1, 2), (2, 2)] {
        tri.peers[i].feed(Event::Command(Command::SyncTree {
            conn: ConnId::new(conn),
            tree,
            subscribe: true,
        }));
        tri.pump();
    }

    let pushes_before: u64 = tri
        .peers
        .iter()
        .map(|p| p.machine.stats().subscription_pushes)
        .sum();

    // A authors one new commit. Push + forward must fan out and STOP.
    tri.peers[0].add_commit(tree, 0x02);
    tri.pump(); // asserts quiescence internally

    // Everyone converged.
    let mut expected = vec![CommitId::new([0x01; 32]), CommitId::new([0x02; 32])];
    expected.sort();
    for (i, peer) in tri.peers.iter().enumerate() {
        assert_eq!(peer.stored_commit_ids(tree), expected, "peer {i} converged");
    }

    // Bounded traffic: one new item, three peers, at most two subscribers
    // per peer → the whole cascade fits comfortably under a small bound.
    let pushes_after: u64 = tri
        .peers
        .iter()
        .map(|p| p.machine.stats().subscription_pushes)
        .sum();
    assert!(
        pushes_after - pushes_before <= 6,
        "push cascade must damp, got {} pushes",
        pushes_after - pushes_before
    );
}

/// Convergence property: whatever divergent linear histories the two
/// sides hold, ONE batch sync converges both stores and both resident
/// trees — PROVIDED the responder holds the tree (a responder without
/// the tree answers `NotFound` and learns nothing; legacy semantics).
/// Alice therefore always holds at least one seed commit.
#[test]
fn prop_one_sync_converges_arbitrary_divergence() {
    let tree = SedimentreeId::new([14u8; 32]);

    bolero::check!()
        .with_iterations(24)
        .with_arbitrary::<(Vec<u8>, Vec<u8>)>()
        .for_each(|(alice_raw, bob_raw)| {
            // Disjoint id ranges; dedup; bounded.
            let alice_heads: Vec<u8> = {
                let mut seen = std::collections::BTreeSet::new();
                // 0x7F is the guaranteed seed (outside both generated
                // ranges); the responder always holds the tree.
                core::iter::once(0x7F)
                    .chain(alice_raw.iter().map(|b| (b % 0x7E) + 1)) // 0x01..=0x7E
                    .filter(|b| seen.insert(*b))
                    .take(6)
                    .collect()
            };
            let bob_heads: Vec<u8> = {
                let mut seen = std::collections::BTreeSet::new();
                bob_raw
                    .iter()
                    .map(|b| (b % 0x7E) + 0x80) // 0x80..=0xFD
                    .filter(|b| seen.insert(*b))
                    .take(6)
                    .collect()
            };

            let mut alice = TestPeer::new(31);
            let mut bob = TestPeer::new(32);
            handshake(&mut alice, &mut bob);

            // Linear chains (each commit's parent is the previous one).
            for (peer, heads) in [(&mut alice, &alice_heads), (&mut bob, &bob_heads)] {
                let mut parent: Option<u8> = None;
                for head in heads.iter().copied() {
                    peer.feed(Event::Command(Command::AddCommits {
                        tree,
                        commits: vec![NewCommit {
                            head: CommitId::new([head; 32]),
                            parents: parent.map(|p| CommitId::new([p; 32])).into_iter().collect(),
                            blob: Blob::new(vec![head; 16]),
                        }],
                    }));
                    parent = Some(head);
                }
            }

            // One sync, initiated by Bob.
            bob.feed(Event::Command(Command::SyncTree {
                conn: CONN,
                tree,
                subscribe: false,
            }));
            pump(&mut alice, &mut bob);

            // Stores converged to the union.
            let mut expected: Vec<CommitId> = alice_heads
                .iter()
                .chain(bob_heads.iter())
                .map(|b| CommitId::new([*b; 32]))
                .collect();
            expected.sort();
            assert_eq!(alice.stored_commit_ids(tree), expected, "alice store");
            assert_eq!(bob.stored_commit_ids(tree), expected, "bob store");

            // Resident trees agree.
            let mut ah = alice.machine.tree_heads(tree).unwrap_or_default();
            let mut bh = bob.machine.tree_heads(tree).unwrap_or_default();
            ah.sort();
            bh.sort();
            assert_eq!(ah, bh, "resident heads");
        });
}

#[test]
fn fragments_sync_and_push() {
    let tree = SedimentreeId::new([15u8; 32]);
    let mut alice = TestPeer::new(41);
    let mut bob = TestPeer::new(42);
    handshake(&mut alice, &mut bob);

    // Alice holds a commit and a fragment summarizing it.
    alice.add_commit(tree, 0x21);
    alice.add_fragment(tree, 0x21, &[0x21]);

    // Bob syncs with subscription: both arrive.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }));
    pump(&mut alice, &mut bob);
    assert_eq!(bob.stored_commit_ids(tree), vec![CommitId::new([0x21; 32])]);
    assert_eq!(
        bob.stored_fragment_ids(tree),
        vec![CommitId::new([0x21; 32])],
        "fragment synced"
    );

    // A NEW fragment authored after the sync is pushed.
    alice.add_commit(tree, 0x22);
    alice.add_fragment(tree, 0x22, &[0x22]);
    pump(&mut alice, &mut bob);
    let mut expected = vec![CommitId::new([0x21; 32]), CommitId::new([0x22; 32])];
    expected.sort();
    assert_eq!(bob.stored_fragment_ids(tree), expected, "fragment pushed");
}

#[test]
fn unsubscribe_stops_pushes() {
    let tree = SedimentreeId::new([16u8; 32]);
    let mut alice = TestPeer::new(43);
    let mut bob = TestPeer::new(44);
    handshake(&mut alice, &mut bob);

    alice.add_commit(tree, 0x31);
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }));
    pump(&mut alice, &mut bob);

    // Pushes flow…
    alice.add_commit(tree, 0x32);
    pump(&mut alice, &mut bob);
    assert_eq!(bob.stored_commit_ids(tree).len(), 2);

    // …until Bob unsubscribes.
    bob.feed(Event::Command(Command::Unsubscribe {
        conn: CONN,
        trees: vec![tree],
    }));
    pump(&mut alice, &mut bob);

    alice.add_commit(tree, 0x33);
    pump(&mut alice, &mut bob);
    assert_eq!(
        bob.stored_commit_ids(tree).len(),
        2,
        "no push after unsubscribe"
    );
}
