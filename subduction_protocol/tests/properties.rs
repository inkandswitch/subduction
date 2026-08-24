//! Bolero property suite at node level: the full Design-D pipeline
//! (`ConnMachine`s + `CoreMachine` + router) under generated inputs.
//!
//! 1. **Total robustness** — arbitrary event sequences never panic.
//! 2. **Ticket safety** — only the exact issued witness lands; any
//!    mutation is ignored without touching state.
//! 3. **Authentication integrity** — under adversarial delivery
//!    (reorder, duplicate, garbage injection), a node that pinned
//!    `Audience::Known(p)` only ever authenticates `p`.
//! 4. **Convergence** — one sync merges arbitrary divergent histories
//!    to the union, on both sides, without leaking a single frame.

use ed25519_dalek::{Signer as _, SigningKey};
use subduction_protocol::{
    blob_ref::{FrameId, Part},
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
    node::{Node, NodeConfig, NodeEffect, NodeEvent},
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    ticket::CryptoTicket,
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};

const fn now_at(millis: u64) -> Now {
    Now {
        monotonic: Timestamp::from_millis(millis),
        wall: TimestampSeconds::new(1_700_000_000 + millis / 1000),
    }
}

fn node(seed: u8) -> (Node, SigningKey) {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let local_peer = PeerId::from(key.verifying_key());
    (
        Node::new(NodeConfig::new(local_peer, [seed ^ 0x55; 32])),
        key,
    )
}

/// Drain effects: complete signs with the real key, collect assembled
/// sends and app events, drop everything else (storage never completes —
/// pending ops stay pending, which the machines must tolerate).
fn run_effects(
    node: &mut Node,
    key: &SigningKey,
    now: Now,
    sent: &mut Vec<Vec<u8>>,
    app: &mut Vec<AppEvent>,
) {
    // Bounded: each turn issues at most a handful of effects.
    for _ in 0..256 {
        let Some(effect) = node.poll_effect() else {
            return;
        };
        match effect {
            NodeEffect::Send { parts, .. } => {
                let mut bytes = Vec::new();
                for part in &parts {
                    match part {
                        Part::Bytes(b) => bytes.extend_from_slice(b),
                        // No storage driver here, so refs cannot be
                        // resolved; skip the blob bytes.
                        Part::Ref(_) => {}
                    }
                }
                sent.push(bytes);
            }
            NodeEffect::Sign { ticket, payload } => {
                let signature = key.sign(&payload).to_bytes();
                let _outcome = node.handle(now, NodeEvent::SignDone { ticket, signature });
            }
            NodeEffect::App(event) => app.push(event),
            NodeEffect::Disconnect { .. }
            | NodeEffect::Storage { .. }
            | NodeEffect::ReleaseFrame(_)
            | NodeEffect::ReleaseBlob(_) => {}
        }
    }
}

/// Property 1: the node never panics, whatever the driver throws at it,
/// in whatever order, at whatever times.
#[test]
fn prop_arbitrary_event_sequences_never_panic() {
    bolero::check!()
        .with_arbitrary::<Vec<(u16, NodeEvent)>>()
        .for_each(|steps| {
            let (mut node, key) = node(42);
            let mut clock: u64 = 0;
            let (mut sent, mut app) = (Vec::new(), Vec::new());

            for (advance, event) in steps {
                clock = clock.saturating_add(u64::from(*advance));
                let now = now_at(clock);
                let _outcome = node.handle(now, event.clone());
                run_effects(&mut node, &key, now, &mut sent, &mut app);
            }

            // Counters only ever grow; deadline is coherent.
            let stats = node.stats();
            assert!(stats.connections_closed <= stats.connections_opened);
        });
}

/// Property 2: given a real in-flight signing operation, the *exact*
/// ticket is the only one that lands. Any arbitrary different ticket is
/// ignored and leaves no observable state change.
// Panicking is bolero's counterexample-reporting channel inside `for_each`
// closures — `?` cannot escape them, so `TestResult` is not an option here.
#[allow(clippy::panic)]
#[test]
fn prop_only_the_exact_ticket_lands() {
    let bob = PeerId::new([0xBB; 32]);

    bolero::check!()
        .with_arbitrary::<(CryptoTicket, [u8; 64])>()
        .for_each(|(mutated, signature)| {
            let (mut node, _key) = node(42);
            let conn = ConnId::new(1);
            let now = now_at(0);

            let _outcome = node.handle(
                now,
                NodeEvent::Connected {
                    conn,
                    direction: Direction::Outbound,
                    audience: Some(Audience::known(bob)),
                },
            );
            let Some(NodeEffect::Sign { ticket, .. }) = node.poll_effect() else {
                panic!("outbound connect must issue a sign effect");
            };

            if *mutated == ticket {
                return; // arbitrary collision with the real witness — skip
            }

            let outcome = node.handle(
                now,
                NodeEvent::SignDone {
                    ticket: *mutated,
                    signature: *signature,
                },
            );
            assert!(
                matches!(
                    outcome,
                    Outcome::Ignored(
                        IgnoreReason::StaleTicket
                            | IgnoreReason::UnknownTicket
                            | IgnoreReason::UnknownConnection(_)
                    )
                ),
                "mutated ticket must be ignored, got {outcome:?}"
            );
            assert_eq!(
                node.poll_effect(),
                None,
                "ignored completion must produce no effects"
            );
        });
}

/// One adversarial scheduling step for property 3.
#[derive(Debug, Clone, arbitrary::Arbitrary)]
enum AdversaryOp {
    /// Deliver the oldest undelivered message from Alice to Bob.
    DeliverToBob,
    /// Deliver the oldest undelivered message from Bob to Alice.
    DeliverToAlice,
    /// Re-deliver a previously seen Alice→Bob message (replay).
    ReplayToBob(u8),
    /// Re-deliver a previously seen Bob→Alice message (replay).
    ReplayToAlice(u8),
    /// Inject arbitrary bytes at Bob.
    GarbageToBob(Vec<u8>),
    /// Inject arbitrary bytes at Alice.
    GarbageToAlice(Vec<u8>),
    /// Let time pass.
    AdvanceTime(u16),
}

/// One honest endpoint plus its adversary-visible mailboxes.
struct Side {
    node: Node,
    key: SigningKey,
    next_frame: u64,
    /// Undelivered outbound messages.
    out: Vec<Vec<u8>>,
    /// Everything this side ever sent (for replays).
    history: Vec<Vec<u8>>,
    app: Vec<AppEvent>,
}

impl Side {
    fn new(seed: u8) -> Self {
        let (node, key) = node(seed);
        Self {
            node,
            key,
            next_frame: 1,
            out: Vec::new(),
            history: Vec::new(),
            app: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.key.verifying_key())
    }

    fn feed(&mut self, now: Now, event: NodeEvent) {
        let _outcome = self.node.handle(now, event);
        run_effects(&mut self.node, &self.key, now, &mut self.out, &mut self.app);
    }

    fn receive(&mut self, now: Now, conn: ConnId, bytes: Vec<u8>) {
        let frame = FrameId::new(self.next_frame);
        self.next_frame += 1;
        self.feed(now, NodeEvent::MessageReceived { conn, frame, bytes });
    }

    /// Deliver this side's oldest undelivered message to `other`.
    fn deliver_to(&mut self, other: &mut Side, now: Now, conn: ConnId) {
        if !self.out.is_empty() {
            let bytes = self.out.remove(0);
            self.history.push(bytes.clone());
            other.receive(now, conn, bytes);
        }
    }

    /// Replay one of this side's past messages to `other`.
    fn replay_to(&mut self, other: &mut Side, now: Now, conn: ConnId, index: u8) {
        let slot = usize::from(index) % self.history.len().max(1);
        if let Some(bytes) = self.history.get(slot) {
            let bytes = bytes.clone();
            other.receive(now, conn, bytes);
        }
    }

    /// Every peer this side's node claimed to authenticate.
    fn authenticated_peers(&self) -> impl Iterator<Item = PeerId> {
        self.app.iter().filter_map(|event| match event {
            AppEvent::PeerAuthenticated { peer, .. } => Some(*peer),
            AppEvent::ConnectionClosed { .. }
            | AppEvent::ExtensionMessage { .. }
            | AppEvent::CommitsStored { .. }
            | AppEvent::FragmentsStored { .. }
            | AppEvent::TreeRemoved { .. }
            | AppEvent::StorageError { .. }
            | AppEvent::SyncFinished { .. }
            | AppEvent::TreeUpdated { .. }
            | AppEvent::SubscriberLagging { .. }
            | AppEvent::RemoteHeadsUpdated { .. } => None,
        })
    }
}

/// Property 3: whatever the adversary does to the network (reordering
/// via selective delivery, replay, garbage), Alice — who pinned
/// `Known(bob)` — never authenticates anyone but Bob, and Bob never
/// authenticates anyone but Alice. (The adversary has no signing keys;
/// it can only shuffle honest bytes.)
#[test]
fn prop_adversarial_delivery_never_misauthenticates() {
    bolero::check!()
        .with_arbitrary::<Vec<AdversaryOp>>()
        .for_each(|ops| {
            let mut alice = Side::new(1);
            let mut bob = Side::new(2);
            let conn = ConnId::new(1);
            let mut clock: u64 = 0;

            let now = now_at(clock);
            let bob_id = bob.peer_id();
            alice.feed(
                now,
                NodeEvent::Connected {
                    conn,
                    direction: Direction::Outbound,
                    audience: Some(Audience::known(bob_id)),
                },
            );
            bob.feed(
                now,
                NodeEvent::Connected {
                    conn,
                    direction: Direction::Inbound,
                    audience: None,
                },
            );

            for op in ops {
                let now = now_at(clock);
                match op {
                    AdversaryOp::DeliverToBob => alice.deliver_to(&mut bob, now, conn),
                    AdversaryOp::DeliverToAlice => bob.deliver_to(&mut alice, now, conn),
                    AdversaryOp::ReplayToBob(index) => {
                        alice.replay_to(&mut bob, now, conn, *index);
                    }
                    AdversaryOp::ReplayToAlice(index) => {
                        bob.replay_to(&mut alice, now, conn, *index);
                    }
                    AdversaryOp::GarbageToBob(bytes) => {
                        bob.receive(now, conn, bytes.clone());
                    }
                    AdversaryOp::GarbageToAlice(bytes) => {
                        alice.receive(now, conn, bytes.clone());
                    }
                    AdversaryOp::AdvanceTime(advance) => {
                        clock = clock.saturating_add(u64::from(*advance));
                        let now = now_at(clock);
                        alice.feed(now, NodeEvent::Wake);
                        bob.feed(now, NodeEvent::Wake);
                    }
                }
            }

            // The integrity property: pinned identities only.
            for peer in alice.authenticated_peers() {
                assert_eq!(peer, bob_id, "Alice pinned Bob; authenticated {peer}");
            }
            for peer in bob.authenticated_peers() {
                assert_eq!(peer, alice.peer_id(), "Bob must only authenticate Alice");
            }
        });
}

/// Property 4: one sync converges arbitrary divergent linear histories
/// to the union on both sides — through the full pipeline, with the
/// no-leak invariant held.
// Panicking is bolero's counterexample-reporting channel inside `for_each`
// closures — `?` cannot escape them, so the harness Results unwrap here.
#[allow(clippy::panic, clippy::expect_used)]
#[test]
fn prop_one_sync_converges_arbitrary_divergence() {
    use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
    use subduction_protocol::command::{Command, NewCommit};
    use subduction_testkit::net::Net;

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

            let mut net = Net::new(&[31, 32]);
            let (_ca, cb) = net.connect(0, 1).expect("connect");

            // Linear chains (each commit's parent is the previous one).
            for (peer, heads) in [(0, &alice_heads), (1, &bob_heads)] {
                let mut parent: Option<u8> = None;
                for head in heads.iter().copied() {
                    net.driver_mut(peer)
                        .feed(NodeEvent::Command(Command::AddCommits {
                            tree,
                            commits: vec![NewCommit {
                                head: CommitId::new([head; 32]),
                                parents: parent
                                    .map(|p| CommitId::new([p; 32]))
                                    .into_iter()
                                    .collect(),
                                blob: Blob::new(vec![head; 16]),
                            }],
                        }))
                        .expect("add commit");
                    parent = Some(head);
                }
            }

            // One sync, initiated by Bob.
            net.driver_mut(1)
                .feed(NodeEvent::Command(Command::SyncTree {
                    conn: cb,
                    tree,
                    subscribe: false,
                }))
                .expect("sync command");
            let _messages = net.pump().expect("pump");

            // Stores converged to the union.
            let mut expected: Vec<CommitId> = alice_heads
                .iter()
                .chain(bob_heads.iter())
                .map(|b| CommitId::new([*b; 32]))
                .collect();
            expected.sort();
            assert_eq!(net.driver(0).stored_commit_ids(tree), expected, "alice");
            assert_eq!(net.driver(1).stored_commit_ids(tree), expected, "bob");

            // Resident trees agree; nothing leaked.
            let mut ah = net.driver_mut(0).node.tree_heads(tree).unwrap_or_default();
            let mut bh = net.driver_mut(1).node.tree_heads(tree).unwrap_or_default();
            ah.sort();
            bh.sort();
            assert_eq!(ah, bh, "resident heads");
            net.check_no_leaks().expect("no leaks");
        });
}
