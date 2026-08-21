//! Bolero property suite for the machine (Phase 1 close-out).
//!
//! Three properties, generalizing the unit tests in
//! `handshake_machine.rs`:
//!
//! 1. **Total robustness** — arbitrary event sequences never panic.
//! 2. **Ticket safety** — only the exact issued witness lands; any
//!    mutation is ignored without touching state.
//! 3. **Authentication integrity** — under adversarial delivery
//!    (reorder, duplicate, garbage injection), a machine that pinned
//!    `Audience::Known(p)` only ever authenticates `p`.

use ed25519_dalek::{Signer as _, SigningKey};
use subduction_protocol::{
    effect::{AppEvent, CryptoOp, CryptoResult, Effect},
    event::{Direction, Event},
    handshake::audience::Audience,
    id::ConnId,
    machine::{Config, Machine, Now},
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    timestamp::Timestamp,
    ticket::CryptoTicket,
    wall_clock::TimestampSeconds,
};

/// Execute crypto effects synchronously; collect sends and app events.
fn run_effects(
    machine: &mut Machine,
    signing_key: &SigningKey,
    now: Now,
    sent: &mut Vec<Vec<u8>>,
    app: &mut Vec<AppEvent>,
) {
    // Bounded: each turn issues at most a handful of effects.
    for _ in 0..64 {
        let Some(effect) = machine.poll_effect() else {
            return;
        };
        match effect {
            Effect::SendMessage { bytes, .. } => sent.push(bytes),
            // Disconnects need no action here; storage ops are not
            // issued during the handshake phase.
            Effect::Disconnect { .. } | Effect::Storage { .. } => {}
            Effect::App(event) => app.push(event),
            Effect::Crypto { ticket, op } => {
                let CryptoOp::Sign { payload } = op;
                let result = CryptoResult::Signed {
                    signature: signing_key.sign(&payload).to_bytes(),
                };
                let _outcome = machine.handle(now, Event::CryptoDone { ticket, result });
            }
        }
    }
}

const fn now_at(millis: u64) -> Now {
    Now {
        monotonic: Timestamp::from_millis(millis),
        wall: TimestampSeconds::new(1_700_000_000 + millis / 1000),
    }
}

/// Property 1: the machine never panics, whatever the driver throws at
/// it, in whatever order, at whatever times. Effects are drained (and
/// crypto completed with a real signer) every step.
#[test]
fn prop_arbitrary_event_sequences_never_panic() {
    let signing_key = SigningKey::from_bytes(&[42u8; 32]);
    let local_peer = PeerId::from(signing_key.verifying_key());

    bolero::check!()
        .with_arbitrary::<Vec<(u16, Event)>>()
        .for_each(|steps| {
            let mut machine = Machine::new(Config::new(local_peer, [7u8; 32]));
            let mut clock: u64 = 0;
            let (mut sent, mut app) = (Vec::new(), Vec::new());

            for (advance, event) in steps {
                clock = clock.saturating_add(u64::from(*advance));
                let now = now_at(clock);
                let _outcome = machine.handle(now, event.clone());
                run_effects(&mut machine, &signing_key, now, &mut sent, &mut app);
            }

            // Counters only ever grow; deadline is coherent.
            let stats = machine.stats();
            assert!(stats.connections_closed <= stats.connections_opened);
        });
}

/// Property 2: given a real in-flight operation, the *exact* ticket is
/// the only one that lands. Any arbitrary different ticket is ignored
/// and leaves no observable state change.
// Panicking is bolero's counterexample-reporting channel inside `for_each`
// closures — `?` cannot escape them, so `TestResult` is not an option here.
#[allow(clippy::panic)]
#[test]
fn prop_only_the_exact_ticket_lands() {
    let signing_key = SigningKey::from_bytes(&[42u8; 32]);
    let local_peer = PeerId::from(signing_key.verifying_key());
    let bob = PeerId::new([0xBB; 32]);

    bolero::check!()
        .with_arbitrary::<(CryptoTicket, [u8; 64])>()
        .for_each(|(mutated, signature)| {
            let mut machine = Machine::new(Config::new(local_peer, [7u8; 32]));
            let conn = ConnId::new(1);
            let now = now_at(0);

            let _outcome = machine.handle(
                now,
                Event::Connected {
                    conn,
                    direction: Direction::Outbound,
                    audience: Some(Audience::known(bob)),
                },
            );
            let Some(Effect::Crypto { ticket, .. }) = machine.poll_effect() else {
                panic!("outbound connect must issue a sign effect");
            };

            if *mutated == ticket {
                return; // arbitrary collision with the real witness — skip
            }

            let outcome = machine.handle(
                now,
                Event::CryptoDone {
                    ticket: *mutated,
                    result: CryptoResult::Signed {
                        signature: *signature,
                    },
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
                machine.poll_effect(),
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
    machine: Machine,
    key: SigningKey,
    /// Undelivered outbound messages.
    out: Vec<Vec<u8>>,
    /// Everything this side ever sent (for replays).
    history: Vec<Vec<u8>>,
    app: Vec<AppEvent>,
}

impl Side {
    fn new(seed: u8, entropy: u8) -> Self {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(key.verifying_key());
        Self {
            machine: Machine::new(Config::new(local_peer, [entropy; 32])),
            key,
            out: Vec::new(),
            history: Vec::new(),
            app: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.key.verifying_key())
    }

    fn feed(&mut self, now: Now, event: Event) {
        let _outcome = self.machine.handle(now, event);
        run_effects(
            &mut self.machine,
            &self.key,
            now,
            &mut self.out,
            &mut self.app,
        );
    }

    /// Deliver this side's oldest undelivered message to `other`.
    fn deliver_to(&mut self, other: &mut Side, now: Now, conn: ConnId) {
        if !self.out.is_empty() {
            let bytes = self.out.remove(0);
            self.history.push(bytes.clone());
            other.feed(now, Event::MessageReceived { conn, bytes });
        }
    }

    /// Replay one of this side's past messages to `other`.
    fn replay_to(&mut self, other: &mut Side, now: Now, conn: ConnId, index: u8) {
        let slot = usize::from(index) % self.history.len().max(1);
        if let Some(bytes) = self.history.get(slot) {
            let bytes = bytes.clone();
            other.feed(now, Event::MessageReceived { conn, bytes });
        }
    }

    /// Every peer this side's machine claimed to authenticate.
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
            let mut alice = Side::new(1, 11);
            let mut bob = Side::new(2, 22);
            let conn = ConnId::new(1);
            let mut clock: u64 = 0;

            let now = now_at(clock);
            let bob_id = bob.peer_id();
            alice.feed(
                now,
                Event::Connected {
                    conn,
                    direction: Direction::Outbound,
                    audience: Some(Audience::known(bob_id)),
                },
            );
            bob.feed(
                now,
                Event::Connected {
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
                        let bytes = bytes.clone();
                        bob.feed(now, Event::MessageReceived { conn, bytes });
                    }
                    AdversaryOp::GarbageToAlice(bytes) => {
                        let bytes = bytes.clone();
                        alice.feed(now, Event::MessageReceived { conn, bytes });
                    }
                    AdversaryOp::AdvanceTime(advance) => {
                        clock = clock.saturating_add(u64::from(*advance));
                        let now = now_at(clock);
                        alice.feed(now, Event::Wake);
                        bob.feed(now, Event::Wake);
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
