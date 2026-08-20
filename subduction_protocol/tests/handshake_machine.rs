//! Phase 1 acceptance: two machines complete a mutual handshake purely by
//! shuttling `Effect::SendMessage` bytes and crypto completions — no async
//! runtime, no locks, no clock.

use ed25519_dalek::{Signer as _, SigningKey};
use sedimentree_core::codec::{
    decode::DecodeFields as _, encode::EncodeFields as _, schema::Schema as _,
};
use subduction_protocol::{
    effect::{AppEvent, CryptoOp, CryptoResult, Effect, SignatureCheck},
    event::{Direction, Event},
    handshake::{audience::Audience, challenge::Challenge, response::Response, HandshakeMessage},
    id::ConnId,
    machine::{Config, Machine, Now},
    outcome::{Fault, IgnoreReason, Outcome},
    peer_id::PeerId,
    timestamp::Timestamp,
    wall_clock::TimestampSeconds,
};
use testresult::TestResult;

/// A synchronous in-test "driver": executes crypto effects immediately and
/// collects sends/app events for the harness to shuttle.
struct TestPeer {
    machine: Machine,
    signing_key: SigningKey,
    sent: Vec<(ConnId, Vec<u8>)>,
    app: Vec<AppEvent>,
    disconnects: Vec<ConnId>,
    /// Faults surfaced by internally-fed crypto completions.
    faults: Vec<(ConnId, Fault)>,
}

impl TestPeer {
    fn new(seed: u8, discovery: Option<Audience>) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(signing_key.verifying_key());
        let mut config = Config::new(local_peer, [seed.wrapping_add(100); 32]);
        config.discovery = discovery;
        Self {
            machine: Machine::new(config),
            signing_key,
            sent: Vec::new(),
            app: Vec::new(),
            disconnects: Vec::new(),
            faults: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.signing_key.verifying_key())
    }

    /// Feed an event, then execute every drained effect synchronously,
    /// feeding crypto completions straight back in.
    fn feed(&mut self, now: Now, event: Event) -> Outcome {
        let outcome = self.machine.handle(now, event);
        self.run_effects(now);
        outcome
    }

    fn run_effects(&mut self, now: Now) {
        while let Some(effect) = self.machine.poll_effect() {
            match effect {
                Effect::SendMessage { conn, bytes } => self.sent.push((conn, bytes)),
                Effect::Disconnect { conn } => self.disconnects.push(conn),
                // No storage ops are issued during the handshake phase.
                Effect::Storage { .. } => {}
                Effect::App(app) => self.app.push(app),
                Effect::Crypto { ticket, op } => {
                    let result = match op {
                        CryptoOp::Sign { payload } => CryptoResult::Signed {
                            signature: self.signing_key.sign(&payload).to_bytes(),
                        },
                        CryptoOp::Verify(item) => CryptoResult::Verified(check_item(&item)),
                        CryptoOp::VerifyBatch(items) => {
                            CryptoResult::BatchVerified(items.iter().map(check_item).collect())
                        }
                    };
                    let outcome = self
                        .machine
                        .handle(now, Event::CryptoDone { ticket, result });
                    if let Outcome::ConnectionFault { conn, fault } = outcome {
                        self.faults.push((conn, fault));
                    }
                }
            }
        }
    }

    fn authenticated_with(&self) -> Option<PeerId> {
        self.app.iter().find_map(|event| match event {
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

fn check_item(item: &subduction_protocol::effect::VerifyItem) -> SignatureCheck {
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&item.verifying_key) else {
        return SignatureCheck::Invalid;
    };
    let sig = ed25519_dalek::Signature::from_bytes(&item.signature);
    if vk.verify_strict(&item.payload, &sig).is_ok() {
        SignatureCheck::Valid
    } else {
        SignatureCheck::Invalid
    }
}

const fn now(secs: u64) -> Now {
    Now {
        monotonic: Timestamp::from_millis(secs * 1000),
        wall: TimestampSeconds::new(1_700_000_000 + secs),
    }
}

const A_CONN: ConnId = ConnId::new(1);
const B_CONN: ConnId = ConnId::new(1);

/// Shuttle pending sends between the two peers until quiescent.
fn pump(a: &mut TestPeer, b: &mut TestPeer, t: Now) {
    for _ in 0..16 {
        let a_out: Vec<_> = a.sent.drain(..).collect();
        let b_out: Vec<_> = b.sent.drain(..).collect();
        if a_out.is_empty() && b_out.is_empty() {
            return;
        }
        for (_conn, bytes) in a_out {
            let _outcome = b.feed(
                t,
                Event::MessageReceived {
                    conn: B_CONN,
                    bytes,
                },
            );
        }
        for (_conn, bytes) in b_out {
            let _outcome = a.feed(
                t,
                Event::MessageReceived {
                    conn: A_CONN,
                    bytes,
                },
            );
        }
    }
}

#[test]
fn known_audience_handshake_completes() {
    let mut alice = TestPeer::new(1, None);
    let mut bob = TestPeer::new(2, None);
    let t = now(0);

    let outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob.peer_id())),
        },
    );
    assert_eq!(outcome, Outcome::Progressed);

    let outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );
    assert_eq!(outcome, Outcome::Progressed);

    pump(&mut alice, &mut bob, t);

    assert_eq!(alice.authenticated_with(), Some(bob.peer_id()));
    assert_eq!(bob.authenticated_with(), Some(alice.peer_id()));
    assert_eq!(alice.machine.stats().handshakes_completed, 1);
    assert_eq!(bob.machine.stats().handshakes_completed, 1);
    assert_eq!(alice.machine.poll_timeout(), None, "deadline disarmed");
    assert_eq!(bob.machine.poll_timeout(), None, "deadline disarmed");
}

#[test]
fn discovery_audience_handshake_completes() {
    let discovery = Audience::discover(b"sync.example.com");
    let mut alice = TestPeer::new(3, None);
    let mut bob = TestPeer::new(4, Some(discovery));
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(discovery),
        },
    );
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );

    pump(&mut alice, &mut bob, t);

    assert_eq!(alice.authenticated_with(), Some(bob.peer_id()));
    assert_eq!(bob.authenticated_with(), Some(alice.peer_id()));
}

#[test]
fn wrong_audience_is_rejected() {
    let mut alice = TestPeer::new(5, None);
    let mut bob = TestPeer::new(6, None);
    let mallory_id = PeerId::new([0xEE; 32]);
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            // Challenge addressed to someone who is not Bob.
            audience: Some(Audience::known(mallory_id)),
        },
    );
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );

    // Deliver Alice's challenge to Bob: he must reject and condemn.
    let (_conn, challenge_bytes) = alice.sent.remove(0);
    let outcome = bob.feed(
        t,
        Event::MessageReceived {
            conn: B_CONN,
            bytes: challenge_bytes,
        },
    );
    assert!(matches!(
        outcome,
        Outcome::ConnectionFault {
            fault: Fault::ChallengeRejected(_),
            ..
        }
    ));
    assert_eq!(bob.disconnects, [B_CONN]);

    // Bob's rejection reaches Alice: she must fault with the reason.
    let (_conn, rejection_bytes) = bob.sent.remove(0);
    let outcome = alice.feed(
        t,
        Event::MessageReceived {
            conn: A_CONN,
            bytes: rejection_bytes,
        },
    );
    assert!(matches!(
        outcome,
        Outcome::ConnectionFault {
            fault: Fault::HandshakeRejected(_),
            ..
        }
    ));
    assert_eq!(alice.disconnects, [A_CONN]);
}

#[test]
fn stale_completion_after_disconnect_is_ignored() -> TestResult {
    let mut alice = TestPeer::new(7, None);
    let bob_id = PeerId::new([0xBB; 32]);
    let t = now(0);

    // Start the handshake but do NOT run effects: the Sign op stays
    // in flight at the "driver".
    let outcome = alice.machine.handle(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob_id)),
        },
    );
    assert_eq!(outcome, Outcome::Progressed);

    let Some(Effect::Crypto { ticket, op }) = alice.machine.poll_effect() else {
        return Err("expected a pending crypto effect".into());
    };

    // The transport dies while the signature is still being computed.
    let outcome = alice
        .machine
        .handle(t, Event::Disconnected { conn: A_CONN });
    assert_eq!(outcome, Outcome::Progressed);

    // The teardown legitimately announces the closure…
    let Some(Effect::App(AppEvent::ConnectionClosed {
        conn: A_CONN,
        peer: None,
    })) = alice.machine.poll_effect()
    else {
        return Err("expected ConnectionClosed app event".into());
    };

    // The completion lands anyway — it must be a no-op.
    let CryptoOp::Sign { payload } = op else {
        return Err("expected a sign op".into());
    };
    let signature = alice.signing_key.sign(&payload).to_bytes();
    let outcome = alice.machine.handle(
        t,
        Event::CryptoDone {
            ticket,
            result: CryptoResult::Signed { signature },
        },
    );
    assert_eq!(
        outcome,
        Outcome::Ignored(IgnoreReason::UnknownConnection(A_CONN))
    );
    assert_eq!(alice.machine.poll_effect(), None, "no state was touched");
    Ok(())
}

#[test]
fn handshake_deadline_fires() -> TestResult {
    let mut alice = TestPeer::new(8, None);
    let bob_id = PeerId::new([0xBB; 32]);
    let t0 = now(0);

    let _outcome = alice.feed(
        t0,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob_id)),
        },
    );

    // Challenge went out; no response ever arrives.
    assert_eq!(alice.sent.len(), 1);
    let deadline = alice
        .machine
        .poll_timeout()
        .ok_or("handshake deadline must be armed")?;
    assert_eq!(
        deadline,
        t0.monotonic
            .saturating_add(Config::DEFAULT_HANDSHAKE_TIMEOUT)
    );

    // Wake before the deadline: nothing happens.
    let outcome = alice.feed(now(1), Event::Wake);
    assert_eq!(outcome, Outcome::Idle);
    assert_eq!(alice.disconnects, Vec::<ConnId>::new());

    // Wake after the deadline: the connection is condemned.
    let late = now(Config::DEFAULT_HANDSHAKE_TIMEOUT.as_secs() + 1);
    let outcome = alice.feed(late, Event::Wake);
    assert_eq!(outcome, Outcome::Progressed);
    assert_eq!(alice.disconnects, [A_CONN]);
    assert_eq!(alice.machine.poll_timeout(), None);
    assert_eq!(alice.machine.stats().handshake_timeouts, 1);
    Ok(())
}

#[test]
fn replayed_challenge_is_rejected() -> TestResult {
    let mut alice = TestPeer::new(9, None);
    let mut bob = TestPeer::new(10, None);
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob.peer_id())),
        },
    );
    let (_conn, challenge_bytes) = alice.sent.first().cloned().ok_or("challenge sent")?;

    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );
    pump(&mut alice, &mut bob, t);
    assert_eq!(bob.authenticated_with(), Some(alice.peer_id()));

    // An attacker replays Alice's original challenge on a new connection.
    let replay_conn = ConnId::new(99);
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: replay_conn,
            direction: Direction::Inbound,
            audience: None,
        },
    );
    // The signature is valid, so the fault fires on the internally-fed
    // verify completion — recorded by the harness.
    let outcome = bob.feed(
        t,
        Event::MessageReceived {
            conn: replay_conn,
            bytes: challenge_bytes,
        },
    );
    assert_eq!(outcome, Outcome::Progressed, "pure checks pass");
    assert_eq!(
        bob.faults,
        [(
            replay_conn,
            Fault::ChallengeRejected(
                subduction_protocol::handshake::rejection::RejectionReason::ReplayedNonce
            )
        )]
    );
    Ok(())
}

#[test]
fn peer_mismatch_is_detected() -> TestResult {
    // Alice pins Bob, but Mallory answers (with her own valid signature).
    let mut alice = TestPeer::new(11, None);
    let bob_id = PeerId::new([0xBB; 32]);
    let mallory = TestPeer::new(12, None);
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob_id)),
        },
    );
    let (_conn, challenge_bytes) = alice.sent.remove(0);

    // Mallory (as responder) would reject the audience normally; simulate
    // a malicious responder that answers anyway by having her accept any
    // audience via a discovery config… which she can't, for Known. So
    // craft it at the message level: Mallory receives the challenge on a
    // connection configured to accept it is impossible — instead, verify
    // the pin check directly by letting Mallory answer with a hand-built
    // response.
    let decoded = HandshakeMessage::try_decode(&challenge_bytes)?;
    let HandshakeMessage::SignedChallenge(signed_challenge) = decoded else {
        return Err("expected challenge".into());
    };
    // Reconstruct the typed challenge from its field bytes (the machine
    // does the same internally).
    let (challenge, _) = Challenge::try_decode_fields(signed_challenge.fields_bytes())?;
    let response = Response::for_challenge(&challenge, t.wall);

    // Mallory signs the response with HER key.
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&Response::SCHEMA);
    if let Some(disc) = Response::DISCRIMINANT {
        preimage.push(disc);
    }
    preimage.extend_from_slice(mallory.peer_id().as_bytes());
    response.encode_fields(&mut preimage);
    let signature = mallory.signing_key.sign(&preimage).to_bytes();
    let mut response_bytes = preimage;
    response_bytes.extend_from_slice(&signature);

    // Mallory's signature is valid, so the digest check and verify pass;
    // the pin check faults on the internally-fed verify completion.
    let outcome = alice.feed(
        t,
        Event::MessageReceived {
            conn: A_CONN,
            bytes: response_bytes,
        },
    );
    assert_eq!(outcome, Outcome::Progressed, "digest binding passes");
    assert_eq!(alice.faults, [(A_CONN, Fault::PeerMismatch)]);
    Ok(())
}

#[test]
fn extension_messages_are_surfaced_post_handshake() {
    let mut alice = TestPeer::new(13, None);
    let mut bob = TestPeer::new(14, None);
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob.peer_id())),
        },
    );
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );
    pump(&mut alice, &mut bob, t);
    assert_eq!(bob.authenticated_with(), Some(alice.peer_id()));

    // An ephemeral-style message (schema the machine does not own).
    let extension_bytes = b"SUE\x00some extension payload".to_vec();
    let outcome = bob.feed(
        t,
        Event::MessageReceived {
            conn: B_CONN,
            bytes: extension_bytes.clone(),
        },
    );
    assert_eq!(outcome, Outcome::Progressed);
    assert!(bob.app.iter().any(|event| matches!(
        event,
        AppEvent::ExtensionMessage { conn, peer, bytes }
            if *conn == B_CONN && *peer == alice.peer_id() && *bytes == extension_bytes
    )));

    // A handshake message post-auth is a violation, not an extension.
    let outcome = bob.feed(
        t,
        Event::MessageReceived {
            conn: B_CONN,
            bytes: b"SUH\x00\x00whatever".to_vec(),
        },
    );
    assert!(matches!(
        outcome,
        Outcome::ConnectionFault {
            conn: B_CONN,
            fault: Fault::UnexpectedMessage,
        }
    ));
}

#[test]
fn extension_messages_never_surface_pre_handshake() {
    let mut bob = TestPeer::new(15, None);
    let t = now(0);

    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Inbound,
            audience: None,
        },
    );

    // Extension bytes before authentication: condemned, never surfaced.
    let outcome = bob.feed(
        t,
        Event::MessageReceived {
            conn: B_CONN,
            bytes: b"SUE\x00sneaky pre-auth payload".to_vec(),
        },
    );
    assert!(matches!(
        outcome,
        Outcome::ConnectionFault {
            conn: B_CONN,
            fault: Fault::MalformedMessage,
        }
    ));
    assert!(bob.app.is_empty());
}

#[test]
fn simultaneous_open_authenticates_both_sides() {
    // Both sides dial each other on the same logical connection.
    let mut alice = TestPeer::new(16, None);
    let mut bob = TestPeer::new(17, None);
    let t = now(0);

    let alice_id = alice.peer_id();
    let bob_id = bob.peer_id();

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob_id)),
        },
    );
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(alice_id)),
        },
    );

    // Challenges cross on the wire; pump resolves the whole dance.
    pump(&mut alice, &mut bob, t);

    assert_eq!(alice.authenticated_with(), Some(bob_id), "alice side");
    assert_eq!(bob.authenticated_with(), Some(alice_id), "bob side");
    assert!(alice.faults.is_empty(), "alice faults: {:?}", alice.faults);
    assert!(bob.faults.is_empty(), "bob faults: {:?}", bob.faults);
    assert_eq!(alice.machine.poll_timeout(), None);
    assert_eq!(bob.machine.poll_timeout(), None);
}

#[test]
fn simultaneous_open_discovery_audience_works() {
    // Both sides dialed the same discovery endpoint (the legacy fallback
    // rule: a crossed challenge may carry OUR dialed discovery audience).
    let discovery = Audience::discover(b"rendezvous.example");
    let mut alice = TestPeer::new(18, Some(discovery));
    let mut bob = TestPeer::new(19, Some(discovery));
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(discovery),
        },
    );
    let _outcome = bob.feed(
        t,
        Event::Connected {
            conn: B_CONN,
            direction: Direction::Outbound,
            audience: Some(discovery),
        },
    );
    pump(&mut alice, &mut bob, t);

    assert_eq!(alice.authenticated_with(), Some(bob.peer_id()));
    assert_eq!(bob.authenticated_with(), Some(alice.peer_id()));
}

#[test]
fn reflected_challenge_is_detected() {
    let mut alice = TestPeer::new(20, None);
    let bob_id = PeerId::new([0xBB; 32]);
    let t = now(0);

    let _outcome = alice.feed(
        t,
        Event::Connected {
            conn: A_CONN,
            direction: Direction::Outbound,
            audience: Some(Audience::known(bob_id)),
        },
    );
    // Reflect alice's own challenge bytes straight back at her.
    let (_conn, own_challenge) = alice.sent.remove(0);
    let outcome = alice.feed(
        t,
        Event::MessageReceived {
            conn: A_CONN,
            bytes: own_challenge,
        },
    );
    assert!(matches!(
        outcome,
        Outcome::ConnectionFault {
            conn: A_CONN,
            fault: Fault::ReflectedChallenge,
        }
    ));
}
