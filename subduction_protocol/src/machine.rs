//! The Subduction protocol state machine.
//!
//! [`Machine`] owns every piece of protocol state as plain fields — no
//! locks, no futures, no clock, no IO. The driver feeds [`Event`]s through
//! [`handle`](Machine::handle) and drains [`Effect`]s via
//! [`poll_effect`](Machine::poll_effect); see the crate docs for the
//! architecture and `design/sans-io.md` for rationale.
//!
//! # Handshake sub-machine
//!
//! Each connection walks an explicit state graph; signing and verification
//! are driver-performed effects, so mid-crypto states are first-class
//! (ADR-006/007):
//!
//! ```text
//!  Outbound                              Inbound
//!  ────────                              ───────
//!  AwaitingChallengeSign                 AwaitingChallenge
//!    │ CryptoDone(Signed)                  │ MessageReceived(Challenge)
//!    │ → SendMessage(challenge)            │ → pure checks (audience,
//!    ▼                                     │    freshness), Verify effect
//!  AwaitingResponse                        ▼
//!    │ MessageReceived(Response)         AwaitingChallengeVerify
//!    │ → digest check, Verify effect       │ CryptoDone(Valid)
//!    ▼                                     │ → nonce claim, Sign effect
//!  AwaitingResponseVerify                  ▼
//!    │ CryptoDone(Valid)                 AwaitingResponseSign
//!    │ → pin check                         │ CryptoDone(Signed)
//!    ▼                                     │ → SendMessage(response)
//!  Authenticated                           ▼
//!                                        Authenticated
//! ```
//!
//! Any deviation (malformed bytes, wrong message for the state, failed
//! verification, expired deadline) condemns the connection: a
//! [`Disconnect`](Effect::Disconnect) effect is queued and the entry waits
//! in `Closing` for the driver's [`Disconnected`](Event::Disconnected)
//! confirmation.
//!
//! Simultaneous open (both sides dial each other on one connection) is not
//! yet ported from legacy — tracked for the iroh transport phase.

use alloc::{collections::VecDeque, vec::Vec};
use core::time::Duration;

use sedimentree_core::{
    codec::{encode::EncodeFields, schema::Schema},
    collections::Map,
};
use subduction_crypto::{nonce::Nonce, signed::Signed};

use crate::{
    effect::{AppEvent, CryptoOp, CryptoResult, Effect, SignatureCheck, VerifyItem},
    event::{Direction, Event},
    handshake::{
        HANDSHAKE_SCHEMA, HandshakeMessage, MAX_PLAUSIBLE_DRIFT,
        audience::Audience,
        challenge::Challenge,
        rejection::{Rejection, RejectionReason},
        response::Response,
    },
    id::{ConnId, Generation, Seq},
    nonce_cache::NonceCache,
    outcome::{Fault, IgnoreReason, Outcome},
    peer_id::PeerId,
    stats::Stats,
    timestamp::Timestamp,
    token::{CryptoToken, Scope},
    wall_clock::TimestampSeconds,
    wire,
};

/// The driver's view of "now", supplied with every [`Machine::handle`] call.
///
/// Two clocks because they answer different questions: `monotonic` orders
/// deadlines and never goes backwards; `wall` is Unix time that crosses the
/// wire in handshake freshness checks and may be corrected/skewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub struct Now {
    /// Monotonic driver time (deadlines).
    pub monotonic: Timestamp,

    /// Wall-clock Unix seconds (handshake freshness, nonce buckets).
    pub wall: TimestampSeconds,
}

/// Static configuration for a [`Machine`].
// Not `Copy`: config will grow non-`Copy` fields (limits, policies) in
// Phase 2, and removing a `Copy` impl later is a breaking change.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct Config {
    /// Our identity: the bytes of our ed25519 verifying key. The driver
    /// holds the signing key; the machine only ever names the identity.
    pub local_peer: PeerId,

    /// Audience we accept as a responder *in addition to*
    /// `Known(local_peer)` — the discovery hash of our public endpoint,
    /// if we have one.
    pub discovery: Option<Audience>,

    /// Maximum tolerated clock drift when validating challenge freshness.
    pub max_drift: Duration,

    /// How long a handshake may take before the connection is condemned.
    pub handshake_timeout: Duration,

    /// Seed for the nonce generator. Must be unpredictable (CSPRNG) and
    /// unique per machine instance.
    pub entropy: [u8; 32],
}

impl Config {
    /// Default handshake deadline.
    pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a config with defaults for everything but identity/entropy.
    #[must_use]
    pub const fn new(local_peer: PeerId, entropy: [u8; 32]) -> Self {
        Self {
            local_peer,
            discovery: None,
            max_drift: MAX_PLAUSIBLE_DRIFT,
            handshake_timeout: Self::DEFAULT_HANDSHAKE_TIMEOUT,
            entropy,
        }
    }
}

/// The sans-io protocol state machine. See the [module docs](self).
#[derive(Debug)]
pub struct Machine {
    config: Config,
    conns: Map<ConnId, ConnEntry>,
    effects: VecDeque<Effect>,
    nonce_cache: NonceCache,
    nonce_counter: u64,
    stats: Stats,
}

impl Machine {
    /// Create a machine from configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            conns: Map::new(),
            effects: VecDeque::new(),
            nonce_cache: NonceCache::default(),
            nonce_counter: 0,
            stats: Stats::default(),
        }
    }

    /// Feed one event to the machine.
    ///
    /// Also processes any deadlines that are due at `now`, regardless of
    /// the event — late or spurious [`Wake`](Event::Wake)s are harmless.
    /// Drain [`poll_effect`](Self::poll_effect) afterwards.
    pub fn handle(&mut self, now: Now, event: Event) -> Outcome {
        let fired = self.process_deadlines(now.monotonic);

        match event {
            Event::Connected {
                conn,
                direction,
                audience,
            } => self.on_connected(now, conn, direction, audience),
            Event::Disconnected { conn } => self.on_disconnected(conn),
            Event::MessageReceived { conn, bytes } => self.on_message(now, conn, &bytes),
            Event::CryptoDone { token, result } => self.on_crypto_done(now, token, result),
            Event::Wake => {
                if fired {
                    Outcome::Progressed
                } else {
                    Outcome::Idle
                }
            }
        }
    }

    /// Next queued effect, if any. Drain after every [`handle`](Self::handle).
    pub fn poll_effect(&mut self) -> Option<Effect> {
        self.effects.pop_front()
    }

    /// The earliest deadline the driver must wake the machine at, if any.
    #[must_use]
    pub fn poll_timeout(&self) -> Option<Timestamp> {
        self.conns.values().filter_map(|c| c.deadline).min()
    }

    /// Tier-2 telemetry snapshot.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    // ── event handlers ─────────────────────────────────────────────

    fn on_connected(
        &mut self,
        now: Now,
        conn: ConnId,
        direction: Direction,
        audience: Option<Audience>,
    ) -> Outcome {
        if self.conns.contains_key(&conn) {
            return Outcome::Ignored(IgnoreReason::DuplicateConnection(conn));
        }

        self.stats.connections_opened = self.stats.connections_opened.saturating_add(1);
        let deadline = Some(now.monotonic.saturating_add(self.config.handshake_timeout));

        match direction {
            Direction::Inbound => {
                self.conns.insert(
                    conn,
                    ConnEntry {
                        generation: Generation::FIRST,
                        next_seq: Seq::FIRST,
                        deadline,
                        peer: None,
                        state: HandshakeState::AwaitingChallenge,
                    },
                );
                Outcome::Progressed
            }
            Direction::Outbound => {
                let Some(audience) = audience else {
                    // Nothing was inserted; the driver must tear the
                    // transport down itself.
                    self.effects.push_back(Effect::Disconnect { conn });
                    self.stats.handshakes_failed = self.stats.handshakes_failed.saturating_add(1);
                    return Outcome::ConnectionFault {
                        conn,
                        fault: Fault::MissingAudience,
                    };
                };

                let pinned = match audience {
                    Audience::Known(peer) => Some(peer),
                    Audience::Discover(_) => None,
                };

                let challenge = Challenge::new(audience, now.wall, self.next_nonce());
                let preimage = signed_preimage(&self.config.local_peer, &challenge);

                let mut entry = ConnEntry {
                    generation: Generation::FIRST,
                    next_seq: Seq::FIRST,
                    deadline,
                    peer: None,
                    state: HandshakeState::AwaitingChallenge, // placeholder
                };
                let token = entry.issue_token(conn);
                entry.state = HandshakeState::AwaitingChallengeSign {
                    token,
                    challenge,
                    preimage: preimage.clone(),
                    pinned,
                };
                self.conns.insert(conn, entry);

                self.effects.push_back(Effect::Crypto {
                    token,
                    op: CryptoOp::Sign { payload: preimage },
                });
                Outcome::Progressed
            }
        }
    }

    fn on_disconnected(&mut self, conn: ConnId) -> Outcome {
        let Some(entry) = self.conns.remove(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };

        self.stats.connections_closed = self.stats.connections_closed.saturating_add(1);

        // A handshake that never completed and wasn't already condemned
        // (Closing counts at fault time) is a failure.
        match entry.state {
            HandshakeState::AwaitingChallenge
            | HandshakeState::AwaitingChallengeSign { .. }
            | HandshakeState::AwaitingResponse { .. }
            | HandshakeState::AwaitingResponseVerify { .. }
            | HandshakeState::AwaitingChallengeVerify { .. }
            | HandshakeState::AwaitingResponseSign { .. } => {
                self.stats.handshakes_failed = self.stats.handshakes_failed.saturating_add(1);
            }
            HandshakeState::Authenticated | HandshakeState::Closing => {}
        }

        self.effects.push_back(Effect::App(AppEvent::ConnectionClosed {
            conn,
            peer: entry.peer,
        }));
        Outcome::Progressed
    }

    fn on_message(&mut self, now: Now, conn: ConnId, bytes: &[u8]) -> Outcome {
        self.stats.messages_received = self.stats.messages_received.saturating_add(1);

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };

        match &entry.state {
            HandshakeState::Closing => Outcome::Ignored(IgnoreReason::ConnectionClosing(conn)),

            HandshakeState::Authenticated => {
                // Post-handshake routing is by 4-byte schema prefix
                // (ADR-010). Unknown schema ≠ malformed: extension protocols
                // multiplex over authenticated connections, so anything we
                // don't own is surfaced to the application.
                let Some(schema) = bytes.get(..4) else {
                    self.stats.malformed_messages =
                        self.stats.malformed_messages.saturating_add(1);
                    return self.fault(conn, Fault::MalformedMessage);
                };
                match schema {
                    // Sync protocol: Phase 2.
                    s if s == wire::MESSAGE_SCHEMA => {
                        Outcome::Ignored(IgnoreReason::NotYetImplemented)
                    }
                    // Re-handshake on an authenticated connection is a
                    // protocol violation (matches legacy: composed wire
                    // enums have no handshake variant).
                    s if s == HANDSHAKE_SCHEMA => self.fault(conn, Fault::UnexpectedMessage),
                    _extension => {
                        let Some(peer) = entry.peer else {
                            // Authenticated entries always carry a peer;
                            // treat a violation defensively as a fault
                            // rather than panicking.
                            return self.fault(conn, Fault::UnexpectedMessage);
                        };
                        self.effects.push_back(Effect::App(AppEvent::ExtensionMessage {
                            conn,
                            peer,
                            bytes: bytes.to_vec(),
                        }));
                        Outcome::Progressed
                    }
                }
            }

            HandshakeState::AwaitingChallenge => {
                let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
                    self.stats.malformed_messages =
                        self.stats.malformed_messages.saturating_add(1);
                    return self.fault(conn, Fault::MalformedMessage);
                };
                match msg {
                    HandshakeMessage::SignedChallenge(signed) => {
                        self.on_inbound_challenge(now, conn, &signed)
                    }
                    HandshakeMessage::SignedResponse(_) | HandshakeMessage::Rejection(_) => {
                        self.fault(conn, Fault::UnexpectedMessage)
                    }
                }
            }

            HandshakeState::AwaitingResponse { .. } => {
                let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
                    self.stats.malformed_messages =
                        self.stats.malformed_messages.saturating_add(1);
                    return self.fault(conn, Fault::MalformedMessage);
                };
                match msg {
                    HandshakeMessage::SignedResponse(signed) => {
                        self.on_outbound_response(conn, &signed)
                    }
                    HandshakeMessage::Rejection(rejection) => {
                        self.fault(conn, Fault::HandshakeRejected(rejection.reason))
                    }
                    // Simultaneous open: not yet ported (see module docs).
                    HandshakeMessage::SignedChallenge(_) => {
                        self.fault(conn, Fault::UnexpectedMessage)
                    }
                }
            }

            // No message is legal while we hold the turn's crypto pending.
            HandshakeState::AwaitingChallengeSign { .. }
            | HandshakeState::AwaitingResponseVerify { .. }
            | HandshakeState::AwaitingChallengeVerify { .. }
            | HandshakeState::AwaitingResponseSign { .. } => {
                self.fault(conn, Fault::UnexpectedMessage)
            }
        }
    }

    /// Responder: a challenge arrived. Pure checks first (audience,
    /// freshness), then hand the signature to the driver.
    fn on_inbound_challenge(
        &mut self,
        now: Now,
        conn: ConnId,
        signed: &Signed<Challenge>,
    ) -> Outcome {
        let challenge = match self.decode_and_validate_challenge(now, signed) {
            Ok(challenge) => challenge,
            Err(reason) => {
                self.effects.push_back(Effect::SendMessage {
                    conn,
                    bytes: HandshakeMessage::Rejection(Rejection::new(reason, now.wall)).encode(),
                });
                return self.fault(conn, Fault::ChallengeRejected(reason));
            }
        };

        let initiator = PeerId::from(signed.issuer());
        let item = verify_item(signed);

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let token = entry.issue_token(conn);
        entry.state = HandshakeState::AwaitingChallengeVerify {
            token,
            challenge,
            initiator,
        };
        self.effects.push_back(Effect::Crypto {
            token,
            op: CryptoOp::Verify(item),
        });
        Outcome::Progressed
    }

    /// Initiator: a response arrived. Check the digest binding (pure),
    /// then hand the signature to the driver.
    fn on_outbound_response(&mut self, conn: ConnId, signed: &Signed<Response>) -> Outcome {
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let HandshakeState::AwaitingResponse { challenge, pinned } = &entry.state else {
            return self.fault(conn, Fault::UnexpectedMessage);
        };

        let Ok((response, _consumed)) = try_decode_payload::<Response>(signed) else {
            self.stats.malformed_messages = self.stats.malformed_messages.saturating_add(1);
            return self.fault(conn, Fault::MalformedMessage);
        };

        if response.validate(challenge).is_err() {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }

        let responder = PeerId::from(signed.issuer());
        let pinned = *pinned;
        let item = verify_item(signed);

        let token = entry.issue_token(conn);
        entry.state = HandshakeState::AwaitingResponseVerify {
            token,
            responder,
            pinned,
        };
        self.effects.push_back(Effect::Crypto {
            token,
            op: CryptoOp::Verify(item),
        });
        Outcome::Progressed
    }

    fn on_crypto_done(&mut self, now: Now, token: CryptoToken, result: CryptoResult) -> Outcome {
        let Scope::Connection(conn) = token.scope else {
            // No Local-scoped operations exist yet (Phase 2).
            self.stats.unknown_tokens = self.stats.unknown_tokens.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::UnknownToken);
        };

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };

        if token.generation != entry.generation {
            self.stats.stale_completions = self.stats.stale_completions.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::StaleToken);
        }

        match (&entry.state, result) {
            // ── initiator: challenge signed ────────────────────────
            (
                HandshakeState::AwaitingChallengeSign {
                    token: expected,
                    challenge,
                    preimage,
                    pinned,
                },
                CryptoResult::Signed { signature },
            ) if *expected == token => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let (challenge, pinned) = (*challenge, *pinned);
                entry.state = HandshakeState::AwaitingResponse { challenge, pinned };
                self.effects.push_back(Effect::SendMessage { conn, bytes });
                Outcome::Progressed
            }

            // ── initiator: response verified ───────────────────────
            (
                HandshakeState::AwaitingResponseVerify {
                    token: expected,
                    responder,
                    pinned,
                },
                CryptoResult::Verified(check),
            ) if *expected == token => match check {
                SignatureCheck::Invalid => self.fault(conn, Fault::HandshakeVerificationFailed),
                SignatureCheck::Valid => {
                    if let Some(pinned) = pinned
                        && pinned != responder
                    {
                        return self.fault(conn, Fault::PeerMismatch);
                    }
                    let peer = *responder;
                    self.authenticate(conn, peer)
                }
            },

            // ── responder: challenge verified ──────────────────────
            (
                HandshakeState::AwaitingChallengeVerify {
                    token: expected,
                    challenge,
                    initiator,
                },
                CryptoResult::Verified(check),
            ) if *expected == token => {
                let (challenge, initiator) = (*challenge, *initiator);
                match check {
                    SignatureCheck::Invalid => {
                        self.reject_challenge(now, conn, RejectionReason::InvalidSignature)
                    }
                    SignatureCheck::Valid => {
                        self.on_challenge_verified(now, conn, &challenge, initiator)
                    }
                }
            }

            // ── responder: response signed ─────────────────────────
            (
                HandshakeState::AwaitingResponseSign {
                    token: expected,
                    preimage,
                    initiator,
                },
                CryptoResult::Signed { signature },
            ) if *expected == token => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let peer = *initiator;
                self.effects.push_back(Effect::SendMessage { conn, bytes });
                self.authenticate(conn, peer)
            }

            // Right generation, but no pending operation matches this
            // token/result shape — a duplicate, or a driver bug.
            (
                HandshakeState::AwaitingChallenge
                | HandshakeState::AwaitingChallengeSign { .. }
                | HandshakeState::AwaitingResponse { .. }
                | HandshakeState::AwaitingResponseVerify { .. }
                | HandshakeState::AwaitingChallengeVerify { .. }
                | HandshakeState::AwaitingResponseSign { .. }
                | HandshakeState::Authenticated
                | HandshakeState::Closing,
                CryptoResult::Signed { .. }
                | CryptoResult::Verified(_)
                | CryptoResult::BatchVerified(_),
            ) => {
                self.stats.unknown_tokens = self.stats.unknown_tokens.saturating_add(1);
                Outcome::Ignored(IgnoreReason::UnknownToken)
            }
        }
    }

    // ── helpers ────────────────────────────────────────────────────

    /// Responder: the challenge signature checked out. Claim the nonce
    /// (only after verification — cache-filling `DoS` prevention, as
    /// legacy), then hand our response to the driver for signing.
    fn on_challenge_verified(
        &mut self,
        now: Now,
        conn: ConnId,
        challenge: &Challenge,
        initiator: PeerId,
    ) -> Outcome {
        if self
            .nonce_cache
            .try_claim(initiator, challenge.nonce, now.wall)
            .is_err()
        {
            return self.reject_challenge(now, conn, RejectionReason::ReplayedNonce);
        }

        let response = Response::for_challenge(challenge, now.wall);
        let preimage = signed_preimage(&self.config.local_peer, &response);
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let token = entry.issue_token(conn);
        entry.state = HandshakeState::AwaitingResponseSign {
            token,
            preimage: preimage.clone(),
            initiator,
        };
        self.effects.push_back(Effect::Crypto {
            token,
            op: CryptoOp::Sign { payload: preimage },
        });
        Outcome::Progressed
    }

    /// Responder: send an unsigned [`Rejection`] and condemn the
    /// connection.
    fn reject_challenge(&mut self, now: Now, conn: ConnId, reason: RejectionReason) -> Outcome {
        self.effects.push_back(Effect::SendMessage {
            conn,
            bytes: HandshakeMessage::Rejection(Rejection::new(reason, now.wall)).encode(),
        });
        self.fault(conn, Fault::ChallengeRejected(reason))
    }

    /// Pure challenge checks: decode fields, audience, freshness.
    fn decode_and_validate_challenge(
        &self,
        now: Now,
        signed: &Signed<Challenge>,
    ) -> Result<Challenge, RejectionReason> {
        let Ok((challenge, _consumed)) = try_decode_payload::<Challenge>(signed) else {
            return Err(RejectionReason::InvalidSignature);
        };

        let known = Audience::known(self.config.local_peer);
        let audience_ok = challenge.audience == known
            || self
                .config
                .discovery
                .as_ref()
                .is_some_and(|d| challenge.audience == *d);
        if !audience_ok {
            return Err(RejectionReason::InvalidAudience);
        }

        if !challenge.is_fresh(now.wall, self.config.max_drift) {
            return Err(RejectionReason::ClockDrift);
        }

        Ok(challenge)
    }

    /// Condemn a connection: queue a disconnect, park it in `Closing`.
    fn fault(&mut self, conn: ConnId, fault: Fault) -> Outcome {
        if let Some(entry) = self.conns.get_mut(&conn) {
            entry.generation = entry.generation.next();
            entry.deadline = None;
            entry.state = HandshakeState::Closing;
        }
        self.stats.handshakes_failed = self.stats.handshakes_failed.saturating_add(1);
        self.effects.push_back(Effect::Disconnect { conn });
        Outcome::ConnectionFault { conn, fault }
    }

    fn authenticate(&mut self, conn: ConnId, peer: PeerId) -> Outcome {
        if let Some(entry) = self.conns.get_mut(&conn) {
            entry.deadline = None;
            entry.peer = Some(peer);
            entry.state = HandshakeState::Authenticated;
        }
        self.stats.handshakes_completed = self.stats.handshakes_completed.saturating_add(1);
        self.effects
            .push_back(Effect::App(AppEvent::PeerAuthenticated { conn, peer }));
        Outcome::Progressed
    }

    /// Condemn connections whose deadlines have passed. Returns whether
    /// any fired. Faults surface via effects and stats (an [`Outcome`]
    /// describes the *event*, and deadlines fire on any event).
    fn process_deadlines(&mut self, now: Timestamp) -> bool {
        let due: Vec<ConnId> = self
            .conns
            .iter()
            .filter_map(|(conn, entry)| {
                entry
                    .deadline
                    .is_some_and(|deadline| deadline.is_due(now))
                    .then_some(*conn)
            })
            .collect();

        for conn in &due {
            self.stats.handshake_timeouts = self.stats.handshake_timeouts.saturating_add(1);
            let _outcome = self.fault(*conn, Fault::HandshakeTimeout);
        }
        !due.is_empty()
    }

    /// Deterministic PRF nonce stream: `blake3_keyed(entropy, counter)`.
    /// Unpredictable to anyone without the entropy seed.
    fn next_nonce(&mut self) -> Nonce {
        let hash = blake3::keyed_hash(&self.config.entropy, &self.nonce_counter.to_be_bytes());
        self.nonce_counter = self.nonce_counter.saturating_add(1);
        let mut bytes = [0u8; 16];
        // blake3 output is 32 bytes; taking the first 16 cannot fail.
        #[allow(clippy::indexing_slicing)]
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        Nonce::from_bytes(bytes)
    }
}

// ── connection entry ────────────────────────────────────────────────

/// Per-connection machine state.
#[derive(Debug)]
struct ConnEntry {
    /// Bumped on teardown so in-flight completions go stale (ADR-006/007).
    generation: Generation,

    /// Next operation sequence number under the current generation.
    next_seq: Seq,

    /// Handshake deadline, if one is armed.
    deadline: Option<Timestamp>,

    /// The authenticated peer, once known.
    peer: Option<PeerId>,

    /// Where this connection is in the handshake.
    state: HandshakeState,
}

impl ConnEntry {
    const fn issue_token(&mut self, conn: ConnId) -> CryptoToken {
        let token = CryptoToken {
            scope: Scope::Connection(conn),
            generation: self.generation,
            seq: self.next_seq,
        };
        self.next_seq = self.next_seq.next();
        token
    }
}

/// The handshake sub-machine. `Awaiting{…}Sign`/`…Verify` states hold the
/// expected [`CryptoToken`] — the witness that pairs the eventual
/// completion with exactly this state (ADR-007).
#[derive(Debug)]
enum HandshakeState {
    /// Inbound: waiting for the initiator's challenge.
    AwaitingChallenge,

    /// Outbound: our challenge is at the driver being signed.
    AwaitingChallengeSign {
        token: CryptoToken,
        challenge: Challenge,
        preimage: Vec<u8>,
        pinned: Option<PeerId>,
    },

    /// Outbound: challenge sent; waiting for the responder.
    AwaitingResponse {
        challenge: Challenge,
        pinned: Option<PeerId>,
    },

    /// Outbound: the response signature is at the driver being verified.
    AwaitingResponseVerify {
        token: CryptoToken,
        responder: PeerId,
        pinned: Option<PeerId>,
    },

    /// Inbound: the challenge signature is at the driver being verified.
    AwaitingChallengeVerify {
        token: CryptoToken,
        challenge: Challenge,
        initiator: PeerId,
    },

    /// Inbound: our response is at the driver being signed.
    AwaitingResponseSign {
        token: CryptoToken,
        preimage: Vec<u8>,
        initiator: PeerId,
    },

    /// Handshake complete; sync protocol takes over (Phase 2).
    Authenticated,

    /// Condemned; waiting for the driver's `Disconnected` confirmation.
    Closing,
}

// ── free helpers ────────────────────────────────────────────────────

/// Build the byte preimage that [`Signed::seal`] signs:
/// `schema + discriminant? + issuer + fields`. Appending an ed25519
/// signature over these bytes yields valid `Signed<T>` wire bytes.
fn signed_preimage<T: Schema + EncodeFields>(issuer: &PeerId, payload: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&T::SCHEMA);
    if let Some(disc) = T::DISCRIMINANT {
        buf.push(disc);
    }
    buf.extend_from_slice(issuer.as_bytes());
    payload.encode_fields(&mut buf);
    buf
}

/// The verification job for a received [`Signed<T>`]: check the claimed
/// issuer's signature over the signed region.
fn verify_item<T: Schema + EncodeFields + sedimentree_core::codec::decode::DecodeFields>(
    signed: &Signed<T>,
) -> VerifyItem {
    VerifyItem {
        verifying_key: signed.issuer().to_bytes(),
        payload: signed.payload_bytes().to_vec(),
        signature: signed.signature().to_bytes(),
    }
}

/// Decode the typed payload fields out of a received [`Signed<T>`]
/// without verifying the signature (verification is a driver effect).
fn try_decode_payload<T>(signed: &Signed<T>) -> Result<(T, usize), sedimentree_core::codec::error::DecodeError>
where
    T: Schema + EncodeFields + sedimentree_core::codec::decode::DecodeFields,
{
    T::try_decode_fields(signed.fields_bytes())
}
