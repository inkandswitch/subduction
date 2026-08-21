//! The per-connection machine: handshake typestate, inline
//! verification, and extension gating for exactly one connection
//! incarnation.
//!
//! A `ConnMachine` is created per transport connection (and re-created,
//! with a bumped [`Generation`], on supervisor restart). It owns nothing
//! shared: the one cross-connection concern in the handshake — nonce
//! replay protection — is delegated to the core over the sealed edge
//! ([`ConnToCore::ClaimNonce`] → [`CoreToConn::NonceVerdict`]), which is
//! this machine's single `Awaiting`-on-the-core state.
//!
//! ```text
//!            Connected (construction: direction + audience)
//!        (Outbound)                     (Inbound)
//!            │                              │
//!   AwaitingChallengeSign            AwaitingChallenge
//!            │ SignDone                     │ challenge: verify inline
//!            ▼                              ▼
//!    AwaitingResponse ──crossed──▶   AwaitingNonceClaim ←─ the one
//!            │ response:  challenge:        │ NonceVerdict   core round
//!            │ verify     sim-open          ▼                trip
//!            │ inline     (see below)  AwaitingResponseSign
//!            ▼                              │ SignDone → send
//!       AUTHENTICATED ◀─────────────────────┘
//! ```
//!
//! Simultaneous open (reflection guards → inline verify → tie-break →
//! loser signs first) reuses this module's sign states.
//!
//! Post-authentication this machine is a verify-and-forward gate:
//! extension-schema messages surface directly (auth-gated); sync-schema
//! messages are decoded, verified, and forwarded as [`SyncForward`].

use alloc::{collections::VecDeque, vec, vec::Vec};
use core::time::Duration;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment as TreeFragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::{nonce::Nonce, signed::Signed};

use crate::{
    blob_ref::{BlobRef, FrameId},
    edge::{
        ConnToCore, CoreToConn, EdgeId, EdgeSequencer, ForwardStatus, Sealed, SyncForward,
        VerifiedCommit, VerifiedFragment,
    },
    event::Direction,
    handshake::{
        HANDSHAKE_SCHEMA, HandshakeMessage, MAX_PLAUSIBLE_DRIFT, SIMULTANEOUS_OPEN_MAX_DRIFT,
        audience::Audience,
        challenge::Challenge,
        pinned_peer,
        rejection::{Rejection, RejectionReason},
        response::Response,
        signed_preimage,
    },
    id::{ConnId, Generation, Seq},
    outcome::{Fault, IgnoreReason, Outcome},
    peer_id::PeerId,
    ticket::{CryptoTicket, Entity},
    timestamp::{Now, Timestamp},
    wire,
};

/// Static configuration for one [`ConnMachine`].
// Not `Copy`: will grow non-`Copy` fields (per-conn policy hooks), and
// removing a `Copy` impl later is a breaking change.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct ConnConfig {
    /// Our identity (the bytes of our verifying key).
    pub local_peer: PeerId,

    /// Discovery audience we accept as a responder, if any.
    pub discovery: Option<Audience>,

    /// Maximum tolerated clock drift for challenge freshness.
    pub max_drift: Duration,

    /// Handshake deadline (also covers the nonce-claim round trip).
    pub handshake_timeout: Duration,

    /// Per-machine entropy for challenge nonces (CSPRNG-seeded, unique
    /// per incarnation).
    pub entropy: [u8; 32],
}

impl ConnConfig {
    /// Defaults for everything but identity/entropy.
    #[must_use]
    pub const fn new(local_peer: PeerId, entropy: [u8; 32]) -> Self {
        Self {
            local_peer,
            discovery: None,
            max_drift: MAX_PLAUSIBLE_DRIFT,
            handshake_timeout: Duration::from_secs(30),
            entropy,
        }
    }
}

/// What a [`ConnMachine`] asks of the world. Everything here is either a
/// leaf effect (driver-executed) or a sealed edge message (router-moved,
/// driver-opaque).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEffect {
    /// Send one complete wire message on this connection.
    Send {
        /// Scatter-gather parts (handshake traffic is always literal
        /// bytes; blob refs appear on the sync path).
        parts: Vec<crate::blob_ref::Part>,
    },

    /// Close this connection.
    Disconnect,

    /// Sign with the machine's identity key (external custody).
    Sign {
        /// Completion witness.
        ticket: CryptoTicket,
        /// Bytes to sign.
        payload: Vec<u8>,
    },

    /// A sealed message for the core (router-moved; unforgeable).
    ToCore(Sealed<ConnToCore>),

    /// No [`BlobRef`]s escaped from this frame; the driver may free it.
    /// (Frames with escaped refs are freed by ref releases + the edge
    /// epoch backstop.)
    ReleaseFrame(FrameId),

    /// An application-facing event from this connection.
    App(ConnAppEvent),
}

/// Application-facing events surfaced directly by a connection machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnAppEvent {
    /// The handshake completed.
    PeerAuthenticated {
        /// The verified peer.
        peer: PeerId,
    },

    /// An extension-protocol message arrived (auth-gated).
    ExtensionMessage {
        /// The authenticated peer.
        peer: PeerId,
        /// The complete message, schema prefix included.
        bytes: Vec<u8>,
    },
}

/// Inputs to a [`ConnMachine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnEvent {
    /// One complete wire message arrived (frame retained by the driver;
    /// the id anchors any [`BlobRef`]s minted
    /// from it).
    MessageReceived {
        /// The retained frame's id.
        frame: crate::blob_ref::FrameId,
        /// The frame's bytes.
        bytes: Vec<u8>,
    },

    /// A signing completion (ticket echoed from [`ConnEffect::Sign`]).
    ///
    /// _Trust boundary:_ the signature bytes are appended to the wire
    /// message _unverified_ — the driver's signer is the local trust
    /// root (it holds the signing key). A corrupt signature costs only
    /// liveness: the remote peer rejects the handshake message.
    SignDone {
        /// The witness.
        ticket: CryptoTicket,
        /// The signature.
        signature: [u8; 64],
    },

    /// A sealed answer from the core.
    FromCore(Sealed<CoreToConn>),

    /// The application wants to send an extension message.
    SendExtension {
        /// The complete extension message.
        bytes: Vec<u8>,
    },

    /// The transport is gone; emit teardown and become terminal.
    TransportClosed,

    /// Timer service (deadlines are re-derived from `now`).
    Wake,
}

/// The per-connection handshake/gating machine. See the [module
/// docs](self).
#[derive(Debug)]
pub struct ConnMachine {
    config: ConnConfig,
    edge: EdgeId,
    state: State,
    peer: Option<PeerId>,
    deadline: Option<Timestamp>,
    effects: VecDeque<ConnEffect>,
    /// Sequence for minting outbound edge messages.
    out_seq: Seq,
    /// Discipline for inbound core answers.
    from_core: EdgeSequencer,
    /// Sequence for sign tickets.
    ticket_seq: Seq,
    nonce_counter: u64,
    /// Sync items dropped by verification (tier-2 telemetry).
    rejected_items: u64,
    /// Whether the current frame minted any escaping [`BlobRef`]s.
    minted_from_frame: bool,
}

impl ConnMachine {
    /// Create the machine for a fresh connection incarnation and begin
    /// the handshake. Outbound connections require an audience.
    #[must_use]
    pub fn new(
        config: ConnConfig,
        conn: ConnId,
        generation: Generation,
        direction: Direction,
        audience: Option<Audience>,
        now: Now,
    ) -> Self {
        let edge = EdgeId { conn, generation };
        let mut machine = Self {
            config,
            edge,
            state: State::Failed, // placeholder; set below
            peer: None,
            deadline: None,
            effects: VecDeque::new(),
            out_seq: Seq::FIRST,
            from_core: EdgeSequencer::new(edge),
            ticket_seq: Seq::FIRST,
            nonce_counter: 0,
            rejected_items: 0,
            minted_from_frame: false,
        };
        machine.deadline = Some(
            now.monotonic
                .saturating_add(machine.config.handshake_timeout),
        );
        machine.send_to_core(ConnToCore::Opened { direction });

        match direction {
            Direction::Inbound => {
                machine.state = State::AwaitingChallenge;
            }
            Direction::Outbound => match audience {
                None => {
                    machine.state = State::Failed;
                    machine.effects.push_back(ConnEffect::Disconnect);
                    machine.send_to_core(ConnToCore::Closed {
                        fault: Some(Fault::MissingAudience),
                    });
                }
                Some(audience) => {
                    let challenge = Challenge::new(audience, now.wall, machine.next_nonce());
                    let preimage = signed_preimage(&machine.config.local_peer, &challenge);
                    let ticket = machine.issue_ticket();
                    machine.state = State::AwaitingChallengeSign {
                        ticket,
                        challenge,
                        preimage: preimage.clone(),
                    };
                    machine.effects.push_back(ConnEffect::Sign {
                        ticket,
                        payload: preimage,
                    });
                }
            },
        }
        machine
    }

    /// Feed one event; drain [`poll_effect`](Self::poll_effect) after.
    pub fn handle(&mut self, now: Now, event: ConnEvent) -> Outcome {
        if self
            .deadline
            .is_some_and(|deadline| deadline.is_due(now.monotonic))
            && !matches!(self.state, State::Authenticated | State::Failed)
        {
            let _outcome = self.fault(Fault::HandshakeTimeout);
        }

        match event {
            ConnEvent::MessageReceived { frame, bytes } => {
                self.minted_from_frame = false;
                let outcome = self.on_message(now, frame, &bytes);
                if !self.minted_from_frame {
                    // Nothing points into this frame; the driver may free it.
                    self.effects.push_back(ConnEffect::ReleaseFrame(frame));
                }
                outcome
            }
            ConnEvent::SignDone { ticket, signature } => self.on_signed(ticket, signature),
            ConnEvent::FromCore(sealed) => self.on_from_core(now, sealed),
            ConnEvent::SendExtension { bytes } => self.on_send_extension(bytes),
            ConnEvent::TransportClosed => self.on_transport_closed(),
            ConnEvent::Wake => {
                if matches!(self.state, State::Failed) {
                    Outcome::Progressed
                } else {
                    Outcome::Idle
                }
            }
        }
    }

    /// Next queued effect.
    pub fn poll_effect(&mut self) -> Option<ConnEffect> {
        self.effects.pop_front()
    }

    /// The next deadline, if armed.
    #[must_use]
    pub const fn poll_timeout(&self) -> Option<Timestamp> {
        self.deadline
    }

    /// This machine's edge identity.
    #[must_use]
    pub const fn edge(&self) -> EdgeId {
        self.edge
    }

    /// The authenticated peer, once the handshake completes.
    #[must_use]
    pub const fn peer(&self) -> Option<PeerId> {
        self.peer
    }

    // ── message handling ───────────────────────────────────────────

    fn on_message(&mut self, now: Now, frame: FrameId, bytes: &[u8]) -> Outcome {
        match &self.state {
            State::Failed => Outcome::Ignored(IgnoreReason::ConnectionClosing(self.edge.conn)),

            State::Authenticated => self.on_authenticated_message(frame, bytes),

            State::AwaitingChallenge => {
                let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
                    return self.fault(Fault::MalformedMessage);
                };
                match msg {
                    HandshakeMessage::SignedChallenge(signed) => {
                        self.on_inbound_challenge(now, &signed)
                    }
                    HandshakeMessage::SignedResponse(_) | HandshakeMessage::Rejection(_) => {
                        self.fault(Fault::UnexpectedMessage)
                    }
                }
            }

            State::AwaitingResponse { .. } => {
                let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
                    return self.fault(Fault::MalformedMessage);
                };
                match msg {
                    HandshakeMessage::SignedResponse(signed) => self.on_outbound_response(&signed),
                    HandshakeMessage::Rejection(rejection) => {
                        self.fault(Fault::HandshakeRejected(rejection.reason))
                    }
                    HandshakeMessage::SignedChallenge(signed) => {
                        self.on_sim_open_challenge(now, bytes, &signed)
                    }
                }
            }

            State::SimOpenAwaitTheirResponse { .. } => self.on_sim_open_message(now, bytes),

            State::AwaitingChallengeSign { .. }
            | State::AwaitingNonceClaim { .. }
            | State::AwaitingResponseSign { .. }
            | State::SimOpenLoserSign { .. } => self.fault(Fault::UnexpectedMessage),
        }
    }

    /// Post-auth routing by schema: sync messages are decoded,
    /// verified inline, and forwarded to the core with blobs by
    /// reference into the retained frame.
    fn on_authenticated_message(&mut self, frame: FrameId, bytes: &[u8]) -> Outcome {
        let Some(schema) = bytes.get(..4) else {
            return self.fault(Fault::MalformedMessage);
        };
        match schema {
            s if s == wire::MESSAGE_SCHEMA => self.forward_sync(frame, bytes),
            s if s == HANDSHAKE_SCHEMA => self.fault(Fault::UnexpectedMessage),
            _extension => {
                let Some(peer) = self.peer else {
                    return self.fault(Fault::UnexpectedMessage);
                };
                self.effects
                    .push_back(ConnEffect::App(ConnAppEvent::ExtensionMessage {
                        peer,
                        bytes: bytes.to_vec(),
                    }));
                Outcome::Progressed
            }
        }
    }

    /// Responder: challenge arrived. Verify inline, validate,
    /// then claim the nonce at the core (the one core round trip).
    fn on_inbound_challenge(&mut self, now: Now, signed: &Signed<Challenge>) -> Outcome {
        let Ok(verified) = signed.try_verify() else {
            return self.reject(now, RejectionReason::InvalidSignature);
        };
        let challenge = *verified.payload();
        let initiator = PeerId::from(verified.issuer());

        let known = Audience::known(self.config.local_peer);
        let audience_ok = challenge.audience == known
            || self
                .config
                .discovery
                .as_ref()
                .is_some_and(|d| challenge.audience == *d);
        if !audience_ok {
            return self.reject(now, RejectionReason::InvalidAudience);
        }
        if !challenge.is_fresh(now.wall, self.config.max_drift) {
            return self.reject(now, RejectionReason::ClockDrift);
        }

        self.state = State::AwaitingNonceClaim {
            challenge,
            initiator,
        };
        self.send_to_core(ConnToCore::ClaimNonce {
            peer: initiator,
            nonce: challenge.nonce,
            // Bucketed by the message-signed timestamp (not arrival
            // time) so the claim is a pure function of the message and
            // stays journal/replay-deterministic. Node-local; wire
            // behavior unchanged.
            timestamp: challenge.timestamp,
        });
        Outcome::Progressed
    }

    /// Initiator: response arrived. Verify + bind + pin, all inline.
    fn on_outbound_response(&mut self, signed: &Signed<Response>) -> Outcome {
        let State::AwaitingResponse { challenge, .. } = &self.state else {
            return self.fault(Fault::UnexpectedMessage);
        };
        let pinned = pinned_peer(challenge);
        let challenge = *challenge;

        let Ok(verified) = signed.try_verify() else {
            return self.fault(Fault::HandshakeVerificationFailed);
        };
        if verified.payload().validate(&challenge).is_err() {
            return self.fault(Fault::HandshakeVerificationFailed);
        }
        let responder = PeerId::from(verified.issuer());
        if let Some(pinned) = pinned
            && pinned != responder
        {
            return self.fault(Fault::PeerMismatch);
        }
        self.authenticate(responder)
    }

    // ── simultaneous open ──────────────────────────────────────────

    fn on_sim_open_challenge(
        &mut self,
        now: Now,
        raw: &[u8],
        signed: &Signed<Challenge>,
    ) -> Outcome {
        let State::AwaitingResponse {
            challenge: our_challenge,
            signed_bytes,
        } = &self.state
        else {
            return self.fault(Fault::UnexpectedMessage);
        };
        let our_challenge = *our_challenge;

        if signed_bytes.as_slice() == raw {
            return self.fault(Fault::ReflectedChallenge);
        }
        let we_win = signed_bytes.as_slice() > raw;

        let Ok(verified) = signed.try_verify() else {
            return self.fault(Fault::HandshakeVerificationFailed);
        };
        let their_challenge = *verified.payload();
        let their_peer = PeerId::from(verified.issuer());

        if their_peer == self.config.local_peer {
            return self.fault(Fault::ReflectionAttack);
        }
        let known = Audience::known(self.config.local_peer);
        let dialed_discovery = matches!(our_challenge.audience, Audience::Discover(_))
            .then_some(our_challenge.audience);
        let audience_ok = their_challenge.audience == known
            || dialed_discovery.is_some_and(|d| their_challenge.audience == d);
        if !audience_ok || !their_challenge.is_fresh(now.wall, SIMULTANEOUS_OPEN_MAX_DRIFT) {
            return self.fault(Fault::HandshakeVerificationFailed);
        }
        if let Some(pinned) = pinned_peer(&our_challenge)
            && pinned != their_peer
        {
            return self.fault(Fault::PeerMismatch);
        }

        if we_win {
            self.state = State::SimOpenAwaitTheirResponse {
                our_challenge,
                owed: Some(their_challenge),
                expected: their_peer,
            };
            Outcome::Progressed
        } else {
            let response = Response::for_challenge(&their_challenge, now.wall);
            let preimage = signed_preimage(&self.config.local_peer, &response);
            let ticket = self.issue_ticket();
            self.state = State::SimOpenLoserSign {
                ticket,
                preimage: preimage.clone(),
                our_challenge,
                expected: their_peer,
            };
            self.effects.push_back(ConnEffect::Sign {
                ticket,
                payload: preimage,
            });
            Outcome::Progressed
        }
    }

    fn on_sim_open_message(&mut self, now: Now, bytes: &[u8]) -> Outcome {
        let Ok(msg) = HandshakeMessage::try_decode(bytes) else {
            return self.fault(Fault::MalformedMessage);
        };
        match msg {
            HandshakeMessage::SignedResponse(signed) => {
                let State::SimOpenAwaitTheirResponse {
                    our_challenge,
                    owed,
                    expected,
                } = &self.state
                else {
                    return self.fault(Fault::UnexpectedMessage);
                };
                let (our_challenge, owed, expected) = (*our_challenge, *owed, *expected);

                let Ok(verified) = signed.try_verify() else {
                    return self.fault(Fault::HandshakeVerificationFailed);
                };
                if verified.payload().validate(&our_challenge).is_err() {
                    return self.fault(Fault::HandshakeVerificationFailed);
                }
                let responder = PeerId::from(verified.issuer());
                if responder != expected {
                    return self.fault(Fault::SimultaneousOpenPeerMismatch);
                }

                match owed {
                    Some(their_challenge) => {
                        let response = Response::for_challenge(&their_challenge, now.wall);
                        let preimage = signed_preimage(&self.config.local_peer, &response);
                        let ticket = self.issue_ticket();
                        self.state = State::AwaitingResponseSign {
                            ticket,
                            preimage: preimage.clone(),
                            initiator: expected,
                        };
                        self.effects.push_back(ConnEffect::Sign {
                            ticket,
                            payload: preimage,
                        });
                        Outcome::Progressed
                    }
                    None => self.authenticate(expected),
                }
            }
            HandshakeMessage::Rejection(rejection) => {
                self.fault(Fault::HandshakeRejected(rejection.reason))
            }
            HandshakeMessage::SignedChallenge(_) => self.fault(Fault::UnexpectedMessage),
        }
    }

    // ── completions ────────────────────────────────────────────────

    fn on_signed(&mut self, ticket: CryptoTicket, signature: [u8; 64]) -> Outcome {
        if ticket.entity != Entity::Connection(self.edge.conn)
            || ticket.generation != self.edge.generation
        {
            return Outcome::Ignored(IgnoreReason::StaleTicket);
        }

        match &self.state {
            State::AwaitingChallengeSign {
                ticket: expected,
                challenge,
                preimage,
            } if *expected == ticket => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let challenge = *challenge;
                self.state = State::AwaitingResponse {
                    challenge,
                    signed_bytes: bytes.clone(),
                };
                self.send_bytes(bytes);
                Outcome::Progressed
            }

            State::AwaitingResponseSign {
                ticket: expected,
                preimage,
                initiator,
            } if *expected == ticket => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let peer = *initiator;
                self.send_bytes(bytes);
                self.authenticate(peer)
            }

            State::SimOpenLoserSign {
                ticket: expected,
                preimage,
                our_challenge,
                expected: expected_peer,
            } if *expected == ticket => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let (our_challenge, expected_peer) = (*our_challenge, *expected_peer);
                self.state = State::SimOpenAwaitTheirResponse {
                    our_challenge,
                    owed: None,
                    expected: expected_peer,
                };
                self.send_bytes(bytes);
                Outcome::Progressed
            }

            State::AwaitingChallenge
            | State::AwaitingChallengeSign { .. }
            | State::AwaitingResponse { .. }
            | State::AwaitingNonceClaim { .. }
            | State::AwaitingResponseSign { .. }
            | State::SimOpenLoserSign { .. }
            | State::SimOpenAwaitTheirResponse { .. }
            | State::Authenticated
            | State::Failed => Outcome::Ignored(IgnoreReason::UnknownTicket),
        }
    }

    fn on_from_core(&mut self, now: Now, sealed: Sealed<CoreToConn>) -> Outcome {
        let (edge, seq, msg) = sealed.open();
        if self.from_core.accept(edge, seq).is_err() {
            return Outcome::Ignored(IgnoreReason::StaleTicket);
        }
        match msg {
            CoreToConn::NonceVerdict { granted } => {
                let State::AwaitingNonceClaim {
                    challenge,
                    initiator,
                } = &self.state
                else {
                    return Outcome::Ignored(IgnoreReason::UnknownTicket);
                };
                let (challenge, initiator) = (*challenge, *initiator);

                if !granted {
                    return self.reject(now, RejectionReason::ReplayedNonce);
                }
                let response = Response::for_challenge(&challenge, now.wall);
                let preimage = signed_preimage(&self.config.local_peer, &response);
                let ticket = self.issue_ticket();
                self.state = State::AwaitingResponseSign {
                    ticket,
                    preimage: preimage.clone(),
                    initiator,
                };
                self.effects.push_back(ConnEffect::Sign {
                    ticket,
                    payload: preimage,
                });
                Outcome::Progressed
            }
        }
    }

    fn on_send_extension(&mut self, bytes: Vec<u8>) -> Outcome {
        if !matches!(self.state, State::Authenticated) {
            return Outcome::Ignored(IgnoreReason::NotAuthenticated(self.edge.conn));
        }
        self.send_bytes(bytes);
        Outcome::Progressed
    }

    fn on_transport_closed(&mut self) -> Outcome {
        self.send_to_core(ConnToCore::Closed { fault: None });
        self.deadline = None;
        self.state = State::Failed;
        Outcome::Progressed
    }

    // ── sync forwarding (post-auth) ────────────────────────────────

    /// Decode, verify inline, and forward one sync message.
    fn forward_sync(&mut self, frame: FrameId, bytes: &[u8]) -> Outcome {
        let Ok((msg, spans)) = wire::SyncMessage::try_decode_indexed(bytes) else {
            return self.fault(Fault::MalformedMessage);
        };
        let mut spans = spans.into_iter();

        match msg {
            wire::SyncMessage::BatchSyncRequest(request) => {
                self.forward(SyncForward::Request(request));
                Outcome::Progressed
            }
            wire::SyncMessage::LooseCommit {
                id,
                commit,
                blob,
                sender_heads,
            } => {
                let Some(span) = spans.next() else {
                    return self.fault(Fault::MalformedMessage);
                };
                match self.verify_commit(id, commit, &blob, frame, span) {
                    Some(item) => {
                        self.minted_from_frame = true;
                        self.forward(SyncForward::Commit {
                            tree: id,
                            item,
                            sender_heads,
                        });
                    }
                    // Item rejected: still deliver the heads rider —
                    // heads are independent of item validity.
                    None => self.forward(SyncForward::HeadsUpdate {
                        tree: id,
                        heads: sender_heads,
                    }),
                }
                Outcome::Progressed
            }
            wire::SyncMessage::Fragment {
                id,
                fragment,
                blob,
                sender_heads,
            } => {
                let Some(span) = spans.next() else {
                    return self.fault(Fault::MalformedMessage);
                };
                match self.verify_fragment(id, fragment, &blob, frame, span) {
                    Some(item) => {
                        self.minted_from_frame = true;
                        self.forward(SyncForward::Fragment {
                            tree: id,
                            item,
                            sender_heads,
                        });
                    }
                    None => self.forward(SyncForward::HeadsUpdate {
                        tree: id,
                        heads: sender_heads,
                    }),
                }
                Outcome::Progressed
            }
            wire::SyncMessage::BatchSyncResponse(response) => {
                self.forward_response(frame, response, &mut spans)
            }
            wire::SyncMessage::HeadsUpdate { id, heads } => {
                self.forward(SyncForward::HeadsUpdate { tree: id, heads });
                Outcome::Progressed
            }
            wire::SyncMessage::RemoveSubscriptions(unsub) => {
                self.forward(SyncForward::RemoveSubscriptions(unsub.ids));
                Outcome::Progressed
            }
            wire::SyncMessage::DataRequestRejected(rejected) => {
                self.forward(SyncForward::DataRequestRejected(rejected.id));
                Outcome::Progressed
            }
        }
    }

    fn forward_response(
        &mut self,
        frame: FrameId,
        response: wire::BatchSyncResponse,
        spans: &mut impl Iterator<Item = (u32, u32)>,
    ) -> Outcome {
        let (status, diff) = match response.result {
            wire::SyncResult::Ok(diff) => (ForwardStatus::Ok, Some(diff)),
            wire::SyncResult::NotFound => (ForwardStatus::NotFound, None),
            wire::SyncResult::Unauthorized => (ForwardStatus::Unauthorized, None),
        };

        let (mut commits, mut fragments, mut requesting) = (Vec::new(), Vec::new(), None);
        let rejected_before = self.rejected_items;
        if let Some(diff) = diff {
            for (signed, blob) in diff.missing_commits {
                let Some(span) = spans.next() else {
                    return self.fault(Fault::MalformedMessage);
                };
                if let Some(item) = self.verify_commit(response.id, signed, &blob, frame, span) {
                    self.minted_from_frame = true;
                    commits.push(item);
                }
            }
            for (signed, blob) in diff.missing_fragments {
                let Some(span) = spans.next() else {
                    return self.fault(Fault::MalformedMessage);
                };
                if let Some(item) = self.verify_fragment(response.id, signed, &blob, frame, span) {
                    self.minted_from_frame = true;
                    fragments.push(item);
                }
            }
            requesting = Some(diff.requesting);
        }

        #[allow(clippy::cast_possible_truncation)] // bounded by u16 wire counts
        let rejected = (self.rejected_items - rejected_before) as u32;
        self.forward(SyncForward::Response {
            req_id: response.req_id,
            tree: response.id,
            commits,
            fragments,
            requesting: requesting.unwrap_or_default(),
            responder_heads: response.responder_heads,
            status,
            rejected,
        });
        Outcome::Progressed
    }

    /// Inline item verification: signature, tree binding,
    /// and blob digest — the forgery gate, in machine code.
    fn verify_commit(
        &mut self,
        tree: SedimentreeId,
        signed: Signed<LooseCommit>,
        blob: &Blob,
        frame: FrameId,
        span: (u32, u32),
    ) -> Option<VerifiedCommit> {
        let Ok(verified) = signed.try_verify() else {
            self.rejected_items = self.rejected_items.saturating_add(1);
            return None;
        };
        let payload = verified.payload();
        if payload.sedimentree_id() != tree || BlobMeta::new(blob) != *payload.blob_meta() {
            self.rejected_items = self.rejected_items.saturating_add(1);
            return None;
        }
        let (offset, len) = span;
        Some(VerifiedCommit {
            commit: signed,
            blob: BlobRef { frame, offset, len },
        })
    }

    /// Fragment twin of [`Self::verify_commit`].
    fn verify_fragment(
        &mut self,
        tree: SedimentreeId,
        signed: Signed<TreeFragment>,
        blob: &Blob,
        frame: FrameId,
        span: (u32, u32),
    ) -> Option<VerifiedFragment> {
        let Ok(verified) = signed.try_verify() else {
            self.rejected_items = self.rejected_items.saturating_add(1);
            return None;
        };
        let payload = verified.payload();
        if payload.sedimentree_id() != tree || BlobMeta::new(blob) != payload.summary().blob_meta()
        {
            self.rejected_items = self.rejected_items.saturating_add(1);
            return None;
        }
        let (offset, len) = span;
        Some(VerifiedFragment {
            fragment: signed,
            blob: BlobRef { frame, offset, len },
        })
    }

    fn forward(&mut self, forward: SyncForward) {
        self.send_to_core(ConnToCore::Inbound(alloc::boxed::Box::new(forward)));
    }

    /// Items dropped by verification so far (tier-2 telemetry).
    #[must_use]
    pub const fn rejected_items(&self) -> u64 {
        self.rejected_items
    }

    // ── helpers ────────────────────────────────────────────────────

    fn authenticate(&mut self, peer: PeerId) -> Outcome {
        self.state = State::Authenticated;
        self.peer = Some(peer);
        self.deadline = None;
        self.send_to_core(ConnToCore::Authenticated { peer });
        self.effects
            .push_back(ConnEffect::App(ConnAppEvent::PeerAuthenticated { peer }));
        Outcome::Progressed
    }

    /// Send an unsigned rejection, notify the core, and become terminal.
    fn reject(&mut self, now: Now, reason: RejectionReason) -> Outcome {
        let msg = HandshakeMessage::Rejection(Rejection::new(reason, now.wall));
        self.send_bytes(msg.encode());
        self.fault(Fault::ChallengeRejected(reason))
    }

    /// Condemn this connection: disconnect, tell the core, go terminal.
    fn fault(&mut self, fault: Fault) -> Outcome {
        self.state = State::Failed;
        self.deadline = None;
        self.effects.push_back(ConnEffect::Disconnect);
        self.send_to_core(ConnToCore::Closed { fault: Some(fault) });
        Outcome::ConnectionFault {
            conn: self.edge.conn,
            fault,
        }
    }

    fn send_bytes(&mut self, bytes: Vec<u8>) {
        self.effects.push_back(ConnEffect::Send {
            parts: vec![crate::blob_ref::Part::Bytes(bytes)],
        });
    }

    fn send_to_core(&mut self, msg: ConnToCore) {
        let sealed = Sealed::mint(self.edge, self.out_seq, msg);
        self.out_seq = self.out_seq.next();
        self.effects.push_back(ConnEffect::ToCore(sealed));
    }

    const fn issue_ticket(&mut self) -> CryptoTicket {
        let ticket = CryptoTicket {
            entity: Entity::Connection(self.edge.conn),
            generation: self.edge.generation,
            seq: self.ticket_seq,
        };
        self.ticket_seq = self.ticket_seq.next();
        ticket
    }

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

/// The handshake typestate for one connection incarnation.
#[derive(Debug)]
enum State {
    /// Inbound: waiting for the initiator's challenge.
    AwaitingChallenge,

    /// Outbound: our challenge is being signed.
    AwaitingChallengeSign {
        ticket: CryptoTicket,
        challenge: Challenge,
        preimage: Vec<u8>,
    },

    /// Outbound: challenge sent; awaiting a response (or a crossed
    /// challenge — simultaneous open).
    AwaitingResponse {
        challenge: Challenge,
        signed_bytes: Vec<u8>,
    },

    /// Responder: challenge verified; the core is arbitrating the nonce.
    AwaitingNonceClaim {
        challenge: Challenge,
        initiator: PeerId,
    },

    /// Responder (and sim-open winner): our response is being signed.
    AwaitingResponseSign {
        ticket: CryptoTicket,
        preimage: Vec<u8>,
        initiator: PeerId,
    },

    /// Sim-open loser: our response to their challenge is being signed.
    SimOpenLoserSign {
        ticket: CryptoTicket,
        preimage: Vec<u8>,
        our_challenge: Challenge,
        expected: PeerId,
    },

    /// Sim-open: awaiting their response to our challenge.
    SimOpenAwaitTheirResponse {
        our_challenge: Challenge,
        owed: Option<Challenge>,
        expected: PeerId,
    },

    /// Handshake complete; this machine is now a verify-and-forward gate.
    Authenticated,

    /// Terminal: condemned or transport-closed.
    Failed,
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer as _, SigningKey};

    const fn now() -> Now {
        Now {
            monotonic: Timestamp::from_millis(0),
            wall: crate::wall_clock::TimestampSeconds::new(1_700_000_000),
        }
    }

    /// A test peer: `ConnMachine` + signer + a hand-rolled core stub that
    /// grants nonces (tracking claims for replay checks).
    struct Peer {
        machine: ConnMachine,
        key: SigningKey,
        /// Wire messages awaiting delivery to the other side.
        outbox: Vec<Vec<u8>>,
        /// App events observed.
        app: Vec<ConnAppEvent>,
        /// Claims seen by the stub core (for replay simulation).
        claims: Vec<(PeerId, Nonce)>,
        /// Frames the machine said were releasable.
        released: Vec<crate::blob_ref::FrameId>,
        /// Non-claim edge traffic observed (sync forwards etc.).
        to_core: Vec<ConnToCore>,
        /// Sequence for minting core→conn verdicts.
        core_seq: Seq,
        disconnected: bool,
    }

    impl Peer {
        fn new(seed: u8, direction: Direction, audience: Option<Audience>) -> Self {
            let key = SigningKey::from_bytes(&[seed; 32]);
            let config = ConnConfig::new(PeerId::from(key.verifying_key()), [seed ^ 0xFF; 32]);
            let machine = ConnMachine::new(
                config,
                ConnId::new(1),
                Generation::FIRST,
                direction,
                audience,
                now(),
            );
            let mut peer = Self {
                machine,
                key,
                outbox: Vec::new(),
                app: Vec::new(),
                claims: Vec::new(),
                released: Vec::new(),
                to_core: Vec::new(),
                core_seq: Seq::FIRST,
                disconnected: false,
            };
            peer.run_effects();
            peer
        }

        fn id(&self) -> PeerId {
            PeerId::from(self.key.verifying_key())
        }

        fn feed(&mut self, event: ConnEvent) -> Outcome {
            let outcome = self.machine.handle(now(), event);
            self.run_effects();
            outcome
        }

        /// Execute effects: sign locally, grant nonce claims (stub core),
        /// collect sends and app events.
        fn run_effects(&mut self) {
            while let Some(effect) = self.machine.poll_effect() {
                match effect {
                    ConnEffect::Send { parts } => {
                        let mut bytes = Vec::new();
                        for part in parts {
                            match part {
                                crate::blob_ref::Part::Bytes(b) => bytes.extend_from_slice(&b),
                                // No blob refs occur in handshake traffic.
                                crate::blob_ref::Part::Ref(_) => {}
                            }
                        }
                        self.outbox.push(bytes);
                    }
                    ConnEffect::Disconnect => self.disconnected = true,
                    ConnEffect::ReleaseFrame(frame) => self.released.push(frame),
                    ConnEffect::Sign { ticket, payload } => {
                        let signature = self.key.sign(&payload).to_bytes();
                        let _outcome = self
                            .machine
                            .handle(now(), ConnEvent::SignDone { ticket, signature });
                    }
                    ConnEffect::ToCore(sealed) => {
                        let (_edge, _seq, msg) = sealed.open();
                        if let ConnToCore::ClaimNonce { peer, nonce, .. } = msg {
                            let fresh = !self.claims.contains(&(peer, nonce));
                            self.claims.push((peer, nonce));
                            let verdict = Sealed::mint(
                                self.machine.edge(),
                                self.core_seq,
                                CoreToConn::NonceVerdict { granted: fresh },
                            );
                            self.core_seq = self.core_seq.next();
                            let _outcome = self.machine.handle(now(), ConnEvent::FromCore(verdict));
                        } else {
                            self.to_core.push(msg);
                        }
                    }
                    ConnEffect::App(event) => self.app.push(event),
                }
            }
        }

        fn authenticated_with(&self) -> Option<PeerId> {
            self.app.iter().find_map(|event| match event {
                ConnAppEvent::PeerAuthenticated { peer } => Some(*peer),
                ConnAppEvent::ExtensionMessage { .. } => None,
            })
        }
    }

    fn pump(a: &mut Peer, b: &mut Peer) {
        for _ in 0..16 {
            let a_out: Vec<_> = a.outbox.drain(..).collect();
            let b_out: Vec<_> = b.outbox.drain(..).collect();
            if a_out.is_empty() && b_out.is_empty() {
                return;
            }
            for bytes in a_out {
                let _outcome = b.feed(ConnEvent::MessageReceived {
                    frame: crate::blob_ref::FrameId::new(0),
                    bytes,
                });
            }
            for bytes in b_out {
                let _outcome = a.feed(ConnEvent::MessageReceived {
                    frame: crate::blob_ref::FrameId::new(0),
                    bytes,
                });
            }
        }
    }

    #[test]
    fn handshake_completes_via_edge_nonce_claim() {
        let mut bob = Peer::new(2, Direction::Inbound, None);
        let mut alice = Peer::new(1, Direction::Outbound, Some(Audience::known(bob.id())));

        pump(&mut alice, &mut bob);

        assert_eq!(alice.authenticated_with(), Some(bob.id()));
        assert_eq!(bob.authenticated_with(), Some(alice.id()));
        assert_eq!(bob.claims.len(), 1, "one nonce claim went to the core");
        assert_eq!(alice.machine.poll_timeout(), None, "deadline disarmed");
    }

    #[test]
    fn denied_nonce_rejects_the_handshake() -> testresult::TestResult {
        let mut bob = Peer::new(4, Direction::Inbound, None);
        let mut alice = Peer::new(3, Direction::Outbound, Some(Audience::known(bob.id())));

        // Pre-poison the stub core so the claim is treated as a replay.
        let challenge = alice.outbox.remove(0);
        // Extract alice's (peer, nonce) by decoding her challenge.
        let HandshakeMessage::SignedChallenge(signed) = HandshakeMessage::try_decode(&challenge)?
        else {
            return Err("expected challenge".into());
        };
        let verified = signed.try_verify().map_err(|_| "challenge must verify")?;
        bob.claims
            .push((PeerId::from(verified.issuer()), verified.payload().nonce));

        let outcome = bob.feed(ConnEvent::MessageReceived {
            frame: crate::blob_ref::FrameId::new(0),
            bytes: challenge,
        });
        assert!(matches!(
            outcome,
            Outcome::Progressed // claim round-trip resolves inside run_effects
        ));
        assert!(bob.disconnected, "replay ⇒ rejection ⇒ disconnect");

        // Alice receives the rejection.
        let rejection = bob.outbox.remove(0);
        let outcome = alice.feed(ConnEvent::MessageReceived {
            frame: crate::blob_ref::FrameId::new(0),
            bytes: rejection,
        });
        assert!(matches!(
            outcome,
            Outcome::ConnectionFault {
                fault: Fault::HandshakeRejected(RejectionReason::ReplayedNonce),
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn simultaneous_open_completes() {
        let alice_key = SigningKey::from_bytes(&[5u8; 32]);
        let bob_key = SigningKey::from_bytes(&[6u8; 32]);
        let alice_id = PeerId::from(alice_key.verifying_key());
        let bob_id = PeerId::from(bob_key.verifying_key());

        let mut alice = Peer::new(5, Direction::Outbound, Some(Audience::known(bob_id)));
        let mut bob = Peer::new(6, Direction::Outbound, Some(Audience::known(alice_id)));

        pump(&mut alice, &mut bob);

        assert_eq!(alice.authenticated_with(), Some(bob_id));
        assert_eq!(bob.authenticated_with(), Some(alice_id));
    }

    #[test]
    fn extension_gating_pre_and_post_auth() {
        let mut bob = Peer::new(8, Direction::Inbound, None);
        let alice = Peer::new(7, Direction::Outbound, Some(Audience::known(bob.id())));

        // Pre-auth extension bytes: condemned.
        let outcome = bob.feed(ConnEvent::MessageReceived {
            frame: crate::blob_ref::FrameId::new(0),
            bytes: b"SUE\x00pre-auth".to_vec(),
        });
        assert!(matches!(outcome, Outcome::ConnectionFault { .. }));

        drop(alice);

        // Fresh pair completes; post-auth extension bytes surface.
        let mut bob = Peer::new(10, Direction::Inbound, None);
        let mut alice = Peer::new(9, Direction::Outbound, Some(Audience::known(bob.id())));
        pump(&mut alice, &mut bob);
        assert!(bob.authenticated_with().is_some());

        let outcome = bob.feed(ConnEvent::MessageReceived {
            frame: crate::blob_ref::FrameId::new(0),
            bytes: b"SUE\x00hello".to_vec(),
        });
        assert_eq!(outcome, Outcome::Progressed);
        assert!(bob.app.iter().any(|event| matches!(
            event,
            ConnAppEvent::ExtensionMessage { bytes, .. } if bytes == b"SUE\x00hello"
        )));
    }

    #[test]
    fn handshake_deadline_fires() {
        let bob_id = PeerId::from(SigningKey::from_bytes(&[12u8; 32]).verifying_key());
        let mut alice = Peer::new(11, Direction::Outbound, Some(Audience::known(bob_id)));
        assert!(alice.machine.poll_timeout().is_some());

        let late = Now {
            monotonic: Timestamp::from_millis(31_000),
            wall: crate::wall_clock::TimestampSeconds::new(1_700_000_031),
        };
        let _outcome = alice.machine.handle(late, ConnEvent::Wake);
        alice.run_effects();
        assert!(alice.disconnected, "deadline ⇒ disconnect");
        assert_eq!(alice.machine.poll_timeout(), None);
    }

    // ── sync forwarding ────────────────────────────────────────

    fn authed_pair(seed_a: u8, seed_b: u8) -> (Peer, Peer) {
        let mut bob = Peer::new(seed_b, Direction::Inbound, None);
        let mut alice = Peer::new(seed_a, Direction::Outbound, Some(Audience::known(bob.id())));
        pump(&mut alice, &mut bob);
        assert!(bob.authenticated_with().is_some());
        // Handshake frames are (correctly) released; start clean so the
        // sync-path assertions see only their own frames.
        for peer in [&mut alice, &mut bob] {
            peer.released.clear();
            peer.to_core.clear();
        }
        (alice, bob)
    }

    fn signed_commit_msg(
        signer_seed: u8,
        tree: SedimentreeId,
        head: u8,
        corrupt_blob: bool,
    ) -> alloc::vec::Vec<u8> {
        use sedimentree_core::loose_commit::id::CommitId;
        let signing_key = SigningKey::from_bytes(&[signer_seed; 32]);
        let blob = Blob::new(alloc::vec![head; 16]);
        let commit = LooseCommit::new(
            tree,
            CommitId::new([head; 32]),
            alloc::collections::BTreeSet::new(),
            BlobMeta::new(&blob),
        );
        let sealed = Signed::seal_sync(&signing_key, commit).into_signed();
        let wire_blob = if corrupt_blob {
            Blob::new(alloc::vec![head ^ 1; 16])
        } else {
            blob
        };
        wire::SyncMessage::LooseCommit {
            id: tree,
            commit: sealed,
            blob: wire_blob,
            sender_heads: crate::remote_heads::RemoteHeads {
                counter: 1,
                heads: alloc::vec![CommitId::new([head; 32])],
            },
        }
        .encode()
    }

    #[test]
    fn verified_commit_is_forwarded_with_frame_ref() -> testresult::TestResult {
        let (_alice, mut bob) = authed_pair(21, 22);
        let tree = SedimentreeId::new([7u8; 32]);
        let bytes = signed_commit_msg(21, tree, 0xA1, false);
        let frame = crate::blob_ref::FrameId::new(77);

        let outcome = bob.feed(ConnEvent::MessageReceived {
            frame,
            bytes: bytes.clone(),
        });
        assert_eq!(outcome, Outcome::Progressed);
        assert!(
            bob.released.is_empty(),
            "refs escaped: frame stays retained"
        );

        let Some(ConnToCore::Inbound(forward)) = bob.to_core.pop() else {
            return Err("expected a forward".into());
        };
        let SyncForward::Commit { tree: t, item, .. } = *forward else {
            return Err("expected a commit forward".into());
        };
        assert_eq!(t, tree);
        assert_eq!(item.blob.frame, frame);
        // The ref slices the exact blob bytes out of the frame.
        let (off, len) = (item.blob.offset as usize, item.blob.len as usize);
        assert_eq!(bytes.get(off..off + len), Some(&[0xA1u8; 16][..]));
        assert_eq!(bob.machine.rejected_items(), 0);
        Ok(())
    }

    #[test]
    fn corrupt_blob_is_rejected_but_heads_still_flow() -> testresult::TestResult {
        let (_alice, mut bob) = authed_pair(23, 24);
        let tree = SedimentreeId::new([7u8; 32]);
        let bytes = signed_commit_msg(23, tree, 0xB2, true); // blob ≠ signed meta
        let frame = crate::blob_ref::FrameId::new(78);

        let outcome = bob.feed(ConnEvent::MessageReceived { frame, bytes });
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(bob.machine.rejected_items(), 1, "forgery gate fired");
        assert_eq!(bob.released, [frame], "no refs escaped: frame releasable");

        let Some(ConnToCore::Inbound(forward)) = bob.to_core.pop() else {
            return Err("expected a forward".into());
        };
        assert!(
            matches!(*forward, SyncForward::HeadsUpdate { tree: t, .. } if t == tree),
            "heads rider still delivered on item rejection"
        );
        Ok(())
    }

    #[test]
    fn non_blob_messages_release_their_frames() {
        let (_alice, mut bob) = authed_pair(25, 26);
        let frame = crate::blob_ref::FrameId::new(79);
        let msg = wire::SyncMessage::HeadsUpdate {
            id: SedimentreeId::new([7u8; 32]),
            heads: crate::remote_heads::RemoteHeads::default(),
        };
        let _outcome = bob.feed(ConnEvent::MessageReceived {
            frame,
            bytes: msg.encode(),
        });
        assert_eq!(bob.released, [frame]);
        assert!(matches!(
            bob.to_core.pop(),
            Some(ConnToCore::Inbound(forward))
                if matches!(*forward, SyncForward::HeadsUpdate { .. })
        ));
    }

    #[test]
    fn handshake_frames_are_released() {
        // Inbound peer: the challenge frame itself must be releasable.
        let mut bob = Peer::new(28, Direction::Inbound, None);
        let alice = Peer::new(27, Direction::Outbound, Some(Audience::known(bob.id())));
        let mut alice = alice;
        pump(&mut alice, &mut bob);
        assert!(
            !bob.released.is_empty(),
            "handshake frames mint no refs and are released"
        );
    }

    #[test]
    fn stale_generation_verdict_is_ignored() {
        let mut bob = Peer::new(14, Direction::Inbound, None);
        let alice_key = SigningKey::from_bytes(&[13u8; 32]);
        let _alice_id = PeerId::from(alice_key.verifying_key());

        // Hand-mint a verdict from a WRONG generation: must be dropped.
        let wrong_edge = EdgeId {
            conn: ConnId::new(1),
            generation: Generation::FIRST.next(),
        };
        let verdict = Sealed::mint(
            wrong_edge,
            Seq::FIRST,
            CoreToConn::NonceVerdict { granted: true },
        );
        let outcome = bob.feed(ConnEvent::FromCore(verdict));
        assert_eq!(outcome, Outcome::Ignored(IgnoreReason::StaleTicket));
    }
}
