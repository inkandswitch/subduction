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

mod sim_open;
mod sync;

use alloc::{collections::VecDeque, vec::Vec};
use core::time::Duration;

use sedimentree_core::{
    blob::Blob,
    codec::{encode::EncodeFields, schema::Schema},
    collections::{Map, Set},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::minimized::MinimizedSedimentree,
};
use subduction_crypto::{nonce::Nonce, signed::Signed};

use crate::{
    command::Command,
    effect::{AppEvent, CryptoOp, CryptoResult, Effect},
    event::{Direction, Event},
    handshake::{
        HANDSHAKE_SCHEMA, HandshakeMessage, MAX_PLAUSIBLE_DRIFT, pinned_peer, signed_preimage,
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
    storage::{Provenance, StorageFailure, StorageOp, StorageResult},
    timestamp::Timestamp,
    ticket::{CryptoTicket, Entity, StorageTicket},
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

    /// How long a batch sync request may await its response.
    pub sync_timeout: Duration,

    /// Seed for the nonce generator. Must be unpredictable (CSPRNG) and
    /// unique per machine instance.
    pub entropy: [u8; 32],
}

impl Config {
    /// Default handshake deadline.
    pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Default batch sync deadline (legacy `DEFAULT_ROUNDTRIP_TIMEOUT`).
    pub const DEFAULT_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a config with defaults for everything but identity/entropy.
    #[must_use]
    pub const fn new(local_peer: PeerId, entropy: [u8; 32]) -> Self {
        Self {
            local_peer,
            discovery: None,
            max_drift: MAX_PLAUSIBLE_DRIFT,
            handshake_timeout: Self::DEFAULT_HANDSHAKE_TIMEOUT,
            sync_timeout: Self::DEFAULT_SYNC_TIMEOUT,
            entropy,
        }
    }
}

/// The sans-io protocol state machine. See the [module docs](self).
#[derive(Debug)]
pub struct Machine {
    config: Config,
    conns: Map<ConnId, ConnEntry>,
    /// Resident sedimentree metadata (hydrated by the driver; blobs stay
    /// in storage — ADR-012 memory model). Minimization is lazy, using
    /// [`CountLeadingZeroBytes`] (the legacy default; pluggable metrics
    /// can return if a platform ever needs one).
    trees: Map<SedimentreeId, MinimizedSedimentree>,
    /// Pending [`Entity::Local`] storage ops, keyed by ticket seq.
    local_pending: Map<Seq, LocalPending>,
    /// Which connections want pushes for which trees (both directions of
    /// legacy's subscriptions map; entries die with their connection).
    subscriptions: Map<SedimentreeId, Set<ConnId>>,
    /// Per-peer monotonic counters for heads we have *received* (staleness
    /// filter) and *sent* (so receivers can filter ours).
    heads_recv: Map<PeerId, u64>,
    heads_sent: Map<PeerId, u64>,
    /// Monotonic nonce for outbound `RequestId`s.
    request_nonce: u64,
    local_generation: Generation,
    local_next_seq: Seq,
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
            trees: Map::new(),
            local_pending: Map::new(),
            subscriptions: Map::new(),
            heads_recv: Map::new(),
            heads_sent: Map::new(),
            request_nonce: 0,
            local_generation: Generation::FIRST,
            local_next_seq: Seq::FIRST,
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
            Event::CryptoDone { ticket, result } => self.on_crypto_done(now, ticket, result),
            Event::StorageDone { ticket, result } => self.on_storage_done(now, ticket, result),
            Event::Command(command) => self.on_command(now, command),
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
        let handshakes = self.conns.values().filter_map(|c| c.deadline);
        let requests = self
            .conns
            .values()
            .flat_map(|c| c.requests.values().map(|r| r.deadline));
        handshakes.chain(requests).min()
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
                        pending: Map::new(),
                        requests: Map::new(),
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

                let challenge = Challenge::new(audience, now.wall, self.next_nonce());
                let preimage = signed_preimage(&self.config.local_peer, &challenge);

                let mut entry = ConnEntry {
                    generation: Generation::FIRST,
                    next_seq: Seq::FIRST,
                    deadline,
                    pending: Map::new(),
                    requests: Map::new(),
                    peer: None,
                    state: HandshakeState::AwaitingChallenge, // placeholder
                };
                let ticket = entry.issue_ticket(conn);
                entry.state = HandshakeState::AwaitingChallengeSign {
                    ticket,
                    challenge,
                    preimage: preimage.clone(),
                };
                self.conns.insert(conn, entry);

                self.effects.push_back(Effect::Crypto {
                    ticket,
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

        // Subscriptions die with their connection.
        self.subscriptions.retain(|_tree, conns| {
            conns.remove(&conn);
            !conns.is_empty()
        });

        // A handshake that never completed and wasn't already condemned
        // (Closing counts at fault time) is a failure.
        match entry.state {
            HandshakeState::AwaitingChallenge
            | HandshakeState::AwaitingChallengeSign { .. }
            | HandshakeState::AwaitingResponse { .. }
            | HandshakeState::AwaitingResponseSign { .. }
            | HandshakeState::SimOpenLoserSign { .. }
            | HandshakeState::SimOpenAwaitTheirResponse { .. } => {
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
                    s if s == wire::MESSAGE_SCHEMA => {
                        let Some(peer) = entry.peer else {
                            return self.fault(conn, Fault::UnexpectedMessage);
                        };
                        let Ok(msg) = wire::SyncMessage::try_decode(bytes) else {
                            self.stats.malformed_messages =
                                self.stats.malformed_messages.saturating_add(1);
                            return self.fault(conn, Fault::MalformedMessage);
                        };
                        self.on_sync_message(now, conn, peer, msg)
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
                    // Simultaneous open: both sides dialed.
                    HandshakeMessage::SignedChallenge(signed) => {
                        self.on_sim_open_challenge(now, conn, bytes, &signed)
                    }
                }
            }

            HandshakeState::SimOpenAwaitTheirResponse { .. } => {
                self.on_sim_open_message(now, conn, bytes)
            }

            // No message is legal while we hold the turn's crypto pending.
            HandshakeState::AwaitingChallengeSign { .. }
            | HandshakeState::AwaitingResponseSign { .. }
            | HandshakeState::SimOpenLoserSign { .. } => {
                self.fault(conn, Fault::UnexpectedMessage)
            }
        }
    }

    /// Responder: a challenge arrived. Verification is inline (ADR-014):
    /// signature first (as legacy — spoofed issuers must not trigger
    /// cheap rejections), then audience/freshness, then the nonce claim,
    /// then our response goes to the driver for signing.
    fn on_inbound_challenge(
        &mut self,
        now: Now,
        conn: ConnId,
        signed: &Signed<Challenge>,
    ) -> Outcome {
        let Ok(verified) = signed.try_verify() else {
            return self.reject_challenge(now, conn, RejectionReason::InvalidSignature);
        };
        let challenge = *verified.payload();
        let initiator = PeerId::from(verified.issuer());

        if let Err(reason) = self.validate_challenge(now, &challenge) {
            return self.reject_challenge(now, conn, reason);
        }

        // Claim the nonce only after signature verification (cache-filling
        // DoS prevention, as legacy).
        if self
            .nonce_cache
            .try_claim(initiator, challenge.nonce, now.wall)
            .is_err()
        {
            return self.reject_challenge(now, conn, RejectionReason::ReplayedNonce);
        }

        let response = Response::for_challenge(&challenge, now.wall);
        let preimage = signed_preimage(&self.config.local_peer, &response);
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let ticket = entry.issue_ticket(conn);
        entry.state = HandshakeState::AwaitingResponseSign {
            ticket,
            preimage: preimage.clone(),
            initiator,
        };
        self.effects.push_back(Effect::Crypto {
            ticket,
            op: CryptoOp::Sign { payload: preimage },
        });
        Outcome::Progressed
    }

    /// Initiator: a response arrived. Verification, digest binding, and
    /// the pin check all run inline (ADR-014) — authentication completes
    /// in this turn.
    fn on_outbound_response(&mut self, conn: ConnId, signed: &Signed<Response>) -> Outcome {
        let Some(entry) = self.conns.get(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let HandshakeState::AwaitingResponse { challenge, .. } = &entry.state else {
            return self.fault(conn, Fault::UnexpectedMessage);
        };
        let pinned = pinned_peer(challenge);
        let challenge = *challenge;

        let Ok(verified) = signed.try_verify() else {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        };
        if verified.payload().validate(&challenge).is_err() {
            return self.fault(conn, Fault::HandshakeVerificationFailed);
        }

        let responder = PeerId::from(verified.issuer());
        if let Some(pinned) = pinned
            && pinned != responder
        {
            return self.fault(conn, Fault::PeerMismatch);
        }
        self.authenticate(conn, responder)
    }

    fn on_command(&mut self, now: Now, command: Command) -> Outcome {
        match command {
            Command::HydrateTree {
                tree,
                commits,
                fragments,
            } => {
                let entry = self.trees.entry(tree).or_default();
                for commit in commits {
                    let _fresh = entry.add_commit(commit);
                }
                for fragment in fragments {
                    let _fresh = entry.add_fragment(fragment);
                }
                Outcome::Progressed
            }

            Command::AddCommits { tree, commits } => {
                self.ingest_local(tree, commits, Vec::new())
            }

            Command::AddFragments { tree, fragments } => {
                self.ingest_local(tree, Vec::new(), fragments)
            }

            Command::Unsubscribe { conn, trees } => {
                let authenticated = self
                    .conns
                    .get(&conn)
                    .is_some_and(|entry| matches!(entry.state, HandshakeState::Authenticated));
                if !authenticated {
                    return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
                }
                let msg = wire::SyncMessage::RemoveSubscriptions(wire::RemoveSubscriptions {
                    ids: trees,
                });
                self.effects.push_back(Effect::SendMessage {
                    conn,
                    bytes: msg.encode(),
                });
                Outcome::Progressed
            }

            Command::RemoveTree { tree } => {
                let _resident = self.trees.remove(&tree);
                let ticket = self.issue_local_ticket();
                self.local_pending
                    .insert(ticket.seq, LocalPending::Delete { tree });
                self.effects.push_back(Effect::Storage {
                    ticket,
                    op: StorageOp::DeleteTree {
                        tree,
                        provenance: Provenance::Local,
                    },
                });
                Outcome::Progressed
            }

            Command::SyncTree {
                conn,
                tree,
                subscribe,
            } => self.start_sync(now, conn, tree, subscribe),

            Command::SendExtension { conn, bytes } => {
                let authenticated = self
                    .conns
                    .get(&conn)
                    .is_some_and(|entry| matches!(entry.state, HandshakeState::Authenticated));
                if authenticated {
                    self.effects.push_back(Effect::SendMessage { conn, bytes });
                    Outcome::Progressed
                } else {
                    Outcome::Ignored(IgnoreReason::NotAuthenticated(conn))
                }
            }
        }
    }

    fn on_storage_done(
        &mut self,
        _now: Now,
        ticket: StorageTicket,
        result: StorageResult,
    ) -> Outcome {
        match ticket.entity {
            Entity::Local => self.on_local_storage_done(ticket, result),
            Entity::Connection(conn) => {
                let Some(entry) = self.conns.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                if ticket.generation != entry.generation {
                    self.stats.stale_completions =
                        self.stats.stale_completions.saturating_add(1);
                    return Outcome::Ignored(IgnoreReason::StaleTicket);
                }
                let Some(pending) = entry.pending.remove(&ticket.seq) else {
                    self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
                    return Outcome::Ignored(IgnoreReason::UnknownTicket);
                };
                self.on_sync_storage_done(conn, pending, result)
            }
        }
    }

    fn on_local_storage_done(&mut self, ticket: StorageTicket, result: StorageResult) -> Outcome {
        if ticket.generation != self.local_generation {
            self.stats.stale_completions = self.stats.stale_completions.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::StaleTicket);
        }
        let Some(pending) = self.local_pending.remove(&ticket.seq) else {
            self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::UnknownTicket);
        };

        match (pending, result) {
            (
                LocalPending::Ingest {
                    tree,
                    commit_blobs,
                    fragment_blobs,
                },
                StorageResult::LocallyIngested { commits, fragments },
            ) => {
                // The tree may have been removed while the write was in
                // flight — the durable data goes with it, so drop quietly.
                if !self.trees.contains_key(&tree) && self.local_delete_pending(tree) {
                    return Outcome::Ignored(IgnoreReason::StaleTicket);
                }
                {
                    let entry = self.trees.entry(tree).or_default();
                    if !commits.is_empty() {
                        let mut heads = Vec::with_capacity(commits.len());
                        for signed in &commits {
                            if let Ok((commit, _)) = try_decode_payload::<LooseCommit>(signed) {
                                heads.push(commit.head());
                                let _fresh = entry.add_commit(commit);
                            }
                        }
                        self.effects
                            .push_back(Effect::App(AppEvent::CommitsStored { tree, heads }));
                    }
                    if !fragments.is_empty() {
                        let mut heads = Vec::with_capacity(fragments.len());
                        for signed in &fragments {
                            if let Ok((fragment, _)) = try_decode_payload::<Fragment>(signed) {
                                heads.push(fragment.head());
                                let _fresh = entry.add_fragment(fragment);
                            }
                        }
                        self.effects
                            .push_back(Effect::App(AppEvent::FragmentsStored { tree, heads }));
                    }
                }

                // Push to subscribers (all of them: the author is us).
                let commit_items: Vec<(Signed<LooseCommit>, Blob)> =
                    commits.into_iter().zip(commit_blobs).collect();
                let fragment_items: Vec<(Signed<Fragment>, Blob)> =
                    fragments.into_iter().zip(fragment_blobs).collect();
                self.broadcast_items(tree, &commit_items, &fragment_items, None);
                Outcome::Progressed
            }

            (LocalPending::Delete { tree }, StorageResult::TreeDeleted) => {
                self.effects
                    .push_back(Effect::App(AppEvent::TreeRemoved { tree }));
                Outcome::Progressed
            }

            (
                LocalPending::Ingest { tree, .. } | LocalPending::Delete { tree },
                StorageResult::Failed(failure),
            ) => {
                self.effects
                    .push_back(Effect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }

            // Result shape mismatched the pending op — driver bug; the
            // pending entry is already consumed, so this is terminal for
            // the op but harmless to the machine.
            (
                LocalPending::Ingest { tree, .. } | LocalPending::Delete { tree },
                StorageResult::Ingested { .. }
                | StorageResult::Fetched { .. }
                | StorageResult::TreeDeleted
                | StorageResult::LocallyIngested { .. }
                | StorageResult::Unauthorized
                | StorageResult::UnknownTree,
            ) => {
                self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
                self.effects.push_back(Effect::App(AppEvent::StorageError {
                    tree,
                    failure: StorageFailure::Permanent,
                }));
                Outcome::Ignored(IgnoreReason::UnknownTicket)
            }
        }
    }

    /// Queue a fused local seal+persist for commits and/or fragments.
    fn ingest_local(
        &mut self,
        tree: SedimentreeId,
        commits: Vec<crate::command::NewCommit>,
        fragments: Vec<crate::command::NewFragment>,
    ) -> Outcome {
        let ticket = self.issue_local_ticket();
        let commit_blobs = commits.iter().map(|new| new.blob.clone()).collect();
        let fragment_blobs = fragments.iter().map(|new| new.blob.clone()).collect();
        self.local_pending.insert(
            ticket.seq,
            LocalPending::Ingest {
                tree,
                commit_blobs,
                fragment_blobs,
            },
        );
        self.effects.push_back(Effect::Storage {
            ticket,
            op: StorageOp::IngestLocal {
                tree,
                commits,
                fragments,
            },
        });
        Outcome::Progressed
    }

    /// Whether a delete for `tree` is still in flight.
    fn local_delete_pending(&self, tree: SedimentreeId) -> bool {
        self.local_pending
            .values()
            .any(|pending| matches!(pending, LocalPending::Delete { tree: t } if *t == tree))
    }

    const fn issue_local_ticket(&mut self) -> StorageTicket {
        let ticket = StorageTicket {
            entity: Entity::Local,
            generation: self.local_generation,
            seq: self.local_next_seq,
        };
        self.local_next_seq = self.local_next_seq.next();
        ticket
    }

    /// Read access for the application/tests: the resident trees.
    pub fn tree_ids(&self) -> impl Iterator<Item = SedimentreeId> {
        self.trees.keys().copied()
    }

    /// Read access for the application/tests: a resident tree's current
    /// heads (`None` if the tree is not resident). `&mut` because
    /// minimization is lazy.
    pub fn tree_heads(&mut self, tree: SedimentreeId) -> Option<Vec<CommitId>> {
        self.trees
            .get_mut(&tree)
            .map(|entry| entry.heads(&CountLeadingZeroBytes))
    }

    fn on_crypto_done(&mut self, now: Now, ticket: CryptoTicket, result: CryptoResult) -> Outcome {
        let Entity::Connection(conn) = ticket.entity else {
            // No Local-scoped crypto operations exist yet.
            self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::UnknownTicket);
        };

        let Some(entry) = self.conns.get(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };

        if ticket.generation != entry.generation {
            self.stats.stale_completions = self.stats.stale_completions.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::StaleTicket);
        }

        match &entry.state {
            HandshakeState::SimOpenLoserSign {
                ticket: expected,
                preimage,
                our_challenge,
                expected: expected_peer,
            } if *expected == ticket => {
                let (preimage, our_challenge, expected_peer) =
                    (preimage.clone(), *our_challenge, *expected_peer);
                let CryptoResult::Signed { signature } = result;
                self.on_sim_open_loser_signed(conn, preimage, our_challenge, expected_peer, signature)
            }
            HandshakeState::AwaitingChallenge
            | HandshakeState::AwaitingChallengeSign { .. }
            | HandshakeState::AwaitingResponse { .. }
            | HandshakeState::AwaitingResponseSign { .. }
            | HandshakeState::SimOpenLoserSign { .. }
            | HandshakeState::SimOpenAwaitTheirResponse { .. }
            | HandshakeState::Authenticated
            | HandshakeState::Closing => self.handshake_crypto_done(now, conn, ticket, result),
        }
    }

    /// Crypto completions for the plain (non-sim-open) handshake states.
    /// The caller has already validated connection and generation.
    fn handshake_crypto_done(
        &mut self,
        _now: Now,
        conn: ConnId,
        ticket: CryptoTicket,
        result: CryptoResult,
    ) -> Outcome {
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match (&entry.state, result) {
            // ── initiator: challenge signed ────────────────────────
            (
                HandshakeState::AwaitingChallengeSign {
                    ticket: expected,
                    challenge,
                    preimage,
                },
                CryptoResult::Signed { signature },
            ) if *expected == ticket => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let challenge = *challenge;
                entry.state = HandshakeState::AwaitingResponse {
                    challenge,
                    signed_bytes: bytes.clone(),
                };
                self.effects.push_back(Effect::SendMessage { conn, bytes });
                Outcome::Progressed
            }

            // ── responder: response signed ─────────────────────────
            (
                HandshakeState::AwaitingResponseSign {
                    ticket: expected,
                    preimage,
                    initiator,
                },
                CryptoResult::Signed { signature },
            ) if *expected == ticket => {
                let mut bytes = preimage.clone();
                bytes.extend_from_slice(&signature);
                let peer = *initiator;
                self.effects.push_back(Effect::SendMessage { conn, bytes });
                self.authenticate(conn, peer)
            }

            // Right generation, but no pending operation matches this
            // ticket/result shape — a duplicate, or a driver bug.
            (
                HandshakeState::AwaitingChallenge
                | HandshakeState::AwaitingChallengeSign { .. }
                | HandshakeState::AwaitingResponse { .. }
                | HandshakeState::AwaitingResponseSign { .. }
                | HandshakeState::SimOpenLoserSign { .. }
                | HandshakeState::SimOpenAwaitTheirResponse { .. }
                | HandshakeState::Authenticated
                | HandshakeState::Closing,
                CryptoResult::Signed { .. },
            ) => {
                self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
                Outcome::Ignored(IgnoreReason::UnknownTicket)
            }
        }
    }

    // ── helpers ────────────────────────────────────────────────────

    /// Responder: send an unsigned [`Rejection`] and condemn the
    /// connection.
    fn reject_challenge(&mut self, now: Now, conn: ConnId, reason: RejectionReason) -> Outcome {
        self.effects.push_back(Effect::SendMessage {
            conn,
            bytes: HandshakeMessage::Rejection(Rejection::new(reason, now.wall)).encode(),
        });
        self.fault(conn, Fault::ChallengeRejected(reason))
    }

    /// Pure challenge checks (audience, freshness) on an
    /// already-verified challenge.
    fn validate_challenge(&self, now: Now, challenge: &Challenge) -> Result<(), RejectionReason> {
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

        Ok(())
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

        let expired = self.expire_sync_requests(now);
        !due.is_empty() || expired
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

    /// Post-handshake driver ops in flight (concurrent, unlike the
    /// exclusive handshake states), keyed by ticket seq.
    pending: Map<Seq, sync::ConnPending>,

    /// In-flight batch sync requests we initiated, keyed by request nonce.
    requests: Map<u64, sync::OutboundRequest>,

    /// The authenticated peer, once known.
    peer: Option<PeerId>,

    /// Where this connection is in the handshake.
    state: HandshakeState,
}

impl ConnEntry {
    const fn issue_ticket(&mut self, conn: ConnId) -> CryptoTicket {
        let ticket = CryptoTicket {
            entity: Entity::Connection(conn),
            generation: self.generation,
            seq: self.next_seq,
        };
        self.next_seq = self.next_seq.next();
        ticket
    }

    const fn issue_storage_ticket(&mut self, conn: ConnId) -> StorageTicket {
        let ticket = StorageTicket {
            entity: Entity::Connection(conn),
            generation: self.generation,
            seq: self.next_seq,
        };
        self.next_seq = self.next_seq.next();
        ticket
    }
}

/// A pending [`Entity::Local`] storage operation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalPending {
    /// An [`IngestLocal`](StorageOp::IngestLocal) awaiting durability.
    Ingest {
        /// The tree being appended to.
        tree: SedimentreeId,
        /// The commits' blobs, in op order, held for the post-durability
        /// subscriber broadcast (transit-only: dropped when the op
        /// resolves).
        commit_blobs: Vec<Blob>,
        /// The fragments' blobs, in op order (same lifecycle).
        fragment_blobs: Vec<Blob>,
    },

    /// A [`DeleteTree`](StorageOp::DeleteTree) awaiting completion.
    Delete {
        /// The tree being removed.
        tree: SedimentreeId,
    },
}

/// The handshake sub-machine. `Awaiting{…}Sign`/`…Verify` states hold the
/// expected [`CryptoTicket`] — the witness that pairs the eventual
/// completion with exactly this state (ADR-007).
#[derive(Debug)]
enum HandshakeState {
    /// Inbound: waiting for the initiator's challenge.
    AwaitingChallenge,

    /// Outbound: our challenge is at the driver being signed. The pin
    /// (for `Audience::Known`) is derived from `challenge.audience`.
    AwaitingChallengeSign {
        ticket: CryptoTicket,
        challenge: Challenge,
        preimage: Vec<u8>,
    },

    /// Outbound: challenge sent; waiting for the responder (or, in a
    /// simultaneous open, their crossed challenge). `signed_bytes` is our
    /// challenge as sent — the reflection check and tie-break need it.
    AwaitingResponse {
        challenge: Challenge,
        signed_bytes: Vec<u8>,
    },

    /// Simultaneous open, loser: our response to their challenge is being
    /// signed (loser sends first, then awaits their response to ours).
    SimOpenLoserSign {
        ticket: CryptoTicket,
        preimage: Vec<u8>,
        our_challenge: Challenge,
        expected: PeerId,
    },

    /// Simultaneous open: waiting for their response to our challenge.
    /// `owed` is their challenge if we still owe them a response
    /// (winner path: receive-verify theirs first, then sign ours).
    SimOpenAwaitTheirResponse {
        our_challenge: Challenge,
        owed: Option<Challenge>,
        expected: PeerId,
    },

    /// Inbound: our response is at the driver being signed.
    AwaitingResponseSign {
        ticket: CryptoTicket,
        preimage: Vec<u8>,
        initiator: PeerId,
    },

    /// Handshake complete; sync protocol takes over (Phase 2).
    Authenticated,

    /// Condemned; waiting for the driver's `Disconnected` confirmation.
    Closing,
}

// ── free helpers ────────────────────────────────────────────────────






/// Decode the typed payload fields out of a received [`Signed<T>`]
/// without verifying the signature (verification is a driver effect).
fn try_decode_payload<T>(signed: &Signed<T>) -> Result<(T, usize), sedimentree_core::codec::error::DecodeError>
where
    T: Schema + EncodeFields + sedimentree_core::codec::decode::DecodeFields,
{
    T::try_decode_fields(signed.fields_bytes())
}
