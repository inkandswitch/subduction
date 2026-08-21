//! The core machine (ADR-015): resident trees, sync sessions,
//! subscriptions, fan-out, and nonce arbitration — everything shared
//! across connections, fed exclusively by sealed edges.
//!
//! The core never sees wire bytes and never verifies signatures: every
//! sync item that reaches it arrived inside a [`Sealed<ConnToCore>`]
//! that only a [`ConnMachine`](crate::conn_machine::ConnMachine) can
//! mint, carrying items that machine already verified. The core's
//! security posture is therefore: enforce edge discipline (in-order,
//! exactly-once, current-generation via [`EdgeSequencer`]), arbitrate
//! nonces, and make protocol decisions.
//!
//! # Leases (ADR-015 condition 2, narrowed)
//!
//! The core arms a lease per edge covering the **handshake window only**
//! (`Opened` → `Authenticated`): a connection machine that dies mid-
//! handshake without a `Closed` message is cleaned up at lease expiry
//! via `poll_timeout`. Post-authentication liveness is a transport
//! concern (keepalives live in the driver; supervision in Phase 3) —
//! an idle authenticated edge is healthy, not expired.
//!
//! Sync sessions land in the next Phase 2.5 commit; this file owns the
//! shell: edge lifecycle, nonce arbitration, and local data commands.

mod sync;

use alloc::{collections::VecDeque, vec::Vec};
use core::time::Duration;

use sedimentree_core::{
    blob::Blob,
    collections::{Map, Set},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::minimized::MinimizedSedimentree,
};
use subduction_crypto::signed::Signed;

use crate::{
    blob_ref::Part,
    command::Command,
    edge::{ConnToCore, CoreToConn, EdgeId, EdgeSequencer, Sealed},
    effect::AppEvent,
    id::{ConnId, Generation, Seq},
    nonce_cache::NonceCache,
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    stats::Stats,
    storage::{Provenance, StorageFailure, StorageOp, StorageResult},
    ticket::{Entity, StorageTicket},
    timestamp::{Now, Timestamp},
    wire,
};

/// Static configuration for the [`CoreMachine`].
// Not `Copy`: will grow policy hooks; removing `Copy` later is breaking.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct CoreConfig {
    /// Our identity (for `RequestId`s and mutual-subscription logic).
    pub local_peer: PeerId,

    /// How long a batch sync request may await its response.
    pub sync_timeout: Duration,

    /// How long an edge may sit between `Opened` and `Authenticated`
    /// before the core presumes the connection machine dead.
    pub handshake_lease: Duration,

    /// Entropy for fingerprint seeds (CSPRNG-seeded, per machine).
    pub entropy: [u8; 32],
}

impl CoreConfig {
    /// Defaults for everything but identity/entropy.
    #[must_use]
    pub const fn new(local_peer: PeerId, entropy: [u8; 32]) -> Self {
        Self {
            local_peer,
            sync_timeout: Duration::from_secs(30),
            handshake_lease: Duration::from_secs(60),
            entropy,
        }
    }
}

/// What the core asks of the world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreEffect {
    /// Send one wire message on a connection (scatter-gather; blob
    /// regions ride as refs).
    Send {
        /// The target connection.
        conn: ConnId,
        /// The message parts.
        parts: Vec<Part>,
    },

    /// A sealed control answer for a connection machine.
    ToConn(Sealed<CoreToConn>),

    /// A storage operation (authorize + persist / fetch; ADR-015).
    Storage {
        /// Completion witness.
        ticket: StorageTicket,
        /// The operation.
        op: StorageOp,
    },

    /// Tear down a connection (lease expiry, lagging subscriber policy).
    Disconnect {
        /// The condemned connection.
        conn: ConnId,
    },

    /// A blob ref has left core state; the driver may decrement its
    /// retention (ADR-015 condition 5).
    ReleaseBlob(crate::blob_ref::BlobRef),

    /// An application-facing event.
    App(AppEvent),
}

/// Inputs to the core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreEvent {
    /// A sealed edge message from a connection machine.
    FromConn(Sealed<ConnToCore>),

    /// A storage completion (ticket echoed from
    /// [`CoreEffect::Storage`]).
    StorageDone {
        /// The witness.
        ticket: StorageTicket,
        /// The result.
        result: StorageResult,
    },

    /// A local application request.
    Command(Command),

    /// Timer service.
    Wake,
}

/// The core machine. See the [module docs](self).
#[derive(Debug)]
pub struct CoreMachine {
    config: CoreConfig,
    edges: Map<ConnId, EdgeEntry>,
    trees: Map<SedimentreeId, MinimizedSedimentree>,
    subscriptions: Map<SedimentreeId, Set<ConnId>>,
    nonce_arbiter: NonceCache,
    local_pending: Map<Seq, LocalPending>,
    local_generation: Generation,
    local_next_seq: Seq,
    /// Per-peer monotonic heads counters (sent side / received-staleness
    /// filter). NOTE: the one deliberately cross-tree piece of sync
    /// state (wire semantics) — see the sharding note in TODO.md.
    heads_sent: Map<PeerId, u64>,
    heads_recv: Map<PeerId, u64>,
    /// Monotonic nonce for outbound `RequestIds`.
    request_nonce: u64,
    /// Counter for fingerprint-seed PRF derivation.
    seed_counter: u64,
    effects: VecDeque<CoreEffect>,
    stats: Stats,
}

impl CoreMachine {
    /// Create the core from configuration.
    #[must_use]
    pub fn new(config: CoreConfig) -> Self {
        Self {
            config,
            edges: Map::new(),
            trees: Map::new(),
            subscriptions: Map::new(),
            nonce_arbiter: NonceCache::default(),
            local_pending: Map::new(),
            local_generation: Generation::FIRST,
            local_next_seq: Seq::FIRST,
            heads_sent: Map::new(),
            heads_recv: Map::new(),
            request_nonce: 0,
            seed_counter: 0,
            effects: VecDeque::new(),
            stats: Stats::default(),
        }
    }

    /// Feed one event; drain [`poll_effect`](Self::poll_effect) after.
    /// Due leases are processed on every call.
    pub fn handle(&mut self, now: Now, event: CoreEvent) -> Outcome {
        let expired = self.expire_leases(now.monotonic) | self.expire_requests(now.monotonic);

        match event {
            CoreEvent::FromConn(sealed) => self.on_from_conn(now, sealed),
            CoreEvent::StorageDone { ticket, result } => self.on_storage_done(now, ticket, result),
            CoreEvent::Command(command) => self.on_command(now, command),
            CoreEvent::Wake => {
                if expired {
                    Outcome::Progressed
                } else {
                    Outcome::Idle
                }
            }
        }
    }

    /// Next queued effect.
    pub fn poll_effect(&mut self) -> Option<CoreEffect> {
        self.effects.pop_front()
    }

    /// The earliest deadline the driver must wake the core at.
    #[must_use]
    pub fn poll_timeout(&self) -> Option<Timestamp> {
        let leases = self.edges.values().filter_map(|edge| edge.lease);
        let requests = self
            .edges
            .values()
            .flat_map(|edge| edge.requests.values().map(|request| request.deadline));
        leases.chain(requests).min()
    }

    /// Tier-2 telemetry snapshot.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    /// Read access: resident tree ids.
    pub fn tree_ids(&self) -> impl Iterator<Item = SedimentreeId> {
        self.trees.keys().copied()
    }

    /// Read access: a resident tree's heads (`&mut`: lazy minimization).
    pub fn tree_heads(&mut self, tree: SedimentreeId) -> Option<Vec<CommitId>> {
        self.trees
            .get_mut(&tree)
            .map(|entry| entry.heads(&CountLeadingZeroBytes))
    }

    // ── edge traffic ───────────────────────────────────────────────

    fn on_from_conn(&mut self, now: Now, sealed: Sealed<ConnToCore>) -> Outcome {
        let (edge, seq, msg) = sealed.open();

        // Registration is the sequencer's birth; everything else must
        // pass the existing sequencer.
        if let ConnToCore::Opened { .. } = &msg {
            return self.on_opened(now, edge, seq);
        }
        let Some(entry) = self.edges.get_mut(&edge.conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(edge.conn));
        };
        if let Err(violation) = entry.sequencer.accept(edge, seq) {
            self.stats.stale_completions = self.stats.stale_completions.saturating_add(1);
            return Outcome::Ignored(IgnoreReason::Edge(violation));
        }

        match msg {
            ConnToCore::Opened { .. } => unreachable!("handled above"),
            ConnToCore::Authenticated { peer } => {
                entry.peer = Some(peer);
                entry.lease = None; // handshake window closed
                Outcome::Progressed
            }
            ConnToCore::ClaimNonce {
                peer,
                nonce,
                timestamp,
            } => {
                let granted = self.nonce_arbiter.try_claim(peer, nonce, timestamp).is_ok();
                let verdict = CoreToConn::NonceVerdict { granted };
                self.send_to_conn(edge.conn, verdict);
                Outcome::Progressed
            }
            ConnToCore::Inbound(forward) => self.on_sync_forward(now, edge.conn, *forward),
            ConnToCore::Closed { fault: _fault } => {
                self.teardown_edge(edge.conn);
                Outcome::Progressed
            }
        }
    }

    fn on_opened(&mut self, now: Now, edge: EdgeId, seq: Seq) -> Outcome {
        // A fresh incarnation replaces any stale entry for this conn.
        let mut sequencer = EdgeSequencer::new(edge);
        if let Err(violation) = sequencer.accept(edge, seq) {
            return Outcome::Ignored(IgnoreReason::Edge(violation));
        }
        let stale = self.edges.insert(
            edge.conn,
            EdgeEntry {
                edge,
                sequencer,
                peer: None,
                lease: Some(now.monotonic.saturating_add(self.config.handshake_lease)),
                out_seq: Seq::FIRST,
                requests: Map::new(),
                pending: Map::new(),
                next_ticket: Seq::FIRST,
            },
        );
        if let Some(stale) = stale {
            // Old incarnation never said Closed: clean up after it.
            self.cleanup_conn_state(edge.conn, stale.peer);
        }
        self.stats.connections_opened = self.stats.connections_opened.saturating_add(1);
        Outcome::Progressed
    }

    // ── lifecycle helpers ──────────────────────────────────────────

    fn teardown_edge(&mut self, conn: ConnId) {
        if let Some(entry) = self.edges.remove(&conn) {
            self.cleanup_conn_state(conn, entry.peer);
        }
        self.stats.connections_closed = self.stats.connections_closed.saturating_add(1);
    }

    /// Remove every trace of a connection from shared state.
    fn cleanup_conn_state(&mut self, conn: ConnId, peer: Option<PeerId>) {
        self.subscriptions.retain(|_tree, conns| {
            let _removed = conns.remove(&conn);
            !conns.is_empty()
        });
        self.effects
            .push_back(CoreEffect::App(AppEvent::ConnectionClosed { conn, peer }));
    }

    /// Expire handshake-window leases. The connection machine is
    /// presumed dead: disconnect the transport and clean up.
    fn expire_leases(&mut self, now: Timestamp) -> bool {
        let due: Vec<ConnId> = self
            .edges
            .iter()
            .filter_map(|(conn, entry)| {
                entry
                    .lease
                    .is_some_and(|lease| lease.is_due(now))
                    .then_some(*conn)
            })
            .collect();
        for conn in &due {
            self.effects
                .push_back(CoreEffect::Disconnect { conn: *conn });
            self.teardown_edge(*conn);
            self.stats.handshake_timeouts = self.stats.handshake_timeouts.saturating_add(1);
        }
        !due.is_empty()
    }

    fn send_to_conn(&mut self, conn: ConnId, msg: CoreToConn) {
        let Some(entry) = self.edges.get_mut(&conn) else {
            return;
        };
        let sealed = Sealed::mint(entry.edge, entry.out_seq, msg);
        entry.out_seq = entry.out_seq.next();
        self.effects.push_back(CoreEffect::ToConn(sealed));
    }

    // ── local data commands (ported from the single machine) ───────

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

            Command::AddCommits { tree, commits } => self.ingest_local(tree, commits, Vec::new()),
            Command::AddFragments { tree, fragments } => {
                self.ingest_local(tree, Vec::new(), fragments)
            }

            Command::RemoveTree { tree } => {
                let _resident = self.trees.remove(&tree);
                let ticket = self.issue_local_ticket();
                self.local_pending
                    .insert(ticket.seq, LocalPending::Delete { tree });
                self.effects.push_back(CoreEffect::Storage {
                    ticket,
                    op: StorageOp::DeleteTree {
                        tree,
                        provenance: Provenance::Local,
                    },
                });
                Outcome::Progressed
            }

            Command::Unsubscribe { conn, trees } => {
                let authenticated = self
                    .edges
                    .get(&conn)
                    .is_some_and(|entry| entry.peer.is_some());
                if !authenticated {
                    return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
                }
                let msg = wire::SyncMessage::RemoveSubscriptions(wire::RemoveSubscriptions {
                    ids: trees,
                });
                self.effects.push_back(CoreEffect::Send {
                    conn,
                    parts: alloc::vec![Part::Bytes(msg.encode())],
                });
                Outcome::Progressed
            }

            Command::SyncTree {
                conn,
                tree,
                subscribe,
            } => self.start_sync(now, conn, tree, subscribe),

            // Extension sends are the connection machine's job; the node
            // routes them there, never here.
            Command::SendExtension { conn, .. } => {
                Outcome::Ignored(IgnoreReason::NotAuthenticated(conn))
            }
        }
    }

    fn ingest_local(
        &mut self,
        tree: SedimentreeId,
        commits: Vec<crate::command::NewCommit>,
        fragments: Vec<crate::command::NewFragment>,
    ) -> Outcome {
        let ticket = self.issue_local_ticket();
        let commit_blobs = commits
            .iter()
            .map(|new| (new.head, new.blob.clone()))
            .collect();
        let fragment_blobs = fragments
            .iter()
            .map(|new| (new.head, new.blob.clone()))
            .collect();
        self.local_pending.insert(
            ticket.seq,
            LocalPending::Ingest {
                tree,
                commit_blobs,
                fragment_blobs,
            },
        );
        self.effects.push_back(CoreEffect::Storage {
            ticket,
            op: StorageOp::IngestLocal {
                tree,
                commits,
                fragments,
            },
        });
        Outcome::Progressed
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
                let Some(entry) = self.edges.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                if ticket.generation != entry.edge.generation {
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
                if !self.trees.contains_key(&tree) && self.local_delete_pending(tree) {
                    return Outcome::Ignored(IgnoreReason::StaleTicket);
                }
                let entry = self.trees.entry(tree).or_default();
                let mut heads = Vec::with_capacity(commits.len());
                let mut push_commits: Vec<(&Signed<LooseCommit>, Part)> = Vec::new();
                for signed in &commits {
                    if let Ok(verified) = signed.try_verify() {
                        let head = verified.payload().head();
                        heads.push(head);
                        let _fresh = entry.add_commit(verified.payload().clone());
                        if let Some(blob) = commit_blobs.get(&head) {
                            push_commits
                                .push((signed, Part::Bytes(blob.as_slice().to_vec())));
                        }
                    }
                }
                let mut fragment_heads = Vec::with_capacity(fragments.len());
                let mut push_fragments: Vec<(&Signed<Fragment>, Part)> = Vec::new();
                for signed in &fragments {
                    if let Ok(verified) = signed.try_verify() {
                        let head = verified.payload().head();
                        fragment_heads.push(head);
                        let _fresh = entry.add_fragment(verified.payload().clone());
                        if let Some(blob) = fragment_blobs.get(&head) {
                            push_fragments
                                .push((signed, Part::Bytes(blob.as_slice().to_vec())));
                        }
                    }
                }
                if !heads.is_empty() {
                    self.effects
                        .push_back(CoreEffect::App(AppEvent::CommitsStored { tree, heads }));
                }
                if !fragment_heads.is_empty() {
                    self.effects
                        .push_back(CoreEffect::App(AppEvent::FragmentsStored {
                            tree,
                            heads: fragment_heads,
                        }));
                }
                // Local writes push to subscribers with inline blob bytes
                // (the core already holds them; no driver frames involved).
                self.broadcast_items(tree, &push_commits, &push_fragments, None);
                Outcome::Progressed
            }

            (LocalPending::Delete { tree }, StorageResult::TreeDeleted) => {
                self.effects
                    .push_back(CoreEffect::App(AppEvent::TreeRemoved { tree }));
                Outcome::Progressed
            }

            (
                LocalPending::Ingest { tree, .. } | LocalPending::Delete { tree },
                StorageResult::Failed(failure),
            ) => {
                self.effects
                    .push_back(CoreEffect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }

            (
                LocalPending::Ingest { tree, .. } | LocalPending::Delete { tree },
                StorageResult::FetchedRefs { .. }
                | StorageResult::Persisted { .. }
                | StorageResult::TreeDeleted
                | StorageResult::LocallyIngested { .. }
                | StorageResult::Unauthorized
                | StorageResult::UnknownTree,
            ) => {
                self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
                self.effects
                    .push_back(CoreEffect::App(AppEvent::StorageError {
                        tree,
                        failure: StorageFailure::Permanent,
                    }));
                Outcome::Ignored(IgnoreReason::UnknownTicket)
            }
        }
    }

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
}

/// Per-edge core-side state.
#[derive(Debug)]
struct EdgeEntry {
    edge: EdgeId,
    sequencer: EdgeSequencer,
    peer: Option<PeerId>,
    /// Handshake-window lease; disarmed at `Authenticated`.
    lease: Option<Timestamp>,
    /// Sequence for minting `CoreToConn` messages.
    out_seq: Seq,
    /// In-flight batch sync requests we initiated on this edge, keyed by
    /// request nonce. (Single-tree each: the no-cross-tree invariant.)
    requests: Map<u64, sync::OutboundRequest>,
    /// Driver storage ops in flight for this edge, keyed by ticket seq.
    pending: Map<Seq, sync::CorePending>,
    /// Sequence for storage tickets on this edge.
    next_ticket: Seq,
}

impl EdgeEntry {
    const fn issue_ticket(&mut self) -> StorageTicket {
        let ticket = StorageTicket {
            entity: Entity::Connection(self.edge.conn),
            generation: self.edge.generation,
            seq: self.next_ticket,
        };
        self.next_ticket = self.next_ticket.next();
        ticket
    }
}

/// A pending local storage operation.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalPending {
    /// A fused local seal+persist awaiting durability. Carries the blob
    /// bytes so the subscriber broadcast can splice them inline — local
    /// writes never touch the driver's frame table.
    Ingest {
        /// The tree being appended to.
        tree: SedimentreeId,

        /// Commit blobs by head id.
        commit_blobs: Map<CommitId, Blob>,

        /// Fragment blobs by head id.
        fragment_blobs: Map<CommitId, Blob>,
    },

    /// A tree deletion awaiting completion.
    Delete {
        /// The tree being removed.
        tree: SedimentreeId,
    },
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::{event::Direction, wall_clock::TimestampSeconds};
    use sedimentree_core::loose_commit::LooseCommit;
    use subduction_crypto::{nonce::Nonce, signed::Signed};

    const fn now_at(ms: u64) -> Now {
        Now {
            monotonic: Timestamp::from_millis(ms),
            wall: TimestampSeconds::new(1_700_000_000),
        }
    }

    fn core() -> CoreMachine {
        CoreMachine::new(CoreConfig::new(PeerId::new([0xCC; 32]), [1u8; 32]))
    }

    const fn edge(conn: u64, generation: Generation) -> EdgeId {
        EdgeId {
            conn: ConnId::new(conn),
            generation,
        }
    }

    /// Open an edge at seq 0 and return (edge, next seq to use).
    fn open_edge(core: &mut CoreMachine, conn: u64) -> (EdgeId, Seq) {
        let e = edge(conn, Generation::FIRST);
        let outcome = core.handle(
            now_at(0),
            CoreEvent::FromConn(Sealed::mint(
                e,
                Seq::FIRST,
                ConnToCore::Opened {
                    direction: Direction::Inbound,
                },
            )),
        );
        assert_eq!(outcome, Outcome::Progressed);
        (e, Seq::FIRST.next())
    }

    #[test]
    fn nonce_arbitration_catches_replay_across_connections() -> testresult::TestResult {
        let mut core = core();
        let peer = PeerId::new([0xAA; 32]);
        let nonce = Nonce::from_u128(7);
        let ts = TimestampSeconds::new(1_700_000_000);

        let (e1, s1) = open_edge(&mut core, 1);
        let (e2, s2) = open_edge(&mut core, 2);

        let claim = |e, s| {
            CoreEvent::FromConn(Sealed::mint(
                e,
                s,
                ConnToCore::ClaimNonce {
                    peer,
                    nonce,
                    timestamp: ts,
                },
            ))
        };

        // First claim (conn 1): granted.
        let _outcome = core.handle(now_at(1), claim(e1, s1));
        let Some(CoreEffect::ToConn(sealed)) = core.poll_effect() else {
            return Err("expected a verdict".into());
        };
        let (_e, _s, msg) = sealed.open();
        assert_eq!(msg, CoreToConn::NonceVerdict { granted: true });

        // Same (peer, nonce) via a DIFFERENT connection: replay, denied.
        let _outcome = core.handle(now_at(2), claim(e2, s2));
        let Some(CoreEffect::ToConn(sealed)) = core.poll_effect() else {
            return Err("expected a verdict".into());
        };
        let (_e, _s, msg) = sealed.open();
        assert_eq!(msg, CoreToConn::NonceVerdict { granted: false });
        Ok(())
    }

    #[test]
    fn edge_discipline_rejects_replay_gap_and_stale_generation() {
        let mut core = core();
        let (e, s1) = open_edge(&mut core, 1);
        let auth = |e, s| {
            CoreEvent::FromConn(Sealed::mint(
                e,
                s,
                ConnToCore::Authenticated {
                    peer: PeerId::new([0xBB; 32]),
                },
            ))
        };

        // In-order: accepted.
        assert_eq!(core.handle(now_at(1), auth(e, s1)), Outcome::Progressed);

        // Replay of the same seq: rejected, state untouched.
        let outcome = core.handle(now_at(2), auth(e, s1));
        assert!(matches!(outcome, Outcome::Ignored(IgnoreReason::Edge(_))));

        // Gap: skipping ahead.
        let far = s1.next().next().next();
        let outcome = core.handle(now_at(3), auth(e, far));
        assert!(matches!(outcome, Outcome::Ignored(IgnoreReason::Edge(_))));

        // Stale generation.
        let stale = edge(1, Generation::FIRST.next());
        let outcome = core.handle(now_at(4), auth(stale, s1.next()));
        assert!(matches!(outcome, Outcome::Ignored(IgnoreReason::Edge(_))));
    }

    #[test]
    fn handshake_lease_expires_unauthenticated_edges_only() {
        let mut core = core();
        let (e1, s1) = open_edge(&mut core, 1);
        let (_e2, _s2) = open_edge(&mut core, 2);

        // Conn 1 authenticates; its lease disarms.
        let _outcome = core.handle(
            now_at(1),
            CoreEvent::FromConn(Sealed::mint(
                e1,
                s1,
                ConnToCore::Authenticated {
                    peer: PeerId::new([0xBB; 32]),
                },
            )),
        );
        assert!(core.poll_timeout().is_some(), "conn 2 lease still armed");

        // Past the lease: conn 2 (still unauthenticated) is condemned.
        let late = now_at(61_000);
        let outcome = core.handle(late, CoreEvent::Wake);
        assert_eq!(outcome, Outcome::Progressed);
        let mut disconnected = Vec::new();
        while let Some(effect) = core.poll_effect() {
            if let CoreEffect::Disconnect { conn } = effect {
                disconnected.push(conn);
            }
        }
        assert_eq!(disconnected, [ConnId::new(2)]);
        assert_eq!(core.poll_timeout(), None, "no leases remain");
    }

    #[test]
    fn closed_edge_is_cleaned_up_and_reopen_supersedes() {
        let mut core = core();
        let (e, s1) = open_edge(&mut core, 1);
        let _outcome = core.handle(
            now_at(1),
            CoreEvent::FromConn(Sealed::mint(e, s1, ConnToCore::Closed { fault: None })),
        );
        let mut closed = false;
        while let Some(effect) = core.poll_effect() {
            if matches!(effect, CoreEffect::App(AppEvent::ConnectionClosed { .. })) {
                closed = true;
            }
        }
        assert!(closed, "closure surfaced to the app");

        // Reopening the same conn with a NEW generation works (fresh
        // sequencer), and the old generation is dead.
        let e2 = edge(1, Generation::FIRST.next());
        let outcome = core.handle(
            now_at(2),
            CoreEvent::FromConn(Sealed::mint(
                e2,
                Seq::FIRST,
                ConnToCore::Opened {
                    direction: Direction::Inbound,
                },
            )),
        );
        assert_eq!(outcome, Outcome::Progressed);
        let outcome = core.handle(
            now_at(3),
            CoreEvent::FromConn(Sealed::mint(
                e,
                s1.next(),
                ConnToCore::Authenticated {
                    peer: PeerId::new([0xBB; 32]),
                },
            )),
        );
        assert!(matches!(outcome, Outcome::Ignored(IgnoreReason::Edge(_))));
    }

    #[test]
    fn local_data_commands_round_trip() -> testresult::TestResult {
        use sedimentree_core::blob::Blob;
        let mut core = core();
        let tree = SedimentreeId::new([7u8; 32]);

        let outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::AddCommits {
                tree,
                commits: alloc::vec![crate::command::NewCommit {
                    head: CommitId::new([9u8; 32]),
                    parents: alloc::collections::BTreeSet::new(),
                    blob: Blob::new(alloc::vec![9u8; 8]),
                }],
            }),
        );
        assert_eq!(outcome, Outcome::Progressed);
        let Some(CoreEffect::Storage { ticket, op }) = core.poll_effect() else {
            return Err("expected a storage effect".into());
        };
        assert_eq!(ticket.entity, Entity::Local);
        let StorageOp::IngestLocal { commits, .. } = op else {
            return Err("expected IngestLocal".into());
        };

        // Driver completes: seal + persist (reuse a real signer).
        let signer = subduction_crypto::signer::memory::MemorySigner::from_bytes(&[42u8; 32]);
        let sealed: Vec<_> = commits
            .iter()
            .map(|new| {
                let commit = LooseCommit::new(
                    tree,
                    new.head,
                    new.parents.clone(),
                    sedimentree_core::blob::BlobMeta::new(&new.blob),
                );
                futures::executor::block_on(Signed::seal::<future_form::Sendable, _>(
                    &signer, commit,
                ))
                .into_signed()
            })
            .collect();
        let outcome = core.handle(
            now_at(1),
            CoreEvent::StorageDone {
                ticket,
                result: StorageResult::LocallyIngested {
                    commits: sealed,
                    fragments: alloc::vec![],
                },
            },
        );
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(
            core.tree_heads(tree),
            Some(alloc::vec![CommitId::new([9u8; 32])])
        );
        assert!(matches!(
            core.poll_effect(),
            Some(CoreEffect::App(AppEvent::CommitsStored { .. }))
        ));
        Ok(())
    }

    /// Seal + persist one `IngestLocal` op like a real driver would.
    fn execute_ingest_local(
        tree: SedimentreeId,
        commits: &[crate::command::NewCommit],
    ) -> StorageResult {
        use sedimentree_core::blob::BlobMeta;
        let signer = subduction_crypto::signer::memory::MemorySigner::from_bytes(&[42u8; 32]);
        let sealed: Vec<_> = commits
            .iter()
            .map(|new| {
                let commit = LooseCommit::new(
                    tree,
                    new.head,
                    new.parents.clone(),
                    BlobMeta::new(&new.blob),
                );
                futures::executor::block_on(Signed::seal::<future_form::Sendable, _>(
                    &signer, commit,
                ))
                .into_signed()
            })
            .collect();
        StorageResult::LocallyIngested {
            commits: sealed,
            fragments: alloc::vec![],
        }
    }

    fn new_commit(head: u8) -> crate::command::NewCommit {
        use sedimentree_core::blob::Blob;
        crate::command::NewCommit {
            head: CommitId::new([head; 32]),
            parents: alloc::collections::BTreeSet::new(),
            blob: Blob::new(alloc::vec![head; 8]),
        }
    }

    #[test]
    fn remove_tree_round_trips() -> testresult::TestResult {
        let mut core = core();
        let tree = SedimentreeId::new([7u8; 32]);

        let _outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::HydrateTree {
                tree,
                commits: alloc::vec![LooseCommit::new(
                    tree,
                    CommitId::new([1u8; 32]),
                    alloc::collections::BTreeSet::new(),
                    sedimentree_core::blob::BlobMeta::new(&sedimentree_core::blob::Blob::new(
                        alloc::vec![1u8; 8],
                    )),
                )],
                fragments: alloc::vec![],
            }),
        );

        let outcome = core.handle(now_at(1), CoreEvent::Command(Command::RemoveTree { tree }));
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(core.tree_heads(tree), None, "resident state gone at once");

        let Some(CoreEffect::Storage { ticket, op }) = core.poll_effect() else {
            return Err("expected a storage effect".into());
        };
        assert!(matches!(op, StorageOp::DeleteTree { tree: t, .. } if t == tree));

        let outcome = core.handle(
            now_at(2),
            CoreEvent::StorageDone {
                ticket,
                result: StorageResult::TreeDeleted,
            },
        );
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(
            core.poll_effect(),
            Some(CoreEffect::App(AppEvent::TreeRemoved { tree }))
        );
        Ok(())
    }

    #[test]
    fn ingest_completion_after_remove_is_dropped() -> testresult::TestResult {
        let mut core = core();
        let tree = SedimentreeId::new([7u8; 32]);

        // Ingest goes out…
        let _outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::AddCommits {
                tree,
                commits: alloc::vec![new_commit(9)],
            }),
        );
        let Some(CoreEffect::Storage { ticket, op }) = core.poll_effect() else {
            return Err("expected ingest effect".into());
        };
        let StorageOp::IngestLocal { commits, .. } = op else {
            return Err("expected IngestLocal".into());
        };

        // …but the app removes the tree while the write is in flight.
        let _outcome = core.handle(now_at(1), CoreEvent::Command(Command::RemoveTree { tree }));
        let Some(CoreEffect::Storage { .. }) = core.poll_effect() else {
            return Err("expected delete effect".into());
        };

        // The ingest completion lands after the removal decision: dropped.
        let result = execute_ingest_local(tree, &commits);
        let outcome = core.handle(now_at(2), CoreEvent::StorageDone { ticket, result });
        assert_eq!(outcome, Outcome::Ignored(IgnoreReason::StaleTicket));
        assert_eq!(core.tree_heads(tree), None, "removed tree stays removed");
        Ok(())
    }

    #[test]
    fn mutated_local_ticket_is_ignored() -> testresult::TestResult {
        let mut core = core();
        let tree = SedimentreeId::new([7u8; 32]);

        let _outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::AddCommits {
                tree,
                commits: alloc::vec![new_commit(9)],
            }),
        );
        let Some(CoreEffect::Storage { ticket, op }) = core.poll_effect() else {
            return Err("expected ingest effect".into());
        };
        let StorageOp::IngestLocal { commits, .. } = op else {
            return Err("expected IngestLocal".into());
        };

        // A completion whose ticket seq was mutated must be ignored…
        let mut mutated = ticket;
        mutated.seq = mutated.seq.next();
        let result = execute_ingest_local(tree, &commits);
        let outcome = core.handle(
            now_at(1),
            CoreEvent::StorageDone {
                ticket: mutated,
                result: result.clone(),
            },
        );
        assert_eq!(outcome, Outcome::Ignored(IgnoreReason::UnknownTicket));
        assert_eq!(core.tree_heads(tree), None, "mutated ticket lands nothing");
        assert_eq!(core.poll_effect(), None);

        // …while the exact witness still lands.
        let outcome = core.handle(now_at(2), CoreEvent::StorageDone { ticket, result });
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(
            core.tree_heads(tree),
            Some(alloc::vec![CommitId::new([9u8; 32])])
        );
        Ok(())
    }
}
