//! The node: connection machines + the core machine + the router,
//! composed into one pure artifact.
//!
//! This is the shape platforms bind: sealed inter-machine traffic is
//! routed *inside* [`Node::handle`] and never crosses to driver code —
//! drivers see only **leaf** effects (bytes to send, storage ops,
//! signing, releases, app events) and feed only world events. A driver
//! cannot fabricate verified data because the types it would need are
//! unconstructible outside this crate.
//!
//! ```text
//!             ┌─ Node (pure; the FFI-bindable facade) ────────────┐
//!  NodeEvent ─▶ route to target machine                           │
//!             │   ConnMachine[conn] ◀─Sealed─▶ CoreMachine        │
//!             │   (internal traffic pumped to quiescence          │
//!             │    within the same handle() turn — deterministic) │
//!             └─▶ NodeEffect (leaf only) ─────────────────────────┘
//! ```
//!
//! The node is still sans-io: composition adds routing, not IO. One
//! `handle` turn is a pure function of `(now, event, state)`, so
//! whole-node property tests and the deterministic simulator work at
//! this boundary.

use alloc::{collections::VecDeque, vec::Vec};

use crate::{
    blob_ref::{FrameId, Part},
    command::Command,
    conn_machine::{ConnAppEvent, ConnConfig, ConnEffect, ConnEvent, ConnMachine},
    core_machine::{CoreConfig, CoreEffect, CoreEvent, CoreMachine},
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::{ConnId, Generation},
    outcome::{IgnoreReason, Outcome},
    storage::{StorageOp, StorageResult},
    ticket::{CryptoTicket, Entity, StorageTicket},
    timestamp::{Now, Timestamp},
};

use sedimentree_core::collections::Map;

/// Node configuration (fans out into per-machine configs).
// Not `Copy`: will grow policy hooks.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Our identity (verifying-key bytes; the signing key stays with the
    /// driver).
    pub local_peer: crate::peer_id::PeerId,

    /// Discovery audience accepted as a responder, if any.
    pub discovery: Option<Audience>,

    /// Root entropy; per-connection entropy is derived deterministically
    /// (journal/replay-friendly).
    pub entropy: [u8; 32],

    /// Maximum unacked subscription pushes per connection before that
    /// subscriber is paused.
    pub max_outstanding_pushes: u32,
}

impl NodeConfig {
    /// Defaults for everything but identity and entropy.
    #[must_use]
    pub const fn new(local_peer: crate::peer_id::PeerId, entropy: [u8; 32]) -> Self {
        Self {
            local_peer,
            discovery: None,
            entropy,
            max_outstanding_pushes: 64,
        }
    }
}

/// Everything the driver tells the node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum NodeEvent {
    /// A transport connection is up. `conn` is driver-allocated, never
    /// reused.
    Connected {
        /// The new connection.
        conn: ConnId,
        /// Who initiated it.
        direction: Direction,
        /// Who we are dialing (required outbound; pins `Known`).
        audience: Option<Audience>,
    },

    /// A transport connection is gone.
    Disconnected {
        /// The closed connection.
        conn: ConnId,
    },

    /// One complete wire message arrived (frame retained by the driver).
    MessageReceived {
        /// The receiving connection.
        conn: ConnId,
        /// The retained frame's id.
        frame: FrameId,
        /// The frame bytes.
        bytes: Vec<u8>,
    },

    /// A signing completion.
    SignDone {
        /// The witness from the issuing effect.
        ticket: CryptoTicket,
        /// The signature.
        signature: [u8; 64],
    },

    /// A storage completion.
    StorageDone {
        /// The witness from the issuing effect.
        ticket: StorageTicket,
        /// The result.
        result: StorageResult,
    },

    /// A local application request.
    Command(Command),

    /// Timer service.
    Wake,
}

/// Everything the node asks of the driver — leaf effects only; sealed
/// inter-machine traffic never appears here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeEffect {
    /// Send one wire message (scatter-gather parts).
    Send {
        /// The connection to send on.
        conn: ConnId,
        /// The message parts.
        parts: Vec<Part>,
    },

    /// Close a connection.
    Disconnect {
        /// The condemned connection.
        conn: ConnId,
    },

    /// Sign with the node's identity key (external custody).
    Sign {
        /// Completion witness.
        ticket: CryptoTicket,
        /// Bytes to sign.
        payload: Vec<u8>,
    },

    /// A storage operation.
    Storage {
        /// Completion witness.
        ticket: StorageTicket,
        /// The operation.
        op: StorageOp,
    },

    /// A frame minted no escaping refs; the driver may free it.
    ReleaseFrame(FrameId),

    /// A blob ref left node state; decrement its retention.
    ReleaseBlob(crate::blob_ref::BlobRef),

    /// An application-facing event.
    App(AppEvent),
}

/// The composed node. See the [module docs](self).
#[derive(Debug)]
pub struct Node {
    config: NodeConfig,
    core: CoreMachine,
    conns: Map<ConnId, ConnMachine>,
    effects: VecDeque<NodeEffect>,
    /// Monotonic high-water mark: the machines never see time move
    /// backwards, even if a driver's clock does (suspend/resume, NTP
    /// slew on a misconfigured monotonic source).
    high_water: Timestamp,
}

impl Node {
    /// Create a node.
    #[must_use]
    pub fn new(config: NodeConfig) -> Self {
        let mut core_config = CoreConfig::new(config.local_peer, config.entropy);
        core_config.max_outstanding_pushes = config.max_outstanding_pushes;
        let core = CoreMachine::new(core_config);
        Self {
            config,
            core,
            conns: Map::new(),
            effects: VecDeque::new(),
            high_water: Timestamp::from_millis(0),
        }
    }

    /// Feed one event; internal edge traffic is routed to quiescence
    /// within this turn. Drain [`poll_effect`](Self::poll_effect) after.
    pub fn handle(&mut self, now: Now, event: NodeEvent) -> Outcome {
        // Time discipline: clamp to the high-water mark. A regressed
        // driver clock must never re-arm deadlines in the past or
        // un-expire state.
        self.high_water = self.high_water.max(now.monotonic);
        let now = Now {
            monotonic: self.high_water,
            wall: now.wall,
        };
        match event {
            NodeEvent::Connected {
                conn,
                direction,
                audience,
            } => self.on_connected(now, conn, direction, audience),
            NodeEvent::Disconnected { conn } => {
                let Some(machine) = self.conns.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                let outcome = machine.handle(now, ConnEvent::TransportClosed);
                self.pump(now, conn);
                let _machine = self.conns.remove(&conn);
                outcome
            }
            NodeEvent::MessageReceived { conn, frame, bytes } => {
                let Some(machine) = self.conns.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                let outcome = machine.handle(now, ConnEvent::MessageReceived { frame, bytes });
                self.pump(now, conn);
                outcome
            }
            NodeEvent::SignDone { ticket, signature } => {
                let Entity::Connection(conn) = ticket.entity else {
                    return Outcome::Ignored(IgnoreReason::UnknownTicket);
                };
                let Some(machine) = self.conns.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                let outcome = machine.handle(now, ConnEvent::SignDone { ticket, signature });
                self.pump(now, conn);
                outcome
            }
            NodeEvent::StorageDone { ticket, result } => {
                let outcome = self
                    .core
                    .handle(now, CoreEvent::StorageDone { ticket, result });
                self.pump_core(now);
                outcome
            }
            NodeEvent::Command(command) => self.on_command(now, command),
            NodeEvent::Wake => self.on_wake(now),
        }
    }

    /// Next queued leaf effect.
    pub fn poll_effect(&mut self) -> Option<NodeEffect> {
        self.effects.pop_front()
    }

    /// A snapshot of the core machine's telemetry counters.
    #[must_use]
    pub const fn stats(&self) -> crate::stats::Stats {
        self.core.stats()
    }

    /// The earliest deadline across all machines.
    #[must_use]
    pub fn poll_timeout(&self) -> Option<Timestamp> {
        let conns = self.conns.values().filter_map(ConnMachine::poll_timeout);
        conns.chain(self.core.poll_timeout()).min()
    }

    /// Read access: resident tree heads (via the core).
    pub fn tree_heads(
        &mut self,
        tree: sedimentree_core::id::SedimentreeId,
    ) -> Option<Vec<sedimentree_core::loose_commit::id::CommitId>> {
        self.core.tree_heads(tree)
    }

    // ── routing ────────────────────────────────────────────────────

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
        let mut conn_config = ConnConfig::new(self.config.local_peer, self.conn_entropy(conn));
        conn_config.discovery = self.config.discovery;
        let machine = ConnMachine::new(
            conn_config,
            conn,
            Generation::FIRST,
            direction,
            audience,
            now,
        );
        self.conns.insert(conn, machine);
        self.pump(now, conn);
        Outcome::Progressed
    }

    fn on_command(&mut self, now: Now, command: Command) -> Outcome {
        match command {
            // Extension sends belong to the connection machine.
            Command::SendExtension { conn, bytes } => {
                let Some(machine) = self.conns.get_mut(&conn) else {
                    return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
                };
                let outcome = machine.handle(now, ConnEvent::SendExtension { bytes });
                self.pump(now, conn);
                outcome
            }
            // Everything else is the core's.
            Command::HydrateTree { .. }
            | Command::AddCommits { .. }
            | Command::AddFragments { .. }
            | Command::RemoveTree { .. }
            | Command::Unsubscribe { .. }
            | Command::SyncTree { .. } => {
                let outcome = self.core.handle(now, CoreEvent::Command(command));
                self.pump_core(now);
                outcome
            }
        }
    }

    fn on_wake(&mut self, now: Now) -> Outcome {
        let mut progressed = false;
        let conns: Vec<ConnId> = self.conns.keys().copied().collect();
        for conn in conns {
            if let Some(machine) = self.conns.get_mut(&conn)
                && machine
                    .poll_timeout()
                    .is_some_and(|t| t.is_due(now.monotonic))
            {
                let outcome = machine.handle(now, ConnEvent::Wake);
                progressed |= !matches!(outcome, Outcome::Idle);
                self.pump(now, conn);
            }
        }
        let outcome = self.core.handle(now, CoreEvent::Wake);
        progressed |= !matches!(outcome, Outcome::Idle);
        self.pump_core(now);
        if progressed {
            Outcome::Progressed
        } else {
            Outcome::Idle
        }
    }

    /// Drain one connection machine, routing sealed traffic to the core
    /// (and pumping the core in turn) until quiescent.
    ///
    /// Termination: every hop strictly consumes one queued effect, and a
    /// machine only queues effects while handling an event — there is no
    /// self-feeding source. A conn↔core ping-pong therefore shrinks the
    /// combined queue unless the protocol itself generates unbounded
    /// traffic from one input, which would be a machine bug — caught by
    /// the debug tripwire below rather than silently truncated.
    fn pump(&mut self, now: Now, conn: ConnId) {
        #[cfg(debug_assertions)]
        let mut hops: u64 = 0;
        loop {
            #[cfg(debug_assertions)]
            {
                hops += 1;
                debug_assert!(
                    hops < 65_536,
                    "pump livelock: conn machine generates unbounded effects"
                );
            }
            let Some(machine) = self.conns.get_mut(&conn) else {
                return;
            };
            let Some(effect) = machine.poll_effect() else {
                return;
            };
            match effect {
                ConnEffect::Send { parts } => {
                    self.effects.push_back(NodeEffect::Send { conn, parts });
                }
                ConnEffect::Disconnect => {
                    self.effects.push_back(NodeEffect::Disconnect { conn });
                }
                ConnEffect::Sign { ticket, payload } => {
                    self.effects.push_back(NodeEffect::Sign { ticket, payload });
                }
                ConnEffect::ReleaseFrame(frame) => {
                    self.effects.push_back(NodeEffect::ReleaseFrame(frame));
                }
                ConnEffect::App(event) => {
                    let app = match event {
                        ConnAppEvent::PeerAuthenticated { peer } => {
                            AppEvent::PeerAuthenticated { conn, peer }
                        }
                        ConnAppEvent::ExtensionMessage { peer, bytes } => {
                            AppEvent::ExtensionMessage { conn, peer, bytes }
                        }
                    };
                    self.effects.push_back(NodeEffect::App(app));
                }
                ConnEffect::ToCore(sealed) => {
                    let _outcome = self.core.handle(now, CoreEvent::FromConn(sealed));
                    self.pump_core(now);
                }
            }
        }
    }

    /// Drain the core, routing sealed answers back to connection
    /// machines (and pumping them in turn) until quiescent.
    ///
    /// Termination: see [`pump`](Self::pump) — same argument, same
    /// debug tripwire.
    fn pump_core(&mut self, now: Now) {
        #[cfg(debug_assertions)]
        let mut hops: u64 = 0;
        loop {
            #[cfg(debug_assertions)]
            {
                hops += 1;
                debug_assert!(
                    hops < 65_536,
                    "pump livelock: core machine generates unbounded effects"
                );
            }
            let Some(effect) = self.core.poll_effect() else {
                return;
            };
            match effect {
                CoreEffect::Send { conn, parts } => {
                    self.effects.push_back(NodeEffect::Send { conn, parts });
                }
                CoreEffect::Disconnect { conn } => {
                    self.effects.push_back(NodeEffect::Disconnect { conn });
                }
                CoreEffect::Storage { ticket, op } => {
                    self.effects.push_back(NodeEffect::Storage { ticket, op });
                }
                CoreEffect::ReleaseBlob(blob) => {
                    self.effects.push_back(NodeEffect::ReleaseBlob(blob));
                }
                CoreEffect::App(event) => {
                    self.effects.push_back(NodeEffect::App(event));
                }
                CoreEffect::ToConn(sealed) => {
                    let conn = sealed.edge().conn;
                    if let Some(machine) = self.conns.get_mut(&conn) {
                        let _outcome = machine.handle(now, ConnEvent::FromCore(sealed));
                        self.pump(now, conn);
                    }
                }
            }
        }
    }

    /// Deterministic per-connection entropy (journal/replay-friendly).
    fn conn_entropy(&self, conn: ConnId) -> [u8; 32] {
        *blake3::keyed_hash(&self.config.entropy, &conn.as_u64().to_be_bytes()).as_bytes()
    }
}
