//! The actor funnel: one task owns the [`Node`], everything else talks to
//! it through channels.
//!
//! ```text
//!  read-loop futures ─┐  inputs   ┌────────────────┐  effects
//!  Handle (ambient)  ─┼─────────▶ │ Driver::run     │ ─────────▶ transports
//!  Connection (cap)  ─┘ (channel) │ (&mut Node)     │            storage
//!                                 └────────────────┘            signer
//!                                         │ app events (channel)
//!                                         ▼
//!                                    application
//! ```
//!
//! No locks anywhere: the driver task has exclusive `&mut` access to the
//! node, the frame table, and the connection registry. Completions
//! (signing, storage) are awaited in the driver task and fed straight
//! back in — correct on every platform; offloading them to workers is a
//! driver-internal optimization that can land later without touching the
//! protocol.
//!
//! Scheduling stays with the caller: [`Handle::connect`] returns the
//! connection's read-loop future for the _application_ to spawn on its
//! runtime. The driver never spawns tasks.
//!
//! # Capability-shaped connection authority
//!
//! The API splits authority in two. [`Handle`] carries only _ambient_
//! (tree-local) operations: local writes, hydration, queries, app
//! events. Everything conn-scoped — sync, unsubscribe, extension sends,
//! disconnect — lives on the [`Connection`] capability, minted only by
//! completing a handshake:
//!
//! ```text
//! Handle::connect ──▶ PendingConnection ── authenticated().await ──▶ Connection
//!                     (no conn-scoped ops                            │ sync_tree
//!                      exist here)                                   │ send_extension
//!                                                                    │ unsubscribe
//!                                                                    ▼ disconnect(self)
//! ```
//!
//! Three misuses become unrepresentable rather than merely tolerated:
//! fabricated connection ids (no public constructor), operations on an
//! unauthenticated connection (typestate), and cross-driver confusion
//! (the capability routes through its own channel). Liveness stays a
//! dynamic property — a `Connection` whose peer died degrades to no-ops
//! and an [`AppEvent::ConnectionClosed`], exactly as before. The plain
//! `ConnId` vocabulary underneath is unchanged: this layer is Rust
//! sugar; FFI bindings keep speaking the frozen L1 surface.

use core::time::Duration;

use async_channel::{Receiver, Sender};
use future_form::FutureForm;
use futures::{
    channel::oneshot,
    future::{self, Either},
    pin_mut,
};
use sedimentree_core::{
    blob::BlobMeta,
    collections::Map,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::{signed::Signed, signer::Signer};
use subduction_protocol::{
    blob_ref::{BlobRef, Part},
    command::{Command, NewCommit, NewFragment},
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
    node::{Node, NodeConfig, NodeEffect, NodeEvent},
    peer_id::PeerId,
    storage::{Provenance, StorageFailure, StorageOp, StorageResult},
    ticket::{Entity, StorageTicket},
};
use thiserror::Error;

use crate::{
    clock::Clock,
    frames::FrameTable,
    storage::{Policy, Storage, StorageAction, Verdict},
    transport::Transport,
};

/// How a connection's handshake concluded.
enum AuthOutcome {
    /// The peer authenticated.
    Authenticated { peer: PeerId },

    /// The connection died before authenticating.
    Closed,
}

/// Everything that can arrive at the driver task.
enum Input<T> {
    /// Register a transport as a new connection.
    Connect {
        transport: T,
        direction: Direction,
        audience: Option<Audience>,
        reply: oneshot::Sender<ConnId>,
        auth: oneshot::Sender<AuthOutcome>,
    },

    /// One complete inbound message (from a read loop).
    Inbound { conn: ConnId, bytes: Vec<u8> },

    /// A read loop ended: the transport is gone.
    ConnClosed { conn: ConnId },

    /// A [`Connection`] capability asked to close its connection.
    Disconnect { conn: ConnId },

    /// A local application command.
    Command(Command),

    /// Query: a resident tree's heads.
    TreeHeads {
        tree: SedimentreeId,
        reply: oneshot::Sender<Option<Vec<CommitId>>>,
    },

    /// Stop the driver.
    Shutdown,
}

/// The driver was shut down or dropped; the operation cannot complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("driver closed")]
pub struct DriverClosed;

/// The handshake did not produce an authenticated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConnectError {
    /// The connection closed (transport death, handshake fault, or
    /// timeout) before authenticating.
    #[error("connection closed before authenticating")]
    Closed,

    /// The driver was shut down.
    #[error(transparent)]
    Driver(#[from] DriverClosed),
}

/// A connection whose handshake is still in flight.
///
/// The only way to reach conn-scoped operations is
/// [`authenticated`](Self::authenticated): sync and extension traffic on
/// an unauthenticated connection is unrepresentable at this surface.
#[derive(Debug)]
pub struct PendingConnection<T> {
    id: ConnId,
    tx: Sender<Input<T>>,
    outcome: oneshot::Receiver<AuthOutcome>,
}

impl<T> PendingConnection<T> {
    /// Wait for the handshake to conclude, upgrading to an
    /// authenticated [`Connection`] capability.
    ///
    /// # Errors
    ///
    /// [`ConnectError::Closed`] if the connection died first;
    /// [`ConnectError::Driver`] if the driver stopped.
    pub async fn authenticated(self) -> Result<Connection<T>, ConnectError> {
        match self.outcome.await {
            Ok(AuthOutcome::Authenticated { peer }) => Ok(Connection {
                id: self.id,
                peer,
                tx: self.tx,
            }),
            Ok(AuthOutcome::Closed) => Err(ConnectError::Closed),
            Err(_canceled) => Err(ConnectError::Driver(DriverClosed)),
        }
    }
}

/// An authenticated-connection capability: the witness that a handshake
/// completed, and the only source of conn-scoped operations.
///
/// Unforgeable — minted solely by
/// [`PendingConnection::authenticated`] — and self-routing: it carries
/// its own channel to the driver that created it, so "wrong driver"
/// misuse is unrepresentable rather than merely tolerated. Cloning is
/// delegation: handing a clone to a subsystem grants it this connection.
#[derive(Debug)]
pub struct Connection<T> {
    id: ConnId,
    peer: PeerId,
    tx: Sender<Input<T>>,
}

impl<T> Clone for Connection<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            peer: self.peer,
            tx: self.tx.clone(),
        }
    }
}

impl<T> Connection<T> {
    /// The verified identity of the peer on the far end (pinned at
    /// handshake time).
    #[must_use]
    pub const fn peer(&self) -> PeerId {
        self.peer
    }

    /// The raw connection id — for correlating with [`AppEvent`]s and
    /// telemetry, which speak the plain FFI vocabulary. Deliberately
    /// one-way: there is no `from_raw`.
    #[must_use]
    pub const fn id(&self) -> ConnId {
        self.id
    }

    /// Batch-sync `tree` with this peer. Concludes with
    /// [`AppEvent::SyncFinished`]; incoming data additionally surfaces
    /// as [`AppEvent::TreeUpdated`].
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn sync_tree(
        &self,
        tree: SedimentreeId,
        subscribe: bool,
    ) -> Result<(), DriverClosed> {
        self.command(Command::SyncTree {
            conn: self.id,
            tree,
            subscribe,
        })
        .await
    }

    /// Stop receiving pushes for `trees` from this peer.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn unsubscribe(&self, trees: Vec<SedimentreeId>) -> Result<(), DriverClosed> {
        self.command(Command::Unsubscribe {
            conn: self.id,
            trees,
        })
        .await
    }

    /// Send one extension-protocol message (schema prefix included).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn send_extension(&self, bytes: Vec<u8>) -> Result<(), DriverClosed> {
        self.command(Command::SendExtension {
            conn: self.id,
            bytes,
        })
        .await
    }

    /// Close this connection. Consumes the capability: no operations on
    /// a connection you asked to tear down.
    pub async fn disconnect(self) {
        let _result = self.tx.send(Input::Disconnect { conn: self.id }).await;
    }

    /// Conn-bearing commands are built here and only here.
    async fn command(&self, command: Command) -> Result<(), DriverClosed> {
        self.tx
            .send(Input::Command(command))
            .await
            .map_err(|_| DriverClosed)
    }
}

/// A clonable handle for talking to a running [`Driver`].
///
/// Carries only _ambient_ (tree-local) authority; conn-scoped
/// operations require the [`Connection`] capability minted by
/// [`connect`](Self::connect).
#[derive(Debug)]
pub struct Handle<T> {
    tx: Sender<Input<T>>,
    app_rx: Receiver<AppEvent>,
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            app_rx: self.app_rx.clone(),
        }
    }
}

impl<T> Handle<T> {
    /// Register `transport` as a new connection.
    ///
    /// Returns the [`PendingConnection`] (upgrade it via
    /// [`PendingConnection::authenticated`]) and the connection's
    /// read-loop future, which the caller must spawn (or otherwise poll)
    /// on its own runtime — the driver never schedules tasks. The read
    /// loop feeds inbound messages to the driver and reports transport
    /// death.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn connect<Async>(
        &self,
        transport: T,
        direction: Direction,
        audience: Option<Audience>,
    ) -> Result<
        (
            PendingConnection<T>,
            impl Future<Output = ()> + use<Async, T>,
        ),
        DriverClosed,
    >
    where
        Async: FutureForm,
        T: Transport<Async>,
    {
        let (reply, response) = oneshot::channel();
        let (auth, outcome) = oneshot::channel();
        self.tx
            .send(Input::Connect {
                transport: transport.clone(),
                direction,
                audience,
                reply,
                auth,
            })
            .await
            .map_err(|_| DriverClosed)?;
        let conn = response.await.map_err(|_| DriverClosed)?;
        let pump = read_loop::<Async, T>(transport, conn, self.tx.clone());
        Ok((
            PendingConnection {
                id: conn,
                tx: self.tx.clone(),
                outcome,
            },
            pump,
        ))
    }

    /// Author new commits locally (sealed + persisted by the driver;
    /// [`AppEvent::CommitsStored`] confirms durability).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn add_commits(
        &self,
        tree: SedimentreeId,
        commits: Vec<NewCommit>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::AddCommits { tree, commits }).await
    }

    /// Author new fragments locally — the fragment twin of
    /// [`add_commits`](Self::add_commits).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn add_fragments(
        &self,
        tree: SedimentreeId,
        fragments: Vec<NewFragment>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::AddFragments { tree, fragments })
            .await
    }

    /// Install a tree's metadata loaded from storage at startup.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn hydrate_tree(
        &self,
        tree: SedimentreeId,
        commits: Vec<LooseCommit>,
        fragments: Vec<Fragment>,
    ) -> Result<(), DriverClosed> {
        self.command(Command::HydrateTree {
            tree,
            commits,
            fragments,
        })
        .await
    }

    /// Remove a tree locally ([`AppEvent::TreeRemoved`] confirms).
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn remove_tree(&self, tree: SedimentreeId) -> Result<(), DriverClosed> {
        self.command(Command::RemoveTree { tree }).await
    }

    /// Tree-local commands only: conn-bearing commands are minted by
    /// [`Connection`], keeping connection authority capability-shaped.
    async fn command(&self, command: Command) -> Result<(), DriverClosed> {
        self.tx
            .send(Input::Command(command))
            .await
            .map_err(|_| DriverClosed)
    }

    /// A resident tree's current heads, or `None` if the tree is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped.
    pub async fn tree_heads(
        &self,
        tree: SedimentreeId,
    ) -> Result<Option<Vec<CommitId>>, DriverClosed> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Input::TreeHeads { tree, reply })
            .await
            .map_err(|_| DriverClosed)?;
        response.await.map_err(|_| DriverClosed)
    }

    /// Receive the next application event.
    ///
    /// Events are distributed, not broadcast: with multiple handles
    /// receiving concurrently, each event goes to exactly one of them.
    ///
    /// # Errors
    ///
    /// Returns [`DriverClosed`] if the driver has stopped and the event
    /// queue is drained.
    pub async fn next_app_event(&self) -> Result<AppEvent, DriverClosed> {
        self.app_rx.recv().await.map_err(|_| DriverClosed)
    }

    /// Stop the driver. Idempotent; pending operations are dropped.
    pub async fn shutdown(&self) {
        let _result = self.tx.send(Input::Shutdown).await;
    }
}

/// Pump one transport's inbound messages into the driver until the
/// transport or the driver goes away.
async fn read_loop<Async, T>(transport: T, conn: ConnId, tx: Sender<Input<T>>)
where
    Async: FutureForm,
    T: Transport<Async>,
{
    while let Ok(Some(bytes)) = transport.recv_bytes().await {
        if tx.send(Input::Inbound { conn, bytes }).await.is_err() {
            return;
        }
    }
    let _result = tx.send(Input::ConnClosed { conn }).await;
}

/// The driver: owns the node and executes its effects. See the
/// [module docs](self).
#[derive(Debug)]
pub struct Driver<Async, T, S, P, Sg, C> {
    node: Node,
    clock: C,
    signer: Sg,
    storage: S,
    policy: P,
    frames: FrameTable,
    conns: Map<ConnId, T>,
    /// Handshake-outcome notifiers for connections whose
    /// [`PendingConnection`] is still waiting.
    pending_auth: Map<ConnId, oneshot::Sender<AuthOutcome>>,
    next_conn: u64,
    rx: Receiver<Input<T>>,
    app_tx: Sender<AppEvent>,
    _form: core::marker::PhantomData<Async>,
}

/// How many queued inputs the driver accepts before senders wait — the
/// single backpressure point.
const INPUT_QUEUE_DEPTH: usize = 1024;

impl<Async, T, S, P, Sg, C> Driver<Async, T, S, P, Sg, C>
where
    Async: FutureForm,
    T: Transport<Async>,
    S: Storage<Async>,
    P: Policy<Async>,
    Sg: Signer<Async>,
    C: Clock<Async>,
{
    /// Build a driver and its handle.
    pub fn new(
        config: NodeConfig,
        clock: C,
        signer: Sg,
        storage: S,
        policy: P,
    ) -> (Self, Handle<T>) {
        let (tx, rx) = async_channel::bounded(INPUT_QUEUE_DEPTH);
        let (app_tx, app_rx) = async_channel::unbounded();
        let driver = Self {
            node: Node::new(config),
            clock,
            signer,
            storage,
            policy,
            frames: FrameTable::new(),
            conns: Map::new(),
            pending_auth: Map::new(),
            next_conn: 1,
            rx,
            app_tx,
            _form: core::marker::PhantomData,
        };
        (driver, Handle { tx, app_rx })
    }

    /// Run until [`Handle::shutdown`] or every handle and read loop is
    /// gone.
    pub async fn run(mut self) {
        loop {
            let input = match self.node.poll_timeout() {
                Some(deadline) => {
                    let now = self.clock.now();
                    let wait = Duration::from_millis(
                        deadline
                            .as_millis()
                            .saturating_sub(now.monotonic.as_millis()),
                    );
                    let sleep = self.clock.sleep(wait);
                    let recv = self.rx.recv();
                    pin_mut!(sleep, recv);
                    match future::select(recv, sleep).await {
                        Either::Left((Ok(input), _)) => Some(input),
                        Either::Left((Err(_closed), _)) => return,
                        Either::Right(((), _)) => None,
                    }
                }
                None => match self.rx.recv().await {
                    Ok(input) => Some(input),
                    Err(_closed) => return,
                },
            };

            let now = self.clock.now();
            match input {
                None => {
                    let _outcome = self.node.handle(now, NodeEvent::Wake);
                }
                Some(Input::Shutdown) => return,
                Some(Input::Connect {
                    transport,
                    direction,
                    audience,
                    reply,
                    auth,
                }) => {
                    let conn = ConnId::new(self.next_conn);
                    self.next_conn += 1;
                    let _previous = self.conns.insert(conn, transport);
                    let _pending = self.pending_auth.insert(conn, auth);
                    let _outcome = self.node.handle(
                        now,
                        NodeEvent::Connected {
                            conn,
                            direction,
                            audience,
                        },
                    );
                    let _receiver = reply.send(conn);
                }
                Some(Input::Inbound { conn, bytes }) => {
                    // A message for a torn-down conn raced its death; drop.
                    if self.conns.contains_key(&conn) {
                        let frame = self.frames.retain(Some(conn), bytes.clone());
                        let _outcome = self
                            .node
                            .handle(now, NodeEvent::MessageReceived { conn, frame, bytes });
                    }
                }
                Some(Input::ConnClosed { conn } | Input::Disconnect { conn }) => {
                    self.teardown(conn).await;
                }
                Some(Input::Command(command)) => {
                    let _outcome = self.node.handle(now, NodeEvent::Command(command));
                }
                Some(Input::TreeHeads { tree, reply }) => {
                    let _receiver = reply.send(self.node.tree_heads(tree));
                }
            }

            self.drain_effects().await;
        }
    }

    /// Execute queued node effects, in emission order, feeding
    /// completions straight back in.
    async fn drain_effects(&mut self) {
        while let Some(effect) = self.node.poll_effect() {
            match effect {
                NodeEffect::Send { conn, parts } => self.send(conn, parts).await,
                NodeEffect::Disconnect { conn } => self.teardown(conn).await,
                NodeEffect::Sign { ticket, payload } => {
                    let signature = self.signer.sign(&payload).await.to_bytes();
                    let now = self.clock.now();
                    let _outcome = self
                        .node
                        .handle(now, NodeEvent::SignDone { ticket, signature });
                }
                NodeEffect::Storage { ticket, op } => {
                    let result = self.execute_storage(&ticket, op).await;
                    let now = self.clock.now();
                    let _outcome = self
                        .node
                        .handle(now, NodeEvent::StorageDone { ticket, result });
                }
                NodeEffect::ReleaseFrame(frame) => self.frames.release_frame(frame),
                NodeEffect::ReleaseBlob(blob) => self.frames.release_blob(blob),
                NodeEffect::App(event) => {
                    // Resolve the waiting connection capability, if any.
                    if let AppEvent::PeerAuthenticated { conn, peer } = &event
                        && let Some(auth) = self.pending_auth.remove(conn)
                    {
                        let _receiver = auth.send(AuthOutcome::Authenticated { peer: *peer });
                    }
                    // Unbounded and receiver-optional: app events must
                    // never wedge the protocol.
                    let _result = self.app_tx.try_send(event);
                }
            }
        }
    }

    /// Assemble scatter-gather parts and send. A ref into a swept frame
    /// means the source connection died mid-flight: the message is
    /// dropped (its destination is being torn down with it).
    async fn send(&mut self, conn: ConnId, parts: Vec<Part>) {
        let Some(transport) = self.conns.get(&conn).cloned() else {
            return;
        };
        let mut bytes = Vec::new();
        for part in &parts {
            match part {
                Part::Bytes(b) => bytes.extend_from_slice(b),
                Part::Ref(r) => match self.frames.resolve(*r) {
                    Some(blob) => bytes.extend_from_slice(blob),
                    None => return,
                },
            }
        }
        if transport.send_bytes(bytes).await.is_err() {
            self.teardown(conn).await;
        }
    }

    /// Tear a connection down: close the transport, epoch-sweep its
    /// frames, and tell the node. Idempotent.
    async fn teardown(&mut self, conn: ConnId) {
        let Some(transport) = self.conns.remove(&conn) else {
            return;
        };
        // A capability still waiting on the handshake learns it died.
        if let Some(auth) = self.pending_auth.remove(&conn) {
            let _receiver = auth.send(AuthOutcome::Closed);
        }
        transport.disconnect().await;
        self.frames.sweep_conn(conn);
        let now = self.clock.now();
        let _outcome = self.node.handle(now, NodeEvent::Disconnected { conn });
    }

    /// Execute one storage op: authorize, resolve refs, run the backend.
    #[allow(clippy::too_many_lines)] // one arm per op variant; splitting obscures the 1:1 shape
    async fn execute_storage(&mut self, ticket: &StorageTicket, op: StorageOp) -> StorageResult {
        match op {
            StorageOp::DeleteTree { tree, provenance } => {
                if self.deny(&provenance, tree, StorageAction::Delete).await {
                    return StorageResult::Unauthorized;
                }
                match self.storage.delete_tree(tree).await {
                    Ok(()) => StorageResult::TreeDeleted,
                    Err(failure) => StorageResult::Failed(failure),
                }
            }

            StorageOp::PersistItems {
                tree,
                provenance,
                commits,
                fragments,
            } => {
                if self.deny(&provenance, tree, StorageAction::Write).await {
                    return StorageResult::Unauthorized;
                }
                let Some(commits) = self.take_blobs(commits) else {
                    return StorageResult::Failed(StorageFailure::Retryable);
                };
                let Some(fragments) = self.take_blobs(fragments) else {
                    return StorageResult::Failed(StorageFailure::Retryable);
                };
                match self.storage.persist_items(tree, commits, fragments).await {
                    Ok(stored) => StorageResult::Persisted { stored },
                    Err(failure) => StorageResult::Failed(failure),
                }
            }

            StorageOp::FetchItemRefs {
                tree,
                provenance,
                commit_ids,
                fragment_heads,
            } => {
                if self.deny(&provenance, tree, StorageAction::Read).await {
                    return StorageResult::Unauthorized;
                }
                let owner = match ticket.entity {
                    Entity::Connection(conn) => Some(conn),
                    Entity::Local => None,
                };
                match self
                    .storage
                    .fetch_items(tree, commit_ids, fragment_heads)
                    .await
                {
                    Ok(Some(items)) => StorageResult::FetchedRefs {
                        commits: self.mint_refs(owner, items.commits),
                        fragments: self.mint_refs(owner, items.fragments),
                    },
                    Ok(None) => StorageResult::UnknownTree,
                    Err(failure) => StorageResult::Failed(failure),
                }
            }

            StorageOp::IngestLocal {
                tree,
                commits,
                fragments,
            } => {
                let mut sealed_commits = Vec::with_capacity(commits.len());
                let mut commit_writes = Vec::with_capacity(commits.len());
                for new in commits {
                    let commit =
                        LooseCommit::new(tree, new.head, new.parents, BlobMeta::new(&new.blob));
                    let signed = Signed::seal::<Async, Sg>(&self.signer, commit)
                        .await
                        .into_signed();
                    sealed_commits.push(signed.clone());
                    commit_writes.push((signed, new.blob.as_slice().to_vec()));
                }
                let mut sealed_fragments = Vec::with_capacity(fragments.len());
                let mut fragment_writes = Vec::with_capacity(fragments.len());
                for new in fragments {
                    let fragment = Fragment::new(
                        tree,
                        new.head,
                        new.boundary,
                        &new.checkpoints,
                        BlobMeta::new(&new.blob),
                    );
                    let signed = Signed::seal::<Async, Sg>(&self.signer, fragment)
                        .await
                        .into_signed();
                    sealed_fragments.push(signed.clone());
                    fragment_writes.push((signed, new.blob.as_slice().to_vec()));
                }

                // Persist before answering: resident state never gets
                // ahead of durability.
                match self
                    .storage
                    .persist_items(tree, commit_writes, fragment_writes)
                    .await
                {
                    Ok(_stored) => StorageResult::LocallyIngested {
                        commits: sealed_commits,
                        fragments: sealed_fragments,
                    },
                    Err(failure) => StorageResult::Failed(failure),
                }
            }
        }
    }

    /// Whether policy denies `action`.
    async fn deny(
        &self,
        provenance: &Provenance,
        tree: SedimentreeId,
        action: StorageAction,
    ) -> bool {
        self.policy.authorize(provenance, tree, action).await == Verdict::Deny
    }

    /// Resolve each item's blob ref to bytes (noting the escapes), or
    /// `None` if any ref is gone (its frame was epoch-swept).
    fn take_blobs<I>(&mut self, items: Vec<(I, BlobRef)>) -> Option<Vec<(I, Vec<u8>)>> {
        let mut resolved = Vec::with_capacity(items.len());
        for (item, r) in items {
            self.frames.note_escape(r);
            resolved.push((item, self.frames.resolve(r)?.to_vec()));
        }
        Some(resolved)
    }

    /// Retain fetched blobs as frames and mint escaped refs to them.
    fn mint_refs<I>(
        &mut self,
        owner: Option<ConnId>,
        items: Vec<(I, Vec<u8>)>,
    ) -> Vec<(I, BlobRef)> {
        items
            .into_iter()
            .map(|(item, bytes)| {
                let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
                let frame = self.frames.retain(owner, bytes);
                let blob = BlobRef {
                    frame,
                    offset: 0,
                    len,
                };
                self.frames.note_escape(blob);
                (item, blob)
            })
            .collect()
    }
}
