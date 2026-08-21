//! In-memory test harness for Subduction [`Node`]s.
//!
//! Wires N nodes through a fake point-to-point network with fake leaf
//! drivers (signing, storage, frame table), so full-pipeline behavior —
//! handshake, sync sessions, subscription fan-out — runs deterministically
//! in plain unit tests with no IO, no clocks, and no async runtime.
//!
//! ```text
//!   ┌ Net ──────────────────────────────────────────────┐
//!   │  TestDriver[0]      TestDriver[1]      TestDriver[n]
//!   │  ┌───────────┐      ┌───────────┐
//!   │  │ Node      │      │ Node      │   links: (i,conn) ↔ (j,conn)
//!   │  │ signer    │      │ signer    │   pump(): shuttle outboxes
//!   │  │ storage   │      │ storage   │           until quiescent
//!   │  │ frames ───┼──────┼─▶ invariants: no use-after-free,
//!   │  └───────────┘      └──── no leak at quiescence
//!   └───────────────────────────────────────────────────┘
//! ```
//!
//! The frame table is the interesting part: every retained frame and
//! every escaped [`BlobRef`] is tracked, so a machine bug in the blob
//! plane (use-after-free, double release, leaked frame) surfaces as a
//! test error with a message rather than silent corruption.

use std::collections::{BTreeMap, BTreeSet};

use ed25519_dalek::{Signer as _, SigningKey};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_protocol::{
    blob_ref::{BlobRef, FrameId, Part},
    command::{Command, NewCommit, NewFragment},
    effect::AppEvent,
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
    node::{Node, NodeConfig, NodeEffect, NodeEvent},
    peer_id::PeerId,
    storage::{StorageOp, StorageResult},
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};

/// The harness error type: invariant violations carry a message and
/// surface through `?` in tests (rather than panicking mid-pipeline).
pub type TestError = Box<dyn std::error::Error>;

/// Fallible invariant check (the harness's assert).
///
/// # Errors
/// Returns `msg` as the error when `cond` is false.
pub fn ensure(cond: bool, msg: &str) -> Result<(), TestError> {
    if cond {
        Ok(())
    } else {
        Err(msg.into())
    }
}

type CommitStore = BTreeMap<SedimentreeId, BTreeMap<CommitId, (Signed<LooseCommit>, Vec<u8>)>>;
type FragmentStore = BTreeMap<SedimentreeId, BTreeMap<CommitId, (Signed<Fragment>, Vec<u8>)>>;

/// One frame in the driver's table.
struct FrameSlot {
    bytes: Vec<u8>,
    freed: bool,
    /// Refs observed escaping into effects, minus releases.
    outstanding: i64,
    saw_refs: bool,
}

/// One node plus its fake leaf drivers (signer, storage, frame table).
pub struct TestDriver {
    /// The node under test.
    pub node: Node,
    /// Outbound wire messages: (sending conn, assembled bytes).
    pub outbox: Vec<(ConnId, Vec<u8>)>,
    /// Application events, in order.
    pub app: Vec<AppEvent>,
    /// Connections the node asked the driver to close.
    pub disconnects: Vec<ConnId>,
    /// Monotonic clock, milliseconds.
    pub clock_ms: u64,
    signing_key: SigningKey,
    signer: MemorySigner,
    frames: BTreeMap<u64, FrameSlot>,
    next_frame: u64,
    store: CommitStore,
    fragment_store: FragmentStore,
    next_conn: u64,
}

impl TestDriver {
    /// A driver whose identity key and entropy derive from `seed`.
    #[must_use]
    pub fn new(seed: u8) -> Self {
        Self::with_discovery(seed, None)
    }

    /// Like [`new`](Self::new), but accepting a discovery audience as a
    /// responder.
    #[must_use]
    pub fn with_discovery(seed: u8, discovery: Option<Audience>) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(signing_key.verifying_key());
        Self {
            node: Node::new(NodeConfig {
                local_peer,
                discovery,
                entropy: [seed ^ 0x55; 32],
            }),
            outbox: Vec::new(),
            app: Vec::new(),
            disconnects: Vec::new(),
            clock_ms: 0,
            signing_key,
            signer,
            frames: BTreeMap::new(),
            next_frame: 1,
            store: BTreeMap::new(),
            fragment_store: BTreeMap::new(),
            next_conn: 1,
        }
    }

    /// The node's peer identity.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        PeerId::from(self.signing_key.verifying_key())
    }

    /// The current fake time.
    #[must_use]
    pub const fn now(&self) -> Now {
        Now {
            monotonic: Timestamp::from_millis(self.clock_ms),
            wall: TimestampSeconds::new(1_700_000_000 + self.clock_ms / 1000),
        }
    }

    /// Advance the clock and deliver a wake.
    ///
    /// # Errors
    /// Propagates effect-execution failures.
    pub fn advance(&mut self, ms: u64) -> Result<(), TestError> {
        self.clock_ms += ms;
        self.feed(NodeEvent::Wake)
    }

    /// Allocate a fresh (never reused) connection id.
    pub const fn alloc_conn(&mut self) -> ConnId {
        let id = self.next_conn;
        self.next_conn += 1;
        ConnId::new(id)
    }

    fn retain_frame(&mut self, bytes: Vec<u8>) -> FrameId {
        let id = self.next_frame;
        self.next_frame += 1;
        let _slot = self.frames.insert(
            id,
            FrameSlot {
                bytes,
                freed: false,
                outstanding: 0,
                saw_refs: false,
            },
        );
        FrameId::new(id)
    }

    fn resolve(&self, r: BlobRef) -> Result<Vec<u8>, TestError> {
        let slot = self
            .frames
            .get(&r.frame.as_u64())
            .ok_or("ref: unknown frame")?;
        ensure(!slot.freed, "USE-AFTER-FREE: ref into freed frame")?;
        Ok(slot
            .bytes
            .get(r.offset as usize..(r.offset + r.len) as usize)
            .ok_or("ref out of bounds")?
            .to_vec())
    }

    fn note_escape(&mut self, r: BlobRef) -> Result<(), TestError> {
        let slot = self
            .frames
            .get_mut(&r.frame.as_u64())
            .ok_or("escape: unknown frame")?;
        slot.outstanding += 1;
        slot.saw_refs = true;
        Ok(())
    }

    /// Deliver one wire message on a connection (retaining its frame),
    /// then run the node's effects to quiescence.
    ///
    /// # Errors
    /// Surfaces data-plane invariant violations and driver failures.
    pub fn deliver_on(&mut self, conn: ConnId, bytes: Vec<u8>) -> Result<(), TestError> {
        let frame = self.retain_frame(bytes.clone());
        let _outcome = self.node.handle(
            self.now(),
            NodeEvent::MessageReceived { conn, frame, bytes },
        );
        self.run_effects()
    }

    /// Feed one event, then run the node's effects to quiescence.
    ///
    /// # Errors
    /// Surfaces data-plane invariant violations and driver failures.
    pub fn feed(&mut self, event: NodeEvent) -> Result<(), TestError> {
        let _outcome = self.node.handle(self.now(), event);
        self.run_effects()
    }

    fn run_effects(&mut self) -> Result<(), TestError> {
        for _ in 0..1024 {
            let Some(effect) = self.node.poll_effect() else {
                return Ok(());
            };
            match effect {
                NodeEffect::Send { conn, parts } => {
                    let mut bytes = Vec::new();
                    for part in &parts {
                        match part {
                            Part::Bytes(b) => bytes.extend_from_slice(b),
                            Part::Ref(r) => {
                                // Refs in sends were escaped earlier by the
                                // op that produced them; resolve only.
                                let blob = self.resolve(*r)?;
                                bytes.extend_from_slice(&blob);
                            }
                        }
                    }
                    self.outbox.push((conn, bytes));
                }
                NodeEffect::Disconnect { conn } => self.disconnects.push(conn),
                NodeEffect::Sign { ticket, payload } => {
                    let signature = self.signing_key.sign(&payload).to_bytes();
                    let _outcome = self
                        .node
                        .handle(self.now(), NodeEvent::SignDone { ticket, signature });
                }
                NodeEffect::ReleaseFrame(frame) => {
                    let slot = self
                        .frames
                        .get_mut(&frame.as_u64())
                        .ok_or("release: unknown frame")?;
                    ensure(!slot.freed, "double free")?;
                    ensure(!slot.saw_refs, "ReleaseFrame on a frame with escaped refs")?;
                    slot.freed = true;
                }
                NodeEffect::ReleaseBlob(r) => {
                    let slot = self
                        .frames
                        .get_mut(&r.frame.as_u64())
                        .ok_or("release-blob: unknown frame")?;
                    slot.outstanding -= 1;
                    ensure(slot.outstanding >= 0, "over-release")?;
                    if slot.outstanding == 0 {
                        slot.freed = true;
                    }
                }
                NodeEffect::App(event) => self.app.push(event),
                NodeEffect::Storage { ticket, op } => {
                    let result = self.run_storage(op)?;
                    let _outcome = self
                        .node
                        .handle(self.now(), NodeEvent::StorageDone { ticket, result });
                }
            }
        }
        Err("effect loop did not quiesce".into())
    }

    fn run_storage(&mut self, op: StorageOp) -> Result<StorageResult, TestError> {
        match op {
            StorageOp::IngestLocal {
                tree,
                commits,
                fragments,
            } => Ok(self.ingest_local(tree, &commits, &fragments)),
            StorageOp::PersistItems {
                tree,
                commits,
                fragments,
                ..
            } => self.persist_items(tree, commits, fragments),
            StorageOp::FetchItemRefs {
                tree,
                commit_ids,
                fragment_heads,
                ..
            } => self.fetch_item_refs(tree, &commit_ids, &fragment_heads),
            StorageOp::DeleteTree { tree, .. } => {
                let _commits = self.store.remove(&tree);
                let _fragments = self.fragment_store.remove(&tree);
                Ok(StorageResult::TreeDeleted)
            }
        }
    }

    fn ingest_local(
        &mut self,
        tree: SedimentreeId,
        commits: &[NewCommit],
        fragments: &[NewFragment],
    ) -> StorageResult {
        let mut sealed_commits = Vec::new();
        for new in commits {
            let commit = LooseCommit::new(
                tree,
                new.head,
                new.parents.clone(),
                BlobMeta::new(&new.blob),
            );
            let signed =
                futures::executor::block_on(Signed::seal::<Sendable, _>(&self.signer, commit))
                    .into_signed();
            let _previous = self
                .store
                .entry(tree)
                .or_default()
                .insert(new.head, (signed.clone(), new.blob.as_slice().to_vec()));
            sealed_commits.push(signed);
        }
        let mut sealed_fragments = Vec::new();
        for new in fragments {
            let fragment = Fragment::new(
                tree,
                new.head,
                new.boundary.clone(),
                &new.checkpoints,
                BlobMeta::new(&new.blob),
            );
            let signed =
                futures::executor::block_on(Signed::seal::<Sendable, _>(&self.signer, fragment))
                    .into_signed();
            let _previous = self
                .fragment_store
                .entry(tree)
                .or_default()
                .insert(new.head, (signed.clone(), new.blob.as_slice().to_vec()));
            sealed_fragments.push(signed);
        }
        StorageResult::LocallyIngested {
            commits: sealed_commits,
            fragments: sealed_fragments,
        }
    }

    fn persist_items(
        &mut self,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, BlobRef)>,
        fragments: Vec<(Signed<Fragment>, BlobRef)>,
    ) -> Result<StorageResult, TestError> {
        let mut stored = 0u32;
        for (signed, blob_ref) in commits {
            self.note_escape(blob_ref)?;
            let bytes = self.resolve(blob_ref)?;
            // Trusted: verification happened in the ConnMachine.
            let verified = signed
                .try_verify()
                .map_err(|e| format!("harness sanity: {e}"))?;
            let _previous = self
                .store
                .entry(tree)
                .or_default()
                .insert(verified.payload().head(), (signed, bytes));
            stored += 1;
        }
        for (signed, blob_ref) in fragments {
            self.note_escape(blob_ref)?;
            let bytes = self.resolve(blob_ref)?;
            let verified = signed
                .try_verify()
                .map_err(|e| format!("harness sanity: {e}"))?;
            let _previous = self
                .fragment_store
                .entry(tree)
                .or_default()
                .insert(verified.payload().head(), (signed, bytes));
            stored += 1;
        }
        Ok(StorageResult::Persisted { stored })
    }

    fn fetch_item_refs(
        &mut self,
        tree: SedimentreeId,
        commit_ids: &[CommitId],
        fragment_heads: &[CommitId],
    ) -> Result<StorageResult, TestError> {
        let commit_items: Vec<_> = commit_ids
            .iter()
            .filter_map(|id| self.store.get(&tree).and_then(|m| m.get(id)).cloned())
            .collect();
        let fragment_items: Vec<_> = fragment_heads
            .iter()
            .filter_map(|id| {
                self.fragment_store
                    .get(&tree)
                    .and_then(|m| m.get(id))
                    .cloned()
            })
            .collect();
        let mut commits = Vec::new();
        for (signed, bytes) in commit_items {
            let blob_ref = self.mint_ref(bytes)?;
            commits.push((signed, blob_ref));
        }
        let mut fragments = Vec::new();
        for (signed, bytes) in fragment_items {
            let blob_ref = self.mint_ref(bytes)?;
            fragments.push((signed, blob_ref));
        }
        Ok(StorageResult::FetchedRefs { commits, fragments })
    }

    fn mint_ref(&mut self, bytes: Vec<u8>) -> Result<BlobRef, TestError> {
        let len = u32::try_from(bytes.len())?;
        let frame = self.retain_frame(bytes);
        let blob_ref = BlobRef {
            frame,
            offset: 0,
            len,
        };
        self.note_escape(blob_ref)?;
        Ok(blob_ref)
    }

    /// Commit ids persisted for `tree`, in id order.
    #[must_use]
    pub fn stored_commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.store
            .get(&tree)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Fragment heads persisted for `tree`, in id order.
    #[must_use]
    pub fn stored_fragment_heads(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.fragment_store
            .get(&tree)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// The no-leak invariant: every frame is freed, either via
    /// `ReleaseFrame` (no refs escaped) or via its last `ReleaseBlob`.
    ///
    /// # Errors
    /// Reports the first leaked frame.
    pub fn check_no_leaks(&self) -> Result<(), TestError> {
        for (id, slot) in &self.frames {
            ensure(
                slot.freed,
                &format!(
                    "LEAK: frame {id} never freed (outstanding refs: {})",
                    slot.outstanding
                ),
            )?;
        }
        Ok(())
    }

    /// Add one local commit with `head` bytes repeated as its id/blob.
    ///
    /// # Errors
    /// Propagates effect-execution failures.
    pub fn add_commit(&mut self, tree: SedimentreeId, head: u8) -> Result<(), TestError> {
        self.feed(NodeEvent::Command(Command::AddCommits {
            tree,
            commits: vec![NewCommit {
                head: CommitId::new([head; 32]),
                parents: BTreeSet::new(),
                blob: Blob::new(vec![head; 16]),
            }],
        }))
    }

    /// Add one local fragment with `head` bytes repeated as its id/blob.
    ///
    /// # Errors
    /// Propagates effect-execution failures.
    pub fn add_fragment(&mut self, tree: SedimentreeId, head: u8) -> Result<(), TestError> {
        self.feed(NodeEvent::Command(Command::AddFragments {
            tree,
            fragments: vec![NewFragment {
                head: CommitId::new([head; 32]),
                boundary: BTreeSet::new(),
                checkpoints: Vec::new(),
                blob: Blob::new(vec![head; 24]),
            }],
        }))
    }
}

impl std::fmt::Debug for TestDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestDriver")
            .field("peer", &self.peer_id())
            .field("clock_ms", &self.clock_ms)
            .field("frames", &self.frames.len())
            .field("outbox", &self.outbox.len())
            .field("app", &self.app.len())
            .finish_non_exhaustive()
    }
}

/// An in-memory network of nodes with point-to-point links.
pub struct Net {
    drivers: Vec<TestDriver>,
    /// (node index, local conn) → (peer index, peer's conn).
    links: BTreeMap<(usize, ConnId), (usize, ConnId)>,
    /// Endpoints whose outgoing messages are silently discarded.
    dropped: BTreeSet<(usize, ConnId)>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net")
            .field("drivers", &self.drivers)
            .field("links", &self.links)
            .field("dropped", &self.dropped)
            .finish()
    }
}

impl Net {
    /// One driver per seed (see [`TestDriver::new`]).
    #[must_use]
    pub fn new(seeds: &[u8]) -> Self {
        Self::from_drivers(seeds.iter().map(|s| TestDriver::new(*s)).collect())
    }

    /// A network over pre-built drivers (custom discovery, etc.).
    #[must_use]
    pub const fn from_drivers(drivers: Vec<TestDriver>) -> Self {
        Self {
            drivers,
            links: BTreeMap::new(),
            dropped: BTreeSet::new(),
        }
    }

    /// Shared access to node `i`'s driver.
    ///
    /// # Panics
    /// Panics when `i` is out of range — harness misuse, i.e. a test bug.
    #[must_use]
    pub fn driver(&self, i: usize) -> &TestDriver {
        #[allow(clippy::indexing_slicing)] // out-of-range = test bug
        &self.drivers[i]
    }

    /// Mutable access to node `i`'s driver.
    ///
    /// # Panics
    /// Panics when `i` is out of range — harness misuse, i.e. a test bug.
    #[must_use]
    pub fn driver_mut(&mut self, i: usize) -> &mut TestDriver {
        #[allow(clippy::indexing_slicing)] // out-of-range = test bug
        &mut self.drivers[i]
    }

    /// Connect `i` (initiator) to `j` (responder) and run the handshake
    /// to completion. Returns `(i's conn, j's conn)`.
    ///
    /// # Errors
    /// Fails when the handshake does not complete.
    pub fn connect(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        let peer_j = self.driver(j).peer_id();
        let (ci, cj) = self.connect_with_audience(i, j, Audience::known(peer_j))?;
        ensure(
            self.driver(i)
                .app
                .iter()
                .any(|e| matches!(e, AppEvent::PeerAuthenticated { conn, .. } if *conn == ci)),
            "handshake must complete",
        )?;
        Ok((ci, cj))
    }

    /// Like [`connect`](Self::connect) with an explicit dialed audience,
    /// and WITHOUT asserting the handshake succeeded — rejection paths
    /// use this.
    ///
    /// # Errors
    /// Propagates pump failures.
    pub fn connect_with_audience(
        &mut self,
        i: usize,
        j: usize,
        audience: Audience,
    ) -> Result<(ConnId, ConnId), TestError> {
        let (ci, cj) = self.wire(i, j)?;
        self.driver_mut(i).feed(NodeEvent::Connected {
            conn: ci,
            direction: Direction::Outbound,
            audience: Some(audience),
        })?;
        self.driver_mut(j).feed(NodeEvent::Connected {
            conn: cj,
            direction: Direction::Inbound,
            audience: None,
        })?;
        let _messages = self.pump()?;
        Ok((ci, cj))
    }

    /// Both ends dial simultaneously over one transport link (sim-open).
    ///
    /// # Errors
    /// Propagates pump failures.
    pub fn connect_simopen(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        let (ci, cj) = self.wire(i, j)?;
        let peer_i = self.driver(i).peer_id();
        let peer_j = self.driver(j).peer_id();
        self.driver_mut(i).feed(NodeEvent::Connected {
            conn: ci,
            direction: Direction::Outbound,
            audience: Some(Audience::known(peer_j)),
        })?;
        self.driver_mut(j).feed(NodeEvent::Connected {
            conn: cj,
            direction: Direction::Outbound,
            audience: Some(Audience::known(peer_i)),
        })?;
        let _messages = self.pump()?;
        Ok((ci, cj))
    }

    fn wire(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        ensure(i != j, "cannot wire a node to itself")?;
        let ci = self.driver_mut(i).alloc_conn();
        let cj = self.driver_mut(j).alloc_conn();
        let _link = self.links.insert((i, ci), (j, cj));
        let _link = self.links.insert((j, cj), (i, ci));
        Ok((ci, cj))
    }

    /// Discard future messages sent from endpoint `(i, conn)`.
    pub fn drop_from(&mut self, i: usize, conn: ConnId) {
        let _new = self.dropped.insert((i, conn));
    }

    /// Shuttle messages until quiescence. Returns messages delivered —
    /// a bound on this is a damping assertion.
    ///
    /// # Errors
    /// Fails when the network does not quiesce (a thrash loop) or a
    /// data-plane invariant breaks during delivery.
    pub fn pump(&mut self) -> Result<usize, TestError> {
        let mut delivered = 0usize;
        for _round in 0..256 {
            let mut queue: Vec<(usize, ConnId, Vec<u8>)> = Vec::new();
            for (idx, driver) in self.drivers.iter_mut().enumerate() {
                for (conn, bytes) in driver.outbox.drain(..) {
                    if self.dropped.contains(&(idx, conn)) {
                        continue;
                    }
                    let Some((target, target_conn)) = self.links.get(&(idx, conn)) else {
                        return Err(format!("no link for node {idx} conn {conn:?}").into());
                    };
                    queue.push((*target, *target_conn, bytes));
                }
            }
            if queue.is_empty() {
                return Ok(delivered);
            }
            for (target, conn, bytes) in queue {
                self.driver_mut(target).deliver_on(conn, bytes)?;
                delivered += 1;
            }
        }
        Err("network did not quiesce (thrash loop?)".into())
    }

    /// Take a node's queued outbound messages without delivering them.
    pub fn take_outbox(&mut self, i: usize) -> Vec<(ConnId, Vec<u8>)> {
        self.driver_mut(i).outbox.drain(..).collect()
    }

    /// [`TestDriver::check_no_leaks`] across every node.
    ///
    /// # Errors
    /// Reports the first leaked frame.
    pub fn check_no_leaks(&self) -> Result<(), TestError> {
        for driver in &self.drivers {
            driver.check_no_leaks()?;
        }
        Ok(())
    }
}
