//! The full Design-D pipeline: two Nodes (`ConnMachine`s + Core + router)
//! handshake, sync, and push through subscriptions — with a driver
//! harness whose frame/blob table enforces the data-plane invariants:
//! no use-after-free (resolving a dead ref fails the test) and no leak
//! (every frame accounted for at quiescence).

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
    command::{Command, NewCommit},
    effect::{AppEvent, SyncStatus},
    event::Direction,
    handshake::audience::Audience,
    id::ConnId,
    machine::Now,
    node::{Node, NodeConfig, NodeEffect, NodeEvent},
    peer_id::PeerId,
    storage::{StorageOp, StorageResult},
    timestamp::Timestamp,
    wall_clock::TimestampSeconds,
};
use testresult::TestResult;

const CONN: ConnId = ConnId::new(1);

type TestError = Box<dyn std::error::Error>;

/// Fallible invariant check (the harness's assert).
fn ensure(cond: bool, msg: &str) -> Result<(), TestError> {
    if cond {
        Ok(())
    } else {
        Err(msg.into())
    }
}

const fn now() -> Now {
    Now {
        monotonic: Timestamp::from_millis(0),
        wall: TimestampSeconds::new(1_700_000_000),
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

/// Driver harness: signer, storage sim, frame table, mailboxes.
struct TestDriver {
    node: Node,
    signing_key: SigningKey,
    signer: MemorySigner,
    frames: BTreeMap<u64, FrameSlot>,
    next_frame: u64,
    store: CommitStore,
    fragment_store: FragmentStore,
    outbox: Vec<(ConnId, Vec<u8>)>,
    app: Vec<AppEvent>,
}

impl TestDriver {
    fn new(seed: u8) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(signing_key.verifying_key());
        Self {
            node: Node::new(NodeConfig {
                local_peer,
                discovery: None,
                entropy: [seed ^ 0x55; 32],
            }),
            signing_key,
            signer,
            frames: BTreeMap::new(),
            next_frame: 1,
            store: BTreeMap::new(),
            fragment_store: BTreeMap::new(),
            outbox: Vec::new(),
            app: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.signing_key.verifying_key())
    }

    fn retain_frame(&mut self, bytes: Vec<u8>) -> FrameId {
        let id = self.next_frame;
        self.next_frame += 1;
        self.frames.insert(
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

    fn deliver(&mut self, bytes: Vec<u8>) -> Result<(), TestError> {
        let frame = self.retain_frame(bytes.clone());
        let _outcome = self.node.handle(
            now(),
            NodeEvent::MessageReceived {
                conn: CONN,
                frame,
                bytes,
            },
        );
        self.run_effects()
    }

    fn feed(&mut self, event: NodeEvent) -> Result<(), TestError> {
        let _outcome = self.node.handle(now(), event);
        self.run_effects()
    }

    fn run_effects(&mut self) -> Result<(), TestError> {
        for _ in 0..512 {
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
                NodeEffect::Disconnect { .. } => {}
                NodeEffect::Sign { ticket, payload } => {
                    let signature = self.signing_key.sign(&payload).to_bytes();
                    let _outcome = self
                        .node
                        .handle(now(), NodeEvent::SignDone { ticket, signature });
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
                        .handle(now(), NodeEvent::StorageDone { ticket, result });
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
            } => {
                ensure(fragments.is_empty(), "fragments unused in this test")?;
                let mut sealed = Vec::new();
                for new in &commits {
                    let commit = LooseCommit::new(
                        tree,
                        new.head,
                        new.parents.clone(),
                        BlobMeta::new(&new.blob),
                    );
                    let signed = futures::executor::block_on(Signed::seal::<Sendable, _>(
                        &self.signer,
                        commit,
                    ))
                    .into_signed();
                    self.store
                        .entry(tree)
                        .or_default()
                        .insert(new.head, (signed.clone(), new.blob.as_slice().to_vec()));
                    sealed.push(signed);
                }
                Ok(StorageResult::LocallyIngested {
                    commits: sealed,
                    fragments: vec![],
                })
            }
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
            } => {
                ensure(fragment_heads.is_empty(), "fragments unused in this test")?;
                self.fetch_item_refs(tree, &commit_ids)
            }
            StorageOp::DeleteTree { tree, .. } => {
                self.store.remove(&tree);
                Ok(StorageResult::TreeDeleted)
            }
            StorageOp::Ingest { .. } | StorageOp::FetchItems { .. } => {
                Err("legacy byte-world ops are never issued by the Node".into())
            }
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
            self.store
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
            self.fragment_store
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
    ) -> Result<StorageResult, TestError> {
        let mut commits = Vec::new();
        let items: Vec<_> = commit_ids
            .iter()
            .filter_map(|id| self.store.get(&tree).and_then(|m| m.get(id)).cloned())
            .collect();
        for (signed, bytes) in items {
            let len = u32::try_from(bytes.len())?;
            let frame = self.retain_frame(bytes);
            let blob_ref = BlobRef {
                frame,
                offset: 0,
                len,
            };
            self.note_escape(blob_ref)?;
            commits.push((signed, blob_ref));
        }
        Ok(StorageResult::FetchedRefs {
            commits,
            fragments: vec![],
        })
    }

    fn stored_commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.store
            .get(&tree)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// The no-leak invariant: every frame is freed, either via
    /// `ReleaseFrame` (no refs escaped) or via its last `ReleaseBlob`.
    fn check_no_leaks(&self) -> Result<(), TestError> {
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

    fn add_commit(&mut self, tree: SedimentreeId, head: u8) -> Result<(), TestError> {
        self.feed(NodeEvent::Command(Command::AddCommits {
            tree,
            commits: vec![NewCommit {
                head: CommitId::new([head; 32]),
                parents: BTreeSet::new(),
                blob: Blob::new(vec![head; 16]),
            }],
        }))
    }
}

fn pump(a: &mut TestDriver, b: &mut TestDriver) -> Result<(), TestError> {
    for _ in 0..64 {
        let a_out: Vec<_> = a.outbox.drain(..).collect();
        let b_out: Vec<_> = b.outbox.drain(..).collect();
        if a_out.is_empty() && b_out.is_empty() {
            return Ok(());
        }
        for (_conn, bytes) in a_out {
            b.deliver(bytes)?;
        }
        for (_conn, bytes) in b_out {
            a.deliver(bytes)?;
        }
    }
    Err("pump did not quiesce".into())
}

fn handshake(alice: &mut TestDriver, bob: &mut TestDriver) -> Result<(), TestError> {
    let bob_id = bob.peer_id();
    alice.feed(NodeEvent::Connected {
        conn: CONN,
        direction: Direction::Outbound,
        audience: Some(Audience::known(bob_id)),
    })?;
    bob.feed(NodeEvent::Connected {
        conn: CONN,
        direction: Direction::Inbound,
        audience: None,
    })?;
    pump(alice, bob)?;
    ensure(
        alice
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { .. })),
        "handshake must complete through the full pipeline",
    )
}

#[test]
fn full_pipeline_handshake_and_convergence() -> TestResult {
    let tree = SedimentreeId::new([7u8; 32]);
    let mut alice = TestDriver::new(1);
    let mut bob = TestDriver::new(2);
    handshake(&mut alice, &mut bob)?;

    // Divergent histories.
    alice.add_commit(tree, 0xA1)?;
    alice.add_commit(tree, 0xA2)?;
    bob.add_commit(tree, 0xB1)?;

    bob.feed(NodeEvent::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }))?;
    pump(&mut alice, &mut bob)?;

    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished {
            status: SyncStatus::Completed,
            ..
        }
    )));

    let mut expected = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xB1; 32]),
    ];
    expected.sort();
    assert_eq!(alice.stored_commit_ids(tree), expected, "alice converged");
    assert_eq!(bob.stored_commit_ids(tree), expected, "bob converged");

    let mut ah = alice.node.tree_heads(tree).unwrap_or_default();
    let mut bh = bob.node.tree_heads(tree).unwrap_or_default();
    ah.sort();
    bh.sort();
    assert_eq!(ah, bh, "resident heads converged");

    // The data-plane invariant: nothing leaked anywhere.
    alice.check_no_leaks()?;
    bob.check_no_leaks()?;
    Ok(())
}

#[test]
fn full_pipeline_subscription_push() -> TestResult {
    let tree = SedimentreeId::new([8u8; 32]);
    let mut alice = TestDriver::new(3);
    let mut bob = TestDriver::new(4);
    handshake(&mut alice, &mut bob)?;

    alice.add_commit(tree, 0xA1)?;
    bob.feed(NodeEvent::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: true,
    }))?;
    pump(&mut alice, &mut bob)?;
    assert_eq!(bob.stored_commit_ids(tree), vec![CommitId::new([0xA1; 32])]);

    // A new local commit at Alice must reach Bob via the push path...
    // once local-write broadcast lands (needs facade-minted refs). For
    // now Bob re-syncs; the push test upgrades with that feature.
    alice.add_commit(tree, 0xA2)?;
    bob.feed(NodeEvent::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }))?;
    pump(&mut alice, &mut bob)?;

    let mut expected = vec![CommitId::new([0xA1; 32]), CommitId::new([0xA2; 32])];
    expected.sort();
    assert_eq!(bob.stored_commit_ids(tree), expected);

    alice.check_no_leaks()?;
    bob.check_no_leaks()?;
    Ok(())
}
