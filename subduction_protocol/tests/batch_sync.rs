//! Phase 2 acceptance: two machines converge to the same tree via one
//! batch sync — handshake, fingerprint diff, bidirectional data return —
//! with a synchronous in-test driver (crypto + in-memory storage), no
//! async runtime anywhere.

use std::collections::BTreeMap;

use ed25519_dalek::{Signer as _, SigningKey};
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    codec::decode::DecodeFields as _,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_protocol::{
    command::{Command, NewCommit},
    effect::{AppEvent, CryptoOp, CryptoResult, Effect, SignatureCheck, SyncStatus},
    event::{Direction, Event},
    handshake::audience::Audience,
    id::ConnId,
    machine::{Config, Machine, Now},
    peer_id::PeerId,
    storage::{IngestRejection, ItemKind, StorageOp, StorageResult},
    timestamp::Timestamp,
    wall_clock::TimestampSeconds,
};

const CONN: ConnId = ConnId::new(1);

const fn now() -> Now {
    Now {
        monotonic: Timestamp::from_millis(0),
        wall: TimestampSeconds::new(1_700_000_000),
    }
}

type CommitStore = BTreeMap<CommitId, (Signed<LooseCommit>, Blob)>;
type FragmentStore = BTreeMap<CommitId, (Signed<Fragment>, Blob)>;

/// In-memory compound store: (commits, fragments) per tree.
#[derive(Default)]
struct Store {
    trees: BTreeMap<SedimentreeId, (CommitStore, FragmentStore)>,
}

fn verify_signed_commit(
    signed: &Signed<LooseCommit>,
    blob: &Blob,
) -> Result<LooseCommit, IngestRejection> {
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&signed.issuer().to_bytes()) else {
        return Err(IngestRejection::BadSignature);
    };
    if vk
        .verify_strict(signed.payload_bytes(), signed.signature())
        .is_err()
    {
        return Err(IngestRejection::BadSignature);
    }
    let Ok((commit, _)) = LooseCommit::try_decode_fields(signed.fields_bytes()) else {
        return Err(IngestRejection::BadSignature);
    };
    if BlobMeta::new(blob) != *commit.blob_meta() {
        return Err(IngestRejection::BlobMismatch);
    }
    Ok(commit)
}

/// One peer: machine + synchronous driver (signer, store, mailboxes).
struct TestPeer {
    machine: Machine,
    signing_key: SigningKey,
    signer: MemorySigner,
    store: Store,
    outbox: Vec<Vec<u8>>,
    app: Vec<AppEvent>,
}

impl TestPeer {
    fn new(seed: u8) -> Self {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let signer = MemorySigner::from_bytes(&[seed; 32]);
        let local_peer = PeerId::from(signing_key.verifying_key());
        Self {
            machine: Machine::new(Config::new(local_peer, [seed.wrapping_add(100); 32])),
            signing_key,
            signer,
            store: Store::default(),
            outbox: Vec::new(),
            app: Vec::new(),
        }
    }

    fn peer_id(&self) -> PeerId {
        PeerId::from(self.signing_key.verifying_key())
    }

    fn feed(&mut self, event: Event) {
        let _outcome = self.machine.handle(now(), event);
        self.run_effects();
    }

    fn run_effects(&mut self) {
        for _ in 0..256 {
            let Some(effect) = self.machine.poll_effect() else {
                return;
            };
            match effect {
                Effect::SendMessage { bytes, .. } => self.outbox.push(bytes),
                Effect::Disconnect { .. } => {}
                Effect::App(event) => self.app.push(event),
                Effect::Crypto { ticket, op } => {
                    let result = match op {
                        CryptoOp::Sign { payload } => CryptoResult::Signed {
                            signature: self.signing_key.sign(&payload).to_bytes(),
                        },
                        CryptoOp::Verify(item) => {
                            let ok = ed25519_dalek::VerifyingKey::from_bytes(&item.verifying_key)
                                .is_ok_and(|vk| {
                                    vk.verify_strict(
                                        &item.payload,
                                        &ed25519_dalek::Signature::from_bytes(&item.signature),
                                    )
                                    .is_ok()
                                });
                            CryptoResult::Verified(if ok {
                                SignatureCheck::Valid
                            } else {
                                SignatureCheck::Invalid
                            })
                        }
                        CryptoOp::VerifyBatch(_) => unimplemented!("not used in this test"),
                    };
                    let _outcome = self
                        .machine
                        .handle(now(), Event::CryptoDone { ticket, result });
                }
                Effect::Storage { ticket, op } => {
                    let result = self.run_storage(op);
                    let _outcome = self
                        .machine
                        .handle(now(), Event::StorageDone { ticket, result });
                }
            }
        }
        // 256 iterations without quiescing means a feedback loop.
        assert!(
            self.machine.poll_effect().is_none(),
            "effect loop did not quiesce"
        );
    }

    fn run_storage(&mut self, op: StorageOp) -> StorageResult {
        match op {
            StorageOp::IngestLocal { tree, commits } => {
                let sealed: Vec<Signed<LooseCommit>> = commits
                    .iter()
                    .map(|new| {
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
                        let slot = self.store.trees.entry(tree).or_default();
                        slot.0.insert(new.head, (signed.clone(), new.blob.clone()));
                        signed
                    })
                    .collect();
                StorageResult::LocallyIngested { commits: sealed }
            }

            StorageOp::Ingest {
                tree,
                commits,
                fragments,
                ..
            } => {
                assert!(fragments.is_empty(), "fragments unused in this test");
                let mut stored = 0u32;
                let mut rejected = Vec::new();
                for (index, (signed, blob)) in commits.into_iter().enumerate() {
                    match verify_signed_commit(&signed, &blob) {
                        Ok(commit) => {
                            let slot = self.store.trees.entry(tree).or_default();
                            slot.0.insert(commit.head(), (signed, blob));
                            stored += 1;
                        }
                        Err(reason) => {
                            #[allow(clippy::cast_possible_truncation)]
                            rejected.push((ItemKind::Commit, index as u32, reason));
                        }
                    }
                }
                StorageResult::Ingested { stored, rejected }
            }

            StorageOp::FetchItems {
                tree,
                commit_ids,
                fragment_heads,
                ..
            } => {
                assert!(fragment_heads.is_empty(), "fragments unused in this test");
                let Some(slot) = self.store.trees.get(&tree) else {
                    return StorageResult::UnknownTree;
                };
                let commits = commit_ids
                    .iter()
                    .filter_map(|id| slot.0.get(id).cloned())
                    .collect();
                StorageResult::Fetched {
                    commits,
                    fragments: vec![],
                }
            }

            StorageOp::DeleteTree { tree, .. } => {
                self.store.trees.remove(&tree);
                StorageResult::TreeDeleted
            }
        }
    }

    fn add_commit(&mut self, tree: SedimentreeId, head: u8) {
        self.feed(Event::Command(Command::AddCommits {
            tree,
            commits: vec![NewCommit {
                head: CommitId::new([head; 32]),
                parents: std::collections::BTreeSet::new(),
                blob: Blob::new(vec![head; 16]),
            }],
        }));
    }

    fn stored_commit_ids(&self, tree: SedimentreeId) -> Vec<CommitId> {
        self.store
            .trees
            .get(&tree)
            .map(|slot| slot.0.keys().copied().collect())
            .unwrap_or_default()
    }
}

/// Shuttle wire messages until both sides quiesce.
fn pump(alice: &mut TestPeer, bob: &mut TestPeer) {
    for _ in 0..64 {
        let a_out: Vec<_> = alice.outbox.drain(..).collect();
        let b_out: Vec<_> = bob.outbox.drain(..).collect();
        if a_out.is_empty() && b_out.is_empty() {
            return;
        }
        for bytes in a_out {
            bob.feed(Event::MessageReceived { conn: CONN, bytes });
        }
        for bytes in b_out {
            alice.feed(Event::MessageReceived { conn: CONN, bytes });
        }
    }
    assert!(
        alice.outbox.is_empty() && bob.outbox.is_empty(),
        "message pump did not quiesce"
    );
}

fn handshake(alice: &mut TestPeer, bob: &mut TestPeer) {
    let bob_id = bob.peer_id();
    alice.feed(Event::Connected {
        conn: CONN,
        direction: Direction::Outbound,
        audience: Some(Audience::known(bob_id)),
    });
    bob.feed(Event::Connected {
        conn: CONN,
        direction: Direction::Inbound,
        audience: None,
    });
    pump(alice, bob);
    assert!(
        alice
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { .. })),
        "handshake must complete"
    );
}

#[test]
fn divergent_trees_converge_via_one_sync() {
    let tree = SedimentreeId::new([7u8; 32]);
    let mut alice = TestPeer::new(1);
    let mut bob = TestPeer::new(2);
    handshake(&mut alice, &mut bob);

    // Divergent local histories.
    alice.add_commit(tree, 0xA1);
    alice.add_commit(tree, 0xA2);
    bob.add_commit(tree, 0xB1);

    // Bob initiates one batch sync.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    // Bob's request concluded successfully.
    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::Completed, .. } if *t == tree
    )));

    // Both stores hold all three commits.
    let mut expected: Vec<CommitId> = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xB1; 32]),
    ];
    expected.sort();
    assert_eq!(alice.stored_commit_ids(tree), expected, "alice converged");
    assert_eq!(bob.stored_commit_ids(tree), expected, "bob converged");

    // Both resident trees agree on heads (as sets; ordering is
    // traversal-dependent).
    let mut alice_heads = alice.machine.tree_heads(tree).unwrap_or_default();
    let mut bob_heads = bob.machine.tree_heads(tree).unwrap_or_default();
    alice_heads.sort();
    bob_heads.sort();
    assert_eq!(alice_heads, bob_heads, "resident heads converged");
    assert!(!alice_heads.is_empty());

    // Both sides saw the remote data arrive.
    assert!(alice
        .app
        .iter()
        .any(|e| matches!(e, AppEvent::TreeUpdated { tree: t, .. } if *t == tree)));
    assert!(bob
        .app
        .iter()
        .any(|e| matches!(e, AppEvent::TreeUpdated { tree: t, .. } if *t == tree)));
}

#[test]
fn syncing_a_tree_we_lack_pulls_everything() {
    let tree = SedimentreeId::new([8u8; 32]);
    let mut alice = TestPeer::new(3);
    let mut bob = TestPeer::new(4);
    handshake(&mut alice, &mut bob);

    alice.add_commit(tree, 0xA1);

    // Bob has nothing; his summary advertises nothing.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    assert_eq!(
        bob.stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])],
        "bob received alice's commit"
    );
    assert_eq!(
        bob.machine.tree_heads(tree),
        Some(vec![CommitId::new([0xA1; 32])])
    );
}

#[test]
fn sync_of_unknown_tree_reports_not_found() {
    let tree = SedimentreeId::new([9u8; 32]);
    let mut alice = TestPeer::new(5);
    let mut bob = TestPeer::new(6);
    handshake(&mut alice, &mut bob);

    // Neither side has the tree.
    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    pump(&mut alice, &mut bob);

    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::NotFound, .. } if *t == tree
    )));
}

#[test]
fn sync_request_times_out_without_response() {
    let tree = SedimentreeId::new([10u8; 32]);
    let mut alice = TestPeer::new(7);
    let mut bob = TestPeer::new(8);
    handshake(&mut alice, &mut bob);

    bob.feed(Event::Command(Command::SyncTree {
        conn: CONN,
        tree,
        subscribe: false,
    }));
    // Discard Bob's request instead of delivering it.
    bob.outbox.clear();
    assert_eq!(bob.machine.pending_sync_requests(), 1);
    assert!(
        bob.machine.poll_timeout().is_some(),
        "request deadline armed"
    );

    // Past the deadline.
    let late = Now {
        monotonic: Timestamp::from_millis(0)
            .saturating_add(Config::DEFAULT_SYNC_TIMEOUT)
            .saturating_add(core::time::Duration::from_secs(1)),
        wall: TimestampSeconds::new(1_700_000_000 + 61),
    };
    let _outcome = bob.machine.handle(late, Event::Wake);
    bob.run_effects();

    assert_eq!(bob.machine.pending_sync_requests(), 0);
    assert!(bob.app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished { tree: t, status: SyncStatus::TimedOut, .. } if *t == tree
    )));
}
