//! Keyline sync between real handlers over in-memory connections.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::unwrap_used
)]

use std::sync::Arc;

use future_form::Sendable;
use futures::executor::block_on;
use keyhive_crypto::verified::Verified;
use keyline::{
    certificate::{Certificate, CertificateId, VerifiedCertificate},
    contract::Keyline,
    delegation::Delegation,
    power::Power,
    test_utils::{id, signing_key},
};
use keyline_memory::MemoryKeyline;
use sedimentree_core::{
    blob::Blob, crypto::fingerprint::FingerprintSeed, depth::CountLeadingZeroBytes,
    loose_commit::LooseCommit,
};
use subduction_core::{
    authenticated::Authenticated, connection::test_utils::ChannelMockConnection, handler::Handler,
    peer::id::PeerId, policy::storage::StoragePolicy, storage::memory::MemoryStorage,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_keyline::{
    batch::{Batch, KEYLINE_TREE_ID},
    handler::{KeylineHandler, PublishError},
    ingest::AdmitError,
    message::KeylineMessage,
    policy::KeylinePolicy,
};
use testresult::TestResult;

type W = ();
type Store = MemoryKeyline<W>;
type Conn = ChannelMockConnection<KeylineMessage>;
type Node = KeylineHandler<Sendable, Store, MemoryStorage, Conn, CountLeadingZeroBytes>;

fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

fn node(n: u8) -> Node {
    node_over(n, MemoryStorage::new())
}

fn node_over(n: u8, storage: MemoryStorage) -> Node {
    block_on(KeylineHandler::hydrate(
        peer(n),
        Arc::new(Store::new()),
        storage,
        CountLeadingZeroBytes,
    ))
    .expect("memory storage and store are infallible")
}

fn grant(issuer: u8, audience: u8, subject: u8, power: Power) -> Certificate<W> {
    let payload = Delegation::new(id(audience), id(subject), power);
    VerifiedCertificate::from(Verified::sign_sync(&payload, &signing_key(issuer)).expect("signs"))
        .into_certificate()
}

fn batch<I: IntoIterator<Item = Certificate<W>>>(certs: I) -> Batch<W> {
    Batch::new(certs).expect("non-empty")
}

fn publish(node: &Node, n: u8, certs: Vec<Certificate<W>>) {
    let signer = MemorySigner::from_bytes(&[n; 32]);
    block_on(node.publish(&signer, batch(certs))).expect("publishes");
}

fn digest(node: &Node) -> [u8; 32] {
    block_on(<Store as Keyline<Sendable>>::digest(node.keyline()))
        .expect("infallible")
        .value
        .into()
}

fn holds(node: &Node, cert: &Certificate<W>) -> bool {
    block_on(<Store as Keyline<Sendable>>::contains(
        node.keyline(),
        cert.id(),
    ))
    .expect("infallible")
    .value
}

fn sorted_heads(node: &Node) -> Vec<sedimentree_core::loose_commit::id::CommitId> {
    let mut heads = block_on(node.heads());
    heads.sort();
    heads
}

/// One direction of a link: what `from` sends, delivered to `to` over
/// `to`'s end of the connection.
struct Wire {
    to: usize,
    outbound: async_channel::Receiver<KeylineMessage>,
    at_to: Authenticated<Conn, Sendable>,
}

/// Handlers linked by in-memory connections.
struct Net {
    nodes: Vec<Node>,
    wires: Vec<Wire>,

    /// `ends[(a, b)]`: `a`'s end of its connection to `b`.
    ends: std::collections::BTreeMap<(usize, usize), Authenticated<Conn, Sendable>>,
}

impl Net {
    fn new(nodes: Vec<Node>) -> Self {
        Self {
            nodes,
            wires: Vec::new(),
            ends: std::collections::BTreeMap::new(),
        }
    }

    fn link(&mut self, a: usize, b: usize) {
        let peer_of = |i: usize| peer(u8::try_from(i).expect("small net") + 1);
        let (a_to_b, a_handle) = Conn::new_with_handle(peer_of(b));
        let (b_to_a, b_handle) = Conn::new_with_handle(peer_of(a));
        let a_end = a_to_b.authenticated();
        let b_end = b_to_a.authenticated();

        self.wires.push(Wire {
            to: b,
            outbound: a_handle.outbound_rx,
            at_to: b_end.clone(),
        });
        self.wires.push(Wire {
            to: a,
            outbound: b_handle.outbound_rx,
            at_to: a_end.clone(),
        });
        self.ends.insert((a, b), a_end);
        self.ends.insert((b, a), b_end);
    }

    fn end(&self, from: usize, to: usize) -> &Authenticated<Conn, Sendable> {
        self.ends.get(&(from, to)).expect("linked")
    }

    fn sync(&self, from: usize, to: usize) {
        block_on(self.nodes[from].sync_with(self.end(from, to), FingerprintSeed::new(7, 11)))
            .expect("sends");
    }

    /// Deliver messages until every wire is quiet.
    fn settle(&self) {
        loop {
            let mut delivered = false;
            for wire in &self.wires {
                while let Ok(message) = wire.outbound.try_recv() {
                    block_on(Handler::<Sendable, Conn>::handle(
                        &self.nodes[wire.to],
                        &wire.at_to,
                        message,
                    ))
                    .expect("handles");
                    delivered = true;
                }
            }
            if !delivered {
                return;
            }
        }
    }

    /// Drop everything in flight.
    fn drain(&self) {
        for wire in &self.wires {
            while wire.outbound.try_recv().is_ok() {}
        }
    }
}

#[test]
fn sync_exchanges_what_each_side_lacks() {
    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);

    let a_root = grant(10, 20, 10, Power::Admin);
    let b_root = grant(11, 21, 11, Power::Edit);
    publish(&net.nodes[0], 1, vec![a_root.clone()]);
    publish(&net.nodes[1], 2, vec![b_root.clone()]);
    assert_ne!(digest(&net.nodes[0]), digest(&net.nodes[1]));

    net.sync(0, 1);
    net.settle();

    for node in &net.nodes {
        assert!(holds(node, &a_root) && holds(node, &b_root));
    }
    assert_eq!(digest(&net.nodes[0]), digest(&net.nodes[1]));
    assert_eq!(sorted_heads(&net.nodes[0]), sorted_heads(&net.nodes[1]));
}

#[test]
fn new_batches_are_pushed_both_ways_after_a_sync() {
    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);
    net.sync(0, 1);
    net.settle();

    let from_a = grant(10, 20, 10, Power::Admin);
    publish(&net.nodes[0], 1, vec![from_a.clone()]);
    net.settle();
    assert!(holds(&net.nodes[1], &from_a));

    let from_b = grant(11, 21, 11, Power::Read);
    publish(&net.nodes[1], 2, vec![from_b.clone()]);
    net.settle();
    assert!(holds(&net.nodes[0], &from_b));
}

#[test]
fn batches_gossip_to_peers_without_a_direct_link() {
    let mut net = Net::new(vec![node(1), node(2), node(3)]);
    net.link(0, 1);
    net.link(1, 2);
    net.sync(0, 1);
    net.sync(2, 1);
    net.settle();

    let root = grant(10, 20, 10, Power::Admin);
    publish(&net.nodes[0], 1, vec![root.clone()]);
    net.settle();

    assert!(holds(&net.nodes[2], &root));
    assert_eq!(digest(&net.nodes[0]), digest(&net.nodes[2]));
}

#[test]
fn a_batch_ahead_of_its_proof_waits_for_the_next_sync() {
    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);

    let root = grant(10, 20, 10, Power::Admin);
    let onward = grant(20, 30, 10, Power::Read);
    publish(&net.nodes[0], 1, vec![root.clone()]);
    publish(&net.nodes[0], 1, vec![onward.clone()]);

    // B learns of A only through A's sync, so A's pushes went nowhere. Hand
    // B the dependent commit alone, as if it overtook its proof.
    let dependent = commit_holding(&net.nodes[0], &onward);
    block_on(Handler::<Sendable, Conn>::handle(
        &net.nodes[1],
        net.end(1, 0),
        KeylineMessage::Commit {
            commit: dependent.0,
            blob: dependent.1,
        },
    ))
    .expect("handles");
    assert!(!holds(&net.nodes[1], &onward));
    assert!(block_on(net.nodes[1].heads()).is_empty());

    net.sync(1, 0);
    net.settle();
    assert!(holds(&net.nodes[1], &root) && holds(&net.nodes[1], &onward));
    assert_eq!(digest(&net.nodes[0]), digest(&net.nodes[1]));
}

#[test]
fn outsider_batches_are_refused_and_not_forwarded() {
    let mut net = Net::new(vec![node(1), node(2), node(3)]);
    net.link(0, 1);
    net.link(1, 2);
    net.sync(2, 1);
    net.settle();

    // 40 has no standing over 10.
    let outsider = batch([grant(40, 41, 10, Power::Admin)]);
    let signer = MemorySigner::from_bytes(&[1; 32]);
    assert!(matches!(
        block_on(net.nodes[0].publish(&signer, outsider.clone())),
        Err(PublishError::Admit(AdmitError::NotYet { .. }))
    ));

    // Send it anyway, as a hostile peer would.
    let (commit, blob) = outsider.into_commit(std::collections::BTreeSet::new());
    let sealed = block_on(Signed::seal::<Sendable, _>(&signer, commit)).into_signed();
    block_on(Handler::<Sendable, Conn>::handle(
        &net.nodes[1],
        net.end(1, 0),
        KeylineMessage::Commit {
            commit: sealed,
            blob,
        },
    ))
    .expect("a refused commit does not drop the connection");
    net.settle();

    assert!(block_on(net.nodes[1].heads()).is_empty());
    assert!(block_on(net.nodes[2].heads()).is_empty());
}

#[test]
fn forged_commits_are_refused() {
    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);

    let valid = batch([grant(10, 20, 10, Power::Admin)]);
    let (commit, blob) = valid.into_commit(std::collections::BTreeSet::new());
    let key = MemorySigner::from_bytes(&[1; 32]);

    // A commit ID that is not derived from its parents and blob.
    let forged = LooseCommit::new(
        KEYLINE_TREE_ID,
        sedimentree_core::loose_commit::id::CommitId::new([9; 32]),
        commit.parents().clone(),
        *commit.blob_meta(),
    );
    let forged = block_on(Signed::seal::<Sendable, _>(&key, forged)).into_signed();

    // A blob that does not match the commit.
    let honest = block_on(Signed::seal::<Sendable, _>(&key, commit)).into_signed();

    for (commit, blob) in [(forged, blob), (honest, Blob::new(vec![0]))] {
        block_on(Handler::<Sendable, Conn>::handle(
            &net.nodes[1],
            net.end(1, 0),
            KeylineMessage::Commit { commit, blob },
        ))
        .expect("handles");
    }
    assert!(block_on(net.nodes[1].heads()).is_empty());
}

#[test]
fn disconnected_peers_stop_receiving_pushes() {
    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);
    net.sync(0, 1);
    net.settle();

    block_on(Handler::<Sendable, Conn>::on_peer_disconnect(
        &net.nodes[1],
        peer(1),
    ));
    publish(&net.nodes[1], 2, vec![grant(11, 21, 11, Power::Read)]);
    assert!(net.wires.iter().all(|wire| wire.outbound.is_empty()));
    net.drain();
}

#[test]
fn hydrate_rebuilds_the_keyline_store_from_storage() {
    let storage = MemoryStorage::new();
    let original = node_over(1, storage.clone());
    publish(&original, 1, vec![grant(10, 20, 10, Power::Admin)]);
    publish(&original, 1, vec![grant(20, 30, 10, Power::Read)]);

    let restarted = node_over(1, storage);
    assert_eq!(digest(&restarted), digest(&original));
    assert_eq!(sorted_heads(&restarted), sorted_heads(&original));
}

#[test]
fn the_policy_reads_what_sync_delivered() -> TestResult {
    const DOC: u8 = 10;
    const ALICE: u8 = 20;

    let mut net = Net::new(vec![node(1), node(2)]);
    net.link(0, 1);
    publish(&net.nodes[0], 1, vec![grant(DOC, ALICE, DOC, Power::Read)]);

    let policy = KeylinePolicy::new(Arc::clone(net.nodes[1].keyline()));
    let alice = PeerId::new(id(ALICE).to_bytes());
    let doc = sedimentree_core::id::SedimentreeId::new(id(DOC).to_bytes());
    let before = block_on(
        <KeylinePolicy<Store> as StoragePolicy<Sendable>>::authorize_fetch(&policy, alice, doc),
    );
    assert!(before.is_err());

    net.sync(1, 0);
    net.settle();
    block_on(
        <KeylinePolicy<Store> as StoragePolicy<Sendable>>::authorize_fetch(&policy, alice, doc),
    )?;
    Ok(())
}

/// The commit (and blob) in `node`'s tree that carries `cert`, found by
/// asking `node` to answer a sync from a node that has nothing.
fn commit_holding(node: &Node, cert: &Certificate<W>) -> (Signed<LooseCommit>, Blob) {
    let CertificateId::Delegation(_) = cert.id() else {
        panic!("delegations only");
    };
    let (conn, handle) = Conn::new_with_handle(peer(99));
    let end = conn.authenticated();
    let empty = sedimentree_core::sedimentree::Sedimentree::default()
        .fingerprint_summarize(&FingerprintSeed::new(1, 1));
    block_on(Handler::<Sendable, Conn>::handle(
        node,
        &end,
        KeylineMessage::SyncRequest {
            req_id: subduction_core::connection::message::RequestId {
                requestor: peer(99),
                nonce: 0,
            },
            summary: empty,
        },
    ))
    .expect("handles");

    let Ok(KeylineMessage::SyncResponse { diff, .. }) = handle.outbound_rx.try_recv() else {
        panic!("expected a sync response");
    };
    diff.missing_commits
        .into_iter()
        .find(|(_, blob)| {
            Batch::<W>::decode(blob.as_slice())
                .is_ok_and(|batch| batch.ids().any(|id| id == cert.id()))
        })
        .expect("node holds a commit with the certificate")
}
