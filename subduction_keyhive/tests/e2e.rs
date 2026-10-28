//! End-to-end: keyhive-enabled Subduction nodes over real connections.
//!
//! Every node runs the keyhive protocol over its Subduction connections and
//! gates sync with membership in its own Keyhive. Membership moves only in
//! `SUK` frames, so these tests check that what one node grants or revokes
//! reaches the others and changes what they serve and store.
//!
//! The usual topology is a server that relays a document for its members:
//!
//! ```text
//!   alice ──┐                 alice owns the document and makes the
//!           ├── server        server a relay; bob only ever talks to
//!   bob ────┘                 the server
//! ```

use std::collections::BTreeSet;

use keyhive_core::principal::document::id::DocumentId;
use keyhive_crypto::identifier::Identifier;
use keyline::power::Power;
use nonempty::nonempty;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::test_utils::ChannelTransport,
    peer::id::PeerId,
    subduction::error::AddConnectionError,
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_keyhive::{
    change_id::ChangeId,
    protocol::is_known_agent,
    test_utils::node::{CONVERGENCE_TIMEOUT, Node, eventually},
};
use testresult::TestResult;

type TestNode = Node<ChannelTransport>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(5_000);

/// Membership travels in `SUK` frames over the same connection as sync,
/// contact cards included: nothing is handed over out of band.
#[tokio::test]
async fn membership_travels_over_the_subduction_connection() -> TestResult {
    let alice = TestNode::new(1).await?;
    let bob = TestNode::new(2).await?;
    alice.connect(&bob).await?;

    alice.sync_keyhive().await?;
    eventually("alice learns bob", CONVERGENCE_TIMEOUT, knows(&alice, &bob)).await?;
    eventually("bob learns alice", CONVERGENCE_TIMEOUT, knows(&bob, &alice)).await?;

    let group = alice.keyhive.generate_group(vec![]).await?;
    alice
        .keyhive
        .add_member(bob.identifier(), group, Power::Read)
        .await?;
    let doc = alice
        .keyhive
        .generate_doc(vec![group.into()], nonempty![change(1)])
        .await?;

    alice.sync_keyhive().await?;
    let reader = &bob;
    eventually(
        "bob can read the document",
        CONVERGENCE_TIMEOUT,
        move || async move {
            reader
                .keyhive
                .reachable_docs()
                .await
                .is_ok_and(|docs| docs.get(&doc) == Some(&Power::Read))
        },
    )
    .await?;
    assert!(
        bob.keyhive.has_document(doc).await,
        "bob holds its CGKA state"
    );
    Ok(())
}

/// The server learns Alice's document, and later Bob's membership, only over
/// `SUK`; until then it neither stores nor serves the document.
#[tokio::test]
async fn the_server_admits_what_alice_grants() -> TestResult {
    let scene = Scene::new().await?;
    let Scene {
        server,
        alice,
        bob,
        doc,
    } = &scene;
    let id = tree(*doc);

    alice.commit(id, 1).await?;
    alice.sync_tree(server, id).await?;
    assert_eq!(
        server.commit_count(id).await,
        0,
        "the server has not heard of the document yet"
    );

    scene.alice_publishes().await?;

    bob.commit(id, 2).await?;
    bob.sync_tree(server, id).await?;
    assert_eq!(bob.commit_count(id).await, 1, "bob is not served");
    assert_eq!(
        server.commit_count(id).await,
        1,
        "bob's commit is not stored"
    );

    scene.alice_admits_bob().await?;

    bob.sync_tree(server, id).await?;
    eventually("bob is served", CONVERGENCE_TIMEOUT, count_is(bob, id, 2)).await?;
    eventually(
        "bob's commit is stored",
        CONVERGENCE_TIMEOUT,
        count_is(server, id, 2),
    )
    .await?;
    Ok(())
}

/// Once Alice's revocation reaches the server over `SUK`, Bob is neither
/// served nor stored again.
#[tokio::test]
async fn the_server_refuses_bob_once_alice_revokes_him() -> TestResult {
    let scene = Scene::new().await?;
    let Scene {
        server,
        alice,
        bob,
        doc,
    } = &scene;
    let id = tree(*doc);

    scene.alice_publishes().await?;
    scene.alice_admits_bob().await?;
    bob.sync_tree(server, id).await?;
    eventually("bob is served", CONVERGENCE_TIMEOUT, count_is(bob, id, 1)).await?;

    alice
        .keyhive
        .revoke_member(bob.identifier(), true, *doc)
        .await?;
    alice.sync_keyhive().await?;
    eventually(
        "the server sees the revocation",
        CONVERGENCE_TIMEOUT,
        access_is(server, bob, *doc, None),
    )
    .await?;

    alice.commit(id, 3).await?;
    alice.sync_tree(server, id).await?;
    eventually(
        "alice still publishes",
        CONVERGENCE_TIMEOUT,
        count_is(server, id, 2),
    )
    .await?;

    bob.commit(id, 4).await?;
    bob.sync_tree(server, id).await?;
    assert_eq!(
        bob.commit_count(id).await,
        2,
        "bob misses alice's new commit"
    );
    assert_eq!(
        server.commit_count(id).await,
        2,
        "bob's commit is not stored"
    );
    Ok(())
}

/// A reader who only talks to a relay gets the document's CGKA state, and so
/// can decrypt.
#[tokio::test]
#[ignore = "relays hold no CGKA operations, so they can't serve them (FIXME.md)"]
async fn readers_get_cgka_state_through_a_relay() -> TestResult {
    let scene = Scene::new().await?;
    scene.alice_publishes().await?;
    scene.alice_admits_bob().await?;

    let (bob, doc) = (&scene.bob, scene.doc);
    eventually("bob holds the document", CONVERGENCE_TIMEOUT, move || {
        Box::pin(async move {
            drop(bob.sync_keyhive().await);
            bob.keyhive.has_document(doc).await
        }) as BoolFuture<'_>
    })
    .await?;
    Ok(())
}

/// Anyone with a valid identity may connect, even a stranger to the server's
/// Keyhive. Knowing them grants nothing: a document the server is not a
/// member of stays refused after a keyhive sync.
#[tokio::test]
async fn strangers_connect_but_unrelayed_documents_stay_refused() -> TestResult {
    let server = TestNode::new(20).await?;
    let carol = TestNode::new(21).await?;
    assert!(!knows(&server, &carol)().await, "carol is a stranger");
    carol.connect(&server).await?;

    let doc = carol
        .keyhive
        .generate_doc(vec![], nonempty![change(1)])
        .await?;
    carol.sync_keyhive().await?;
    eventually(
        "server learns carol",
        CONVERGENCE_TIMEOUT,
        knows(&server, &carol),
    )
    .await?;

    let id = tree(doc);
    carol.commit(id, 1).await?;
    carol.sync_tree(&server, id).await?;
    assert_eq!(server.commit_count(id).await, 0);
    assert!(!server.keyhive.has_document(doc).await);
    Ok(())
}

/// A peer ID that is not a usable Keyhive identity is turned away at connect.
#[tokio::test]
async fn invalid_identities_are_refused_at_connect() -> TestResult {
    const SMALL_ORDER_POINT: [u8; 32] = {
        let mut bytes = [0; 32];
        bytes[0] = 1;
        bytes
    };

    let server = TestNode::new(30).await?;
    let (transport, _other_end) = ChannelTransport::pair();
    let refused = server
        .attach(Authenticated::new_for_test(
            MessageTransport::new(transport),
            PeerId::new(SMALL_ORDER_POINT),
            Direction::Accepted,
        ))
        .await;
    assert!(matches!(
        refused,
        Err(AddConnectionError::ConnectionDisallowed(_))
    ));
    Ok(())
}

/// Alice owns a document and has made the server a relay for it. Alice and
/// Bob are each connected to the server only, and know it over `SUK`. Bob's
/// contact card reached Alice out of band, as an invite would.
///
/// The server has not yet heard of the document.
struct Scene {
    server: TestNode,
    alice: TestNode,
    bob: TestNode,
    doc: DocumentId,
}

impl Scene {
    async fn new() -> TestResult<Self> {
        let server = TestNode::new(10).await?;
        let alice = TestNode::new(11).await?;
        let bob = TestNode::new(12).await?;

        for client in [&alice, &bob] {
            client.connect(&server).await?;
            client.sync_keyhive().await?;
            eventually(
                "client learns server",
                CONVERGENCE_TIMEOUT,
                knows(client, &server),
            )
            .await?;
            eventually(
                "server learns client",
                CONVERGENCE_TIMEOUT,
                knows(&server, client),
            )
            .await?;
        }

        let card = bob.keyhive.generate_contact_card().await?;
        alice.keyhive.receive_contact_card(&card).await?;

        let doc = alice
            .keyhive
            .generate_doc(vec![], nonempty![change(1)])
            .await?;
        alice
            .keyhive
            .add_member(server.identifier(), doc, Power::Relay)
            .await?;

        Ok(Self {
            server,
            alice,
            bob,
            doc,
        })
    }

    /// Alice syncs keyhive so the server learns the document, then pushes her
    /// commits to it.
    async fn alice_publishes(&self) -> TestResult {
        let id = tree(self.doc);
        let before = self.alice.commit_count(id).await;
        if before == 0 {
            self.alice.commit(id, 1).await?;
        }

        self.alice.sync_keyhive().await?;
        eventually(
            "the server learns alice owns the document",
            CONVERGENCE_TIMEOUT,
            access_is(&self.server, &self.alice, self.doc, Some(Power::Admin)),
        )
        .await?;

        self.alice.sync_tree(&self.server, id).await?;
        let expected = self.alice.commit_count(id).await;
        eventually(
            "the server stores alice's commits",
            CONVERGENCE_TIMEOUT,
            count_is(&self.server, id, expected),
        )
        .await?;
        Ok(())
    }

    /// Alice makes Bob a reader and syncs keyhive until the server knows.
    /// Bob then learns the membership from the server.
    ///
    /// Bob needs it too: his own policy only accepts the document's commits
    /// from a peer he knows relays it.
    async fn alice_admits_bob(&self) -> TestResult {
        self.alice
            .keyhive
            .add_member(self.bob.identifier(), self.doc, Power::Read)
            .await?;
        self.alice.sync_keyhive().await?;
        eventually(
            "the server learns bob can read",
            CONVERGENCE_TIMEOUT,
            access_is(&self.server, &self.bob, self.doc, Some(Power::Read)),
        )
        .await?;

        self.bob.sync_keyhive().await?;
        eventually(
            "bob learns the server relays the document",
            CONVERGENCE_TIMEOUT,
            access_is(&self.bob, &self.server, self.doc, Some(Power::Relay)),
        )
        .await?;
        Ok(())
    }
}

trait TestNodeExt {
    async fn commit(&self, id: SedimentreeId, seed: u8) -> TestResult;
    async fn sync_tree(&self, peer: &TestNode, id: SedimentreeId) -> TestResult;
    async fn commit_count(&self, id: SedimentreeId) -> usize;
}

impl TestNodeExt for TestNode {
    /// Author a commit locally.
    async fn commit(&self, id: SedimentreeId, seed: u8) -> TestResult {
        self.subduction
            .add_commit(
                id,
                CommitId::new([seed; 32]),
                BTreeSet::new(),
                Blob::new(vec![seed; 8]),
            )
            .await?;
        Ok(())
    }

    /// Sync one tree with `peer`, both ways.
    async fn sync_tree(&self, peer: &TestNode, id: SedimentreeId) -> TestResult {
        self.subduction
            .sync_with_peer(&peer.peer_id(), id, false, SYNC_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn commit_count(&self, id: SedimentreeId) -> usize {
        self.subduction.get_commits(id).await.map_or(0, |c| c.len())
    }
}

const fn change(seed: u8) -> ChangeId {
    ChangeId::new(CommitId::new([seed; 32]))
}

fn tree(doc: DocumentId) -> SedimentreeId {
    SedimentreeId::new(Identifier::from(doc).to_bytes())
}

/// Whether `who` is a known agent in `node`'s Keyhive.
fn knows<'a>(node: &'a TestNode, who: &'a TestNode) -> impl FnMut() -> BoolFuture<'a> + 'a {
    move || {
        Box::pin(async move {
            is_known_agent(&node.keyhive, who.identifier())
                .await
                .unwrap_or(false)
        })
    }
}

/// Whether `node` holds exactly `n` commits of `id`.
fn count_is<'a>(
    node: &'a TestNode,
    id: SedimentreeId,
    n: usize,
) -> impl FnMut() -> BoolFuture<'a> + 'a {
    move || Box::pin(async move { node.commit_count(id).await == n })
}

/// Whether `node` sees `who` with exactly `power` over `doc`.
fn access_is<'a>(
    node: &'a TestNode,
    who: &'a TestNode,
    doc: DocumentId,
    power: Option<Power>,
) -> impl FnMut() -> BoolFuture<'a> + 'a {
    move || {
        Box::pin(async move {
            node.keyhive
                .access_for_doc(who.identifier(), doc)
                .await
                .is_ok_and(|p| p == power)
        })
    }
}

type BoolFuture<'a> = std::pin::Pin<Box<dyn Future<Output = bool> + 'a>>;
