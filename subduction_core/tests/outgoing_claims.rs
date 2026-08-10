//! Outgoing-subscription claims must not outlive the connection era they
//! were made in.
//!
//! A claim records "we already propagated our subscription to this peer" so
//! `propagate_subscription` can skip redundant re-sends. The remote forgets
//! our subscriptions when its side of the connection dies, so a claim that
//! survives into the peer's next connection era would suppress
//! re-establishment entirely — silent upstream unsubscription. Claims are
//! therefore invalidated on the peer's absent → present transition, rather
//! than relying on teardown (which can be skipped or interrupted).

#![allow(clippy::panic)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{depth::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<PausableChannelTransport>;

type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;

fn make_node() -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[7u8; 32]))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Returns the connection plus its remote transport half. The remote half
/// must stay alive for the duration of the test: dropping it makes the
/// spawned reader error out and emit a closure event, which would remove
/// the peer and turn the next `add_connection` into an absent → present
/// transition — silently changing what the assertions test.
fn make_conn(peer: PeerId) -> (Authenticated<Conn, Sendable>, PausableChannelTransport) {
    let (transport, remote) = PausableChannelTransport::pair();
    (
        Authenticated::new_for_test(MessageTransport::new(transport), peer),
        remote,
    )
}

/// A claim left behind for an absent peer (as an interrupted teardown
/// would) is cleared when the peer next connects; a claim made while the
/// peer is connected survives additional connections from the same peer;
/// a full disconnect/reconnect cycle invalidates again.
#[tokio::test(flavor = "current_thread")]
async fn stale_claims_cleared_on_peer_arrival() -> TestResult {
    let node = make_node();
    let peer = PeerId::new([1u8; 32]);
    let tree = SedimentreeId::new([42u8; 32]);

    // A stale claim for an absent peer: what an interrupted teardown leaves.
    node.insert_outgoing_claim_for_test(peer, tree).await;

    // The peer arrives (absent → present): the stale claim must be gone.
    let (conn1, _remote1) = make_conn(peer);
    node.add_connection(conn1.clone()).await?;
    assert_eq!(
        node.outgoing_claims(&peer).await,
        None,
        "claims from a previous connection era must be invalidated on arrival"
    );

    // A claim made while connected survives further connections from the
    // same peer (present → present is not a new era).
    node.insert_outgoing_claim_for_test(peer, tree).await;
    let (conn2, _remote2) = make_conn(peer);
    node.add_connection(conn2.clone()).await?;
    assert!(
        node.outgoing_claims(&peer)
            .await
            .is_some_and(|claims| claims.contains(&tree)),
        "claims made in the current connection era must survive"
    );

    // Full disconnect then reconnect: the next era starts clean even if a
    // claim were somehow left behind.
    node.disconnect_from_peer(&peer).await?;
    node.insert_outgoing_claim_for_test(peer, tree).await;
    // Let the manager process the disconnects (and any straggling closure
    // events) before starting the next era, so the arrival below is a
    // clean absent → present transition.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let (conn3, _remote3) = make_conn(peer);
    node.add_connection(conn3).await?;
    assert_eq!(
        node.outgoing_claims(&peer).await,
        None,
        "reconnect after full teardown must start a fresh claim era"
    );

    Ok(())
}
