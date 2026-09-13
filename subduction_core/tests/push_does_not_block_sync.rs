//! A relay's sync round must not wait on its downstream subscribers.
//!
//! Pushes triggered by requester-side ingest are sent on a detached task
//! (`SendPushes`), so a subscriber whose transport has wedged cannot hold
//! `sync_with_peer` open. The wedged peer is simulated with
//! [`PausableChannelTransport::pause`]: the wire stays open, but sends to it
//! park forever.
//!
//! ```text
//!   A ──dials──▶ R ──dials──▶ B (holds X)
//!   (R→A sends parked)
//! ```

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<PausableChannelTransport>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);

/// A sync round with one wedged subscriber must finish well inside this.
const BOUND: Duration = Duration::from_secs(3);

fn make_node(seed: u8) -> (Node, PeerId) {
    let signer = MemorySigner::from_bytes(&[seed; 32]);
    let peer = PeerId::from(signer.verifying_key());
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    (sd, peer)
}

/// `dialer` dials `acceptor`; returns the dialer-side and acceptor-side
/// transports so a test can pause either direction.
async fn dial(
    dialer: &Node,
    dialer_peer: PeerId,
    acceptor: &Node,
    acceptor_peer: PeerId,
) -> TestResult<(PausableChannelTransport, PausableChannelTransport)> {
    let (t_d, t_a) = PausableChannelTransport::pair();
    dialer
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(t_d.clone()),
            acceptor_peer,
            Direction::Dialed,
        ))
        .await?;
    acceptor
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(t_a.clone()),
            dialer_peer,
            Direction::Accepted,
        ))
        .await?;
    Ok((t_d, t_a))
}

fn make_blob(seed: u8) -> Blob {
    Blob::new((0..64).map(|i| seed.wrapping_add(i)).collect())
}

const fn make_head(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    CommitId::new(bytes)
}

#[tokio::test]
async fn wedged_subscriber_does_not_block_relay_sync() -> TestResult {
    let (a, a_peer) = make_node(1);
    let (r, r_peer) = make_node(2);
    let (b, b_peer) = make_node(3);
    let (_a_side, r_to_a) = dial(&a, a_peer, &r, r_peer).await?;
    dial(&r, r_peer, &b, b_peer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // A subscribes to X on R while nobody has X yet, so R records A as a
    // subscriber (and propagates upstream, which finds nothing).
    let x = SedimentreeId::new([9u8; 32]);
    a.sync_with_peer(&r_peer, x, true, SYNC_TIMEOUT).await?;
    assert!(r.get_subscribers(x).await.contains(&a_peer));

    // R's sends to A now park forever: A is byte-connected but wedged.
    r_to_a.pause();

    // X appears upstream. R pulls it as a requester and must push it to A;
    // that push must not hold R's sync round open.
    b.store_commit(x, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    let round = tokio::time::timeout(BOUND, r.sync_with_peer(&b_peer, x, true, SYNC_TIMEOUT)).await;
    assert!(
        round.is_ok(),
        "sync_with_peer did not return within {BOUND:?}; a parked subscriber send is blocking it"
    );
    round??;

    assert!(
        r.get_commits(x).await.is_some_and(|c| c.len() == 1),
        "R should hold X regardless of A being wedged"
    );
    Ok(())
}

/// Same, for the all-peers round used by the batch write path.
#[tokio::test]
async fn wedged_subscriber_does_not_block_sync_with_all_peers() -> TestResult {
    let (a, a_peer) = make_node(4);
    let (r, r_peer) = make_node(5);
    let (b, b_peer) = make_node(6);
    let (_a_side, r_to_a) = dial(&a, a_peer, &r, r_peer).await?;
    dial(&r, r_peer, &b, b_peer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let x = SedimentreeId::new([10u8; 32]);
    a.sync_with_peer(&r_peer, x, true, SYNC_TIMEOUT).await?;
    r_to_a.pause();

    b.store_commit(x, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    let round = tokio::time::timeout(BOUND, r.sync_with_all_peers(x, true, SYNC_TIMEOUT)).await;
    assert!(
        round.is_ok(),
        "sync_with_all_peers did not return within {BOUND:?}"
    );
    round??;
    assert!(r.get_commits(x).await.is_some_and(|c| c.len() == 1));
    Ok(())
}
