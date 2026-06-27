//! Negative-case relay tests: transport-level failure (mid-sync close,
//! peer disconnect) must leave local state intact and consistent.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{CloseableChannelTransport, InstantTimeout, TokioSpawn},
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

type Conn = MessageTransport<CloseableChannelTransport>;

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
        InstantTimeout,
        TokioSpawn,
    >,
>;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);
const PROPAGATION_PAUSE: Duration = Duration::from_millis(50);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node(signer: MemorySigner) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult<(CloseableChannelTransport, CloseableChannelTransport)> {
    let (t_a, t_b) = CloseableChannelTransport::pair();

    let conn_a = MessageTransport::new(t_a.clone());
    let conn_b = MessageTransport::new(t_b.clone());

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok((t_a, t_b))
}

/// Closing A's transport after successful sync must not corrupt A's
/// local state. A retains everything it had; subsequent sync attempts
/// fail and drop the peer without crashing.
#[tokio::test]
async fn close_after_sync_preserves_local_state() -> TestResult {
    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (t_a, _t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_id = SedimentreeId::new([2u8; 32]);
    for i in 0..3_u8 {
        a.add_commit(
            sed_id,
            CommitId::new([i + 1; 32]),
            BTreeSet::new(),
            Blob::new(vec![i; 16]),
        )
        .await?;
    }
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    b.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(3));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(3));

    t_a.close();

    // A's sync attempt will error out internally; must not panic and
    // must leave local state untouched.
    a.full_sync_with_all_peers(SYNC_TIMEOUT).await;
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    assert_eq!(a.get_commits(sed_id).await.map(|c| c.len()), Some(3));
    assert_eq!(b.get_commits(sed_id).await.map(|c| c.len()), Some(3));

    Ok(())
}

/// Mid-flight close: A authors a commit while the transport is closing.
/// Either the commit lands on B (raced ahead of the close) or it
/// doesn't (close won) — never both peers in an inconsistent state.
#[tokio::test]
async fn mid_flight_close_leaves_peers_consistent() -> TestResult {
    let a_signer = make_signer(10);
    let b_signer = make_signer(20);
    let a = make_node(a_signer.clone());
    let b = make_node(b_signer.clone());

    let (t_a, _t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let sed_id = SedimentreeId::new([3u8; 32]);

    // Concurrently: schedule a commit and close the transport.
    let a_clone = a.clone();
    let commit_handle = tokio::spawn(async move {
        a_clone
            .add_commit(
                sed_id,
                CommitId::new([99u8; 32]),
                BTreeSet::new(),
                Blob::new(vec![99u8; 16]),
            )
            .await
    });
    t_a.close();
    let _ = commit_handle.await?;

    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let a_count = a.get_commits(sed_id).await.map_or(0, |c| c.len());
    let b_count = b.get_commits(sed_id).await.map_or(0, |c| c.len());

    // A always sees its own commit. B's view may or may not include it
    // depending on whether the broadcast won the race with the close.
    assert_eq!(a_count, 1, "A retains its own commit");
    assert!(b_count <= 1, "B saw at most the one commit; got {b_count}");

    Ok(())
}
