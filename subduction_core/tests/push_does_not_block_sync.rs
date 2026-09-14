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

use std::{collections::BTreeSet, time::Duration};

use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    connection::test_utils::TokioTimeout,
    test_utils::{dial_pausable, make_blob, make_head, spawn_node},
    timeout::call::CallTimeout,
};
use testresult::TestResult;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);

/// A sync round with one wedged subscriber must finish well inside this.
const BOUND: Duration = Duration::from_secs(3);

#[tokio::test]
async fn wedged_subscriber_does_not_block_relay_sync() -> TestResult {
    let (a, a_peer) = spawn_node(1, TokioTimeout);
    let (r, r_peer) = spawn_node(2, TokioTimeout);
    let (b, b_peer) = spawn_node(3, TokioTimeout);
    let (_a_side, r_to_a) = dial_pausable(&a, a_peer, &r, r_peer).await;
    dial_pausable(&r, r_peer, &b, b_peer).await;
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
    let (a, a_peer) = spawn_node(4, TokioTimeout);
    let (r, r_peer) = spawn_node(5, TokioTimeout);
    let (b, b_peer) = spawn_node(6, TokioTimeout);
    let (_a_side, r_to_a) = dial_pausable(&a, a_peer, &r, r_peer).await;
    dial_pausable(&r, r_peer, &b, b_peer).await;
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
