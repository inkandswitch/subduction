//! Heads notifications must quiesce once two peers have converged.
//!
//! An observer that syncs on every notification must terminate once peers
//! converge: each sync answers with `responder_heads`, and reporting
//! unchanged heads would restart the loop.
//!
//! Unit tests of the filter itself live in `subduction_core::remote_heads`;
//! these tests exercise the wiring through a running node. Each watches the
//! tree first, since only watched trees reach the observer (see
//! `tests/heads_watch.rs`).

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{message::SyncMessage, test_utils::ChannelTransport},
    handler::Handler,
    remote_heads::RemoteHeads,
    test_utils::{
        ChannelConn,
        heads::{
            FlagPolicy, RecordingObserver, WatchedNode, dial_watched, spawn_watched_node,
            spawn_watched_node_with_handler,
        },
        make_head, make_peer_id, make_signer, make_tree_id,
    },
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

type Node = WatchedNode<ChannelConn>;

async fn pair(
    a_seed: u8,
    b_seed: u8,
) -> Result<(Node, RecordingObserver, Node), testresult::TestError> {
    let (a, a_obs, a_peer) = spawn_watched_node(a_seed, FlagPolicy::allow_all());
    let (b, _, b_peer) = spawn_watched_node(b_seed, FlagPolicy::allow_all());
    dial_watched(&a, a_peer, &b, b_peer).await?;
    Ok((a, a_obs, b))
}

async fn add_commit(node: &Node, doc: SedimentreeId, n: u8) -> TestResult {
    node.add_commit(doc, make_head(n), BTreeSet::new(), Blob::new(vec![n; 16]))
        .await?;
    Ok(())
}

/// `sync_with_peer` reports `responder_heads` before returning, so no settling
/// is needed between rounds.
#[tokio::test]
async fn repeated_sync_of_converged_trees_stops_notifying() -> TestResult {
    let (a, observer, b) = pair(1, 2).await?;
    let doc = make_tree_id(9);

    add_commit(&b, doc, 1).await?;
    a.watch_heads(doc).await;

    a.sync_with_peer(&make_peer_id(2), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    let after_first = observer.count();
    assert!(after_first >= 1, "the first sync reports B's heads");

    for _ in 0..5 {
        a.sync_with_peer(&make_peer_id(2), doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
    }

    let distinct: BTreeSet<Vec<CommitId>> = observer.heads().into_iter().collect();
    assert_eq!(distinct.len(), 1, "one distinct heads value: {distinct:?}");
    assert_eq!(
        observer.count(),
        after_first,
        "syncing a converged tree re-reported unchanged heads"
    );
    Ok(())
}

#[tokio::test]
async fn changed_heads_still_notify() -> TestResult {
    let (a, observer, b) = pair(3, 4).await?;
    let doc = make_tree_id(8);

    for i in 1..=3u8 {
        add_commit(&b, doc, i).await?;
        if i == 1 {
            a.watch_heads(doc).await;
        }
        a.sync_with_peer(&make_peer_id(4), doc, true, CallTimeout::TimeoutMillis(500))
            .await?;
    }

    let distinct: BTreeSet<Vec<CommitId>> = observer.heads().into_iter().collect();
    assert_eq!(distinct.len(), 3, "each new commit is a distinct report");
    Ok(())
}

/// A proactive disconnect must reach `FilteredHeadsNotifier::remove_peer`
/// through `Handler::on_peer_disconnect`, so a reconnecting peer's unchanged
/// heads are reported again.
#[tokio::test]
async fn disconnect_through_the_api_clears_filter_state() -> TestResult {
    let (a, observer, b) = pair(5, 6).await?;
    let doc = make_tree_id(3);

    add_commit(&b, doc, 1).await?;
    a.watch_heads(doc).await;
    a.sync_with_peer(&make_peer_id(6), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;
    let before = observer.count();
    assert!(before >= 1);

    a.disconnect_from_peer(&make_peer_id(6)).await?;

    dial_watched(&a, make_peer_id(5), &b, make_peer_id(6)).await?;
    a.sync_with_peer(&make_peer_id(6), doc, true, CallTimeout::TimeoutMillis(500))
        .await?;

    assert!(
        observer.count() > before,
        "after a disconnect the peer's heads are reported again"
    );
    Ok(())
}

/// Heads on a push the storage policy refuses never reach the observer.
/// Drives the handler directly so the outcome is observable without settling.
mod policy_denied_pushes {
    use super::*;

    const DOC: SedimentreeId = make_tree_id(13);
    fn refused() -> CommitId {
        make_head(1)
    }

    fn sender_heads() -> RemoteHeads {
        RemoteHeads {
            counter: 1,
            heads: vec![refused()],
        }
    }

    async fn assert_not_notified(message: SyncMessage) -> TestResult {
        let (locked, handler, observer, _) =
            spawn_watched_node_with_handler::<ChannelConn>(21, FlagPolicy::deny_put());
        // Open the watch gate so only the policy can stop delivery.
        locked.watch_heads(DOC).await;
        let (transport, _far_end) = ChannelTransport::pair();
        let conn = Authenticated::new_for_test(
            MessageTransport::new(transport),
            make_peer_id(22),
            Direction::Accepted,
        );

        handler.handle(&conn, message).await?;

        assert_eq!(
            locked.get_commits(DOC).await.map_or(0, |c| c.len()),
            0,
            "the policy should have refused the write"
        );
        assert!(
            observer.deliveries().is_empty(),
            "heads from a refused push reached the observer: {:?}",
            observer.deliveries()
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit() -> TestResult {
        let blob = Blob::new(b"refused".to_vec());
        let commit = LooseCommit::new(DOC, refused(), BTreeSet::new(), BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&make_signer(22), commit).await;

        assert_not_notified(SyncMessage::LooseCommit {
            id: DOC,
            commit: signed.into_signed(),
            blob,
            sender_heads: sender_heads(),
        })
        .await
    }

    #[tokio::test]
    async fn fragment() -> TestResult {
        let blob = Blob::new(b"refused".to_vec());
        let fragment = Fragment::new(DOC, refused(), BTreeSet::new(), &[], BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&make_signer(22), fragment).await;

        assert_not_notified(SyncMessage::Fragment {
            id: DOC,
            fragment: signed.into_signed(),
            blob,
            sender_heads: sender_heads(),
        })
        .await
    }
}
