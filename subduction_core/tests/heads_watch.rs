//! Heads reach the observer only for watched sedimentrees; see
//! `design/sync/subscriptions.md` § Heads Watches.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm
)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};
use subduction_core::{
    subduction::fragment_batch_item::FragmentBatchItem,
    test_utils::{
        ChannelConn,
        heads::{
            FlagPolicy, RecordingObserver, WatchedNode, dial_watched, settle, spawn_watched_node,
        },
        make_head, make_peer_id, make_tree_id, wait_until,
    },
    timeout::call::CallTimeout,
};
use testresult::TestResult;

type Node = WatchedNode<ChannelConn>;

const DOC: SedimentreeId = make_tree_id(0xD0);
const OTHER: SedimentreeId = make_tree_id(0x0E);

fn blob(n: u8) -> Blob {
    Blob::new(vec![n; 8])
}

async fn add_commit(node: &Node, doc: SedimentreeId, n: u8) -> TestResult {
    node.add_commit(doc, make_head(n), BTreeSet::new(), blob(n))
        .await?;
    Ok(())
}

/// Two connected nodes; `a` dials `b`.
async fn pair(
    a_seed: u8,
    b_seed: u8,
) -> Result<(Node, RecordingObserver, Node, RecordingObserver), testresult::TestError> {
    let (a, a_obs, a_peer) = spawn_watched_node(a_seed, FlagPolicy::allow_all());
    let (b, b_obs, b_peer) = spawn_watched_node(b_seed, FlagPolicy::allow_all());
    dial_watched(&a, a_peer, &b, b_peer).await?;
    Ok((a, a_obs, b, b_obs))
}

async fn snapshot_arrived(obs: &RecordingObserver, at_least: usize) -> bool {
    wait_until(|| async { obs.count() >= at_least }).await
}

/// Covers the three unwatched paths (`responder_heads`, `sender_heads`, the
/// push ack), then watches as the positive control.
#[tokio::test]
async fn unwatched_trees_never_reach_the_observer() -> TestResult {
    let (a, a_obs, b, b_obs) = pair(1, 2).await?;

    add_commit(&b, DOC, 1).await?;
    a.sync_with_peer(&make_peer_id(2), DOC, true, CallTimeout::TimeoutMillis(500))
        .await?;
    add_commit(&b, DOC, 2).await?;
    add_commit(&a, DOC, 3).await?;
    settle().await;

    assert!(a_obs.deliveries().is_empty(), "{:?}", a_obs.deliveries());
    assert!(b_obs.deliveries().is_empty(), "{:?}", b_obs.deliveries());

    a.watch_heads(DOC).await;
    assert!(
        snapshot_arrived(&a_obs, 1).await,
        "control watch not reported"
    );
    Ok(())
}

#[tokio::test]
async fn watch_delivers_a_snapshot() -> TestResult {
    let (a, a_obs, b, _) = pair(3, 4).await?;

    add_commit(&b, DOC, 1).await?;
    a.watch_heads(DOC).await;

    assert!(snapshot_arrived(&a_obs, 1).await);
    assert_eq!(
        a_obs.deliveries().first().map(|(_, peer, _)| *peer),
        Some(make_peer_id(4))
    );
    assert_eq!(a_obs.heads_for(DOC), vec![vec![make_head(1)]]);
    Ok(())
}

/// A watch on a tree the peer does not hold yet is answered with empty
/// heads, then followed by the first commit.
#[tokio::test]
async fn watch_on_absent_tree_reports_empty_then_first_commit() -> TestResult {
    let (a, a_obs, b, _) = pair(5, 6).await?;

    a.watch_heads(DOC).await;
    assert!(snapshot_arrived(&a_obs, 1).await);
    assert_eq!(a_obs.heads_for(DOC), vec![Vec::<CommitId>::new()]);

    add_commit(&b, DOC, 1).await?;
    assert!(snapshot_arrived(&a_obs, 2).await);
    assert_eq!(a_obs.heads_for(DOC), vec![vec![], vec![make_head(1)]]);
    Ok(())
}

/// The observer sees one report per change whether or not the watcher is
/// also a subscriber. (Exactly-once on the wire is `on_the_wire`'s job.)
#[tokio::test]
async fn observer_sees_one_report_per_change() -> TestResult {
    for subscribe in [false, true] {
        let (a, a_obs, b, _) = pair(7, 8).await?;

        add_commit(&b, DOC, 1).await?;
        if subscribe {
            a.sync_with_peer(&make_peer_id(8), DOC, true, CallTimeout::TimeoutMillis(500))
                .await?;
        }
        a.watch_heads(DOC).await;
        assert!(snapshot_arrived(&a_obs, 1).await);

        for n in 2..=4u8 {
            add_commit(&b, DOC, n).await?;
            assert!(
                wait_until(|| async {
                    a_obs
                        .heads_for(DOC)
                        .last()
                        .is_some_and(|h| h.contains(&make_head(n)))
                })
                .await
            );
        }
        settle().await;

        assert_eq!(
            a_obs.count(),
            4,
            "subscribe={subscribe}: {:?}",
            a_obs.deliveries()
        );
    }
    Ok(())
}

/// After `unwatch_heads`, a re-watch delivers the snapshot again even though
/// the heads are unchanged: the notifier forgot the tree.
#[tokio::test]
async fn rewatch_after_unwatch_redelivers_the_snapshot() -> TestResult {
    let (a, a_obs, b, _) = pair(9, 10).await?;

    add_commit(&b, DOC, 1).await?;
    a.watch_heads(DOC).await;
    assert!(snapshot_arrived(&a_obs, 1).await);

    a.unwatch_heads(DOC).await;
    add_commit(&b, DOC, 2).await?;
    settle().await;
    assert_eq!(
        a_obs.count(),
        1,
        "heads after unwatch: {:?}",
        a_obs.deliveries()
    );

    a.watch_heads(DOC).await;
    assert!(
        snapshot_arrived(&a_obs, 2).await,
        "re-watch snapshot suppressed"
    );
    assert_eq!(
        a_obs.heads_for(DOC).last(),
        Some(&vec![make_head(1), make_head(2)])
    );
    Ok(())
}

/// C watches A. A pulls from B; C is told A's new heads although A
/// originated nothing (the requester-side ingest path).
#[tokio::test]
async fn watcher_hears_changes_the_peer_pulled_in() -> TestResult {
    let (a, _, a_peer) = spawn_watched_node(11, FlagPolicy::allow_all());
    let (b, _, b_peer) = spawn_watched_node(12, FlagPolicy::allow_all());
    let (c, c_obs, c_peer) = spawn_watched_node(13, FlagPolicy::allow_all());
    dial_watched(&a, a_peer, &b, b_peer).await?;
    dial_watched(&c, c_peer, &a, a_peer).await?;

    c.watch_heads(DOC).await;
    assert!(snapshot_arrived(&c_obs, 1).await);

    add_commit(&b, DOC, 1).await?;
    a.sync_with_peer(&b_peer, DOC, false, CallTimeout::TimeoutMillis(500))
        .await?;

    assert!(
        snapshot_arrived(&c_obs, 2).await,
        "pulled change never reached the watcher"
    );
    let (_, from, heads) = c_obs.deliveries()[1].clone();
    assert_eq!((from, heads.heads), (a_peer, vec![make_head(1)]));
    Ok(())
}

/// Watches survive the peer's session: reconnecting replays them.
#[tokio::test]
async fn watch_is_replayed_on_reconnect() -> TestResult {
    let (a, a_obs, a_peer) = spawn_watched_node(15, FlagPolicy::allow_all());
    let (b, _, b_peer) = spawn_watched_node(16, FlagPolicy::allow_all());
    dial_watched(&a, a_peer, &b, b_peer).await?;

    add_commit(&b, DOC, 1).await?;
    a.watch_heads(DOC).await;
    assert!(snapshot_arrived(&a_obs, 1).await);

    a.disconnect_from_peer(&b_peer).await?;
    b.disconnect_from_peer(&a_peer).await?;
    dial_watched(&a, a_peer, &b, b_peer).await?;

    assert!(snapshot_arrived(&a_obs, 2).await, "snapshot not replayed");
    add_commit(&b, DOC, 2).await?;
    assert!(snapshot_arrived(&a_obs, 3).await);

    assert!(
        a_obs
            .deliveries()
            .iter()
            .all(|(_, peer, _)| *peer == b_peer)
    );
    assert_eq!(
        a_obs.heads_for(DOC),
        vec![
            vec![make_head(1)],
            vec![make_head(1)],
            vec![make_head(1), make_head(2)],
        ]
    );
    Ok(())
}

/// Every write API reports to watchers. Each row spends one `HeadsChanged`
/// witness; a write path that skips `propagate` gets a `must_use` warning,
/// and a new API that bypasses `HeadsChanged` shows up here as a missing row.
mod every_write_path_notifies_watchers {
    use super::*;

    async fn watched_pair() -> Result<(Node, RecordingObserver), testresult::TestError> {
        let (a, a_obs, b, _) = pair(30, 31).await?;
        a.watch_heads(DOC).await;
        assert!(snapshot_arrived(&a_obs, 1).await);
        Ok((b, a_obs))
    }

    async fn expect_report<F, Fut>(write: F) -> TestResult
    where
        F: FnOnce(Node) -> Fut,
        Fut: core::future::Future<Output = TestResult>,
    {
        let (b, a_obs) = watched_pair().await?;
        let before = a_obs.count();
        write(b).await?;
        assert!(
            snapshot_arrived(&a_obs, before + 1).await,
            "watcher was not told about the change"
        );
        Ok(())
    }

    #[tokio::test]
    async fn add_commit() -> TestResult {
        expect_report(|b| async move { super::add_commit(&b, DOC, 1).await }).await
    }

    #[tokio::test]
    async fn add_fragment() -> TestResult {
        expect_report(|b| async move {
            b.add_fragment(DOC, make_head(1), BTreeSet::new(), &[], blob(1))
                .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_commits_batch() -> TestResult {
        expect_report(|b| async move {
            b.add_commits_batch(
                DOC,
                vec![(make_head(1), BTreeSet::new(), blob(1))],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_fragments_batch() -> TestResult {
        expect_report(|b| async move {
            b.add_fragments_batch(
                DOC,
                vec![FragmentBatchItem {
                    head: make_head(1),
                    boundary: BTreeSet::new(),
                    checkpoints: Vec::new(),
                    blob: blob(1),
                }],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_built_batch() -> TestResult {
        expect_report(|b| async move {
            let blob = blob(1);
            let c = LooseCommit::new(DOC, make_head(1), BTreeSet::new(), BlobMeta::new(&blob));
            b.add_built_batch(
                DOC,
                vec![(c, blob)],
                Vec::new(),
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn add_sedimentree() -> TestResult {
        expect_report(|b| async move {
            let blob = blob(1);
            let c = LooseCommit::new(DOC, make_head(1), BTreeSet::new(), BlobMeta::new(&blob));
            let f = Fragment::new(
                DOC,
                make_head(2),
                BTreeSet::new(),
                &[],
                BlobMeta::new(&blob),
            );
            b.add_sedimentree(
                DOC,
                Sedimentree::new(vec![f], vec![c]),
                vec![blob],
                CallTimeout::TimeoutMillis(500),
            )
            .await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn remove_sedimentree() -> TestResult {
        let (b, a_obs) = watched_pair().await?;
        super::add_commit(&b, DOC, 1).await?;
        assert!(snapshot_arrived(&a_obs, 2).await);

        b.remove_sedimentree(DOC).await?;
        assert!(
            wait_until(|| async { a_obs.heads_for(DOC).last().is_some_and(Vec::is_empty) }).await,
            "removal never reported as empty heads"
        );
        Ok(())
    }

    /// Re-adding data the tree already holds is `Unchanged`: nothing goes out.
    #[tokio::test]
    async fn duplicate_batch_is_unchanged() -> TestResult {
        let (b, a_obs) = watched_pair().await?;
        let items = vec![(make_head(1), BTreeSet::new(), blob(1))];
        b.add_commits_batch(DOC, items.clone(), CallTimeout::TimeoutMillis(500))
            .await?;
        assert!(snapshot_arrived(&a_obs, 2).await);

        b.add_commits_batch(DOC, items, CallTimeout::TimeoutMillis(500))
            .await?;
        settle().await;
        assert_eq!(a_obs.count(), 2, "{:?}", a_obs.deliveries());
        Ok(())
    }

    /// `store_*` is local-only; the watcher hears nothing until a later sync.
    /// Positive control: `add_commit` on the same pair reports.
    #[tokio::test]
    async fn store_is_local_only() -> TestResult {
        let (b, a_obs) = watched_pair().await?;
        let before = a_obs.count();
        b.store_commits_batch(DOC, vec![(make_head(1), BTreeSet::new(), blob(1))])
            .await?;
        settle().await;
        assert_eq!(a_obs.count(), before, "{:?}", a_obs.deliveries());

        super::add_commit(&b, DOC, 2).await?;
        assert!(
            snapshot_arrived(&a_obs, before + 1).await,
            "control write not reported"
        );
        Ok(())
    }
}

/// Wire-level view from mock peers: exactly one heads-carrying frame per
/// change, and the responder's own bookkeeping.
mod on_the_wire {
    use future_form::Sendable;
    use sedimentree_core::{crypto::fingerprint::FingerprintSeed, sedimentree::FingerprintSummary};
    use subduction_core::{
        authenticated::{Authenticated, Direction},
        connection::{
            message::{
                BatchSyncRequest, RequestId, SyncMessage, UnwatchHeads, WatchHeads,
                WatchHeadsResponse, WatchOutcome, WatchResult,
            },
            test_utils::ChannelMockConnectionHandle,
        },
        peer::id::PeerId,
        remote_heads::{RemoteHeads, watches::MAX_WATCHES_PER_PEER},
        subduction::WATCH_HEADS_BATCH,
        test_utils::MockConn,
    };
    use subduction_crypto::signed::Signed;

    use super::*;

    type Handle = ChannelMockConnectionHandle<SyncMessage>;
    type MockNode = WatchedNode<MockConn>;

    /// A real node with one mock peer attached.
    async fn node_with_mock(
        seed: u8,
        mock_seed: u8,
        policy: FlagPolicy,
    ) -> Result<(MockNode, RecordingObserver, Handle), testresult::TestError> {
        let (node, obs, _) = spawn_watched_node(seed, policy);
        let handle = attach(&node, make_peer_id(mock_seed)).await?;
        Ok((node, obs, handle))
    }

    async fn attach(node: &MockNode, peer: PeerId) -> Result<Handle, testresult::TestError> {
        let (conn, handle) = MockConn::new_with_handle(peer);
        node.add_connection(Authenticated::new_for_test(conn, peer, Direction::Accepted))
            .await?;
        Ok(handle)
    }

    /// Wait for the first frame matching `want`, let the round settle, and
    /// return everything received.
    async fn frames_after(
        handle: &Handle,
        want: impl Fn(&SyncMessage) -> bool,
    ) -> Vec<SyncMessage> {
        let frames = std::sync::Mutex::new(Vec::new());
        let found = wait_until(|| async {
            let mut frames = frames.lock().expect("poisoned");
            while let Ok(msg) = handle.outbound_rx.try_recv() {
                let done = want(&msg);
                frames.push(msg);
                if done {
                    return true;
                }
            }
            false
        })
        .await;
        let mut frames = frames.into_inner().expect("poisoned");
        assert!(found, "expected frame never arrived: {frames:?}");
        settle().await;
        while let Ok(msg) = handle.outbound_rx.try_recv() {
            frames.push(msg);
        }
        frames
    }

    fn drain(handle: &Handle) -> Vec<SyncMessage> {
        let mut frames = Vec::new();
        while let Ok(f) = handle.outbound_rx.try_recv() {
            frames.push(f);
        }
        frames
    }

    fn heads_frames(frames: &[SyncMessage]) -> Vec<Vec<CommitId>> {
        frames
            .iter()
            .filter_map(|f| match f {
                SyncMessage::HeadsUpdate { heads, .. } => Some(heads.heads.clone()),
                _ => None,
            })
            .collect()
    }

    const fn is_heads_update(f: &SyncMessage) -> bool {
        matches!(f, SyncMessage::HeadsUpdate { .. })
    }

    async fn watch(
        handle: &Handle,
        ids: Vec<SedimentreeId>,
    ) -> Result<Vec<WatchResult>, testresult::TestError> {
        handle.inbound_tx.send(WatchHeads { ids }.into()).await?;
        let frames =
            frames_after(handle, |f| matches!(f, SyncMessage::WatchHeadsResponse(_))).await;
        frames
            .into_iter()
            .find_map(|f| match f {
                SyncMessage::WatchHeadsResponse(WatchHeadsResponse { results }) => Some(results),
                _ => None,
            })
            .ok_or_else(|| "no WatchHeadsResponse".into())
    }

    fn all_watching(results: &[WatchResult]) -> bool {
        results
            .iter()
            .all(|r| matches!(r.outcome, WatchOutcome::Watching(_)))
    }

    #[tokio::test]
    async fn one_heads_frame_per_change_in_every_shape() -> TestResult {
        let (a, _, b) = node_with_mock(40, 41, FlagPolicy::allow_all()).await?;
        let b_peer = make_peer_id(41);

        let results = watch(&b, vec![DOC]).await?;
        assert!(matches!(
            results.as_slice(),
            [WatchResult { id, outcome: WatchOutcome::Watching(h) }] if *id == DOC && h.heads.is_empty()
        ));

        // Watch only: a standalone HeadsUpdate.
        a.add_commit(DOC, make_head(1), BTreeSet::new(), blob(1))
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(frames.len(), 1, "one HeadsUpdate, no pushes: {frames:?}");

        // Watch + subscribe: heads ride the push, no separate HeadsUpdate.
        b.inbound_tx
            .send(SyncMessage::BatchSyncRequest(BatchSyncRequest {
                id: DOC,
                req_id: RequestId {
                    requestor: b_peer,
                    nonce: 1,
                },
                fingerprint_summary: FingerprintSummary::new(
                    FingerprintSeed::new(1, 2),
                    BTreeSet::new(),
                    BTreeSet::new(),
                ),
                subscribe: true,
            }))
            .await?;
        frames_after(&b, |f| matches!(f, SyncMessage::BatchSyncResponse(_))).await;
        a.add_commit(DOC, make_head(2), BTreeSet::new(), blob(2))
            .await?;
        let frames = frames_after(&b, |f| matches!(f, SyncMessage::LooseCommit { .. })).await;
        assert_eq!(frames.len(), 1, "one push and nothing else: {frames:?}");

        // B originates: the ack is the only heads frame.
        let blob = blob(3);
        let c = LooseCommit::new(DOC, make_head(3), BTreeSet::new(), BlobMeta::new(&blob));
        let signed = Signed::seal::<Sendable, _>(&subduction_core::test_utils::make_signer(41), c)
            .await
            .into_signed();
        b.inbound_tx
            .send(SyncMessage::LooseCommit {
                id: DOC,
                commit: signed,
                blob,
                sender_heads: RemoteHeads {
                    counter: 1,
                    heads: vec![make_head(3)],
                },
            })
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(frames.len(), 1, "exactly the ack: {frames:?}");
        Ok(())
    }

    /// `UnwatchHeads` stops `HeadsUpdate`s to that peer while another watcher
    /// (the positive control) keeps receiving them.
    #[tokio::test]
    async fn unwatch_stops_heads_updates_to_that_peer() -> TestResult {
        let (a, _, b) = node_with_mock(42, 43, FlagPolicy::allow_all()).await?;
        let c = attach(&a, make_peer_id(44)).await?;

        watch(&b, vec![DOC]).await?;
        watch(&c, vec![DOC]).await?;

        b.inbound_tx
            .send(UnwatchHeads { ids: vec![DOC] }.into())
            .await?;
        settle().await;

        a.add_commit(DOC, make_head(1), BTreeSet::new(), blob(1))
            .await?;
        let c_frames = frames_after(&c, is_heads_update).await;
        assert_eq!(
            heads_frames(&c_frames).len(),
            1,
            "control watcher: {c_frames:?}"
        );
        assert!(
            heads_frames(&drain(&b)).is_empty(),
            "unwatched peer still told"
        );
        Ok(())
    }

    /// The table, not the per-message index, refuses past the cap; ids the
    /// peer already holds are re-confirmed regardless; unwatching frees a slot.
    #[tokio::test]
    async fn watches_past_cap_are_refused_until_a_slot_frees() -> TestResult {
        let (a, _, b) = node_with_mock(45, 46, FlagPolicy::allow_all()).await?;
        let ids: Vec<SedimentreeId> = (0..=MAX_WATCHES_PER_PEER)
            .map(|n| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&n.to_le_bytes());
                SedimentreeId::new(bytes)
            })
            .collect();
        let extra = ids[MAX_WATCHES_PER_PEER];

        assert!(all_watching(
            &watch(&b, ids[..MAX_WATCHES_PER_PEER].to_vec()).await?
        ));

        // Second frame: `extra` is new and refused by the table; `ids[0]` is
        // already held and re-confirmed despite the peer being at cap.
        let results = watch(&b, vec![extra, ids[0]]).await?;
        assert!(
            matches!(results[0].outcome, WatchOutcome::AtCapacity),
            "{results:?}"
        );
        assert!(
            matches!(results[1].outcome, WatchOutcome::Watching(_)),
            "{results:?}"
        );

        // Not recorded: a change to the refused tree is not reported.
        a.add_commit(extra, make_head(1), BTreeSet::new(), blob(1))
            .await?;
        settle().await;
        assert!(heads_frames(&drain(&b)).is_empty());

        b.inbound_tx
            .send(UnwatchHeads { ids: vec![ids[0]] }.into())
            .await?;
        assert!(all_watching(&watch(&b, vec![extra]).await?));
        a.add_commit(extra, make_head(2), BTreeSet::new(), blob(2))
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        assert_eq!(heads_frames(&frames).len(), 1, "{frames:?}");
        Ok(())
    }

    /// A watch attempted while refused is answered `Unauthorized`; a watcher
    /// whose fetch right is revoked hears nothing and hears again when it is
    /// restored, without re-watching.
    #[tokio::test]
    async fn revoked_watcher_is_filtered_at_fanout_and_kept() -> TestResult {
        let policy = FlagPolicy::allow_all();
        let (a, _, b) = node_with_mock(50, 51, policy.clone()).await?;

        assert!(all_watching(&watch(&b, vec![DOC]).await?));

        policy.set_fetch(false);
        let results = watch(&b, vec![OTHER]).await?;
        assert!(matches!(
            results.as_slice(),
            [WatchResult { id, outcome: WatchOutcome::Unauthorized }] if *id == OTHER
        ));

        a.add_commit(DOC, make_head(1), BTreeSet::new(), blob(1))
            .await?;
        settle().await;
        assert!(
            heads_frames(&drain(&b)).is_empty(),
            "revoked watcher was told"
        );

        policy.set_fetch(true);
        a.add_commit(DOC, make_head(2), BTreeSet::new(), blob(2))
            .await?;
        let frames = frames_after(&b, is_heads_update).await;
        let reported: Vec<BTreeSet<CommitId>> = heads_frames(&frames)
            .into_iter()
            .map(|h| h.into_iter().collect())
            .collect();
        assert_eq!(
            reported,
            vec![BTreeSet::from([make_head(1), make_head(2)])],
            "exactly the post-restore frame: {frames:?}"
        );
        Ok(())
    }

    /// `watch_heads` is idempotent, `unwatch_heads` of a never-watched tree
    /// is silent, a peer connecting after `watch_heads` gets the replay in
    /// bounded chunks, and `disconnect_all` clears the watchers.
    #[tokio::test]
    async fn watch_lifecycle_on_the_wire() -> TestResult {
        let (a, _, b) = node_with_mock(60, 61, FlagPolicy::allow_all()).await?;

        a.watch_heads(DOC).await;
        a.watch_heads(DOC).await;
        a.unwatch_heads(OTHER).await;
        settle().await;
        let frames = drain(&b);
        assert!(
            matches!(frames.as_slice(), [SyncMessage::WatchHeads(WatchHeads { ids })] if ids == &[DOC]),
            "one WatchHeads, no UnwatchHeads: {frames:?}"
        );

        // A fresh peer receives the replay, chunked.
        for n in 0..WATCH_HEADS_BATCH {
            let mut bytes = [0xAAu8; 32];
            bytes[..8].copy_from_slice(&n.to_le_bytes());
            a.watch_heads(SedimentreeId::new(bytes)).await;
        }
        let c = attach(&a, make_peer_id(62)).await?;
        settle().await;
        let sizes: Vec<usize> = drain(&c)
            .iter()
            .filter_map(|f| match f {
                SyncMessage::WatchHeads(WatchHeads { ids }) => Some(ids.len()),
                _ => None,
            })
            .collect();
        assert_eq!(sizes, vec![WATCH_HEADS_BATCH, 1]);

        // Watchers do not survive `disconnect_all`.
        drain(&b);
        watch(&b, vec![DOC]).await?;
        a.disconnect_all().await?;
        let b = attach(&a, make_peer_id(61)).await?;
        settle().await;
        drain(&b);
        a.add_commit(DOC, make_head(1), BTreeSet::new(), blob(1))
            .await?;
        settle().await;
        assert!(
            heads_frames(&drain(&b)).is_empty(),
            "watcher survived disconnect_all"
        );
        assert!(all_watching(&watch(&b, vec![DOC]).await?));
        a.add_commit(DOC, make_head(2), BTreeSet::new(), blob(2))
            .await?;
        assert_eq!(
            heads_frames(&frames_after(&b, is_heads_update).await).len(),
            1
        );
        Ok(())
    }
}
