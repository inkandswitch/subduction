//! An observer set via [`SubductionBuilder::heads_observer`] must be wired
//! into the built [`SyncHandler`]: fresh heads updates reach the observer,
//! stale ones (non-increasing per-peer counter) are filtered out.
//!
//! Regression guard for the builder silently discarding the observer.

use core::time::Duration;
use std::sync::Arc;

use future_form::Sendable;
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
    test_utils::heads::{RecordingObserver, settle},
};
use testresult::TestResult;

/// Poll until `predicate` holds or the deadline passes.
async fn wait_until(mut predicate: impl FnMut() -> bool, failure: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(tokio::time::Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn builder_observer_receives_heads_updates() -> TestResult {
    let observer = RecordingObserver::default();

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .heads_observer(observer.clone())
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    subduction.watch_heads(sedimentree_id).await;
    let heads = RemoteHeads {
        counter: 1,
        heads: vec![CommitId::new([7u8; 32])],
    };

    handle
        .inbound_tx
        .send(SyncMessage::HeadsUpdate {
            id: sedimentree_id,
            heads: heads.clone(),
        })
        .await?;

    wait_until(
        || !observer.deliveries().is_empty(),
        "observer set via the builder never received the heads update",
    )
    .await;

    assert_eq!(
        observer.deliveries(),
        vec![(sedimentree_id, peer_id, heads)]
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn stale_heads_updates_are_filtered() -> TestResult {
    let observer = RecordingObserver::default();

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .heads_observer(observer.clone())
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);

    let peer_id = PeerId::new([2u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let sedimentree_id = SedimentreeId::new([43u8; 32]);
    subduction.watch_heads(sedimentree_id).await;
    let update = |counter: u8| SyncMessage::HeadsUpdate {
        id: sedimentree_id,
        heads: RemoteHeads {
            counter: u64::from(counter),
            heads: vec![CommitId::new([counter; 32])],
        },
    };

    // Dispatch is spawned per message (post-#220), so messages from one
    // peer may be handled concurrently. Phase the sends with observer
    // barriers so freshness/staleness is deterministic regardless of
    // interleaving within each phase.

    // Phase 1: counter 0 is never fresh (initial per-peer watermark is 0);
    // counter 2 is fresh whichever handles first.
    handle.inbound_tx.send(update(0)).await?;
    handle.inbound_tx.send(update(2)).await?;
    wait_until(
        || observer.deliveries().len() == 1,
        "fresh heads update (counter 2) never reached the observer",
    )
    .await;

    // Phase 2: with the watermark at 2, the replayed 2 and the older 1 are
    // stale in every interleaving; 3 is fresh in every interleaving.
    handle.inbound_tx.send(update(2)).await?;
    handle.inbound_tx.send(update(1)).await?;
    handle.inbound_tx.send(update(3)).await?;
    wait_until(
        || observer.deliveries().len() >= 2,
        "fresh heads update (counter 3) never reached the observer",
    )
    .await;

    let counters: Vec<u64> = observer
        .deliveries()
        .into_iter()
        .map(|(_, _, heads)| heads.counter)
        .collect();
    assert_eq!(
        counters,
        vec![2, 3],
        "observer must see exactly the fresh updates"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// Heads for a tree the application never watched are dropped whatever
/// message carries them, including a `WatchHeadsResponse` nobody asked for.
/// A watched tree on the same connection anchors the negative.
#[tokio::test]
async fn unwatched_heads_are_dropped_at_the_gate() -> TestResult {
    use subduction_core::connection::message::{WatchHeadsResponse, WatchOutcome, WatchResult};

    let observer = RecordingObserver::default();

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .heads_observer(observer.clone())
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);

    let peer_id = PeerId::new([3u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let watched = SedimentreeId::new([44u8; 32]);
    let unwatched = SedimentreeId::new([45u8; 32]);
    subduction.watch_heads(watched).await;

    let heads = |counter: u64| RemoteHeads {
        counter,
        heads: vec![CommitId::new([7u8; 32])],
    };

    handle
        .inbound_tx
        .send(SyncMessage::HeadsUpdate {
            id: unwatched,
            heads: heads(1),
        })
        .await?;
    handle
        .inbound_tx
        .send(
            WatchHeadsResponse {
                results: vec![WatchResult {
                    id: unwatched,
                    outcome: WatchOutcome::Watching(heads(2)),
                }],
            }
            .into(),
        )
        .await?;
    handle
        .inbound_tx
        .send(SyncMessage::HeadsUpdate {
            id: watched,
            heads: heads(3),
        })
        .await?;

    wait_until(
        || !observer.deliveries().is_empty(),
        "the watched tree's heads never arrived",
    )
    .await;
    // Dispatch is concurrent per message, so the anchor may land first.
    settle().await;
    assert_eq!(observer.deliveries(), vec![(watched, peer_id, heads(3))]);

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// Empty `sender_heads` on a push are ignored (the sender could not read
/// them); the next non-empty report is delivered.
#[tokio::test]
async fn empty_sender_heads_on_a_push_are_ignored() -> TestResult {
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        loose_commit::LooseCommit,
    };
    use std::collections::BTreeSet;
    use subduction_core::test_utils::make_signer;
    use subduction_crypto::signed::Signed;

    let observer = RecordingObserver::default();

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .heads_observer(observer.clone())
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);

    let signer = make_signer(9);
    let peer_id = PeerId::from(signer.verifying_key());
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let id = SedimentreeId::new([46u8; 32]);
    subduction.watch_heads(id).await;

    let push = |n: u8, heads: Vec<CommitId>| {
        let signer = signer.clone();
        async move {
            let blob = Blob::new(vec![n; 8]);
            let commit = LooseCommit::new(
                id,
                CommitId::new([n; 32]),
                BTreeSet::new(),
                BlobMeta::new(&blob),
            );
            let sealed = Signed::seal::<Sendable, _>(&signer, commit)
                .await
                .into_signed();
            SyncMessage::LooseCommit {
                id,
                commit: sealed,
                blob,
                sender_heads: RemoteHeads {
                    counter: u64::from(n),
                    heads,
                },
            }
        }
    };

    handle.inbound_tx.send(push(1, Vec::new()).await).await?;
    handle
        .inbound_tx
        .send(push(2, vec![CommitId::new([2u8; 32])]).await)
        .await?;

    wait_until(
        || !observer.deliveries().is_empty(),
        "the non-empty heads never arrived",
    )
    .await;
    settle().await;
    assert_eq!(observer.heads(), vec![vec![CommitId::new([2u8; 32])]]);

    actor_task.abort();
    listener_task.abort();
    Ok(())
}
