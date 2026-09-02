//! An observer set via [`SubductionBuilder::heads_observer`] must be wired
//! into the built [`SyncHandler`]: fresh heads updates reach the observer,
//! stale ones (non-increasing per-peer counter) are filtered out.
//!
//! Regression guard for the builder silently discarding the observer.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use std::sync::{Arc, Mutex};

use future_form::Sendable;
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

/// Records every notification it receives.
#[derive(Clone, Debug, Default)]
struct RecordingObserver {
    seen: Arc<Mutex<Vec<(SedimentreeId, PeerId, RemoteHeads)>>>,
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<(SedimentreeId, PeerId, RemoteHeads)> {
        self.seen.lock().expect("observer mutex poisoned").clone()
    }
}

impl RemoteHeadsObserver for RecordingObserver {
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.seen
            .lock()
            .expect("observer mutex poisoned")
            .push((id, peer, heads));
    }
}

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
        || !observer.snapshot().is_empty(),
        "observer set via the builder never received the heads update",
    )
    .await;

    assert_eq!(observer.snapshot(), vec![(sedimentree_id, peer_id, heads)]);

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
        || observer.snapshot().len() == 1,
        "fresh heads update (counter 2) never reached the observer",
    )
    .await;

    // Phase 2: with the watermark at 2, the replayed 2 and the older 1 are
    // stale in every interleaving; 3 is fresh in every interleaving.
    handle.inbound_tx.send(update(2)).await?;
    handle.inbound_tx.send(update(1)).await?;
    handle.inbound_tx.send(update(3)).await?;
    wait_until(
        || observer.snapshot().len() >= 2,
        "fresh heads update (counter 3) never reached the observer",
    )
    .await;

    let counters: Vec<u64> = observer
        .snapshot()
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
