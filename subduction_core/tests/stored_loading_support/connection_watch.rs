//! Connection discovery and reconnection belong to the lifetime of a loader.
use super::{Node, connect, node};
use crate::next;
use futures::StreamExt;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::{collections::BTreeSet, time::Duration};
use subduction_core::{
    peer::id::PeerId,
    storage::memory::MemoryStorage,
    stored::{StoredEvent, StoredItem, StoredUpdates},
};

async fn seed(node: &Node, id: SedimentreeId, value: u8) {
    node.store_commit(
        id,
        CommitId::new([value; 32]),
        BTreeSet::new(),
        Blob::new(vec![value]),
    )
    .await
    .expect("seed");
}
async fn synced(stream: &mut StoredUpdates, peer: PeerId) {
    loop {
        if matches!(next(stream).await, StoredEvent::PeerSynced { peer_id } if peer_id == peer) {
            return;
        }
    }
}
async fn contains(node: &Node, id: SedimentreeId, value: u8) {
    assert!(
        node.get_blobs(id)
            .await
            .expect("storage")
            .expect("document")
            .iter()
            .any(|blob| blob.as_slice() == [value])
    );
}
async fn push(node: &Node, stream: &mut StoredUpdates, id: SedimentreeId, value: u8) {
    node.add_commit(
        id,
        CommitId::new([value; 32]),
        BTreeSet::new(),
        Blob::new(vec![value]),
    )
    .await
    .expect("push");
    loop {
        if matches!(next(stream).await, StoredEvent::Item(StoredItem::Commit(commit)) if commit.blob().as_slice() == [value])
        {
            break;
        }
    }
}

#[tokio::test]
async fn empty_loader_discovers_later_peer_and_receives_pushes() {
    let a = node(120, MemoryStorage::new());
    let b = node(121, MemoryStorage::new());
    let id = SedimentreeId::new([120; 32]);
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    seed(&b, id, 1).await;
    connect(&a, &b).await;
    synced(&mut stream, b.peer_id()).await;
    contains(&a, id, 1).await;
    push(&b, &mut stream, id, 2).await;
    contains(&a, id, 2).await;
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn cached_and_synced_loader_discovers_new_peers_and_restores_reconnections() {
    let a = node(122, MemoryStorage::new());
    let b = node(123, MemoryStorage::new());
    let c = node(124, MemoryStorage::new());
    let id = SedimentreeId::new([122; 32]);
    seed(&a, id, 1).await;
    seed(&b, id, 2).await;
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    synced(&mut stream, b.peer_id()).await;
    contains(&a, id, 2).await;

    seed(&c, id, 3).await;
    connect(&a, &c).await;
    synced(&mut stream, c.peer_id()).await;
    contains(&a, id, 3).await;
    push(&c, &mut stream, id, 4).await;

    a.disconnect_from_peer(&b.peer_id())
        .await
        .expect("disconnect a");
    b.disconnect_from_peer(&a.peer_id())
        .await
        .expect("disconnect b");
    seed(&b, id, 5).await;
    connect(&a, &b).await;
    synced(&mut stream, b.peer_id()).await;
    contains(&a, id, 5).await;
    assert!(b.get_subscribers(id).await.contains(&a.peer_id()));
    push(&b, &mut stream, id, 6).await;
    contains(&a, id, 6).await;
    for node in [a, b, c] {
        node.shutdown();
    }
}

#[tokio::test]
async fn fresh_connection_to_same_peer_bypasses_stalled_attempt() {
    let a = node(125, MemoryStorage::new());
    let b = node(126, MemoryStorage::new());
    let id = SedimentreeId::new([125; 32]);
    seed(&b, id, 1).await;
    connect(&a, &b).await.pause();
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    // The original transport is still registered. A peer-ID-only retry would
    // select it again and never reach this new, working transport.
    connect(&a, &b).await;
    synced(&mut stream, b.peer_id()).await;
    contains(&a, id, 1).await;
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn arrival_during_backpressured_catchup_is_not_missed() {
    let a = node(127, MemoryStorage::new());
    let b = node(128, MemoryStorage::new());
    let id = SedimentreeId::new([127; 32]);
    seed(&a, id, 1).await;
    seed(&a, id, 2).await;
    seed(&b, id, 3).await;
    let mut stream = a.load_and_watch(id);
    assert!(matches!(next(&mut stream).await, StoredEvent::Item(_)));
    connect(&a, &b).await;
    synced(&mut stream, b.peer_id()).await;
    contains(&a, id, 3).await;
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn peer_missing_document_can_publish_it_later_without_reconnecting() {
    let a = node(129, MemoryStorage::new());
    let b = node(130, MemoryStorage::new());
    let id = SedimentreeId::new([129; 32]);
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while !b.get_subscribers(id).await.contains(&a.peer_id()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription even when missing");
    while let Ok(event) = tokio::time::timeout(Duration::from_millis(30), stream.next()).await {
        assert!(matches!(event, Some(Ok(StoredEvent::PeerSynced { .. }))));
    }
    push(&b, &mut stream, id, 1).await;
    contains(&a, id, 1).await;
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn closing_one_loader_does_not_stop_another_following_connections() {
    let a = node(131, MemoryStorage::new());
    let b = node(132, MemoryStorage::new());
    let id = SedimentreeId::new([131; 32]);
    let mut first = a.load_and_watch(id);
    let mut second = a.load_and_watch(id);
    assert!(matches!(
        next(&mut first).await,
        StoredEvent::StorageChecked
    ));
    assert!(matches!(
        next(&mut second).await,
        StoredEvent::StorageChecked
    ));
    first.close();
    assert!(first.next().await.is_none());
    seed(&b, id, 1).await;
    connect(&a, &b).await;
    synced(&mut second, b.peer_id()).await;
    contains(&a, id, 1).await;
    push(&b, &mut second, id, 2).await;
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn closing_loader_stops_syncing_future_connections() {
    let a = node(133, MemoryStorage::new());
    let b = node(134, MemoryStorage::new());
    let id = SedimentreeId::new([133; 32]);
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    stream.close();
    assert!(stream.next().await.is_none());
    seed(&b, id, 1).await;
    connect(&a, &b).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(a.get_blobs(id).await.expect("storage").is_none());
    assert!(!b.get_subscribers(id).await.contains(&a.peer_id()));
    a.shutdown();
    b.shutdown();
}
