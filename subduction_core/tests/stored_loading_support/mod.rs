mod connection_watch;

use future_form::Sendable;
use futures::StreamExt;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::{memory::MemoryStorage, traits::Storage},
    stored::{LoadError, StoredEvent, StoredItem},
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;

type Conn = MessageTransport<PausableChannelTransport>;
type Handler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>;
type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        Handler,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;
fn node(seed: u8, storage: MemoryStorage) -> Node {
    node_with_timeout(seed, storage, Duration::from_secs(5))
}
fn node_with_timeout(seed: u8, storage: MemoryStorage, timeout: Duration) -> Node {
    let (node, _, listener, manager) = SubductionBuilder::new()
        .roundtrip_timeout(timeout)
        .signer(MemorySigner::from_bytes(&[seed; 32]))
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    node
}
async fn connect(a: &Node, b: &Node) -> PausableChannelTransport {
    let (a_transport, b_transport) = PausableChannelTransport::pair();
    a.add_connection(Authenticated::new_for_test(
        MessageTransport::new(a_transport),
        b.peer_id(),
        Direction::Dialed,
    ))
    .await
    .expect("register a");
    b.add_connection(Authenticated::new_for_test(
        MessageTransport::new(b_transport.clone()),
        a.peer_id(),
        Direction::Accepted,
    ))
    .await
    .expect("register b");
    b_transport
}

#[tokio::test]
async fn first_available_peer_finishes_before_wedged_peer_and_keeps_streaming() {
    let a = node(1, MemoryStorage::new());
    let missing = node(2, MemoryStorage::new());
    let wedged = node(3, MemoryStorage::new());
    let available = node(4, MemoryStorage::new());
    connect(&a, &missing).await;
    connect(&a, &wedged).await.pause();
    connect(&a, &available).await;
    let id = SedimentreeId::new([30; 32]);
    for seed in 1..=3 {
        available
            .store_commit(
                id,
                CommitId::new([seed; 32]),
                BTreeSet::new(),
                Blob::new(vec![seed]),
            )
            .await
            .expect("write");
    }
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    let mut count = 0;
    loop {
        match super::next(&mut stream).await {
            StoredEvent::Item(_) => count += 1,
            StoredEvent::PeerSynced { peer_id } if peer_id == available.peer_id() => break,
            // An empty peer may complete while another response is arriving;
            // it must not cancel the supplier's unfinished round.
            StoredEvent::PeerSynced { .. } => {}
            event @ (StoredEvent::StorageChecked | StoredEvent::RescanRequired) => {
                panic!("unexpected {event:?}")
            }
        }
    }
    assert_eq!(count, 3, "completion must follow the entire response");
    available
        .add_commit(
            id,
            CommitId::new([5; 32]),
            BTreeSet::new(),
            Blob::new(vec![5]),
        )
        .await
        .expect("push");
    loop {
        match super::next(&mut stream).await {
            StoredEvent::Item(StoredItem::Commit(commit)) => {
                assert_eq!(commit.payload().head(), CommitId::new([5; 32]));
                break;
            }
            // The empty peer's round can finish after the supplier's marker,
            // too. Completion ordering across peers is not guaranteed.
            StoredEvent::PeerSynced { peer_id } if peer_id == missing.peer_id() => {}
            event => panic!("unexpected {event:?}"),
        }
    }
    for node in [a, missing, wedged, available] {
        node.shutdown();
    }
}

#[tokio::test]
async fn empty_successful_round_has_a_marker_but_does_not_supply_a_document() {
    let id = SedimentreeId::new([31; 32]);
    let storage = MemoryStorage::new();
    Storage::<Sendable>::save_batch(&storage, id, vec![], vec![])
        .await
        .expect("register empty tree");
    let a = node(6, MemoryStorage::new());
    let b = node(7, storage);
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::PeerSynced { .. }
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    b.add_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("created after empty sync");
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn overflowing_peer_response_recovers_before_completion_marker() {
    use sedimentree_core::{blob::BlobMeta, loose_commit::LooseCommit};
    let a = node(10, MemoryStorage::new());
    let b = node(11, MemoryStorage::new());
    let id = SedimentreeId::new([35; 32]);
    let commits = (0..260_u32)
        .map(|n| {
            let mut head = [0; 32];
            head[..4].copy_from_slice(&n.to_le_bytes());
            let blob = Blob::new(n.to_le_bytes().to_vec());
            let commit = LooseCommit::new(
                id,
                CommitId::new(head),
                BTreeSet::new(),
                BlobMeta::new(&blob),
            )
            .expect("metadata");
            (commit, blob)
        })
        .collect();
    b.store_built_batch(id, commits, vec![])
        .await
        .expect("batch");
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    let mut count = 0;
    let mut recovered = false;
    let mut checked = false;
    loop {
        match super::next(&mut stream).await {
            StoredEvent::Item(_) => count += 1,
            StoredEvent::RescanRequired => {
                count = 0;
                recovered = true;
                checked = false;
            }
            StoredEvent::StorageChecked => {
                assert_eq!(count, 260);
                checked = true;
            }
            StoredEvent::PeerSynced { peer_id } => {
                assert_eq!(peer_id, b.peer_id());
                assert!(recovered && checked);
                assert_eq!(count, 260);
                break;
            }
        }
    }
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn cached_loading_keeps_streaming_while_peer_is_stalled() {
    let a = node_with_timeout(40, MemoryStorage::new(), Duration::from_millis(30));
    let b = node(41, MemoryStorage::new());
    connect(&a, &b).await.pause();
    let id = SedimentreeId::new([40; 32]);
    a.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("cached");
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    // Wait beyond the configured roundtrip timeout. Loading does not use it,
    // and a stalled peer must not prevent local updates from being delivered.
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(a.get_peer_subscriptions(b.peer_id()).await.is_empty());
    a.store_commit(
        id,
        CommitId::new([2; 32]),
        BTreeSet::new(),
        Blob::new(vec![2]),
    )
    .await
    .expect("ongoing");
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn configured_timeout_does_not_interrupt_recovery_delivery() {
    let a = node_with_timeout(110, MemoryStorage::new(), Duration::from_millis(100));
    let b = node(111, MemoryStorage::new());
    connect(&a, &b).await.pause();
    let id = SedimentreeId::new([110; 32]);
    a.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("cached");
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    // Deletion invalidates observation; recreate with enough items that the
    // one-slot delivery queue cannot hold the entire recovery snapshot.
    a.remove_sedimentree(id).await.expect("invalidate");
    for n in 2..5 {
        a.store_commit(
            id,
            CommitId::new([n; 32]),
            BTreeSet::new(),
            Blob::new(vec![n]),
        )
        .await
        .expect("recreate");
    }
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::RescanRequired
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // The configured timeout must not cancel or restart a backpressured snapshot.
    let mut count = 1;
    loop {
        match super::next(&mut stream).await {
            StoredEvent::Item(_) => count += 1,
            StoredEvent::StorageChecked => break,
            event @ (StoredEvent::PeerSynced { .. } | StoredEvent::RescanRequired) => {
                panic!("unexpected recovery event: {event:?}")
            }
        }
    }
    assert_eq!(count, 3);
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn cached_loading_syncs_stale_contents_and_subscribes() {
    let a = node(80, MemoryStorage::new());
    let b = node(81, MemoryStorage::new());
    let id = SedimentreeId::new([80; 32]);
    a.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("cached");
    b.store_commit(
        id,
        CommitId::new([2; 32]),
        BTreeSet::new(),
        Blob::new(vec![2]),
    )
    .await
    .expect("remote");
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(super::next(&mut stream).await,
        StoredEvent::PeerSynced { peer_id } if peer_id == b.peer_id()));
    assert!(a.get_peer_subscriptions(b.peer_id()).await.contains(&id));
    b.add_commit(
        id,
        CommitId::new([3; 32]),
        BTreeSet::new(),
        Blob::new(vec![3]),
    )
    .await
    .expect("push");
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    a.shutdown();
    b.shutdown();
}

// Exercise multiple peers and continuing past successful rounds, including
// rounds with no new content to ingest.
async fn loading_subscribes_to_all_current_peers(cached: bool) {
    let a = node(90, MemoryStorage::new());
    let id = SedimentreeId::new([90; 32]);
    if cached {
        a.store_commit(
            id,
            CommitId::new([1; 32]),
            BTreeSet::new(),
            Blob::new(vec![1]),
        )
        .await
        .expect("cached");
    }
    let mut peers = Vec::new();
    for seed in 91..96 {
        let b = node(seed, MemoryStorage::new());
        b.store_commit(
            id,
            CommitId::new([1; 32]),
            BTreeSet::new(),
            Blob::new(vec![1]),
        )
        .await
        .expect("remote");
        connect(&a, &b).await;
        peers.push(b);
    }
    let mut stream = a.load_and_watch(id);
    let mut synced = BTreeSet::new();
    while synced.len() < peers.len() {
        if let StoredEvent::PeerSynced { peer_id } = super::next(&mut stream).await {
            assert!(synced.insert(peer_id), "one marker per peer");
        }
    }
    for b in peers {
        assert!(synced.contains(&b.peer_id()));
        assert!(a.get_peer_subscriptions(b.peer_id()).await.contains(&id));
        assert!(b.get_subscribers(id).await.contains(&a.peer_id()));
        b.shutdown();
    }
    a.shutdown();
}

#[tokio::test]
async fn cached_loading_subscribes_even_when_peers_have_no_new_data() {
    loading_subscribes_to_all_current_peers(true).await;
}

#[tokio::test]
async fn uncached_loading_continues_syncing_after_first_supplier() {
    loading_subscribes_to_all_current_peers(false).await;
}

#[tokio::test]
async fn cached_fetch_still_does_not_contact_peers() {
    let a = node(100, MemoryStorage::new());
    let b = node(101, MemoryStorage::new());
    let id = SedimentreeId::new([100; 32]);
    a.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("cached");
    connect(&a, &b).await;
    let before = a.send_counter_value(&b.peer_id()).await;
    assert_eq!(
        a.fetch_blobs(id, CallTimeout::Default)
            .await
            .expect("fetch")
            .expect("cached")
            .len(),
        1
    );
    assert_eq!(a.send_counter_value(&b.peer_id()).await, before);
    assert!(a.get_peer_subscriptions(b.peer_id()).await.is_empty());
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn slow_cached_delivery_has_no_overall_deadline() {
    let a = node_with_timeout(42, MemoryStorage::new(), Duration::from_millis(30));
    let id = SedimentreeId::new([41; 32]);
    for n in [1, 2] {
        a.store_commit(
            id,
            CommitId::new([n; 32]),
            BTreeSet::new(),
            Blob::new(vec![n]),
        )
        .await
        .expect("cached");
    }
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    tokio::time::sleep(Duration::from_millis(60)).await;
    // Delivery is controlled by the caller, not the network timeout.
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::Item(_)
    ));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    stream.close();
    assert!(stream.next().await.is_none());
    a.shutdown();
}

// An unrelated local write supplies `present`; it must not turn a heads-only or
// wholly rejected response into a supplier and abandon another scoped round.
#[allow(clippy::too_many_lines)]
async fn nonsupplier_marker_keeps_other_round_alive(rejected_offer: bool) {
    use sedimentree_core::{blob::verified::VerifiedBlobMeta, loose_commit::LooseCommit};
    use subduction_core::{
        connection::{
            message::{BatchSyncResponse, RequestedData, SyncDiff, SyncMessage, SyncResult},
            test_utils::ChannelMockConnection,
        },
        peer::id::PeerId,
        remote_heads::RemoteHeads,
    };
    use subduction_crypto::verified_meta::VerifiedMeta;
    let (a, _, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(MemorySigner::from_bytes(&[70; 32]))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    let first_peer = PeerId::new([71; 32]);
    let supplier = PeerId::new([72; 32]);
    let (first, remote) = ChannelMockConnection::new_with_handle(first_peer);
    let (second, other) = ChannelMockConnection::new_with_handle(supplier);
    a.add_connection(first.authenticated())
        .await
        .expect("first");
    a.add_connection(second.authenticated())
        .await
        .expect("second");
    let id = SedimentreeId::new([53; 32]);
    let mut updates = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut updates).await,
        StoredEvent::StorageChecked
    ));
    let SyncMessage::BatchSyncRequest(first) =
        remote.outbound_rx.recv().await.expect("first request")
    else {
        panic!("request")
    };
    let SyncMessage::BatchSyncRequest(second) =
        other.outbound_rx.recv().await.expect("second request")
    else {
        panic!("request")
    };
    a.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("concurrent data");
    assert!(matches!(
        super::next(&mut updates).await,
        StoredEvent::Item(_)
    ));
    let verified = VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
        &MemorySigner::from_bytes(&[72; 32]),
        (id, CommitId::new([2; 32]), BTreeSet::new()),
        VerifiedBlobMeta::new(Blob::new(vec![2])),
    )
    .await
    .expect("signed data");
    remote
        .inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id,
            req_id: first.req_id,
            responder_heads: RemoteHeads {
                counter: 1,
                heads: vec![CommitId::new([1; 32])],
            },
            result: SyncResult::Ok(SyncDiff {
                missing_commits: if rejected_offer {
                    vec![(verified.signed().clone(), Blob::new(vec![99]))]
                } else {
                    vec![]
                },
                missing_fragments: vec![],
                requesting: RequestedData::default(),
            }),
        }))
        .await
        .expect("nonsupplier response");
    assert!(
        matches!(super::next(&mut updates).await, StoredEvent::PeerSynced { peer_id } if peer_id == first_peer)
    );
    other
        .inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id,
            req_id: second.req_id,
            responder_heads: RemoteHeads::default(),
            result: SyncResult::Ok(SyncDiff {
                missing_commits: vec![(verified.signed().clone(), verified.blob().clone())],
                missing_fragments: vec![],
                requesting: RequestedData::default(),
            }),
        }))
        .await
        .expect("supplier response");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match updates.next().await {
                Some(Ok(StoredEvent::PeerSynced { peer_id })) if peer_id == supplier => break,
                Some(Ok(_)) => {}
                event => panic!("supplier round abandoned: {event:?}"),
            }
        }
    })
    .await
    .expect("nonsupplier marker must not abandon the supplier's unfinished round");
    a.shutdown();
}
#[tokio::test]
async fn heads_only_response_does_not_qualify_using_concurrent_data() {
    nonsupplier_marker_keeps_other_round_alive(false).await;
}
#[tokio::test]
async fn rejected_offer_does_not_qualify_using_concurrent_data() {
    nonsupplier_marker_keeps_other_round_alive(true).await;
}

#[tokio::test]
async fn all_peers_start_without_a_concurrency_cap_or_peer_deadline() {
    use subduction_core::{
        connection::{
            message::{BatchSyncResponse, RequestedData, SyncDiff, SyncMessage, SyncResult},
            test_utils::ChannelMockConnection,
        },
        peer::id::PeerId,
        remote_heads::RemoteHeads,
    };
    let (a, _, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(MemorySigner::from_bytes(&[43; 32]))
        .roundtrip_timeout(Duration::from_millis(100))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    let mut handles = Vec::new();
    for seed in 44..49 {
        let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([seed; 32]));
        a.add_connection(Authenticated::new_for_test(
            conn,
            PeerId::new([seed; 32]),
            Direction::Dialed,
        ))
        .await
        .expect("register");
        handles.push(handle);
    }
    let mut stream = a.load_and_watch(SedimentreeId::new([42; 32]));
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    let mut requests = Vec::new();
    // All five peers must receive a request without any peer responding.
    tokio::time::timeout(Duration::from_secs(1), async {
        for handle in &handles {
            let SyncMessage::BatchSyncRequest(request) =
                handle.outbound_rx.recv().await.expect("request")
            else {
                panic!("expected sync request");
            };
            requests.push(request);
        }
    })
    .await
    .expect("all peers start concurrently");

    // A response arriving beyond the node's configured timeout still completes
    // its round, while the other four attempts remain pending until cancelled.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let request = requests.first().expect("first request");
    handles
        .first()
        .expect("first peer")
        .inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id: request.id,
            req_id: request.req_id,
            responder_heads: RemoteHeads::default(),
            result: SyncResult::Ok(SyncDiff {
                missing_commits: vec![],
                missing_fragments: vec![],
                requesting: RequestedData::default(),
            }),
        }))
        .await
        .expect("late response");
    assert!(matches!(super::next(&mut stream).await,
        StoredEvent::PeerSynced { peer_id } if peer_id == PeerId::new([44; 32])));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    stream.close();
    assert!(stream.next().await.is_none());
    a.shutdown();
}

#[tokio::test]
async fn dropping_a_loaded_watcher_leaves_its_subscription_node_owned() {
    let a = node(49, MemoryStorage::new());
    let b = node(50, MemoryStorage::new());
    let id = SedimentreeId::new([43; 32]);
    b.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("seed");
    connect(&a, &b).await;
    let mut stream = a.load_and_watch(id);
    while !matches!(
        super::next(&mut stream).await,
        StoredEvent::PeerSynced { .. }
    ) {}
    stream.close();
    assert!(a.get_peer_subscriptions(b.peer_id()).await.contains(&id));
    assert!(b.get_subscribers(id).await.contains(&a.peer_id()));
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn collecting_unknown_tree_uses_supplier_readiness_and_establishes_no_subscriptions() {
    let a = node(60, MemoryStorage::new());
    let empty_store = MemoryStorage::new();
    let id = SedimentreeId::new([50; 32]);
    Storage::<Sendable>::save_batch(&empty_store, id, vec![], vec![])
        .await
        .expect("empty candidate");
    let empty = node(61, empty_store);
    let supplier = node(62, MemoryStorage::new());
    let wedged = node(63, MemoryStorage::new());
    for n in 0..3 {
        supplier
            .store_commit(
                id,
                CommitId::new([n; 32]),
                BTreeSet::new(),
                Blob::new(vec![n]),
            )
            .await
            .expect("seed");
    }
    connect(&a, &empty).await;
    connect(&a, &supplier).await;
    connect(&a, &wedged).await.pause();
    let blobs = a
        .fetch_blobs(id, CallTimeout::TimeoutMillis(1_000))
        .await
        .expect("collect")
        .expect("unknown tree supplied");
    assert_eq!(blobs.len(), 3, "not the first item or empty peer marker");
    assert!(
        a.get_peer_subscriptions(supplier.peer_id())
            .await
            .is_empty()
    );
    assert!(supplier.get_subscribers(id).await.is_empty());
    for peer in [a, empty, supplier, wedged] {
        peer.shutdown();
    }
}

#[tokio::test]
async fn collecting_overflow_recovers_before_returning_and_preserves_timeout_errors() {
    use subduction_core::subduction::error::IoError;
    let a = node(64, MemoryStorage::new());
    let b = node(65, MemoryStorage::new());
    let id = SedimentreeId::new([51; 32]);
    for n in 0..260_u32 {
        let mut head = [0; 32];
        head[..4].copy_from_slice(&n.to_le_bytes());
        b.store_commit(
            id,
            CommitId::new(head),
            BTreeSet::new(),
            Blob::new(n.to_le_bytes().to_vec()),
        )
        .await
        .expect("seed");
    }
    let transport = connect(&a, &b).await;
    let blobs = a
        .fetch_blobs(id, CallTimeout::TimeoutMillis(5_000))
        .await
        .expect("recovery")
        .expect("available");
    assert_eq!(blobs.len(), 260);
    assert!(a.get_peer_subscriptions(b.peer_id()).await.is_empty());
    transport.pause();
    assert!(matches!(
        a.fetch_blobs(SedimentreeId::new([52; 32]), CallTimeout::TimeoutMillis(30))
            .await,
        Err(IoError::Load(LoadError::TimedOut))
    ));
    a.shutdown();
    b.shutdown();
}

#[tokio::test]
async fn caller_timeout_and_close_interrupt_loading() {
    let a = node(8, MemoryStorage::new());
    let b = node(9, MemoryStorage::new());
    connect(&a, &b).await.pause();
    let id = SedimentreeId::new([32; 32]);
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    stream.close();
    assert!(stream.next().await.is_none());
    let mut stream = a.load_and_watch(id);
    assert!(matches!(
        super::next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    a.shutdown();
    b.shutdown();
    assert!(matches!(stream.next().await, Some(Err(LoadError::Closed))));
}
