//! Subscription propagation goes upstream only.
//!
//! When a node accepts `BatchSyncRequest { subscribe: true }` for tree T it
//! forwards that interest so it can receive T from wherever T lives. Upstream
//! is the set of connections this node *dialed*; a peer that dialed us is
//! never subscribed to T on someone else's behalf.
//!
//! Tests 1 and 3 use the hub shape (two clients, one server, no direct link
//! between the clients); tests 2, 4, and 5 use a chain.
//!
//! ```text
//!   A ──dials──▸ S ◂──dials── B          A ──dials──▸ R ──dials──▸ B
//! ```
#![allow(clippy::expect_used)]

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{
    blob::BlobMeta, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::LooseCommit,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::{BatchSyncResponse, RequestedData, SyncDiff, SyncMessage, SyncResult},
        test_utils::{
            ChannelMockConnection, ChannelMockConnectionHandle, InstantTimeout, TokioSpawn,
        },
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    test_utils::{
        ChannelConn, TestNode, dial, make_blob, make_head, make_signer, spawn_channel_node,
        subscribe_request, wait_until,
    },
    timeout::call::CallTimeout,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use testresult::TestResult;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);
/// How long to wait for a frame that must arrive. Not a settling period:
/// every wait below is anchored to a state change or a sentinel frame.
const WIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything on `rx` up to the response for `sentinel`.
///
/// The hub answers every `BatchSyncRequest`, so subscribing to an unrelated
/// tree and reading until its response arrives flushes the wire: whatever the
/// hub was going to send about the tree under test is already in `frames`.
/// A timer would only guess at that.
async fn drain_until_answered(
    mock: &ChannelMockConnectionHandle<SyncMessage>,
    peer: PeerId,
    sentinel: SedimentreeId,
) -> Vec<SyncMessage> {
    mock.inbound_tx
        .send(subscribe_request(peer, sentinel))
        .await
        .expect("mock inbound is open");

    let mut frames = Vec::new();
    loop {
        let msg = tokio::time::timeout(WIRE_TIMEOUT, mock.outbound_rx.recv())
            .await
            .expect("hub should answer the sentinel subscribe")
            .expect("mock outbound is open");

        if matches!(
            &msg,
            SyncMessage::BatchSyncResponse(r) if r.id == sentinel
        ) {
            return frames;
        }

        frames.push(msg);
    }
}

type MockConn = ChannelMockConnection<SyncMessage>;

type MockHub = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        MockConn,
        SyncHandler<
            Sendable,
            MemoryStorage,
            MockConn,
            OpenPolicy,
            CountLeadingZeroBytes,
            TokioSpawn,
        >,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

fn make_hub() -> MockHub {
    let (sd, _h, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(make_signer(100))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn attach_mock(
    hub: &MockHub,
    peer: PeerId,
    direction: Direction,
) -> TestResult<ChannelMockConnectionHandle<SyncMessage>> {
    let (conn, handle) = MockConn::new_with_handle(peer);
    hub.add_connection(Authenticated::new_for_test(conn, peer, direction))
        .await?;
    Ok(handle)
}

/// Answer every subscribing `BatchSyncRequest` on `handle` with an empty
/// `Ok` response, and count them.
fn answer_and_count(
    handle: ChannelMockConnectionHandle<SyncMessage>,
    id: SedimentreeId,
) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    tokio::spawn(async move {
        while let Ok(msg) = handle.outbound_rx.recv().await {
            if let SyncMessage::BatchSyncRequest(req) = msg
                && req.id == id
            {
                seen.fetch_add(1, Ordering::SeqCst);
                let response = SyncMessage::BatchSyncResponse(BatchSyncResponse {
                    id,
                    req_id: req.req_id,
                    result: SyncResult::Ok(SyncDiff {
                        missing_commits: Vec::new(),
                        missing_fragments: Vec::new(),
                        requesting: RequestedData::default(),
                    }),
                    responder_heads: RemoteHeads::default(),
                });
                if handle.inbound_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });
    count
}

/// B (a downstream) subscribes to X on the hub. A (another downstream)
/// must not hear about X at all, and must not become a subscriber.
#[tokio::test]
async fn subscribe_is_not_forwarded_to_other_downstreams() -> TestResult {
    let hub = make_hub();
    let a_peer = PeerId::new([1u8; 32]);
    let b_peer = PeerId::new([2u8; 32]);
    let up_peer = PeerId::new([3u8; 32]);
    let a = attach_mock(&hub, a_peer, Direction::Accepted).await?;
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await?;
    // The hub dials U, so propagation has somewhere legitimate to go. Without
    // it there would be nothing to wait *for*, and the assertion below would
    // pass on any machine slow enough.
    let _up = attach_mock(&hub, up_peer, Direction::Dialed).await?;
    let x = SedimentreeId::new([9u8; 32]);
    let a_requests = answer_and_count(a, x);

    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;
    assert!(
        wait_until(|| async { hub.get_subscribers(x).await.contains(&b_peer) }).await,
        "hub should record B's subscription"
    );

    // The claim is recorded before the request goes out, so this is proof the
    // propagation pass ran and chose its peers.
    assert!(
        wait_until(|| async {
            hub.outgoing_claims(&up_peer)
                .await
                .is_some_and(|claims| claims.contains(&x))
        })
        .await,
        "hub should propagate B's subscribe to the upstream it dialed"
    );

    assert_eq!(
        a_requests.load(Ordering::SeqCst),
        0,
        "A dialed the hub and never asked for X; it must not receive a BatchSyncRequest for X"
    );
    assert_eq!(
        hub.get_subscribers(x).await,
        BTreeSet::from([b_peer]).into_iter().collect(),
        "only B asked for X"
    );
    Ok(())
}

/// The same subscribe *is* forwarded over a connection the hub dialed
/// (its upstream), and that upstream becomes a subscriber via the mutual
/// rule (the relay case).
#[tokio::test]
async fn subscribe_is_forwarded_to_dialed_upstream() -> TestResult {
    let hub = make_hub();
    let b_peer = PeerId::new([2u8; 32]);
    let up_peer = PeerId::new([3u8; 32]);
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await?;
    let up = attach_mock(&hub, up_peer, Direction::Dialed).await?;
    let x = SedimentreeId::new([9u8; 32]);
    let up_requests = answer_and_count(up, x);

    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;
    assert!(
        wait_until(|| async { up_requests.load(Ordering::SeqCst) == 1 }).await,
        "hub should forward B's subscribe to its dialed upstream"
    );
    assert!(
        wait_until(|| async { hub.get_subscribers(x).await.contains(&up_peer) }).await,
        "mutual subscription: the upstream should now receive the hub's pushes for X"
    );
    Ok(())
}

type Node = TestNode<ChannelConn, InstantTimeout>;

async fn has(node: &Node, id: SedimentreeId) -> bool {
    node.get_commits(id).await.is_some_and(|c| !c.is_empty())
}

async fn has_fragment(node: &Node, id: SedimentreeId) -> bool {
    node.get_fragments(id).await.is_some_and(|f| !f.is_empty())
}

/// Two clients on one server. B creates and syncs a document; A, who never
/// asked for it, must not end up storing it. Positive control: once A does
/// ask, it gets it.
#[tokio::test]
async fn client_does_not_receive_documents_it_never_asked_for() -> TestResult {
    let (a, a_peer) = spawn_channel_node(1);
    let (b, b_peer) = spawn_channel_node(2);
    let (s, s_peer) = spawn_channel_node(3);
    dial(&a, a_peer, &s, s_peer).await;
    dial(&b, b_peer, &s, s_peer).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // B: the automerge-repo write path — store, then subscribing sync.
    let x = SedimentreeId::new([7u8; 32]);
    b.store_commit(x, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    b.sync_with_all_peers(x, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| has(&s, x)).await,
        "server should receive X from B"
    );

    // The server stamps a per-recipient send counter before pushing, so an
    // unstamped counter is proof nothing was ever addressed to A 2014 stronger
    // than A merely not holding X, which could also mean it dropped it.
    assert_eq!(
        s.send_counter_value(&a_peer).await,
        None,
        "the server addressed a frame to A, who never asked for X"
    );

    assert_eq!(
        s.get_subscribers(x).await.into_iter().collect::<Vec<_>>(),
        vec![b_peer],
        "only B subscribed to X on the server"
    );
    assert!(
        !has(&a, x).await,
        "A must not have X after B's initial sync"
    );

    // Later edits must not leak either.
    b.store_commit(x, make_head(2), BTreeSet::new(), make_blob(2))
        .await?;
    b.sync_with_all_peers(x, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| async { s.get_commits(x).await.map(|c| c.len()) == Some(2) }).await,
        "server should receive B's later edit"
    );
    assert_eq!(
        s.send_counter_value(&a_peer).await,
        None,
        "the server addressed B's later edit to A"
    );
    assert!(!has(&a, x).await, "A must not have X after B's later edit");

    // Positive control.
    a.sync_with_peer(&s_peer, x, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| async { a.get_commits(x).await.map(|c| c.len()) == Some(2) }).await,
        "once A asks for X it receives it"
    );
    Ok(())
}

/// A relay forwards data it learns *as a requester*. Here R pulls X from
/// its dialed upstream B (via propagation of A's subscribe) and must push
/// it to A, who subscribed to R, without A opening another sync round.
///
/// ```text
///   A ──dials──▸ R ──dials──▸ B (holds X)
/// ```
#[tokio::test]
async fn relay_forwards_data_it_pulled_from_upstream() -> TestResult {
    let (a, a_peer) = spawn_channel_node(4);
    let (r, r_peer) = spawn_channel_node(5);
    let (b, b_peer) = spawn_channel_node(6);
    dial(&a, a_peer, &r, r_peer).await;
    dial(&r, r_peer, &b, b_peer).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let x = SedimentreeId::new([8u8; 32]);
    b.store_commit(x, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;

    // A asks R for X. R has nothing, propagates upstream to B, pulls X in
    // the response, and must forward it to A.
    a.sync_with_peer(&r_peer, x, true, SYNC_TIMEOUT).await?;
    assert!(wait_until(|| has(&r, x)).await, "R should pull X from B");
    assert!(
        wait_until(|| has(&a, x)).await,
        "R must forward to A what it pulled from B; A has {:?}",
        a.get_commits(x).await.map(|c| c.len())
    );
    Ok(())
}

/// A response that lists the same commit twice is pushed on once. B
/// subscribes on the hub, the hub propagates to its dialed upstream U, and U
/// answers with a duplicated item; B must receive exactly one frame.
#[tokio::test]
async fn duplicate_items_in_a_response_are_pushed_once() -> TestResult {
    let hub = make_hub();
    let b_peer = PeerId::new([2u8; 32]);
    let up_peer = PeerId::new([3u8; 32]);
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await?;
    let up = attach_mock(&hub, up_peer, Direction::Dialed).await?;
    let x = SedimentreeId::new([9u8; 32]);

    // U answers the hub's propagated request with commit C, listed twice.
    let blob = make_blob(1);
    let commit = LooseCommit::new(x, make_head(1), BTreeSet::new(), BlobMeta::new(&blob));
    let signed = Signed::seal::<Sendable, _>(&make_signer(200), commit)
        .await
        .into_signed();
    tokio::spawn(async move {
        while let Ok(msg) = up.outbound_rx.recv().await {
            if let SyncMessage::BatchSyncRequest(req) = msg
                && req.id == x
            {
                let response = SyncMessage::BatchSyncResponse(BatchSyncResponse {
                    id: x,
                    req_id: req.req_id,
                    result: SyncResult::Ok(SyncDiff {
                        missing_commits: vec![
                            (signed.clone(), blob.clone()),
                            (signed.clone(), blob.clone()),
                        ],
                        missing_fragments: Vec::new(),
                        requesting: RequestedData::default(),
                    }),
                    responder_heads: RemoteHeads::default(),
                });
                if up.inbound_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });

    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;

    // Wait for the hub to have ingested U's response, then drain B's wire
    // until quiet and count LooseCommit frames.
    assert!(
        wait_until(|| async { hub.get_commits(x).await.is_some() }).await,
        "hub should ingest U's response"
    );
    let frames = drain_until_answered(&b, b_peer, SedimentreeId::new([0xEE; 32])).await;
    let pushed = frames
        .iter()
        .filter(|m| matches!(m, SyncMessage::LooseCommit { .. }))
        .count();
    assert_eq!(
        pushed, 1,
        "one push for a duplicated item; wire had {frames:?}"
    );
    assert_eq!(
        hub.get_commits(x).await.map(|c| c.len()),
        Some(1),
        "the hub stores the commit once"
    );
    Ok(())
}

/// Same as [`relay_forwards_data_it_pulled_from_upstream`], for a fragment.
#[tokio::test]
async fn relay_forwards_fragments_it_pulled_from_upstream() -> TestResult {
    let (a, a_peer) = spawn_channel_node(7);
    let (r, r_peer) = spawn_channel_node(8);
    let (b, b_peer) = spawn_channel_node(9);
    dial(&a, a_peer, &r, r_peer).await;
    dial(&r, r_peer, &b, b_peer).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let x = SedimentreeId::new([11u8; 32]);
    b.store_fragment(
        x,
        make_head(1),
        BTreeSet::from([make_head(200)]),
        &[],
        make_blob(1),
    )
    .await?;

    a.sync_with_peer(&r_peer, x, true, SYNC_TIMEOUT).await?;
    assert!(
        wait_until(|| has_fragment(&r, x)).await,
        "R should pull the fragment from B"
    );
    assert!(
        wait_until(|| has_fragment(&a, x)).await,
        "R must forward the fragment to A"
    );
    Ok(())
}

/// An item the node already holds is not pushed again when a later
/// response repeats it. After the first pull the hub asks U once more; U
/// answers with the same commit; B's wire stays quiet.
#[tokio::test]
async fn already_known_items_are_not_re_pushed() -> TestResult {
    let hub = make_hub();
    let b_peer = PeerId::new([2u8; 32]);
    let up_peer = PeerId::new([3u8; 32]);
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await?;
    let up = attach_mock(&hub, up_peer, Direction::Dialed).await?;
    let x = SedimentreeId::new([12u8; 32]);

    let blob = make_blob(1);
    let commit = LooseCommit::new(x, make_head(1), BTreeSet::new(), BlobMeta::new(&blob));
    let signed = Signed::seal::<Sendable, _>(&make_signer(201), commit)
        .await
        .into_signed();
    // U answers every request for X with the same commit.
    tokio::spawn(async move {
        while let Ok(msg) = up.outbound_rx.recv().await {
            if let SyncMessage::BatchSyncRequest(req) = msg
                && req.id == x
            {
                let response = SyncMessage::BatchSyncResponse(BatchSyncResponse {
                    id: x,
                    req_id: req.req_id,
                    result: SyncResult::Ok(SyncDiff {
                        missing_commits: vec![(signed.clone(), blob.clone())],
                        missing_fragments: Vec::new(),
                        requesting: RequestedData::default(),
                    }),
                    responder_heads: RemoteHeads::default(),
                });
                if up.inbound_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });

    let pushes = async |sentinel: u8| {
        drain_until_answered(&b, b_peer, SedimentreeId::new([sentinel; 32]))
            .await
            .iter()
            .filter(|m| matches!(m, SyncMessage::LooseCommit { .. }))
            .count()
    };

    // First pull: B subscribes, hub propagates to U, gets X, pushes to B.
    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;
    assert!(wait_until(|| async { hub.get_commits(x).await.is_some() }).await);
    assert_eq!(pushes(0xE1).await, 1, "first pull pushes X to B once");

    // Second pull: the hub asks U again and gets the same X back.
    hub.sync_with_peer(&up_peer, x, false, SYNC_TIMEOUT).await?;
    assert_eq!(
        pushes(0xE2).await,
        0,
        "X is already known; it must not be pushed to B again"
    );
    Ok(())
}
