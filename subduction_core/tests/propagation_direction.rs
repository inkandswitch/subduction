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
    blob::{Blob, BlobMeta},
    crypto::fingerprint::FingerprintSeed,
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
        test_utils::{
            ChannelMockConnection, ChannelMockConnectionHandle, ChannelTransport, InstantTimeout,
            TokioSpawn,
        },
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use testresult::TestResult;

const SYNC_TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(500);
const SETTLE: Duration = Duration::from_millis(150);
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

async fn wait_until<F, Fut>(mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_blob(seed: u8) -> Blob {
    Blob::new((0..64).map(|i| seed.wrapping_add(i)).collect())
}

const fn make_head(seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    CommitId::new(bytes)
}

const fn subscribe_request(from: PeerId, id: SedimentreeId) -> SyncMessage {
    SyncMessage::BatchSyncRequest(BatchSyncRequest {
        id,
        req_id: RequestId {
            requestor: from,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            FingerprintSeed::new(0, 0),
            BTreeSet::new(),
            BTreeSet::new(),
        ),
        subscribe: true,
    })
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
    let a = attach_mock(&hub, a_peer, Direction::Accepted).await?;
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await?;
    let x = SedimentreeId::new([9u8; 32]);
    let a_requests = answer_and_count(a, x);

    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;
    assert!(
        wait_until(|| async { hub.get_subscribers(x).await.contains(&b_peer) }).await,
        "hub should record B's subscription"
    );
    tokio::time::sleep(SETTLE).await;

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

type Conn = MessageTransport<ChannelTransport>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

fn make_node(seed: u8) -> (Node, PeerId) {
    let signer = make_signer(seed);
    let peer = PeerId::from(signer.verifying_key());
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    (sd, peer)
}

/// `dialer` dials `acceptor`.
async fn dial(
    dialer: &Node,
    dialer_peer: PeerId,
    acceptor: &Node,
    acceptor_peer: PeerId,
) -> TestResult {
    let (t_d, t_a) = ChannelTransport::pair();
    dialer
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(t_d),
            acceptor_peer,
            Direction::Dialed,
        ))
        .await?;
    acceptor
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(t_a),
            dialer_peer,
            Direction::Accepted,
        ))
        .await?;
    Ok(())
}

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
    let (a, a_peer) = make_node(1);
    let (b, b_peer) = make_node(2);
    let (s, s_peer) = make_node(3);
    dial(&a, a_peer, &s, s_peer).await?;
    dial(&b, b_peer, &s, s_peer).await?;
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
    tokio::time::sleep(SETTLE).await;

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
    tokio::time::sleep(SETTLE).await;
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
    let (a, a_peer) = make_node(4);
    let (r, r_peer) = make_node(5);
    let (b, b_peer) = make_node(6);
    dial(&a, a_peer, &r, r_peer).await?;
    dial(&r, r_peer, &b, b_peer).await?;
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
    let mut frames = Vec::new();
    while let Ok(Ok(msg)) = tokio::time::timeout(SETTLE, b.outbound_rx.recv()).await {
        frames.push(msg);
    }
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
    let (a, a_peer) = make_node(7);
    let (r, r_peer) = make_node(8);
    let (b, b_peer) = make_node(9);
    dial(&a, a_peer, &r, r_peer).await?;
    dial(&r, r_peer, &b, b_peer).await?;
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

    let drain = |rx: &async_channel::Receiver<SyncMessage>| {
        let rx = rx.clone();
        async move {
            let mut n = 0;
            while let Ok(Ok(msg)) = tokio::time::timeout(SETTLE, rx.recv()).await {
                if matches!(msg, SyncMessage::LooseCommit { .. }) {
                    n += 1;
                }
            }
            n
        }
    };

    // First pull: B subscribes, hub propagates to U, gets X, pushes to B.
    b.inbound_tx.send(subscribe_request(b_peer, x)).await?;
    assert!(wait_until(|| async { hub.get_commits(x).await.is_some() }).await);
    assert_eq!(
        drain(&b.outbound_rx).await,
        1,
        "first pull pushes X to B once"
    );

    // Second pull: the hub asks U again and gets the same X back.
    hub.sync_with_peer(&up_peer, x, false, SYNC_TIMEOUT).await?;
    assert_eq!(
        drain(&b.outbound_rx).await,
        0,
        "X is already known; it must not be pushed to B again"
    );
    Ok(())
}
