//! Missing-document responses wait for a shared, bounded upstream attempt.
#![allow(clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{blob::BlobMeta, id::SedimentreeId, loose_commit::LooseCommit};
use subduction_core::{
    authenticated::Direction,
    connection::{
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestedData, SyncDiff, SyncMessage, SyncResult,
        },
        test_utils::{ChannelMockConnectionHandle, TokioSpawn, TokioTimeout},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
    test_utils::{
        ChannelConn, MockConn, TestNode, attach_mock, dial, make_blob, make_head, make_signer,
        spawn_channel_node, spawn_mock_node, subscribe_request, wait_until,
    },
    timeout::call::CallTimeout,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

const DEADLINE: Duration = Duration::from_secs(3);

type Wire = ChannelMockConnectionHandle<SyncMessage>;

async fn recv(wire: &Wire) -> SyncMessage {
    tokio::time::timeout(DEADLINE, wire.outbound_rx.recv())
        .await
        .expect("frame should arrive")
        .expect("wire should stay open")
}

async fn request(wire: &Wire) -> BatchSyncRequest {
    let SyncMessage::BatchSyncRequest(req) = recv(wire).await else {
        panic!("expected an upstream request");
    };
    req
}

async fn response(wire: &Wire) -> BatchSyncResponse {
    let SyncMessage::BatchSyncResponse(resp) = recv(wire).await else {
        panic!("expected a response, not a later push");
    };
    resp
}

async fn answer(wire: &Wire, req: BatchSyncRequest, seed: Option<u8>) {
    let mut commits = Vec::new();
    if let Some(seed) = seed {
        let blob = make_blob(seed);
        let commit = LooseCommit::new(
            req.id,
            make_head(seed),
            BTreeSet::new(),
            BlobMeta::new(&blob),
        )
        .expect("valid commit");
        let signed = Signed::seal::<Sendable, _>(&make_signer(200), commit)
            .await
            .into_signed();
        commits.push((signed, blob));
    }
    wire.inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id: req.id,
            req_id: req.req_id,
            result: SyncResult::Ok(SyncDiff {
                missing_commits: commits,
                missing_fragments: Vec::new(),
                requesting: RequestedData::default(),
            }),
            responder_heads: RemoteHeads::default(),
        }))
        .await
        .expect("wire should stay open");
}

fn commit_count(response: BatchSyncResponse) -> usize {
    let SyncResult::Ok(diff) = response.result else {
        panic!("expected Ok");
    };
    diff.missing_commits.len()
}

/// Two callers share the upstream requests and both get the complete initial
/// diff. One fast upstream must not cause an early, partial response.
#[tokio::test]
async fn concurrent_subscribers_wait_for_all_upstreams() -> TestResult {
    let (hub, _) = spawn_mock_node(100);
    let a_peer = PeerId::new([1; 32]);
    let b_peer = PeerId::new([2; 32]);
    let a = attach_mock(&hub, a_peer, Direction::Accepted).await;
    let b = attach_mock(&hub, b_peer, Direction::Accepted).await;
    let u = attach_mock(&hub, PeerId::new([3; 32]), Direction::Dialed).await;
    let v = attach_mock(&hub, PeerId::new([4; 32]), Direction::Dialed).await;
    let id = SedimentreeId::new([10; 32]);

    a.inbound_tx.send(subscribe_request(a_peer, id)).await?;
    let u_req = request(&u).await;
    let v_req = request(&v).await;
    assert!(
        a.outbound_rx.is_empty(),
        "no response before upstream answers"
    );
    answer(&u, u_req, Some(1)).await;
    assert!(wait_until(|| async { hub.get_commits(id).await.is_some() }).await);

    // Join after a partial result has already been ingested.
    b.inbound_tx.send(subscribe_request(b_peer, id)).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), b.outbound_rx.recv())
            .await
            .is_err()
    );
    assert!(a.outbound_rx.is_empty(), "one upstream is still pending");
    answer(&v, v_req, Some(2)).await;

    assert_eq!(commit_count(response(&a).await), 2);
    assert_eq!(commit_count(response(&b).await), 2);
    // Mutual subscriptions may push V's commit to U, but must not fetch again.
    for wire in [&u, &v] {
        while let Ok(msg) = wire.outbound_rx.try_recv() {
            assert!(
                !matches!(msg, SyncMessage::BatchSyncRequest(_)),
                "fetch must be shared"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn absent_everywhere_returns_empty_only_after_upstream_answers() -> TestResult {
    let (hub, _) = spawn_mock_node(101);
    let peer = PeerId::new([1; 32]);
    let client = attach_mock(&hub, peer, Direction::Accepted).await;
    let up = attach_mock(&hub, PeerId::new([2; 32]), Direction::Dialed).await;
    let id = SedimentreeId::new([11; 32]);
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    let req = request(&up).await;
    assert!(client.outbound_rx.is_empty());
    answer(&up, req, None).await;
    assert_eq!(commit_count(response(&client).await), 0);
    assert!(hub.get_commits(id).await.is_none());
    Ok(())
}

#[tokio::test]
async fn local_document_and_non_subscribing_requests_do_not_wait() -> TestResult {
    let (hub, _) = spawn_mock_node(102);
    let peer = PeerId::new([1; 32]);
    let client = attach_mock(&hub, peer, Direction::Accepted).await;
    let up = attach_mock(&hub, PeerId::new([2; 32]), Direction::Dialed).await;
    let id = SedimentreeId::new([12; 32]);
    hub.store_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    assert_eq!(commit_count(response(&client).await), 1);
    let _unanswered = request(&up).await;

    // Cache eviction is not document absence: durable data still responds
    // immediately, even while the upstream is silent.
    hub.sedimentrees().remove(&id).await;
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    assert_eq!(commit_count(response(&client).await), 1);

    let mut req = subscribe_request(peer, SedimentreeId::new([13; 32]));
    if let SyncMessage::BatchSyncRequest(req) = &mut req {
        req.subscribe = false;
    }
    client.inbound_tx.send(req).await?;
    assert_eq!(commit_count(response(&client).await), 0);
    assert!(up.outbound_rx.is_empty());
    Ok(())
}

fn timed_mock_node(seed: u8) -> TestNode<MockConn, TokioTimeout> {
    let (node, _, listener, manager) = SubductionBuilder::new()
        .signer(make_signer(seed))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .roundtrip_timeout(Duration::from_millis(400))
        .build::<Sendable, MockConn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    node
}

/// Timing out a waiter must not cancel the fetch or lose a late response.
#[tokio::test]
async fn silent_upstream_is_bounded_and_late_data_is_pushed() -> TestResult {
    let hub = timed_mock_node(103);
    let peer = PeerId::new([1; 32]);
    let client = attach_mock(&hub, peer, Direction::Accepted).await;
    let up_peer = PeerId::new([2; 32]);
    let up = attach_mock(&hub, up_peer, Direction::Dialed).await;
    let id = SedimentreeId::new([14; 32]);
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    let req = request(&up).await;
    assert_eq!(commit_count(response(&client).await), 0);
    answer(&up, req, Some(1)).await;
    assert!(
        matches!(recv(&client).await, SyncMessage::LooseCommit { id: pushed, .. } if pushed == id)
    );
    assert!(hub.get_commits(id).await.is_some());
    Ok(())
}

#[tokio::test]
async fn failed_upstream_claim_is_retried() -> TestResult {
    let hub = timed_mock_node(104);
    let peer = PeerId::new([1; 32]);
    let client = attach_mock(&hub, peer, Direction::Accepted).await;
    let up_peer = PeerId::new([2; 32]);
    let up = attach_mock(&hub, up_peer, Direction::Dialed).await;
    let id = SedimentreeId::new([15; 32]);
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    let _unanswered = request(&up).await;
    assert_eq!(commit_count(response(&client).await), 0);
    assert!(wait_until(|| async { hub.outgoing_claims(&up_peer).await.is_none() }).await);
    client.inbound_tx.send(subscribe_request(peer, id)).await?;
    answer(&up, request(&up).await, None).await;
    assert_eq!(commit_count(response(&client).await), 0);
    Ok(())
}

fn timed_channel_node(seed: u8) -> (TestNode<ChannelConn, TokioTimeout>, PeerId) {
    let signer = make_signer(seed);
    let peer = PeerId::from(signer.verifying_key());
    let (node, _, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .roundtrip_timeout(Duration::from_millis(400))
        .build::<Sendable, ChannelConn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    (node, peer)
}

#[tokio::test]
async fn directed_relay_cycle_does_not_deadlock() -> TestResult {
    let (client, c) = timed_channel_node(110);
    let (a, ap) = timed_channel_node(111);
    let (b, bp) = timed_channel_node(112);
    let (d, dp) = timed_channel_node(113);
    dial(&client, c, &a, ap).await;
    dial(&a, ap, &b, bp).await;
    dial(&b, bp, &d, dp).await;
    dial(&d, dp, &a, ap).await;
    let id = SedimentreeId::new([16; 32]);
    let (success, _, errors) = tokio::time::timeout(
        DEADLINE,
        client.sync_with_peer(&ap, id, true, CallTimeout::Default),
    )
    .await??;
    assert!(
        success,
        "cycle must yield a response, not a caller timeout: {errors:?}"
    );
    // The downstream response alone is not sufficient: all propagation calls
    // must settle and establish their subscriptions, too.
    assert!(
        wait_until(|| async {
            a.get_subscribers(id).await.contains(&bp)
                && b.get_subscribers(id).await.contains(&dp)
                && d.get_subscribers(id).await.contains(&ap)
        })
        .await
    );
    Ok(())
}

#[tokio::test]
async fn multi_hop_response_contains_document() -> TestResult {
    let (client, c) = spawn_channel_node(120);
    let (a, ap) = spawn_channel_node(121);
    let (b, bp) = spawn_channel_node(122);
    let (source, sp) = spawn_channel_node(123);
    dial(&client, c, &a, ap).await;
    dial(&a, ap, &b, bp).await;
    dial(&b, bp, &source, sp).await;
    let id = SedimentreeId::new([17; 32]);
    source
        .store_commit(id, make_head(1), BTreeSet::new(), make_blob(1))
        .await?;
    let (success, _, _) = tokio::time::timeout(
        DEADLINE,
        client.sync_with_peer(&ap, id, true, CallTimeout::Default),
    )
    .await??;
    assert!(success);
    assert_eq!(client.get_commits(id).await.map(|c| c.len()), Some(1));
    Ok(())
}
