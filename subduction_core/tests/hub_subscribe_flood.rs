//! Reproducer for the "sync flood" seen by clients of a public sync server
//! (see `SYNC_FLOOD.md` at the workspace root).
//!
//! A hub H with an open policy accepts a subscribing `BatchSyncRequest`
//! from client A and then re-issues that subscribe to *every other
//! connected peer* (`Subduction::propagate_subscription`). On a hub whose
//! other peers are unrelated clients — bystanders that have never heard of
//! the document — this turns each subscribe into a broadcast. Worse, a
//! bystander answers `NotFound`, which rolls back the idempotency claim,
//! so the *next* subscribe for the same document floods it again.
//!
//! Both tests count subscribing `BatchSyncRequest`s on the wire at the
//! bystanders, using the same mock-connection harness as
//! `relay_topology_sync.rs`. They assert the behaviour a client of a
//! public hub should be able to expect, and therefore **fail today**:
//! the numbers in the failure messages are the size of the flood.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed, depth::CountLeadingZeroBytes, id::SedimentreeId,
    sedimentree::FingerprintSummary,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage, SyncResult},
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
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type MockConn = ChannelMockConnection<SyncMessage>;

#[allow(clippy::type_complexity)]
type HubSubduction = Arc<
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

/// Time allowed for any propagation to land on the wire before counting.
const PROPAGATION_PAUSE: Duration = Duration::from_millis(100);

/// Fail-fast cap for [`wait_until`] polling.
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

/// A hub with an open policy: what the public sync server runs
/// (`CliKeyhivePolicyHandle::open()`), so `authorize_fetch` always passes
fn make_open_hub() -> HubSubduction {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[99u8; 32]))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConn>();

    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

const fn peer(seed: u8) -> PeerId {
    PeerId::new([seed; 32])
}

const fn doc(seed: u8) -> SedimentreeId {
    SedimentreeId::new([seed; 32])
}

/// Connect a mock client as `peer` to the hub, returning the test-side
/// handle for injecting inbound and observing outbound messages.
async fn attach_client(
    hub: &HubSubduction,
    peer: PeerId,
) -> TestResult<ChannelMockConnectionHandle<SyncMessage>> {
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer);
    let auth: Authenticated<MockConn, Sendable> = Authenticated::new_for_test(conn, peer);
    hub.add_connection(auth).await?;
    Ok(handle)
}

/// A subscribing `BatchSyncRequest` as a client sends on open/reconnect.
const fn subscribing_request(from: PeerId, id: SedimentreeId, nonce: u64) -> SyncMessage {
    SyncMessage::BatchSyncRequest(BatchSyncRequest {
        id,
        req_id: RequestId {
            requestor: from,
            nonce,
        },
        fingerprint_summary: FingerprintSummary::new(
            FingerprintSeed::new(0, 0),
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::new(),
        ),
        subscribe: true,
    })
}

/// A bystander: a client that has nothing to do with any of the documents
/// under test. It answers every `BatchSyncRequest` from the hub with
/// `NotFound` (it genuinely doesn't have the document) and counts how many
/// subscribing requests the hub pushed at it.
fn spawn_bystander(handle: ChannelMockConnectionHandle<SyncMessage>) -> Arc<AtomicUsize> {
    let unsolicited = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&unsolicited);

    tokio::spawn(async move {
        while let Ok(msg) = handle.outbound_rx.recv().await {
            if let SyncMessage::BatchSyncRequest(req) = &msg {
                if req.subscribe {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                let response = SyncMessage::BatchSyncResponse(BatchSyncResponse {
                    req_id: req.req_id,
                    id: req.id,
                    result: SyncResult::NotFound,
                    responder_heads: RemoteHeads::default(),
                });
                if handle.inbound_tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });

    unsolicited
}

/// Wait until the hub has answered `expected` `BatchSyncResponse`s to
/// the subscriber, so that a subsequent count reflects fully-processed
/// subscribes rather than ones still queued.
async fn wait_for_responses(
    handle: &ChannelMockConnectionHandle<SyncMessage>,
    expected: usize,
) -> bool {
    let seen = Arc::new(AtomicUsize::new(0));
    wait_until(|| {
        let rx = handle.outbound_rx.clone();
        let seen = Arc::clone(&seen);
        async move {
            while let Ok(msg) = rx.try_recv() {
                if matches!(msg, SyncMessage::BatchSyncResponse(_)) {
                    seen.fetch_add(1, Ordering::SeqCst);
                }
            }
            seen.load(Ordering::SeqCst) >= expected
        }
    })
    .await
}

/// One client opening `DOCS` documents against a hub with `BYSTANDERS`
/// unrelated clients attached. Each bystander should receive nothing —
/// it never asked about any of these documents and does not hold them.
///
/// Today each bystander receives `DOCS` subscribing `BatchSyncRequest`s:
/// `DOCS × BYSTANDERS` unsolicited messages for one client's open.
#[tokio::test]
async fn hub_does_not_fan_out_subscribes_to_unrelated_clients() -> TestResult {
    const DOCS: u8 = 20;
    const BYSTANDERS: u8 = 5;

    let hub = make_open_hub();

    let subscriber = peer(1);
    let subscriber_handle = attach_client(&hub, subscriber).await?;

    let mut bystander_counts = Vec::new();
    for i in 0..BYSTANDERS {
        let handle = attach_client(&hub, peer(100 + i)).await?;
        bystander_counts.push(spawn_bystander(handle));
    }

    // The subscriber opens its documents, as a client does on connect.
    for d in 0..DOCS {
        subscriber_handle
            .inbound_tx
            .send(subscribing_request(subscriber, doc(d + 1), u64::from(d)))
            .await?;
    }

    let answered = wait_for_responses(&subscriber_handle, usize::from(DOCS)).await;
    assert!(
        answered,
        "hub never answered all of the subscriber's requests"
    );

    // Propagation is spawned off the dispatch path; give it time to land.
    tokio::time::sleep(PROPAGATION_PAUSE).await;

    let per_bystander: Vec<usize> = bystander_counts
        .iter()
        .map(|c| c.load(Ordering::SeqCst))
        .collect();
    let total: usize = per_bystander.iter().sum();

    assert_eq!(
        total, 0,
        "hub fanned out one client's {DOCS} subscribes to {BYSTANDERS} unrelated \
         clients: {total} unsolicited subscribing BatchSyncRequests on the wire \
         (per bystander: {per_bystander:?}; expected 0)"
    );

    Ok(())
}

/// The idempotency claim is supposed to make a repeated subscribe for the
/// same document cost at most one upstream request (the sibling test in
/// `relay_topology_sync.rs` proves that for a peer answering `Ok`). For a
/// peer answering `NotFound`, the claim is rolled back on every attempt,
/// so every resubscribe — e.g. each reconnect of the subscriber — hits the
/// bystander again.
///
/// Today the bystander receives `RESUBSCRIBES` requests for one document.
#[tokio::test]
async fn hub_does_not_reflood_bystander_on_every_resubscribe() -> TestResult {
    const RESUBSCRIBES: u64 = 10;

    let hub = make_open_hub();

    let subscriber = peer(2);
    let subscriber_handle = attach_client(&hub, subscriber).await?;

    let bystander = peer(200);
    let bystander_handle = attach_client(&hub, bystander).await?;
    let unsolicited = spawn_bystander(bystander_handle);

    let id = doc(42);

    // The same client subscribes to the same document repeatedly, as it
    // does on every reconnect (design/sync/reconnection.md).
    for nonce in 0..RESUBSCRIBES {
        subscriber_handle
            .inbound_tx
            .send(subscribing_request(subscriber, id, nonce))
            .await?;
        // Let each round fully settle (including the NotFound rollback)
        // before the next, so this measures steady-state per-subscribe
        // cost rather than a race between concurrent propagations.
        let answered = wait_for_responses(&subscriber_handle, 1).await;
        assert!(answered, "hub never answered subscribe #{nonce}");
        tokio::time::sleep(PROPAGATION_PAUSE).await;
    }

    let count = unsolicited.load(Ordering::SeqCst);
    assert!(
        count <= 1,
        "{RESUBSCRIBES} resubscribes for one document sent the bystander {count} \
         subscribing BatchSyncRequests (expected at most 1): the NotFound \
         rollback re-arms propagation towards a peer that does not hold the \
         document, so every resubscribe floods it again"
    );

    // The claim state confirms the mechanism: nothing is recorded toward
    // the bystander, so the next subscribe will propagate yet again.
    assert!(
        !hub.get_peer_subscriptions(bystander).await.contains(&id),
        "expected no surviving claim toward the bystander after NotFound"
    );

    Ok(())
}
