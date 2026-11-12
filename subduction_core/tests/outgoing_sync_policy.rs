//! Outgoing sync must not disclose IDs or inventories to unauthorized peers.

#![allow(clippy::expect_used, clippy::panic)]

use core::{convert::Infallible, fmt};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

use future_form::Sendable;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use sedimentree_core::{
    blob::Blob, crypto::fingerprint::FingerprintSeed, depth::CountLeadingZeroBytes,
    id::SedimentreeId, loose_commit::id::CommitId, sedimentree::FingerprintSummary,
};
use subduction_core::{
    connection::{
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
        test_utils::{ChannelMockConnection, TokioSpawn, TokioTimeout, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    stored::StoredEvent,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_author::VerifiedAuthor};
use testresult::TestResult;

const ID: SedimentreeId = SedimentreeId::new([42; 32]);
const ALLOWED: PeerId = PeerId::new([1; 32]);
const DENIED: PeerId = PeerId::new([2; 32]);
const WAIT: Duration = Duration::from_secs(2);

struct Policy {
    checked: async_channel::Sender<(PeerId, SedimentreeId)>,
}

#[derive(Debug)]
struct Denied;
impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("denied")
    }
}
impl std::error::Error for Denied {}

impl ConnectionPolicy<Sendable> for Policy {
    type ConnectionDisallowed = Infallible;
    fn authorize_connect(&self, _: PeerId) -> BoxFuture<'_, Result<(), Infallible>> {
        async { Ok(()) }.boxed()
    }
}
impl StoragePolicy<Sendable> for Policy {
    type FetchDisallowed = Denied;
    type PutDisallowed = Infallible;
    fn authorize_fetch(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Denied>> {
        async move {
            self.checked.try_send((peer, id)).expect("record check");
            if peer == ALLOWED && id == ID {
                Ok(())
            } else {
                Err(Denied)
            }
        }
        .boxed()
    }
    fn authorize_put(
        &self,
        _: PeerId,
        _: VerifiedAuthor,
        _: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Infallible>> {
        async { Ok(()) }.boxed()
    }
    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move {
            ids.into_iter()
                .filter(|id| peer == ALLOWED && *id == ID)
                .collect()
        }
        .boxed()
    }
}

type Conn = ChannelMockConnection<SyncMessage>;
type Handler =
    SyncHandler<Sendable, MemoryStorage, Conn, Policy, CountLeadingZeroBytes, TokioSpawn>;
type Node = Subduction<
    'static,
    Sendable,
    MemoryStorage,
    Conn,
    Handler,
    Policy,
    MemorySigner,
    TokioTimeout,
    TokioSpawn,
>;

fn node() -> (Arc<Node>, async_channel::Receiver<(PeerId, SedimentreeId)>) {
    let (checked, checks) = async_channel::unbounded();
    let (node, _, listener, manager) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(Policy { checked }))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    (node, checks)
}

async fn seed(node: &Node) -> TestResult {
    node.store_commit(
        ID,
        CommitId::new([3; 32]),
        BTreeSet::new(),
        Blob::new(b"private".to_vec()),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn explicit_sync_skips_denied_peers_for_missing_and_cached_documents() -> TestResult {
    let (node, _checks) = node();
    let (conn, remote) = Conn::new_with_handle(DENIED);
    node.add_connection(conn.authenticated()).await?;
    for cached in [false, true] {
        if cached {
            seed(&node).await?;
        }
        let (success, stats, errors) = tokio::time::timeout(
            WAIT,
            node.sync_with_peer(&DENIED, ID, true, CallTimeout::Uncapped),
        )
        .await??;
        assert!(!success);
        assert!(errors.is_empty());
        assert_eq!(stats.commits_sent + stats.fragments_sent, 0);
        let results = tokio::time::timeout(
            WAIT,
            node.sync_with_all_peers(ID, true, CallTimeout::Uncapped),
        )
        .await??;
        let (success, _, errors) = results.get(&DENIED).expect("denied peer result");
        assert!(!success);
        assert!(errors.is_empty());
        assert!(
            remote.outbound_rx.is_empty(),
            "no ID or inventory may be sent"
        );
    }
    let (success, _, errors, io_errors) = tokio::time::timeout(
        WAIT,
        node.full_sync_with_peer(&DENIED, true, CallTimeout::Uncapped),
    )
    .await?;
    assert!(!success && errors.is_empty() && io_errors.is_empty());
    let (success, _, errors, io_errors) =
        tokio::time::timeout(WAIT, node.full_sync_with_all_peers(CallTimeout::Uncapped)).await?;
    assert!(!success && errors.is_empty() && io_errors.is_empty());
    assert!(remote.outbound_rx.is_empty());
    node.shutdown();
    Ok(())
}

async fn watch_respects_policy(cached: bool) -> TestResult {
    let (node, checks) = node();
    if cached {
        seed(&node).await?;
    }
    let (conn, current) = Conn::new_with_handle(DENIED);
    node.add_connection(conn.authenticated()).await?;
    let mut updates = node.load_and_watch(ID);
    // Drain local replay so the driver can start its network rounds.
    loop {
        match tokio::time::timeout(WAIT, updates.next()).await? {
            Some(Ok(StoredEvent::StorageChecked)) => break,
            Some(Ok(StoredEvent::Item(_))) => {}
            other => panic!("unexpected local replay event: {other:?}"),
        }
    }
    assert_eq!(
        tokio::time::timeout(WAIT, checks.recv()).await??,
        (DENIED, ID)
    );
    assert!(current.outbound_rx.is_empty());

    let later_peer = PeerId::new([4; 32]);
    let (conn, later) = Conn::new_with_handle(later_peer);
    node.add_connection(conn.authenticated()).await?;
    assert_eq!(
        tokio::time::timeout(WAIT, checks.recv()).await??,
        (later_peer, ID)
    );
    assert!(later.outbound_rx.is_empty());

    node.disconnect_from_peer(&DENIED).await?;
    let (conn, reconnected) = Conn::new_with_handle(DENIED);
    node.add_connection(conn.authenticated()).await?;
    assert_eq!(
        tokio::time::timeout(WAIT, checks.recv()).await??,
        (DENIED, ID)
    );
    assert!(reconnected.outbound_rx.is_empty());

    // Denials must not end the watcher or prevent an authorized peer syncing.
    let (conn, allowed) = Conn::new_with_handle(ALLOWED);
    node.add_connection(conn.authenticated()).await?;
    let SyncMessage::BatchSyncRequest(request) =
        tokio::time::timeout(WAIT, allowed.outbound_rx.recv()).await??
    else {
        panic!("expected authorized sync request");
    };
    assert_eq!(request.id, ID);
    assert!(request.subscribe);
    assert_eq!(
        !request.fingerprint_summary.commit_fingerprints().is_empty(),
        cached
    );
    allowed
        .inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id: ID,
            req_id: request.req_id,
            result: SyncResult::Ok(SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: Vec::new(),
                requesting: RequestedData::default(),
            }),
            responder_heads: RemoteHeads::default(),
        }))
        .await?;
    assert!(matches!(
        tokio::time::timeout(WAIT, updates.next()).await?,
        Some(Ok(StoredEvent::PeerSynced { peer_id })) if peer_id == ALLOWED
    ));

    // Permission is per document, not just per connection.
    let (success, _, errors) = tokio::time::timeout(
        WAIT,
        node.sync_with_peer(
            &ALLOWED,
            SedimentreeId::new([99; 32]),
            false,
            CallTimeout::Uncapped,
        ),
    )
    .await??;
    assert!(!success && errors.is_empty());
    assert!(allowed.outbound_rx.is_empty());
    assert!(current.outbound_rx.is_empty());
    assert!(later.outbound_rx.is_empty());
    assert!(reconnected.outbound_rx.is_empty());
    updates.close();
    node.shutdown();
    Ok(())
}

#[tokio::test]
async fn forwarded_subscription_does_not_disclose_id_to_denied_upstream() -> TestResult {
    let (node, checks) = node();
    let (conn, upstream) = Conn::new_with_handle(DENIED);
    node.add_connection(conn.authenticated()).await?;
    let (conn, subscriber) = Conn::new_with_handle(ALLOWED);
    node.add_connection(conn.authenticated()).await?;
    subscriber
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(BatchSyncRequest {
            id: ID,
            req_id: RequestId {
                requestor: ALLOWED,
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
    // Wait for the upstream attempt, not just the inbound authorization check.
    tokio::time::timeout(WAIT, async {
        loop {
            if checks.recv().await? == (DENIED, ID) {
                return Ok::<_, async_channel::RecvError>(());
            }
        }
    })
    .await??;
    assert!(matches!(
        tokio::time::timeout(WAIT, subscriber.outbound_rx.recv()).await??,
        SyncMessage::BatchSyncResponse(_)
    ));
    assert!(upstream.outbound_rx.is_empty());
    node.shutdown();
    Ok(())
}

#[tokio::test]
async fn missing_document_watch_gates_current_future_and_reconnected_peers() -> TestResult {
    watch_respects_policy(false).await
}

#[tokio::test]
async fn cached_document_watch_gates_current_future_and_reconnected_peers() -> TestResult {
    watch_respects_policy(true).await
}
