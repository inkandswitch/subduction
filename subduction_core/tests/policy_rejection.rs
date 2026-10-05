//! Tests for policy rejection behavior.

#![allow(clippy::panic)]

use core::{convert::Infallible, fmt, time::Duration};
use std::{collections::BTreeSet, sync::Arc, vec::Vec};

use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{
    blob::Blob,
    crypto::fingerprint::FingerprintSeed,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_core::{
    connection::{
        message::{
            BatchSyncResponse, DataRequestRejected, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
        test_utils::{
            ChannelMockConnection, InstantTimeout, MockConnection, TestSpawn, TokioSpawn,
            TokioTimeout, test_signer,
        },
    },
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
    timeout::call::CallTimeout,
};
use subduction_crypto::verified_author::VerifiedAuthor;
use testresult::TestResult;

/// A policy that rejects all puts but allows connections and fetches.
#[derive(Clone, Copy)]
struct RejectPutsPolicy;

/// Error returned when a put is rejected.
#[derive(Debug, Clone, Copy)]
struct PutRejected;

impl fmt::Display for PutRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "put rejected by policy")
    }
}

impl core::error::Error for PutRejected {}

impl ConnectionPolicy<Sendable> for RejectPutsPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RejectPutsPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = PutRejected;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async { Err(PutRejected) }.boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

/// A policy that allows puts only to a specific sedimentree ID.
#[derive(Clone)]
struct AllowSpecificIdPolicy {
    allowed_id: SedimentreeId,
}

impl ConnectionPolicy<Sendable> for AllowSpecificIdPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for AllowSpecificIdPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = PutRejected;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let allowed = self.allowed_id;
        async move {
            if sedimentree_id == allowed {
                Ok(())
            } else {
                Err(PutRejected)
            }
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

fn make_test_blob(data: &[u8]) -> Blob {
    Blob::new(data.to_vec())
}

fn make_commit_parts(data: &[u8]) -> (CommitId, BTreeSet<CommitId>, Blob) {
    let blob = make_test_blob(data);
    (CommitId::new([0xDD; 32]), BTreeSet::new(), blob)
}

/// Local operations (`add_sedimentree`, `add_commit`) bypass policy — the
/// node trusts itself. Policy rejection only applies to remote data
/// received via sync; see the
/// `unauthorized_fetch_returns_unauthorized_result` test below.
#[tokio::test]
async fn local_add_sedimentree_bypasses_put_policy() {
    let (subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(RejectPutsPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    // Local operations succeed even with a rejecting put policy
    let result = subduction
        .add_sedimentree(id, tree, blobs, CallTimeout::Default)
        .await;
    assert!(result.is_ok(), "Local add_sedimentree should bypass policy");

    let ids = subduction.sedimentree_ids().await;
    assert!(ids.contains(&id), "Sedimentree should be stored");
}

#[tokio::test]
async fn local_add_commit_bypasses_put_policy() {
    let (subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(RejectPutsPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();

    let id = SedimentreeId::new([1u8; 32]);
    let (head, parents, blob) = make_commit_parts(b"test data");

    // Local operations succeed even with a rejecting put policy
    let result = subduction.add_commit(id, head, parents, blob).await;
    assert!(result.is_ok(), "Local add_commit should bypass policy");
}

#[tokio::test]
async fn local_adds_bypass_id_specific_policy() -> TestResult {
    let allowed_id = SedimentreeId::new([42u8; 32]);
    let other_id = SedimentreeId::new([99u8; 32]);

    let (subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(
                MemoryStorage::new(),
                Arc::new(AllowSpecificIdPolicy { allowed_id }),
            )
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();

    // Local operations succeed for both IDs — policy only applies to remote data
    let tree = Sedimentree::default();
    let result = subduction
        .add_sedimentree(allowed_id, tree.clone(), Vec::new(), CallTimeout::Default)
        .await;
    assert!(result.is_ok(), "Local add to allowed ID should succeed");

    let result = subduction
        .add_sedimentree(other_id, tree, Vec::new(), CallTimeout::Default)
        .await;
    assert!(
        result.is_ok(),
        "Local add to other ID should also succeed (policy bypassed)"
    );

    let ids = subduction.sedimentree_ids().await;
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&allowed_id));
    assert!(ids.contains(&other_id));

    Ok(())
}

#[tokio::test]
async fn local_add_stores_data_despite_rejecting_policy() {
    let (subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(RejectPutsPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();

    // Local add succeeds (policy bypassed)
    let result = subduction
        .add_sedimentree(id, tree, Vec::new(), CallTimeout::Default)
        .await;
    assert!(result.is_ok());

    // Data was stored
    let ids = subduction.sedimentree_ids().await;
    assert!(!ids.is_empty(), "Sedimentree ID should be stored");
}

/// A policy that rejects all fetches but allows connections and puts.
#[derive(Clone, Copy)]
struct RejectFetchPolicy;

/// Error returned when a fetch is rejected.
#[derive(Debug, Clone, Copy)]
struct FetchRejected;

impl fmt::Display for FetchRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fetch rejected by policy")
    }
}

impl core::error::Error for FetchRejected {}

impl ConnectionPolicy<Sendable> for RejectFetchPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for RejectFetchPolicy {
    type FetchDisallowed = FetchRejected;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Err(FetchRejected) }.boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        _ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async { Vec::new() }.boxed()
    }
}

#[tokio::test]
async fn multiple_local_adds_succeed_despite_rejecting_policy() {
    let (subduction, _handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(RejectPutsPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, MockConnection>();

    // All local operations succeed (policy bypassed)
    for i in 0..5u8 {
        let id = SedimentreeId::new([i; 32]);
        let tree = Sedimentree::default();
        let result = subduction
            .add_sedimentree(id, tree, Vec::new(), CallTimeout::Default)
            .await;
        assert!(result.is_ok(), "Local add {i} should succeed");
    }

    let ids = subduction.sedimentree_ids().await;
    assert_eq!(ids.len(), 5, "All 5 sedimentrees should be stored");
}

/// Both sync entry points must finish processing while telling the responder
/// why no return-direction data follows its request.
#[allow(clippy::too_many_lines)]
async fn requested_data_rejection(all_peers: bool) -> TestResult {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use subduction_core::{metrics::names, remote_heads::RemoteHeads};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    metrics::counter!(names::REQUESTED_DATA_SEND_FAILURES_TOTAL).absolute(0);

    let (node, _, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(RejectFetchPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();
    let id = SedimentreeId::new([73; 32]);
    node.store_commit(
        id,
        CommitId::new([74; 32]),
        BTreeSet::new(),
        Blob::new(b"private".to_vec()),
    )
    .await?;
    let peer = PeerId::new([75; 32]);
    let (conn, remote) = ChannelMockConnection::new_with_handle(peer);
    node.add_connection(conn.authenticated()).await?;
    let listener = tokio::spawn(listener);
    let manager = tokio::spawn(manager);

    let responder = async {
        let SyncMessage::BatchSyncRequest(request) = remote.outbound_rx.recv().await? else {
            panic!("expected a sync request");
        };
        assert!(!request.fingerprint_summary.commit_fingerprints().is_empty());
        remote
            .inbound_tx
            .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
                id,
                req_id: request.req_id,
                result: SyncResult::Ok(SyncDiff {
                    missing_commits: Vec::new(),
                    missing_fragments: Vec::new(),
                    requesting: RequestedData {
                        commit_fingerprints: request
                            .fingerprint_summary
                            .commit_fingerprints()
                            .iter()
                            .copied()
                            .collect(),
                        fragment_fingerprints: Vec::new(),
                    },
                }),
                responder_heads: RemoteHeads::default(),
            }))
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    let syncing = async {
        let (success, stats, errors) = if all_peers {
            let mut results = node
                .sync_with_all_peers(id, false, CallTimeout::Default)
                .await?
                .into_map();
            results.remove(&peer).ok_or("peer result missing")?
        } else {
            node.sync_with_peer(&peer, id, false, CallTimeout::Default)
                .await?
        };
        assert!(success, "intentional rejection still completes the round");
        assert!(errors.is_empty());
        assert_eq!(stats.commits_sent + stats.fragments_sent, 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        let (response, synced) = futures::join!(responder, syncing);
        response?;
        synced
    })
    .await??;

    // Processing has finished, so all return-direction messages are queued.
    // Check the whole queue: a rejection plus leaked data must fail too.
    let messages: Vec<_> = std::iter::from_fn(|| remote.outbound_rx.try_recv().ok()).collect();
    assert_eq!(
        messages.len(),
        1,
        "only the rejection should be sent: {messages:?}"
    );
    assert!(
        matches!(messages.first(), Some(SyncMessage::DataRequestRejected(DataRequestRejected { id: rejected })) if *rejected == id)
    );
    let failed_sends =
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _, _, value)| {
                let (_, key) = key.into_parts();
                match value {
                    DebugValue::Counter(count)
                        if key.name() == names::REQUESTED_DATA_SEND_FAILURES_TOTAL =>
                    {
                        Some(count)
                    }
                    DebugValue::Counter(_) | DebugValue::Gauge(_) | DebugValue::Histogram(_) => {
                        None
                    }
                }
            });
    assert_eq!(
        failed_sends,
        Some(0),
        "policy rejection is not a send failure"
    );
    node.shutdown();
    listener.await??;
    manager.await??;
    Ok(())
}

#[tokio::test]
async fn single_peer_requested_data_rejection_is_reported_without_disclosure() -> TestResult {
    requested_data_rejection(false).await
}

#[tokio::test]
async fn all_peer_requested_data_rejection_is_reported_without_disclosure() -> TestResult {
    requested_data_rejection(true).await
}

/// A policy that allows puts only from a specific author (verifying key).
#[derive(Clone)]
struct AllowSpecificAuthorPolicy {
    allowed_author: ed25519_dalek::VerifyingKey,
}

impl ConnectionPolicy<Sendable> for AllowSpecificAuthorPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async { Ok(()) }.boxed()
    }
}

impl StoragePolicy<Sendable> for AllowSpecificAuthorPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = PutRejected;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async { Ok(()) }.boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        author: VerifiedAuthor,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let allowed = self.allowed_author;
        async move {
            if *author.verifying_key() == allowed {
                Ok(())
            } else {
                Err(PutRejected)
            }
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move { ids }.boxed()
    }
}

/// Remote commits signed by an unauthorized author are rejected by the
/// policy, even though the relay peer is allowed to connect and fetch.
/// This is the end-to-end test for the `VerifiedAuthor` security fix.
#[tokio::test]
async fn remote_commit_from_unauthorized_author_is_rejected() -> TestResult {
    use subduction_core::{
        connection::{
            message::SyncMessage,
            test_utils::{ChannelMockConnection, TokioSpawn},
        },
        remote_heads::RemoteHeads,
    };
    use subduction_crypto::signer::memory::MemorySigner;

    // The "allowed" author — only commits signed by this key are accepted.
    let allowed_signer = MemorySigner::from_bytes(&[0xAA; 32]);
    let allowed_vk = allowed_signer.verifying_key();

    // The "unauthorized" author — commits signed by this key should be rejected.
    let unauthorized_signer = MemorySigner::from_bytes(&[0xBB; 32]);

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let relay_peer_id = PeerId::new([1u8; 32]);

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(
                MemoryStorage::new(),
                Arc::new(AllowSpecificAuthorPolicy {
                    allowed_author: allowed_vk,
                }),
            )
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let (conn, handle) = ChannelMockConnection::new_with_handle(relay_peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a commit signed by the UNAUTHORIZED author.
    let blob = Blob::new(b"unauthorized data".to_vec());
    let blob_meta = sedimentree_core::blob::BlobMeta::new(&blob);
    let commit = LooseCommit::new(
        sedimentree_id,
        CommitId::new([0xBB; 32]),
        BTreeSet::new(),
        blob_meta,
    )?;
    let verified =
        subduction_crypto::signed::Signed::seal::<Sendable, _>(&unauthorized_signer, commit).await;
    let signed_commit = verified.into_signed();

    // Send the commit via the relay peer.
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: signed_commit,
            blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // The commit should NOT have been stored — the policy rejects the author.
    let commits = subduction.get_commits(sedimentree_id).await;
    assert!(
        commits.is_none() || commits.as_ref().is_some_and(Vec::is_empty),
        "Commit from unauthorized author should be rejected by policy"
    );

    // Now send a commit signed by the ALLOWED author — should succeed.
    let allowed_blob = Blob::new(b"authorized data".to_vec());
    let allowed_blob_meta = sedimentree_core::blob::BlobMeta::new(&allowed_blob);
    let allowed_commit = LooseCommit::new(
        sedimentree_id,
        CommitId::new([0xAA; 32]),
        BTreeSet::new(),
        allowed_blob_meta,
    )?;
    let allowed_verified =
        subduction_crypto::signed::Signed::seal::<Sendable, _>(&allowed_signer, allowed_commit)
            .await;
    let allowed_commit_signed = allowed_verified.into_signed();

    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: allowed_commit_signed,
            blob: allowed_blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // This commit SHOULD be stored — the author is allowed.
    let commits = subduction.get_commits(sedimentree_id).await;
    assert_eq!(
        commits.map(|c| c.len()),
        Some(1),
        "Commit from allowed author should be accepted"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// When fetch policy rejects, `recv_batch_sync_request` should respond
/// with `SyncResult::Unauthorized` so the peer knows they're not authorized.
#[tokio::test]
async fn unauthorized_fetch_returns_unauthorized_result() -> TestResult {
    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(RejectFetchPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let seed = FingerprintSeed::new(1, 2);

    // Send a BatchSyncRequest — the fetch policy should reject it
    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(
            subduction_core::connection::message::BatchSyncRequest {
                id: sedimentree_id,
                req_id: subduction_core::connection::message::RequestId {
                    requestor: peer_id,
                    nonce: 1,
                },
                fingerprint_summary: FingerprintSummary::new(
                    seed,
                    BTreeSet::new(),
                    BTreeSet::new(),
                ),
                subscribe: false,
            },
        ))
        .await?;

    // Wait for the dispatch to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should receive a BatchSyncResponse with SyncResult::Unauthorized
    let response = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .map_err(|e| format!("channel closed: {e}"))?;

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, id, .. }) = response else {
        panic!("expected BatchSyncResponse, got {response:?}");
    };

    assert_eq!(
        id, sedimentree_id,
        "response should be for the requested sedimentree"
    );
    assert!(
        matches!(result, SyncResult::Unauthorized),
        "expected SyncResult::Unauthorized, got {result:?}"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}
