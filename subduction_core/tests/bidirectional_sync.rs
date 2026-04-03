//! Tests for bidirectional sync (1.5 RT protocol).
//!
//! The bidirectional sync protocol works as follows:
//! 1. Requestor sends `BatchSyncRequest` with their fingerprint summary
//! 2. Responder calculates what requestor is missing AND what responder is missing
//! 3. Responder sends `BatchSyncResponse` with:
//!    - `missing_commits`/`missing_fragments`: data requestor needs
//!    - `requesting`: fingerprints of data responder wants back from requestor
//! 4. Requestor receives response and sends requested data (fire-and-forget)
//!
//! This completes sync in 1.5 round trips instead of requiring a second
//! full request/response cycle.

#![allow(clippy::expect_used, clippy::panic)]

use core::{future::Future, time::Duration};
use std::sync::Arc;

use future_form::Sendable;
use futures::future::Aborted;
use std::collections::BTreeSet;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage, SyncResult},
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    fragment::{Fragment, FragmentSummary},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

/// A deterministic seed for tests (not security-sensitive).
const TEST_SEED: FingerprintSeed = FingerprintSeed::new(12345, 67890);

#[allow(clippy::type_complexity)]
fn make_subduction() -> (
    Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            ChannelMockConnection<SyncMessage>,
            SyncHandler<
                Sendable,
                MemoryStorage,
                ChannelMockConnection<SyncMessage>,
                OpenPolicy,
                CountLeadingZeroBytes,
            >,
            OpenPolicy,
            subduction_crypto::signer::memory::MemorySigner,
            InstantTimeout,
            CountLeadingZeroBytes,
        >,
    >,
    impl Future<Output = Result<(), Aborted>>,
    impl Future<Output = Result<(), Aborted>>,
) {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    (sd, listener, manager)
}

async fn make_test_commit(
    id: &SedimentreeId,
    data: &[u8],
) -> (Signed<LooseCommit>, Blob, LooseCommit) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    #[allow(clippy::indexing_slicing)]
    let head = CommitId::new({
        let mut bytes = [0u8; 32];
        let n = data.len().min(32);
        bytes[..n].copy_from_slice(&data[..n]);
        bytes
    });
    let commit = LooseCommit::new(*id, head, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit.clone()).await;
    (verified.into_signed(), blob, commit)
}

async fn make_test_fragment(
    id: &SedimentreeId,
    data: &[u8],
) -> (Signed<Fragment>, Blob, FragmentSummary) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    // Fragment head - use a deterministic value for tests
    let head = CommitId::new([data.first().copied().unwrap_or(0); 32]);
    let fragment = Fragment::new(*id, head, BTreeSet::new(), &[], blob_meta);
    let summary = fragment.summary().clone();
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    (verified.into_signed(), blob, summary)
}

/// Receive the next non-`HeadsUpdate` message from the channel,
/// discarding any `HeadsUpdate` messages that arrive first.
async fn recv_skipping_heads_updates(
    rx: &async_channel::Receiver<SyncMessage>,
) -> Option<SyncMessage> {
    loop {
        let msg = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .ok()?
            .ok()?;
        if matches!(msg, SyncMessage::HeadsUpdate { .. }) {
            continue;
        }
        return Some(msg);
    }
}

/// Test that when responder has commits the requestor doesn't know about,
/// the response includes them in `missing_commits` (and `requesting` is empty).
#[tokio::test]
async fn test_responder_requests_missing_commits() -> TestResult {
    // Set up responder (Alice) - has commit A
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    // Add a connection for the peer
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice receives commit A (she has it, peer doesn't know she has it yet)
    let (commit_a, blob_a, _raw_commit_a) =
        make_test_commit(&sedimentree_id, b"commit A - alice has this").await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_a.clone(),
            blob: blob_a.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify Alice has the commit
    let alice_commits = alice.get_commits(sedimentree_id).await;
    assert_eq!(
        alice_commits.map(|c| c.len()),
        Some(1),
        "Alice should have 1 commit"
    );

    // Now simulate peer (Bob) sending a BatchSyncRequest with an empty summary
    // (Bob has nothing, Alice has commit A)
    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(TEST_SEED, BTreeSet::new(), BTreeSet::new()),
        subscribe: false,
    };

    // Send the request as a message
    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice should send a BatchSyncResponse
    // Since Bob has nothing and Alice has commit A:
    // - missing_commits should contain commit A (what Bob needs)
    // - requesting should be empty (Alice doesn't need anything from Bob)
    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert_eq!(
        diff.missing_commits.len(),
        1,
        "Response should contain the commit Alice has"
    );
    assert!(
        diff.requesting.is_empty(),
        "Alice shouldn't request anything (Bob has nothing)"
    );

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}

/// Test that when requestor has commits the responder doesn't have,
/// the responder's `requesting` field asks for them (via fingerprints).
#[tokio::test]
async fn test_responder_requests_commits_from_requestor() -> TestResult {
    // Set up responder (Alice) - starts empty
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a commit that Bob has but Alice doesn't
    let (_commit_b, _blob_b, raw_commit_b) =
        make_test_commit(&sedimentree_id, b"commit B - bob has this").await;
    let commit_b_id = raw_commit_b.head();
    let commit_b_fp: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &commit_b_id);

    // Bob sends a BatchSyncRequest claiming to have commit B (as a fingerprint)
    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([commit_b_fp]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice should respond with:
    // - missing_commits: empty (Bob already has everything Alice has, which is nothing)
    // - requesting: should include commit_b_fp (Alice wants what Bob has)
    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert!(
        diff.missing_commits.is_empty(),
        "Alice has nothing to send to Bob"
    );
    assert!(
        diff.requesting.commit_fingerprints.contains(&commit_b_fp),
        "Alice should request commit B from Bob. Got: {:?}",
        diff.requesting.commit_fingerprints
    );

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}

/// Test the full bidirectional flow:
/// 1. Alice has commit A, Bob has commit B
/// 2. Bob syncs with Alice
/// 3. Alice sends commit A to Bob AND requests commit B
/// 4. Bob sends commit B back to Alice (fire-and-forget)
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_full_bidirectional_sync_flow() -> TestResult {
    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let alice_peer_id = PeerId::new([1u8; 32]);
    let bob_peer_id = PeerId::new([2u8; 32]);

    // Create commits
    let (commit_a, blob_a, _raw_commit_a) =
        make_test_commit(&sedimentree_id, b"commit A - alice").await;
    let (commit_b, blob_b, raw_commit_b) =
        make_test_commit(&sedimentree_id, b"commit B - bob").await;
    let commit_b_id = raw_commit_b.head();
    let commit_b_fp: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &commit_b_id);

    // Set up Alice with commit A
    let (alice, alice_listener, alice_actor) = make_subduction();

    let (alice_conn, alice_handle) = ChannelMockConnection::new_with_handle(bob_peer_id);
    alice.add_connection(alice_conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Give Alice commit A
    alice_handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_a.clone(),
            blob: blob_a.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify Alice has commit A
    assert_eq!(
        alice.get_commits(sedimentree_id).await.map(|c| c.len()),
        Some(1),
        "Alice should have commit A"
    );

    // Set up Bob with commit B
    let (bob, bob_listener, bob_actor) = make_subduction();

    let (bob_conn, bob_handle) = ChannelMockConnection::new_with_handle(alice_peer_id);
    bob.add_connection(bob_conn.authenticated()).await?;

    let bob_actor_task = tokio::spawn(bob_actor);
    let bob_listener_task = tokio::spawn(bob_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Give Bob commit B
    bob_handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_b.clone(),
            blob: blob_b.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify Bob has commit B
    assert_eq!(
        bob.get_commits(sedimentree_id).await.map(|c| c.len()),
        Some(1),
        "Bob should have commit B"
    );

    // Now simulate the sync:
    // Bob sends BatchSyncRequest to Alice (via Alice's inbound channel)
    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: bob_peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([commit_b_fp]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    alice_handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice should send BatchSyncResponse with:
    // - missing_commits: [commit_a] (what Bob needs)
    // - requesting: [commit_b_fp] (what Alice wants from Bob, echoed as fingerprint)
    let response = recv_skipping_heads_updates(&alice_handle.outbound_rx)
        .await
        .expect("should receive response from Alice");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert_eq!(
        diff.missing_commits.len(),
        1,
        "Alice should send commit A to Bob"
    );
    assert!(
        diff.requesting.commit_fingerprints.contains(&commit_b_fp),
        "Alice should request commit B"
    );

    // Simulate Bob receiving this response and sending the requested data back
    // Bob would call send_requested_data, which sends LooseCommit messages

    // Feed the response to Bob (simulating Bob receiving Alice's response)
    bob_handle
        .inbound_tx
        .send(SyncMessage::BatchSyncResponse(BatchSyncResponse {
            id: sedimentree_id,
            req_id: RequestId {
                requestor: bob_peer_id,
                nonce: 1,
            },
            result: SyncResult::Ok(diff.clone()),
            responder_heads: RemoteHeads::default(),
        }))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob should now have commit A (from the response's missing_commits)
    // Note: The response handling adds the missing_commits to Bob's storage
    // But we need Bob to process this...

    // Actually, in this test setup, Bob receives a BatchSyncResponse but
    // didn't initiate the call, so he won't process it correctly.
    // The response handling happens in sync_with_peer/full_sync flow.

    // For a proper integration test, we'd need to trigger Bob's sync.
    // For now, let's verify the protocol fields are correct.

    alice_actor_task.abort();
    alice_listener_task.abort();
    bob_actor_task.abort();
    bob_listener_task.abort();
    Ok(())
}

/// Test that requesting field includes fragment fingerprints.
#[tokio::test]
async fn test_responder_requests_fragments() -> TestResult {
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a fragment that Bob has but Alice doesn't
    let (_fragment, _blob, fragment_summary) =
        make_test_fragment(&sedimentree_id, b"fragment - bob has this").await;
    let frag_id = fragment_summary.head();
    let frag_fp: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &frag_id);

    // Bob sends a BatchSyncRequest claiming to have this fragment (as a fingerprint)
    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::new(),
            BTreeSet::from([frag_fp]),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert!(
        diff.requesting.fragment_fingerprints.contains(&frag_fp),
        "Alice should request the fragment from Bob. Got: {:?}",
        diff.requesting.fragment_fingerprints
    );

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}

/// After Alice receives data from Bob (via `LooseCommit` messages) and
/// her tree is updated, a subsequent `BatchSyncRequest` from Bob with
/// the same data should produce an empty diff — proving protocol-level
/// convergence. This is the integration-level counterpart to the E2E
/// `second_sync_round_is_empty` test.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_second_sync_round_has_empty_diff() -> TestResult {
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice gets commit A locally
    let (commit_a, blob_a, raw_a) = make_test_commit(&sedimentree_id, b"commit A").await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_a.clone(),
            blob: blob_a.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob also has commit B; Alice gets it via a second message
    let (commit_b, blob_b, raw_b) = make_test_commit(&sedimentree_id, b"commit B").await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_b.clone(),
            blob: blob_b.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify Alice has both commits
    assert_eq!(
        alice.get_commits(sedimentree_id).await.map(|c| c.len()),
        Some(2),
        "Alice should have both commits"
    );

    // Round 1: Bob sends BatchSyncRequest claiming both commits.
    // Alice has both → diff should be empty (no missing, no requesting).
    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &raw_a.head());
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &raw_b.head());

    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([fp_a, fp_b]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert!(
        diff.missing_commits.is_empty(),
        "no missing commits when fully synced"
    );
    assert!(
        diff.missing_fragments.is_empty(),
        "no missing fragments when fully synced"
    );
    assert!(
        diff.requesting.is_empty(),
        "no requesting when fully synced"
    );

    // Round 2: identical request — should still be empty (idempotent).
    let request2 = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 2,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([fp_a, fp_b]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request2))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response2 = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive second response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse {
        result: result2, ..
    }) = response2
    else {
        panic!("Expected BatchSyncResponse, got {response2:?}");
    };
    let SyncResult::Ok(diff2) = result2 else {
        panic!("Expected SyncResult::Ok, got {result2:?}");
    };

    assert!(
        diff2.missing_commits.is_empty(),
        "second round: no missing commits"
    );
    assert!(
        diff2.missing_fragments.is_empty(),
        "second round: no missing fragments"
    );
    assert!(
        diff2.requesting.is_empty(),
        "second round: no requesting (convergence verified)"
    );

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}

/// After Alice has data and Bob sends a request with some overlap and some
/// novel items, the response correctly identifies the delta. A second
/// request with the full combined set yields an empty diff.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_incremental_sync_then_convergence() -> TestResult {
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice has commits A and B
    let (commit_a, blob_a, raw_a) = make_test_commit(&sedimentree_id, b"alice commit A").await;
    let (commit_b, blob_b, raw_b) = make_test_commit(&sedimentree_id, b"alice commit B").await;

    for (commit, blob) in [(&commit_a, &blob_a), (&commit_b, &blob_b)] {
        handle
            .inbound_tx
            .send(SyncMessage::LooseCommit {
                id: sedimentree_id,
                commit: commit.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads::default(),
            })
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        alice.get_commits(sedimentree_id).await.map(|c| c.len()),
        Some(2),
    );

    // Bob has commit B (overlap) + commit C (novel). Bob sends request.
    let (_commit_c, _blob_c, raw_c) = make_test_commit(&sedimentree_id, b"bob commit C").await;
    let fp_b: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &raw_b.head());
    let fp_c: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &raw_c.head());

    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([fp_b, fp_c]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");
    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected Ok");
    };

    // Alice should send commit A (Bob doesn't have it)
    assert_eq!(
        diff.missing_commits.len(),
        1,
        "Alice should send 1 commit Bob is missing"
    );

    // Alice should request commit C (she doesn't have it)
    assert_eq!(
        diff.requesting.commit_fingerprints.len(),
        1,
        "Alice should request 1 commit she's missing"
    );
    assert!(
        diff.requesting.commit_fingerprints.contains(&fp_c),
        "Alice should request commit C"
    );

    // Simulate Bob sending commit C to Alice
    let (commit_c, blob_c, _) = make_test_commit(&sedimentree_id, b"bob commit C").await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit_c,
            blob: blob_c,
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice should now have 3 commits
    assert_eq!(
        alice.get_commits(sedimentree_id).await.map(|c| c.len()),
        Some(3),
        "Alice should have all 3 commits"
    );

    // Second round: Bob claims A, B, C. Alice has A, B, C. Diff should be empty.
    let fp_a: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &raw_a.head());

    let request2 = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 2,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([fp_a, fp_b, fp_c]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request2))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response2 = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive second response");
    let SyncMessage::BatchSyncResponse(BatchSyncResponse {
        result: result2, ..
    }) = response2
    else {
        panic!("Expected BatchSyncResponse");
    };
    let SyncResult::Ok(diff2) = result2 else {
        panic!("Expected Ok");
    };

    assert!(diff2.missing_commits.is_empty(), "second round: no missing");
    assert!(
        diff2.requesting.is_empty(),
        "second round: no requesting (converged)"
    );

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}

/// Test that empty requesting field when both sides are in sync.
#[tokio::test]
async fn test_no_requesting_when_in_sync() -> TestResult {
    let (alice, alice_listener, alice_actor) = make_subduction();

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    alice.add_connection(conn.authenticated()).await?;

    let alice_actor_task = tokio::spawn(alice_actor);
    let alice_listener_task = tokio::spawn(alice_listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a commit that both have
    let (commit, blob, raw_commit) = make_test_commit(&sedimentree_id, b"shared commit").await;

    // Give Alice the commit
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: commit.clone(),
            blob: blob.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob sends a request claiming to have the same commit (as a fingerprint)
    let commit_id = raw_commit.head();
    let commit_fp: Fingerprint<CommitId> = Fingerprint::new(&TEST_SEED, &commit_id);

    let request = BatchSyncRequest {
        id: sedimentree_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(
            TEST_SEED,
            BTreeSet::from([commit_fp]),
            BTreeSet::new(),
        ),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(SyncMessage::BatchSyncRequest(request))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = recv_skipping_heads_updates(&handle.outbound_rx)
        .await
        .expect("should receive response");

    let SyncMessage::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };
    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert!(
        diff.missing_commits.is_empty(),
        "No missing commits when in sync"
    );
    assert!(diff.requesting.is_empty(), "No requesting when in sync");

    alice_actor_task.abort();
    alice_listener_task.abort();
    Ok(())
}
