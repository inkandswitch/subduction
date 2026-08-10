//! A blob whose bytes don't match the commit's claimed `BlobMeta` must be
//! rejected like a signature or policy failure — without tearing down the
//! connection. A disconnect here sends a client with corrupt local state
//! into a reconnect-and-resend loop.

#![allow(clippy::panic)]

use core::time::Duration;
use std::{collections::BTreeSet, sync::Arc};

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::RemoteHeads,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use testresult::TestResult;

/// A commit whose blob doesn't match its claimed `BlobMeta` is dropped, the
/// connection survives, and a subsequent valid commit on the *same*
/// connection is accepted.
///
/// The `connected_peer_ids` assertion is the discriminating one: on the old
/// (connection-fatal) behavior the removal was state-only, so the reader
/// kept running and even the follow-up commit could land — only the map
/// membership reveals the disconnect.
#[tokio::test(flavor = "current_thread")]
async fn blob_mismatch_is_rejected_without_disconnecting() -> TestResult {
    let signer = MemorySigner::from_bytes(&[0xAA; 32]);
    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let peer_id = PeerId::new([1u8; 32]);

    let (subduction, _handler, listener_fut, actor_fut) =
        SubductionBuilder::<_, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.add_connection(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Claim metadata for one payload, but ship different bytes.
    let claimed_blob = Blob::new(b"the bytes the client indexed".to_vec());
    let actual_blob = Blob::new(b"the bytes the client uploads".to_vec());
    let commit = LooseCommit::new(
        sedimentree_id,
        CommitId::new([0xBB; 32]),
        BTreeSet::new(),
        BlobMeta::new(&claimed_blob),
    );
    let signed_commit = Signed::seal::<Sendable, _>(&signer, commit)
        .await
        .into_signed();

    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: signed_commit,
            blob: actual_blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // The mismatched commit must not be stored.
    let commits = subduction.get_commits(sedimentree_id).await;
    assert!(
        commits.is_none() || commits.as_ref().is_some_and(Vec::is_empty),
        "mismatched blob should not be stored, got: {commits:?}"
    );

    // A blob mismatch is a defective message, not a broken transport.
    assert!(
        subduction.connected_peer_ids().await.contains(&peer_id),
        "blob mismatch must not disconnect the peer"
    );

    // The same connection must still dispatch.
    let valid_blob = Blob::new(b"consistent bytes".to_vec());
    let valid_commit = LooseCommit::new(
        sedimentree_id,
        CommitId::new([0xCC; 32]),
        BTreeSet::new(),
        BlobMeta::new(&valid_blob),
    );
    let signed_valid = Signed::seal::<Sendable, _>(&signer, valid_commit)
        .await
        .into_signed();

    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: sedimentree_id,
            commit: signed_valid,
            blob: valid_blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let commits = subduction.get_commits(sedimentree_id).await;
    assert_eq!(
        commits.map(|c| c.len()),
        Some(1),
        "valid commit after a blob mismatch should be accepted on the same connection"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}
