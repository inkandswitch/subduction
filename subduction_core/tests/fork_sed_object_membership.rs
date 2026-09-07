//! Object-membership regression test for the fork-sedimentree shape.
//!
//! Motivation (townframe investigation): branch docs are created by forking a
//! parent automerge doc. The child's sedimentree contains the parent's commit
//! history plus one child boot commit whose `parents` point at the parent's
//! tip (same commit ids appear in both seds' DAGs). During sync, if a child
//! boot commit is ever delivered under the *parent's* sedimentree id (a stale
//! or mislabelled sed id at the message/relay boundary), subduction's receive
//! path must not admit it into the parent's tree: object membership must be
//! decided by the commit's own payload `sedimentree_id`, not trusted from the
//! wire `id` field.
//!
//! This test pins that boundary: a `LooseCommit` whose payload is bound to
//! `S_child` but whose wire message id is `S_parent` must never land in
//! `S_parent`'s tree. If it does, the parent doc's automerge history on the
//! replica silently absorbs the child's boot commit (the exact corruption
//! observed: parent facet overwritten with child identity), and the next
//! head's payload published under the parent sed carries a foreign head.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use std::sync::Arc;

use future_form::Sendable;
use futures::future::Aborted;
use std::collections::BTreeSet;
use subduction_core::{
    connection::{
        message::{SyncMessage, SyncResult},
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
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

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
                TokioSpawn,
            >,
            OpenPolicy,
            subduction_crypto::signer::memory::MemorySigner,
            InstantTimeout,
            TokioSpawn,
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

/// Build a signed loose commit plus its blob.
///
/// The commit's *payload* is bound to `payload_sed` (this is what the
/// subduction protocol seals). The caller may deliver it under a *different*
/// wire `id` to exercise the receive-path membership boundary.
async fn make_signed_commit(
    payload_sed: SedimentreeId,
    head: [u8; 32],
    parents: BTreeSet<sedimentree_core::loose_commit::id::CommitId>,
) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(head.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    #[allow(clippy::indexing_slicing)]
    let head = sedimentree_core::loose_commit::id::CommitId::new(head);
    let commit = LooseCommit::new(payload_sed, head, parents, blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

/// A child boot commit delivered under the parent's sed id is rejected.
///
/// Setup mirrors the fork shape: the child commit's parents point at the
/// parent's tip commit (shared DAG prefix). The wire message, however,
/// claims `S_parent`. Object membership must be payload-derived; subduction
/// must not admit the child commit into the parent's tree purely because
/// the message said so.
#[tokio::test]
async fn child_boot_commit_with_parent_wire_id_is_not_admitted_into_parent_sed() -> TestResult {
    let (node, listener, actor) = make_subduction();

    let parent_sed = SedimentreeId::new([1u8; 32]);
    let child_sed = SedimentreeId::new([2u8; 32]);
    let peer_id = PeerId::new([9u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    node.add_connection(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor);
    let listener_task = tokio::spawn(listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // ── Legit parent history: parent tip commit under S_parent. ──
    let parent_tip_id: [u8; 32] = {
        let mut b = [0u8; 32];
        b[0] = 0xAA;
        b
    };
    let (parent_tip, parent_blob) = make_signed_commit(parent_sed, parent_tip_id, BTreeSet::new()).await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: parent_sed,
            commit: parent_tip.clone(),
            blob: parent_blob.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── The child boot commit: payload bound to S_child, parents = parent tip.
    //    Delivered under wire id = S_parent (the mislabel this test pins). ──
    let child_boot_id: [u8; 32] = {
        let mut b = [0u8; 32];
        b[0] = 0xBB;
        b
    };
    let (child_boot, child_boot_blob) = make_signed_commit(
        child_sed,
        child_boot_id,
        BTreeSet::from([sedimentree_core::loose_commit::id::CommitId::new(parent_tip_id)]),
    )
    .await;
    handle
        .inbound_tx
        .send(SyncMessage::LooseCommit {
            id: parent_sed, // <-- deliberately wrong: should be child_sed
            commit: child_boot.clone(),
            blob: child_boot_blob.clone(),
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Assert: the parent's tree admits only payload-S_parent commits. ──
    let parent_commits = node.get_commits(parent_sed).await;
    let parent_heads: BTreeSet<_> = parent_commits
        .as_ref()
        .map(|cs| cs.iter().map(|c| c.head()).collect())
        .unwrap_or_default();

    assert!(
        !parent_heads.contains(&sedimentree_core::loose_commit::id::CommitId::new(child_boot_id)),
        "parent_sed admitted the child boot commit delivered under its wire id \
         (object membership violated: receive path trusted the message id instead of the \
          commit's payload sedimentree_id — the fork-sedimentree corruption bug)\n\
          parent_heads={parent_heads:?}"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// Fragments are subject to the same boundary: a fragment whose payload is
/// bound to `S_child` delivered under `S_parent` must not join the parent's
/// tree either.
#[tokio::test]
async fn child_boot_fragment_with_parent_wire_id_is_not_admitted_into_parent_sed() -> TestResult {
    let (node, listener, actor) = make_subduction();

    let parent_sed = SedimentreeId::new([1u8; 32]);
    let child_sed = SedimentreeId::new([2u8; 32]);
    let peer_id = PeerId::new([9u8; 32]);

    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    node.add_connection(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor);
    let listener_task = tokio::spawn(listener);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A fragment payload bound to the child sed, delivered under the parent id.
    let frag_head: [u8; 32] = {
        let mut b = [0u8; 32];
        b[0] = 0xCC;
        b
    };
    let blob = Blob::new(frag_head.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let fragment = sedimentree_core::fragment::Fragment::new(
        child_sed,
        sedimentree_core::loose_commit::id::CommitId::new(frag_head),
        BTreeSet::new(),
        &[],
        blob_meta,
    );
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment.clone()).await;

    handle
        .inbound_tx
        .send(SyncMessage::Fragment {
            id: parent_sed, // <-- deliberately wrong
            fragment: verified.into_signed(),
            blob,
            sender_heads: RemoteHeads::default(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let parent_fragments = node.get_fragments(parent_sed).await;
    let parent_frag_heads: BTreeSet<_> = parent_fragments
        .as_ref()
        .map(|fs| fs.iter().map(|f| f.head()).collect())
        .unwrap_or_default();

    assert!(
        !parent_frag_heads.contains(&sedimentree_core::loose_commit::id::CommitId::new(frag_head)),
        "parent_sed admitted the child-bound fragment under S_parent wire id (object membership violated at the fragment boundary too)\n\
         parent_fragments={parent_frag_heads:?}"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}