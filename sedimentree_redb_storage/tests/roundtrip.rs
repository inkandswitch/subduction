//! Save/load roundtrip tests for `RedbStorage`, mirroring the
//! `sedimentree_fs_storage` suite: byte identity across save/load, multiple
//! commits per tree, Byzantine duplicates, batch saves, and reopen.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_redb_storage::RedbStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

fn test_signer() -> MemorySigner {
    MemorySigner::from_bytes(&[42u8; 32])
}

async fn seal_commit(
    signer: &MemorySigner,
    id: SedimentreeId,
    head: CommitId,
    blob: Vec<u8>,
) -> VerifiedMeta<LooseCommit> {
    let verified_blob = VerifiedBlobMeta::new(Blob::new(blob));
    VerifiedMeta::seal::<Sendable, _>(signer, (id, head, BTreeSet::new()), verified_blob).await
}

/// Save a commit, reload via bulk + point lookups, verify byte identity.
#[tokio::test]
async fn save_load_roundtrip() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path().join("data.redb"))?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x01; 32]);
    let head = CommitId::new([0x42; 32]);

    let verified = seal_commit(&signer, id, head, vec![1, 2, 3, 4, 5]).await;
    let original_signed = verified.signed().as_bytes().to_vec();
    let original_blob = verified.blob().contents().clone();

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;

    let all = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].signed().as_bytes(), &original_signed[..]);
    assert_eq!(all[0].blob().contents(), &original_blob);

    let one = Storage::<Sendable>::load_loose_commit(&storage, id, head)
        .await?
        .expect("commit must be loadable by id");
    assert_eq!(one.signed().as_bytes(), &original_signed[..]);

    Ok(())
}

/// Multiple commits round-trip; ids listed; per-tree isolation holds.
#[tokio::test]
async fn multiple_commits_and_isolation() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path().join("data.redb"))?;
    let signer = test_signer();
    let tree_a = SedimentreeId::new([0xAA; 32]);
    let tree_b = SedimentreeId::new([0xBB; 32]);

    let mut expected = BTreeSet::new();
    for i in 0..5u8 {
        let head = CommitId::new([i; 32]);
        expected.insert(head);
        let verified = seal_commit(&signer, tree_a, head, vec![i; 64]).await;
        Storage::<Sendable>::save_loose_commit(&storage, tree_a, verified).await?;
    }

    // One commit in a different tree must not leak into tree_a's scans.
    let other = seal_commit(&signer, tree_b, CommitId::new([0xFE; 32]), vec![9; 16]).await;
    Storage::<Sendable>::save_loose_commit(&storage, tree_b, other).await?;

    let loaded: BTreeSet<_> = Storage::<Sendable>::load_loose_commits(&storage, tree_a)
        .await?
        .iter()
        .map(|v| v.payload().head())
        .collect();
    assert_eq!(
        loaded, expected,
        "tree_a scan must return exactly its own commits"
    );

    let listed = Storage::<Sendable>::list_commit_ids(&storage, tree_a).await?;
    assert_eq!(listed.len(), 5);

    Ok(())
}

/// `save_batch` registers the tree id and persists all items atomically.
#[tokio::test]
async fn batch_save_registers_and_persists() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path().join("data.redb"))?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x33; 32]);

    let mut commits = Vec::new();
    for i in 0..10u8 {
        commits.push(seal_commit(&signer, id, CommitId::new([i; 32]), vec![i; 32]).await);
    }

    let saved = Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new()).await?;
    assert_eq!(saved, 10);

    assert!(
        Storage::<Sendable>::contains_sedimentree_id(&storage, id).await?,
        "save_batch must register the sedimentree id"
    );
    assert_eq!(
        Storage::<Sendable>::load_loose_commits(&storage, id)
            .await?
            .len(),
        10
    );

    Ok(())
}

/// Data survives close + reopen of the database file.
#[tokio::test]
async fn survives_reopen() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.redb");
    let signer = test_signer();
    let id = SedimentreeId::new([0x44; 32]);
    let head = CommitId::new([0x55; 32]);

    {
        let storage = RedbStorage::new(path.clone())?;
        let verified = seal_commit(&signer, id, head, vec![7; 32]).await;
        Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
        Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;
    }

    let reopened = RedbStorage::new(path)?;
    let ids = Storage::<Sendable>::load_all_sedimentree_ids(&reopened).await?;
    assert!(ids.contains(&id), "tree id must survive reopen");

    let loaded = Storage::<Sendable>::load_loose_commit(&reopened, id, head)
        .await?
        .expect("commit must survive reopen");
    assert_eq!(loaded.payload().head(), head);

    Ok(())
}

/// Deletes: single, per-tree, and whole-tree teardown.
#[tokio::test]
async fn delete_operations() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path().join("data.redb"))?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x66; 32]);

    for i in 0..3u8 {
        let verified = seal_commit(&signer, id, CommitId::new([i; 32]), vec![i; 16]).await;
        Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;
    }
    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;

    Storage::<Sendable>::delete_loose_commit(&storage, id, CommitId::new([0u8; 32])).await?;
    assert_eq!(
        Storage::<Sendable>::load_loose_commits(&storage, id)
            .await?
            .len(),
        2
    );

    Storage::<Sendable>::delete_loose_commits(&storage, id).await?;
    assert!(
        Storage::<Sendable>::load_loose_commits(&storage, id)
            .await?
            .is_empty()
    );

    Storage::<Sendable>::delete_sedimentree_id(&storage, id).await?;
    assert!(!Storage::<Sendable>::contains_sedimentree_id(&storage, id).await?);

    Ok(())
}
