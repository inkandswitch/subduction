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
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use subduction_redb_storage::RedbStorage;

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
    let storage = RedbStorage::new(dir.path())?;
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
    let storage = RedbStorage::new(dir.path())?;
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
    let storage = RedbStorage::new(dir.path())?;
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
    let path = dir.path().to_path_buf();
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
    let storage = RedbStorage::new(dir.path())?;
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

/// Recursively collect every external blob file under the `blobs/` dir.
fn blob_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }

    let mut out = Vec::new();
    walk(
        &root.join(subduction_redb_storage::BLOBS_DIR_NAME),
        &mut out,
    );
    out
}

/// A blob over the inline threshold is stored as an external file and
/// round-trips byte-identically (including across reopen).
#[tokio::test]
async fn large_blob_externalized_and_roundtrips() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x88; 32]);
    let head = CommitId::new([0x99; 32]);

    // Threshold of 64 bytes so a 1 KiB blob goes external.
    let big_blob: Vec<u8> = (0..1024u32)
        .map(|i| u8::try_from(i % 256).unwrap_or(0))
        .collect();

    {
        let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
        let verified = seal_commit(&signer, id, head, big_blob.clone()).await;
        Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;
    }

    let files = blob_files(dir.path());
    assert_eq!(files.len(), 1, "expected exactly one external blob file");
    assert_eq!(
        std::fs::read(&files[0])?,
        big_blob,
        "external file must hold the raw blob bytes"
    );

    // Reload through a fresh handle (and the *default* threshold: reads
    // dispatch on the stored tag, not the configured threshold).
    let reopened = RedbStorage::new(dir.path())?;
    let loaded = Storage::<Sendable>::load_loose_commit(&reopened, id, head)
        .await?
        .expect("commit must be loadable");
    assert_eq!(
        loaded.blob().contents(),
        &big_blob,
        "blob must round-trip byte-identically through the external file"
    );

    let bulk = Storage::<Sendable>::load_loose_commits(&reopened, id).await?;
    assert_eq!(bulk.len(), 1);
    assert_eq!(bulk[0].blob().contents(), &big_blob);

    Ok(())
}

/// A blob at or under the threshold stays inline: no external files appear.
#[tokio::test]
async fn small_blob_stays_inline() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x8A; 32]);

    let verified = seal_commit(&signer, id, CommitId::new([0x01; 32]), vec![7; 256]).await;
    Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;

    assert!(
        blob_files(dir.path()).is_empty(),
        "small blobs must not create external files"
    );

    Ok(())
}

/// Two commits sharing identical blob contents share one external file
/// (content addressing deduplicates).
#[tokio::test]
async fn identical_large_blobs_deduplicate() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x8B; 32]);

    let shared_blob = vec![0xEE; 512];
    for i in 0..2u8 {
        let verified = seal_commit(&signer, id, CommitId::new([i; 32]), shared_blob.clone()).await;
        Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;
    }

    assert_eq!(
        blob_files(dir.path()).len(),
        1,
        "identical blobs must share one content-addressed file"
    );

    let loaded = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(loaded.len(), 2, "both commits must load");
    for vm in &loaded {
        assert_eq!(vm.blob().contents(), &shared_blob);
    }

    Ok(())
}

/// Large blobs flow through `save_batch` too: files written before the
/// transaction commits, and the batch loads back complete.
#[tokio::test]
async fn batch_with_large_blobs_roundtrips() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x8C; 32]);

    let mut commits = Vec::new();
    for i in 0..5u8 {
        // Mix of inline (32 B) and external (300 B) blobs.
        let size = if i % 2 == 0 { 32 } else { 300 };
        commits.push(seal_commit(&signer, id, CommitId::new([i; 32]), vec![i; size]).await);
    }

    let saved = Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new()).await?;
    assert_eq!(saved, 5);

    assert_eq!(
        blob_files(dir.path()).len(),
        2,
        "the two over-threshold blobs must be external"
    );

    let loaded = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(loaded.len(), 5, "all batch items must load");

    Ok(())
}

/// Cross-backend `Storage` contract: persisting any item registers its
/// sedimentree id — including across a reopen (the registration is part of
/// the same transaction as the item).
#[tokio::test]
async fn saves_register_tree_id_conformance() -> testresult::TestResult {
    use sedimentree_core::fragment::Fragment;
    use subduction_core::storage::conformance;

    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let signer = test_signer();

    let commit_tree = SedimentreeId::new([0x70; 32]);
    let commit = seal_commit(&signer, commit_tree, CommitId::new([0x10; 32]), vec![1; 16]).await;
    conformance::assert_commit_save_registers_tree_id::<Sendable, _>(&storage, commit).await;

    let fragment_tree = SedimentreeId::new([0x71; 32]);
    let fragment: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Sendable, _>(
        &signer,
        (
            fragment_tree,
            CommitId::new([0x11; 32]),
            BTreeSet::from([CommitId::new([0x12; 32])]),
            vec![CommitId::new([0x13; 32])],
        ),
        VerifiedBlobMeta::new(Blob::new(vec![2; 16])),
    )
    .await;
    conformance::assert_fragment_save_registers_tree_id::<Sendable, _>(&storage, fragment).await;

    // Registration survives reopen.
    drop(storage);
    let reopened = RedbStorage::new(dir.path())?;
    assert!(Storage::<Sendable>::contains_sedimentree_id(&reopened, commit_tree).await?);
    assert!(Storage::<Sendable>::contains_sedimentree_id(&reopened, fragment_tree).await?);

    Ok(())
}
