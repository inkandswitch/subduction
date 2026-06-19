//! Failure-path tests for `RedbStorage`: corrupt stored values, damaged
//! external blob files, exclusive-lock conflicts, and failed-save
//! registration leaks.
//!
//! The filesystem backend earns its crash-consistency claims with tests
//! that simulate the exact artifacts a crash leaves behind
//! (`sedimentree_fs_storage/tests/roundtrip.rs`); this suite holds the
//! redb backend to the same standard.

#![allow(clippy::indexing_slicing)]

use std::collections::BTreeSet;

use future_form::Sendable;
use redb::TableDefinition;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use subduction_redb_storage::{BLOBS_DIR_NAME, DB_FILE_NAME, RedbStorage, RedbStorageError};

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
    walk(&root.join(BLOBS_DIR_NAME), &mut out);
    out
}

/// Malformed compound values (unknown tag; truncated inline length) are
/// skipped with a warning, not surfaced as errors — a single corrupt
/// record must not take down loads of the rest of the tree. Mirrors the
/// fs backend's skip-and-warn tolerance.
#[tokio::test]
async fn malformed_compound_values_are_skipped() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x90; 32]);
    let good_head = CommitId::new([0x01; 32]);

    {
        let storage = RedbStorage::new(dir.path())?;
        let good = seal_commit(&signer, id, good_head, vec![5; 32]).await;
        Storage::<Sendable>::save_loose_commit(&storage, id, good).await?;
    }

    // Inject garbage values directly into the commits table under the same
    // tree prefix (key = tree ++ item_id ++ digest).
    {
        let commits: TableDefinition<'_, &[u8; 96], &[u8]> = TableDefinition::new("commits");
        let db = redb::Database::open(dir.path().join(DB_FILE_NAME))?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(commits)?;

            // Unknown value tag.
            let mut bad_tag_key = [0u8; 96];
            bad_tag_key[..32].copy_from_slice(id.as_bytes());
            bad_tag_key[32..64].copy_from_slice(&[0x02; 32]);
            table.insert(&bad_tag_key, [0xFFu8, 1, 2, 3].as_slice())?;

            // Inline tag whose meta_len overruns the value.
            let mut short_key = [0u8; 96];
            short_key[..32].copy_from_slice(id.as_bytes());
            short_key[32..64].copy_from_slice(&[0x03; 32]);
            let mut truncated = vec![0x00u8]; // TAG_INLINE
            truncated.extend_from_slice(&1_000u32.to_be_bytes()); // claims 1000 B
            truncated.extend_from_slice(&[0xAB; 10]); // ...holds 10
            table.insert(&short_key, truncated.as_slice())?;
        }
        txn.commit()?;
    }

    let storage = RedbStorage::new(dir.path())?;

    // Bulk load: exactly the good commit survives, no error.
    let loaded = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(loaded.len(), 1, "only the intact record must load");
    assert_eq!(loaded[0].payload().head(), good_head);

    // Point reads of the corrupt ids: None, not an error.
    assert!(
        Storage::<Sendable>::load_loose_commit(&storage, id, CommitId::new([0x02; 32]))
            .await?
            .is_none(),
        "a corrupt record must point-read as absent"
    );
    assert!(
        Storage::<Sendable>::load_loose_commit(&storage, id, CommitId::new([0x03; 32]))
            .await?
            .is_none(),
        "a truncated record must point-read as absent"
    );

    Ok(())
}

/// A missing external blob file (e.g. lost to a partial restore) is
/// skipped on load and healed by a re-save of the same content: the
/// stage-blobs CAS writes the file when it is absent even though the
/// B+tree record already exists.
#[tokio::test]
async fn missing_external_blob_skipped_then_healed_on_resave() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x91; 32]);
    let head = CommitId::new([0x10; 32]);
    let blob = vec![0xCD; 300];

    let make = || seal_commit(&signer, id, head, blob.clone());

    Storage::<Sendable>::save_loose_commit(&storage, id, make().await).await?;

    let files = blob_files(dir.path());
    assert_eq!(files.len(), 1, "expected exactly one external blob file");
    std::fs::remove_file(&files[0])?;

    // The orphaned record is skipped on load (bulk and point), not an error.
    assert!(
        Storage::<Sendable>::load_loose_commits(&storage, id)
            .await?
            .is_empty(),
        "a record whose external blob is missing must be skipped on load"
    );
    assert!(
        Storage::<Sendable>::load_loose_commit(&storage, id, head)
            .await?
            .is_none(),
        "a record whose external blob is missing must point-read as absent"
    );
    // Item-set parity: the metadata-only load must skip it too (its external
    // `stat` finds the file absent), so hydration sees the same items.
    assert!(
        Storage::<Sendable>::load_loose_commit_metas(&storage, id)
            .await?
            .is_empty(),
        "metadata-only load must skip a record whose external blob is missing"
    );

    // Re-saving the same content restores the file...
    Storage::<Sendable>::save_loose_commit(&storage, id, make().await).await?;

    let healed = Storage::<Sendable>::load_loose_commit(&storage, id, head)
        .await?
        .ok_or("commit must be loadable after re-save")?;
    assert_eq!(
        healed.blob().contents(),
        &blob,
        "healed external blob must hold the original bytes"
    );

    Ok(())
}

/// A crash-truncated external blob file is skipped on load (the blob no
/// longer matches the signed `BlobMeta`) and rewritten by a re-save: the
/// CAS check validates the file's *size*, not bare existence — the same
/// lesson as the fs backend's pair-validating CAS.
#[tokio::test]
async fn truncated_external_blob_skipped_then_healed_on_resave() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x92; 32]);
    let head = CommitId::new([0x11; 32]);
    let blob = vec![0xEF; 300];

    let make = || seal_commit(&signer, id, head, blob.clone());

    Storage::<Sendable>::save_loose_commit(&storage, id, make().await).await?;

    // Simulate the crash artifact: the file exists but holds half the bytes.
    let files = blob_files(dir.path());
    assert_eq!(files.len(), 1, "expected exactly one external blob file");
    std::fs::write(&files[0], &blob[..150])?;

    assert!(
        Storage::<Sendable>::load_loose_commits(&storage, id)
            .await?
            .is_empty(),
        "a record whose external blob is truncated must be skipped on load"
    );
    // Item-set parity: the metadata-only load `stat`s the file, sees the size
    // mismatch, and skips it too — same items as the full load.
    assert!(
        Storage::<Sendable>::load_loose_commit_metas(&storage, id)
            .await?
            .is_empty(),
        "metadata-only load must skip a record whose external blob is size-mismatched"
    );

    // Re-save: the size-mismatch CAS rewrites the file instead of skipping.
    Storage::<Sendable>::save_loose_commit(&storage, id, make().await).await?;

    let healed = Storage::<Sendable>::load_loose_commit(&storage, id, head)
        .await?
        .ok_or("commit must be loadable after re-save")?;
    assert_eq!(
        healed.blob().contents(),
        &blob,
        "healed external blob must hold the original bytes"
    );

    Ok(())
}

/// redb takes an exclusive process lock on the database file: opening a
/// second `RedbStorage` over a root with a live instance fails with a
/// database error instead of corrupting anything. (Deliberate divergence
/// from `FsStorage`, which tolerates multiple instances over one root.)
#[tokio::test]
async fn second_live_instance_is_rejected() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let first = RedbStorage::new(dir.path())?;

    let second = RedbStorage::new(dir.path());
    assert!(
        matches!(second, Err(RedbStorageError::Database(_))),
        "a second live instance must be rejected with a database error, got {second:?}"
    );

    // The first instance keeps working.
    let signer = test_signer();
    let id = SedimentreeId::new([0x93; 32]);
    let commit = seal_commit(&signer, id, CommitId::new([0x01; 32]), vec![1; 8]).await;
    Storage::<Sendable>::save_loose_commit(&first, id, commit).await?;

    // ...and once it drops, the root opens fine again.
    drop(first);
    let reopened = RedbStorage::new(dir.path())?;
    assert!(Storage::<Sendable>::contains_sedimentree_id(&reopened, id).await?);

    Ok(())
}

/// Negative registration contract: a save that fails before its
/// transaction commits must not register the tree id. Failure is induced
/// by replacing the `blobs/` directory with a regular file, so staging an
/// external blob fails before `begin_write()`.
#[tokio::test]
async fn failed_external_blob_save_does_not_register_tree_id() -> testresult::TestResult {
    use subduction_core::storage::conformance;

    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let signer = test_signer();
    let id = SedimentreeId::new([0x94; 32]);

    // Poison the blob store: a regular file where the bucket dirs must go.
    let blobs_dir = dir.path().join(BLOBS_DIR_NAME);
    std::fs::remove_dir_all(&blobs_dir)?;
    std::fs::write(&blobs_dir, b"roadblock")?;

    // Over-threshold blob ⇒ the save must stage an external file ⇒ fails.
    let commit = seal_commit(&signer, id, CommitId::new([0x01; 32]), vec![7; 300]).await;
    conformance::assert_failed_commit_save_does_not_register_tree_id::<Sendable, _>(
        &storage, commit,
    )
    .await;

    Ok(())
}
