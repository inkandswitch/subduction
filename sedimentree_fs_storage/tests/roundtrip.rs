//! Save/load roundtrip tests for `FsStorage`.
//!
//! These catch storage corruption bugs where a `Signed<T>` written to disk
//! cannot be decoded on reload (e.g., truncation from `fields_size()`
//! disagreeing with actual encoded byte count).
#![allow(clippy::indexing_slicing)]
#![allow(clippy::expect_used, clippy::missing_const_for_fn)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

fn test_signer() -> MemorySigner {
    MemorySigner::from_bytes(&[42u8; 32])
}

fn make_sedimentree_id(seed: u8) -> SedimentreeId {
    SedimentreeId::new([seed; 32])
}

/// Save a `LooseCommit` via `FsStorage`, reload via `load_loose_commits`, and verify byte identity.
#[tokio::test]
async fn save_load_loose_commit_roundtrip() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x01);

    let head = CommitId::new([0x42; 32]);
    let blob = Blob::new(vec![1, 2, 3, 4, 5]);
    let verified_blob = VerifiedBlobMeta::new(blob);
    let verified: VerifiedMeta<LooseCommit> =
        VerifiedMeta::seal::<Sendable, _>(&signer, (id, head, BTreeSet::new()), verified_blob)
            .await;

    let original_signed_bytes = verified.signed().as_bytes().to_vec();
    let original_blob_bytes = verified.blob().contents().clone();

    // Save
    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;

    // Reload via bulk load
    let loaded_all = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(loaded_all.len(), 1, "expected exactly one commit");
    let loaded = &loaded_all[0];

    // Verify byte identity
    assert_eq!(
        loaded.signed().as_bytes(),
        &original_signed_bytes[..],
        "signed bytes must survive save/load roundtrip"
    );
    assert_eq!(
        loaded.blob().contents(),
        &original_blob_bytes,
        "blob bytes must survive save/load roundtrip"
    );
    assert_eq!(
        loaded.payload(),
        &LooseCommit::new(
            id,
            head,
            BTreeSet::new(),
            sedimentree_core::blob::BlobMeta::new(&Blob::new(vec![1, 2, 3, 4, 5])),
        ),
        "decoded payload must match original"
    );

    Ok(())
}

/// Save a `LooseCommit` with parents, reload, verify parents are preserved.
#[tokio::test]
async fn save_load_loose_commit_with_parents_roundtrip() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x02);

    let head = CommitId::new([0x50; 32]);
    let parents = BTreeSet::from([
        CommitId::new([0x10; 32]),
        CommitId::new([0x20; 32]),
        CommitId::new([0x30; 32]),
    ]);

    let blob = Blob::new(vec![10; 128]);
    let verified_blob = VerifiedBlobMeta::new(blob);
    let verified: VerifiedMeta<LooseCommit> =
        VerifiedMeta::seal::<Sendable, _>(&signer, (id, head, parents.clone()), verified_blob)
            .await;

    let original_signed_bytes = verified.signed().as_bytes().to_vec();

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;

    let loaded_all = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(loaded_all.len(), 1, "expected exactly one commit");
    let loaded = &loaded_all[0];

    assert_eq!(
        loaded.signed().as_bytes(),
        &original_signed_bytes[..],
        "signed bytes must survive roundtrip with parents"
    );
    assert_eq!(
        loaded.payload().parents(),
        &parents,
        "parents must be preserved through save/load"
    );

    Ok(())
}

/// Save a `Fragment` via `FsStorage`, reload it, and verify byte identity.
#[tokio::test]
async fn save_load_fragment_roundtrip() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x03);

    let head = CommitId::new([0x01; 32]);
    let boundary = BTreeSet::from([CommitId::new([0x02; 32])]);
    let checkpoints = vec![CommitId::new([0x03; 32])];

    let blob = Blob::new(vec![42; 256]);
    let verified_blob = VerifiedBlobMeta::new(blob);
    let verified: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Sendable, _>(
        &signer,
        (id, head, boundary, checkpoints),
        verified_blob,
    )
    .await;

    let original_signed_bytes = verified.signed().as_bytes().to_vec();
    let original_blob_bytes = verified.blob().contents().clone();
    let fragment_id = verified.payload().fragment_id();

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_fragment(&storage, id, verified).await?;

    let loaded = Storage::<Sendable>::load_fragment(&storage, id, fragment_id)
        .await?
        .expect("fragment should exist after save");

    assert_eq!(
        loaded.signed().as_bytes(),
        &original_signed_bytes[..],
        "signed bytes must survive save/load roundtrip"
    );
    assert_eq!(
        loaded.blob().contents(),
        &original_blob_bytes,
        "blob bytes must survive save/load roundtrip"
    );

    Ok(())
}

/// Verify that `Digest::hash` on a fragment matches the storage key.
#[tokio::test]
async fn fragment_digest_matches_storage_key() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x05);

    let head = CommitId::new([0xAA; 32]);
    let boundary = BTreeSet::from([CommitId::new([0xBB; 32]), CommitId::new([0xCC; 32])]);
    let checkpoints = vec![CommitId::new([0xDD; 32])];

    let blob = Blob::new(vec![99; 100]);
    let verified_blob = VerifiedBlobMeta::new(blob);
    let verified: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Sendable, _>(
        &signer,
        (id, head, boundary, checkpoints),
        verified_blob,
    )
    .await;

    let fragment_id = verified.payload().fragment_id();
    let digest_from_hash = Digest::hash(verified.payload());

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_fragment(&storage, id, verified).await?;

    let loaded = Storage::<Sendable>::load_fragment(&storage, id, fragment_id)
        .await?
        .expect("fragment must be loadable by FragmentId");

    assert_eq!(
        Digest::hash(loaded.payload()),
        digest_from_hash,
        "loaded fragment's digest must match"
    );

    Ok(())
}

/// Save multiple commits, `load_loose_commits` returns all of them.
#[tokio::test]
async fn save_load_multiple_commits_roundtrip() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x04);

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;

    let mut expected_ids = BTreeSet::new();

    for i in 0..5u8 {
        let head = CommitId::new({
            let mut bytes = [0u8; 32];
            bytes[0] = i;
            bytes
        });
        let blob = Blob::new(vec![i; 64]);
        let verified_blob = VerifiedBlobMeta::new(blob);
        let verified: VerifiedMeta<LooseCommit> =
            VerifiedMeta::seal::<Sendable, _>(&signer, (id, head, BTreeSet::new()), verified_blob)
                .await;
        expected_ids.insert(verified.payload().head());
        if let Err(e) = Storage::<Sendable>::save_loose_commit(&storage, id, verified).await {
            eprintln!("save_loose_commit failed: {e:?}");
            return Err(e.into());
        }
    }

    let loaded = Storage::<Sendable>::load_loose_commits(&storage, id).await?;

    let loaded_ids: BTreeSet<_> = loaded.iter().map(|v| v.payload().head()).collect();

    assert_eq!(
        loaded_ids, expected_ids,
        "all saved commits must be loadable"
    );

    Ok(())
}
