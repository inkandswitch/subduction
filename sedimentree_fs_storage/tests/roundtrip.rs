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
    let fragment_head = verified.payload().head();

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_fragment(&storage, id, verified).await?;

    let loaded = Storage::<Sendable>::load_fragment(&storage, id, fragment_head)
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

    let fragment_head = verified.payload().head();
    let digest_from_hash = Digest::hash(verified.payload());

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    Storage::<Sendable>::save_fragment(&storage, id, verified).await?;

    let loaded = Storage::<Sendable>::load_fragment(&storage, id, fragment_head)
        .await?
        .expect("fragment must be loadable by head CommitId");

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

/// Sedimentree IDs are sharded on disk as `trees/{first-2-bytes}/{rest}`.
/// Reopening storage must reconstruct every id exactly by concatenating the
/// bucket and leaf directory names — including ids that share a bucket prefix
/// (same first two bytes, different remainder), which exercises the two-level
/// `load_tree_ids` walk rather than a flat single-level scan.
#[tokio::test]
async fn sedimentree_ids_roundtrip_through_sharded_layout() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;

    // Construct ids that deliberately collide on the first two bytes (the
    // bucket) but differ in the remainder (the leaf), plus an id in a
    // different bucket entirely.
    let mut share_a = [0u8; 32];
    share_a[0] = 0xAB;
    share_a[1] = 0xCD;
    share_a[31] = 0x01;

    let mut share_b = [0u8; 32];
    share_b[0] = 0xAB;
    share_b[1] = 0xCD;
    share_b[31] = 0x02;

    let mut other = [0u8; 32];
    other[0] = 0x12;
    other[1] = 0x34;
    other[15] = 0xFF;

    let expected: BTreeSet<SedimentreeId> = [share_a, share_b, other]
        .into_iter()
        .map(SedimentreeId::new)
        .collect();

    {
        let storage = FsStorage::new(dir.path().to_path_buf())?;
        for id in &expected {
            Storage::<Sendable>::save_sedimentree_id(&storage, *id).await?;
        }
    }

    // Reopen: forces load_tree_ids to walk trees/{bucket}/{leaf} from disk and
    // rebuild the id set, rather than reading a warm in-memory cache.
    let reopened = FsStorage::new(dir.path().to_path_buf())?;
    let loaded = Storage::<Sendable>::load_all_sedimentree_ids(&reopened).await?;

    let loaded_set: BTreeSet<SedimentreeId> = loaded.into_iter().collect();
    assert_eq!(
        loaded_set, expected,
        "every sedimentree id must round-trip through the sharded on-disk layout"
    );

    Ok(())
}

/// A tree written under the sharded layout must be fully readable after a
/// reopen — i.e. the bucket path used for writes matches the one used for
/// reads. Guards against a bucket/leaf split mismatch between `tree_path` on
/// the write and read sides.
#[tokio::test]
async fn commit_readable_after_reopen_under_sharding() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x7E);
    let head = CommitId::new([0x99; 32]);

    {
        let storage = FsStorage::new(dir.path().to_path_buf())?;
        let blob = Blob::new(vec![7; 32]);
        let verified_blob = VerifiedBlobMeta::new(blob);
        let verified: VerifiedMeta<LooseCommit> =
            VerifiedMeta::seal::<Sendable, _>(&signer, (id, head, BTreeSet::new()), verified_blob)
                .await;
        Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
        Storage::<Sendable>::save_loose_commit(&storage, id, verified).await?;
    }

    let reopened = FsStorage::new(dir.path().to_path_buf())?;
    let loaded = Storage::<Sendable>::load_loose_commit(&reopened, id, head)
        .await?
        .expect("commit must be loadable from the sharded path after reopen");

    assert_eq!(loaded.payload().head(), head);

    Ok(())
}

/// A tree registered with NO commits or fragments must still persist across a
/// reopen. This guards the invariant the cache-gated `save_sedimentree_id`
/// optimization leans on: the *first* save must materialize the on-disk leaf
/// directory (via creating its `commits`/`fragments` children) so boot-time
/// `load_tree_ids` can rediscover the id, and a *repeated* save for an
/// already-cached id must be a harmless no-op.
#[tokio::test]
async fn empty_registered_tree_survives_reopen() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let id = make_sedimentree_id(0x5E);

    {
        let storage = FsStorage::new(dir.path().to_path_buf())?;
        // Register the id (creates dirs on the first call)...
        Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
        // ...and again (cache-gated no-op; must not error or lose the id).
        Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    }

    // Reopen: id discovery walks the on-disk leaf layout, not a warm cache.
    let reopened = FsStorage::new(dir.path().to_path_buf())?;
    let ids = Storage::<Sendable>::load_all_sedimentree_ids(&reopened).await?;

    assert!(
        ids.contains(&id),
        "an empty registered tree's id must survive reopen, but it was not rediscovered from disk"
    );

    Ok(())
}

/// Recursively collect every `.meta` file under `dir`.
fn find_meta_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(find_meta_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "meta") {
            out.push(path);
        }
    }

    out
}

/// A truncated `.meta` (e.g. left visible-but-empty by a crash before its
/// data blocks reached disk) must be *rewritten* by the next save of the
/// same content — the CAS skip validates the existing file's size instead
/// of trusting bare existence. Guards against the corruption trap where a
/// crash artifact is preserved forever because re-saves no-op and loads
/// skip-and-warn.
#[tokio::test]
async fn corrupt_meta_self_heals_on_resave() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x6C);
    let head = CommitId::new([0x77; 32]);

    let make_verified = || async {
        let blob = Blob::new(vec![3; 64]);
        let verified_blob = VerifiedBlobMeta::new(blob);
        VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
            &signer,
            (id, head, BTreeSet::new()),
            verified_blob,
        )
        .await
    };

    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;
    let original = make_verified().await;
    let expected_signed_bytes = original.signed().as_bytes().to_vec();
    Storage::<Sendable>::save_loose_commit(&storage, id, original).await?;

    // Simulate the crash artifact: the `.meta` exists but is empty.
    let metas = find_meta_files(dir.path());
    assert_eq!(metas.len(), 1, "expected exactly one .meta on disk");
    std::fs::write(&metas[0], [])?;

    // The corrupt entry is skipped on load (not returned, not an error)...
    let loaded = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert!(
        loaded.is_empty(),
        "truncated .meta must be skipped on load, got {} items",
        loaded.len()
    );

    // ...and a re-save of the same content must rewrite it, not no-op.
    Storage::<Sendable>::save_loose_commit(&storage, id, make_verified().await).await?;

    let healed = Storage::<Sendable>::load_loose_commits(&storage, id).await?;
    assert_eq!(healed.len(), 1, "commit must be loadable after re-save");
    assert_eq!(
        healed[0].signed().as_bytes(),
        &expected_signed_bytes[..],
        "healed .meta must hold the original signed bytes"
    );

    Ok(())
}

/// Find the first directory named `name` under `dir` (recursive).
fn find_dir_named(dir: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == name) {
                return Some(path);
            }
            if let Some(found) = find_dir_named(&path, name) {
                return Some(found);
            }
        }
    }

    None
}

/// Recursively collect every `.tmp` file under `dir`.
fn find_tmp_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(find_tmp_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "tmp") {
            out.push(path);
        }
    }

    out
}

/// A batch save that fails partway must not strand `.tmp` files: staged
/// writes clean their temps on drop. (The realistic trigger is ENOSPC,
/// where stranded temps would worsen the disk-full condition on every
/// retry; here the failure is forced by pre-creating a regular FILE where
/// the second commit's directory must go.)
#[tokio::test]
async fn failed_batch_leaves_no_temp_files() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();
    let id = make_sedimentree_id(0x6D);

    // Register the tree so the commits/ dir exists.
    Storage::<Sendable>::save_sedimentree_id(&storage, id).await?;

    let good_head = CommitId::new([0x01; 32]);
    let blocked_head = CommitId::new([0x02; 32]);

    // Block the second commit's id_dir with a regular file
    // (trees/{bucket}/{leaf}/commits/{commit_id_hex}).
    let commits_dir =
        find_dir_named(dir.path(), "commits").expect("commits dir must exist after registration");
    let mut blocked_hex = String::with_capacity(64);
    for byte in blocked_head.as_bytes() {
        use std::fmt::Write as _;
        let _unused = write!(blocked_hex, "{byte:02x}");
    }
    std::fs::write(commits_dir.join(blocked_hex), b"roadblock")?;

    let make = |head: CommitId| {
        let signer = &signer;
        async move {
            let verified_blob = VerifiedBlobMeta::new(Blob::new(vec![9; 32]));
            VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
                signer,
                (id, head, BTreeSet::new()),
                verified_blob,
            )
            .await
        }
    };

    let commits = vec![make(good_head).await, make(blocked_head).await];
    let result = Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new()).await;
    assert!(result.is_err(), "batch must fail on the blocked id_dir");

    assert_eq!(
        find_tmp_files(dir.path()),
        Vec::<std::path::PathBuf>::new(),
        "failed batch must not strand .tmp files"
    );

    Ok(())
}

/// Cross-backend `Storage` contract: persisting any item registers its
/// sedimentree id — including across a reopen (recovered from the on-disk
/// layout).
#[tokio::test]
async fn saves_register_tree_id_conformance() -> testresult::TestResult {
    use subduction_core::storage::conformance;

    let dir = tempfile::tempdir()?;
    let storage = FsStorage::new(dir.path().to_path_buf())?;
    let signer = test_signer();

    let commit_tree = make_sedimentree_id(0x70);
    let commit: VerifiedMeta<LooseCommit> = VerifiedMeta::seal::<Sendable, _>(
        &signer,
        (commit_tree, CommitId::new([0x10; 32]), BTreeSet::new()),
        VerifiedBlobMeta::new(Blob::new(vec![1; 16])),
    )
    .await;
    conformance::assert_commit_save_registers_tree_id::<Sendable, _>(&storage, commit).await;

    let fragment_tree = make_sedimentree_id(0x71);
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

    // Registration survives reopen (rediscovered from the directory layout).
    drop(storage);
    let reopened = FsStorage::new(dir.path().to_path_buf())?;
    assert!(Storage::<Sendable>::contains_sedimentree_id(&reopened, commit_tree).await?);
    assert!(Storage::<Sendable>::contains_sedimentree_id(&reopened, fragment_tree).await?);

    Ok(())
}
