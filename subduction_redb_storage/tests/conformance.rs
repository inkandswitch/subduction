//! Certify the redb backend against the storage conformance suite, in
//! both future forms.

use std::rc::Rc;

use future_form::{Local, Sendable};
use subduction_redb_storage::storage::RedbStorage;
use subduction_runtime::conformance;
use testresult::TestResult;

#[tokio::test(flavor = "multi_thread")]
async fn redb_conforms_sendable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::open(dir.path().join("conformance.redb"))?;
    conformance::certify::<Sendable, _>(&storage).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn redb_conforms_local() -> TestResult {
    let dir = tempfile::tempdir()?;
    let storage = Rc::new(RedbStorage::open(dir.path().join("conformance.redb"))?);
    conformance::certify::<Local, _>(&storage).await?;
    Ok(())
}

/// Durability across reopen: what one handle persists, a fresh handle
/// on the same file sees (the property `MemoryStorage` cannot have).
#[tokio::test(flavor = "multi_thread")]
async fn persists_across_reopen() -> TestResult {
    use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
    use subduction_runtime::storage::Storage;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("reopen.redb");
    let tree = SedimentreeId::new([7u8; 32]);

    let (signed, blob) = test_commit(tree, 0xA1);
    {
        let storage = RedbStorage::open(&path)?;
        let stored = Storage::<Sendable>::persist_items(
            &storage,
            tree,
            vec![(signed.clone(), blob.clone())],
            vec![],
        )
        .await
        .map_err(|e| format!("persist: {e:?}"))?;
        assert_eq!(stored, 1);
    }

    let reopened = RedbStorage::open(&path)?;
    let items = Storage::<Sendable>::fetch_items(
        &reopened,
        tree,
        vec![CommitId::new([0xA1; 32])],
        vec![],
    )
    .await
    .map_err(|e| format!("fetch: {e:?}"))?
    .ok_or("tree unknown after reopen")?;
    let (fetched, fetched_blob) = items.commits.first().ok_or("commit missing after reopen")?;
    assert_eq!(fetched.as_bytes(), signed.as_bytes());
    assert_eq!(fetched_blob, &blob);
    Ok(())
}

fn test_commit(
    tree: sedimentree_core::id::SedimentreeId,
    head: u8,
) -> (
    subduction_crypto::signed::Signed<sedimentree_core::loose_commit::LooseCommit>,
    Vec<u8>,
) {
    use std::collections::BTreeSet;

    use ed25519_dalek::SigningKey;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        loose_commit::{LooseCommit, id::CommitId},
    };
    use subduction_crypto::signed::Signed;

    let signing_key = SigningKey::from_bytes(&[9u8; 32]);
    let blob = Blob::new(vec![head; 16]);
    let commit = LooseCommit::new(
        tree,
        CommitId::new([head; 32]),
        BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    (
        Signed::seal_sync(&signing_key, commit).into_signed(),
        blob.as_slice().to_vec(),
    )
}
