//! Storage conformance suite: the certification bar for backends.
//!
//! Every [`Storage`] implementation must pass [`certify`] before a
//! driver ships on it. Platform crates run it against their adapter in a
//! test; [`crate::memory::MemoryStorage`] is the reference backend.
//!
//! # Layer boundaries
//!
//! This suite certifies the _custody + durability_ contract only.
//! Adversarial wire data (forged signatures, blob/digest mismatches)
//! never reaches a backend — the connection machine rejects it inline
//! (covered by `subduction_protocol`'s tests), and policy denial happens
//! in the driver's effect executor before the backend is called
//! (covered by the runtime's driver tests). A backend that "helpfully"
//! re-verifies is wasting work; one that persists whatever it is handed
//! is correct.
//!
//! # Usage
//!
//! ```ignore
//! #[test]
//! fn my_backend_conforms() {
//!     let storage = MyBackend::new();
//!     futures::executor::block_on(
//!         subduction_runtime::conformance::certify::<Local, _>(&storage),
//!     )
//!     .expect("conformance");
//! }
//! ```

use std::collections::BTreeSet;

use ed25519_dalek::SigningKey;
use future_form::FutureForm;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::signed::Signed;
use thiserror::Error;

use crate::storage::Storage;

/// A conformance violation: which check failed and how.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("storage conformance violation in `{check}`: {detail}")]
pub struct Violation {
    check: &'static str,
    detail: String,
}

impl Violation {
    fn new(check: &'static str, detail: impl Into<String>) -> Self {
        Self {
            check,
            detail: detail.into(),
        }
    }
}

/// Run every conformance check against `storage`.
///
/// Checks use disjoint tree ids, so one (possibly shared or persistent)
/// backend instance may be certified in a single call.
///
/// # Errors
///
/// Returns the first [`Violation`] encountered.
pub async fn certify<Async: FutureForm, S: Storage<Async>>(storage: &S) -> Result<(), Violation> {
    round_trip(storage).await?;
    unknown_tree_is_none(storage).await?;
    missing_items_are_absent(storage).await?;
    re_persist_is_idempotent(storage).await?;
    trees_are_isolated(storage).await?;
    delete_removes_the_tree(storage).await?;
    Ok(())
}

/// A signed commit + blob for tree `tree` with `head` repeated.
fn test_commit(tree: SedimentreeId, head: u8) -> (Signed<LooseCommit>, Vec<u8>) {
    let signing_key = SigningKey::from_bytes(&[0x5C; 32]);
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

/// A signed fragment + blob for tree `tree` with `head` repeated.
fn test_fragment(tree: SedimentreeId, head: u8) -> (Signed<Fragment>, Vec<u8>) {
    let signing_key = SigningKey::from_bytes(&[0x5C; 32]);
    let blob = Blob::new(vec![head; 24]);
    let fragment = Fragment::new(
        tree,
        CommitId::new([head; 32]),
        BTreeSet::new(),
        &[],
        BlobMeta::new(&blob),
    );
    (
        Signed::seal_sync(&signing_key, fragment).into_signed(),
        blob.as_slice().to_vec(),
    )
}

const fn tree(tag: u8) -> SedimentreeId {
    SedimentreeId::new([tag; 32])
}

/// Persisted items come back with byte-identical signed bytes and blobs.
async fn round_trip<Async: FutureForm, S: Storage<Async>>(storage: &S) -> Result<(), Violation> {
    const CHECK: &str = "round_trip";
    let t = tree(0xC1);
    let (commit_a, blob_a) = test_commit(t, 1);
    let (commit_b, blob_b) = test_commit(t, 2);
    let (fragment, blob_f) = test_fragment(t, 3);

    let stored = storage
        .persist_items(
            t,
            vec![
                (commit_a.clone(), blob_a.clone()),
                (commit_b.clone(), blob_b.clone()),
            ],
            vec![(fragment.clone(), blob_f.clone())],
        )
        .await
        .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;
    if stored != 3 {
        return Err(Violation::new(
            CHECK,
            format!("stored count {stored}, want 3"),
        ));
    }

    let items = storage
        .fetch_items(
            t,
            vec![CommitId::new([1; 32]), CommitId::new([2; 32])],
            vec![CommitId::new([3; 32])],
        )
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .ok_or_else(|| Violation::new(CHECK, "tree unknown after persist"))?;

    if items.commits.len() != 2 || items.fragments.len() != 1 {
        return Err(Violation::new(
            CHECK,
            format!(
                "fetched {} commits / {} fragments, want 2 / 1",
                items.commits.len(),
                items.fragments.len()
            ),
        ));
    }
    for (signed, blob) in &items.commits {
        let expected = if signed.as_bytes() == commit_a.as_bytes() {
            &blob_a
        } else if signed.as_bytes() == commit_b.as_bytes() {
            &blob_b
        } else {
            return Err(Violation::new(CHECK, "fetched commit bytes differ"));
        };
        if blob != expected {
            return Err(Violation::new(CHECK, "fetched commit blob differs"));
        }
    }
    let Some((fetched_fragment, fetched_blob)) = items.fragments.first() else {
        return Err(Violation::new(CHECK, "fragment missing"));
    };
    if fetched_fragment.as_bytes() != fragment.as_bytes() || fetched_blob != &blob_f {
        return Err(Violation::new(CHECK, "fetched fragment bytes differ"));
    }
    Ok(())
}

/// Fetching a never-written tree answers `None`, not an empty result.
async fn unknown_tree_is_none<Async: FutureForm, S: Storage<Async>>(
    storage: &S,
) -> Result<(), Violation> {
    const CHECK: &str = "unknown_tree_is_none";
    let found = storage
        .fetch_items(tree(0xC2), vec![CommitId::new([1; 32])], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?;
    if found.is_some() {
        return Err(Violation::new(CHECK, "unknown tree answered Some"));
    }
    Ok(())
}

/// Items that were never stored are absent from the result — not an
/// error, and never fabricated.
async fn missing_items_are_absent<Async: FutureForm, S: Storage<Async>>(
    storage: &S,
) -> Result<(), Violation> {
    const CHECK: &str = "missing_items_are_absent";
    let t = tree(0xC3);
    let (commit, blob) = test_commit(t, 1);
    let _stored = storage
        .persist_items(t, vec![(commit, blob)], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;

    let items = storage
        .fetch_items(
            t,
            vec![CommitId::new([1; 32]), CommitId::new([0xEE; 32])],
            vec![CommitId::new([0xEF; 32])],
        )
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .ok_or_else(|| Violation::new(CHECK, "tree unknown after persist"))?;
    if items.commits.len() != 1 || !items.fragments.is_empty() {
        return Err(Violation::new(
            CHECK,
            format!(
                "fetched {} commits / {} fragments, want 1 / 0",
                items.commits.len(),
                items.fragments.len()
            ),
        ));
    }
    Ok(())
}

/// Re-persisting identical content is not an error and does not
/// duplicate items.
async fn re_persist_is_idempotent<Async: FutureForm, S: Storage<Async>>(
    storage: &S,
) -> Result<(), Violation> {
    const CHECK: &str = "re_persist_is_idempotent";
    let t = tree(0xC4);
    let (commit, blob) = test_commit(t, 1);

    for _ in 0..2 {
        let _stored = storage
            .persist_items(t, vec![(commit.clone(), blob.clone())], vec![])
            .await
            .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;
    }
    let items = storage
        .fetch_items(t, vec![CommitId::new([1; 32])], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .ok_or_else(|| Violation::new(CHECK, "tree unknown after persist"))?;
    if items.commits.len() != 1 {
        return Err(Violation::new(
            CHECK,
            format!("{} copies after re-persist, want 1", items.commits.len()),
        ));
    }
    Ok(())
}

/// Writes to one tree are invisible to another.
async fn trees_are_isolated<Async: FutureForm, S: Storage<Async>>(
    storage: &S,
) -> Result<(), Violation> {
    const CHECK: &str = "trees_are_isolated";
    let (t1, t2) = (tree(0xC5), tree(0xC6));
    let (commit, blob) = test_commit(t1, 1);
    let _stored = storage
        .persist_items(t1, vec![(commit, blob)], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;

    if storage
        .fetch_items(t2, vec![CommitId::new([1; 32])], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .is_some()
    {
        return Err(Violation::new(CHECK, "t2 visible after writing only t1"));
    }

    let (other, other_blob) = test_commit(t2, 9);
    let _stored = storage
        .persist_items(t2, vec![(other, other_blob)], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;
    let items = storage
        .fetch_items(
            t2,
            vec![CommitId::new([1; 32]), CommitId::new([9; 32])],
            vec![],
        )
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .ok_or_else(|| Violation::new(CHECK, "t2 unknown after persist"))?;
    if items.commits.len() != 1 {
        return Err(Violation::new(CHECK, "t1's items leaked into t2"));
    }
    Ok(())
}

/// Deleting a tree removes it entirely; deleting an unknown tree is a
/// no-op, not an error.
async fn delete_removes_the_tree<Async: FutureForm, S: Storage<Async>>(
    storage: &S,
) -> Result<(), Violation> {
    const CHECK: &str = "delete_removes_the_tree";
    let t = tree(0xC7);
    let (commit, blob) = test_commit(t, 1);
    let _stored = storage
        .persist_items(t, vec![(commit, blob)], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("persist failed: {e:?}")))?;

    storage
        .delete_tree(t)
        .await
        .map_err(|e| Violation::new(CHECK, format!("delete failed: {e:?}")))?;
    if storage
        .fetch_items(t, vec![CommitId::new([1; 32])], vec![])
        .await
        .map_err(|e| Violation::new(CHECK, format!("fetch failed: {e:?}")))?
        .is_some()
    {
        return Err(Violation::new(CHECK, "tree still visible after delete"));
    }

    storage
        .delete_tree(tree(0xC8))
        .await
        .map_err(|e| Violation::new(CHECK, format!("delete of unknown tree errored: {e:?}")))?;
    Ok(())
}
