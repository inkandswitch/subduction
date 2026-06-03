//! Tests for [`MetricsStorage`], the metrics-recording storage wrapper.
//!
//! These tests verify that each [`Storage`] method on `MetricsStorage`
//! actually delegates to the inner storage, not just records timing
//! metrics. They guard against `cargo mutants` survivors that replace
//! wrapper bodies with `Future::from(Ok(()))`, `Future::from(Ok(vec![...]))`,
//! and similar fabricated-success returns.
//!
//! The pattern is identical for every test: wrap a [`MemoryStorage`] in
//! [`MetricsStorage`], invoke a method through the wrapper, then read
//! through the inner storage to confirm the operation actually happened.
//! A wholesale wrapper-body mutation breaks the post-condition because
//! the inner storage is unchanged (for writes) or returns its true state
//! instead of the fabricated value (for reads).
//!
//! Feature-gated on `metrics`; runs under `cargo test --all-features`
//! and (with `--features metrics`) under `cargo mutants`.

#![cfg(feature = "metrics")]

use std::collections::BTreeSet;

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::test_utils::test_signer,
    storage::{
        memory::MemoryStorage,
        metrics::{MetricsStorage, RefreshMetrics},
        traits::Storage,
    },
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use testresult::TestResult;

// ============================================================================
// Helpers
// ============================================================================

/// Build a verified loose commit for the given sedimentree from a unique
/// data seed. Each call with a different `data` produces a distinct
/// commit identity (so callers can distinguish them after a round-trip).
async fn make_verified_commit(
    sed_id: SedimentreeId,
    data: &[u8],
) -> Result<VerifiedMeta<LooseCommit>, Box<dyn std::error::Error>> {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    #[allow(clippy::indexing_slicing)]
    let head = CommitId::new({
        let mut bytes = [0u8; 32];
        let n = data.len().min(32);
        bytes[..n].copy_from_slice(&data[..n]);
        bytes
    });
    let commit = LooseCommit::new(sed_id, head, BTreeSet::new(), blob_meta);
    let sealed = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    let verified_sig = sealed.into_signed().try_verify()?;
    Ok(VerifiedMeta::new(verified_sig, blob)?)
}

/// Build a verified fragment for the given sedimentree from a unique
/// data seed. See [`make_verified_commit`] for the identity convention.
async fn make_verified_fragment(
    sed_id: SedimentreeId,
    head_byte: u8,
    data: &[u8],
) -> Result<VerifiedMeta<Fragment>, Box<dyn std::error::Error>> {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new([head_byte; 32]);
    let fragment = Fragment::new(sed_id, head, BTreeSet::new(), &[], blob_meta);
    let sealed = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    let verified_sig = sealed.into_signed().try_verify()?;
    Ok(VerifiedMeta::new(verified_sig, blob)?)
}

// ============================================================================
// Sedimentree-ID methods
// ============================================================================

/// `save_sedimentree_id` must register the ID with the inner storage.
///
/// Catches `cargo mutants` survivors on `storage/metrics.rs:113-126` such
/// as `Future::from(Ok(()))` — the wrapper returns success without
/// invoking the inner store, so the ID never lands.
#[tokio::test]
async fn save_sedimentree_id_delegates_to_inner() -> TestResult {
    let inner = MemoryStorage::new();
    let ms = MetricsStorage::new(inner);
    let id = SedimentreeId::new([1u8; 32]);

    Storage::<Sendable>::save_sedimentree_id(&ms, id).await?;

    let ids = Storage::<Sendable>::load_all_sedimentree_ids(ms.inner()).await?;
    assert!(
        ids.contains(&id),
        "inner storage should contain the saved id, got {ids:?}"
    );
    Ok(())
}

/// `delete_sedimentree_id` must remove the ID from the inner storage.
///
/// Catches mutants on `storage/metrics.rs:128-141` (`Future::from(Ok(()))`
/// would leave the id in place).
#[tokio::test]
async fn delete_sedimentree_id_delegates_to_inner() -> TestResult {
    let inner = MemoryStorage::new();
    let id = SedimentreeId::new([2u8; 32]);
    Storage::<Sendable>::save_sedimentree_id(&inner, id).await?;

    let ms = MetricsStorage::new(inner);
    Storage::<Sendable>::delete_sedimentree_id(&ms, id).await?;

    let ids = Storage::<Sendable>::load_all_sedimentree_ids(ms.inner()).await?;
    assert!(
        !ids.contains(&id),
        "inner storage should no longer contain the deleted id, got {ids:?}"
    );
    Ok(())
}

/// `load_all_sedimentree_ids` must reflect the inner storage's actual
/// state, not a fabricated `Set::new()` or `Set::from_iter([Default])`.
///
/// Catches mutants on `storage/metrics.rs:143-155`. Asserts on both
/// length and membership so `Set::from_iter([Default::default()])`
/// (a single zero-valued id) is also caught — we use two non-zero ids.
#[tokio::test]
async fn load_all_sedimentree_ids_reflects_inner_state() -> TestResult {
    let inner = MemoryStorage::new();
    let id1 = SedimentreeId::new([3u8; 32]);
    let id2 = SedimentreeId::new([4u8; 32]);
    Storage::<Sendable>::save_sedimentree_id(&inner, id1).await?;
    Storage::<Sendable>::save_sedimentree_id(&inner, id2).await?;

    let ms = MetricsStorage::new(inner);
    let loaded = Storage::<Sendable>::load_all_sedimentree_ids(&ms).await?;

    let expected: Set<SedimentreeId> = [id1, id2].into_iter().collect();
    assert_eq!(
        loaded, expected,
        "wrapper should return exactly the inner's id set"
    );
    Ok(())
}

// ============================================================================
// Fragment methods
// ============================================================================

/// `load_fragments` must return whatever the inner storage holds, not a
/// fabricated empty or default-valued vec.
///
/// Catches mutants on `storage/metrics.rs:292-302`. Asserts on payload
/// identity (the fragment's head) so the `vec![VerifiedMeta::new(Default)]`
/// variants are detected — `Default::default()` would not match our
/// non-zero head byte.
#[tokio::test]
async fn load_fragments_returns_inner_data() -> TestResult {
    let inner = MemoryStorage::new();
    let sed_id = SedimentreeId::new([5u8; 32]);
    let expected_head = CommitId::new([0xAB; 32]);
    let verified = make_verified_fragment(sed_id, 0xAB, b"load_fragments_payload").await?;
    Storage::<Sendable>::save_fragment(&inner, sed_id, verified).await?;

    let ms = MetricsStorage::new(inner);
    let loaded = Storage::<Sendable>::load_fragments(&ms, sed_id).await?;

    let heads: Vec<CommitId> = loaded.iter().map(|v| v.payload().head()).collect();
    assert_eq!(
        heads,
        vec![expected_head],
        "loaded fragments should be exactly the one we stored"
    );
    Ok(())
}

/// `delete_fragment` must remove the fragment from the inner storage.
///
/// Catches mutants on `storage/metrics.rs:304-318`.
#[tokio::test]
async fn delete_fragment_removes_from_inner() -> TestResult {
    let inner = MemoryStorage::new();
    let sed_id = SedimentreeId::new([6u8; 32]);
    let head_byte = 0xCD;
    let head = CommitId::new([head_byte; 32]);
    let verified = make_verified_fragment(sed_id, head_byte, b"delete_one").await?;
    Storage::<Sendable>::save_fragment(&inner, sed_id, verified).await?;

    let ms = MetricsStorage::new(inner);
    Storage::<Sendable>::delete_fragment(&ms, sed_id, head).await?;

    let loaded = Storage::<Sendable>::load_fragment(ms.inner(), sed_id, head).await?;
    assert!(
        loaded.is_none(),
        "fragment should be gone from inner storage after delete"
    );
    Ok(())
}

/// `delete_fragments` must remove all fragments for the sedimentree from
/// the inner storage.
///
/// Catches mutants on `storage/metrics.rs:320-330`. Seeds two fragments
/// so a partial mutation (deleting one of two by accident) would also be
/// caught.
#[tokio::test]
async fn delete_fragments_clears_all_for_sedimentree() -> TestResult {
    let inner = MemoryStorage::new();
    let sed_id = SedimentreeId::new([7u8; 32]);
    let f1 = make_verified_fragment(sed_id, 0x10, b"frag-one").await?;
    let f2 = make_verified_fragment(sed_id, 0x20, b"frag-two").await?;
    Storage::<Sendable>::save_fragment(&inner, sed_id, f1).await?;
    Storage::<Sendable>::save_fragment(&inner, sed_id, f2).await?;

    let ms = MetricsStorage::new(inner);
    Storage::<Sendable>::delete_fragments(&ms, sed_id).await?;

    let remaining = Storage::<Sendable>::load_fragments(ms.inner(), sed_id).await?;
    assert!(
        remaining.is_empty(),
        "all fragments for the sedimentree should be gone, got {} remaining",
        remaining.len()
    );
    Ok(())
}

// ============================================================================
// Batch operations
// ============================================================================

/// `save_batch` must write all commits and fragments through to the
/// inner storage AND register the sedimentree id (per the trait
/// contract).
///
/// Catches mutants on `storage/metrics.rs:334-349`. The most pernicious
/// surviving variants are `Future::from(Ok(0))` and `Future::from(Ok(1))`
/// — the wrapper returns a count without persisting anything. We assert
/// on both the returned count AND the post-state of inner storage so
/// either bug is detected.
#[tokio::test]
async fn save_batch_writes_through_to_inner() -> TestResult {
    let inner = MemoryStorage::new();
    let sed_id = SedimentreeId::new([8u8; 32]);

    let commit = make_verified_commit(sed_id, b"batch-commit").await?;
    let commit_head = commit.payload().head();
    let fragment = make_verified_fragment(sed_id, 0xEF, b"batch-fragment").await?;
    let fragment_head = fragment.payload().head();

    let ms = MetricsStorage::new(inner);
    let count = Storage::<Sendable>::save_batch(&ms, sed_id, vec![commit], vec![fragment]).await?;

    assert_eq!(count, 2, "save_batch should report 2 items written");

    // ID must be registered (trait contract).
    let ids = Storage::<Sendable>::load_all_sedimentree_ids(ms.inner()).await?;
    assert!(
        ids.contains(&sed_id),
        "save_batch should register the sedimentree id"
    );

    // Commit must be persisted under its head.
    let stored_commit =
        Storage::<Sendable>::load_loose_commit(ms.inner(), sed_id, commit_head).await?;
    assert!(
        stored_commit.is_some(),
        "save_batch should have persisted the commit"
    );

    // Fragment must be persisted under its head.
    let stored_fragment =
        Storage::<Sendable>::load_fragment(ms.inner(), sed_id, fragment_head).await?;
    assert!(
        stored_fragment.is_some(),
        "save_batch should have persisted the fragment"
    );
    Ok(())
}

// ============================================================================
// refresh_metrics scaling guarantee
// ============================================================================

/// A storage wrapper that delegates everything to [`MemoryStorage`] except
/// the per-tree listing calls, which `panic!`. It lets a test assert that a
/// code path does *not* enumerate per-tree contents.
#[derive(Clone)]
struct ListPanicsStorage {
    inner: MemoryStorage,
}

impl ListPanicsStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
        }
    }
}

impl Storage<Sendable> for ListPanicsStorage {
    type Error = <MemoryStorage as Storage<Sendable>>::Error;

    fn save_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::save_sedimentree_id(&self.inner, id)
    }

    fn delete_sedimentree_id(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_sedimentree_id(&self.inner, id)
    }

    fn load_all_sedimentree_ids(&self) -> BoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Storage::<Sendable>::load_all_sedimentree_ids(&self.inner)
    }

    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::save_loose_commit(&self.inner, id, verified)
    }

    // The metrics refresh must NOT call this: it would be an O(trees) per-tree
    // directory sweep. Panicking here turns a regression that re-introduces the
    // sweep into a hard test failure — the panic is the assertion.
    #[allow(clippy::panic)]
    fn list_commit_ids(&self, _id: SedimentreeId) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        panic!("list_commit_ids must not be called by refresh_metrics");
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Storage::<Sendable>::load_loose_commits(&self.inner, id)
    }

    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Storage::<Sendable>::load_loose_commit(&self.inner, id, commit_id)
    }

    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_loose_commit(&self.inner, id, commit_id)
    }

    fn delete_loose_commits(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_loose_commits(&self.inner, id)
    }

    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::save_fragment(&self.inner, id, verified)
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragment(&self.inner, id, fragment_head)
    }

    #[allow(clippy::panic)]
    fn list_fragment_ids(&self, _id: SedimentreeId) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        panic!("list_fragment_ids must not be called by refresh_metrics");
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragments(&self.inner, id)
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_fragment(&self.inner, id, fragment_head)
    }

    fn delete_fragments(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_fragments(&self.inner, id)
    }

    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> BoxFuture<'_, Result<usize, Self::Error>> {
        Storage::<Sendable>::save_batch(&self.inner, id, commits, fragments)
    }
}

/// `refresh_metrics` must refresh only the tree count, which is served from
/// the backend's in-memory id set — it must never enumerate per-tree
/// contents. We register several trees against a storage whose per-tree
/// listing methods panic; a successful refresh proves the O(trees) sweep is
/// gone (and guards against it being re-introduced).
#[tokio::test]
async fn refresh_metrics_does_not_sweep_per_tree() -> TestResult {
    let inner = ListPanicsStorage::new();
    for i in 1..=5u8 {
        Storage::<Sendable>::save_sedimentree_id(&inner, SedimentreeId::new([i; 32])).await?;
    }

    let ms = MetricsStorage::new(inner);

    // If refresh_metrics still swept per tree, list_*_ids would panic here.
    ms.refresh_metrics().await?;

    Ok(())
}
