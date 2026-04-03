//! Integration tests verifying that the `Subduction` layer minimizes
//! in-memory sedimentrees after mutations.
//!
//! These tests confirm that `minimize_tree` is called at the right points
//! in the `Subduction` API, so the in-memory tree always reflects the
//! minimal covering — dominated fragments are pruned, and fingerprint
//! summaries are compact.

#![allow(clippy::expect_used, clippy::panic)]

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::fingerprint::FingerprintSeed,
    id::SedimentreeId, loose_commit::id::CommitId,
};
use std::{collections::BTreeSet, sync::Arc};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use testresult::TestResult;

/// Create a `CommitId` with `n` leading zero bytes.
///
/// This controls the depth assigned by `CountLeadingZeroBytes`.
fn digest_with_leading_zeros(n: u8, seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    // First non-zero byte (ensures exact depth)
    if let Some(slot) = bytes.get_mut(n as usize) {
        *slot = 1;
    }
    // Seed for uniqueness
    if let Some(slot) = bytes.get_mut(n as usize + 1) {
        *slot = seed;
    }
    CommitId::new(bytes)
}

fn make_unique_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

/// Helper: create and spawn a Subduction instance for testing.
///
/// Returns only the `Arc<Subduction>` handle; listener and actor
/// futures are spawned onto the tokio runtime automatically.
///
/// Must be called from within a tokio runtime context.
type TestSyncHandler = SyncHandler<
    Sendable,
    MemoryStorage,
    ChannelMockConnection<SyncMessage>,
    OpenPolicy,
    CountLeadingZeroBytes,
>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        ChannelMockConnection<SyncMessage>,
        TestSyncHandler,
        OpenPolicy,
        subduction_crypto::signer::memory::MemorySigner,
        InstantTimeout,
        CountLeadingZeroBytes,
    >,
>;

fn make_subduction() -> TestSubduction {
    let (subduction, _handler, listener_fut, actor_fut) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);
    subduction
}

/// After `add_fragment`, a deep fragment that dominates a shallower one
/// should cause the shallow fragment to be pruned from the in-memory tree.
///
/// This tests that `minimize_tree` is called within `add_fragment`.
#[tokio::test]
async fn add_fragment_prunes_dominated_shallow_fragment() -> TestResult {
    let subduction = make_subduction();

    let sed_id = SedimentreeId::new([1u8; 32]);

    // Shallow fragment: depth 2 (head has 2 leading zero bytes)
    let shallow_head = digest_with_leading_zeros(2, 1);
    let shallow_boundary = digest_with_leading_zeros(1, 100);

    subduction
        .add_fragment(
            sed_id,
            shallow_head,
            BTreeSet::from([shallow_boundary]),
            &[],
            make_unique_blob(10),
        )
        .await?;

    // Verify shallow fragment is present
    let fragments_before = subduction.get_fragments(sed_id).await;
    assert_eq!(
        fragments_before.as_ref().map(Vec::len),
        Some(1),
        "should have 1 fragment after first add"
    );

    // Deep fragment: depth 3 (head has 3 leading zero bytes)
    // Its checkpoints include the shallow fragment's head and boundary,
    // so it fully dominates the shallow fragment.
    let deep_head = digest_with_leading_zeros(3, 2);
    let deep_boundary = digest_with_leading_zeros(1, 101);

    subduction
        .add_fragment(
            sed_id,
            deep_head,
            BTreeSet::from([deep_boundary]),
            &[shallow_head, shallow_boundary],
            make_unique_blob(20),
        )
        .await?;

    // After adding the deep fragment, minimize_tree should have pruned
    // the shallow fragment (dominated by the deep one).
    let fragments_after = subduction.get_fragments(sed_id).await;
    assert_eq!(
        fragments_after.as_ref().map(Vec::len),
        Some(1),
        "shallow fragment should be pruned — only deep fragment remains"
    );

    Ok(())
}

/// After `add_commit`, the in-memory tree should reflect the minimized state.
/// With only loose commits (no fragments), minimize is a no-op —
/// all commits survive. This test confirms the call doesn't lose data.
#[tokio::test]
async fn add_commit_preserves_all_commits_without_fragments() -> TestResult {
    let subduction = make_subduction();

    let sed_id = SedimentreeId::new([2u8; 32]);

    for i in 0..5u8 {
        subduction
            .add_commit(
                sed_id,
                CommitId::new([i + 100; 32]),
                BTreeSet::new(),
                make_unique_blob(i),
            )
            .await?;
    }

    let commits = subduction.get_commits(sed_id).await;
    assert_eq!(
        commits.map(|c| c.len()),
        Some(5),
        "all 5 commits should survive minimize (no fragments to prune them)"
    );

    Ok(())
}

/// The fingerprint summary computed from the in-memory tree after
/// `add_fragment` should only include non-dominated fragments.
///
/// This is the integration-level version of the unit test
/// `fingerprint_summarize_on_minimized_excludes_dominated_fragments`.
#[tokio::test]
async fn fingerprint_summary_excludes_dominated_fragments() -> TestResult {
    let subduction = make_subduction();

    let sed_id = SedimentreeId::new([3u8; 32]);

    // Add shallow then deep (deep dominates shallow)
    let shallow_head = digest_with_leading_zeros(2, 1);
    let shallow_boundary = digest_with_leading_zeros(1, 100);
    subduction
        .add_fragment(
            sed_id,
            shallow_head,
            BTreeSet::from([shallow_boundary]),
            &[],
            make_unique_blob(10),
        )
        .await?;

    let deep_head = digest_with_leading_zeros(3, 2);
    let deep_boundary = digest_with_leading_zeros(1, 101);
    subduction
        .add_fragment(
            sed_id,
            deep_head,
            BTreeSet::from([deep_boundary]),
            &[shallow_head, shallow_boundary],
            make_unique_blob(20),
        )
        .await?;

    // Get the in-memory tree and compute fingerprint summary
    let tree = subduction
        .sedimentrees()
        .get_cloned(&sed_id)
        .await
        .expect("sedimentree should exist");

    let seed = FingerprintSeed::new(42, 99);
    let summary = tree.fingerprint_summarize(&seed);

    assert_eq!(
        summary.fragment_fingerprints().len(),
        1,
        "only the deep (non-dominated) fragment should be in the fingerprint summary"
    );

    Ok(())
}

/// Two independent fragments (non-overlapping) at the same depth
/// should both survive minimization.
#[tokio::test]
async fn independent_fragments_both_survive_minimize() -> TestResult {
    let subduction = make_subduction();

    let sed_id = SedimentreeId::new([4u8; 32]);

    // Fragment 1: depth 2
    subduction
        .add_fragment(
            sed_id,
            digest_with_leading_zeros(2, 1),
            BTreeSet::from([digest_with_leading_zeros(1, 100)]),
            &[],
            make_unique_blob(10),
        )
        .await?;

    // Fragment 2: depth 2, different range (no overlap)
    subduction
        .add_fragment(
            sed_id,
            digest_with_leading_zeros(2, 2),
            BTreeSet::from([digest_with_leading_zeros(1, 101)]),
            &[],
            make_unique_blob(20),
        )
        .await?;

    let fragments = subduction.get_fragments(sed_id).await;
    assert_eq!(
        fragments.map(|f| f.len()),
        Some(2),
        "both independent fragments should survive minimization"
    );

    Ok(())
}
