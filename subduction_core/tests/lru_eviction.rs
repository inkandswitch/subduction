//! The in-memory sedimentree map is a bounded LRU cache over durable
//! storage. These tests prove that *eviction is transparent*: once a cold
//! tree has been pushed out of memory, the public read paths must still
//! return its real contents by re-hydrating from storage — never an empty
//! tree, and never a "not found".
//!
//! This is the correctness guarantee the OOM fix depends on: bounding the
//! resident set must not change observable behaviour.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{collections::BTreeSet, sync::Arc};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    connection::test_utils::{MockConnection, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MockConnection;
type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;
type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
    >,
>;

/// Build a node whose in-memory sedimentree cache is capped at `cap`
/// resident trees.
fn capped_node(cap: usize) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[7u8; 32]))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .max_resident_trees(cap)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

/// Distinct sedimentree id derived from a 32-bit seed (the first four
/// bytes), giving plenty of distinct trees to overflow the cache.
fn sid(n: u32) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    SedimentreeId::new(bytes)
}

/// Distinct commit head derived from a 32-bit seed.
fn commit_id(n: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    bytes[4] = 0x01; // keep distinct from any sedimentree-id byte pattern
    CommitId::new(bytes)
}

fn blob(seed: u32) -> Blob {
    let base = seed.to_le_bytes()[0];
    Blob::new((0..48).map(|i| base.wrapping_add(i)).collect::<Vec<u8>>())
}

/// Store a single commit in tree `id`, returning its head.
async fn store_one_commit(
    sd: &TestSubduction,
    id: SedimentreeId,
    head_seed: u32,
) -> TestResult<CommitId> {
    let head_id = commit_id(head_seed);
    sd.store_commit(id, head_id, BTreeSet::new(), blob(head_seed))
        .await?;
    Ok(head_id)
}

/// Total number of sedimentrees currently resident in the in-memory cache.
async fn resident_count(sd: &TestSubduction) -> usize {
    sd.sedimentrees().len().await
}

/// Number of trees to store. The builder uses the default 256 shards;
/// `with_capacity(cap)` with `cap < 256` floors at 1 tree per shard, so the
/// resident set can never exceed 256. Storing comfortably more than that
/// guarantees the cache overflows and trees are genuinely evicted —
/// regardless of how the `SipHash` distributes them across shards. (600 keeps
/// the margin while bounding test runtime.)
const STORE_COUNT: u32 = 600;
const SHARD_COUNT: usize = 256; // SubductionBuilder default

/// After far more trees than the cache can hold are stored, a previously
/// stored tree is evicted from memory yet must still return its real
/// commits, fragments and heads via the public getters (re-hydration).
///
/// This test *asserts* eviction actually happened (resident count is
/// bounded well below the number stored, and the target tree is verifiably
/// non-resident) before exercising the rehydrate path — so a regression
/// that broke eviction would fail here rather than silently pass.
#[tokio::test]
async fn reads_after_eviction_rehydrate_from_storage() -> TestResult {
    let sd = capped_node(4); // 4 / 256 shards → ≤ 1 per shard → ≤ 256 resident

    // Tree A: store a distinctive commit first, so it is the oldest and a
    // prime eviction candidate once the cache fills.
    let a = sid(0xA000_0000);
    let a_head = store_one_commit(&sd, a, 0x1111_1111).await?;

    // Confirm it reads back while resident.
    let commits = sd.get_commits(a).await.expect("A exists");
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].head(), a_head);

    // Flood with far more trees than can be resident.
    for n in 0..STORE_COUNT {
        store_one_commit(&sd, sid(n), n.wrapping_add(1)).await?;
    }

    // Assert eviction really happened: the resident set is capped at one
    // tree per shard, so it is far below the number stored.
    let resident = resident_count(&sd).await;
    assert!(
        resident <= SHARD_COUNT,
        "resident set ({resident}) must not exceed the per-shard cap ceiling ({SHARD_COUNT})"
    );
    assert!(
        resident < STORE_COUNT as usize,
        "resident set ({resident}) must be far below the {STORE_COUNT} stored trees"
    );

    // And specifically: A is no longer resident (it was the oldest). If it
    // somehow is, evict it explicitly so the read below provably takes the
    // rehydrate path; assert at least that the cache is genuinely bounded.
    if sd.sedimentrees().get_cloned(&a).await.is_some() {
        // Extremely unlikely (A is oldest and the cache is full), but make
        // the rehydrate path deterministic regardless of hash distribution.
        sd.sedimentrees().remove(&a).await;
    }
    assert!(
        sd.sedimentrees().get_cloned(&a).await.is_none(),
        "A must be non-resident before the rehydrating read"
    );

    // The getter must STILL return A's real contents, transparently
    // re-hydrating from storage.
    let commits = sd
        .get_commits(a)
        .await
        .expect("A must still be readable after eviction (re-hydrate)");
    assert_eq!(commits.len(), 1, "evicted tree must rehydrate its commit");
    assert_eq!(
        commits[0].head(),
        a_head,
        "rehydrated commit must be the original, not empty/default"
    );

    // Fragments getter: A has none, but the tree EXISTS, so this must be
    // Some(empty), not None.
    let frags = sd.get_fragments(a).await;
    assert!(
        frags.is_some(),
        "existing tree must not read as nonexistent"
    );

    Ok(())
}

/// `sedimentree_ids` must list every stored tree, including ones evicted
/// from the in-memory cache (it enumerates from durable storage).
#[tokio::test]
async fn sedimentree_ids_complete_across_eviction() -> TestResult {
    let sd = capped_node(4);

    let mut stored = BTreeSet::new();
    for n in 0..STORE_COUNT {
        let id = sid(n);
        store_one_commit(&sd, id, n.wrapping_add(100)).await?;
        stored.insert(id);
    }

    // Confirm the cache genuinely evicted (resident ≪ stored), so the
    // completeness assertion below is meaningful rather than vacuous.
    let resident = resident_count(&sd).await;
    assert!(
        resident < stored.len(),
        "cache must have evicted: resident ({resident}) < stored ({})",
        stored.len()
    );

    // Enumeration must still report ALL stored trees, not just resident ones.
    let listed: BTreeSet<SedimentreeId> = sd.sedimentree_ids().await.into_iter().collect();
    assert_eq!(
        listed, stored,
        "sedimentree_ids must enumerate ALL stored trees from storage, \
         not just the resident set"
    );

    Ok(())
}

/// Writing to a tree *after it has been evicted* must preserve its prior
/// history: the write path has to hydrate the full tree from storage before
/// adding, not start from an empty default. Otherwise the resident tree
/// becomes a partial view (only the latest commit) and `add_commit` would
/// broadcast wrong heads.
///
/// This is the regression test for the write-path partial-tree bug.
#[tokio::test]
async fn write_after_eviction_preserves_prior_history() -> TestResult {
    let sd = capped_node(4); // ≤ 256 resident

    // Store commit #1 into tree A.
    let a = sid(0xA111_0000);
    let h1 = store_one_commit(&sd, a, 0x1111_0001).await?;

    // Flood to evict A.
    for n in 0..STORE_COUNT {
        store_one_commit(&sd, sid(n), n.wrapping_add(1)).await?;
    }
    // Deterministically ensure A is not resident before the second write.
    sd.sedimentrees().remove(&a).await;
    assert!(
        sd.sedimentrees().get_cloned(&a).await.is_none(),
        "A must be evicted before the second write"
    );

    // Store commit #2 into the SAME tree A while it is evicted.
    let h2 = store_one_commit(&sd, a, 0x1111_0002).await?;

    // Both commits must be present: the write hydrated A's full history
    // (h1) before adding h2, rather than starting from an empty tree.
    let commits = sd
        .get_commits(a)
        .await
        .expect("A must exist after the second write");
    let heads: BTreeSet<CommitId> = commits
        .iter()
        .map(sedimentree_core::loose_commit::LooseCommit::head)
        .collect();
    assert!(
        heads.contains(&h1),
        "the pre-eviction commit must survive a write-after-eviction \
         (got heads {heads:?})"
    );
    assert!(
        heads.contains(&h2),
        "the newly-written commit must be present"
    );
    assert_eq!(heads.len(), 2, "exactly the two stored commits, no loss");

    Ok(())
}

/// A nonexistent tree still reads as `None` (the getters distinguish
/// "evicted but exists" from "never stored").
///
/// The stored tree acts as a positive control: without it, a regression
/// that returned `None` for *every* id (including stored ones) would still
/// pass the `is_none` assertions and falsely look correct.
#[tokio::test]
async fn nonexistent_tree_reads_as_none() -> TestResult {
    let sd = capped_node(4);
    let stored = sid(1);
    let head = store_one_commit(&sd, stored, 1).await?;

    // Positive control: a stored id must read as `Some` with its real commit.
    let commits = sd
        .get_commits(stored)
        .await
        .expect("stored tree must read as Some");
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].head(), head);
    assert!(
        sd.get_fragments(stored).await.is_some(),
        "stored tree must read as Some(empty), not None"
    );

    // Negative: a never-stored id reads as `None` on both getters.
    assert!(sd.get_commits(sid(0xDEAD_BEEF)).await.is_none());
    assert!(sd.get_fragments(sid(0xDEAD_BEEF)).await.is_none());

    Ok(())
}
