//! Coverage for the inspection API: store/tree counts, per-tree bytes
//! (inline records + external blob files), head listing with Byzantine
//! equivocation, and cross-tree head lookup.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use subduction_redb_storage::{HeadKind, RedbStorage};

const EXTERNAL_BLOB_LEN: usize = 20 * 1024; // > the 16 KiB inline threshold

fn signer() -> MemorySigner {
    MemorySigner::from_bytes(&[9u8; 32])
}

async fn commit(
    s: &MemorySigner,
    id: SedimentreeId,
    head: u8,
    blob: Vec<u8>,
) -> VerifiedMeta<LooseCommit> {
    VerifiedMeta::seal::<Sendable, _>(
        s,
        (id, CommitId::new([head; 32]), BTreeSet::new()),
        VerifiedBlobMeta::new(Blob::new(blob)),
    )
    .await
}

async fn fragment(
    s: &MemorySigner,
    id: SedimentreeId,
    head: u8,
    blob: Vec<u8>,
) -> VerifiedMeta<Fragment> {
    VerifiedMeta::seal::<Sendable, _>(
        s,
        (
            id,
            CommitId::new([head; 32]),
            BTreeSet::from([CommitId::new([0xF0; 32])]),
            vec![CommitId::new([0xF1; 32])],
        ),
        VerifiedBlobMeta::new(Blob::new(blob)),
    )
    .await
}

/// Build a two-tree store. Tree A holds an equivocating commit head (`0x01`,
/// two variants from differing blobs), an external-blob commit (`0x02`), and a
/// fragment (`0x03`). Tree B holds one commit (`0x10`).
async fn populate(storage: &RedbStorage) -> (SedimentreeId, SedimentreeId) {
    let s = signer();
    let tree_a = SedimentreeId::new([0xA1; 32]);
    let tree_b = SedimentreeId::new([0xB2; 32]);

    let a_commits = vec![
        commit(&s, tree_a, 0x01, vec![0xAA; 16]).await,
        commit(&s, tree_a, 0x01, vec![0xBB; 16]).await, // same head, different blob → 2nd variant
        commit(&s, tree_a, 0x02, vec![0xCC; EXTERNAL_BLOB_LEN]).await,
    ];
    let a_fragments = vec![fragment(&s, tree_a, 0x03, vec![0xDD; 32]).await];
    Storage::<Sendable>::save_batch(storage, tree_a, a_commits, a_fragments)
        .await
        .expect("populate tree A");

    Storage::<Sendable>::save_batch(
        storage,
        tree_b,
        vec![commit(&s, tree_b, 0x10, vec![0xEE; 64]).await],
        Vec::new(),
    )
    .await
    .expect("populate tree B");

    (tree_a, tree_b)
}

#[tokio::test]
async fn overview_counts_trees_records_and_blob_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RedbStorage::new(dir.path()).expect("open storage");
    let (tree_a, tree_b) = populate(&storage).await;

    let (store, per_tree) = storage.inspect_overview().await.expect("overview");

    assert_eq!(store.trees, 2);
    assert_eq!(store.commits, 4, "3 in A (incl. equivocation) + 1 in B");
    assert_eq!(store.fragments, 1);
    assert_eq!(store.blob_file_count, 1, "only the 20 KiB blob is external");
    assert_eq!(store.blob_total_bytes, EXTERNAL_BLOB_LEN as u64);
    assert!(store.redb_file_bytes > 0, "the db file has a size");
    assert!(store.logical_bytes >= EXTERNAL_BLOB_LEN as u64);

    // Per-tree stats are sorted by id (A1.. before B2..).
    assert_eq!(per_tree.len(), 2);
    assert_eq!(per_tree[0].id, tree_a);
    assert_eq!(per_tree[0].commits, 3);
    assert_eq!(per_tree[0].fragments, 1);
    assert!(
        per_tree[0].bytes >= EXTERNAL_BLOB_LEN as u64,
        "tree A's bytes include its external blob file"
    );
    assert_eq!(per_tree[1].id, tree_b);
    assert_eq!(per_tree[1].commits, 1);
    assert_eq!(per_tree[1].fragments, 0);
}

#[tokio::test]
async fn inspect_tree_returns_none_for_unknown_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RedbStorage::new(dir.path()).expect("open storage");
    let (tree_a, _) = populate(&storage).await;

    let stats = storage.inspect_tree(tree_a).await.expect("inspect A");
    let stats = stats.expect("tree A is registered");
    assert_eq!(stats.commits, 3);
    assert_eq!(stats.fragments, 1);

    let missing = SedimentreeId::new([0x00; 32]);
    assert!(
        storage
            .inspect_tree(missing)
            .await
            .expect("inspect missing")
            .is_none(),
        "an unregistered id yields None"
    );
}

#[tokio::test]
async fn heads_dedupe_and_surface_equivocation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RedbStorage::new(dir.path()).expect("open storage");
    let (tree_a, _) = populate(&storage).await;

    let heads = storage
        .inspect_tree_heads(tree_a, true)
        .await
        .expect("heads");

    // Two distinct commit heads (0x01 with 2 variants, 0x02 with 1).
    assert_eq!(heads.commits.len(), 2);
    let equivocated = heads
        .commits
        .iter()
        .find(|h| h.id == CommitId::new([0x01; 32]))
        .expect("head 0x01 present");
    assert_eq!(equivocated.variants, 2, "0x01 was signed over two blobs");
    assert_eq!(equivocated.digests.len(), 2, "both digests returned");
    assert_ne!(
        equivocated.digests[0], equivocated.digests[1],
        "equivocating variants have distinct content digests"
    );

    let single = heads
        .commits
        .iter()
        .find(|h| h.id == CommitId::new([0x02; 32]))
        .expect("head 0x02 present");
    assert_eq!(single.variants, 1);

    assert_eq!(heads.fragments.len(), 1);
    assert_eq!(heads.fragments[0].id, CommitId::new([0x03; 32]));
}

#[tokio::test]
async fn heads_omit_digests_when_not_requested() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RedbStorage::new(dir.path()).expect("open storage");
    let (tree_a, _) = populate(&storage).await;

    let heads = storage
        .inspect_tree_heads(tree_a, false)
        .await
        .expect("heads");
    for entry in heads.commits.iter().chain(heads.fragments.iter()) {
        assert!(
            entry.digests.is_empty(),
            "digests withheld unless requested"
        );
        assert!(entry.variants >= 1);
    }
}

#[tokio::test]
async fn find_head_locates_across_trees() {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RedbStorage::new(dir.path()).expect("open storage");
    let (tree_a, tree_b) = populate(&storage).await;

    let equivocated = storage
        .inspect_find_head(CommitId::new([0x01; 32]), false)
        .await
        .expect("find 0x01");
    assert_eq!(equivocated.len(), 1);
    assert_eq!(equivocated[0].tree, tree_a);
    assert_eq!(equivocated[0].kind, HeadKind::Commit);
    assert_eq!(equivocated[0].variants, 2);

    let in_b = storage
        .inspect_find_head(CommitId::new([0x10; 32]), false)
        .await
        .expect("find 0x10");
    assert_eq!(in_b.len(), 1);
    assert_eq!(in_b[0].tree, tree_b);

    let frag = storage
        .inspect_find_head(CommitId::new([0x03; 32]), false)
        .await
        .expect("find 0x03");
    assert_eq!(frag.len(), 1);
    assert_eq!(frag[0].kind, HeadKind::Fragment);

    let absent = storage
        .inspect_find_head(CommitId::new([0xEE; 32]), false)
        .await
        .expect("find absent");
    assert!(absent.is_empty(), "an unknown head is found nowhere");
}
