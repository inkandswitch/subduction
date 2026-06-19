//! Coverage for the inspection API: store/tree counts, per-tree bytes
//! (inline records + external blob files), head listing with Byzantine
//! equivocation, and cross-tree head lookup.

#![allow(clippy::indexing_slicing)]

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
async fn populate(storage: &RedbStorage) -> testresult::TestResult<(SedimentreeId, SedimentreeId)> {
    let s = signer();
    let tree_a = SedimentreeId::new([0xA1; 32]);
    let tree_b = SedimentreeId::new([0xB2; 32]);

    let a_commits = vec![
        commit(&s, tree_a, 0x01, vec![0xAA; 16]).await,
        commit(&s, tree_a, 0x01, vec![0xBB; 16]).await, // same head, different blob → 2nd variant
        commit(&s, tree_a, 0x02, vec![0xCC; EXTERNAL_BLOB_LEN]).await,
    ];
    let a_fragments = vec![fragment(&s, tree_a, 0x03, vec![0xDD; 32]).await];
    Storage::<Sendable>::save_batch(storage, tree_a, a_commits, a_fragments).await?;

    Storage::<Sendable>::save_batch(
        storage,
        tree_b,
        vec![commit(&s, tree_b, 0x10, vec![0xEE; 64]).await],
        Vec::new(),
    )
    .await?;

    Ok((tree_a, tree_b))
}

#[tokio::test]
async fn overview_counts_trees_records_and_blob_files() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, tree_b) = populate(&storage).await?;

    let (store, per_tree) = storage.inspect_overview().await?;

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

    Ok(())
}

#[tokio::test]
async fn inspect_tree_returns_none_for_unknown_id() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, _) = populate(&storage).await?;

    let stats = storage.inspect_tree(tree_a).await?;
    let stats = stats.ok_or("tree A is registered")?;
    assert_eq!(stats.commits, 3);
    assert_eq!(stats.fragments, 1);

    let missing = SedimentreeId::new([0x00; 32]);
    assert!(
        storage.inspect_tree(missing).await?.is_none(),
        "an unregistered id yields None"
    );

    Ok(())
}

#[tokio::test]
async fn heads_dedupe_and_surface_equivocation() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, _) = populate(&storage).await?;

    let heads = storage.inspect_tree_heads(tree_a, true).await?;

    // Two distinct commit heads (0x01 with 2 variants, 0x02 with 1).
    assert_eq!(heads.commits.len(), 2);
    let equivocated = heads
        .commits
        .iter()
        .find(|h| h.id == CommitId::new([0x01; 32]))
        .ok_or("head 0x01 present")?;
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
        .ok_or("head 0x02 present")?;
    assert_eq!(single.variants, 1);

    assert_eq!(heads.fragments.len(), 1);
    assert_eq!(heads.fragments[0].id, CommitId::new([0x03; 32]));

    Ok(())
}

#[tokio::test]
async fn heads_omit_digests_when_not_requested() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, _) = populate(&storage).await?;

    let heads = storage.inspect_tree_heads(tree_a, false).await?;
    for entry in heads.commits.iter().chain(heads.fragments.iter()) {
        assert!(
            entry.digests.is_empty(),
            "digests withheld unless requested"
        );
        assert!(entry.variants >= 1);
    }

    Ok(())
}

#[tokio::test]
async fn find_head_locates_across_trees() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, tree_b) = populate(&storage).await?;

    let equivocated = storage
        .inspect_find_head(CommitId::new([0x01; 32]), false)
        .await?;
    assert_eq!(equivocated.len(), 1);
    assert_eq!(equivocated[0].tree, tree_a);
    assert_eq!(equivocated[0].kind, HeadKind::Commit);
    assert_eq!(equivocated[0].variants, 2);

    let in_b = storage
        .inspect_find_head(CommitId::new([0x10; 32]), false)
        .await?;
    assert_eq!(in_b.len(), 1);
    assert_eq!(in_b[0].tree, tree_b);

    let frag = storage
        .inspect_find_head(CommitId::new([0x03; 32]), false)
        .await?;
    assert_eq!(frag.len(), 1);
    assert_eq!(frag[0].kind, HeadKind::Fragment);

    let absent = storage
        .inspect_find_head(CommitId::new([0xEE; 32]), false)
        .await?;
    assert!(absent.is_empty(), "an unknown head is found nowhere");

    Ok(())
}

/// `inspect_all_heads` dumps every tree's heads, sorted by sedimentree id, with
/// per-tree commit/fragment heads (and digests when requested).
#[tokio::test]
async fn all_heads_dumps_every_tree_sorted() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::new(dir.path())?;
    let (tree_a, tree_b) = populate(&storage).await?;

    let dump = storage.inspect_all_heads(true).await?;
    assert_eq!(dump.len(), 2);
    // Sorted by id: A1.. before B2..
    assert_eq!(dump[0].0, tree_a);
    assert_eq!(dump[1].0, tree_b);

    // Tree A: equivocating commit 0x01 (2 variants) + commit 0x02; fragment 0x03.
    let a = &dump[0].1;
    assert_eq!(a.commits.len(), 2);
    let equiv = a
        .commits
        .iter()
        .find(|h| h.id == CommitId::new([0x01; 32]))
        .ok_or("head 0x01 present")?;
    assert_eq!(equiv.variants, 2);
    assert_eq!(
        equiv.digests.len(),
        2,
        "with_digests=true returns the digests"
    );
    assert_eq!(a.fragments.len(), 1);
    assert_eq!(a.fragments[0].id, CommitId::new([0x03; 32]));

    // Tree B: one commit 0x10, no fragments.
    let b = &dump[1].1;
    assert_eq!(b.commits.len(), 1);
    assert_eq!(b.commits[0].id, CommitId::new([0x10; 32]));
    assert!(b.fragments.is_empty());

    Ok(())
}

/// Per-tree `bytes` is exactly the inline record bytes + the external record
/// metas + the external blob file sizes — each counted once. Pins the summation
/// against an exactly computed expectation (not a `>=` lower bound).
#[tokio::test]
async fn per_tree_bytes_counts_inline_and_external_exactly() -> testresult::TestResult {
    let dir = tempfile::tempdir()?;
    // 64 B threshold: the 32 B blob stays inline, the 128 B blob goes external.
    let storage = RedbStorage::with_inline_threshold(dir.path(), 64)?;
    let s = signer();
    let id = SedimentreeId::new([0xC0; 32]);

    let inline_blob = vec![0xAA; 32];
    let external_blob = vec![0xBB; 128];
    let c_inline = commit(&s, id, 0x01, inline_blob.clone()).await;
    let c_external = commit(&s, id, 0x02, external_blob.clone()).await;
    // The compound value carries the `Signed<T>` wire bytes verbatim.
    let inline_meta = c_inline.signed().as_bytes().len();
    let external_meta = c_external.signed().as_bytes().len();

    Storage::<Sendable>::save_batch(&storage, id, vec![c_inline, c_external], Vec::new()).await?;

    // inline record  = tag(1) + meta_len(4) + meta + blob          (all in the db)
    // external record = tag(1) + meta                              (db)
    //                 + the external file (= blob.len())           (disk)
    let expected =
        (1 + 4 + inline_meta + inline_blob.len()) + (1 + external_meta + external_blob.len());

    let stats = storage.inspect_tree(id).await?.ok_or("tree present")?;
    assert_eq!(stats.commits, 2);
    assert_eq!(
        stats.bytes, expected as u64,
        "per-tree bytes = inline value + external meta + external file, each counted once"
    );

    Ok(())
}

/// Property: `inspect_overview`'s counts exactly reflect what was saved across an
/// arbitrary corpus of trees with varied commit/fragment counts and a mix of
/// inline and external blobs.
///
/// ```text
/// forall corpus.
///   trees           == #trees with >=1 item
///   commits          == Σ commit counts
///   fragments        == Σ fragment counts
///   blob_file_count  == #trees with >=1 commit   (each tree's first commit is
///                                                  external; externals are unique)
///   per_tree sorted by id, Σ per_tree.commits == commits, Σ fragments == fragments
/// ```
#[test]
#[allow(clippy::expect_used, clippy::cast_possible_truncation)]
fn prop_overview_counts_match_corpus() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let s = signer();

    // Each tuple element is one tree's `(commit_count, fragment_count)`, bounded
    // by the generator so iterations stay cheap.
    bolero::check!()
        .with_generator((
            (0u8..=4, 0u8..=2),
            (0u8..=4, 0u8..=2),
            (0u8..=4, 0u8..=2),
            (0u8..=4, 0u8..=2),
        ))
        .for_each(|&(t0, t1, t2, t3)| {
            rt.block_on(async {
                let dir = tempfile::tempdir().expect("tempdir");
                // 64 B threshold: each tree's first commit (65 B) is external.
                let storage =
                    RedbStorage::with_inline_threshold(dir.path(), 64).expect("open storage");

                let (mut exp_trees, mut exp_commits, mut exp_frags, mut exp_ext) = (0, 0, 0, 0);
                for (i, &(n_c, n_f)) in [t0, t1, t2, t3].iter().enumerate() {
                    if n_c + n_f == 0 {
                        continue;
                    }
                    exp_trees += 1;
                    exp_commits += usize::from(n_c);
                    exp_frags += usize::from(n_f);
                    if n_c >= 1 {
                        exp_ext += 1;
                    }

                    let mut id_bytes = [0u8; 32];
                    id_bytes[0] = i as u8 + 1; // distinct, non-zero tree ids
                    let id = SedimentreeId::new(id_bytes);

                    let mut commits = Vec::new();
                    for j in 0..n_c {
                        // First commit external (unique per tree); the rest inline.
                        let blob = if j == 0 {
                            vec![i as u8 + 1; 65]
                        } else {
                            vec![j; 16]
                        };
                        commits.push(commit(&s, id, j, blob).await);
                    }
                    let mut frags = Vec::new();
                    for j in 0..n_f {
                        frags.push(fragment(&s, id, 0xF0 + j, vec![j; 8]).await);
                    }
                    Storage::<Sendable>::save_batch(&storage, id, commits, frags)
                        .await
                        .expect("save");
                }

                let (store, per_tree) = storage.inspect_overview().await.expect("overview");
                assert_eq!(store.trees, exp_trees, "tree count");
                assert_eq!(store.commits, exp_commits, "commit count");
                assert_eq!(store.fragments, exp_frags, "fragment count");
                assert_eq!(store.blob_file_count, exp_ext, "external blob file count");
                assert!(
                    per_tree
                        .windows(2)
                        .all(|w| w[0].id.as_bytes() <= w[1].id.as_bytes()),
                    "per-tree rows sorted by id"
                );
                assert_eq!(
                    per_tree.iter().map(|t| t.commits).sum::<usize>(),
                    store.commits,
                    "per-tree commits sum to the global count"
                );
                assert_eq!(
                    per_tree.iter().map(|t| t.fragments).sum::<usize>(),
                    store.fragments,
                    "per-tree fragments sum to the global count"
                );
            });
        });
}
