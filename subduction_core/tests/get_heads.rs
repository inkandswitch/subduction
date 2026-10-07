//! Laws for [`Subduction::get_heads`]:
//!
//! - For a loose-commit DAG, the heads are the commits no other commit names
//!   as a parent, in whatever order the commits arrive.
//! - With fragments, including one dominated by a deeper fragment, the heads
//!   match [`Sedimentree::heads`] on the stored content.
//! - A resident read, a cold read by a fresh node, and `get_all_heads` agree.
//! - An unknown tree is `None`; a registered tree with no data is `Some([])`.
//! - A failed storage read is returned, is not cached, and cold reads load
//!   metadata only.

#![cfg(feature = "bolero")]
#![allow(clippy::expect_used)]

use std::{collections::BTreeSet, sync::Arc};

use future_form::Sendable;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
    test_utils::{ArbitraryDag, commit_id_with_depth},
};
use subduction_core::{
    connection::test_utils::{MockConnection, TokioSpawn, TokioTimeout},
    handler::sync::SyncHandler,
    policy::open::OpenPolicy,
    storage::traits::Storage,
    subduction::{Subduction, builder::SubductionBuilder},
    test_utils::probe_storage::{ProbeError, ProbeStorage},
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MockConnection;
type Node = Arc<
    Subduction<
        'static,
        Sendable,
        ProbeStorage,
        Conn,
        SyncHandler<Sendable, ProbeStorage, Conn, OpenPolicy, CountLeadingZeroBytes, TokioSpawn>,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;

const TREE: SedimentreeId = SedimentreeId::new([1u8; 32]);

/// A fresh node over `storage`: nothing is resident until it is read.
///
/// The listener and manager loops are not spawned: these tests only make
/// local reads and writes, and loops that never stop would pile up across
/// property iterations.
fn node_over(storage: ProbeStorage) -> Node {
    let (sd, _handler, _listener, _manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[7u8; 32]))
        .storage(storage, Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    sd
}

/// A leading `0xFF` keeps every commit at depth 0 under
/// `CountLeadingZeroBytes`, so no fragment boundaries come into play.
const fn commit_id(n: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0] = 0xFF;
    bytes[1] = n;
    CommitId::new(bytes)
}

fn blob(n: usize) -> Blob {
    Blob::new(n.to_le_bytes().to_vec())
}

fn as_set(heads: Vec<CommitId>) -> BTreeSet<CommitId> {
    heads.into_iter().collect()
}

#[derive(Debug, Clone)]
enum Item {
    Commit {
        head: CommitId,
        parents: BTreeSet<CommitId>,
    },
    Fragment {
        head: CommitId,
        boundary: BTreeSet<CommitId>,
        checkpoints: Vec<CommitId>,
    },
}

/// Store `items` in a shuffled order on a fresh node, then read the heads
/// back three ways: resident, cold from a second node, and through
/// `get_all_heads` on a third.
async fn store_and_read(items: &[Item], seed: u64) -> [Option<BTreeSet<CommitId>>; 3] {
    let storage = ProbeStorage::default();
    let writer = node_over(storage.clone());

    let mut order: Vec<(usize, &Item)> = items.iter().enumerate().collect();
    order.shuffle(&mut StdRng::seed_from_u64(seed));
    for (n, item) in order {
        match item {
            Item::Commit { head, parents } => {
                writer
                    .store_commit(TREE, *head, parents.clone(), blob(n))
                    .await
                    .expect("store commit");
            }
            Item::Fragment {
                head,
                boundary,
                checkpoints,
            } => writer
                .store_fragment(TREE, *head, boundary.clone(), checkpoints, blob(n))
                .await
                .expect("store fragment"),
        }
    }

    let warm = writer.get_heads(TREE).await.expect("resident read");
    let cold = node_over(storage.clone())
        .get_heads(TREE)
        .await
        .expect("cold read");
    let all = node_over(storage)
        .get_all_heads()
        .await
        .into_iter()
        .find_map(|(id, heads)| (id == TREE).then_some(heads));

    [warm.map(as_set), cold.map(as_set), all.map(as_set)]
}

fn assert_all_eq(got: [Option<BTreeSet<CommitId>>; 3], expected: Option<&BTreeSet<CommitId>>) {
    let [warm, cold, all] = got;
    assert_eq!(warm.as_ref(), expected, "resident");
    assert_eq!(cold.as_ref(), expected, "cold");
    assert_eq!(all.as_ref(), expected, "get_all_heads");
}

#[test]
fn prop_loose_dag_heads_are_unreferenced_commits() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    bolero::check!()
        .with_iterations(256)
        .with_arbitrary::<(Vec<Vec<u8>>, u64)>()
        .for_each(|(parent_picks, seed)| {
            // Commit `i` may only name commits `< i` as parents: always a DAG.
            // `checked_rem` is `None` for `i == 0`, so the first commit is a root.
            let items: Vec<Item> = parent_picks
                .iter()
                .take(24)
                .enumerate()
                .map(|(i, picks)| {
                    let i = u8::try_from(i).expect("at most 24 commits");
                    Item::Commit {
                        head: commit_id(i),
                        parents: picks
                            .iter()
                            .filter_map(|p| p.checked_rem(i))
                            .map(commit_id)
                            .collect(),
                    }
                })
                .collect();

            let referenced: BTreeSet<CommitId> = items
                .iter()
                .flat_map(|item| match item {
                    Item::Commit { parents, .. } => parents.iter().copied(),
                    Item::Fragment { .. } => unreachable!("loose commits only"),
                })
                .collect();
            let expected = (!items.is_empty()).then(|| {
                items
                    .iter()
                    .filter_map(|item| match item {
                        Item::Commit { head, .. } => Some(*head),
                        Item::Fragment { .. } => None,
                    })
                    .filter(|head| !referenced.contains(head))
                    .collect()
            });

            assert_all_eq(
                rt.block_on(store_and_read(&items, *seed)),
                expected.as_ref(),
            );
        });
}

#[test]
fn prop_heads_with_fragments_match_sedimentree_heads() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    bolero::check!()
        .with_iterations(256)
        .with_arbitrary::<(ArbitraryDag, Option<u8>, u64)>()
        .for_each(|(ArbitraryDag { tree }, dominate, seed)| {
            let mut items: Vec<Item> = tree
                .fragments()
                .map(|f| Item::Fragment {
                    head: f.head(),
                    boundary: f.boundary().clone(),
                    checkpoints: Vec::new(),
                })
                .chain(tree.loose_commits().map(|c| Item::Commit {
                    head: c.head(),
                    parents: c.parents().clone(),
                }))
                .collect();

            // A depth-2 fragment covering a shallower one, so minimization
            // has something to drop.
            let fragments: Vec<&Fragment> = tree.fragments().collect();
            let covered = dominate.and_then(|k| {
                let i = usize::from(k).checked_rem(fragments.len())?;
                fragments.get(i).map(|f| (k, *f))
            });
            if let Some((k, covered)) = covered {
                items.push(Item::Fragment {
                    head: commit_id_with_depth(2, k),
                    boundary: covered.boundary().clone(),
                    checkpoints: vec![covered.head()],
                });
            }

            let (frags, commits) = items.iter().enumerate().fold(
                (Vec::new(), Vec::new()),
                |(mut frags, mut commits), (n, item)| {
                    let meta = BlobMeta::new(&blob(n));
                    match item {
                        Item::Commit { head, parents } => {
                            commits.push(LooseCommit::new(TREE, *head, parents.clone(), meta));
                        }
                        Item::Fragment {
                            head,
                            boundary,
                            checkpoints,
                        } => frags.push(Fragment::new(
                            TREE,
                            *head,
                            boundary.clone(),
                            checkpoints,
                            meta,
                        )),
                    }
                    (frags, commits)
                },
            );
            let expected = (!items.is_empty())
                .then(|| as_set(Sedimentree::new(frags, commits).heads(&CountLeadingZeroBytes)));

            assert_all_eq(
                rt.block_on(store_and_read(&items, *seed)),
                expected.as_ref(),
            );
        });
}

#[tokio::test]
async fn unknown_is_none_and_registered_empty_is_some_empty() -> TestResult {
    let storage = ProbeStorage::default();
    let empty = SedimentreeId::new([2u8; 32]);
    Storage::<Sendable>::save_sedimentree_id(&storage, empty).await?;
    let sd = node_over(storage);

    assert_eq!(sd.get_heads(SedimentreeId::new([3u8; 32])).await?, None);
    assert_eq!(sd.get_heads(empty).await?, Some(Vec::new()), "cold");
    assert_eq!(sd.get_heads(empty).await?, Some(Vec::new()), "resident");
    Ok(())
}

#[tokio::test]
async fn failed_read_is_returned_and_not_cached() -> TestResult {
    let probe = ProbeStorage::default();
    node_over(probe.clone())
        .store_commit(TREE, commit_id(0), BTreeSet::new(), blob(0))
        .await?;

    let sd = node_over(probe.clone());

    probe.fail(TREE);
    assert!(matches!(
        sd.get_heads(TREE).await,
        Err(ProbeError::Injected(id)) if id == TREE
    ));
    assert_eq!(sd.get_all_heads().await, vec![(TREE, Vec::new())]);

    probe.recover(TREE);
    assert_eq!(sd.get_heads(TREE).await?, Some(vec![commit_id(0)]));
    Ok(())
}

#[tokio::test]
async fn cold_reads_load_no_blobs() -> TestResult {
    let probe = ProbeStorage::default();
    let writer = node_over(probe.clone());
    writer
        .store_fragment(TREE, commit_id(0), BTreeSet::new(), &[], blob(0))
        .await?;
    writer
        .store_commit(TREE, commit_id(1), BTreeSet::from([commit_id(0)]), blob(1))
        .await?;

    let before = probe.blob_loads();
    node_over(probe.clone()).get_heads(TREE).await?;
    node_over(probe.clone()).get_all_heads().await;
    assert_eq!(probe.blob_loads(), before);
    Ok(())
}
