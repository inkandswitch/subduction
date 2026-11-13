//! Several nodes over one storage must share one minimized tree store.
//!
//! Each node here has its own coordination, as separate processes would; only
//! the storage backend and the store are shared. A per-node store cannot see
//! another node's writes, while [`SharedMemoryMinimizedTreeStore`] keeps every
//! node coherent.

#![allow(clippy::expect_used, clippy::panic)]

use std::{collections::BTreeSet, sync::Arc};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    connection::test_utils::{MockConnection, TokioSpawn, TokioTimeout},
    effects::config::Config,
    handler::sync::SyncHandler,
    minimized_tree_store::{
        memory::MemoryMinimizedTreeStore, shared::SharedMemoryMinimizedTreeStore,
    },
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MockConnection;
type Node<Trees> = Arc<
    Subduction<
        'static,
        Sendable,
        Config<MemoryStorage, OpenPolicy, MemorySigner, TokioTimeout, TokioSpawn, Trees>,
        Conn,
        SyncHandler<
            Sendable,
            Config<MemoryStorage, OpenPolicy, MemorySigner, TokioTimeout, TokioSpawn, Trees>,
            Conn,
            CountLeadingZeroBytes,
        >,
    >,
>;

const ID: SedimentreeId = SedimentreeId::new([0x5C; 32]);

fn shared_node(
    storage: &MemoryStorage,
    trees: &SharedMemoryMinimizedTreeStore,
    seed: u8,
) -> Node<SharedMemoryMinimizedTreeStore> {
    let (node, _handler, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[seed; 32]))
        .storage(storage.clone(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .minimized_tree_store(trees.clone())
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    node
}

fn local_node(storage: &MemoryStorage, seed: u8) -> Node<MemoryMinimizedTreeStore> {
    let (node, _handler, listener, manager) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[seed; 32]))
        .storage(storage.clone(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    node
}

fn head(n: u32) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[0..4].copy_from_slice(&n.to_le_bytes());
    bytes[4] = 0x01;
    CommitId::new(bytes)
}

fn blob(n: u32) -> Blob {
    let base = n.to_le_bytes()[0];
    Blob::new((0..48).map(|i| base.wrapping_add(i)).collect::<Vec<u8>>())
}

async fn commits<Trees>(node: &Node<Trees>) -> usize
where
    Trees:
        subduction_core::minimized_tree_store::MinimizedTreeStore<Sendable> + Send + Sync + 'static,
    Trees::Error: Send,
    Trees::Generation: Send + Sync,
{
    node.get_commits(ID)
        .await
        .map_or(0, |commits| commits.len())
}

#[tokio::test]
async fn nodes_sharing_a_store_see_each_others_writes() -> TestResult {
    let storage = MemoryStorage::new();
    let trees = SharedMemoryMinimizedTreeStore::new();
    let a = shared_node(&storage, &trees, 1);
    let b = shared_node(&storage, &trees, 2);

    a.store_commit(ID, head(1), BTreeSet::new(), blob(1))
        .await?;
    assert_eq!(commits(&a).await, 1, "a warms the shared store");

    b.store_commit(ID, head(2), BTreeSet::new(), blob(2))
        .await?;
    assert_eq!(commits(&a).await, 2, "b's write reaches a's next read");
    assert_eq!(commits(&b).await, 2);
    Ok(())
}

/// This is why nodes sharing storage must share the store: each node's own
/// store keeps the tree it last read, and never sees the other node's write.
#[tokio::test]
async fn per_node_stores_over_shared_storage_go_stale() -> TestResult {
    let storage = MemoryStorage::new();
    let a = local_node(&storage, 1);
    let b = local_node(&storage, 2);

    a.store_commit(ID, head(1), BTreeSet::new(), blob(1))
        .await?;
    assert_eq!(commits(&a).await, 1, "a warms its own store");

    b.store_commit(ID, head(2), BTreeSet::new(), blob(2))
        .await?;
    assert_eq!(commits(&b).await, 2, "storage holds both commits");
    assert_eq!(
        commits(&a).await,
        1,
        "a's own store still serves the tree from before b's write"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_and_reads_across_nodes_converge() -> TestResult {
    const NODES: u8 = 3;
    const WRITES_PER_NODE: u32 = 20;

    let storage = MemoryStorage::new();
    let trees = SharedMemoryMinimizedTreeStore::new();
    let nodes: Vec<_> = (1..=NODES)
        .map(|seed| shared_node(&storage, &trees, seed))
        .collect();

    let mut tasks = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        let node = node.clone();
        let first = u32::try_from(index).expect("few nodes") * WRITES_PER_NODE;
        tasks.push(tokio::spawn(async move {
            for n in first..first + WRITES_PER_NODE {
                node.store_commit(ID, head(n), BTreeSet::new(), blob(n))
                    .await
                    .expect("write");
                // Interleave reads so populates race the other nodes' writes.
                let _seen = node.get_commits(ID).await;
            }
        }));
    }
    for task in tasks {
        task.await?;
    }

    let total = usize::from(NODES) * usize::try_from(WRITES_PER_NODE)?;
    for node in &nodes {
        assert_eq!(
            commits(node).await,
            total,
            "every node sees every committed write once writes settle"
        );
    }
    Ok(())
}
