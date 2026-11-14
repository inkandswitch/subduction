//! A `Local` node runs with a minimized tree store that is not `Send`, such
//! as one holding JS values on Wasm.

use std::{collections::BTreeSet, rc::Rc, sync::Arc};

use future_form::Local;
use futures::future::LocalBoxFuture;
use sedimentree_core::{
    blob::Blob,
    id::SedimentreeId,
    loose_commit::id::CommitId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn},
    },
    minimized_tree_store::{
        Lookup, MinimizedTreeStore, Populate, memory::MemoryMinimizedTreeStore,
    },
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

const ID: SedimentreeId = SedimentreeId::new([0x10; 32]);

/// A memory store behind an `Rc`, so it is neither `Send` nor `Sync`.
#[derive(Debug, Clone, Default)]
struct NotSendStore {
    inner: Rc<MemoryMinimizedTreeStore>,
}

type Inner = MemoryMinimizedTreeStore;
type InnerEntry = <Inner as MinimizedTreeStore<Local>>::Entry;
type InnerError = <Inner as MinimizedTreeStore<Local>>::Error;

impl MinimizedTreeStore<Local> for NotSendStore {
    type Error = InnerError;
    type Generation = ();
    type Entry = InnerEntry;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Lookup<InnerEntry, ()>, InnerError>> {
        MinimizedTreeStore::<Local>::get(&*self.inner, id)
    }

    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: (),
    ) -> LocalBoxFuture<'_, Result<Populate, InnerError>> {
        MinimizedTreeStore::<Local>::populate(&*self.inner, id, tree, seen)
    }

    fn merge(
        &self,
        id: SedimentreeId,
        update: Sedimentree,
    ) -> LocalBoxFuture<'_, Result<(), InnerError>> {
        MinimizedTreeStore::<Local>::merge(&*self.inner, id, update)
    }

    fn remove(&self, id: SedimentreeId) -> LocalBoxFuture<'_, Result<(), InnerError>> {
        MinimizedTreeStore::<Local>::remove(&*self.inner, id)
    }
}

const fn head(n: u8) -> CommitId {
    CommitId::new([n; 32])
}

#[tokio::test]
async fn a_local_node_writes_and_reads_through_a_store_that_is_not_send() -> TestResult {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = NotSendStore::default();
            let (node, _handler, _listener, _manager) = SubductionBuilder::new()
                .signer(MemorySigner::from_bytes(&[3; 32]))
                .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
                .spawner(TokioSpawn)
                .timer(InstantTimeout)
                .minimized_tree_store(store.clone())
                .build::<Local, ChannelMockConnection<SyncMessage>>();

            node.store_commit(ID, head(1), BTreeSet::new(), Blob::new(vec![1]))
                .await?;
            assert_eq!(node.get_heads(ID).await?, Some(vec![head(1)]));
            assert_eq!(store.inner.len().await, 1, "the read populated the store");

            // A write merges through the worker into the resident tree.
            node.store_commit(ID, head(2), BTreeSet::from([head(1)]), Blob::new(vec![2]))
                .await?;
            assert_eq!(node.get_heads(ID).await?, Some(vec![head(2)]));
            assert_eq!(store.inner.len().await, 1);
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?;
    Ok(())
}
