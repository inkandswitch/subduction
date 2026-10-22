//! Storage that pauses the first existence probe, before reading its result.

use std::sync::atomic::{AtomicBool, Ordering};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::{memory::MemoryStorage, traits::Storage};
use subduction_crypto::verified_meta::VerifiedMeta;

#[derive(Debug)]
pub(super) struct ProbeStorage {
    inner: MemoryStorage,
    pause_next: AtomicBool,
    entered: async_channel::Sender<()>,
    resume: async_channel::Receiver<()>,
}

impl ProbeStorage {
    pub(super) fn new() -> (Self, async_channel::Receiver<()>, async_channel::Sender<()>) {
        let (entered, observed) = async_channel::bounded(1);
        let (release, resume) = async_channel::bounded(1);
        (
            Self {
                inner: MemoryStorage::new(),
                pause_next: AtomicBool::new(true),
                entered,
                resume,
            },
            observed,
            release,
        )
    }
}

impl Storage<Sendable> for ProbeStorage {
    type Error = <MemoryStorage as Storage<Sendable>>::Error;

    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<bool, Self::Error>> {
        Box::pin(async move {
            if self.pause_next.swap(false, Ordering::SeqCst) {
                self.entered
                    .send(())
                    .await
                    .expect("test should observe probe");
                self.resume.recv().await.expect("test should release probe");
            }
            Storage::<Sendable>::contains_sedimentree_id(&self.inner, id).await
        })
    }

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

    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Storage::<Sendable>::list_commit_ids(&self.inner, id)
    }

    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Storage::<Sendable>::load_loose_commits(&self.inner, id)
    }

    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Storage::<Sendable>::load_loose_commit_metas(&self.inner, id)
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

    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Storage::<Sendable>::list_fragment_ids(&self.inner, id)
    }

    fn load_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> BoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragment(&self.inner, id, head)
    }

    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Storage::<Sendable>::load_fragments(&self.inner, id)
    }

    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Storage::<Sendable>::load_fragment_metas(&self.inner, id)
    }

    fn delete_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        Storage::<Sendable>::delete_fragment(&self.inner, id, head)
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
