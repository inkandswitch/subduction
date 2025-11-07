//! Storage abstraction for `Sedimentree` data.

use dashmap::mapref::entry::Entry;

use dashmap::{DashMap, DashSet};
use futures::{
    future::{BoxFuture, LocalBoxFuture},
    FutureExt,
};

use crate::{
    blob::Blob,
    future::{FutureKind, Local, Sendable},
    Digest, SedimentreeId,
};

use super::{Fragment, LooseCommit};

/// Abstraction over storage for `Sedimentree` data.
pub trait Storage<K: FutureKind + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Load all loose commits from storage.
    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>>;

    /// Save a loose commit to storage.
    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a fragment to storage.
    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all fragments from storage.
    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<Fragment>, Self::Error>>;

    /// Save a blob to storage.
    fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, Self::Error>>;

    /// Load a blob from storage.
    fn load_blob(&self, blob_digest: Digest) -> K::Future<'_, Result<Option<Blob>, Self::Error>>;
}

/// Errors that can occur when loading tree data (commits or fragments)
#[derive(Debug, thiserror::Error)]
pub enum LoadTreeData {
    /// An error occurred in the storage subsystem itself.
    #[error("error from storage: {0}")]
    Storage(String),

    /// A blob is missing.
    #[error("missing blob: {0}")]
    MissingBlob(Digest),
}

/// An in-memory storage backend.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    fragments: DashMap<SedimentreeId, DashSet<Fragment>>,
    commits: DashMap<SedimentreeId, DashSet<LooseCommit>>,
    blobs: DashMap<Digest, Blob>,
}

impl Storage<Local> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            match self.commits.entry(sedimentree_id) {
                Entry::Occupied(e) => {
                    e.get().insert(loose_commit);
                }
                Entry::Vacant(e) => {
                    let set = DashSet::new();
                    set.insert(loose_commit);
                    e.insert(set);
                }
            }
            Ok(())
        }
        .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            if let Some(commit_entry) = self.commits.get(&sedimentree_id) {
                let set = commit_entry.value();
                let mut commits = Vec::with_capacity(set.len());
                for commit in set.iter() {
                    commits.push(commit.clone());
                }
                Ok(commits)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed_local()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            match self.fragments.entry(sedimentree_id) {
                Entry::Occupied(e) => {
                    e.get().insert(fragment);
                }
                Entry::Vacant(e) => {
                    let set = DashSet::new();
                    set.insert(fragment);
                    e.insert(set);
                }
            }
            Ok(())
        }
        .boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            if let Some(fragment_entry) = self.fragments.get(&sedimentree_id) {
                let set = fragment_entry.value();
                let mut fragments = Vec::with_capacity(set.len());
                for commit in set.iter() {
                    fragments.push(commit.clone());
                }
                Ok(fragments)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.entry(digest).or_insert(blob);
            Ok(digest)
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let maybe_entry = self.blobs.get(&blob_digest);
            Ok(maybe_entry.map(|e| e.value().clone()))
        }
        .boxed_local()
    }
}

impl Storage<Sendable> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            match self.commits.entry(sedimentree_id) {
                Entry::Occupied(e) => {
                    e.get().insert(loose_commit);
                }
                Entry::Vacant(e) => {
                    let set = DashSet::new();
                    set.insert(loose_commit);
                    e.insert(set);
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            if let Some(commit_entry) = self.commits.get(&sedimentree_id) {
                let set = commit_entry.value();
                let mut commits = Vec::with_capacity(set.len());
                for commit in set.iter() {
                    commits.push(commit.clone());
                }
                Ok(commits)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            // NOTE match to avoid cloning when using `.or_insert_with`
            match self.fragments.entry(sedimentree_id) {
                Entry::Occupied(e) => {
                    e.get().insert(fragment);
                }
                Entry::Vacant(e) => {
                    let set = DashSet::new();
                    set.insert(fragment);
                    e.insert(set);
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            if let Some(fragment_entry) = self.fragments.get(&sedimentree_id) {
                let set = fragment_entry.value();
                let mut fragments = Vec::with_capacity(set.len());
                for commit in set.iter() {
                    fragments.push(commit.clone());
                }
                Ok(fragments)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn save_blob(&self, blob: Blob) -> BoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.entry(digest).or_insert(blob);
            Ok(digest)
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let maybe_entry = self.blobs.get(&blob_digest);
            Ok(maybe_entry.map(|e| e.value().clone()))
        }
        .boxed()
    }
}
