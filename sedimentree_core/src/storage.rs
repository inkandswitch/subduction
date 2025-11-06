//! Storage abstraction for `Sedimentree` data.

use std::{collections::HashMap, sync::Arc};

use futures::{
    future::{BoxFuture, LocalBoxFuture},
    lock::Mutex,
    FutureExt,
};

use crate::{
    blob::Blob,
    future::{FutureKind, Local, Sendable},
    Digest,
};

use super::{Fragment, LooseCommit};

/// Abstraction over storage for `Sedimentree` data.
pub trait Storage<K: FutureKind + ?Sized> {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Load all loose commits from storage.
    fn load_loose_commits(&self) -> K::Future<'_, Result<Vec<LooseCommit>, Self::Error>>;

    /// Save a loose commit to storage.
    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), Self::Error>>;

    /// Save a fragment to storage.
    fn save_fragment(&self, fragment: Fragment) -> K::Future<'_, Result<(), Self::Error>>;

    /// Load all fragments from storage.
    fn load_fragments(&self) -> K::Future<'_, Result<Vec<Fragment>, Self::Error>>;

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
    fragments: Arc<Mutex<HashMap<Digest, Fragment>>>,
    commits: Arc<Mutex<HashMap<Digest, LooseCommit>>>,
    blobs: Arc<Mutex<HashMap<Digest, Blob>>>,
}

impl Storage<Sendable> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn load_loose_commits(&self) -> BoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let commits = self.commits.lock().await.values().cloned().collect();
            Ok(commits)
        }
        .boxed()
    }

    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let digest = loose_commit.blob_meta().digest();
            self.commits.lock().await.insert(digest, loose_commit);
            Ok(())
        }
        .boxed()
    }

    fn save_fragment(&self, fragment: Fragment) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let digest = fragment.summary().blob_meta().digest();
            self.fragments.lock().await.insert(digest, fragment);
            Ok(())
        }
        .boxed()
    }

    fn load_fragments(&self) -> BoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let fragments = self.fragments.lock().await.values().cloned().collect();
            Ok(fragments)
        }
        .boxed()
    }

    fn save_blob(&self, blob: Blob) -> BoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.insert(digest, blob);
            Ok(digest)
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let maybe_blob = self.blobs.lock().await.get(&blob_digest).cloned();
            Ok(maybe_blob)
        }
        .boxed()
    }
}

impl Storage<Local> for MemoryStorage {
    type Error = std::convert::Infallible;

    fn load_loose_commits(&self) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let commits = self.commits.lock().await.values().cloned().collect();
            Ok(commits)
        }
        .boxed_local()
    }

    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let digest = loose_commit.blob_meta().digest();
            self.commits.lock().await.insert(digest, loose_commit);
            Ok(())
        }
        .boxed_local()
    }

    fn save_fragment(&self, fragment: Fragment) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let digest = fragment.summary().blob_meta().digest();
            self.fragments.lock().await.insert(digest, fragment);
            Ok(())
        }
        .boxed_local()
    }

    fn load_fragments(&self) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let fragments = self.fragments.lock().await.values().cloned().collect();
            Ok(fragments)
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let digest = Digest::hash(blob.contents());
            self.blobs.lock().await.insert(digest, blob);
            Ok(digest)
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let maybe_blob = self.blobs.lock().await.get(&blob_digest).cloned();
            Ok(maybe_blob)
        }
        .boxed_local()
    }
}
