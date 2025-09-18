//! Storage abstraction for `Sedimentree` data.

use futures::lock::Mutex;
use std::{collections::HashMap, sync::Arc};

use crate::{Blob, Digest};

use super::{Chunk, CommitOrChunk, Diff, LooseCommit, Sedimentree};
pub use error::LoadTreeData;

/// Abstraction over storage for `Sedimentree` data.
pub trait Storage {
    /// The error type for storage operations.
    type Error: core::error::Error;

    /// Load all loose commits from storage.
    fn load_loose_commits(&self) -> impl Future<Output = Result<Vec<LooseCommit>, Self::Error>>;

    /// Save a loose commit to storage.
    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Save a chunk to storage.
    fn save_chunk(&self, chunk: Chunk) -> impl Future<Output = Result<(), Self::Error>>;

    /// Load all chunks from storage.
    fn load_chunks(&self) -> impl Future<Output = Result<Vec<Chunk>, Self::Error>>;

    /// Save a blob to storage.
    fn save_blob(&self, blob: Blob) -> impl Future<Output = Result<Digest, Self::Error>>;

    /// Load a blob from storage.
    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> impl Future<Output = Result<Option<Blob>, Self::Error>>;
}

/// Load the local `Sedimentree` state from storage.
///
/// # Returns
///
/// * `Ok(None)` if no commits or chunks are found in storage.
/// * `Ok(Some(tree))` if commits or chunks are found in storage.
///
/// # Errors
///
/// * Returns [`S::Error`] if the storage backend encounters an problem loading commits or chunks.
pub async fn load<S: Storage + Clone>(storage: S) -> Result<Option<Sedimentree>, S::Error> {
    let chunks = {
        let storage = storage.clone();
        async move { storage.load_chunks().await }
    };
    let commits = storage.load_loose_commits();
    let (chunk, commits) = futures::future::try_join(chunks, commits).await?;
    tracing::trace!(
        num_commits = commits.len(),
        num_chunk = chunk.len(),
        "loading local tree"
    );
    if chunk.is_empty() && commits.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Sedimentree::new(chunk, commits)))
    }
}

/// Update storage to reflect the new state of the `Sedimentree`.
///
/// # Errors
///
/// * Returns [`S::Error`] if the storage backend encounters an problem saving commits or chunks.
pub async fn update<S: Storage + Clone>(
    storage: S,
    original: Option<&Sedimentree>,
    new: &Sedimentree,
) -> Result<(), S::Error> {
    let (new_chunks, new_commits) = if let Some(o) = original {
        let Diff {
            left_missing_chunks: _deleted_chunks,
            left_missing_commits: _deleted_commits,
            right_missing_chunks: new_chunks,
            right_missing_commits: new_commits,
        } = o.diff(new);
        (new_chunks, new_commits)
    } else {
        (new.chunks.iter().collect(), new.commits.iter().collect())
    };

    let save_chunks = new_chunks.into_iter().map(|chunk| {
        let inner_storage = storage.clone();
        async move {
            inner_storage.save_chunk(chunk.clone()).await?;
            Ok(())
        }
    });

    let save_commits = new_commits.into_iter().map(|commit| {
        let inner_storage = storage.clone();
        async move {
            inner_storage.save_loose_commit(commit.clone()).await?;
            Ok(())
        }
    });

    futures::future::try_join(
        futures::future::try_join_all(save_chunks),
        futures::future::try_join_all(save_commits),
    )
    .await?;
    Ok(())
}

/// Stream the data for all commits and chunks in a `Sedimentree`.
pub fn data<S: Storage + Clone>(
    storage: &S,
    tree: Sedimentree,
) -> impl futures::Stream<Item = Result<(CommitOrChunk, Blob), LoadTreeData>> {
    let items = tree.into_items().map(|item| {
        let storage = storage.clone();
        async move {
            match item {
                super::CommitOrChunk::Commit(c) => {
                    let data = storage
                        .load_blob(c.blob().digest())
                        .await
                        .map_err(|e| LoadTreeData::Storage(e.to_string()))?
                        .ok_or_else(|| LoadTreeData::MissingBlob(c.blob().digest()))?;
                    Ok((CommitOrChunk::Commit(c), data))
                }
                super::CommitOrChunk::Chunk(s) => {
                    let data = storage
                        .load_blob(s.summary().blob_meta().digest())
                        .await
                        .map_err(|e| LoadTreeData::Storage(e.to_string()))?
                        .ok_or_else(|| {
                            LoadTreeData::MissingBlob(s.summary().blob_meta().digest())
                        })?;
                    Ok((CommitOrChunk::Chunk(s), data))
                }
            }
        }
    });
    items.collect::<futures::stream::FuturesUnordered<_>>()
}

/// Write a [`LooseCommit`] to storage.
///
/// # Errors
///
/// * Returns [`S::Error`] if the storage backend encounters an problem saving the commit.
pub async fn write_loose_commit<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<(), S::Error> {
    storage.save_loose_commit(commit.clone()).await
}

/// Write a [`Chunk`] to storage.
///
/// # Errors
///
/// * Returns [`S::Error`] if the storage backend encounters an problem saving the chunk.
pub async fn write_chunk<S: Storage>(storage: S, chunk: Chunk) -> Result<(), S::Error> {
    storage.save_chunk(chunk).await
}

/// Load the data for a [`LooseCommit`].
///
/// # Returns
///
/// * `Ok(None)` if the blob for the commit is not found in storage.
/// * `Ok(Some(blob))` if the blob for the commit is found in storage.
///
/// # Errors
///
/// * Returns [`S::Error`] if the storage backend encounters an problem loading the blob.
pub async fn load_loose_commit_data<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<Option<Blob>, S::Error> {
    storage.load_blob(commit.blob().digest()).await
}

/// Load the data for a [`Chunk`].
///
/// # Errors
///
/// * Returns an error if the storage backend encounters an problem loading the blob.
pub async fn load_chunk_data<S: Storage>(
    storage: S,
    chunk: &Chunk,
) -> Result<Option<Blob>, S::Error> {
    storage
        .load_blob(chunk.summary().blob_meta().digest())
        .await
}

mod error {
    use crate::Digest;

    /// Errors that can occur when loading tree data (commits or chunks)
    #[derive(Debug, thiserror::Error)]
    pub enum LoadTreeData {
        /// An error occurred in the storage subsystem itself.
        #[error("error from storage: {0}")]
        Storage(String),

        /// A blob is missing.
        #[error("missing blob: {0}")]
        MissingBlob(Digest),
    }
}

/// An in-memory storage backend.
#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    chunks: Arc<Mutex<HashMap<Digest, Chunk>>>,
    commits: Arc<Mutex<HashMap<Digest, LooseCommit>>>,
    blobs: Arc<Mutex<HashMap<Digest, Blob>>>,
}

impl Storage for MemoryStorage {
    type Error = std::convert::Infallible;

    async fn load_loose_commits(&self) -> Result<Vec<LooseCommit>, Self::Error> {
        let commits = self.commits.lock().await.values().cloned().collect();
        Ok(commits)
    }

    async fn save_loose_commit(&self, loose_commit: LooseCommit) -> Result<(), Self::Error> {
        let digest = loose_commit.blob().digest();
        self.commits.lock().await.insert(digest, loose_commit);
        Ok(())
    }

    async fn save_chunk(&self, chunk: Chunk) -> Result<(), Self::Error> {
        let digest = chunk.summary().blob_meta().digest();
        self.chunks.lock().await.insert(digest, chunk);
        Ok(())
    }

    async fn load_chunks(&self) -> Result<Vec<Chunk>, Self::Error> {
        let chunks = self.chunks.lock().await.values().cloned().collect();
        Ok(chunks)
    }

    async fn save_blob(&self, blob: Blob) -> Result<Digest, Self::Error> {
        let digest = Digest::hash(blob.contents());
        self.blobs.lock().await.insert(digest, blob);
        Ok(digest)
    }

    async fn load_blob(&self, blob_digest: Digest) -> Result<Option<Blob>, Self::Error> {
        let maybe_blob = self.blobs.lock().await.get(&blob_digest).cloned();
        Ok(maybe_blob)
    }
}
