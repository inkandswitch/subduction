//! Storage abstraction for `Sedimentree` data.

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
#[tracing::instrument(skip(storage))]
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
pub async fn update<S: Storage + Clone>(
    storage: S,
    original: Option<&Sedimentree>,
    new: &Sedimentree,
) -> Result<(), S::Error> {
    let (new_chunks, new_commits) = original
        .map(|o| {
            let Diff {
                left_missing_chunks: _deleted_chunks,
                left_missing_commits: _deleted_commits,
                right_missing_chunks: new_chunks,
                right_missing_commits: new_commits,
            } = o.diff(new);
            (new_chunks, new_commits)
        })
        .unwrap_or_else(|| (new.chunks.iter().collect(), new.commits.iter().collect()));

    let save_chunks = new_chunks.into_iter().map(|chunk| {
        let storage = storage.clone();
        async move {
            storage.save_chunk(chunk.clone()).await?;
            Ok(())
        }
    });

    let save_commits = new_commits.into_iter().map(|commit| {
        let storage = storage.clone();
        async move {
            storage.save_loose_commit(commit.clone()).await?;
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
    storage: S,
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
    futures::stream::FuturesUnordered::from_iter(items)
}

/// Write a [`LooseCommit`] to storage.
pub async fn write_loose_commit<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<(), S::Error> {
    storage.save_loose_commit(commit.clone()).await
}

/// Write a [`Chunk`] to storage.
pub async fn write_chunk<S: Storage>(storage: S, chunk: Chunk) -> Result<(), S::Error> {
    storage.save_chunk(chunk).await
}

/// Load the data for a [`LooseCommit`].
pub async fn load_loose_commit_data<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<Option<Blob>, S::Error> {
    storage.load_blob(commit.blob().digest()).await
}

/// Load the data for a [`Chunk`].
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
