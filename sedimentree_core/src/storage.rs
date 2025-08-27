use super::{Chunk, CommitOrChunk, Diff, LooseCommit, Sedimentree};
pub use error::LoadTreeData;

#[allow(async_fn_in_trait)] // TODO: re-evaluate this decision
pub trait Storage {
    type Error: core::error::Error;
    async fn load_loose_commits(&self) -> Result<Vec<LooseCommit>, Self::Error>;
    async fn load_chunks(&self) -> Result<Vec<Chunk>, Self::Error>;
    async fn save_loose_commit(&self, commit: LooseCommit) -> Result<(), Self::Error>;
    async fn save_chunk(&self, chunk: Chunk) -> Result<(), Self::Error>;
    async fn load_blob(&self, blob_hash: crate::Digest) -> Result<Option<Vec<u8>>, Self::Error>;
}

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

pub fn data<S: Storage + Clone>(
    storage: S,
    tree: Sedimentree,
) -> impl futures::Stream<Item = Result<(CommitOrChunk, Vec<u8>), LoadTreeData>> {
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

pub async fn write_loose_commit<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<(), S::Error> {
    storage.save_loose_commit(commit.clone()).await
}

pub async fn write_chunk<S: Storage>(storage: S, chunk: Chunk) -> Result<(), S::Error> {
    storage.save_chunk(chunk).await
}

pub async fn load_loose_commit_data<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<Option<Vec<u8>>, S::Error> {
    storage.load_blob(commit.blob().digest()).await
}

pub async fn load_chunk_data<S: Storage>(
    storage: S,
    chunk: &Chunk,
) -> Result<Option<Vec<u8>>, S::Error> {
    storage
        .load_blob(chunk.summary().blob_meta().digest())
        .await
}

mod error {
    use crate::Digest;

    #[derive(Debug, thiserror::Error)]
    pub enum LoadTreeData {
        #[error("error from storage: {0}")]
        Storage(String),
        #[error("missing blob: {0}")]
        MissingBlob(Digest),
    }
}
