use super::{Chunk, CommitOrChunk, Diff, LooseCommit, Sedimentree};
pub use error::LoadTreeData;

#[allow(async_fn_in_trait)] // TODO: re-evaluate this decision
pub trait Storage {
    type Error: core::error::Error;
    async fn load_loose_commits(&self) -> Result<Vec<LooseCommit>, Self::Error>;
    async fn load_strata(&self) -> Result<Vec<Chunk>, Self::Error>;
    async fn save_loose_commit(&self, commit: LooseCommit) -> Result<(), Self::Error>;
    async fn save_stratum(&self, stratum: Chunk) -> Result<(), Self::Error>;
    async fn load_blob(&self, blob_hash: crate::Digest) -> Result<Option<Vec<u8>>, Self::Error>;
}

#[tracing::instrument(skip(storage))]
pub async fn load<S: Storage + Clone>(storage: S) -> Result<Option<Sedimentree>, S::Error> {
    let strata = {
        let storage = storage.clone();
        async move { storage.load_strata().await }
    };
    let commits = storage.load_loose_commits();
    let (stratum, commits) = futures::future::try_join(strata, commits).await?;
    tracing::trace!(
        num_commits = commits.len(),
        num_stratum = stratum.len(),
        "loading local tree"
    );
    if stratum.is_empty() && commits.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Sedimentree::new(stratum, commits)))
    }
}

pub async fn update<S: Storage + Clone>(
    storage: S,
    original: Option<&Sedimentree>,
    new: &Sedimentree,
) -> Result<(), S::Error> {
    let (new_strata, new_commits) = original
        .map(|o| {
            let Diff {
                left_missing_strata: _deleted_strata,
                left_missing_commits: _deleted_commits,
                right_missing_strata: new_strata,
                right_missing_commits: new_commits,
            } = o.diff(new);
            (new_strata, new_commits)
        })
        .unwrap_or_else(|| (new.strata.iter().collect(), new.commits.iter().collect()));

    let save_strata = new_strata.into_iter().map(|stratum| {
        let storage = storage.clone();
        async move {
            storage.save_stratum(stratum.clone()).await?;
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
        futures::future::try_join_all(save_strata),
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

pub async fn write_stratum<S: Storage>(storage: S, stratum: Chunk) -> Result<(), S::Error> {
    storage.save_stratum(stratum).await
}

pub async fn load_loose_commit_data<S: Storage>(
    storage: S,
    commit: &LooseCommit,
) -> Result<Option<Vec<u8>>, S::Error> {
    storage.load_blob(commit.blob().digest()).await
}

pub async fn load_stratum_data<S: Storage>(
    storage: S,
    stratum: &Chunk,
) -> Result<Option<Vec<u8>>, S::Error> {
    storage
        .load_blob(stratum.summary().blob_meta().digest())
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
