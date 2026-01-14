//! Filesystem-based storage for Subduction.

use async_lock::Mutex;
use futures::{
    future::{BoxFuture, LocalBoxFuture},
    FutureExt,
};
use futures_kind::{Local, Sendable};
use sedimentree_core::{
    blob::{Blob, Digest},
    storage::Storage,
    Fragment, LooseCommit, SedimentreeId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;

/// Errors that can occur during filesystem storage operations.
#[derive(Debug, Error)]
pub(crate) enum FsStorageError {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// CBOR serialization error.
    #[error("CBOR serialization error: {0}")]
    CborSerialization(String),

    /// CBOR deserialization error.
    #[error("CBOR deserialization error: {0}")]
    CborDeserialization(String),
}

/// Filesystem-based storage backend.
#[derive(Debug, Clone)]
pub(crate) struct FsStorage {
    root: PathBuf,
    // In-memory cache for performance
    ids_cache: Arc<Mutex<BTreeSet<SedimentreeId>>>,
    fragments_cache: Arc<Mutex<BTreeMap<SedimentreeId, BTreeSet<Fragment>>>>,
    commits_cache: Arc<Mutex<BTreeMap<SedimentreeId, BTreeSet<LooseCommit>>>>,
}

impl FsStorage {
    /// Create a new filesystem storage backend at the given root directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub(crate) fn new(root: PathBuf) -> Result<Self, FsStorageError> {
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join("trees"))?;
        std::fs::create_dir_all(root.join("blobs"))?;

        // Load existing data into cache
        let ids_cache = Arc::new(Mutex::new(Self::load_tree_ids(&root)?));

        Ok(Self {
            root,
            ids_cache,
            fragments_cache: Arc::new(Mutex::new(BTreeMap::new())),
            commits_cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn load_tree_ids(root: &PathBuf) -> Result<BTreeSet<SedimentreeId>, FsStorageError> {
        let trees_dir = root.join("trees");
        let mut ids = BTreeSet::new();

        if let Ok(entries) = std::fs::read_dir(trees_dir) {
            for entry in entries.flatten() {
                if let Ok(name) = entry.file_name().into_string() {
                    if let Ok(bytes) = hex::decode(&name) {
                        if bytes.len() == 32 {
                            let mut arr = [0u8; 32];
                            arr.copy_from_slice(&bytes);
                            ids.insert(SedimentreeId::new(arr));
                        }
                    }
                }
            }
        }

        Ok(ids)
    }

    fn tree_path(&self, id: SedimentreeId) -> PathBuf {
        let hex = hex::encode(id.as_bytes());
        self.root.join("trees").join(hex)
    }

    fn blob_path(&self, digest: Digest) -> PathBuf {
        let hex = hex::encode(digest.as_bytes());
        self.root.join("blobs").join(hex)
    }

    fn commits_file(&self, id: SedimentreeId) -> PathBuf {
        self.tree_path(id).join("commits.cbor")
    }

    fn fragments_file(&self, id: SedimentreeId) -> PathBuf {
        self.tree_path(id).join("fragments.cbor")
    }
}

impl Storage<Sendable> for FsStorage {
    type Error = FsStorageError;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: saving sedimentree_id {:?}", sedimentree_id);

            // Update cache
            self.ids_cache.lock().await.insert(sedimentree_id);

            // Create directory for this tree
            let tree_dir = self.tree_path(sedimentree_id);
            tokio::fs::create_dir_all(&tree_dir).await?;

            Ok(())
        }
        .boxed()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: deleting sedimentree_id {:?}", sedimentree_id);

            // Update cache
            self.ids_cache.lock().await.remove(&sedimentree_id);

            // Remove directory
            let tree_dir = self.tree_path(sedimentree_id);
            if tree_dir.exists() {
                tokio::fs::remove_dir_all(&tree_dir).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> BoxFuture<'_, Result<BTreeSet<SedimentreeId>, Self::Error>> {
        async move {
            tracing::debug!("FsStorage: loading all sedimentree_ids");
            Ok(self.ids_cache.lock().await.clone())
        }
        .boxed()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: saving loose commit for {:?}", sedimentree_id);

            // Update cache
            self.commits_cache
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(loose_commit.clone());

            // Load existing commits
            let commits_file = self.commits_file(sedimentree_id);
            let mut commits: Vec<LooseCommit> = if commits_file.exists() {
                let data = tokio::fs::read(&commits_file).await?;
                ciborium::from_reader(&data[..])
                    .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?
            } else {
                Vec::new()
            };

            // Add new commit if not already present
            if !commits.contains(&loose_commit) {
                commits.push(loose_commit);
                let mut data = Vec::new();
                ciborium::into_writer(&commits, &mut data)
                    .map_err(|e| FsStorageError::CborSerialization(e.to_string()))?;
                tokio::fs::write(&commits_file, data).await?;
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
            tracing::debug!("FsStorage: loading loose commits for {:?}", sedimentree_id);

            // Try cache first
            if let Some(commits) = self.commits_cache.lock().await.get(&sedimentree_id) {
                return Ok(commits.iter().cloned().collect());
            }

            // Load from disk
            let commits_file = self.commits_file(sedimentree_id);
            if commits_file.exists() {
                let data = tokio::fs::read(&commits_file).await?;
                let commits: Vec<LooseCommit> = ciborium::from_reader(&data[..])
                    .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;

                // Update cache
                let commits_set: BTreeSet<_> = commits.iter().cloned().collect();
                self.commits_cache
                    .lock()
                    .await
                    .insert(sedimentree_id, commits_set);

                Ok(commits)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: deleting loose commits for {:?}", sedimentree_id);

            // Update cache
            self.commits_cache.lock().await.remove(&sedimentree_id);

            // Delete file
            let commits_file = self.commits_file(sedimentree_id);
            if commits_file.exists() {
                tokio::fs::remove_file(&commits_file).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: saving fragment for {:?}", sedimentree_id);

            // Update cache
            self.fragments_cache
                .lock()
                .await
                .entry(sedimentree_id)
                .or_default()
                .insert(fragment.clone());

            // Load existing fragments
            let fragments_file = self.fragments_file(sedimentree_id);
            let mut fragments: Vec<Fragment> = if fragments_file.exists() {
                let data = tokio::fs::read(&fragments_file).await?;
                ciborium::from_reader(&data[..])
                    .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?
            } else {
                Vec::new()
            };

            // Add new fragment if not already present
            if !fragments.contains(&fragment) {
                fragments.push(fragment);
                let mut data = Vec::new();
                ciborium::into_writer(&fragments, &mut data)
                    .map_err(|e| FsStorageError::CborSerialization(e.to_string()))?;
                tokio::fs::write(&fragments_file, data).await?;
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
            tracing::debug!("FsStorage: loading fragments for {:?}", sedimentree_id);

            // Try cache first
            if let Some(fragments) = self.fragments_cache.lock().await.get(&sedimentree_id) {
                return Ok(fragments.iter().cloned().collect());
            }

            // Load from disk
            let fragments_file = self.fragments_file(sedimentree_id);
            if fragments_file.exists() {
                let data = tokio::fs::read(&fragments_file).await?;
                let fragments: Vec<Fragment> = ciborium::from_reader(&data[..])
                    .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;

                // Update cache
                let fragments_set: BTreeSet<_> = fragments.iter().cloned().collect();
                self.fragments_cache
                    .lock()
                    .await
                    .insert(sedimentree_id, fragments_set);

                Ok(fragments)
            } else {
                Ok(Vec::new())
            }
        }
        .boxed()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: deleting fragments for {:?}", sedimentree_id);

            // Update cache
            self.fragments_cache.lock().await.remove(&sedimentree_id);

            // Delete file
            let fragments_file = self.fragments_file(sedimentree_id);
            if fragments_file.exists() {
                tokio::fs::remove_file(&fragments_file).await?;
            }

            Ok(())
        }
        .boxed()
    }

    fn save_blob(&self, blob: Blob) -> BoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let digest = Digest::hash(blob.contents());
            tracing::debug!("FsStorage: saving blob {:?}", digest);

            let blob_path = self.blob_path(digest);
            tokio::fs::write(&blob_path, blob.contents()).await?;

            Ok(digest)
        }
        .boxed()
    }

    fn load_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            tracing::debug!("FsStorage: loading blob {:?}", blob_digest);

            let blob_path = self.blob_path(blob_digest);
            if blob_path.exists() {
                let data = tokio::fs::read(&blob_path).await?;
                Ok(Some(Blob::new(data)))
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn delete_blob(&self, blob_digest: Digest) -> BoxFuture<'_, Result<(), Self::Error>> {
        async move {
            tracing::debug!("FsStorage: deleting blob {:?}", blob_digest);

            let blob_path = self.blob_path(blob_digest);
            if blob_path.exists() {
                tokio::fs::remove_file(&blob_path).await?;
            }

            Ok(())
        }
        .boxed()
    }
}

impl Storage<Local> for FsStorage {
    type Error = FsStorageError;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::save_sedimentree_id(self, sedimentree_id).boxed_local()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::delete_sedimentree_id(self, sedimentree_id).boxed_local()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<BTreeSet<SedimentreeId>, Self::Error>> {
        <Self as Storage<Sendable>>::load_all_sedimentree_ids(self).boxed_local()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::save_loose_commit(self, sedimentree_id, loose_commit)
            .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        <Self as Storage<Sendable>>::load_loose_commits(self, sedimentree_id).boxed_local()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::delete_loose_commits(self, sedimentree_id).boxed_local()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::save_fragment(self, sedimentree_id, fragment).boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        <Self as Storage<Sendable>>::load_fragments(self, sedimentree_id).boxed_local()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::delete_fragments(self, sedimentree_id).boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        <Self as Storage<Sendable>>::save_blob(self, blob).boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        <Self as Storage<Sendable>>::load_blob(self, blob_digest).boxed_local()
    }

    fn delete_blob(&self, blob_digest: Digest) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        <Self as Storage<Sendable>>::delete_blob(self, blob_digest).boxed_local()
    }
}
