//! Filesystem-based storage for Sedimentree.
//!
//! This crate provides [`FsStorage`], a content-addressed filesystem storage
//! implementation that implements the [`Storage`] trait from `subduction_core`.
//!
//! # Storage Layout
//!
//! ```text
//! root/
//! └── trees/
//!     └── {sedimentree_id_hex}/
//!         ├── blobs/
//!         │   └── {digest_hex}           ← raw bytes
//!         ├── commits/
//!         │   └── {digest_hex}.cbor      ← Signed<LooseCommit>
//!         └── fragments/
//!             └── {digest_hex}.cbor      ← Signed<Fragment>
//! ```
//!
//! # Example
//!
//! ```no_run
//! use sedimentree_fs_storage::FsStorage;
//! use std::path::PathBuf;
//!
//! let storage = FsStorage::new(PathBuf::from("./data")).expect("failed to create storage");
//! ```

#![forbid(unsafe_code)]

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable};
use sedimentree_core::{
    blob::Blob, collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subduction_core::storage::traits::{BatchResult, Storage};
use subduction_crypto::signed::Signed;
use thiserror::Error;

/// Errors that can occur during filesystem storage operations.
#[derive(Debug, Error)]
pub enum FsStorageError {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// CBOR serialization error.
    #[error("CBOR serialization error: {0}")]
    CborSerialization(String),

    /// CBOR deserialization error.
    #[error("CBOR deserialization error: {0}")]
    CborDeserialization(String),

    /// Failed to compute digest from signed payload.
    #[error("Failed to compute digest from signed payload")]
    DigestComputationFailed,
}

/// Filesystem-based storage backend.
///
/// Uses a CAS layout:
/// ```text
/// root/
/// └── trees/
///     └── {sedimentree_id_hex}/
///         ├── blobs/
///         │   └── {digest_hex}           ← raw bytes
///         ├── commits/
///         │   └── {digest_hex}.cbor      ← Signed<LooseCommit>
///         └── fragments/
///             └── {digest_hex}.cbor      ← Signed<Fragment>
/// ```
#[derive(Debug, Clone)]
pub struct FsStorage {
    root: PathBuf,
    ids_cache: Arc<Mutex<Set<SedimentreeId>>>,
}

impl FsStorage {
    /// Create a new filesystem storage backend at the given root directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub fn new(root: PathBuf) -> Result<Self, FsStorageError> {
        std::fs::create_dir_all(&root)?;
        std::fs::create_dir_all(root.join("trees"))?;
        std::fs::create_dir_all(root.join("blobs"))?;

        let ids_cache = Arc::new(Mutex::new(Self::load_tree_ids(&root)));

        Ok(Self { root, ids_cache })
    }

    /// Returns the root directory of the storage.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn load_tree_ids(root: &Path) -> Set<SedimentreeId> {
        let trees_dir = root.join("trees");
        let mut ids = Set::new();

        if let Ok(entries) = std::fs::read_dir(trees_dir) {
            for entry in entries.flatten() {
                if let Ok(name) = entry.file_name().into_string()
                    && let Ok(bytes) = hex::decode(&name)
                    && bytes.len() == 32
                {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    ids.insert(SedimentreeId::new(arr));
                }
            }
        }

        ids
    }

    fn tree_path(&self, id: SedimentreeId) -> PathBuf {
        let hex = hex::encode(id.as_bytes());
        self.root.join("trees").join(hex)
    }

    fn commits_dir(&self, id: SedimentreeId) -> PathBuf {
        self.tree_path(id).join("commits")
    }

    fn fragments_dir(&self, id: SedimentreeId) -> PathBuf {
        self.tree_path(id).join("fragments")
    }

    fn commit_path(&self, id: SedimentreeId, digest: Digest<LooseCommit>) -> PathBuf {
        self.commits_dir(id)
            .join(format!("{}.cbor", hex::encode(digest.as_bytes())))
    }

    fn fragment_path(&self, id: SedimentreeId, digest: Digest<Fragment>) -> PathBuf {
        self.fragments_dir(id)
            .join(format!("{}.cbor", hex::encode(digest.as_bytes())))
    }

    fn blob_path(&self, sedimentree_id: SedimentreeId, digest: Digest<Blob>) -> PathBuf {
        let tree_hex = hex::encode(sedimentree_id.as_bytes());
        let blob_hex = hex::encode(digest.as_bytes());
        self.root
            .join("trees")
            .join(tree_hex)
            .join("blobs")
            .join(blob_hex)
    }

    fn commit_digest(signed: &Signed<LooseCommit>) -> Option<Digest<LooseCommit>> {
        signed.decode_payload().ok().map(|c| c.digest())
    }

    fn fragment_digest(signed: &Signed<Fragment>) -> Option<Digest<Fragment>> {
        signed.decode_payload().ok().map(|f| f.digest())
    }

    fn parse_commit_digest_from_filename(name: &str) -> Option<Digest<LooseCommit>> {
        let hex_str = name.strip_suffix(".cbor")?;
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(Digest::from_bytes(arr))
        } else {
            None
        }
    }

    fn parse_fragment_digest_from_filename(name: &str) -> Option<Digest<Fragment>> {
        let hex_str = name.strip_suffix(".cbor")?;
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(Digest::from_bytes(arr))
        } else {
            None
        }
    }
}

impl Storage<Sendable> for FsStorage {
    type Error = FsStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::save_sedimentree_id");

            self.ids_cache.lock().await.insert(sedimentree_id);

            let tree_dir = self.tree_path(sedimentree_id);
            tokio::fs::create_dir_all(&tree_dir).await?;
            tokio::fs::create_dir_all(self.commits_dir(sedimentree_id)).await?;
            tokio::fs::create_dir_all(self.fragments_dir(sedimentree_id)).await?;

            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::delete_sedimentree_id");

            self.ids_cache.lock().await.remove(&sedimentree_id);

            let tree_dir = self.tree_path(sedimentree_id);
            if let Err(e) = tokio::fs::remove_dir_all(&tree_dir).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!("FsStorage::load_all_sedimentree_ids");
            Ok(self.ids_cache.lock().await.clone())
        })
    }

    // ==================== Commits (CAS) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Digest<LooseCommit>, Self::Error>> {
        Sendable::from_future(async move {
            let digest = Self::commit_digest(&loose_commit)
                .ok_or(FsStorageError::DigestComputationFailed)?;
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::save_loose_commit");

            let commit_path = self.commit_path(sedimentree_id, digest);
            if tokio::fs::try_exists(&commit_path).await.unwrap_or(false) {
                return Ok(digest);
            }

            tokio::fs::create_dir_all(self.commits_dir(sedimentree_id)).await?;

            let data = minicbor::to_vec(&loose_commit)
                .map_err(|e| FsStorageError::CborSerialization(e.to_string()))?;

            let temp_path = commit_path.with_extension("tmp");
            tokio::fs::write(&temp_path, &data).await?;
            tokio::fs::rename(&temp_path, &commit_path).await?;

            Ok(digest)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<Signed<LooseCommit>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::load_loose_commit");

            let commit_path = self.commit_path(sedimentree_id, digest);
            match tokio::fs::read(&commit_path).await {
                Ok(data) => {
                    let signed: Signed<LooseCommit> = minicbor::decode(&data)
                        .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;
                    Ok(Some(signed))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::list_commit_digests");

            let commits_dir = self.commits_dir(sedimentree_id);
            let mut digests = Set::new();

            let mut entries = match tokio::fs::read_dir(&commits_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(digests),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(digest) = Self::parse_commit_digest_from_filename(name)
                {
                    digests.insert(digest);
                }
            }

            Ok(digests)
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>,
    > {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::load_loose_commits");

            let commits_dir = self.commits_dir(sedimentree_id);
            let mut result = Vec::new();

            let mut entries = match tokio::fs::read_dir(&commits_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(digest) = Self::parse_commit_digest_from_filename(name)
                {
                    let data = tokio::fs::read(entry.path()).await?;
                    let signed: Signed<LooseCommit> = minicbor::decode(&data)
                        .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;
                    result.push((digest, signed));
                }
            }

            Ok(result)
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::delete_loose_commit");

            let commit_path = self.commit_path(sedimentree_id, digest);
            if let Err(e) = tokio::fs::remove_file(&commit_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::delete_loose_commits");

            let commits_dir = self.commits_dir(sedimentree_id);
            if let Err(e) = tokio::fs::remove_dir_all(&commits_dir).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    // ==================== Fragments (CAS) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Digest<Fragment>, Self::Error>> {
        Sendable::from_future(async move {
            let digest =
                Self::fragment_digest(&fragment).ok_or(FsStorageError::DigestComputationFailed)?;
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::save_fragment");

            let fragment_path = self.fragment_path(sedimentree_id, digest);
            if tokio::fs::try_exists(&fragment_path).await.unwrap_or(false) {
                return Ok(digest);
            }

            tokio::fs::create_dir_all(self.fragments_dir(sedimentree_id)).await?;

            let data = minicbor::to_vec(&fragment)
                .map_err(|e| FsStorageError::CborSerialization(e.to_string()))?;

            let temp_path = fragment_path.with_extension("tmp");
            tokio::fs::write(&temp_path, &data).await?;
            tokio::fs::rename(&temp_path, &fragment_path).await?;

            Ok(digest)
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<Signed<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::load_fragment");

            let fragment_path = self.fragment_path(sedimentree_id, digest);
            match tokio::fs::read(&fragment_path).await {
                Ok(data) => {
                    let signed: Signed<Fragment> = minicbor::decode(&data)
                        .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;
                    Ok(Some(signed))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::list_fragment_digests");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            let mut digests = Set::new();

            let mut entries = match tokio::fs::read_dir(&fragments_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(digests),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(digest) = Self::parse_fragment_digest_from_filename(name)
                {
                    digests.insert(digest);
                }
            }

            Ok(digests)
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<
        '_,
        Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>,
    > {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::load_fragments");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            let mut result = Vec::new();

            let mut entries = match tokio::fs::read_dir(&fragments_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(digest) = Self::parse_fragment_digest_from_filename(name)
                {
                    let data = tokio::fs::read(entry.path()).await?;
                    let signed: Signed<Fragment> = minicbor::decode(&data)
                        .map_err(|e| FsStorageError::CborDeserialization(e.to_string()))?;
                    result.push((digest, signed));
                }
            }

            Ok(result)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::delete_fragment");

            let fragment_path = self.fragment_path(sedimentree_id, digest);
            if let Err(e) = tokio::fs::remove_file(&fragment_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::delete_fragments");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            if let Err(e) = tokio::fs::remove_dir_all(&fragments_dir).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    // ==================== Blobs ====================

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Sendable::from_future(async move {
            let digest: Digest<Blob> = Digest::hash_bytes(blob.contents());
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::save_blob");

            let blob_path = self.blob_path(sedimentree_id, digest);
            if tokio::fs::try_exists(&blob_path).await.unwrap_or(false) {
                return Ok(digest);
            }

            if let Some(parent) = blob_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let temp_path = blob_path.with_extension("tmp");
            tokio::fs::write(&temp_path, blob.contents()).await?;
            tokio::fs::rename(&temp_path, &blob_path).await?;

            Ok(digest)
        })
    }

    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<Blob>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?blob_digest, "FsStorage::load_blob");
            let blob_path = self.blob_path(sedimentree_id, blob_digest);
            match tokio::fs::read(&blob_path).await {
                Ok(data) => Ok(Some(Blob::new(data))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn load_blobs(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digests: &[Digest<Blob>],
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, Self::Error>> {
        let blob_digests = blob_digests.to_vec();
        Sendable::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                count = blob_digests.len(),
                "FsStorage::load_blobs"
            );

            let mut results = Vec::with_capacity(blob_digests.len());
            for digest in blob_digests {
                let blob_path = self.blob_path(sedimentree_id, digest);
                match tokio::fs::read(&blob_path).await {
                    Ok(data) => results.push((digest, Blob::new(data))),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(results)
        })
    }

    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?blob_digest, "FsStorage::delete_blob");

            let blob_path = self.blob_path(sedimentree_id, blob_digest);
            if let Err(e) = tokio::fs::remove_file(&blob_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    // ==================== Convenience Methods ====================

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::save_commit_with_blob");
            let blob_digest = Storage::<Sendable>::save_blob(self, sedimentree_id, blob).await?;
            Storage::<Sendable>::save_loose_commit(self, sedimentree_id, commit).await?;
            Ok(blob_digest)
        })
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::save_fragment_with_blob");
            let blob_digest = Storage::<Sendable>::save_blob(self, sedimentree_id, blob).await?;
            Storage::<Sendable>::save_fragment(self, sedimentree_id, fragment).await?;
            Ok(blob_digest)
        })
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<BatchResult, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                num_commits = commits.len(),
                num_fragments = fragments.len(),
                "FsStorage::save_batch"
            );

            Storage::<Sendable>::save_sedimentree_id(self, sedimentree_id).await?;

            let mut commit_digests = Vec::with_capacity(commits.len());
            let mut fragment_digests = Vec::with_capacity(fragments.len());

            for (commit, blob) in commits {
                Storage::<Sendable>::save_blob(self, sedimentree_id, blob).await?;
                let digest =
                    Storage::<Sendable>::save_loose_commit(self, sedimentree_id, commit).await?;
                commit_digests.push(digest);
            }

            for (fragment, blob) in fragments {
                Storage::<Sendable>::save_blob(self, sedimentree_id, blob).await?;
                let digest =
                    Storage::<Sendable>::save_fragment(self, sedimentree_id, fragment).await?;
                fragment_digests.push(digest);
            }

            Ok(BatchResult {
                commit_digests,
                fragment_digests,
            })
        })
    }
}

impl Storage<Local> for FsStorage {
    type Error = FsStorageError;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_sedimentree_id(
            self,
            sedimentree_id,
        ))
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_sedimentree_id(
            self,
            sedimentree_id,
        ))
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_all_sedimentree_ids(self))
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> <Local as FutureForm>::Future<'_, Result<Digest<LooseCommit>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_loose_commit(
            self,
            sedimentree_id,
            loose_commit,
        ))
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<Signed<LooseCommit>>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_loose_commit(
            self,
            sedimentree_id,
            digest,
        ))
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::list_commit_digests(
            self,
            sedimentree_id,
        ))
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<
        '_,
        Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>,
    > {
        Local::from_future(<Self as Storage<Sendable>>::load_loose_commits(
            self,
            sedimentree_id,
        ))
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_loose_commit(
            self,
            sedimentree_id,
            digest,
        ))
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_loose_commits(
            self,
            sedimentree_id,
        ))
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> <Local as FutureForm>::Future<'_, Result<Digest<Fragment>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_fragment(
            self,
            sedimentree_id,
            fragment,
        ))
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<Signed<Fragment>>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_fragment(
            self,
            sedimentree_id,
            digest,
        ))
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::list_fragment_digests(
            self,
            sedimentree_id,
        ))
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<
        '_,
        Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>,
    > {
        Local::from_future(<Self as Storage<Sendable>>::load_fragments(
            self,
            sedimentree_id,
        ))
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_fragment(
            self,
            sedimentree_id,
            digest,
        ))
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_fragments(
            self,
            sedimentree_id,
        ))
    }

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> <Local as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_blob(
            self,
            sedimentree_id,
            blob,
        ))
    }

    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<Blob>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_blob(
            self,
            sedimentree_id,
            blob_digest,
        ))
    }

    fn load_blobs(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digests: &[Digest<Blob>],
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_blobs(
            self,
            sedimentree_id,
            blob_digests,
        ))
    }

    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_blob(
            self,
            sedimentree_id,
            blob_digest,
        ))
    }

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> <Local as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_commit_with_blob(
            self,
            sedimentree_id,
            commit,
            blob,
        ))
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> <Local as FutureForm>::Future<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_fragment_with_blob(
            self,
            sedimentree_id,
            fragment,
            blob,
        ))
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> <Local as FutureForm>::Future<'_, Result<BatchResult, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_batch(
            self,
            sedimentree_id,
            commits,
            fragments,
        ))
    }
}
