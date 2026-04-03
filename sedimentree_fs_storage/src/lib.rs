//! Filesystem-based storage for Sedimentree.
//!
//! This crate provides [`FsStorage`], a filesystem storage implementation
//! that implements the [`Storage`] trait from `subduction_core`.
//!
//! Both commits and fragments are content-addressed internally: the CAS
//! digest is used for file naming within identity subdirectories. The
//! trait surface uses [`CommitId`] and [`FragmentId`] for lookup.
//!
//! # Storage Layout
//!
//! Commits and fragments are stored together with their blobs:
//!
//! ```text
//! root/
//! └── trees/
//!     └── {sedimentree_id_hex}/
//!         ├── commits/
//!         │   └── {commit_id_hex}/
//!         │       ├── {digest_hex}.meta   ← Signed<LooseCommit> bytes
//!         │       └── {digest_hex}.blob   ← Blob bytes
//!         └── fragments/
//!             └── {fragment_id_hex}/
//!                 ├── {digest_hex}.meta   ← Signed<Fragment> bytes
//!                 └── {digest_hex}.blob   ← Blob bytes
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
    blob::Blob,
    codec::error::DecodeError,
    collections::Set,
    crypto::digest::Digest,
    fragment::{Fragment, id::FragmentId},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

/// Errors that can occur during filesystem storage operations.
#[derive(Debug, Error)]
pub enum FsStorageError {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Decoding error.
    #[error(transparent)]
    Decode(#[from] DecodeError),

    /// Signed data is too short to be valid — refusing to write corrupt data.
    #[error("signed data too short: have {have} bytes, need at least {need} bytes")]
    SignedDataTooShort {
        /// Actual size of the signed data.
        have: usize,
        /// Minimum expected size.
        need: usize,
    },
}

/// Filesystem-based storage backend.
///
/// Uses a CAS layout with compound storage (commits/fragments stored with their blobs):
/// ```text
/// root/
/// └── trees/
///     └── {sedimentree_id_hex}/
///         ├── commits/
///         │   └── {commit_id_hex}/
///         │       ├── {digest_hex}.meta   ← Signed<LooseCommit>
///         │       └── {digest_hex}.blob   ← Blob
///         └── fragments/
///             └── {fragment_id_hex}/
///                 ├── {digest_hex}.meta   ← Signed<Fragment>
///                 └── {digest_hex}.blob   ← Blob
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

    fn commit_id_dir(&self, id: SedimentreeId, commit_id: CommitId) -> PathBuf {
        self.commits_dir(id).join(hex::encode(commit_id.as_bytes()))
    }

    fn fragment_id_dir(&self, id: SedimentreeId, fragment_id: FragmentId) -> PathBuf {
        self.fragments_dir(id)
            .join(hex::encode(fragment_id.head().as_bytes()))
    }

    fn commit_meta_path(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
        digest: Digest<LooseCommit>,
    ) -> PathBuf {
        self.commit_id_dir(id, commit_id)
            .join(format!("{}.meta", hex::encode(digest.as_bytes())))
    }

    fn commit_blob_path(
        &self,
        id: SedimentreeId,
        commit_id: CommitId,
        digest: Digest<LooseCommit>,
    ) -> PathBuf {
        self.commit_id_dir(id, commit_id)
            .join(format!("{}.blob", hex::encode(digest.as_bytes())))
    }

    fn fragment_meta_path(
        &self,
        id: SedimentreeId,
        fragment_id: FragmentId,
        digest: Digest<Fragment>,
    ) -> PathBuf {
        self.fragment_id_dir(id, fragment_id)
            .join(format!("{}.meta", hex::encode(digest.as_bytes())))
    }

    fn fragment_blob_path(
        &self,
        id: SedimentreeId,
        fragment_id: FragmentId,
        digest: Digest<Fragment>,
    ) -> PathBuf {
        self.fragment_id_dir(id, fragment_id)
            .join(format!("{}.blob", hex::encode(digest.as_bytes())))
    }

    fn parse_commit_id_from_dirname(name: &str) -> Option<CommitId> {
        let bytes = hex::decode(name).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(CommitId::new(arr))
        } else {
            None
        }
    }

    fn parse_fragment_id_from_dirname(name: &str) -> Option<FragmentId> {
        let bytes = hex::decode(name).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(FragmentId::new(CommitId::new(arr)))
        } else {
            None
        }
    }

    /// Read the first `.meta` + `.blob` pair from a directory as a `Signed<T>` + `Blob`.
    async fn read_first_meta_blob_pair(
        dir: &Path,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, FsStorageError> {
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        while let Some(entry) = entries.next_entry().await? {
            if let Ok(name) = entry.file_name().into_string()
                && let Some(stem) = name.strip_suffix(".meta")
            {
                let meta_path = dir.join(&name);
                let blob_name = format!("{stem}.blob");
                let blob_path = dir.join(&blob_name);

                let signed_data = match tokio::fs::read(&meta_path).await {
                    Ok(data) => data,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };

                let blob_data = match tokio::fs::read(&blob_path).await {
                    Ok(data) => data,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e.into()),
                };

                return Ok(Some((signed_data, blob_data)));
            }
        }

        Ok(None)
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

    // ==================== Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let commit_id = verified.payload().head();
            let digest = Digest::hash(verified.payload());
            tracing::debug!(
                ?sedimentree_id,
                ?commit_id,
                ?digest,
                "FsStorage::save_loose_commit"
            );

            let id_dir = self.commit_id_dir(sedimentree_id, commit_id);
            let meta_path = self.commit_meta_path(sedimentree_id, commit_id, digest);
            let blob_path = self.commit_blob_path(sedimentree_id, commit_id, digest);

            // CAS: skip if already exists
            if tokio::fs::try_exists(&meta_path).await.unwrap_or(false) {
                return Ok(());
            }

            tokio::fs::create_dir_all(&id_dir).await?;

            // Validate signed data before writing
            let signed_data = verified.signed().as_bytes().to_vec();
            let min_size =
                <LooseCommit as sedimentree_core::codec::decode::DecodeFields>::MIN_SIGNED_SIZE;
            if signed_data.len() < min_size {
                tracing::error!(
                    ?sedimentree_id,
                    ?digest,
                    have = signed_data.len(),
                    need = min_size,
                    "refusing to write undersized LooseCommit .meta file"
                );
                return Err(FsStorageError::SignedDataTooShort {
                    have: signed_data.len(),
                    need: min_size,
                });
            }

            let blob_data = verified.blob().contents().clone();
            let blob_temp = blob_path.with_extension("blob.tmp");
            let meta_temp = meta_path.with_extension("meta.tmp");

            tokio::fs::write(&blob_temp, &blob_data).await?;
            tokio::fs::write(&meta_temp, &signed_data).await?;
            tokio::fs::rename(&blob_temp, &blob_path).await?;
            tokio::fs::rename(&meta_temp, &meta_path).await?;

            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::list_commit_ids");

            let commits_dir = self.commits_dir(sedimentree_id);
            let mut ids = Set::new();

            let mut entries = match tokio::fs::read_dir(&commits_dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && let Some(commit_id) = Self::parse_commit_id_from_dirname(&name)
                {
                    ids.insert(commit_id);
                }
            }

            Ok(ids)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?commit_id, "FsStorage::load_loose_commit");

            let id_dir = self.commit_id_dir(sedimentree_id, commit_id);

            match Self::read_first_meta_blob_pair(&id_dir).await? {
                Some((signed_data, blob_data)) => {
                    let signed = Signed::try_decode(signed_data)?;
                    let blob = Blob::new(blob_data);
                    Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
                }
                None => Ok(None),
            }
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?commit_id,
                "FsStorage::delete_loose_commit"
            );

            let id_dir = self.commit_id_dir(sedimentree_id, commit_id);
            if let Err(e) = tokio::fs::remove_dir_all(&id_dir).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            Ok(())
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::load_loose_commits");

            let commits_dir = self.commits_dir(sedimentree_id);
            let mut results = Vec::new();

            let mut entries = match tokio::fs::read_dir(&commits_dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(results),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && Self::parse_commit_id_from_dirname(&name).is_some()
                {
                    let subdir = commits_dir.join(&name);
                    match Self::read_first_meta_blob_pair(&subdir).await {
                        Ok(Some((signed_data, blob_data))) => {
                            let signed = match Signed::try_decode(signed_data) {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!(
                                        ?sedimentree_id,
                                        dir = %name,
                                        "skipping corrupt loose commit dir: {e}"
                                    );
                                    continue;
                                }
                            };
                            let blob = Blob::new(blob_data);
                            match VerifiedMeta::try_from_trusted(signed, blob) {
                                Ok(verified) => results.push(verified),
                                Err(e) => {
                                    tracing::warn!(
                                        ?sedimentree_id,
                                        dir = %name,
                                        "skipping corrupt loose commit dir: {e}"
                                    );
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(FsStorageError::Decode(e)) => {
                            tracing::warn!(
                                ?sedimentree_id,
                                dir = %name,
                                "skipping corrupt loose commit dir: {e}"
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(results)
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

            // Recreate the empty directory
            tokio::fs::create_dir_all(&commits_dir).await?;

            Ok(())
        })
    }

    // ==================== Fragments (compound with blob) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let fragment_id = verified.payload().fragment_id();
            let digest = Digest::hash(verified.payload());
            tracing::debug!(
                ?sedimentree_id,
                ?fragment_id,
                ?digest,
                "FsStorage::save_fragment"
            );

            let id_dir = self.fragment_id_dir(sedimentree_id, fragment_id);
            let meta_path = self.fragment_meta_path(sedimentree_id, fragment_id, digest);
            let blob_path = self.fragment_blob_path(sedimentree_id, fragment_id, digest);

            // Skip if already exists (CAS)
            if tokio::fs::try_exists(&meta_path).await.unwrap_or(false) {
                return Ok(());
            }

            tokio::fs::create_dir_all(&id_dir).await?;

            // Validate signed data before writing
            let signed_data = verified.signed().as_bytes().to_vec();
            let min_size =
                <Fragment as sedimentree_core::codec::decode::DecodeFields>::MIN_SIGNED_SIZE;
            if signed_data.len() < min_size {
                tracing::error!(
                    ?sedimentree_id,
                    ?digest,
                    have = signed_data.len(),
                    need = min_size,
                    "refusing to write undersized Fragment .meta file"
                );
                return Err(FsStorageError::SignedDataTooShort {
                    have: signed_data.len(),
                    need: min_size,
                });
            }

            let blob_data = verified.blob().contents().clone();
            let blob_temp = blob_path.with_extension("blob.tmp");
            let meta_temp = meta_path.with_extension("meta.tmp");

            tokio::fs::write(&blob_temp, &blob_data).await?;
            tokio::fs::write(&meta_temp, &signed_data).await?;
            tokio::fs::rename(&blob_temp, &blob_path).await?;
            tokio::fs::rename(&meta_temp, &meta_path).await?;

            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_id: FragmentId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?fragment_id, "FsStorage::load_fragment");

            let id_dir = self.fragment_id_dir(sedimentree_id, fragment_id);

            match Self::read_first_meta_blob_pair(&id_dir).await? {
                Some((signed_data, blob_data)) => {
                    let signed = match Signed::try_decode(signed_data) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(
                                ?sedimentree_id,
                                ?fragment_id,
                                "corrupt .meta file for Fragment: {e}"
                            );
                            return Err(FsStorageError::from(e));
                        }
                    };
                    let blob = Blob::new(blob_data);
                    Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
                }
                None => Ok(None),
            }
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<FragmentId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::list_fragment_ids");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            let mut ids = Set::new();

            let mut entries = match tokio::fs::read_dir(&fragments_dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && let Some(fragment_id) = Self::parse_fragment_id_from_dirname(&name)
                {
                    ids.insert(fragment_id);
                }
            }

            Ok(ids)
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, "FsStorage::load_fragments");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            let mut results = Vec::new();

            let mut entries = match tokio::fs::read_dir(&fragments_dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(results),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && let Some(fragment_id) = Self::parse_fragment_id_from_dirname(&name)
                {
                    match Storage::<Sendable>::load_fragment(self, sedimentree_id, fragment_id)
                        .await
                    {
                        Ok(Some(verified)) => results.push(verified),
                        Ok(None) => {}
                        Err(FsStorageError::Decode(e)) => {
                            tracing::warn!(
                                ?sedimentree_id,
                                ?fragment_id,
                                "skipping corrupt fragment dir: {e}"
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(results)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_id: FragmentId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?fragment_id, "FsStorage::delete_fragment");

            let id_dir = self.fragment_id_dir(sedimentree_id, fragment_id);
            if let Err(e) = tokio::fs::remove_dir_all(&id_dir).await
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

            // Recreate the empty directory
            tokio::fs::create_dir_all(&fragments_dir).await?;

            Ok(())
        })
    }

    // ==================== Batch Operations ====================

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<usize, Self::Error>> {
        Sendable::from_future(async move {
            let num_commits = commits.len();
            let num_fragments = fragments.len();
            tracing::debug!(
                ?sedimentree_id,
                num_commits,
                num_fragments,
                "FsStorage::save_batch"
            );

            Storage::<Sendable>::save_sedimentree_id(self, sedimentree_id).await?;

            for verified in commits {
                Storage::<Sendable>::save_loose_commit(self, sedimentree_id, verified).await?;
            }

            for verified in fragments {
                Storage::<Sendable>::save_fragment(self, sedimentree_id, verified).await?;
            }

            Ok(num_commits + num_fragments)
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
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_loose_commit(
            self,
            sedimentree_id,
            verified,
        ))
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::list_commit_ids(
            self,
            sedimentree_id,
        ))
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Local::from_future(<Self as Storage<Sendable>>::load_loose_commit(
            self,
            sedimentree_id,
            commit_id,
        ))
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_loose_commit(
            self,
            sedimentree_id,
            commit_id,
        ))
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Local::from_future(<Self as Storage<Sendable>>::load_loose_commits(
            self,
            sedimentree_id,
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
        verified: VerifiedMeta<Fragment>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_fragment(
            self,
            sedimentree_id,
            verified,
        ))
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_id: FragmentId,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Local::from_future(<Self as Storage<Sendable>>::load_fragment(
            self,
            sedimentree_id,
            fragment_id,
        ))
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<FragmentId>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::list_fragment_ids(
            self,
            sedimentree_id,
        ))
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::load_fragments(
            self,
            sedimentree_id,
        ))
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_id: FragmentId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_fragment(
            self,
            sedimentree_id,
            fragment_id,
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

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> <Local as FutureForm>::Future<'_, Result<usize, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::save_batch(
            self,
            sedimentree_id,
            commits,
            fragments,
        ))
    }
}
