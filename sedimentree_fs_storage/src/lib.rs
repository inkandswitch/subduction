//! Filesystem-based storage for Sedimentree.
//!
//! This crate provides [`FsStorage`], a content-addressed filesystem storage
//! implementation that implements the [`Storage`] trait from `subduction_core`.
//!
//! # Storage Layout
//!
//! With compound storage, commits and fragments are stored together with their blobs:
//!
//! ```text
//! root/
//! └── trees/
//!     └── {sedimentree_id_hex}/
//!         ├── commits/
//!         │   ├── {digest_hex}.signed   ← Signed<LooseCommit> bytes
//!         │   └── {digest_hex}.blob     ← Blob bytes
//!         └── fragments/
//!             ├── {digest_hex}.signed   ← Signed<Fragment> bytes
//!             └── {digest_hex}.blob     ← Blob bytes
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
    fragment::Fragment,
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
///         │   ├── {digest_hex}.signed   ← Signed<LooseCommit>
///         │   └── {digest_hex}.blob     ← Blob
///         └── fragments/
///             ├── {digest_hex}.signed   ← Signed<Fragment>
///             └── {digest_hex}.blob     ← Blob
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

    fn commit_signed_path(&self, id: SedimentreeId, commit_id: CommitId) -> PathBuf {
        self.commits_dir(id)
            .join(format!("{}.signed", hex::encode(commit_id.as_bytes())))
    }

    fn commit_blob_path(&self, id: SedimentreeId, commit_id: CommitId) -> PathBuf {
        self.commits_dir(id)
            .join(format!("{}.blob", hex::encode(commit_id.as_bytes())))
    }

    fn fragment_signed_path(&self, id: SedimentreeId, digest: Digest<Fragment>) -> PathBuf {
        self.fragments_dir(id)
            .join(format!("{}.signed", hex::encode(digest.as_bytes())))
    }

    fn fragment_blob_path(&self, id: SedimentreeId, digest: Digest<Fragment>) -> PathBuf {
        self.fragments_dir(id)
            .join(format!("{}.blob", hex::encode(digest.as_bytes())))
    }

    fn parse_commit_id_from_filename(name: &str) -> Option<CommitId> {
        let hex_str = name.strip_suffix(".signed")?;
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(CommitId::new(arr))
        } else {
            None
        }
    }

    fn parse_fragment_digest_from_filename(name: &str) -> Option<Digest<Fragment>> {
        let hex_str = name.strip_suffix(".signed")?;
        let bytes = hex::decode(hex_str).ok()?;
        if bytes.len() == 32 {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(Digest::force_from_bytes(arr))
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

    // ==================== Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            let commit_id = verified.payload().head();
            tracing::debug!(?sedimentree_id, ?commit_id, "FsStorage::save_loose_commit");

            let signed_path = self.commit_signed_path(sedimentree_id, commit_id);
            let blob_path = self.commit_blob_path(sedimentree_id, commit_id);

            // Skip if already exists (CAS)
            if tokio::fs::try_exists(&signed_path).await.unwrap_or(false) {
                return Ok(());
            }

            tokio::fs::create_dir_all(self.commits_dir(sedimentree_id)).await?;

            // Validate signed data before writing
            let signed_data = verified.signed().as_bytes().to_vec();
            let min_size =
                <LooseCommit as sedimentree_core::codec::decode::DecodeFields>::MIN_SIGNED_SIZE;
            if signed_data.len() < min_size {
                tracing::error!(
                    ?sedimentree_id,
                    ?commit_id,
                    have = signed_data.len(),
                    need = min_size,
                    "refusing to write undersized LooseCommit .signed file"
                );
                return Err(FsStorageError::SignedDataTooShort {
                    have: signed_data.len(),
                    need: min_size,
                });
            }

            // Write both temp files first, then rename both.
            // The `.signed` rename is last — it's the CAS marker.
            // A crash before the final rename leaves either:
            //   - orphaned .tmp files (harmless, overwritten on re-save)
            //   - .blob committed but no .signed (CAS allows re-save)
            let blob_data = verified.blob().contents().clone();
            let blob_temp = blob_path.with_extension("blob.tmp");
            let signed_temp = signed_path.with_extension("signed.tmp");

            tokio::fs::write(&blob_temp, &blob_data).await?;
            tokio::fs::write(&signed_temp, &signed_data).await?;
            tokio::fs::rename(&blob_temp, &blob_path).await?;
            tokio::fs::rename(&signed_temp, &signed_path).await?;

            Ok(())
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

            let signed_path = self.commit_signed_path(sedimentree_id, commit_id);
            let blob_path = self.commit_blob_path(sedimentree_id, commit_id);

            // Load signed data
            let signed_data = match tokio::fs::read(&signed_path).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Load blob data
            let blob_data = match tokio::fs::read(&blob_path).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            let signed = match Signed::try_decode(signed_data) {
                Ok(s) => s,
                Err(e) => {
                    let raw = tokio::fs::read(&signed_path).await.unwrap_or_default();
                    let hex_prefix: Vec<u8> = raw.iter().take(36).copied().collect();
                    tracing::error!(
                        path = %signed_path.display(),
                        file_size = raw.len(),
                        hex_prefix = hex::encode(&hex_prefix),
                        "corrupt .signed file for LooseCommit: {e}"
                    );
                    return Err(FsStorageError::from(e));
                }
            };
            let blob = Blob::new(blob_data);

            // Reconstruct from trusted storage without re-verification
            Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
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
                    && let Some(commit_id) = Self::parse_commit_id_from_filename(&name)
                {
                    ids.insert(commit_id);
                }
            }

            Ok(ids)
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
                    && let Some(commit_id) = Self::parse_commit_id_from_filename(&name)
                {
                    match Storage::<Sendable>::load_loose_commit(self, sedimentree_id, commit_id)
                        .await
                    {
                        Ok(Some(verified)) => results.push(verified),
                        Ok(None) => {}
                        Err(FsStorageError::Decode(e)) => {
                            tracing::warn!(
                                ?sedimentree_id,
                                ?commit_id,
                                "skipping corrupt loose commit file: {e}"
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(results)
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

            let signed_path = self.commit_signed_path(sedimentree_id, commit_id);
            let blob_path = self.commit_blob_path(sedimentree_id, commit_id);

            // Delete both files (compound deletion)
            if let Err(e) = tokio::fs::remove_file(&signed_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            if let Err(e) = tokio::fs::remove_file(&blob_path).await
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
            let digest = Digest::hash(verified.payload());
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::save_fragment");

            let signed_path = self.fragment_signed_path(sedimentree_id, digest);
            let blob_path = self.fragment_blob_path(sedimentree_id, digest);

            // Skip if already exists (CAS)
            if tokio::fs::try_exists(&signed_path).await.unwrap_or(false) {
                return Ok(());
            }

            tokio::fs::create_dir_all(self.fragments_dir(sedimentree_id)).await?;

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
                    "refusing to write undersized Fragment .signed file"
                );
                return Err(FsStorageError::SignedDataTooShort {
                    have: signed_data.len(),
                    need: min_size,
                });
            }

            // Write both temp files first, then rename both.
            // The `.signed` rename is last — it's the CAS marker.
            let blob_data = verified.blob().contents().clone();
            let blob_temp = blob_path.with_extension("blob.tmp");
            let signed_temp = signed_path.with_extension("signed.tmp");

            tokio::fs::write(&blob_temp, &blob_data).await?;
            tokio::fs::write(&signed_temp, &signed_data).await?;
            tokio::fs::rename(&blob_temp, &blob_path).await?;
            tokio::fs::rename(&signed_temp, &signed_path).await?;

            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::load_fragment");

            let signed_path = self.fragment_signed_path(sedimentree_id, digest);
            let blob_path = self.fragment_blob_path(sedimentree_id, digest);

            // Load signed data
            let signed_data = match tokio::fs::read(&signed_path).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            // Load blob data
            let blob_data = match tokio::fs::read(&blob_path).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };

            let signed = match Signed::try_decode(signed_data) {
                Ok(s) => s,
                Err(e) => {
                    let raw = tokio::fs::read(&signed_path).await.unwrap_or_default();
                    let hex_prefix: Vec<u8> = raw.iter().take(36).copied().collect();
                    tracing::error!(
                        path = %signed_path.display(),
                        file_size = raw.len(),
                        hex_prefix = hex::encode(&hex_prefix),
                        "corrupt .signed file for Fragment: {e}"
                    );
                    return Err(FsStorageError::from(e));
                }
            };
            let blob = Blob::new(blob_data);

            // Reconstruct from trusted storage without re-verification
            Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
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
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(digests),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && let Some(digest) = Self::parse_fragment_digest_from_filename(&name)
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
                    && let Some(digest) = Self::parse_fragment_digest_from_filename(&name)
                {
                    match Storage::<Sendable>::load_fragment(self, sedimentree_id, digest).await {
                        Ok(Some(verified)) => results.push(verified),
                        Ok(None) => {}
                        Err(FsStorageError::Decode(e)) => {
                            tracing::warn!(
                                ?sedimentree_id,
                                ?digest,
                                "skipping corrupt fragment file: {e}"
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
        digest: Digest<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "FsStorage::delete_fragment");

            let signed_path = self.fragment_signed_path(sedimentree_id, digest);
            let blob_path = self.fragment_blob_path(sedimentree_id, digest);

            // Delete both files (compound deletion)
            if let Err(e) = tokio::fs::remove_file(&signed_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }

            if let Err(e) = tokio::fs::remove_file(&blob_path).await
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

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::list_commit_ids(
            self,
            sedimentree_id,
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
        digest: Digest<Fragment>,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
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
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
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
