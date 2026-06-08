//! Filesystem-based storage for Sedimentree.
//!
//! This crate provides [`FsStorage`], a filesystem storage implementation
//! that implements the [`Storage`] trait from `subduction_core`.
//!
//! Both commits and fragments are content-addressed internally: the CAS
//! digest is used for file naming within identity subdirectories. The
//! trait surface uses [`CommitId`] for lookup.
//!
//! # Storage Layout
//!
//! Trees are sharded one level deep under `trees/` by a hex bucket taken from
//! the first two bytes of the `SedimentreeId` (65,536 buckets, `0000`..`ffff`).
//! The leaf directory is the hex of the remaining 30 bytes, so the full id is
//! never stored redundantly — it is reconstructed by concatenating the bucket
//! and leaf names. Because the id is a cryptographic hash, its prefix bytes are
//! uniformly distributed and buckets fill evenly with no extra hashing.
//!
//! Commits and fragments are stored together with their blobs:
//!
//! ```text
//! root/
//! └── trees/
//!     └── {id_prefix_hex}/                   ← bucket: first 2 bytes (4 hex chars)
//!         └── {id_remainder_hex}/            ← leaf: remaining 30 bytes (60 hex chars)
//!             ├── commits/
//!             │   └── {commit_id_hex}/
//!             │       ├── {digest_hex}.meta   ← Signed<LooseCommit> bytes
//!             │       └── {digest_hex}.blob   ← Blob bytes
//!             └── fragments/
//!                 └── {fragment_head_hex}/
//!                     ├── {digest_hex}.meta   ← Signed<Fragment> bytes
//!                     └── {digest_hex}.blob   ← Blob bytes
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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

/// Process-wide counter for distinguishing concurrent writers that target
/// the same content-addressed path.
static TMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// One compound item (meta + blob) to persist, with its target paths already
/// resolved. Carried into a blocking closure so the whole write sequence runs
/// in a single `spawn_blocking` hop.
struct PendingWrite {
    id_dir: PathBuf,
    meta_path: PathBuf,
    blob_path: PathBuf,
    signed_data: Vec<u8>,
    blob_data: Vec<u8>,
}

/// Synchronously persist one compound item using the durable temp-then-rename
/// dance. CAS: a no-op if the `.meta` already exists. Must be called from a
/// blocking context (e.g. inside `spawn_blocking`).
fn write_compound_sync(item: &PendingWrite) -> Result<(), FsStorageError> {
    // CAS: skip if already exists.
    if std::fs::metadata(&item.meta_path).is_ok() {
        return Ok(());
    }

    std::fs::create_dir_all(&item.id_dir)?;

    let nonce = TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let blob_temp = item.blob_path.with_extension(format!("{nonce}.blob.tmp"));
    let meta_temp = item.meta_path.with_extension(format!("{nonce}.meta.tmp"));

    std::fs::write(&blob_temp, &item.blob_data)?;
    std::fs::write(&meta_temp, &item.signed_data)?;
    std::fs::rename(&blob_temp, &item.blob_path)?;
    std::fs::rename(&meta_temp, &item.meta_path)?;

    Ok(())
}

/// The undecoded `.meta` + `.blob` byte pair read from one identity directory.
type MetaBlobPair = (Vec<u8>, Vec<u8>);

/// One raw on-disk compound item: its identity-dir name plus the undecoded
/// `.meta` and `.blob` bytes. Decoding is deferred to async land so the
/// blocking closure stays pure I/O.
type RawCompound = (String, Vec<u8>, Vec<u8>);

/// Synchronously walk every `<id>/` subdirectory of `parent`, reading the first
/// `.meta` + `.blob` pair from each. Returns the raw, undecoded bytes for all
/// items in a single blocking-pool hop (vs. one hop per `tokio::fs` call).
///
/// `parent` absent → empty result. Subdirectories whose name isn't a valid id,
/// or that hold no complete pair, are skipped. Must be called from a blocking
/// context.
fn read_all_compound_sync(parent: &Path) -> Result<Vec<RawCompound>, FsStorageError> {
    let entries = match std::fs::read_dir(parent) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };

        if FsStorage::parse_commit_id_from_dirname(&name).is_none() {
            continue;
        }

        if let Some((signed_data, blob_data)) = read_first_meta_blob_pair_sync(&entry.path())? {
            out.push((name, signed_data, blob_data));
        }
    }

    Ok(out)
}

/// Synchronously read the first `.meta` + `.blob` pair from `dir`. Returns
/// `None` if the directory is absent or holds no complete pair. Must be called
/// from a blocking context.
fn read_first_meta_blob_pair_sync(dir: &Path) -> Result<Option<MetaBlobPair>, FsStorageError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    for entry in entries {
        let entry = entry?;
        if let Ok(name) = entry.file_name().into_string()
            && let Some(stem) = name.strip_suffix(".meta")
        {
            let meta_path = dir.join(&name);
            let blob_path = dir.join(format!("{stem}.blob"));

            let signed_data = match std::fs::read(&meta_path) {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };

            let blob_data = match std::fs::read(&blob_path) {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };

            return Ok(Some((signed_data, blob_data)));
        }
    }

    Ok(None)
}

/// Number of leading [`SedimentreeId`] bytes used as the on-disk bucket name
/// under `trees/`. Two bytes give 65,536 buckets (`0000`..`ffff`).
///
/// Because a `SedimentreeId` is a cryptographic hash, its leading bytes are
/// uniformly distributed, so this prefix shards trees evenly across buckets
/// with no additional hashing. The remaining bytes form the leaf directory
/// name, so the full id is never stored redundantly.
const TREE_BUCKET_PREFIX_BYTES: usize = 2;

/// Errors that can occur during filesystem storage operations.
#[derive(Debug, Error)]
pub enum FsStorageError {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Decoding error.
    #[error(transparent)]
    Decode(#[from] DecodeError),

    /// A blocking storage task panicked or was cancelled.
    #[error("blocking storage task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

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
/// Uses a CAS layout with compound storage (commits/fragments stored with their
/// blobs), sharded one level deep by a hex bucket from the first two bytes of
/// the `SedimentreeId`:
/// ```text
/// root/
/// └── trees/
///     └── {id_prefix_hex}/                   ← bucket: first 2 bytes (4 hex chars)
///         └── {id_remainder_hex}/            ← leaf: remaining 30 bytes (60 hex chars)
///             ├── commits/
///             │   └── {commit_id_hex}/
///             │       ├── {digest_hex}.meta   ← Signed<LooseCommit>
///             │       └── {digest_hex}.blob   ← Blob
///             └── fragments/
///                 └── {fragment_head_hex}/
///                     ├── {digest_hex}.meta   ← Signed<Fragment>
///                     └── {digest_hex}.blob   ← Blob
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

        // Two-level walk: trees/{bucket}/{leaf}. The bucket is the first
        // `TREE_BUCKET_PREFIX_BYTES` of the id (hex-encoded), the leaf is the
        // remaining bytes. A full id is reconstructed by concatenating the two.
        let Ok(buckets) = std::fs::read_dir(&trees_dir) else {
            return ids;
        };

        for bucket in buckets.flatten() {
            let Ok(bucket_name) = bucket.file_name().into_string() else {
                continue;
            };

            // Bucket dir names are exactly the hex of the prefix bytes.
            if bucket_name.len() != TREE_BUCKET_PREFIX_BYTES * 2 {
                continue;
            }

            let Ok(leaves) = std::fs::read_dir(bucket.path()) else {
                continue;
            };

            for leaf in leaves.flatten() {
                if let Ok(leaf_name) = leaf.file_name().into_string()
                    && let Some(id) = Self::reconstruct_id(&bucket_name, &leaf_name)
                {
                    ids.insert(id);
                }
            }
        }

        ids
    }

    /// Reconstruct a [`SedimentreeId`] from its on-disk bucket + leaf names.
    ///
    /// The bucket holds the hex of the first `TREE_BUCKET_PREFIX_BYTES` of the
    /// id; the leaf holds the hex of the remaining bytes. Concatenating them
    /// yields the full 64-char hex id. Returns `None` for any name that does
    /// not decode to exactly 32 bytes.
    fn reconstruct_id(bucket_name: &str, leaf_name: &str) -> Option<SedimentreeId> {
        let mut full_hex = String::with_capacity(bucket_name.len() + leaf_name.len());
        full_hex.push_str(bucket_name);
        full_hex.push_str(leaf_name);

        let bytes = hex::decode(&full_hex).ok()?;
        let arr: [u8; 32] = bytes.try_into().ok()?;
        Some(SedimentreeId::new(arr))
    }

    fn tree_path(&self, id: SedimentreeId) -> PathBuf {
        // Shard into one level of hex buckets: trees/{bucket}/{leaf}. The id is
        // already a cryptographic hash, so its prefix bytes are uniformly
        // distributed — slicing the prefix gives an even fan-out for free with
        // no additional hashing. See the module docs for the rationale.
        let (prefix, rest) = id.as_bytes().split_at(TREE_BUCKET_PREFIX_BYTES);
        let bucket = hex::encode(prefix);
        let leaf = hex::encode(rest);
        self.root.join("trees").join(bucket).join(leaf)
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

    fn fragment_id_dir(&self, id: SedimentreeId, fragment_head: CommitId) -> PathBuf {
        self.fragments_dir(id)
            .join(hex::encode(fragment_head.as_bytes()))
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
        fragment_head: CommitId,
        digest: Digest<Fragment>,
    ) -> PathBuf {
        self.fragment_id_dir(id, fragment_head)
            .join(format!("{}.meta", hex::encode(digest.as_bytes())))
    }

    fn fragment_blob_path(
        &self,
        id: SedimentreeId,
        fragment_head: CommitId,
        digest: Digest<Fragment>,
    ) -> PathBuf {
        self.fragment_id_dir(id, fragment_head)
            .join(format!("{}.blob", hex::encode(digest.as_bytes())))
    }

    /// Parse a hex-encoded directory name into a [`CommitId`].
    ///
    /// Used for both commit and fragment identity subdirectories, since
    /// both are keyed by [`CommitId`] (fragments use their head commit).
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

    /// Read the first `.meta` + `.blob` pair from a directory as a `Signed<T>` + `Blob`.
    async fn read_first_meta_blob_pair(
        dir: &Path,
    ) -> Result<Option<MetaBlobPair>, FsStorageError> {
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

    /// Resolve paths and validate a loose commit into a [`PendingWrite`].
    ///
    /// Validation (the undersized-`.meta` guard) is pure CPU and runs here,
    /// off the filesystem, so the blocking closure only does I/O.
    fn build_commit_write(
        &self,
        sedimentree_id: SedimentreeId,
        verified: &VerifiedMeta<LooseCommit>,
    ) -> Result<PendingWrite, FsStorageError> {
        let commit_id = verified.payload().head();
        let digest = Digest::hash(verified.payload());
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

        Ok(PendingWrite {
            id_dir: self.commit_id_dir(sedimentree_id, commit_id),
            meta_path: self.commit_meta_path(sedimentree_id, commit_id, digest),
            blob_path: self.commit_blob_path(sedimentree_id, commit_id, digest),
            signed_data,
            blob_data: verified.blob().contents().clone(),
        })
    }

    /// Resolve paths and validate a fragment into a [`PendingWrite`].
    fn build_fragment_write(
        &self,
        sedimentree_id: SedimentreeId,
        verified: &VerifiedMeta<Fragment>,
    ) -> Result<PendingWrite, FsStorageError> {
        let fragment_head = verified.payload().head();
        let digest = Digest::hash(verified.payload());
        let signed_data = verified.signed().as_bytes().to_vec();
        let min_size = <Fragment as sedimentree_core::codec::decode::DecodeFields>::MIN_SIGNED_SIZE;
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

        Ok(PendingWrite {
            id_dir: self.fragment_id_dir(sedimentree_id, fragment_head),
            meta_path: self.fragment_meta_path(sedimentree_id, fragment_head, digest),
            blob_path: self.fragment_blob_path(sedimentree_id, fragment_head, digest),
            signed_data,
            blob_data: verified.blob().contents().clone(),
        })
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
            tracing::trace!(?sedimentree_id, "FsStorage::save_sedimentree_id");

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
            tracing::trace!(?sedimentree_id, "FsStorage::delete_sedimentree_id");

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
            tracing::trace!("FsStorage::load_all_sedimentree_ids");
            Ok(self.ids_cache.lock().await.clone())
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<bool, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "FsStorage::contains_sedimentree_id");
            // Single-key check against the in-memory id cache (no directory scan).
            Ok(self.ids_cache.lock().await.contains(&sedimentree_id))
        })
    }

    // ==================== Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "FsStorage::save_loose_commit");

            // Validate + resolve paths off the filesystem, then collapse the
            // CAS check + mkdir + 2 writes + 2 renames into a single
            // blocking-pool hop. With `tokio::fs` each call is its own
            // `spawn_blocking` round-trip, so a 6-step save pays the scheduling
            // jitter 6×; doing the whole sequence in one closure pays it once,
            // tightening the latency tail under concurrent load.
            let item = self.build_commit_write(sedimentree_id, &verified)?;
            tokio::task::spawn_blocking(move || write_compound_sync(&item)).await??;

            Ok(())
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "FsStorage::list_commit_ids");

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
            tracing::trace!(?sedimentree_id, ?commit_id, "FsStorage::load_loose_commit");

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
            tracing::trace!(
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
            tracing::trace!(?sedimentree_id, "FsStorage::load_loose_commits");

            // Read every commit dir's meta+blob bytes in a single blocking hop,
            // then decode in async land. Previously this was N+1 `read_dir`s
            // plus 2N sequential `tokio::fs::read`s — each its own blocking-pool
            // round-trip — on the hydration hot path.
            let commits_dir = self.commits_dir(sedimentree_id);
            let raw =
                tokio::task::spawn_blocking(move || read_all_compound_sync(&commits_dir)).await??;

            let mut results = Vec::with_capacity(raw.len());
            for (name, signed_data, blob_data) in raw {
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

            Ok(results)
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "FsStorage::delete_loose_commits");

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
            tracing::trace!(?sedimentree_id, "FsStorage::save_fragment");

            // Single blocking-pool hop for the whole CAS + write + rename
            // sequence; see `save_loose_commit` for the rationale.
            let item = self.build_fragment_write(sedimentree_id, &verified)?;
            tokio::task::spawn_blocking(move || write_compound_sync(&item)).await??;

            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, ?fragment_head, "FsStorage::load_fragment");

            let id_dir = self.fragment_id_dir(sedimentree_id, fragment_head);

            match Self::read_first_meta_blob_pair(&id_dir).await? {
                Some((signed_data, blob_data)) => {
                    let signed = match Signed::try_decode(signed_data) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(
                                ?sedimentree_id,
                                ?fragment_head,
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "FsStorage::list_fragment_ids");

            let fragments_dir = self.fragments_dir(sedimentree_id);
            let mut ids = Set::new();

            let mut entries = match tokio::fs::read_dir(&fragments_dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
                Err(e) => return Err(e.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                if let Ok(name) = entry.file_name().into_string()
                    && let Some(fragment_id) = Self::parse_commit_id_from_dirname(&name)
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
            tracing::trace!(?sedimentree_id, "FsStorage::load_fragments");

            // One blocking hop for the whole directory walk, then decode in
            // async land. Previously this re-entered the full `load_fragment`
            // trait method per fragment — a fresh `read_dir` + 2 reads each,
            // all sequential blocking-pool round-trips.
            let fragments_dir = self.fragments_dir(sedimentree_id);
            let raw =
                tokio::task::spawn_blocking(move || read_all_compound_sync(&fragments_dir)).await??;

            let mut results = Vec::with_capacity(raw.len());
            for (name, signed_data, blob_data) in raw {
                let signed = match Signed::try_decode(signed_data) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            ?sedimentree_id,
                            dir = %name,
                            "skipping corrupt fragment dir: {e}"
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
                            "skipping corrupt fragment dir: {e}"
                        );
                    }
                }
            }

            Ok(results)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(
                ?sedimentree_id,
                ?fragment_head,
                "FsStorage::delete_fragment"
            );

            let id_dir = self.fragment_id_dir(sedimentree_id, fragment_head);
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
            tracing::trace!(?sedimentree_id, "FsStorage::delete_fragments");

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
            tracing::trace!(
                ?sedimentree_id,
                num_commits,
                num_fragments,
                "FsStorage::save_batch"
            );

            Storage::<Sendable>::save_sedimentree_id(self, sedimentree_id).await?;

            // Validate + resolve every item off the filesystem, then persist the
            // whole batch in a single blocking-pool hop. Previously each item
            // was its own `spawn_blocking` save, so an N-item batch paid the
            // scheduling round-trip N times back-to-back; now it pays it once.
            let mut items = Vec::with_capacity(num_commits + num_fragments);
            for verified in &commits {
                items.push(self.build_commit_write(sedimentree_id, verified)?);
            }
            for verified in &fragments {
                items.push(self.build_fragment_write(sedimentree_id, verified)?);
            }

            tokio::task::spawn_blocking(move || -> Result<(), FsStorageError> {
                for item in &items {
                    write_compound_sync(item)?;
                }
                Ok(())
            })
            .await??;

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

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<bool, Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::contains_sedimentree_id(
            self,
            sedimentree_id,
        ))
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
        fragment_head: CommitId,
    ) -> <Local as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Local::from_future(<Self as Storage<Sendable>>::load_fragment(
            self,
            sedimentree_id,
            fragment_head,
        ))
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Local as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
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
        fragment_head: CommitId,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Local::from_future(<Self as Storage<Sendable>>::delete_fragment(
            self,
            sedimentree_id,
            fragment_head,
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
