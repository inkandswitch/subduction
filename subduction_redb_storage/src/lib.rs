//! Hybrid [redb] + filesystem storage for Sedimentree.
//!
//! This crate provides [`RedbStorage`], a storage implementation of the
//! [`Storage`](subduction_core::storage::traits::Storage) trait from
//! `subduction_core` intended as an alternative to `sedimentree_fs_storage`
//! for native servers. Metadata and small blobs live in a transactional redb
//! B+tree; large blobs live as flat content-addressed files beside it. The
//! split captures both measured sweet spots: a B+tree packs small records
//! densely (no per-file block rounding) and scans them fast, while the
//! filesystem streams large values faster and without the ~2x B+tree page
//! amplification.
//!
//! # Layout
//!
//! ```text
//! root/
//! ├── sedimentree.redb           ← all metadata + blobs ≤ inline threshold
//! └── blobs/                     ← blobs > inline threshold (CAS)
//!     └── {hex[0..2]}/           ← 256 buckets by digest prefix
//!         └── {blob_digest_hex}  ← one flat file per blob, deduplicated
//! ```
//!
//! Database tables:
//!
//! ```text
//! trees:     [u8; 32]                                  → ()
//! commits:   [u8; 96] = tree_id ++ commit_id ++ digest → tagged compound value
//! fragments: [u8; 96] = tree_id ++ head_id  ++ digest → tagged compound value
//!
//! tagged compound value:
//!   0x00 ++ meta_len:u32be ++ meta ++ blob    (inline)
//!   0x01 ++ meta                              (external)
//! ```
//!
//! External records store *only* the signed metadata: the blob's digest
//! (and size) already live inside the signed payload's `BlobMeta`, so the
//! file name is derived by decoding the meta rather than duplicating the
//! digest in the value. Bulk loads therefore run in three phases: one
//! blocking hop for the B+tree range scan, decoding in async land, then
//! the external blob files read in parallel chunks on the blocking pool.
//!
//! Keys sort lexicographically, so all items of a tree (or of one
//! commit/fragment identity) are contiguous: bulk loads are a single B+tree
//! range scan, and the Byzantine-equivocation contract (multiple payloads
//! per [`CommitId`](sedimentree_core::loose_commit::id::CommitId) coexist)
//! falls out of the trailing content digest in the key, mirroring the CAS
//! filenames of the filesystem backend.
//!
//! # Durability & crash consistency
//!
//! redb's default durability fsyncs on every transaction commit, so each
//! `save_*` call is durable when it returns and `save_batch` amortizes one
//! fsync across the whole batch. External blob files are written durably
//! (temp file fsynced before rename, bucket directory fsynced after)
//! **before** the referencing database transaction commits: a record in the
//! database always points at a complete blob file. A crash in between leaves
//! at most an *orphan* blob file, which is harmless — files are
//! content-addressed, so a later save of the same blob adopts it.
//!
//! # Garbage collection
//!
//! Deleting records does **not** delete external blob files: blob files are
//! content-addressed and may be shared by multiple records (the same blob
//! saved under different commits or trees), so unreferenced files are left
//! behind rather than risking dangling references. A GC sweep is a future
//! concern; re-saving previously deleted content adopts the existing file.
//!
//! # Example
//!
//! ```no_run
//! use subduction_redb_storage::RedbStorage;
//! use std::path::PathBuf;
//!
//! let storage = RedbStorage::new(PathBuf::from("./data")).expect("failed to open storage");
//! ```
//!
//! [redb]: https://github.com/cberner/redb

#![forbid(unsafe_code)]

mod blob_store;
mod codec;
mod error;
mod insert;
mod inspect;
mod key;
mod scan;
mod storage;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use redb::{Database, TableDefinition};

use crate::blob_store::fsync_dir_sync;

pub use error::RedbStorageError;
pub use inspect::{HeadEntry, HeadKind, HeadLocation, StoreStats, TreeHeads, TreeStats};

/// Registered sedimentree ids.
pub(crate) const TREES: TableDefinition<'_, &[u8; 32], ()> = TableDefinition::new("trees");

/// Loose commits: `tree_id ++ commit_id ++ digest` → compound value.
pub(crate) const COMMITS: TableDefinition<'_, &[u8; 96], &[u8]> = TableDefinition::new("commits");

/// Fragments: `tree_id ++ fragment_head ++ digest` → compound value.
pub(crate) const FRAGMENTS: TableDefinition<'_, &[u8; 96], &[u8]> =
    TableDefinition::new("fragments");

/// File name of the redb database inside the storage root.
pub const DB_FILE_NAME: &str = "sedimentree.redb";

/// Directory name for external (large) blob files inside the storage root.
pub const BLOBS_DIR_NAME: &str = "blobs";

/// Default largest blob size (in bytes) stored inline in the database.
///
/// Blobs larger than this go to flat content-addressed files under
/// `blobs/`. 16 KiB keeps commit-sized records (typically well under 1 KiB)
/// inline — where the B+tree packs them ~10x denser than block-rounded
/// files — while large fragment blobs avoid the ~2x B+tree page
/// amplification and slower streaming the shoot-out benchmark measured at
/// 64 KiB values.
pub const DEFAULT_INLINE_THRESHOLD: usize = 16 * 1024;

/// Hybrid redb + filesystem storage.
///
/// Cheap to clone (the database handle and paths are shared). All
/// operations run on the blocking pool; the [`Database`] itself is
/// internally synchronized with MVCC (concurrent readers, single writer).
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
    blobs_dir: Arc<PathBuf>,
    inline_threshold: usize,
}

impl RedbStorage {
    /// Open (or create) hybrid storage rooted at `root`, with the
    /// [default inline threshold](DEFAULT_INLINE_THRESHOLD).
    ///
    /// Creates `root/sedimentree.redb` and `root/blobs/` as needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories or database cannot be
    /// created/opened, or the tables cannot be initialized.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, RedbStorageError> {
        Self::with_inline_threshold(root, DEFAULT_INLINE_THRESHOLD)
    }

    /// Open (or create) hybrid storage with a custom inline threshold.
    ///
    /// Blobs strictly larger than `inline_threshold` bytes are stored as
    /// external content-addressed files; everything else lives inline in
    /// the database.
    ///
    /// The threshold only affects *writes*: reads dispatch on the stored
    /// value's tag, so a store written with one threshold can be reopened
    /// with another.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories or database cannot be
    /// created/opened, or the tables cannot be initialized.
    pub fn with_inline_threshold(
        root: impl AsRef<Path>,
        inline_threshold: usize,
    ) -> Result<Self, RedbStorageError> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let blobs_dir = root.join(BLOBS_DIR_NAME);
        std::fs::create_dir_all(&blobs_dir)?;

        // Make the directory links themselves durable (one-time cost):
        // `blobs/`'s link lives in `root`, `root`'s in its parent.
        fsync_dir_sync(root)?;
        if let Some(parent) = root.parent().filter(|p| !p.as_os_str().is_empty()) {
            fsync_dir_sync(parent)?;
        }

        let db = Database::create(root.join(DB_FILE_NAME))?;

        // Materialize all tables up front so read transactions never hit
        // `TableDoesNotExist`.
        let txn = db.begin_write()?;
        {
            txn.open_table(TREES)?;
            txn.open_table(COMMITS)?;
            txn.open_table(FRAGMENTS)?;
        }
        txn.commit()?;

        // Make the database file's own directory entry durable. redb
        // fsyncs the file *contents* on commit, but the `sedimentree.redb`
        // link in `root` was created after the directory fsyncs above —
        // strictly per POSIX, a crash here could otherwise lose the link
        // (and with it transactions that were already fsynced and acked).
        fsync_dir_sync(root)?;

        Ok(Self {
            db: Arc::new(db),
            blobs_dir: Arc::new(blobs_dir),
            inline_threshold,
        })
    }

    /// Run `f` against the shared database (and blob directory) on the
    /// blocking pool.
    pub(crate) async fn with_db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database, &Path) -> Result<T, RedbStorageError> + Send + 'static,
    ) -> Result<T, RedbStorageError> {
        let db = Arc::clone(&self.db);
        let blobs_dir = Arc::clone(&self.blobs_dir);
        tokio::task::spawn_blocking(move || f(&db, &blobs_dir)).await?
    }
}
