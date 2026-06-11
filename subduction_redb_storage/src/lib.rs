//! Hybrid [redb] + filesystem storage for Sedimentree.
//!
//! This crate provides [`RedbStorage`], a storage implementation of the
//! [`Storage`] trait from `subduction_core` intended as an alternative to
//! `sedimentree_fs_storage` for native servers. Metadata and small blobs
//! live in a transactional redb B+tree; large blobs live as flat
//! content-addressed files beside it. The split captures both measured
//! sweet spots: a B+tree packs small records densely (no per-file block
//! rounding) and scans them fast, while the filesystem streams large values
//! faster and without the ~2x B+tree page amplification.
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
//! per [`CommitId`] coexist) falls out of the trailing content digest in the
//! key, mirroring the CAS filenames of the filesystem backend.
//!
//! # Durability & crash consistency
//!
//! redb's default durability fsyncs on every transaction commit, so each
//! `save_*` call is durable when it returns and
//! [`save_batch`](Storage::save_batch) amortizes one fsync across the whole
//! batch. External blob files are written durably (temp file fsynced before
//! rename, bucket directory fsynced after) **before** the referencing
//! database transaction commits: a record in the database always points at
//! a complete blob file. A crash in between leaves at most an *orphan* blob
//! file, which is harmless — files are content-addressed, so a later save
//! of the same blob adopts it.
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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use future_form::{FutureForm, Local, Sendable};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use sedimentree_core::{
    blob::{Blob, has_meta::HasBlobMeta},
    collections::Set,
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;

/// Registered sedimentree ids.
const TREES: TableDefinition<'_, &[u8; 32], ()> = TableDefinition::new("trees");

/// Loose commits: `tree_id ++ commit_id ++ digest` → compound value.
const COMMITS: TableDefinition<'_, &[u8; 96], &[u8]> = TableDefinition::new("commits");

/// Fragments: `tree_id ++ fragment_head ++ digest` → compound value.
const FRAGMENTS: TableDefinition<'_, &[u8; 96], &[u8]> = TableDefinition::new("fragments");

/// A 96-byte composite key: `tree_id ++ item_id ++ content_digest`.
type Key96 = [u8; 96];

/// Undecoded `meta` + `blob` byte pair from one stored compound value
/// (external blobs already resolved to their file contents).
type RawCompound = (Vec<u8>, Vec<u8>);

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

/// Value tag: blob bytes stored inline after the meta.
const TAG_INLINE: u8 = 0x00;

/// Value tag: blob stored externally; value holds digest + length.
const TAG_EXTERNAL: u8 = 0x01;

/// Process-wide counter distinguishing concurrent writers that target the
/// same content-addressed blob path.
static TMP_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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

        Ok(Self {
            db: Arc::new(db),
            blobs_dir: Arc::new(blobs_dir),
            inline_threshold,
        })
    }

    /// Run `f` against the shared database (and blob directory) on the
    /// blocking pool.
    async fn with_db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database, &Path) -> Result<T, RedbStorageError> + Send + 'static,
    ) -> Result<T, RedbStorageError> {
        let db = Arc::clone(&self.db);
        let blobs_dir = Arc::clone(&self.blobs_dir);
        tokio::task::spawn_blocking(move || f(&db, &blobs_dir)).await?
    }
}

/// Build the composite key for one stored item.
fn key96(tree: SedimentreeId, item: CommitId, digest: &[u8; 32]) -> Key96 {
    let mut key = [0u8; 96];
    key[..32].copy_from_slice(tree.as_bytes());
    key[32..64].copy_from_slice(item.as_bytes());
    key[64..].copy_from_slice(digest);
    key
}

/// Inclusive key range covering every item under a 32-byte tree prefix.
fn tree_range(tree: SedimentreeId) -> (Key96, Key96) {
    let mut lo = [0u8; 96];
    let mut hi = [0xffu8; 96];
    lo[..32].copy_from_slice(tree.as_bytes());
    hi[..32].copy_from_slice(tree.as_bytes());
    (lo, hi)
}

/// Inclusive key range covering every item under a 64-byte (tree, id) prefix.
fn item_range(tree: SedimentreeId, item: CommitId) -> (Key96, Key96) {
    let (mut lo, mut hi) = tree_range(tree);
    lo[32..64].copy_from_slice(item.as_bytes());
    hi[32..64].copy_from_slice(item.as_bytes());
    (lo, hi)
}

/// Extract the item id (bytes 32..64) from a composite key.
const fn item_id_of(key: &Key96) -> CommitId {
    let (_, rest) = key.split_at(32);
    let (item_id, _) = rest.split_at(32);
    let mut id = [0u8; 32];
    id.copy_from_slice(item_id);
    CommitId::new(id)
}

/// A decoded compound value, before external blob resolution.
enum DecodedCompound {
    /// Blob bytes were stored inline.
    Inline {
        /// `Signed<T>` wire bytes.
        meta: Vec<u8>,
        /// Blob contents.
        blob: Vec<u8>,
    },

    /// Blob lives in an external content-addressed file, named by the
    /// digest inside the meta's own `BlobMeta` (no duplication).
    External {
        /// `Signed<T>` wire bytes.
        meta: Vec<u8>,
    },
}

/// Encode an inline compound value: `0x00 ++ meta_len:u32be ++ meta ++ blob`.
fn encode_inline(meta: &[u8], blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + meta.len() + blob.len());
    out.push(TAG_INLINE);
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    out.extend_from_slice(meta);
    out.extend_from_slice(blob);
    out
}

/// Encode an external compound value: `0x01 ++ meta`.
///
/// No length prefix and no blob reference: the meta is the remainder of
/// the value, and the blob's digest/size are already inside the signed
/// payload's `BlobMeta`.
fn encode_external(meta: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + meta.len());
    out.push(TAG_EXTERNAL);
    out.extend_from_slice(meta);
    out
}

/// Decode a compound value. `None` on a malformed buffer.
fn decode_compound(bytes: &[u8]) -> Option<DecodedCompound> {
    let tag = *bytes.first()?;

    match tag {
        TAG_INLINE => {
            let len_bytes: [u8; 4] = bytes.get(1..5)?.try_into().ok()?;
            let meta_len = u32::from_be_bytes(len_bytes) as usize;
            Some(DecodedCompound::Inline {
                meta: bytes.get(5..5 + meta_len)?.to_vec(),
                blob: bytes.get(5 + meta_len..)?.to_vec(),
            })
        }
        TAG_EXTERNAL => Some(DecodedCompound::External {
            meta: bytes.get(1..)?.to_vec(),
        }),
        _ => None,
    }
}

/// Path of the external blob file for `digest`:
/// `blobs/{hex[0..2]}/{digest_hex}`.
fn blob_file_path(blobs_dir: &Path, digest: &[u8; 32]) -> PathBuf {
    let hex = hex_encode(digest);
    let (bucket, _) = hex.split_at(2);
    blobs_dir.join(bucket).join(&hex)
}

/// Lowercase hex of a 32-byte digest.
fn hex_encode(digest: &[u8; 32]) -> String {
    use core::fmt::Write;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Durably write an external blob file (CAS: a no-op if an intact file for
/// this digest already exists).
///
/// Must complete — including fsyncs — *before* the database transaction
/// referencing the digest commits, so a stored record always points at a
/// complete file. Crash before the commit leaves only a harmless
/// content-addressed orphan. Must be called from a blocking context.
fn write_blob_file_sync(
    blobs_dir: &Path,
    digest: &[u8; 32],
    data: &[u8],
) -> Result<(), RedbStorageError> {
    use std::io::Write;

    let hex = hex_encode(digest);
    let (bucket, _) = hex.split_at(2);
    let bucket_dir = blobs_dir.join(bucket);
    let path = bucket_dir.join(&hex);

    // CAS: skip only if the existing file is intact (a crash-truncated file
    // is rewritten — same lesson as the filesystem backend).
    if let Ok(existing) = std::fs::metadata(&path) {
        if existing.len() == data.len() as u64 {
            return Ok(());
        }

        tracing::warn!(
            path = %path.display(),
            have = existing.len(),
            need = data.len(),
            "rewriting corrupt external blob file (size mismatch)"
        );
    }

    // Bucket dirs are one level below `blobs/`; if this one is new, fsync
    // `blobs/` so the bucket's link survives a crash (the constructor made
    // `blobs/` itself durable).
    if std::fs::metadata(&bucket_dir).is_err() {
        std::fs::create_dir_all(&bucket_dir)?;
        fsync_dir_sync(blobs_dir)?;
    }

    // Temp-then-rename with the fsyncs ordered for crash consistency:
    // contents durable before the name appears, name durable before return.
    // The guard removes the temp on any early error exit (e.g. ENOSPC), so
    // failures don't strand `.tmp` files that worsen a full disk on retry.
    let nonce = TMP_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut temp = TempFileGuard {
        path: path.with_extension(format!("{nonce}.tmp")),
        renamed: false,
    };

    let mut file = std::fs::File::create(&temp.path)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);

    std::fs::rename(&temp.path, &path)?;
    temp.renamed = true;
    fsync_dir_sync(&bucket_dir)?;

    Ok(())
}

/// Fsync a directory so its entries survive a crash.
///
/// No-op on non-Unix targets: `std` cannot open a directory handle on
/// Windows (it would need `FILE_FLAG_BACKUP_SEMANTICS`), and Windows has
/// no portable directory-fsync concept — directory-entry durability there
/// is best-effort.
fn fsync_dir_sync(dir: &Path) -> Result<(), RedbStorageError> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;

    Ok(())
}

/// Removes its temp file on drop unless the rename into place completed.
struct TempFileGuard {
    path: PathBuf,
    renamed: bool,
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.renamed {
            std::fs::remove_file(&self.path).ok();
        }
    }
}

/// Read an external blob file. `Ok(None)` if absent.
fn read_blob_file_sync(
    blobs_dir: &Path,
    digest: &[u8; 32],
) -> Result<Option<Vec<u8>>, RedbStorageError> {
    match std::fs::read(blob_file_path(blobs_dir, digest)) {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Decode a raw compound pair into a [`VerifiedMeta`], skip-and-warn on
/// corruption (mirrors the filesystem backend's tolerance).
fn decode_verified<T>(raw: RawCompound, what: &str) -> Option<VerifiedMeta<T>>
where
    T: sedimentree_core::blob::has_meta::HasBlobMeta
        + sedimentree_core::codec::schema::Schema
        + sedimentree_core::codec::encode::EncodeFields
        + sedimentree_core::codec::decode::DecodeFields,
{
    let (meta, blob) = raw;
    let signed: Signed<T> = match Signed::try_decode(meta) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("skipping corrupt stored {what}: {e}");
            return None;
        }
    };

    match VerifiedMeta::try_from_trusted(signed, Blob::new(blob)) {
        Ok(verified) => Some(verified),
        Err(e) => {
            tracing::warn!("skipping corrupt stored {what}: {e}");
            None
        }
    }
}

/// Number of external blob files read per blocking task when bulk loads
/// resolve them (mirrors the filesystem backend's bulk-read fan-out).
const EXTERNAL_READ_CHUNK: usize = 128;

/// Scan the compound values in `range` from `table`, decoding each one
/// straight off the borrowed page (a single copy of the meta/blob bytes —
/// no intermediate whole-value `Vec`). Malformed values are skipped with a
/// warning. No file I/O.
fn scan_decoded(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<Vec<DecodedCompound>, RedbStorageError> {
    let mut out = Vec::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (_, value) = entry?;
        if let Some(decoded) = decode_compound(value.value()) {
            out.push(decoded);
        } else {
            tracing::warn!("skipping malformed compound value");
        }
    }
    Ok(out)
}

/// Decode scanned compound values into [`VerifiedMeta`]s, resolving
/// external blobs via parallel chunked file reads.
///
/// Three phases: the values arrive pre-decoded from a single B+tree scan
/// hop, the metas decode here in async land (yielding each external
/// record's blob digest from its own `BlobMeta`), and the external files
/// are then read in chunks of [`EXTERNAL_READ_CHUNK`] across blocking
/// tasks so a large tree's blob resolution is not bound by one thread's
/// sequential open/read throughput.
///
/// Undecodable metas, missing external files, and corrupt items are
/// skipped with a warning (mirroring the filesystem backend's tolerance).
async fn resolve_items<T>(
    blobs_dir: Arc<PathBuf>,
    items: Vec<DecodedCompound>,
    what: &str,
) -> Result<Vec<VerifiedMeta<T>>, RedbStorageError>
where
    T: sedimentree_core::blob::has_meta::HasBlobMeta
        + sedimentree_core::codec::schema::Schema
        + sedimentree_core::codec::encode::EncodeFields
        + sedimentree_core::codec::decode::DecodeFields,
{
    let mut out = Vec::with_capacity(items.len());
    let mut pending: Vec<(Signed<T>, [u8; 32])> = Vec::new();

    for item in items {
        match item {
            DecodedCompound::Inline { meta, blob } => {
                if let Some(verified) = decode_verified((meta, blob), what) {
                    out.push(verified);
                }
            }
            DecodedCompound::External { meta } => match Signed::<T>::try_decode(meta) {
                Ok(signed) => match signed.try_decode_trusted_payload() {
                    Ok(payload) => {
                        pending.push((signed, *payload.blob_meta().digest().as_bytes()));
                    }
                    Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
                },
                Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
            },
        }
    }

    if pending.is_empty() {
        return Ok(out);
    }

    let digests: Vec<[u8; 32]> = pending.iter().map(|(_, digest)| *digest).collect();
    let handles: Vec<_> = digests
        .chunks(EXTERNAL_READ_CHUNK)
        .map(|chunk| {
            let chunk = chunk.to_vec();
            let blobs_dir = Arc::clone(&blobs_dir);
            tokio::task::spawn_blocking(
                move || -> Result<Vec<Option<Vec<u8>>>, RedbStorageError> {
                    chunk
                        .iter()
                        .map(|digest| read_blob_file_sync(&blobs_dir, digest))
                        .collect()
                },
            )
        })
        .collect();

    let mut blobs = Vec::with_capacity(pending.len());
    for handle in handles {
        blobs.extend(handle.await??);
    }

    for ((signed, digest), blob) in pending.into_iter().zip(blobs) {
        let Some(blob) = blob else {
            tracing::warn!(
                digest = %hex_encode(&digest),
                "external blob file missing; skipping {what}"
            );
            continue;
        };

        match VerifiedMeta::try_from_trusted(signed, Blob::new(blob)) {
            Ok(verified) => out.push(verified),
            Err(e) => tracing::warn!("skipping corrupt stored {what}: {e}"),
        }
    }

    Ok(out)
}

/// Collect the item ids present in `range` (deduplicated).
fn scan_ids(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<Set<CommitId>, RedbStorageError> {
    let mut out = Set::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (key, _) = entry?;
        out.insert(item_id_of(key.value()));
    }
    Ok(out)
}

/// Delete every key in `range` from `table` within an open write transaction.
fn delete_range(
    table: &mut redb::Table<'_, &'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<(), RedbStorageError> {
    let keys: Vec<Key96> = table
        .range::<&[u8; 96]>(lo..=hi)?
        .map(|entry| entry.map(|(k, _)| *k.value()))
        .collect::<Result<_, _>>()?;

    for key in &keys {
        table.remove(key)?;
    }

    Ok(())
}

/// Durably write the external blob files for every over-threshold item.
///
/// Runs **before** `begin_write()`: redb has a single database-wide writer,
/// so file fsyncs done inside an open write transaction would stall every
/// other writer (all trees, all peers) for the duration of the blob I/O.
/// Staging first keeps the exclusive window down to the B+tree inserts and
/// one commit fsync, with identical crash consistency — the only invariant
/// is "file durable before the referencing transaction commits", and a
/// crash before that commit leaves only harmless content-addressed
/// orphans. Must be called from a blocking context.
fn stage_external_blobs_sync(
    items: &[PendingInsert],
    blobs_dir: &Path,
    inline_threshold: usize,
) -> Result<(), RedbStorageError> {
    for item in items {
        if item.blob.len() > inline_threshold {
            write_blob_file_sync(blobs_dir, &item.blob_digest, &item.blob)?;
        }
    }
    Ok(())
}

/// Insert one compound item (CAS: no-op if the key already exists).
///
/// Pure B+tree work: any external blob file must already have been staged
/// via [`stage_external_blobs_sync`] before the surrounding transaction
/// commits.
fn insert_compound(
    table: &mut redb::Table<'_, &'static [u8; 96], &'static [u8]>,
    item: &PendingInsert,
    inline_threshold: usize,
) -> Result<(), RedbStorageError> {
    if table.get(&item.key)?.is_some() {
        return Ok(());
    }

    if item.blob.len() > inline_threshold {
        table.insert(&item.key, encode_external(&item.meta).as_slice())?;
    } else {
        table.insert(&item.key, encode_inline(&item.meta, &item.blob).as_slice())?;
    }

    Ok(())
}

/// Resolved write for one item: key + byte payloads + the blob's own
/// content digest (names the external file when the blob is large).
struct PendingInsert {
    key: Key96,
    meta: Vec<u8>,
    blob: Vec<u8>,
    blob_digest: [u8; 32],
}

/// Resolve a verified commit into its key and payloads.
fn pending_commit(tree: SedimentreeId, verified: &VerifiedMeta<LooseCommit>) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
        blob_digest: *verified.payload().blob_meta().digest().as_bytes(),
    }
}

/// Resolve a verified fragment into its key and payloads.
fn pending_fragment(tree: SedimentreeId, verified: &VerifiedMeta<Fragment>) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
        blob_digest: *verified.payload().blob_meta().digest().as_bytes(),
    }
}

impl Storage<Sendable> for RedbStorage {
    type Error = RedbStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::save_sedimentree_id");
            self.with_db(move |db, _blobs_dir| {
                let txn = db.begin_write()?;
                txn.open_table(TREES)?
                    .insert(sedimentree_id.as_bytes(), ())?;
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::delete_sedimentree_id");
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_write()?;
                {
                    txn.open_table(TREES)?.remove(sedimentree_id.as_bytes())?;
                    delete_range(&mut txn.open_table(COMMITS)?, &lo, &hi)?;
                    delete_range(&mut txn.open_table(FRAGMENTS)?, &lo, &hi)?;
                }
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!("RedbStorage::load_all_sedimentree_ids");
            self.with_db(|db, _blobs_dir| {
                let txn = db.begin_read()?;
                let table = txn.open_table(TREES)?;
                let mut ids = Set::new();
                for entry in table.iter()? {
                    let (key, _) = entry?;
                    ids.insert(SedimentreeId::new(*key.value()));
                }
                Ok(ids)
            })
            .await
        })
    }

    fn contains_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<bool, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::contains_sedimentree_id");
            self.with_db(move |db, _blobs_dir| {
                let txn = db.begin_read()?;
                let table = txn.open_table(TREES)?;
                Ok(table.get(sedimentree_id.as_bytes())?.is_some())
            })
            .await
        })
    }

    // ==================== Loose Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::save_loose_commit");
            let pending = pending_commit(sedimentree_id, &verified);
            let threshold = self.inline_threshold;
            self.with_db(move |db, blobs_dir| {
                // External blob (if any) staged before the write txn so its
                // file I/O doesn't hold the database-wide writer slot.
                stage_external_blobs_sync(core::slice::from_ref(&pending), blobs_dir, threshold)?;

                let txn = db.begin_write()?;
                {
                    // Contract: persisting an item registers its
                    // sedimentree id, atomically with the item itself.
                    txn.open_table(TREES)?
                        .insert(sedimentree_id.as_bytes(), ())?;
                    insert_compound(&mut txn.open_table(COMMITS)?, &pending, threshold)?;
                }
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn list_commit_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::list_commit_ids");
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_read()?;
                scan_ids(&txn.open_table(COMMITS)?, &lo, &hi)
            })
            .await
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::load_loose_commits");

            // Scan hop → decode in async land → parallel external blob reads.
            let raw = self
                .with_db(move |db, _blobs_dir| {
                    let (lo, hi) = tree_range(sedimentree_id);
                    let txn = db.begin_read()?;
                    scan_decoded(&txn.open_table(COMMITS)?, &lo, &hi)
                })
                .await?;

            resolve_items(Arc::clone(&self.blobs_dir), raw, "loose commit").await
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::trace!(
                ?sedimentree_id,
                ?commit_id,
                "RedbStorage::load_loose_commit"
            );

            let raw = self
                .with_db(move |db, _blobs_dir| {
                    let (lo, hi) = item_range(sedimentree_id, commit_id);
                    let txn = db.begin_read()?;
                    scan_decoded(&txn.open_table(COMMITS)?, &lo, &hi)
                })
                .await?;

            Ok(
                resolve_items(Arc::clone(&self.blobs_dir), raw, "loose commit")
                    .await?
                    .into_iter()
                    .next(),
            )
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
                "RedbStorage::delete_loose_commit"
            );
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = item_range(sedimentree_id, commit_id);
                let txn = db.begin_write()?;
                delete_range(&mut txn.open_table(COMMITS)?, &lo, &hi)?;
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::delete_loose_commits");
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_write()?;
                delete_range(&mut txn.open_table(COMMITS)?, &lo, &hi)?;
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    // ==================== Fragments (compound with blob) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::save_fragment");
            let pending = pending_fragment(sedimentree_id, &verified);
            let threshold = self.inline_threshold;
            self.with_db(move |db, blobs_dir| {
                // External blob (if any) staged before the write txn so its
                // file I/O doesn't hold the database-wide writer slot.
                stage_external_blobs_sync(core::slice::from_ref(&pending), blobs_dir, threshold)?;

                let txn = db.begin_write()?;
                {
                    // Contract: persisting an item registers its
                    // sedimentree id, atomically with the item itself.
                    txn.open_table(TREES)?
                        .insert(sedimentree_id.as_bytes(), ())?;
                    insert_compound(&mut txn.open_table(FRAGMENTS)?, &pending, threshold)?;
                }
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::trace!(
                ?sedimentree_id,
                ?fragment_head,
                "RedbStorage::load_fragment"
            );

            let raw = self
                .with_db(move |db, _blobs_dir| {
                    let (lo, hi) = item_range(sedimentree_id, fragment_head);
                    let txn = db.begin_read()?;
                    scan_decoded(&txn.open_table(FRAGMENTS)?, &lo, &hi)
                })
                .await?;

            Ok(resolve_items(Arc::clone(&self.blobs_dir), raw, "fragment")
                .await?
                .into_iter()
                .next())
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::list_fragment_ids");
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_read()?;
                scan_ids(&txn.open_table(FRAGMENTS)?, &lo, &hi)
            })
            .await
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>>
    {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::load_fragments");

            // Scan hop → decode in async land → parallel external blob reads.
            let raw = self
                .with_db(move |db, _blobs_dir| {
                    let (lo, hi) = tree_range(sedimentree_id);
                    let txn = db.begin_read()?;
                    scan_decoded(&txn.open_table(FRAGMENTS)?, &lo, &hi)
                })
                .await?;

            resolve_items(Arc::clone(&self.blobs_dir), raw, "fragment").await
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
                "RedbStorage::delete_fragment"
            );
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = item_range(sedimentree_id, fragment_head);
                let txn = db.begin_write()?;
                delete_range(&mut txn.open_table(FRAGMENTS)?, &lo, &hi)?;
                txn.commit()?;
                Ok(())
            })
            .await
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::delete_fragments");
            self.with_db(move |db, _blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_write()?;
                delete_range(&mut txn.open_table(FRAGMENTS)?, &lo, &hi)?;
                txn.commit()?;
                Ok(())
            })
            .await
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
                "RedbStorage::save_batch"
            );

            let pending_commits: Vec<_> = commits
                .iter()
                .map(|v| pending_commit(sedimentree_id, v))
                .collect();
            let pending_fragments: Vec<_> = fragments
                .iter()
                .map(|v| pending_fragment(sedimentree_id, v))
                .collect();

            // The whole batch — id registration included — is one
            // transaction: all-or-nothing, with a single fsync at commit.
            // External blob files are staged durably *before* the write txn
            // opens, so their file I/O never holds the database-wide writer
            // slot; the commit then makes the references visible only after
            // the files exist.
            let threshold = self.inline_threshold;
            self.with_db(move |db, blobs_dir| {
                stage_external_blobs_sync(&pending_commits, blobs_dir, threshold)?;
                stage_external_blobs_sync(&pending_fragments, blobs_dir, threshold)?;

                let txn = db.begin_write()?;
                {
                    txn.open_table(TREES)?
                        .insert(sedimentree_id.as_bytes(), ())?;

                    let mut commits_table = txn.open_table(COMMITS)?;
                    for p in &pending_commits {
                        insert_compound(&mut commits_table, p, threshold)?;
                    }

                    let mut fragments_table = txn.open_table(FRAGMENTS)?;
                    for p in &pending_fragments {
                        insert_compound(&mut fragments_table, p, threshold)?;
                    }
                }
                txn.commit()?;
                Ok(())
            })
            .await?;

            Ok(num_commits + num_fragments)
        })
    }
}

impl Storage<Local> for RedbStorage {
    type Error = RedbStorageError;

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

/// Errors that can occur during redb storage operations.
#[derive(Debug, Error)]
pub enum RedbStorageError {
    /// I/O error (external blob files or directory setup).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to open or create the database.
    #[error(transparent)]
    Database(#[from] redb::DatabaseError),

    /// Failed to begin a transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Failed to open a table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Low-level storage failure.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Failed to commit a transaction.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),

    /// A blocking storage task panicked or was cancelled.
    #[error("blocking storage task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
