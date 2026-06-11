//! [redb]-based storage for Sedimentree.
//!
//! This crate provides [`RedbStorage`], a transactional single-file storage
//! implementation of the [`Storage`] trait from `subduction_core`, intended
//! as an alternative to `sedimentree_fs_storage` for native servers.
//!
//! # Layout
//!
//! All data lives in one redb database file with three tables:
//!
//! ```text
//! trees:     [u8; 32]                                  → ()
//! commits:   [u8; 96] = tree_id ++ commit_id ++ digest → meta_len:u32be ++ meta ++ blob
//! fragments: [u8; 96] = tree_id ++ head_id  ++ digest → meta_len:u32be ++ meta ++ blob
//! ```
//!
//! Keys sort lexicographically, so all items of a tree (or of one
//! commit/fragment identity) are contiguous: bulk loads are a single B+tree
//! range scan, and the Byzantine-equivocation contract (multiple payloads
//! per [`CommitId`] coexist) falls out of the trailing content digest in the
//! key, mirroring the CAS filenames of the filesystem backend.
//!
//! # Durability
//!
//! redb's default durability ([`Immediate`](redb::Durability::Immediate))
//! fsyncs on every transaction commit, so each `save_*` call is durable when
//! it returns and [`save_batch`](Storage::save_batch) amortizes one fsync
//! across the whole batch — all-or-nothing, unlike the filesystem backend.
//!
//! # Example
//!
//! ```no_run
//! use sedimentree_redb_storage::RedbStorage;
//! use std::path::PathBuf;
//!
//! let storage = RedbStorage::new(PathBuf::from("./data.redb")).expect("failed to open database");
//! ```
//!
//! [redb]: https://github.com/cberner/redb

#![forbid(unsafe_code)]

use std::{path::PathBuf, sync::Arc};

use future_form::{FutureForm, Local, Sendable};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use sedimentree_core::{
    blob::Blob,
    codec::error::DecodeError,
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

/// Undecoded `meta` + `blob` byte pair from one stored compound value.
type RawCompound = (Vec<u8>, Vec<u8>);

/// redb-backed storage.
///
/// Cheap to clone (the database handle is shared). All operations run on the
/// blocking pool; the [`Database`] itself is internally synchronized with
/// MVCC (concurrent readers, single writer).
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

impl RedbStorage {
    /// Open (or create) a redb database at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created/opened or the
    /// tables cannot be initialized.
    pub fn new(path: PathBuf) -> Result<Self, RedbStorageError> {
        let db = Database::create(path)?;

        // Materialize all tables up front so read transactions never hit
        // `TableDoesNotExist`.
        let txn = db.begin_write()?;
        {
            txn.open_table(TREES)?;
            txn.open_table(COMMITS)?;
            txn.open_table(FRAGMENTS)?;
        }
        txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Run `f` against the shared database on the blocking pool.
    async fn with_db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database) -> Result<T, RedbStorageError> + Send + 'static,
    ) -> Result<T, RedbStorageError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db)).await?
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
fn item_id_of(key: &Key96) -> CommitId {
    let mut id = [0u8; 32];
    id.copy_from_slice(key.get(32..64).unwrap_or(&[0u8; 32]));
    CommitId::new(id)
}

/// Encode a compound value: `meta_len:u32be ++ meta ++ blob`.
fn encode_compound(meta: &[u8], blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + meta.len() + blob.len());
    #[allow(clippy::cast_possible_truncation)]
    out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    out.extend_from_slice(meta);
    out.extend_from_slice(blob);
    out
}

/// Decode a compound value. `None` on a malformed buffer.
fn decode_compound(bytes: &[u8]) -> Option<RawCompound> {
    let len_bytes: [u8; 4] = bytes.get(..4)?.try_into().ok()?;
    let meta_len = u32::from_be_bytes(len_bytes) as usize;
    let meta = bytes.get(4..4 + meta_len)?.to_vec();
    let blob = bytes.get(4 + meta_len..)?.to_vec();
    Some((meta, blob))
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

/// Read every raw compound value in `range` from `table`.
fn scan_range(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    lo: &Key96,
    hi: &Key96,
) -> Result<Vec<RawCompound>, RedbStorageError> {
    let mut out = Vec::new();
    for entry in table.range::<&[u8; 96]>(lo..=hi)? {
        let (_, value) = entry?;
        if let Some(raw) = decode_compound(value.value()) {
            out.push(raw);
        } else {
            tracing::warn!("skipping malformed compound value");
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

/// Insert one compound item (CAS: no-op if the key already exists).
fn insert_compound(
    table: &mut redb::Table<'_, &'static [u8; 96], &'static [u8]>,
    key: &Key96,
    meta: &[u8],
    blob: &[u8],
) -> Result<(), RedbStorageError> {
    if table.get(key)?.is_some() {
        return Ok(());
    }

    table.insert(key, encode_compound(meta, blob).as_slice())?;
    Ok(())
}

/// Resolved write for one item: key + borrowed-from-owned byte payloads.
struct PendingInsert {
    key: Key96,
    meta: Vec<u8>,
    blob: Vec<u8>,
}

/// Resolve a verified commit into its key and payloads.
fn pending_commit(tree: SedimentreeId, verified: &VerifiedMeta<LooseCommit>) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
    }
}

/// Resolve a verified fragment into its key and payloads.
fn pending_fragment(tree: SedimentreeId, verified: &VerifiedMeta<Fragment>) -> PendingInsert {
    let digest = Digest::hash(verified.payload());
    PendingInsert {
        key: key96(tree, verified.payload().head(), digest.as_bytes()),
        meta: verified.signed().as_bytes().to_vec(),
        blob: verified.blob().contents().clone(),
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
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
            self.with_db(|db| {
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
                let txn = db.begin_write()?;
                insert_compound(
                    &mut txn.open_table(COMMITS)?,
                    &pending.key,
                    &pending.meta,
                    &pending.blob,
                )?;
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
            self.with_db(move |db| {
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

            // One range scan in a blocking hop; decode in async land.
            let raw = self
                .with_db(move |db| {
                    let (lo, hi) = tree_range(sedimentree_id);
                    let txn = db.begin_read()?;
                    scan_range(&txn.open_table(COMMITS)?, &lo, &hi)
                })
                .await?;

            Ok(raw
                .into_iter()
                .filter_map(|pair| decode_verified(pair, "loose commit"))
                .collect())
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
                .with_db(move |db| {
                    let (lo, hi) = item_range(sedimentree_id, commit_id);
                    let txn = db.begin_read()?;
                    Ok(scan_range(&txn.open_table(COMMITS)?, &lo, &hi)?
                        .into_iter()
                        .next())
                })
                .await?;

            Ok(raw.and_then(|pair| decode_verified(pair, "loose commit")))
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
                let txn = db.begin_write()?;
                insert_compound(
                    &mut txn.open_table(FRAGMENTS)?,
                    &pending.key,
                    &pending.meta,
                    &pending.blob,
                )?;
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
                .with_db(move |db| {
                    let (lo, hi) = item_range(sedimentree_id, fragment_head);
                    let txn = db.begin_read()?;
                    Ok(scan_range(&txn.open_table(FRAGMENTS)?, &lo, &hi)?
                        .into_iter()
                        .next())
                })
                .await?;

            Ok(raw.and_then(|pair| decode_verified(pair, "fragment")))
        })
    }

    fn list_fragment_ids(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> <Sendable as FutureForm>::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Sendable::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::list_fragment_ids");
            self.with_db(move |db| {
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

            let raw = self
                .with_db(move |db| {
                    let (lo, hi) = tree_range(sedimentree_id);
                    let txn = db.begin_read()?;
                    scan_range(&txn.open_table(FRAGMENTS)?, &lo, &hi)
                })
                .await?;

            Ok(raw
                .into_iter()
                .filter_map(|pair| decode_verified(pair, "fragment"))
                .collect())
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
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
            self.with_db(move |db| {
                let txn = db.begin_write()?;
                {
                    txn.open_table(TREES)?
                        .insert(sedimentree_id.as_bytes(), ())?;

                    let mut commits_table = txn.open_table(COMMITS)?;
                    for p in &pending_commits {
                        insert_compound(&mut commits_table, &p.key, &p.meta, &p.blob)?;
                    }

                    let mut fragments_table = txn.open_table(FRAGMENTS)?;
                    for p in &pending_fragments {
                        insert_compound(&mut fragments_table, &p.key, &p.meta, &p.blob)?;
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

    /// Decoding error.
    #[error(transparent)]
    Decode(#[from] DecodeError),

    /// A blocking storage task panicked or was cancelled.
    #[error("blocking storage task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
