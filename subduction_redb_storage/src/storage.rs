//! The redb-backed [`Storage`] implementation.

use std::{path::Path, sync::Arc};

use future_form::{FutureForm, Local, Sendable, future_form};
use redb::{Database, ReadableDatabase as _, TableDefinition};
use sedimentree_core::{
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::signed::Signed;
use subduction_protocol::storage::StorageFailure;
use subduction_runtime::storage::{FetchedItems, Storage};
use thiserror::Error;

/// Registered tree ids.
const TREES: TableDefinition<'_, [u8; 32], ()> = TableDefinition::new("trees");

/// Commits: `tree_id ++ commit_id` → framed signed bytes + blob.
const COMMITS: TableDefinition<'_, [u8; 64], Vec<u8>> = TableDefinition::new("commits");

/// Fragments: `tree_id ++ head_id` → framed signed bytes + blob.
const FRAGMENTS: TableDefinition<'_, [u8; 64], Vec<u8>> = TableDefinition::new("fragments");

/// A durable [`Storage`] over one redb database file. See the
/// [crate docs](crate) for layout and batching.
///
/// Cheap to clone (shared handle); safe to use from multithreaded
/// (`Sendable`) drivers — every transaction runs on the blocking pool.
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
}

impl RedbStorage {
    /// Open (or create) the database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] if the file cannot be created/opened or the
    /// tables cannot be initialized.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenError> {
        let db = Database::create(path)?;
        // Create the tables eagerly so reads on a fresh database see
        // empty tables rather than missing ones.
        let tx = db.begin_write().map_err(redb::Error::from)?;
        {
            let _trees = tx.open_table(TREES).map_err(redb::Error::from)?;
            let _commits = tx.open_table(COMMITS).map_err(redb::Error::from)?;
            let _fragments = tx.open_table(FRAGMENTS).map_err(redb::Error::from)?;
        }
        tx.commit().map_err(redb::Error::from)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// One blocking persist transaction: all items, one commit, one fsync.
    fn persist_blocking(
        db: &Database,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
        fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
    ) -> Result<u32, redb::Error> {
        let tx = db.begin_write()?;
        let mut stored = 0u32;
        {
            let mut trees = tx.open_table(TREES)?;
            let _previous = trees.insert(*tree.as_bytes(), ())?;

            let mut table = tx.open_table(COMMITS)?;
            for (signed, blob) in commits {
                let Ok(payload) = signed.try_decode_trusted_payload() else {
                    continue; // undecodable ⇒ never persisted, never fetched
                };
                let key = item_key(tree, payload.head());
                let _previous = table.insert(key, frame(&signed, &blob))?;
                stored += 1;
            }

            let mut table = tx.open_table(FRAGMENTS)?;
            for (signed, blob) in fragments {
                let Ok(payload) = signed.try_decode_trusted_payload() else {
                    continue;
                };
                let key = item_key(tree, payload.head());
                let _previous = table.insert(key, frame(&signed, &blob))?;
                stored += 1;
            }
        }
        tx.commit()?;
        Ok(stored)
    }

    /// One blocking read transaction for a fetch op.
    fn fetch_blocking(
        db: &Database,
        tree: SedimentreeId,
        commit_ids: Vec<CommitId>,
        fragment_heads: Vec<CommitId>,
    ) -> Result<Option<FetchedItems>, redb::Error> {
        let tx = db.begin_read()?;
        let trees = tx.open_table(TREES)?;
        if trees.get(*tree.as_bytes())?.is_none() {
            return Ok(None);
        }

        let mut items = FetchedItems::default();
        let commits = tx.open_table(COMMITS)?;
        for id in commit_ids {
            if let Some(value) = commits.get(item_key(tree, id))?
                && let Some(found) = unframe::<LooseCommit>(&value.value()) {
                    items.commits.push(found);
                }
        }
        let fragments = tx.open_table(FRAGMENTS)?;
        for head in fragment_heads {
            if let Some(value) = fragments.get(item_key(tree, head))?
                && let Some(found) = unframe::<Fragment>(&value.value()) {
                    items.fragments.push(found);
                }
        }
        Ok(Some(items))
    }

    /// One blocking transaction removing a tree and all its items.
    fn delete_blocking(db: &Database, tree: SedimentreeId) -> Result<(), redb::Error> {
        let tx = db.begin_write()?;
        {
            let mut trees = tx.open_table(TREES)?;
            let _previous = trees.remove(*tree.as_bytes())?;
            // `extract_from_if` drains lazily: entries are only removed
            // as the iterator is consumed, so consume it fully.
            let mut commits = tx.open_table(COMMITS)?;
            for entry in commits.extract_from_if(tree_range(tree), |_, _| true)? {
                let _removed = entry?;
            }
            let mut fragments = tx.open_table(FRAGMENTS)?;
            for entry in fragments.extract_from_if(tree_range(tree), |_, _| true)? {
                let _removed = entry?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Storage<Async> for RedbStorage {
    fn persist_items(
        &self,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
        fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
    ) -> Async::Future<'_, Result<u32, StorageFailure>> {
        let db = Arc::clone(&self.db);
        Async::from_future(async move {
            tokio::task::spawn_blocking(move || {
                Self::persist_blocking(&db, tree, commits, fragments)
            })
            .await
            .map_err(|_join| StorageFailure::Permanent)?
            .map_err(|error| coarsen(&error))
        })
    }

    fn fetch_items(
        &self,
        tree: SedimentreeId,
        commit_ids: Vec<CommitId>,
        fragment_heads: Vec<CommitId>,
    ) -> Async::Future<'_, Result<Option<FetchedItems>, StorageFailure>> {
        let db = Arc::clone(&self.db);
        Async::from_future(async move {
            tokio::task::spawn_blocking(move || {
                Self::fetch_blocking(&db, tree, commit_ids, fragment_heads)
            })
            .await
            .map_err(|_join| StorageFailure::Permanent)?
            .map_err(|error| coarsen(&error))
        })
    }

    fn delete_tree(&self, tree: SedimentreeId) -> Async::Future<'_, Result<(), StorageFailure>> {
        let db = Arc::clone(&self.db);
        Async::from_future(async move {
            tokio::task::spawn_blocking(move || Self::delete_blocking(&db, tree))
                .await
                .map_err(|_join| StorageFailure::Permanent)?
                .map_err(|error| coarsen(&error))
        })
    }
}

/// `tree_id ++ item_id`, so a tree's items are one contiguous key range.
fn item_key(tree: SedimentreeId, id: CommitId) -> [u8; 64] {
    let mut key = [0u8; 64];
    key[..32].copy_from_slice(tree.as_bytes());
    key[32..].copy_from_slice(id.as_bytes());
    key
}

/// The whole key range belonging to `tree`.
fn tree_range(tree: SedimentreeId) -> core::ops::RangeInclusive<[u8; 64]> {
    let mut start = [0u8; 64];
    start[..32].copy_from_slice(tree.as_bytes());
    let mut end = [0xFFu8; 64];
    end[..32].copy_from_slice(tree.as_bytes());
    start..=end
}

/// `meta_len:u32be ++ signed_bytes ++ blob`.
fn frame<T>(signed: &Signed<T>, blob: &[u8]) -> Vec<u8>
where
    T: sedimentree_core::codec::schema::Schema
        + sedimentree_core::codec::encode::EncodeFields
        + sedimentree_core::codec::decode::DecodeFields,
{
    let meta = signed.as_bytes();
    let mut value = Vec::with_capacity(4 + meta.len() + blob.len());
    #[allow(clippy::cast_possible_truncation)] // wire messages are < 4 GiB
    value.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    value.extend_from_slice(meta);
    value.extend_from_slice(blob);
    value
}

/// Inverse of [`frame`]. `None` on a corrupt record (skipped: absent
/// items are the fetch contract; corruption is surfaced by digests
/// downstream, not invented here).
fn unframe<T>(value: &[u8]) -> Option<(Signed<T>, Vec<u8>)>
where
    T: sedimentree_core::codec::schema::Schema
        + sedimentree_core::codec::encode::EncodeFields
        + sedimentree_core::codec::decode::DecodeFields,
{
    let meta_len = u32::from_be_bytes(value.get(..4)?.try_into().ok()?) as usize;
    let meta = value.get(4..4 + meta_len)?;
    let blob = value.get(4 + meta_len..)?;
    let signed = Signed::<T>::try_decode(meta).ok()?;
    Some((signed, blob.to_vec()))
}

/// Coarsen backend errors to the protocol vocabulary.
fn coarsen(error: &redb::Error) -> StorageFailure {
    tracing::error!(%error, "redb operation failed");
    StorageFailure::Retryable
}

/// The database could not be opened or initialized.
#[derive(Debug, Error)]
pub enum OpenError {
    /// The database file could not be created or opened.
    #[error("failed to open redb database: {0}")]
    Database(#[from] redb::DatabaseError),

    /// The initial table-creating transaction failed.
    #[error("failed to initialize redb tables: {0}")]
    Initialize(#[from] redb::Error),
}
