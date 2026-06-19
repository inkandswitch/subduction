//! Read-only inspection of a [`RedbStorage`], powering the `subduction
//! inspect` admin endpoint.
//!
//! Every query runs in a redb read transaction — MVCC, so it is concurrent
//! with the live writer and never blocks it. The server can therefore answer
//! inspection requests while serving normal traffic. These functions return
//! plain data; rendering lives in the caller.

use std::{collections::BTreeMap, path::Path};

use redb::{ReadableDatabase, ReadableTable};
use sedimentree_core::{
    blob::has_meta::HasBlobMeta,
    codec::{decode::DecodeFields, encode::EncodeFields, schema::Schema},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::signed::Signed;

use crate::{
    COMMITS, DB_FILE_NAME, FRAGMENTS, RedbStorage, RedbStorageError, TREES,
    blob_store::blob_file_path,
    codec::is_external,
    key::{digest_of, item_id_of, item_range, tree_range},
};

/// Whole-store summary.
#[derive(Debug, Clone, Copy)]
pub struct StoreStats {
    /// Registered sedimentree ids.
    pub trees: usize,

    /// Stored loose commit records (counting equivocation variants).
    pub commits: usize,

    /// Stored fragment records (counting equivocation variants).
    pub fragments: usize,

    /// Logical bytes: the sum of every [`TreeStats::bytes`]. A blob shared by
    /// several trees is counted once per referencing tree, so this can exceed
    /// the deduplicated on-disk total.
    pub logical_bytes: u64,

    /// Size of the `sedimentree.redb` file on disk.
    pub redb_file_bytes: u64,

    /// Number of external blob files under `blobs/`.
    pub blob_file_count: usize,

    /// Total size of all external blob files (deduplicated on-disk footprint).
    pub blob_total_bytes: u64,
}

/// Per-tree summary.
#[derive(Debug, Clone, Copy)]
pub struct TreeStats {
    /// The tree's id.
    pub id: SedimentreeId,

    /// Stored loose commit records.
    pub commits: usize,

    /// Stored fragment records.
    pub fragments: usize,

    /// Inline record bytes + external record metas + the sizes of the external
    /// blob files this tree references.
    pub bytes: u64,
}

/// A distinct item head and its content-digest variants. More than one variant
/// means the same id was signed over different payloads — Byzantine
/// equivocation.
#[derive(Debug, Clone)]
pub struct HeadEntry {
    /// The item id (a commit id, or a fragment head).
    pub id: CommitId,

    /// Number of stored variants (distinct content digests) under this id.
    pub variants: usize,

    /// The content digests, when requested (empty otherwise).
    pub digests: Vec<[u8; 32]>,
}

/// The commit and fragment heads of one tree.
#[derive(Debug, Clone)]
pub struct TreeHeads {
    /// Distinct commit heads.
    pub commits: Vec<HeadEntry>,

    /// Distinct fragment heads.
    pub fragments: Vec<HeadEntry>,
}

/// Whether a located head is a commit or a fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadKind {
    /// A loose commit.
    Commit,

    /// A fragment.
    Fragment,
}

/// Where a head id was found — one entry per tree-and-kind it appears in.
#[derive(Debug, Clone)]
pub struct HeadLocation {
    /// The tree containing the head.
    pub tree: SedimentreeId,

    /// Commit or fragment.
    pub kind: HeadKind,

    /// Number of stored variants under this id in this tree.
    pub variants: usize,

    /// The content digests, when requested (empty otherwise).
    pub digests: Vec<[u8; 32]>,
}

impl RedbStorage {
    /// Whole-store summary plus per-tree stats, sorted by sedimentree id.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or any table scan fails.
    pub async fn inspect_overview(&self) -> Result<(StoreStats, Vec<TreeStats>), RedbStorageError> {
        self.with_db(|db, blobs_dir| {
            let read = db.begin_read()?;
            let trees = read.open_table(TREES)?;
            let commits = read.open_table(COMMITS)?;
            let fragments = read.open_table(FRAGMENTS)?;

            let mut per_tree = Vec::new();
            for entry in trees.iter()? {
                let (key, _) = entry?;
                let id = SedimentreeId::new(*key.value());
                per_tree.push(scan_tree(&commits, &fragments, id, blobs_dir)?);
            }
            per_tree.sort_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));

            let (blob_file_count, blob_total_bytes) = blob_dir_totals(blobs_dir);
            let redb_file_bytes = blobs_dir
                .parent()
                .map(|root| root.join(DB_FILE_NAME))
                .and_then(|p| std::fs::metadata(p).ok())
                .map_or(0, |m| m.len());

            let store = StoreStats {
                trees: per_tree.len(),
                commits: per_tree.iter().map(|t| t.commits).sum(),
                fragments: per_tree.iter().map(|t| t.fragments).sum(),
                logical_bytes: per_tree.iter().map(|t| t.bytes).sum(),
                redb_file_bytes,
                blob_file_count,
                blob_total_bytes,
            };
            Ok((store, per_tree))
        })
        .await
    }

    /// Stats for a single tree, or `None` if the id is not registered.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or any table scan fails.
    pub async fn inspect_tree(
        &self,
        id: SedimentreeId,
    ) -> Result<Option<TreeStats>, RedbStorageError> {
        self.with_db(move |db, blobs_dir| {
            let read = db.begin_read()?;
            let trees = read.open_table(TREES)?;
            if trees.get(id.as_bytes())?.is_none() {
                return Ok(None);
            }
            let commits = read.open_table(COMMITS)?;
            let fragments = read.open_table(FRAGMENTS)?;
            Ok(Some(scan_tree(&commits, &fragments, id, blobs_dir)?))
        })
        .await
    }

    /// The commit and fragment heads of a tree, deduped with variant counts.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or any table scan fails.
    pub async fn inspect_tree_heads(
        &self,
        id: SedimentreeId,
        with_digests: bool,
    ) -> Result<TreeHeads, RedbStorageError> {
        self.with_db(move |db, _| {
            let read = db.begin_read()?;
            let commits = read.open_table(COMMITS)?;
            let fragments = read.open_table(FRAGMENTS)?;
            Ok(TreeHeads {
                commits: heads(&commits, id, with_digests)?,
                fragments: heads(&fragments, id, with_digests)?,
            })
        })
        .await
    }

    /// Every tree's heads, sorted by sedimentree id. Potentially large — for
    /// dumping the whole store while debugging.
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or any table scan fails.
    pub async fn inspect_all_heads(
        &self,
        with_digests: bool,
    ) -> Result<Vec<(SedimentreeId, TreeHeads)>, RedbStorageError> {
        self.with_db(move |db, _| {
            let read = db.begin_read()?;
            let trees = read.open_table(TREES)?;
            let commits = read.open_table(COMMITS)?;
            let fragments = read.open_table(FRAGMENTS)?;

            let mut out = Vec::new();
            for entry in trees.iter()? {
                let (key, _) = entry?;
                let id = SedimentreeId::new(*key.value());
                out.push((
                    id,
                    TreeHeads {
                        commits: heads(&commits, id, with_digests)?,
                        fragments: heads(&fragments, id, with_digests)?,
                    },
                ));
            }
            out.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            Ok(out)
        })
        .await
    }

    /// Locate a head id across every tree (both commits and fragments).
    ///
    /// # Errors
    ///
    /// Returns an error if the read transaction or any table scan fails.
    pub async fn inspect_find_head(
        &self,
        head: CommitId,
        with_digests: bool,
    ) -> Result<Vec<HeadLocation>, RedbStorageError> {
        self.with_db(move |db, _| {
            let read = db.begin_read()?;
            let trees = read.open_table(TREES)?;
            let commits = read.open_table(COMMITS)?;
            let fragments = read.open_table(FRAGMENTS)?;

            let mut out = Vec::new();
            for entry in trees.iter()? {
                let (key, _) = entry?;
                let tree = SedimentreeId::new(*key.value());
                if let Some(loc) = locate(&commits, tree, head, HeadKind::Commit, with_digests)? {
                    out.push(loc);
                }
                if let Some(loc) = locate(&fragments, tree, head, HeadKind::Fragment, with_digests)?
                {
                    out.push(loc);
                }
            }
            Ok(out)
        })
        .await
    }
}

/// Count records and sum logical bytes for one tree across both tables.
fn scan_tree(
    commits: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    fragments: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    id: SedimentreeId,
    blobs_dir: &Path,
) -> Result<TreeStats, RedbStorageError> {
    let (commits, c_bytes) = table_count_bytes::<LooseCommit>(commits, id, blobs_dir)?;
    let (fragments, f_bytes) = table_count_bytes::<Fragment>(fragments, id, blobs_dir)?;
    Ok(TreeStats {
        id,
        commits,
        fragments,
        bytes: c_bytes + f_bytes,
    })
}

/// Count records and sum bytes (inline value bytes + external blob file sizes)
/// for one tree in one table. A missing external blob file contributes zero.
fn table_count_bytes<T>(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    id: SedimentreeId,
    blobs_dir: &Path,
) -> Result<(usize, u64), RedbStorageError>
where
    T: HasBlobMeta + Schema + EncodeFields + DecodeFields,
{
    let (lo, hi) = tree_range(id);
    let mut count = 0;
    let mut bytes = 0u64;

    for entry in table.range::<&[u8; 96]>(&lo..=&hi)? {
        let (_, value) = entry?;
        let v = value.value();
        count += 1;
        bytes += u64::try_from(v.len()).unwrap_or(u64::MAX);

        // External records keep only the meta in the db; add the blob file.
        if is_external(v) == Some(true)
            && let Some(meta) = v.get(1..)
            && let Ok(payload) =
                Signed::<T>::try_decode(meta).and_then(|signed| signed.try_decode_trusted_payload())
        {
            let path = blob_file_path(blobs_dir, payload.blob_meta().digest().as_bytes());
            bytes += std::fs::metadata(path).map_or(0, |m| m.len());
        }
    }

    Ok((count, bytes))
}

/// Distinct heads in one table under a tree, deduped with variant counts and
/// (optionally) their content digests.
fn heads(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    id: SedimentreeId,
    with_digests: bool,
) -> Result<Vec<HeadEntry>, RedbStorageError> {
    let (lo, hi) = tree_range(id);
    let mut grouped: BTreeMap<[u8; 32], (usize, Vec<[u8; 32]>)> = BTreeMap::new();

    for entry in table.range::<&[u8; 96]>(&lo..=&hi)? {
        let (key, _) = entry?;
        let k = key.value();
        let slot = grouped.entry(*item_id_of(k).as_bytes()).or_default();
        slot.0 += 1;
        if with_digests {
            slot.1.push(digest_of(k));
        }
    }

    Ok(grouped
        .into_iter()
        .map(|(id, (variants, digests))| HeadEntry {
            id: CommitId::new(id),
            variants,
            digests,
        })
        .collect())
}

/// Find a single head id in one table of one tree (a contiguous key range).
fn locate(
    table: &impl ReadableTable<&'static [u8; 96], &'static [u8]>,
    tree: SedimentreeId,
    head: CommitId,
    kind: HeadKind,
    with_digests: bool,
) -> Result<Option<HeadLocation>, RedbStorageError> {
    let (lo, hi) = item_range(tree, head);
    let mut variants = 0;
    let mut digests = Vec::new();

    for entry in table.range::<&[u8; 96]>(&lo..=&hi)? {
        let (key, _) = entry?;
        variants += 1;
        if with_digests {
            digests.push(digest_of(key.value()));
        }
    }

    if variants == 0 {
        Ok(None)
    } else {
        Ok(Some(HeadLocation {
            tree,
            kind,
            variants,
            digests,
        }))
    }
}

/// Walk `blobs/{bucket}/{file}` and return `(file count, total bytes)`.
fn blob_dir_totals(blobs_dir: &Path) -> (usize, u64) {
    let mut count = 0;
    let mut bytes = 0u64;

    let Ok(buckets) = std::fs::read_dir(blobs_dir) else {
        return (0, 0);
    };
    for bucket in buckets.flatten() {
        let Ok(files) = std::fs::read_dir(bucket.path()) else {
            continue;
        };
        for file in files.flatten() {
            if let Ok(meta) = file.metadata()
                && meta.is_file()
            {
                count += 1;
                bytes += meta.len();
            }
        }
    }

    (count, bytes)
}
