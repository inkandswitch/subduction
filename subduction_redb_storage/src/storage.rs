//! `impl Storage<Async> for RedbStorage` for both [`Sendable`] and [`Local`]
//! future forms.
//!
//! The implementation is written once, generic over the [`FutureForm`], and
//! the [`future_form`] macro expands it into the two concrete impls — so the
//! `Local` form is the same code, not a hand-written delegation. Every method
//! runs its blocking redb/file work inside
//! [`RedbStorage::with_db`](crate::RedbStorage) on the blocking pool.

use std::sync::Arc;

use future_form::{FutureForm, Local, Sendable, future_form};
use redb::{ReadableDatabase, ReadableTable};
use sedimentree_core::{
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::verified_meta::VerifiedMeta;

use crate::{
    COMMITS, FRAGMENTS, RedbStorage, RedbStorageError, TREES,
    insert::{insert_compound, pending_commit, pending_fragment, stage_external_blobs_sync},
    key::{item_range, tree_range},
    scan::{delete_range, resolve_items, scan_decoded, scan_ids, scan_payloads},
};

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Storage<Async> for RedbStorage {
    type Error = RedbStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<bool, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::save_loose_commit");
            let pending = pending_commit(sedimentree_id, verified);
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
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
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

    fn load_loose_commit_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::load_loose_commit_metas");
            // One blocking hop: B+tree range scan decoding payloads only — no
            // inline blob copies; external blobs only `stat`-ed, never read.
            self.with_db(move |db, blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_read()?;
                scan_payloads::<LooseCommit>(
                    &txn.open_table(COMMITS)?,
                    &lo,
                    &hi,
                    blobs_dir,
                    "loose commit",
                )
            })
            .await
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::save_fragment");
            let pending = pending_fragment(sedimentree_id, verified);
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
    ) -> Async::Future<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<Set<CommitId>, Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Async::from_future(async move {
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

    fn load_fragment_metas(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<Fragment>, Self::Error>> {
        Async::from_future(async move {
            tracing::trace!(?sedimentree_id, "RedbStorage::load_fragment_metas");
            self.with_db(move |db, blobs_dir| {
                let (lo, hi) = tree_range(sedimentree_id);
                let txn = db.begin_read()?;
                scan_payloads::<Fragment>(
                    &txn.open_table(FRAGMENTS)?,
                    &lo,
                    &hi,
                    blobs_dir,
                    "fragment",
                )
            })
            .await
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
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
    ) -> Async::Future<'_, Result<usize, Self::Error>> {
        Async::from_future(async move {
            let num_commits = commits.len();
            let num_fragments = fragments.len();
            tracing::trace!(
                ?sedimentree_id,
                num_commits,
                num_fragments,
                "RedbStorage::save_batch"
            );

            let pending_commits: Vec<_> = commits
                .into_iter()
                .map(|v| pending_commit(sedimentree_id, v))
                .collect();
            let pending_fragments: Vec<_> = fragments
                .into_iter()
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
