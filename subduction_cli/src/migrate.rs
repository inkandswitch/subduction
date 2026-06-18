//! Migrate sedimentree storage from the filesystem backend to redb.
//!
//! Reads every tree from a legacy [`FsStorage`] root and rewrites it into a
//! [`RedbStorage`] root, one tree per durable batch. Streams tree-by-tree so
//! the whole store is never held in memory at once.
//!
//! Idempotent and resumable: `RedbStorage` registers a tree's id in the same
//! transaction as its items, so a tree already present in the destination was
//! fully written and is skipped on a re-run.
//!
//! Stop the server first — both stores must be quiescent for the migration.

use std::path::PathBuf;

use eyre::{Context, Result, ensure};
use future_form::Sendable;
use sedimentree_fs_storage::FsStorage;
use subduction_core::storage::traits::Storage;
use subduction_redb_storage::RedbStorage;

/// Arguments for the migrate command.
#[derive(Debug, clap::Parser)]
pub(crate) struct MigrateArgs {
    /// Source directory holding the legacy filesystem store.
    #[arg(long)]
    pub(crate) from: PathBuf,

    /// Destination directory for the redb store. Must differ from `--from`:
    /// both backends use a `blobs/` subdirectory, so an in-place migration
    /// would read from and write to the same files.
    #[arg(long)]
    pub(crate) to: PathBuf,

    /// Log a progress line every N trees processed.
    #[arg(long, default_value_t = 1000)]
    pub(crate) progress_every: usize,
}

/// Tallies from a migration run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MigrationStats {
    total: usize,
    migrated: usize,
    skipped: usize,
    commits: usize,
    fragments: usize,
}

/// Run the migrate command.
pub(crate) async fn run(args: MigrateArgs) -> Result<()> {
    ensure!(
        args.from != args.to,
        "--from and --to must differ: both backends use a `blobs/` subdirectory, \
         so an in-place migration would corrupt the source"
    );
    ensure!(
        args.from.exists(),
        "source directory does not exist: {}",
        args.from.display()
    );

    let source = FsStorage::new(args.from.clone()).wrap_err("open source filesystem store")?;
    let dest = RedbStorage::new(&args.to).wrap_err("open destination redb store")?;

    tracing::info!(
        from = %args.from.display(),
        to = %args.to.display(),
        "starting filesystem → redb migration"
    );

    let stats = migrate_all(&source, &dest, args.progress_every).await?;

    tracing::info!(
        total = stats.total,
        migrated = stats.migrated,
        skipped = stats.skipped,
        commits = stats.commits,
        fragments = stats.fragments,
        "filesystem → redb migration complete"
    );
    println!(
        "Migrated {} trees ({} commits, {} fragments); skipped {} already present; {} total.",
        stats.migrated, stats.commits, stats.fragments, stats.skipped, stats.total
    );

    Ok(())
}

/// Copy every tree from `source` into `dest`, one durable batch per tree.
///
/// Skips trees already registered in `dest` (a re-run after interruption
/// resumes where it left off), since `RedbStorage` commits a tree's id and
/// items atomically.
async fn migrate_all(
    source: &FsStorage,
    dest: &RedbStorage,
    progress_every: usize,
) -> Result<MigrationStats> {
    let progress_every = progress_every.max(1);

    let ids = Storage::<Sendable>::load_all_sedimentree_ids(source)
        .await
        .wrap_err("enumerate source sedimentrees")?;

    let mut stats = MigrationStats {
        total: ids.len(),
        ..MigrationStats::default()
    };

    for (processed, id) in ids.into_iter().enumerate() {
        if Storage::<Sendable>::contains_sedimentree_id(dest, id)
            .await
            .wrap_err("check destination for existing tree")?
        {
            stats.skipped += 1;
        } else {
            let commits = Storage::<Sendable>::load_loose_commits(source, id)
                .await
                .wrap_err_with(|| format!("load commits for {id:?}"))?;
            let fragments = Storage::<Sendable>::load_fragments(source, id)
                .await
                .wrap_err_with(|| format!("load fragments for {id:?}"))?;

            stats.commits += commits.len();
            stats.fragments += fragments.len();

            // One durable redb transaction per tree (atomic id + items).
            Storage::<Sendable>::save_batch(dest, id, commits, fragments)
                .await
                .wrap_err_with(|| format!("write tree {id:?} into redb"))?;
            stats.migrated += 1;
        }

        if (processed + 1) % progress_every == 0 {
            tracing::info!(
                processed = processed + 1,
                total = stats.total,
                migrated = stats.migrated,
                skipped = stats.skipped,
                "migration progress"
            );
        }
    }

    Ok(stats)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use sedimentree_core::{
        blob::{Blob, verified::VerifiedBlobMeta},
        crypto::digest::Digest,
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
    };
    use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

    use super::*;

    fn signer() -> MemorySigner {
        MemorySigner::from_bytes(&[7u8; 32])
    }

    async fn commit(
        s: &MemorySigner,
        id: SedimentreeId,
        head: u8,
        blob_len: usize,
    ) -> VerifiedMeta<LooseCommit> {
        VerifiedMeta::seal::<Sendable, _>(
            s,
            (id, CommitId::new([head; 32]), BTreeSet::new()),
            VerifiedBlobMeta::new(Blob::new(vec![head; blob_len])),
        )
        .await
    }

    async fn fragment(
        s: &MemorySigner,
        id: SedimentreeId,
        head: u8,
        blob_len: usize,
    ) -> VerifiedMeta<Fragment> {
        VerifiedMeta::seal::<Sendable, _>(
            s,
            (
                id,
                CommitId::new([head; 32]),
                BTreeSet::from([CommitId::new([0xF0; 32])]),
                vec![CommitId::new([0xF1; 32])],
            ),
            VerifiedBlobMeta::new(Blob::new(vec![head; blob_len])),
        )
        .await
    }

    async fn commit_digests<S>(s: &S, id: SedimentreeId) -> BTreeSet<Digest<LooseCommit>>
    where
        S: Storage<Sendable>,
    {
        Storage::<Sendable>::load_loose_commits(s, id)
            .await
            .expect("load commits")
            .iter()
            .map(|vm| Digest::hash(vm.payload()))
            .collect()
    }

    async fn fragment_digests<S>(s: &S, id: SedimentreeId) -> BTreeSet<Digest<Fragment>>
    where
        S: Storage<Sendable>,
    {
        Storage::<Sendable>::load_fragments(s, id)
            .await
            .expect("load fragments")
            .iter()
            .map(|vm| Digest::hash(vm.payload()))
            .collect()
    }

    /// Every tree migrates with byte-identical content (including an external
    /// >16 KiB blob), and a second run skips everything already present.
    #[tokio::test]
    async fn migrates_all_trees_and_is_resumable() {
        let src_dir = tempfile::tempdir().expect("src tempdir");
        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let source = FsStorage::new(src_dir.path().to_path_buf()).expect("open fs source");
        let dest = RedbStorage::new(dst_dir.path()).expect("open redb dest");
        let s = signer();

        let tree_a = SedimentreeId::new([0xA1; 32]);
        let tree_b = SedimentreeId::new([0xB2; 32]);

        // Tree A: an inline commit, an external (20 KiB > 16 KiB) commit, and
        // a fragment — exercises both the inline and external blob paths.
        let a_commits = vec![
            commit(&s, tree_a, 0x01, 16).await,
            commit(&s, tree_a, 0x02, 20 * 1024).await,
        ];
        let a_fragments = vec![fragment(&s, tree_a, 0x03, 32).await];
        Storage::<Sendable>::save_batch(&source, tree_a, a_commits, a_fragments)
            .await
            .expect("populate tree A");

        // Tree B: a single inline commit.
        let b_commits = vec![commit(&s, tree_b, 0x10, 64).await];
        Storage::<Sendable>::save_batch(&source, tree_b, b_commits, Vec::new())
            .await
            .expect("populate tree B");

        let stats = migrate_all(&source, &dest, 1).await.expect("migrate");
        assert_eq!(
            stats,
            MigrationStats {
                total: 2,
                migrated: 2,
                skipped: 0,
                commits: 3,
                fragments: 1,
            }
        );

        for id in [tree_a, tree_b] {
            assert!(
                Storage::<Sendable>::contains_sedimentree_id(&dest, id)
                    .await
                    .expect("contains"),
                "migrated tree {id:?} must be registered in the destination"
            );
            assert_eq!(
                commit_digests(&source, id).await,
                commit_digests(&dest, id).await,
                "commit content must survive migration for {id:?}"
            );
            assert_eq!(
                fragment_digests(&source, id).await,
                fragment_digests(&dest, id).await,
                "fragment content must survive migration for {id:?}"
            );
        }

        // Resumable: a second pass writes nothing.
        let again = migrate_all(&source, &dest, 1).await.expect("re-migrate");
        assert_eq!(
            again,
            MigrationStats {
                total: 2,
                migrated: 0,
                skipped: 2,
                commits: 0,
                fragments: 0,
            }
        );
    }
}
