//! Migrate a legacy filesystem server data dir to the redb backend.
//!
//! Reads every tree from a legacy [`FsStorage`] root and rewrites it into a
//! [`RedbStorage`] root, one tree per durable batch. Streams tree-by-tree so
//! the whole store is never held in memory at once. The server's keyhive
//! state (`.keyhive/`) is copied across too, so the destination is a complete
//! data dir the server can run against directly.
//!
//! Idempotent and resumable: `RedbStorage` registers a tree's id in the same
//! transaction as its items, so a tree already present in the destination was
//! fully written and is skipped on a re-run; keyhive files are content-addressed
//! and skipped if already present.
//!
//! `--dry-run` reports what would be copied without writing anything.
//!
//! Stop the server first — both stores must be quiescent for the migration.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use eyre::{Context, Result, ensure};
use future_form::Sendable;
use sedimentree_fs_storage::FsStorage;
use subduction_core::storage::traits::Storage;
use subduction_redb_storage::RedbStorage;

use crate::keyhive::{ARCHIVES_SUBDIR, KEYHIVE_DIR, OPS_SUBDIR};

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

    /// Report what would be migrated without writing anything to the
    /// destination.
    #[arg(long, default_value_t = false)]
    pub(crate) dry_run: bool,
}

/// Tallies from a migration run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MigrationStats {
    total: usize,
    migrated: usize,
    skipped: usize,
    commits: usize,
    fragments: usize,
    keyhive_archives: usize,
    keyhive_ops: usize,
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

    tracing::info!(
        from = %args.from.display(),
        to = %args.to.display(),
        dry_run = args.dry_run,
        "starting filesystem → redb migration"
    );

    // A dry run never opens (and so never creates) the destination.
    let dest = if args.dry_run {
        None
    } else {
        Some(RedbStorage::new(&args.to).wrap_err("open destination redb store")?)
    };

    let mut stats = migrate_all(&source, dest.as_ref(), args.progress_every).await?;

    let (archives, ops) =
        migrate_keyhive(&args.from, &args.to, args.dry_run).wrap_err("copy keyhive state")?;
    stats.keyhive_archives = archives;
    stats.keyhive_ops = ops;

    tracing::info!(
        total = stats.total,
        migrated = stats.migrated,
        skipped = stats.skipped,
        commits = stats.commits,
        fragments = stats.fragments,
        keyhive_archives = stats.keyhive_archives,
        keyhive_ops = stats.keyhive_ops,
        dry_run = args.dry_run,
        "filesystem → redb migration complete"
    );

    let keyhive = stats.keyhive_archives + stats.keyhive_ops;
    if args.dry_run {
        println!(
            "[dry run] Would migrate {} trees ({} commits, {} fragments) and {keyhive} keyhive \
             files ({} archives, {} ops). Source totals; destination not inspected.",
            stats.total, stats.commits, stats.fragments, stats.keyhive_archives, stats.keyhive_ops,
        );
    } else {
        println!(
            "Migrated {} trees ({} commits, {} fragments) and {keyhive} keyhive files \
             ({} archives, {} ops); skipped {} trees already present; {} trees total.",
            stats.migrated,
            stats.commits,
            stats.fragments,
            stats.keyhive_archives,
            stats.keyhive_ops,
            stats.skipped,
            stats.total,
        );
    }

    Ok(())
}

/// Copy every tree from `source` into `dest`, one durable batch per tree.
///
/// Skips trees already registered in `dest` (a re-run after interruption
/// resumes where it left off), since `RedbStorage` commits a tree's id and
/// items atomically.
///
/// When `dest` is `None` (a dry run) nothing is written: every source tree is
/// loaded and tallied but the destination is neither inspected nor modified,
/// so `migrated` and `skipped` stay zero and the counts reflect the source.
async fn migrate_all(
    source: &FsStorage,
    dest: Option<&RedbStorage>,
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
        let already_present = match dest {
            Some(dest) => Storage::<Sendable>::contains_sedimentree_id(dest, id)
                .await
                .wrap_err("check destination for existing tree")?,
            None => false,
        };

        if already_present {
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

            if let Some(dest) = dest {
                // One durable redb transaction per tree (atomic id + items).
                Storage::<Sendable>::save_batch(dest, id, commits, fragments)
                    .await
                    .wrap_err_with(|| format!("write tree {id:?} into redb"))?;
                stats.migrated += 1;
            }
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

/// Copy the server's keyhive state (`<from>/.keyhive/{archives,ops}/*.bin`)
/// into the destination data dir, so the migrated dir is one the server can
/// run against directly.
///
/// Files are content-addressed, so one already at the destination is skipped
/// (idempotent and resumable). The transient `tmp/` directory is not copied.
/// Returns `(archives, ops)` copied — or, under `dry_run`, the counts that
/// *would* be copied (nothing is written).
fn migrate_keyhive(from: &Path, to: &Path, dry_run: bool) -> Result<(usize, usize)> {
    let from_root = from.join(KEYHIVE_DIR);
    let to_root = to.join(KEYHIVE_DIR);

    let archives = copy_keyhive_subdir(
        &from_root.join(ARCHIVES_SUBDIR),
        &to_root.join(ARCHIVES_SUBDIR),
        dry_run,
    )
    .wrap_err("copy keyhive archives")?;
    let ops = copy_keyhive_subdir(
        &from_root.join(OPS_SUBDIR),
        &to_root.join(OPS_SUBDIR),
        dry_run,
    )
    .wrap_err("copy keyhive ops")?;

    Ok((archives, ops))
}

/// Copy every `*.bin` file from `src` to `dst`, skipping any already present
/// (content-addressed) and the source's non-`.bin` entries. Durable: each file
/// is staged to a temp, fsynced, and renamed into place, then the destination
/// directory is fsynced once. Returns the number copied — or, under `dry_run`,
/// the number that *would* be copied (nothing is written).
fn copy_keyhive_subdir(src: &Path, dst: &Path, dry_run: bool) -> io::Result<usize> {
    let entries = match fs::read_dir(src) {
        Ok(entries) => entries,
        // No keyhive state of this kind (e.g. an `--auth open` server, or a
        // store that never created the subdir) — nothing to migrate.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let mut copied = 0;
    let mut created_dst = false;

    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("bin") {
            continue;
        }
        let Some(name) = path.file_name() else {
            continue;
        };
        let target = dst.join(name);
        if target.try_exists()? {
            continue; // already migrated — content-addressed, so identical
        }

        copied += 1;
        if dry_run {
            continue;
        }

        if !created_dst {
            fs::create_dir_all(dst)?;
            created_dst = true;
        }
        durable_copy(&path, &target)?;
    }

    if created_dst {
        fsync_dir(dst)?;
    }

    Ok(copied)
}

/// Copy `src` to `dst` durably: stage to a temp file, fsync it, then rename
/// into place. The caller fsyncs the destination directory afterward.
fn durable_copy(src: &Path, dst: &Path) -> io::Result<()> {
    let tmp = dst.with_extension("bin.tmp");
    fs::copy(src, &tmp)?;
    fs::File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, dst)?;
    Ok(())
}

/// Fsync a directory so creations/renames of its entries are durable.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
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

        let stats = migrate_all(&source, Some(&dest), 1).await.expect("migrate");
        assert_eq!(
            stats,
            MigrationStats {
                total: 2,
                migrated: 2,
                skipped: 0,
                commits: 3,
                fragments: 1,
                keyhive_archives: 0,
                keyhive_ops: 0,
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
        let again = migrate_all(&source, Some(&dest), 1)
            .await
            .expect("re-migrate");
        assert_eq!(
            again,
            MigrationStats {
                total: 2,
                migrated: 0,
                skipped: 2,
                commits: 0,
                fragments: 0,
                keyhive_archives: 0,
                keyhive_ops: 0,
            }
        );
    }

    /// Seed a `.keyhive/{archives,ops}` tree under `root` with the given file
    /// stems, returning nothing — the test asserts on the copy afterward.
    fn seed_keyhive(root: &Path, archives: &[&str], ops: &[&str]) {
        for (sub, stems) in [(ARCHIVES_SUBDIR, archives), (OPS_SUBDIR, ops)] {
            let dir = root.join(KEYHIVE_DIR).join(sub);
            std::fs::create_dir_all(&dir).expect("create keyhive subdir");
            for stem in stems {
                std::fs::write(
                    dir.join(format!("{stem}.bin")),
                    format!("data-{stem}").as_bytes(),
                )
                .expect("write keyhive file");
            }
        }
        // A transient temp file that must NOT be copied.
        let tmp = root.join(KEYHIVE_DIR).join("tmp");
        std::fs::create_dir_all(&tmp).expect("create tmp dir");
        std::fs::write(tmp.join("stale.bin.tmp"), b"junk").expect("write tmp file");
    }

    /// Keyhive archives and ops are copied (skipping `tmp/`), with byte-identical
    /// content, and a re-run copies nothing (content-addressed idempotency).
    #[test]
    fn migrate_keyhive_copies_archives_ops_and_is_idempotent() {
        let from = tempfile::tempdir().expect("from tempdir");
        let to = tempfile::tempdir().expect("to tempdir");
        seed_keyhive(from.path(), &["aa", "bb"], &["cc"]);

        let (archives, ops) = migrate_keyhive(from.path(), to.path(), false).expect("copy keyhive");
        assert_eq!((archives, ops), (2, 1));

        let to_kh = to.path().join(KEYHIVE_DIR);
        for (sub, stem) in [
            (ARCHIVES_SUBDIR, "aa"),
            (ARCHIVES_SUBDIR, "bb"),
            (OPS_SUBDIR, "cc"),
        ] {
            let copied = to_kh.join(sub).join(format!("{stem}.bin"));
            assert_eq!(
                std::fs::read(&copied).expect("read copied file"),
                format!("data-{stem}").into_bytes(),
                "{} must be copied with identical content",
                copied.display()
            );
        }
        // The transient tmp file must not have been copied.
        assert!(
            !to_kh.join("tmp").join("stale.bin.tmp").exists(),
            "tmp/ must not be migrated"
        );

        // Re-run is idempotent: everything is already present.
        let (archives, ops) =
            migrate_keyhive(from.path(), to.path(), false).expect("re-copy keyhive");
        assert_eq!((archives, ops), (0, 0));
    }

    /// A dry run reports source totals (trees, commits, fragments, keyhive
    /// files) and writes absolutely nothing to the destination.
    #[tokio::test]
    async fn dry_run_writes_nothing() {
        let src_dir = tempfile::tempdir().expect("src tempdir");
        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let source = FsStorage::new(src_dir.path().to_path_buf()).expect("open fs source");
        let s = signer();

        let tree = SedimentreeId::new([0xC3; 32]);
        Storage::<Sendable>::save_batch(
            &source,
            tree,
            vec![commit(&s, tree, 0x01, 16).await],
            vec![fragment(&s, tree, 0x02, 32).await],
        )
        .await
        .expect("populate source");
        seed_keyhive(src_dir.path(), &["aa"], &["bb", "cc"]);

        // The destination directory is a fresh, empty path that must stay empty.
        let dst = dst_dir.path().join("redb-out");

        let stats = migrate_all(&source, None, 1).await.expect("dry-run trees");
        let (archives, ops) = migrate_keyhive(src_dir.path(), &dst, true).expect("dry-run keyhive");

        assert_eq!(stats.total, 1);
        assert_eq!(stats.commits, 1);
        assert_eq!(stats.fragments, 1);
        assert_eq!(stats.migrated, 0, "dry run must migrate nothing");
        assert_eq!(stats.skipped, 0, "dry run does not inspect the destination");
        assert_eq!((archives, ops), (1, 2));

        assert!(
            !dst.exists(),
            "dry run must not create the destination directory"
        );
    }
}
