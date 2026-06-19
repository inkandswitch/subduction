//! Purge all storage data.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use eyre::Result;
use subduction_redb_storage::{BLOBS_DIR_NAME, DB_FILE_NAME};

/// Arguments for the purge command.
#[derive(Debug, clap::Parser)]
pub(crate) struct PurgeArgs {
    /// Data directory for storage
    #[arg(short, long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,

    /// Skip confirmation prompt
    #[arg(short, long)]
    pub(crate) yes: bool,
}

/// Run the purge command.
///
/// Removes the redb database file (`sedimentree.redb`) and the external blob
/// directory (`blobs/`) that make up a `RedbStorage` root. Deletes the files
/// directly rather than opening the database, so a corrupt store can still be
/// purged.
///
/// Stop the server first: removing the database out from under a running
/// server leaves it operating on an unlinked inode and corrupts its view.
pub(crate) async fn run(args: PurgeArgs) -> Result<()> {
    let data_dir = &args.data_dir;

    if !data_dir.exists() {
        println!("Storage directory does not exist: {}", data_dir.display());
        return Ok(());
    }

    let db_file = data_dir.join(DB_FILE_NAME);
    let blobs_dir = data_dir.join(BLOBS_DIR_NAME);

    let db_present = db_file.exists();
    let blob_count = count_blob_files(&blobs_dir);

    if !db_present && blob_count == 0 {
        println!("Storage is already empty.");
        return Ok(());
    }

    println!("Storage directory: {}", data_dir.display());
    println!(
        "  Database: {}",
        if db_present { DB_FILE_NAME } else { "(absent)" }
    );
    println!("  External blob files: {blob_count}");

    if !args.yes {
        println!();
        println!("Stop the server before purging a live store.");
        print!("Are you sure you want to delete all storage? [y/N] ");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
            println!("Aborted.");
            return Ok(());
        }
    }

    if db_present {
        tokio::fs::remove_file(&db_file).await?;
        println!("Deleted database file");
    }

    if blobs_dir.exists() {
        tokio::fs::remove_dir_all(&blobs_dir).await?;
        println!("Deleted blobs directory");
    }

    println!("Storage purged successfully.");
    Ok(())
}

/// Count external blob files across the `blobs/{hex2}/` bucket directories
/// (the layout `RedbStorage` writes). Returns 0 if the directory is absent.
fn count_blob_files(blobs_dir: &Path) -> usize {
    let Ok(buckets) = std::fs::read_dir(blobs_dir) else {
        return 0;
    };

    buckets
        .flatten()
        .filter_map(|bucket| std::fs::read_dir(bucket.path()).ok())
        .map(Iterator::count)
        .sum()
}
