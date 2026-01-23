//! Purge all storage data.

use anyhow::Result;
use std::path::PathBuf;

/// Arguments for the purge command.
#[derive(Debug, clap::Parser)]
pub(crate) struct PurgeArgs {
    /// Data directory for filesystem storage
    #[arg(short, long, default_value = "./data")]
    pub(crate) data_dir: PathBuf,

    /// Skip confirmation prompt
    #[arg(short, long)]
    pub(crate) yes: bool,
}

/// Run the purge command.
pub(crate) async fn run(args: PurgeArgs) -> Result<()> {
    let data_dir = &args.data_dir;

    if !data_dir.exists() {
        println!("Storage directory does not exist: {}", data_dir.display());
        return Ok(());
    }

    let trees_dir = data_dir.join("trees");
    let blobs_dir = data_dir.join("blobs");

    // Count items to give user feedback
    let tree_count = count_items(&trees_dir);
    let blob_count = count_items(&blobs_dir);

    if tree_count == 0 && blob_count == 0 {
        println!("Storage is already empty.");
        return Ok(());
    }

    println!("Storage directory: {}", data_dir.display());
    println!("  Trees: {tree_count}");
    println!("  Blobs: {blob_count}");

    if !args.yes {
        println!();
        print!("Are you sure you want to delete all storage? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Delete trees directory
    if trees_dir.exists() {
        tokio::fs::remove_dir_all(&trees_dir).await?;
        println!("Deleted trees directory");
    }

    // Delete blobs directory
    if blobs_dir.exists() {
        tokio::fs::remove_dir_all(&blobs_dir).await?;
        println!("Deleted blobs directory");
    }

    println!("Storage purged successfully.");
    Ok(())
}

fn count_items(dir: &PathBuf) -> usize {
    if !dir.exists() {
        return 0;
    }

    std::fs::read_dir(dir)
        .map(|entries| entries.count())
        .unwrap_or(0)
}
