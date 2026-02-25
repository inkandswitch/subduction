//! Key loading utilities.

use eyre::{eyre, Result, WrapErr};
use std::{
    fs,
    path::{Path, PathBuf},
};
use subduction_crypto::signer::memory::MemorySigner;

/// Common key-related arguments for CLI commands.
#[derive(Debug, clap::Args)]
pub(crate) struct KeyArgs {
    /// Key seed (64 hex characters) for deterministic key generation.
    /// Mutually exclusive with --key-file and --ephemeral-key.
    #[arg(short, long, conflicts_with_all = ["key_file", "ephemeral_key"])]
    pub(crate) key_seed: Option<String>,

    /// Path to a file containing the signing key seed (32 bytes, hex or raw).
    /// The file must already exist.
    /// Mutually exclusive with --key-seed and --ephemeral-key.
    #[arg(long, conflicts_with_all = ["key_seed", "ephemeral_key"])]
    pub(crate) key_file: Option<PathBuf>,

    /// Use a random ephemeral key (lost on restart).
    /// Mutually exclusive with --key-seed and --key-file.
    #[arg(long, conflicts_with_all = ["key_seed", "key_file"])]
    pub(crate) ephemeral_key: bool,
}

/// Load a signer from the configured source.
///
/// Requires one of:
/// - `--key-seed`: Use the provided hex seed
/// - `--key-file`: Load from an existing file
/// - `--ephemeral-key`: Generate a random ephemeral key (lost on restart)
pub(crate) fn load_signer(args: &KeyArgs) -> Result<MemorySigner> {
    if let Some(hex_seed) = &args.key_seed {
        let seed_bytes = crate::parse_32_bytes(hex_seed, "key seed")?;
        tracing::info!("Using signing key from --key-seed");
        return Ok(MemorySigner::from_bytes(&seed_bytes));
    }

    if let Some(key_path) = &args.key_file {
        return load_key_file(key_path);
    }

    if args.ephemeral_key {
        tracing::warn!("Using ephemeral key (will be lost on restart)");
        return Ok(MemorySigner::generate());
    }

    Err(eyre!(
        "No key source specified. Use one of:\n  \
         --key-file <PATH>   Load key from file\n  \
         --key-seed <HEX>    Key from hex seed\n  \
         --ephemeral-key     Random key (lost on restart)"
    ))
}

/// Load a signing key from an existing file.
fn load_key_file(path: &Path) -> Result<MemorySigner> {
    let contents =
        fs::read(path).wrap_err_with(|| format!("Failed to read key file: {}", path.display()))?;

    let seed_bytes = parse_key_file_contents(&contents, path)?;
    tracing::info!("Loaded signing key from {}", path.display());
    Ok(MemorySigner::from_bytes(&seed_bytes))
}

/// Parse key file contents as either hex or raw bytes.
fn parse_key_file_contents(contents: &[u8], path: &Path) -> Result<[u8; 32]> {
    // Try parsing as hex first (64 hex chars = 32 bytes)
    let trimmed = String::from_utf8_lossy(contents);
    let trimmed = trimmed.trim();

    if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut seed = [0u8; 32];
        hex::decode_to_slice(trimmed, &mut seed)
            .wrap_err_with(|| format!("Invalid hex in key file: {}", path.display()))?;
        return Ok(seed);
    }

    // Try as raw 32 bytes
    if contents.len() == 32 {
        let mut seed = [0u8; 32];
        seed.copy_from_slice(contents);
        return Ok(seed);
    }

    Err(eyre!(
        "Key file {} must contain either 64 hex characters or 32 raw bytes (found {} bytes)",
        path.display(),
        contents.len()
    ))
}
