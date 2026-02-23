//! Key loading and persistence utilities.

use eyre::{Result, WrapErr, eyre};
use std::{
    fs,
    path::{Path, PathBuf},
};
use subduction_crypto::signer::memory::MemorySigner;

/// Common key-related arguments for CLI commands.
#[derive(Debug, clap::Args)]
pub(crate) struct KeyArgs {
    /// Key seed (64 hex characters) for deterministic key generation.
    /// Mutually exclusive with --key-file.
    #[arg(short, long, conflicts_with = "key_file")]
    pub(crate) key_seed: Option<String>,

    /// Path to a file containing the signing key seed (32 bytes, hex or raw).
    /// If the file doesn't exist, a new key will be generated and saved.
    /// Mutually exclusive with --key-seed.
    #[arg(long, conflicts_with = "key_seed")]
    pub(crate) key_file: Option<PathBuf>,
}

/// Load a signer from the configured source, or generate a new one.
///
/// Priority:
/// 1. `--key-seed`: Use the provided hex seed
/// 2. `--key-file`: Load from file, or generate and save if file doesn't exist
/// 3. Neither: Generate a random ephemeral key (warning: lost on restart)
pub(crate) fn load_or_create_signer(args: &KeyArgs) -> Result<MemorySigner> {
    if let Some(hex_seed) = &args.key_seed {
        let seed_bytes = crate::parse_32_bytes(hex_seed, "key seed")?;
        tracing::info!("Using signing key from --key-seed");
        return Ok(MemorySigner::from_bytes(&seed_bytes));
    }

    if let Some(key_path) = &args.key_file {
        return load_or_create_key_file(key_path);
    }

    // No key source specified — generate ephemeral key
    tracing::warn!(
        "No --key-seed or --key-file specified. Generating ephemeral key (will be lost on restart)."
    );
    Ok(MemorySigner::generate())
}

/// Load a signing key from a file, or create one if it doesn't exist.
fn load_or_create_key_file(path: &Path) -> Result<MemorySigner> {
    if path.exists() {
        let contents = fs::read(path)
            .wrap_err_with(|| format!("Failed to read key file: {}", path.display()))?;

        let seed_bytes = parse_key_file_contents(&contents, path)?;
        tracing::info!("Loaded signing key from {}", path.display());
        Ok(MemorySigner::from_bytes(&seed_bytes))
    } else {
        // Generate new key and save it
        let signer = MemorySigner::generate();
        let seed_bytes = signer.seed_bytes();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .wrap_err_with(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        // Write as hex for human readability
        let hex_seed = hex::encode(seed_bytes);
        fs::write(path, hex_seed.as_bytes())
            .wrap_err_with(|| format!("Failed to write key file: {}", path.display()))?;

        tracing::info!("Generated new signing key and saved to {}", path.display());
        Ok(signer)
    }
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
