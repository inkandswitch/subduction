//! Key loading utilities.

use eyre::{Result, WrapErr, eyre};
use rand::{RngCore, rngs::OsRng};
use std::{
    fs,
    path::{Path, PathBuf},
};
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

/// Obtain a 32-byte signing-key seed, loaded from a file or hex string,
/// or generated fresh when `--ephemeral-key` is set.
///
/// Shared bottom layer for `signer_from_seed` and `keyhive_signer_from_seed`
/// so the subduction and keyhive signers always derive from the same key
/// material.
pub(crate) fn resolve_key_seed(args: &KeyArgs) -> Result<[u8; 32]> {
    if let Some(hex_seed) = &args.key_seed {
        let seed_bytes = crate::parse_32_bytes(hex_seed, "key seed")?;
        tracing::info!("Using signing key from --key-seed");
        return Ok(seed_bytes);
    }

    if let Some(key_path) = &args.key_file {
        return load_key_file_bytes(key_path);
    }

    if args.ephemeral_key {
        tracing::warn!("Using ephemeral key (will be lost on restart)");
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        return Ok(bytes);
    }

    Err(eyre!(
        "No key source specified. Use one of:\n  \
         --key-file <PATH>   Load key from file\n  \
         --key-seed <HEX>    Key from hex seed\n  \
         --ephemeral-key     Random key (lost on restart)"
    ))
}

/// Build a subduction signer from a 32-byte seed.
pub(crate) fn signer_from_seed(seed: &[u8; 32]) -> subduction_crypto::signer::memory::MemorySigner {
    subduction_crypto::signer::memory::MemorySigner::from_bytes(seed)
}

/// Build a keyhive signer from a 32-byte seed.
pub(crate) fn keyhive_signer_from_seed(
    seed: &[u8; 32],
) -> keyhive_crypto::signer::memory::MemorySigner {
    ed25519_dalek::SigningKey::from_bytes(seed).into()
}

/// Read and parse a key-file path into raw 32-byte seed material.
fn load_key_file_bytes(path: &Path) -> Result<[u8; 32]> {
    let contents =
        fs::read(path).wrap_err_with(|| format!("Failed to read key file: {}", path.display()))?;

    let seed_bytes = parse_key_file_contents(&contents, path)?;
    tracing::info!(path = %path.display(), "Loaded signing key");
    Ok(seed_bytes)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signers_from_same_seed_have_matching_identities() {
        use keyhive_crypto::verifiable::Verifiable;

        let seed = [42u8; 32];
        let signer = signer_from_seed(&seed);
        let kh_signer = keyhive_signer_from_seed(&seed);

        let subduction_vk = signer.verifying_key();
        let keyhive_vk = kh_signer.verifying_key();
        assert_eq!(
            subduction_vk.as_bytes(),
            keyhive_vk.as_bytes(),
            "subduction and keyhive signers must derive the same public key"
        );
    }
}
