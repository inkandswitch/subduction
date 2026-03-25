//! CLI tool for ingesting Automerge documents into Subduction.
//!
//! Reads an `.am` file, decomposes it into sedimentree fragments and loose
//! commits, connects to a Subduction sync server via WebSocket, and uploads
//! the data.
//!
//! # Usage
//!
//! ```sh
//! automerge_subduction_ingest \
//!   --server wss://sync.example.com \
//!   --ephemeral-key \
//!   document.am
//! ```

use std::{path::PathBuf, sync::Arc};

use automerge::Automerge;
use automerge_sedimentree::ingest::{IngestResult, ingest_automerge_par};
use clap::Parser;
use eyre::{Result, WrapErr, eyre};
use future_form::Sendable;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    handshake::audience::Audience, policy::open::OpenPolicy, storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder, transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::tokio::{TimeoutTokio, TokioSpawn, client::TokioWebSocketClient};

/// Ingest an Automerge document into a Subduction sync server.
#[derive(Debug, Parser)]
#[command(name = "automerge-subduction-ingest", version, about)]
struct Args {
    /// Path to the Automerge document file (.am).
    ///
    /// The filename (minus extension) is used as the document ID by default.
    #[arg(value_name = "FILE")]
    file: PathBuf,

    /// WebSocket URL of the Subduction sync server.
    #[arg(short, long, value_name = "URL")]
    server: String,

    /// Service name for audience discovery during handshake.
    ///
    /// Must match the server's `--service-name`. Defaults to the hostname
    /// from the server URL.
    #[arg(long)]
    service_name: Option<String>,

    /// Document ID in `automerge:<base58check>` format, or raw hex (64 chars).
    ///
    /// By default, derived from the filename (minus extension) as an
    /// `automerge:` URL. The base58check-decoded bytes (typically 16-byte
    /// UUID) are zero-padded to 32 bytes for the `SedimentreeId`.
    #[arg(long, value_name = "ID")]
    doc_id: Option<String>,

    /// Key seed (64 hex characters) for deterministic key generation.
    #[arg(short, long, conflicts_with_all = ["key_file", "ephemeral_key"])]
    key_seed: Option<String>,

    /// Path to a file containing the signing key seed (32 bytes, hex or raw).
    #[arg(long, conflicts_with_all = ["key_seed", "ephemeral_key"])]
    key_file: Option<PathBuf>,

    /// Use a random ephemeral key (identity lost on exit).
    #[arg(long, conflicts_with_all = ["key_seed", "key_file"])]
    ephemeral_key: bool,

    /// Sync timeout in seconds (default: 30).
    #[arg(long, default_value = "30")]
    timeout: u64,

    /// Dry run: ingest the document and print stats but don't upload.
    #[arg(long)]
    dry_run: bool,
}

fn parse_32_bytes(hex: &str, name: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(hex)?;
    if bytes.len() != 32 {
        eyre::bail!("{name} must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn load_signer(args: &Args) -> Result<MemorySigner> {
    if let Some(hex_seed) = &args.key_seed {
        let seed = parse_32_bytes(hex_seed, "key seed")?;
        tracing::info!("using signing key from --key-seed");
        return Ok(MemorySigner::from_bytes(&seed));
    }

    if let Some(path) = &args.key_file {
        let contents = std::fs::read(path)
            .wrap_err_with(|| format!("failed to read key file: {}", path.display()))?;
        let trimmed = String::from_utf8_lossy(&contents);
        let trimmed = trimmed.trim();
        if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            let seed = parse_32_bytes(trimmed, "key file")?;
            tracing::info!("loaded signing key from {}", path.display());
            return Ok(MemorySigner::from_bytes(&seed));
        }
        if contents.len() == 32 {
            let mut seed = [0u8; 32];
            seed.copy_from_slice(&contents);
            tracing::info!("loaded signing key from {}", path.display());
            return Ok(MemorySigner::from_bytes(&seed));
        }
        eyre::bail!(
            "key file {} must contain 64 hex chars or 32 raw bytes (found {} bytes)",
            path.display(),
            contents.len()
        );
    }

    if args.ephemeral_key {
        tracing::warn!("using ephemeral key (identity lost on exit)");
        return Ok(MemorySigner::generate());
    }

    Err(eyre!(
        "no key source specified; use --key-seed, --key-file, or --ephemeral-key"
    ))
}

/// Decode a bs58check-encoded document ID into a zero-padded 32-byte
/// `SedimentreeId`.
fn sed_id_from_bs58check(encoded: &str) -> Result<SedimentreeId> {
    let decoded = bs58::decode(encoded)
        .with_check(None)
        .into_vec()
        .wrap_err_with(|| format!("invalid base58check document ID: {encoded}"))?;
    if decoded.len() > 32 {
        eyre::bail!("document ID too long: {} bytes (max 32)", decoded.len());
    }
    let mut padded = [0u8; 32];
    #[allow(clippy::indexing_slicing)] // len checked above
    padded[..decoded.len()].copy_from_slice(&decoded);
    Ok(SedimentreeId::new(padded))
}

/// Parse a document ID from various formats:
/// - `automerge:<base58check>` → decode the base58check portion
/// - Raw base58check string → decode directly
/// - 64 hex characters → decode as 32 raw bytes
fn parse_doc_id(input: &str) -> Result<SedimentreeId> {
    let stripped = input.strip_prefix("automerge:").unwrap_or(input);

    // Strip optional #heads suffix (automerge URLs can have `#head1|head2`)
    let stripped = stripped.split('#').next().unwrap_or(stripped);

    // Try hex first (64 hex chars = 32 bytes)
    if stripped.len() == 64 && stripped.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = parse_32_bytes(stripped, "doc-id")?;
        return Ok(SedimentreeId::new(bytes));
    }

    // Otherwise treat as base58check
    sed_id_from_bs58check(stripped)
}

/// Derive a document ID from the filename (minus extension), treating
/// the stem as a base58check-encoded automerge document ID.
fn sed_id_from_filename(path: &std::path::Path) -> Result<SedimentreeId> {
    let stem = path
        .file_stem()
        .ok_or_else(|| eyre!("file has no name: {}", path.display()))?
        .to_string_lossy();
    parse_doc_id(&stem)
}

fn print_ingest_stats(result: &IngestResult, sed_id: SedimentreeId) {
    eprintln!("  doc id:     {sed_id}");
    eprintln!("  changes:    {}", result.change_count);
    eprintln!("  fragments:  {}", result.fragment_count);
    eprintln!("  loose:      {}", result.loose_count);
    eprintln!("  covered:    {}", result.covered_count);

    let fragment_bytes: usize = result
        .blobs
        .iter()
        .take(result.fragment_count)
        .map(|b| b.as_slice().len())
        .sum();
    let loose_bytes: usize = result
        .blobs
        .iter()
        .skip(result.fragment_count)
        .map(|b| b.as_slice().len())
        .sum();
    eprintln!(
        "  blob bytes: {} (fragments: {}, loose: {})",
        fragment_bytes + loose_bytes,
        fragment_bytes,
        loose_bytes,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Resolve document ID.
    let sed_id = if let Some(id) = &args.doc_id {
        parse_doc_id(id)?
    } else {
        sed_id_from_filename(&args.file)?
    };

    // Load the automerge document.
    eprintln!("loading {}...", args.file.display());
    let file_bytes = std::fs::read(&args.file)
        .wrap_err_with(|| format!("failed to read {}", args.file.display()))?;
    let doc = Automerge::load(&file_bytes).wrap_err_with(|| {
        format!(
            "failed to parse automerge document: {}",
            args.file.display()
        )
    })?;
    eprintln!("loaded ({} bytes)", file_bytes.len());

    // Ingest: automerge → sedimentree.
    eprintln!("ingesting...");
    let result = ingest_automerge_par(&doc, sed_id).wrap_err("ingestion failed")?;
    print_ingest_stats(&result, sed_id);

    if args.dry_run {
        eprintln!("dry run -- skipping upload");
        return Ok(());
    }

    // Set up signer and subduction instance.
    let signer = load_signer(&args)?;

    let (subduction, _handler, listener_fut, manager_fut) =
        SubductionBuilder::<_, _, _, _, _, 256>::new()
            .signer(signer.clone())
            .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(TimeoutTokio)
            .build::<Sendable, MessageTransport<TokioWebSocketClient<MemorySigner>>>();

    tokio::spawn(async move {
        if let Err(e) = listener_fut.await {
            tracing::error!("listener task failed: {e}");
        }
    });
    tokio::spawn(async move {
        if let Err(e) = manager_fut.await {
            tracing::error!("manager task failed: {e}");
        }
    });

    // Connect to the server.
    eprintln!("connecting to {}...", args.server);
    let uri: tungstenite::http::Uri = args.server.parse().wrap_err("invalid server URL")?;

    let service_name = args
        .service_name
        .unwrap_or_else(|| uri.host().unwrap_or("localhost").to_string());
    eprintln!("service name: {service_name}");
    let audience = Audience::discover(service_name.as_bytes());

    let (authenticated, listener_task, sender_task) =
        TokioWebSocketClient::new(uri, signer, audience)
            .await
            .wrap_err("failed to connect to server")?;

    tokio::spawn(listener_task.into_future());
    tokio::spawn(sender_task.into_future());

    let server_peer_id = authenticated.peer_id();
    eprintln!("connected to peer {server_peer_id}");

    subduction
        .add_connection(authenticated.map(MessageTransport::new))
        .await
        .wrap_err("failed to register connection")?;

    // Upload the sedimentree.
    eprintln!(
        "uploading {} fragments + {} loose commits...",
        result.fragment_count, result.loose_count
    );
    let timeout_dur = std::time::Duration::from_secs(args.timeout);
    tokio::time::timeout(timeout_dur, async {
        subduction
            .add_sedimentree(sed_id, result.sedimentree, result.blobs)
            .await
            .map_err(|e| eyre!("upload failed: {e}"))
    })
    .await
    .map_err(|_| eyre!("sync timed out after {}s", args.timeout))??;

    eprintln!("upload and sync complete");

    Ok(())
}
