//! CLI tool for ingesting Automerge documents into Subduction.
//!
//! Reads an `.am` file, decomposes it into sedimentree fragments and loose
//! commits, connects to a Subduction sync server via WebSocket, and uploads
//! the data.
//!
//! # Usage
//!
//! ```sh
//! subduction_ingest \
//!   --server wss://sync.example.com \
//!   --ephemeral-key \
//!   document.am
//! ```

use std::{path::PathBuf, sync::Arc, time::Duration};

use automerge::Automerge;
use automerge_sedimentree::ingest::{ingest_automerge, IngestResult};
use clap::Parser;
use eyre::{eyre, Result, WrapErr};
use future_form::Sendable;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    handshake::audience::Audience, policy::open::OpenPolicy, storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder, transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::tokio::{client::TokioWebSocketClient, TimeoutTokio, TokioSpawn};

/// Ingest an Automerge document into a Subduction sync server.
#[derive(Debug, Parser)]
#[command(name = "subduction-ingest", version, about)]
struct Args {
    /// Path to the Automerge document file (.am).
    ///
    /// The filename (minus extension) is used as the document ID by default.
    #[arg(value_name = "FILE")]
    file: PathBuf,

    /// WebSocket URL of the Subduction sync server.
    #[arg(short, long, value_name = "URL")]
    server: String,

    /// Override the document ID (64 hex chars = 32 bytes).
    ///
    /// By default, the sedimentree ID is derived from a blake3 hash of the
    /// filename (minus extension).
    #[arg(long, value_name = "HEX")]
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

/// Derive a SedimentreeId from the filename (minus extension).
fn sed_id_from_filename(path: &PathBuf) -> Result<SedimentreeId> {
    let stem = path
        .file_stem()
        .ok_or_else(|| eyre!("file has no name: {}", path.display()))?
        .to_string_lossy();
    let hash = blake3::hash(stem.as_bytes());
    Ok(SedimentreeId::new(*hash.as_bytes()))
}

fn print_ingest_stats(result: &IngestResult, sed_id: SedimentreeId) {
    eprintln!("  doc id:     {sed_id}");
    eprintln!("  changes:    {}", result.change_count);
    eprintln!("  fragments:  {}", result.fragment_count);
    eprintln!("  loose:      {}", result.loose_count);
    eprintln!("  covered:    {}", result.covered_count);
    let blob_bytes: usize = result.blobs.iter().map(|b| b.as_slice().len()).sum();
    eprintln!("  blob bytes: {blob_bytes}");
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Resolve document ID.
    let sed_id = if let Some(hex) = &args.doc_id {
        let bytes = parse_32_bytes(hex, "doc-id")?;
        SedimentreeId::new(bytes)
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
    let result = ingest_automerge(&doc, sed_id).wrap_err("ingestion failed")?;
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
    let audience = Audience::discover(b"subduction");

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
    subduction
        .add_sedimentree(sed_id, result.sedimentree, result.blobs)
        .await
        .map_err(|e| eyre!("upload failed: {e}"))?;

    eprintln!("upload complete, syncing...");

    let timeout = Duration::from_secs(args.timeout);
    let (had_success, stats, _call_errs, _io_errs) =
        subduction.full_sync_with_all_peers(Some(timeout)).await;

    eprintln!("sync complete:");
    eprintln!("  success:        {had_success}");
    eprintln!("  fragments sent: {}", stats.fragments_sent);
    eprintln!("  commits sent:   {}", stats.commits_sent);

    Ok(())
}
