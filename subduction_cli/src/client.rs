//! WebSocket client for Subduction.

use crate::fs_storage::FsStorage;
use anyhow::Result;
use std::{path::PathBuf, time::Duration};
use subduction_core::crypto::signer::{LocalSigner, Signer};
use subduction_websocket::{timeout::FuturesTimerTimeout, tokio::client::TokioWebSocketClient};
use tokio_util::sync::CancellationToken;
use tungstenite::http::Uri;

/// Arguments for the client command.
#[derive(Debug, clap::Parser)]
pub(crate) struct ClientArgs {
    /// Server WebSocket URL to connect to
    #[arg(short, long)]
    pub(crate) server: String,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Key seed (64 hex characters) for deterministic key generation.
    /// If not provided, a random key will be generated.
    #[arg(short, long)]
    pub(crate) key_seed: Option<String>,

    /// Expected server peer ID (64 hex characters).
    /// Required to verify the server's identity during handshake.
    #[arg(long)]
    pub(crate) server_peer_id: String,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    pub(crate) timeout: u64,
}

/// Run the WebSocket client.
pub(crate) async fn run(args: ClientArgs, token: CancellationToken) -> Result<()> {
    let uri: Uri = args.server.parse()?;
    let data_dir = args
        .data_dir
        .unwrap_or_else(|| PathBuf::from("./client-data"));

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let _storage = FsStorage::new(data_dir)?;

    let signer = match &args.key_seed {
        Some(hex_seed) => {
            let seed_bytes = crate::parse_32_bytes(hex_seed, "key seed")?;
            LocalSigner::from_bytes(&seed_bytes)
        }
        None => LocalSigner::generate(),
    };
    let peer_id = signer.peer_id();

    let server_peer_id = crate::parse_peer_id(&args.server_peer_id)?;

    tracing::info!("Connecting to WebSocket server at {}", uri);
    let (_client, listen_fut): (TokioWebSocketClient<LocalSigner, _>, _) =
        TokioWebSocketClient::new(
            uri,
            FuturesTimerTimeout,
            Duration::from_secs(args.timeout),
            signer,
            server_peer_id,
        )
        .await?;

    tracing::info!("WebSocket client connected");
    tracing::info!("Client Peer ID: {}", peer_id);
    tracing::info!("Server Peer ID: {}", server_peer_id);

    // Spawn the listener task
    let listener_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = listen_fut => {
                if let Err(e) = result {
                    tracing::error!("Listener error: {}", e);
                }
            }
            () = listener_token.cancelled() => {
                tracing::info!("Listener shutting down...");
            }
        }
    });

    // Wait for cancellation signal
    token.cancelled().await;
    tracing::info!("Shutting down client...");

    Ok(())
}
