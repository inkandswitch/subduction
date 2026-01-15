//! WebSocket client for Subduction.

use crate::fs_storage::FsStorage;
use anyhow::Result;
use std::{path::PathBuf, time::Duration};
use subduction_core::peer::id::PeerId;
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

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    pub(crate) peer_id: Option<String>,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    pub(crate) timeout: u64,
}

/// Run the WebSocket client.
pub(crate) async fn run(args: ClientArgs, token: CancellationToken) -> Result<()> {
    let uri: Uri = args.server.parse()?;
    let data_dir = args.data_dir.unwrap_or_else(|| PathBuf::from("./client-data"));

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let _storage = FsStorage::new(data_dir)?;

    let peer_id = args
        .peer_id
        .map(|s| crate::parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 32];
            bytes[0] = 1; // Differentiate from server default
            PeerId::new(bytes)
        });

    tracing::info!("Connecting to WebSocket server at {}", uri);
    let (_client, listen_fut): (TokioWebSocketClient<_>, _) = TokioWebSocketClient::new(
        uri,
        FuturesTimerTimeout,
        Duration::from_secs(args.timeout),
        peer_id,
    )
    .await?;

    tracing::info!("WebSocket client connected");
    tracing::info!("Peer ID: {}", peer_id);

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
