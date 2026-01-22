//! WebSocket server for Subduction.

use crate::fs_storage::FsStorage;
use crate::metrics;
use anyhow::Result;
use sedimentree_core::commit::CountLeadingZeroBytes;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use subduction_core::{
    peer::id::PeerId,
    storage::{MetricsStorage, RefreshMetrics},
};
use subduction_websocket::{timeout::FuturesTimerTimeout, tokio::server::TokioWebSocketServer};
use tungstenite::http::Uri;
use tokio_util::sync::CancellationToken;

/// Arguments for the server command.
#[derive(Debug, clap::Parser)]
pub(crate) struct ServerArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    pub(crate) socket: String,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    pub(crate) peer_id: Option<String>,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    pub(crate) timeout: u64,

    /// Metrics server port (Prometheus endpoint)
    #[arg(long, default_value = "9090")]
    pub(crate) metrics_port: u16,

    /// Enable the Prometheus metrics server
    #[arg(long, default_value_t = false)]
    pub(crate) metrics: bool,

    /// Interval in seconds for refreshing storage metrics from disk
    #[arg(long, default_value_t = DEFAULT_METRICS_REFRESH_SECS)]
    pub(crate) metrics_refresh_interval: u64,

    /// Peer WebSocket URLs to connect to on startup
    #[arg(long = "peer", value_name = "URL")]
    pub(crate) peers: Vec<String>,
}

/// Default interval for refreshing storage metrics (1 minute).
const DEFAULT_METRICS_REFRESH_SECS: u64 = 60;

/// Run the WebSocket server.
pub(crate) async fn run(args: ServerArgs, token: CancellationToken) -> Result<()> {
    let addr: SocketAddr = args.socket.parse()?;
    let data_dir = args.data_dir.unwrap_or_else(|| PathBuf::from("./data"));

    // Initialize and start metrics server if enabled
    if args.metrics {
        let metrics_handle = metrics::init_metrics();
        let metrics_addr: SocketAddr = ([0, 0, 0, 0], args.metrics_port).into();
        metrics::start_metrics_server(metrics_addr, metrics_handle).await?;
    }

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let fs_storage = FsStorage::new(data_dir)?;
    let storage = MetricsStorage::new(fs_storage);

    // Initial metrics refresh and start background refresh task
    if args.metrics {
        storage.refresh_metrics().await?;

        // Spawn background task to periodically refresh metrics
        let metrics_storage = storage.clone();
        let metrics_token = token.clone();
        let refresh_interval = Duration::from_secs(args.metrics_refresh_interval);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.tick().await; // Skip immediate tick (already did initial refresh)

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = metrics_storage.refresh_metrics().await {
                            tracing::warn!("Failed to refresh storage metrics: {}", e);
                        }
                    }
                    () = metrics_token.cancelled() => {
                        tracing::debug!("Stopping metrics refresh task");
                        break;
                    }
                }
            }
        });
    }

    let peer_id = args
        .peer_id
        .map(|s| crate::parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| PeerId::new([0; 32]));

    let server: TokioWebSocketServer<MetricsStorage<FsStorage>> = TokioWebSocketServer::setup(
        addr,
        FuturesTimerTimeout,
        Duration::from_secs(args.timeout),
        peer_id,
        storage,
        CountLeadingZeroBytes,
    )
    .await?;

    tracing::info!("WebSocket server started on {}", addr);
    tracing::info!("Peer ID: {}", peer_id);

    // Connect to configured peers for bidirectional sync
    for peer_url in &args.peers {
        let uri: Uri = match peer_url.parse() {
            Ok(uri) => uri,
            Err(e) => {
                tracing::error!("Invalid peer URL '{}': {}", peer_url, e);
                continue;
            }
        };

        // Generate a peer ID from the URI (temporary until proper peer authentication)
        let remote_peer_id = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(uri.to_string().as_bytes());
            PeerId::new(*hasher.finalize().as_bytes())
        };

        let timeout_duration = Duration::from_secs(args.timeout);

        match server.connect_to_peer(uri.clone(), FuturesTimerTimeout, timeout_duration, remote_peer_id).await {
            Ok(conn_id) => {
                tracing::info!("Connected to peer at {} (connection ID: {:?})", uri, conn_id);
            }
            Err(e) => {
                tracing::error!("Failed to connect to peer at {}: {}", uri, e);
            }
        }
    }

    // Wait for cancellation signal
    token.cancelled().await;
    tracing::info!("Shutting down server...");

    Ok(())
}
