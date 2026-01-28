//! WebSocket server for Subduction.

use crate::{fs_storage::FsStorage, metrics};
use anyhow::Result;
use sedimentree_core::commit::CountLeadingZeroBytes;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use subduction_core::{
    connection::nonce_cache::NonceCache,
    crypto::signer::MemorySigner,
    policy::OpenPolicy,
    storage::{MetricsStorage, RefreshMetrics},
};
use subduction_websocket::{timeout::FuturesTimerTimeout, tokio::server::TokioWebSocketServer};
use tokio_util::sync::CancellationToken;
use tungstenite::http::Uri;

/// Arguments for the server command.
#[derive(Debug, clap::Parser)]
pub(crate) struct ServerArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    pub(crate) socket: String,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Key seed (64 hex characters) for deterministic key generation.
    /// If not provided, a random key will be generated.
    #[arg(short, long)]
    pub(crate) key_seed: Option<String>,

    /// Maximum clock drift allowed during handshake (in seconds)
    #[arg(long, default_value = "600")]
    pub(crate) handshake_max_drift: u64,

    /// Service name for discovery mode (e.g., `sync.example.com`).
    /// Clients can connect without knowing the server's peer ID.
    /// The name is hashed to a 32-byte identifier for the handshake.
    /// Defaults to the socket address if not specified.
    /// Omit the protocol so the same name works across `wss://`, `https://`, etc.
    #[arg(long)]
    pub(crate) service_name: Option<String>,

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
#[allow(clippy::too_many_lines)]
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

    let signer = match &args.key_seed {
        Some(hex_seed) => {
            let seed_bytes = crate::parse_32_bytes(hex_seed, "key seed")?;
            MemorySigner::from_bytes(&seed_bytes)
        }
        None => MemorySigner::generate(),
    };
    let peer_id = signer.peer_id();

    // Default service name to socket address if not specified
    let service_name = args
        .service_name
        .clone()
        .unwrap_or_else(|| args.socket.clone());

    let server: TokioWebSocketServer<MetricsStorage<FsStorage>, OpenPolicy, MemorySigner> =
        TokioWebSocketServer::setup(
            addr,
            FuturesTimerTimeout,
            Duration::from_secs(args.timeout),
            Duration::from_secs(args.handshake_max_drift),
            signer.clone(),
            Some(service_name.as_str()),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
        )
        .await?;

    tracing::info!("WebSocket server started on {}", addr);
    tracing::info!("Peer ID: {}", peer_id);

    // Connect to configured peers for bidirectional sync (in background)
    for peer_url in &args.peers {
        let uri: Uri = match peer_url.parse() {
            Ok(uri) => uri,
            Err(e) => {
                tracing::error!("Invalid peer URL '{}': {}", peer_url, e);
                continue;
            }
        };

        let timeout_duration = Duration::from_secs(args.timeout);
        let peer_server = server.clone();
        let peer_service_name = service_name.clone();

        // Spawn connection attempt in background to avoid blocking startup
        tokio::spawn(async move {
            match peer_server
                .try_connect_discover(
                    uri.clone(),
                    FuturesTimerTimeout,
                    timeout_duration,
                    &peer_service_name,
                )
                .await
            {
                Ok(peer_id) => {
                    tracing::info!("Connected to peer at {} (peer ID: {})", uri, peer_id);
                }
                Err(e) => {
                    tracing::error!("Failed to connect to peer at {}: {}", uri, e);
                }
            }
        });
    }

    // Wait for cancellation signal
    token.cancelled().await;
    tracing::info!("Shutting down server...");

    Ok(())
}
