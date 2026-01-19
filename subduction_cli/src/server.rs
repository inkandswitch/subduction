//! WebSocket server for Subduction.

use crate::fs_storage::FsStorage;
use crate::metrics;
use anyhow::Result;
use sedimentree_core::commit::CountLeadingZeroBytes;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use subduction_core::peer::id::PeerId;
use subduction_websocket::{
    timeout::FuturesTimerTimeout, tokio::server::TokioWebSocketServer,
};
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

    /// Enable the Prometheus metrics server (use --metrics=false to disable)
    #[arg(long, default_value_t = true)]
    pub(crate) metrics: bool,
}

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
    let storage = FsStorage::new(data_dir)?;

    let peer_id = args
        .peer_id
        .map(|s| crate::parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| PeerId::new([0; 32]));

    let _server: TokioWebSocketServer<FsStorage> = TokioWebSocketServer::setup(
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

    // Wait for cancellation signal
    token.cancelled().await;
    tracing::info!("Shutting down server...");

    Ok(())
}
