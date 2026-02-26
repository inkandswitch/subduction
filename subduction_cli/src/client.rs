//! Subduction client supporting WebSocket and HTTP long-poll transports.

use crate::key;
use eyre::Result;
use sedimentree_fs_storage::FsStorage;
use std::{path::PathBuf, time::Duration};
use subduction_core::{
    connection::handshake::Audience, peer::id::PeerId, timestamp::TimestampSeconds,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{client::HttpLongPollClient, http_client::ReqwestHttpClient};
use subduction_websocket::{timeout::FuturesTimerTimeout, tokio::client::TokioWebSocketClient};
use tokio_util::sync::CancellationToken;
use tungstenite::http::Uri;

/// Transport selection for the client.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub(crate) enum Transport {
    /// WebSocket (default)
    #[default]
    Ws,
    /// HTTP long-poll
    Longpoll,
}

/// Arguments for the client command.
#[derive(Debug, clap::Parser)]
pub(crate) struct ClientArgs {
    /// Server URL to connect to
    #[arg(short, long)]
    pub(crate) server: String,

    /// Transport to use for the connection
    #[arg(long, value_enum, default_value_t = Transport::Ws)]
    pub(crate) transport: Transport,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    pub(crate) data_dir: Option<PathBuf>,

    #[command(flatten)]
    pub(crate) key: key::KeyArgs,

    /// Expected server peer ID (64 hex characters).
    /// If omitted, uses discovery mode instead.
    #[arg(long, conflicts_with = "service_name")]
    pub(crate) server_peer_id: Option<String>,

    /// Service name for discovery mode.
    /// Defaults to the server URL if not specified.
    #[arg(long, conflicts_with = "server_peer_id")]
    pub(crate) service_name: Option<String>,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    pub(crate) timeout: u64,
}

/// Run the client.
pub(crate) async fn run(args: ClientArgs, token: CancellationToken) -> Result<()> {
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./client-data"));

    tracing::info!(?data_dir, "Initializing filesystem storage");
    let _storage = FsStorage::new(data_dir)?;

    let signer = key::load_signer(&args.key)?;
    let peer_id = PeerId::from(signer.verifying_key());
    let timeout_duration = Duration::from_secs(args.timeout);

    let audience = if let Some(id_hex) = &args.server_peer_id {
        Audience::known(crate::parse_peer_id(id_hex)?)
    } else {
        let service = args.service_name.as_deref().unwrap_or(args.server.as_str());
        Audience::discover(service.as_bytes())
    };

    match args.transport {
        Transport::Ws => run_ws(&args, &signer, peer_id, timeout_duration, audience, token).await,
        Transport::Longpoll => {
            run_longpoll(&args, &signer, peer_id, timeout_duration, audience, token).await
        }
    }
}

/// Run a WebSocket client connection.
async fn run_ws(
    args: &ClientArgs,
    signer: &MemorySigner,
    peer_id: PeerId,
    timeout_duration: Duration,
    audience: Audience,
    token: CancellationToken,
) -> Result<()> {
    let uri: Uri = args.server.parse()?;

    tracing::info!("Connecting via WebSocket to {uri}");

    // TODO: create a Subduction instance and call register(authenticated)
    let (_auth, listener, sender) = TokioWebSocketClient::new(
        uri,
        FuturesTimerTimeout,
        timeout_duration,
        signer.clone(),
        audience,
    )
    .await?;

    let remote_id = _auth.peer_id();
    tracing::info!("WebSocket client connected");
    tracing::info!("Client Peer ID: {peer_id}");
    tracing::info!("Server Peer ID: {remote_id}");

    let listener_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = listener => {
                if let Err(e) = result {
                    tracing::error!("Listener error: {e}");
                }
            }
            () = listener_token.cancelled() => {
                tracing::info!("Listener shutting down...");
            }
        }
    });

    let sender_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = sender => {
                if let Err(e) = result {
                    tracing::error!("Sender error: {e}");
                }
            }
            () = sender_token.cancelled() => {
                tracing::info!("Sender shutting down...");
            }
        }
    });

    token.cancelled().await;
    tracing::info!("Shutting down WebSocket client...");
    Ok(())
}

/// Run an HTTP long-poll client connection.
async fn run_longpoll(
    args: &ClientArgs,
    signer: &MemorySigner,
    peer_id: PeerId,
    timeout_duration: Duration,
    audience: Audience,
    token: CancellationToken,
) -> Result<()> {
    let base_url = args.server.trim_end_matches('/');

    tracing::info!("Connecting via HTTP long-poll to {base_url}");

    let http = ReqwestHttpClient::with_timeout(Duration::from_secs(60));
    let lp_client = HttpLongPollClient::new(base_url, http, FuturesTimerTimeout, timeout_duration);

    let result = match &audience {
        Audience::Known(server_id) => lp_client
            .connect(signer, *server_id, TimestampSeconds::now())
            .await
            .map_err(|e| eyre::eyre!("long-poll connect failed: {e}"))?,
        Audience::Discover(_) => {
            let service = args.service_name.as_deref().unwrap_or(args.server.as_str());
            lp_client
                .connect_discover(signer, service, TimestampSeconds::now())
                .await
                .map_err(|e| eyre::eyre!("long-poll discover failed: {e}"))?
        }
    };

    let remote_id = result.authenticated.peer_id();
    tracing::info!("HTTP long-poll connected");
    tracing::info!("Client Peer ID: {peer_id}");
    tracing::info!("Server Peer ID: {remote_id}");
    tracing::info!("Session: {}", result.session_id);

    // Spawn background tasks
    let poll_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            () = result.poll_task => {}
            () = poll_token.cancelled() => {
                tracing::info!("Poll task shutting down...");
            }
        }
    });

    let send_token = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            () = result.send_task => {}
            () = send_token.cancelled() => {
                tracing::info!("Send task shutting down...");
            }
        }
    });

    // TODO: create a Subduction instance and call register(authenticated.map(UnifiedTransport::HttpLongPoll))
    let _auth = result.authenticated;

    token.cancelled().await;
    tracing::info!("Shutting down HTTP long-poll client...");
    Ok(())
}
