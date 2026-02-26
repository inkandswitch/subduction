//! Subduction client supporting WebSocket and HTTP long-poll transports.

use crate::key;
use eyre::Result;
use future_form::Sendable;
use sedimentree_fs_storage::FsStorage;
use std::{path::PathBuf, time::Duration};
use subduction_core::{
    connection::{authenticated::Authenticated, handshake::Audience},
    peer::id::PeerId,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{client::HttpLongPollClient, http_client::ReqwestHttpClient};
use subduction_websocket::{
    timeout::FuturesTimerTimeout,
    tokio::{TokioSpawn, client::TokioWebSocketClient},
};
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
    /// If omitted, uses discovery mode with the server URL as service name.
    #[arg(long)]
    pub(crate) server_peer_id: Option<String>,

    /// Service name for discovery mode.
    /// Defaults to the server URL's host if not specified.
    #[arg(long)]
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

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let _storage = FsStorage::new(data_dir)?;

    let signer = key::load_signer(&args.key)?;
    let peer_id = PeerId::from(signer.verifying_key());
    let timeout_duration = Duration::from_secs(args.timeout);

    match args.transport {
        Transport::Ws => run_ws(&args, &signer, peer_id, timeout_duration, token).await,
        Transport::Longpoll => run_longpoll(&args, &signer, peer_id, timeout_duration, token).await,
    }
}

/// Run a WebSocket client connection.
async fn run_ws(
    args: &ClientArgs,
    signer: &MemorySigner,
    peer_id: PeerId,
    timeout_duration: Duration,
    token: CancellationToken,
) -> Result<()> {
    let uri: Uri = args.server.parse()?;

    let audience = match &args.server_peer_id {
        Some(id_hex) => Audience::known(crate::parse_peer_id(id_hex)?),
        None => {
            let service = args
                .service_name
                .as_deref()
                .unwrap_or_else(|| args.server.as_str());
            Audience::discover(service.as_bytes())
        }
    };

    tracing::info!("Connecting via WebSocket to {uri}");

    type AuthenticatedClient =
        Authenticated<TokioWebSocketClient<MemorySigner, FuturesTimerTimeout>, Sendable>;

    let (_authenticated_client, listener, sender): (AuthenticatedClient, _, _) =
        TokioWebSocketClient::new(
            uri,
            FuturesTimerTimeout,
            timeout_duration,
            signer.clone(),
            audience,
        )
        .await?;

    tracing::info!("WebSocket client connected");
    tracing::info!("Client Peer ID: {peer_id}");

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
    token: CancellationToken,
) -> Result<()> {
    let base_url = args.server.trim_end_matches('/');

    tracing::info!("Connecting via HTTP long-poll to {base_url}");

    let http = ReqwestHttpClient::with_timeout(Duration::from_secs(60));
    let lp_client = HttpLongPollClient::new(
        base_url,
        http,
        FuturesTimerTimeout,
        TokioSpawn,
        timeout_duration,
    );

    let cancel = token.clone();
    let cancel_fut = async move { cancel.cancelled().await };

    let (authenticated, session_id) = match &args.server_peer_id {
        Some(id_hex) => {
            let server_id = crate::parse_peer_id(id_hex)?;
            lp_client
                .connect(signer, server_id, cancel_fut)
                .await
                .map_err(|e| eyre::eyre!("long-poll connect failed: {e}"))?
        }
        None => {
            let service = args
                .service_name
                .as_deref()
                .unwrap_or_else(|| args.server.as_str());
            lp_client
                .connect_discover(signer, service, cancel_fut)
                .await
                .map_err(|e| eyre::eyre!("long-poll discover failed: {e}"))?
        }
    };

    let remote_id = authenticated.peer_id();
    tracing::info!("HTTP long-poll connected");
    tracing::info!("Client Peer ID: {peer_id}");
    tracing::info!("Server Peer ID: {remote_id}");
    tracing::info!("Session: {session_id}");

    // The authenticated connection is ready for registration with Subduction.
    // For now, hold it alive until shutdown. A full client would create a
    // Subduction instance and call register(authenticated.map(UnifiedTransport::HttpLongPoll)).
    let _auth = authenticated;

    token.cancelled().await;
    tracing::info!("Shutting down HTTP long-poll client...");
    Ok(())
}
