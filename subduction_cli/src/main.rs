//! Subduction CLI

#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

mod fs_storage;

use clap::{Parser, Subcommand};
use fs_storage::FsStorage;
use sedimentree_core::commit::CountLeadingZeroBytes;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use subduction_core::peer::id::PeerId;
use subduction_websocket::{
    timeout::FuturesTimerTimeout,
    tokio::{client::TokioWebSocketClient, server::TokioWebSocketServer},
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{prelude::*, util::SubscriberInitExt, EnvFilter};
use tungstenite::http::Uri;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();

    setup_tracing();
    let token = setup_signal_handlers();

    match args.command {
        Command::Server(server_args) => run_server(server_args, token).await?,
        Command::Client { command } => run_client(command).await?,
    }

    Ok(())
}

fn setup_tracing() {
    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Only enable tokio-console if explicitly requested
    if std::env::var("TOKIO_CONSOLE").is_ok() {
        let console_filter = EnvFilter::new("tokio=trace,runtime=trace");
        let console_layer = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn();

        tracing_subscriber::registry()
            .with(console_layer.with_filter(console_filter))
            .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter))
            .init();
    }
}

fn setup_signal_handlers() -> CancellationToken {
    let token = CancellationToken::new();
    let hits = Arc::new(AtomicUsize::new(0));

    {
        let token = token.clone();
        let hits = hits.clone();
        tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_ok() {
                    if hits.fetch_add(1, Ordering::Relaxed) == 0 {
                        eprintln!("Ctrl+C — attempting graceful shutdown… (press again to force)");
                        token.cancel();
                    } else {
                        eprintln!("Force exiting.");
                        std::process::exit(130);
                    }
                }
            }
        });
    }

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let t = token.clone();
        tokio::spawn(async move {
            if let Ok(mut term) = signal(SignalKind::terminate()) {
                term.recv().await;
                eprintln!("SIGTERM — graceful shutdown…");
                t.cancel();
            }
        });
    }

    token
}

async fn run_server(args: ServerArgs, _token: CancellationToken) -> anyhow::Result<()> {
    let addr: SocketAddr = args.socket.parse()?;
    let data_dir = args.data_dir.unwrap_or_else(|| PathBuf::from("./data"));

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let storage = FsStorage::new(data_dir)?;

    let peer_id = args
        .peer_id
        .map(|s| parse_peer_id(&s))
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

    futures::future::pending::<()>().await;

    Ok(())
}

async fn run_client(command: ClientCommand) -> anyhow::Result<()> {
    match command {
        ClientCommand::Sync(args) => client_sync(args).await?,
        ClientCommand::List(args) => client_list(args).await?,
        ClientCommand::Status(args) => client_status(args).await?,
        ClientCommand::Send(args) => client_send(args).await?,
    }

    Ok(())
}

async fn client_sync(args: SyncArgs) -> anyhow::Result<()> {
    let uri: Uri = args.server.parse()?;
    let data_dir = args.data_dir.unwrap_or_else(|| PathBuf::from("./client-data"));

    tracing::info!("Initializing filesystem storage at {:?}", data_dir);
    let _storage = FsStorage::new(data_dir)?;

    let peer_id = args
        .peer_id
        .map(|s| parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 32];
            bytes[0] = 1; // Differentiate from server default
            PeerId::new(bytes)
        });

    tracing::info!("Connecting to server at {}", uri);
    let (_client, listen_fut): (TokioWebSocketClient<_>, _) =
        TokioWebSocketClient::new(uri, FuturesTimerTimeout, Duration::from_secs(5), peer_id)
            .await?;

    // Spawn listener
    tokio::spawn(async move {
        if let Err(e) = listen_fut.await {
            tracing::error!("Listener error: {}", e);
        }
    });

    tracing::info!("Connected! Peer ID: {}", peer_id);

    if let Some(tree_id_str) = args.tree_id {
        let tree_id = parse_sedimentree_id(&tree_id_str)?;
        tracing::info!("Syncing sedimentree: {}", tree_id_str);

        // TODO: Implement actual sync logic using the Subduction instance
        // For now, just demonstrate connection
        tracing::info!("Sync would happen here for tree: {:?}", tree_id);
    } else {
        tracing::info!("No tree ID specified, staying connected...");
    }

    // Keep connection alive briefly to show it works
    tokio::time::sleep(Duration::from_secs(2)).await;
    tracing::info!("Sync complete");

    Ok(())
}

async fn client_list(args: ListArgs) -> anyhow::Result<()> {
    let uri: Uri = args.server.parse()?;
    let peer_id = args
        .peer_id
        .map(|s| parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 32];
            bytes[0] = 1;
            PeerId::new(bytes)
        });

    tracing::info!("Connecting to server at {}", uri);
    let (_client, listen_fut): (TokioWebSocketClient<_>, _) =
        TokioWebSocketClient::new(uri, FuturesTimerTimeout, Duration::from_secs(5), peer_id)
            .await?;

    tokio::spawn(async move {
        if let Err(e) = listen_fut.await {
            tracing::error!("Listener error: {}", e);
        }
    });

    tracing::info!("Connected! Listing sedimentrees...");

    // TODO: Query remote sedimentrees via Subduction API
    println!("(List functionality not yet implemented)");

    Ok(())
}

async fn client_status(args: StatusArgs) -> anyhow::Result<()> {
    let uri: Uri = args.server.parse()?;
    let peer_id = args
        .peer_id
        .map(|s| parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 32];
            bytes[0] = 1;
            PeerId::new(bytes)
        });

    tracing::info!("Checking connection to {}", uri);
    match TokioWebSocketClient::new(uri.clone(), FuturesTimerTimeout, Duration::from_secs(5), peer_id).await {
        Ok((_client, _listen_fut)) => {
            println!("✓ Connected to {}", uri);
            println!("  Peer ID: {}", peer_id);
            Ok(())
        }
        Err(e) => {
            println!("✗ Failed to connect to {}", uri);
            println!("  Error: {}", e);
            Err(e.into())
        }
    }
}

async fn client_send(args: SendArgs) -> anyhow::Result<()> {
    let uri: Uri = args.server.parse()?;
    let peer_id = args
        .peer_id
        .map(|s| parse_peer_id(&s))
        .transpose()?
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 32];
            bytes[0] = 1;
            PeerId::new(bytes)
        });

    tracing::info!("Connecting to server at {}", uri);
    let (client, listen_fut): (TokioWebSocketClient<_>, _) =
        TokioWebSocketClient::new(uri, FuturesTimerTimeout, Duration::from_secs(5), peer_id)
            .await?;

    tokio::spawn(async move {
        if let Err(e) = listen_fut.await {
            tracing::error!("Listener error: {}", e);
        }
    });

    tracing::info!("Connected! Sending raw message...");
    tracing::info!("Message: {}", args.message);

    // TODO: Parse and send actual message
    println!("(Send functionality not yet implemented)");
    println!("Would send: {}", args.message);

    drop(client);
    Ok(())
}

fn parse_peer_id(s: &str) -> anyhow::Result<PeerId> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        anyhow::bail!("Peer ID must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(PeerId::new(arr))
}

fn parse_sedimentree_id(s: &str) -> anyhow::Result<sedimentree_core::SedimentreeId> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        anyhow::bail!("Sedimentree ID must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(sedimentree_core::SedimentreeId::new(arr))
}

#[derive(Debug, Parser)]
#[command(author = "Ink & Switch", version, about = "CLI for Subduction")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a Subduction server
    #[command(name = "server", alias = "start")]
    Server(ServerArgs),

    /// Run client commands
    #[command(name = "client")]
    Client {
        #[command(subcommand)]
        command: ClientCommand,
    },
}

#[derive(Debug, Parser)]
struct ServerArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    socket: String,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    data_dir: Option<PathBuf>,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    peer_id: Option<String>,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "5")]
    timeout: u64,
}

#[derive(Debug, Subcommand)]
enum ClientCommand {
    /// Sync a sedimentree with the server
    Sync(SyncArgs),

    /// List sedimentrees on the server
    List(ListArgs),

    /// Check connection status
    Status(StatusArgs),

    /// Send a raw message to the server
    Send(SendArgs),
}

#[derive(Debug, Parser)]
struct SyncArgs {
    /// Server WebSocket URL
    #[arg(short, long)]
    server: String,

    /// Sedimentree ID to sync (64 hex characters)
    #[arg(short, long)]
    tree_id: Option<String>,

    /// Data directory for filesystem storage
    #[arg(short, long)]
    data_dir: Option<PathBuf>,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    peer_id: Option<String>,
}

#[derive(Debug, Parser)]
struct ListArgs {
    /// Server WebSocket URL
    #[arg(short, long)]
    server: String,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    peer_id: Option<String>,
}

#[derive(Debug, Parser)]
struct StatusArgs {
    /// Server WebSocket URL
    #[arg(short, long)]
    server: String,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    peer_id: Option<String>,
}

#[derive(Debug, Parser)]
struct SendArgs {
    /// Server WebSocket URL
    #[arg(short, long)]
    server: String,

    /// Message to send (JSON format)
    #[arg(short, long)]
    message: String,

    /// Peer ID (64 hex characters)
    #[arg(short, long)]
    peer_id: Option<String>,
}
