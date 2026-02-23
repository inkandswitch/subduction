//! Subduction CLI

#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

mod client;
mod key;
pub mod metrics;
mod purge;
mod server;

use clap::{Parser, Subcommand};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use subduction_core::peer::id::PeerId;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, prelude::*, util::SubscriberInitExt};

#[cfg(feature = "native-tls")]
use url::Url;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    let args = Arguments::parse();

    setup_tracing();
    let token = setup_signal_handlers();

    match args.command {
        Command::Server(server_args) => server::run(server_args, token).await?,
        Command::Client(client_args) => client::run(client_args, token).await?,
        Command::Purge(purge_args) => purge::run(purge_args).await?,
    }

    Ok(())
}

fn setup_tracing() {
    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Build base registry with fmt layer
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter));

    // Optionally add tokio-console layer
    let console_layer = if std::env::var("TOKIO_CONSOLE").is_ok() {
        let console_filter = EnvFilter::new("tokio=trace,runtime=trace");
        let layer = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn();
        Some(layer.with_filter(console_filter))
    } else {
        None
    };

    // Optionally add Loki layer (native-tls only)
    // Set LOKI_URL=http://localhost:3100 to enable
    #[cfg(feature = "native-tls")]
    let (loki_layer, loki_task) = match std::env::var("LOKI_URL") {
        Ok(loki_url) => match setup_loki(&loki_url) {
            Ok((layer, task)) => {
                eprintln!("Loki tracing enabled: {loki_url}");
                (Some(layer), Some(task))
            }
            Err(e) => {
                eprintln!("Failed to initialize Loki: {e}");
                (None, None)
            }
        },
        Err(_) => (None, None),
    };

    #[cfg(not(feature = "native-tls"))]
    let loki_layer: Option<tracing_subscriber::layer::Identity> = None;

    // Initialize with all layers
    registry.with(console_layer).with(loki_layer).init();

    // Spawn Loki background task after subscriber is initialized
    #[cfg(feature = "native-tls")]
    if let Some(task) = loki_task {
        tokio::spawn(task);
    }
}

#[cfg(feature = "native-tls")]
fn setup_loki(loki_url: &str) -> eyre::Result<(tracing_loki::Layer, tracing_loki::BackgroundTask)> {
    let url = Url::parse(loki_url)?;

    let service_name = std::env::var("LOKI_SERVICE_NAME").unwrap_or_else(|_| "subduction".into());

    Ok(tracing_loki::builder()
        .label("service", service_name)?
        .label("host", hostname())?
        .extra_field("pid", std::process::id().to_string())?
        .build_url(url)?)
}

#[cfg(feature = "native-tls")]
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| "unknown".into())
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
        use tokio::signal::unix::{SignalKind, signal};
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

pub(crate) fn parse_peer_id(s: &str) -> eyre::Result<PeerId> {
    let arr = parse_32_bytes(s, "Peer ID")?;
    Ok(PeerId::new(arr))
}

pub(crate) fn parse_32_bytes(s: &str, name: &str) -> eyre::Result<[u8; 32]> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        eyre::bail!("{name} must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

#[derive(Debug, Parser)]
#[command(author = "Ink & Switch", version, about = "CLI for Subduction")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a Subduction node with WebSocket server
    #[command(name = "server", alias = "start")]
    Server(server::ServerArgs),

    /// Start a Subduction node connecting to a WebSocket server
    #[command(name = "client", alias = "connect")]
    Client(client::ClientArgs),

    /// Purge all storage data (destructive)
    #[command(name = "purge")]
    Purge(purge::PurgeArgs),
}
