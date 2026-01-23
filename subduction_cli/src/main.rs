//! Subduction CLI

#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

mod automerge_ephemeral_relay;
mod client;
mod fs_storage;
pub mod metrics;
mod purge;
mod server;

use clap::{Parser, Subcommand};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use subduction_core::peer::id::PeerId;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{prelude::*, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();

    setup_tracing();
    let token = setup_signal_handlers();

    match args.command {
        Command::Server(server_args) => server::run(server_args, token).await?,
        Command::Client(client_args) => client::run(client_args, token).await?,
        Command::EphemeralRelay(relay_args) => automerge_ephemeral_relay::run(relay_args, token).await?,
        Command::Purge(purge_args) => purge::run(purge_args).await?,
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

pub(crate) fn parse_peer_id(s: &str) -> anyhow::Result<PeerId> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        anyhow::bail!("Peer ID must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(PeerId::new(arr))
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

    /// Start an ephemeral message relay server for presence/awareness (automerge-repo protocol)
    #[command(name = "ephemeral-relay", alias = "relay")]
    EphemeralRelay(automerge_ephemeral_relay::EphemeralRelayArgs),

    /// Purge all storage data (destructive)
    #[command(name = "purge")]
    Purge(purge::PurgeArgs),
}
