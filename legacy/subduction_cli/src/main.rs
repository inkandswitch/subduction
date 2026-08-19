//! Subduction CLI

#![cfg_attr(not(windows), allow(clippy::multiple_crate_versions))] // windows-sys

pub mod metrics;

mod admin;
mod handler;
mod inspect;
mod key;
mod keyhive;
mod migrate;
mod policy;
mod purge;
mod server;
mod transport;
mod wire;

use clap::{Parser, Subcommand};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, Layer, prelude::*, util::SubscriberInitExt};

#[cfg(feature = "native-tls")]
use url::Url;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    let args = Arguments::parse();

    setup_tracing(args.log_format);
    let token = setup_signal_handlers();

    match args.command {
        Command::Server(server_args) => server::run(*server_args, token).await?,
        Command::Migrate(migrate_args) => migrate::run(migrate_args).await?,
        Command::Inspect(inspect_args) => inspect::run(inspect_args).await?,
        Command::Purge(purge_args) => purge::run(purge_args).await?,
    }

    Ok(())
}

fn setup_tracing(log_format: LogFormat) {
    // Default to INFO (not the library-typical WARN) so lifecycle landmarks
    // show by default; `RUST_LOG` overrides.
    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // JSON carries span fields so correlation spans are queryable structurally.
    let fmt_layer = match log_format {
        LogFormat::Text => tracing_subscriber::fmt::layer().boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .boxed(),
    };

    let registry = tracing_subscriber::registry().with(fmt_layer.with_filter(fmt_filter));

    // Optionally add tokio-console layer (requires `--features tokio-console`).
    #[cfg(feature = "tokio-console")]
    let console_layer = if std::env::var("TOKIO_CONSOLE").is_ok() {
        let console_filter = EnvFilter::new("tokio=trace,runtime=trace");
        let layer = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn();
        Some(layer.with_filter(console_filter))
    } else {
        None
    };

    #[cfg(not(feature = "tokio-console"))]
    let console_layer: Option<tracing_subscriber::layer::Identity> = None;

    // Optionally add Loki layer (native-tls only). Set LOKI_URL to enable.
    // LOKI_LOG (default `info`) controls which events ship.
    #[cfg(feature = "native-tls")]
    let (loki_layer, loki_task) = match std::env::var("LOKI_URL") {
        Ok(loki_url) => match setup_loki(&loki_url) {
            Ok((layer, task)) => {
                let loki_filter =
                    EnvFilter::try_from_env("LOKI_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
                eprintln!("Loki tracing enabled: {loki_url}");
                (Some(layer.with_filter(loki_filter)), Some(task))
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
    /// Log output format. `text` is human-readable (journald/TTY); `json`
    /// emits structured lines with span fields for Loki/aggregators.
    ///
    /// The log *level* is controlled separately via `RUST_LOG` (default `info`).
    #[arg(long, value_enum, default_value_t = LogFormat::Text, env = "SUBDUCTION_LOG_FORMAT", global = true)]
    log_format: LogFormat,

    #[command(subcommand)]
    command: Command,
}

/// Log output format for the fmt subscriber layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum LogFormat {
    /// Human-readable, for `journald` / interactive terminals (the default).
    Text,

    /// Structured JSON (with span fields), for Loki / log aggregators.
    Json,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a Subduction node (WebSocket, HTTP long-poll, and/or Iroh transport)
    #[command(name = "server", alias = "start")]
    Server(Box<server::ServerArgs>),

    /// Migrate a legacy filesystem store into a redb store
    #[command(name = "migrate")]
    Migrate(migrate::MigrateArgs),

    /// Inspect a running server's store via its admin endpoint
    #[command(name = "inspect")]
    Inspect(inspect::InspectArgs),

    /// Purge all storage data (destructive)
    #[command(name = "purge")]
    Purge(purge::PurgeArgs),
}
