//! Subduction CLI

use clap::Parser;
use sedimentree_core::{commit::CountLeadingZeroBytes, storage::MemoryStorage};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use subduction_core::peer::id::PeerId;
use subduction_websocket::tokio::server::TokioWebSocketServer;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{prelude::*, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let console_filter = EnvFilter::new("tokio=trace,runtime=trace");

    let console_layer = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .spawn();

    tracing_subscriber::registry()
        .with(console_layer.with_filter(console_filter))
        .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter))
        .init();

    let token = CancellationToken::new();
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let token = token.clone();
        let hits = hits.clone();
        tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_ok() {
                    match hits.fetch_add(1, Ordering::Relaxed) {
                        0 => {
                            eprintln!(
                                "Ctrl+C — attempting graceful shutdown… (press again to force)"
                            );
                            token.cancel();
                        }
                        _ => {
                            eprintln!("Force exiting.");
                            std::process::exit(130);
                        }
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

    let args = Arguments::parse();

    match args.command.as_deref() {
        Some("start") => {
            let addr: SocketAddr = args.socket.parse()?;
            let _server: TokioWebSocketServer<MemoryStorage> = TokioWebSocketServer::setup(
                addr,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
                MemoryStorage::new(),
                CountLeadingZeroBytes,
            )
            .await?;

            tracing::info!("WebSocket server started on {}", addr);
            futures::future::pending::<()>().await; // Keep alive
            tracing::error!("error starting server");
        }
        // Some("connect") => {
        //     let (syncer, actor_fut) =
        //         Subduction::new(MemoryStorage::default(), CountLeadingZeroBytes);

        //     tokio::spawn(async move {
        //         if let Err(Aborted) = actor_fut.await {
        //             tracing::debug!("Subduction actor aborted");
        //         }
        //     });

        //     let ws = TokioWebSocketClient::new(
        //         Uri::try_from(&args.ws)?,
        //         Duration::from_secs(5),
        //         PeerId::new([0; 32]),
        //     )
        //     .await?
        //     .start();

        //     syncer.register(ws).await?;
        //     let listen = syncer.listen();
        //     syncer.request_all_batch_sync_all(None).await?;
        //     listen.await?;
        // }
        _ => {
            eprintln!("Please specify either 'start' or 'connect' command");
            std::process::exit(1);
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[command(author = "Ink & Switch", version, about = "CLI for Subduction")]
struct Arguments {
    command: Option<String>,

    #[arg(short, long, default_value = "0.0.0.0:8080")]
    socket: String,
}
