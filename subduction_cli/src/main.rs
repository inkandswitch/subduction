use clap::Parser;
use dashmap::DashMap;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, storage::MemoryStorage, Sedimentree, SedimentreeId,
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use subduction_core::{peer::id::PeerId, Subduction};
use subduction_websocket::tokio::{
    client::TokioWebSocketClient, server::TokioWebSocketServer, start::Unstarted,
};
use tokio_util::sync::CancellationToken;
use tungstenite::http::Uri;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
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

    let sed = Sedimentree::new(vec![], vec![]);
    let sed_id = SedimentreeId::new([0u8; 32]);

    match args.command.as_deref() {
        Some("start") => {
            let addr: SocketAddr = args.ws.parse()?;
            let server: Unstarted<TokioWebSocketServer<MemoryStorage>> =
                TokioWebSocketServer::setup(
                    addr,
                    Duration::from_secs(5),
                    PeerId::new([0; 32]),
                    MemoryStorage::default(),
                    CountLeadingZeroBytes,
                )
                .await?;

            let inner = server.ignore();
            inner.start().await?; // FIXME use unstarted run
            futures::future::pending::<()>().await; // Keep alive
        }
        Some("connect") => {
            let (syncer, mut actor) = Subduction::new(
                Arc::new(DashMap::from_iter([(sed_id, sed)])),
                MemoryStorage::default(),
                Arc::new(DashMap::new()),
                CountLeadingZeroBytes,
            );

            tokio::spawn(async move { actor.listen().await });

            let ws = TokioWebSocketClient::new(
                Uri::try_from(&args.ws)?,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
            )
            .await?
            .start();

            syncer.register(ws).await?;
            let listen = syncer.listen();
            syncer.request_all_batch_sync_all(None).await?;
            listen.await?;
        }
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
    ws: String,
}
