use clap::Parser;
use sedimentree_core::{storage::MemoryStorage, Sedimentree, SedimentreeId};
use std::{collections::HashMap, time::Duration};
use subduction_core::{peer::id::PeerId, Subduction};
use subduction_websocket::tokio::{client::TokioWebSocketClient, server::TokioWebSocketServer};
use tungstenite::http::Uri;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Arguments::parse();

    let sed = Sedimentree::new(vec![], vec![]);
    let sed_id = SedimentreeId::new([0u8; 32]);

    match args.command.as_deref() {
        Some("start") => {
            let syncer = Subduction::new(
                HashMap::from_iter([(sed_id, sed)]),
                MemoryStorage::default(),
                HashMap::new(),
            );

            let ws: TokioWebSocketServer = {
                let addr = args.ws.parse()?;
                TokioWebSocketServer::setup(addr, Duration::from_secs(5), PeerId::new([0; 32]))
                    .await?
                    .start()
            };

            syncer.register(ws).await?;
            syncer.run().await?;
        }
        Some("connect") => {
            let syncer = Subduction::new(
                HashMap::from_iter([(sed_id, sed)]),
                MemoryStorage::default(),
                HashMap::new(),
            );

            let ws = TokioWebSocketClient::new(
                Uri::try_from(&args.ws)?,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
            )
            .await?
            .start();

            syncer.register(ws).await?;
            let listen = syncer.run();
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

    #[arg(short, long, default_value = "localhost:8080")]
    ws: String,
}
