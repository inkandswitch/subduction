// FIXME mod black_box;

// FIXME use self::black_box::BlackBox;
use clap::Parser;
use sedimentree_core::{storage::MemoryStorage, Sedimentree, SedimentreeId};
use sedimentree_sync_core::{peer::id::PeerId, SedimentreeSync};
use sedimentree_sync_websocket::WebSocket;
use std::{collections::HashMap, time::Duration};
use tokio::net::TcpListener;
use tokio_tungstenite::MaybeTlsStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Arguments::parse();

    let sed = Sedimentree::new(vec![], vec![]);
    let sed_id = SedimentreeId::new([0u8; 32]);
    let syncer = SedimentreeSync::new(
        HashMap::from_iter([(sed_id, sed)]),
        MemoryStorage::default(),
        HashMap::new(),
    );

    match args.command.as_deref() {
        Some("start") => {
            tracing::info!("Starting WebSocket server on {}", args.ws);

            let listener = TcpListener::bind(args.ws.clone()).await?;
            let (tcp, _peer) = listener.accept().await?;
            let stream = MaybeTlsStream::Plain(tcp);
            let ws_stream = tokio_tungstenite::accept_async(stream).await?;
            tracing::info!("WebSocket server listening on {}", args.ws);

            let ws = WebSocket::new(
                ws_stream,
                Duration::from_secs(5),
                PeerId::new([0u8; 32]),
                0.into(),
            );

            syncer.register(ws).await?;
            syncer.run().await?;
        }
        Some("connect") => {
            tracing::info!("Connecting to WebSocket server at {}", args.ws);

            let (ws_stream, _) = tokio_tungstenite::connect_async(&args.ws).await?;

            tracing::info!("WebSocket server listening on {}", args.ws);

            let ws = WebSocket::new(
                ws_stream,
                Duration::from_secs(5),
                PeerId::new([1u8; 32]),
                0.into(),
            );

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
#[command(author = "Ink & Switch", version, about = "CLI for Sedimentree Sync")]
struct Arguments {
    command: Option<String>,

    #[arg(short, long, default_value = "localhost:8080")]
    ws: String,
}
