use tokio::net::TcpListener;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    let listener = TcpListener::bind(&args.ws)
        .await
        .expect("Failed to bind");

    println!("WebSocket server listening on ws://{}", args.ws);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let ws_stream = accept_async(stream).await.expect("WebSocket handshake failed");

            println!("New WebSocket connection");

            let (mut write, mut read) = ws_stream.split();

            while let Some(Ok(msg)) = read.next().await {
                if msg.is_text() || msg.is_binary() {
                    println!("Received: {:?}", msg);
                    let blank_am = automerge::Automerge::new().save();
                    write.send(blank_am.into()).await.expect("Failed to send");
                }
            }

            println!("Connection closed");
        });
    }
}

#[derive(Debug, Parser)]
#[command(author = "Ink & Switch", version, about = "CLI for Sedimentree Sync")]
struct Arguments {
    command: Option<String>,

    #[arg(short, long, default_value = "localhost:8080")]
    ws: String,
}
