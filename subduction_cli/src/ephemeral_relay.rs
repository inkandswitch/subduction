//! Simple WebSocket relay server for ephemeral messages (presence, awareness).
//!
//! This server doesn't process messages - it just broadcasts them between peers.
//! It speaks the automerge-repo NetworkSubsystem protocol (CBOR-encoded messages).

use anyhow::Result;
use async_tungstenite::{tokio::accept_async, tungstenite::Message as WsMessage};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;

type PeerId = String;
type PeerSender = tokio::sync::mpsc::Sender<Vec<u8>>;
type PeerConnections = Arc<RwLock<HashMap<PeerId, PeerSender>>>;

/// Arguments for the ephemeral relay server.
#[derive(Debug, clap::Parser)]
pub(crate) struct EphemeralRelayArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8081")]
    pub(crate) socket: String,
}

/// Run the ephemeral message relay server.
pub(crate) async fn run(args: EphemeralRelayArgs, token: CancellationToken) -> Result<()> {
    let addr: SocketAddr = args.socket.parse()?;
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("Ephemeral relay server listening on {}", addr);
    tracing::info!("This server relays presence/awareness messages between peers");

    let peers: PeerConnections = Arc::new(RwLock::new(HashMap::new()));

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("Ephemeral relay shutting down...");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        tracing::info!("New ephemeral connection from {}", addr);
                        let peers = peers.clone();
                        tokio::spawn(handle_connection(stream, addr, peers));
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    peers: PeerConnections,
) -> Result<()> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Create a channel for sending messages to this peer
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);

    // Generate a simple peer ID based on the connection
    let peer_id = format!("ephemeral-peer-{}", addr);

    // Register this peer
    peers.write().await.insert(peer_id.clone(), tx);
    tracing::info!("Registered ephemeral peer: {}", peer_id);

    // Spawn sender task to forward messages to this peer
    let sender_task = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if let Err(e) = ws_sender.send(WsMessage::Binary(data)).await {
                tracing::error!("Failed to send to peer {}: {}", peer_id, e);
                break;
            }
        }
    });

    // Receiver task - relay incoming messages to all other peers
    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(WsMessage::Binary(data)) => {
                tracing::debug!("Relaying {} bytes from {}", data.len(), peer_id);

                // Broadcast to all other peers
                let peer_map = peers.read().await;
                for (other_id, sender) in peer_map.iter() {
                    if other_id != &peer_id {
                        // Fire and forget - if channel is full or closed, skip
                        let _ = sender.try_send(data.clone());
                    }
                }
            }
            Ok(WsMessage::Text(text)) => {
                tracing::warn!("Received unexpected text message: {}", text);
            }
            Ok(WsMessage::Ping(data)) => {
                // Respond to ping
                if let Err(e) = sender_task.abort_handle().is_finished() {
                    tracing::error!("Ping/pong error: {:?}", e);
                }
            }
            Ok(WsMessage::Close(_)) => {
                tracing::info!("Peer {} requested close", peer_id);
                break;
            }
            Err(e) => {
                tracing::error!("WebSocket error from {}: {}", peer_id, e);
                break;
            }
            _ => {}
        }
    }

    // Cleanup
    peers.write().await.remove(&peer_id);
    sender_task.abort();
    tracing::info!("Peer disconnected: {}", peer_id);

    Ok(())
}
