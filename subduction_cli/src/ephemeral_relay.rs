//! Simple WebSocket relay server for ephemeral messages (presence, awareness).
//!
//! This server implements the automerge-repo `NetworkSubsystem` protocol handshake
//! and then broadcasts messages between connected peers.
//!
//! ## Performance and Security
//!
//! Uses `AHash` (keyed) for DoS-resistant sharding and dedup keying; faster than
//! `SipHash` in practice on short inputs; not used as a cryptographic primitive.
//!
//! TODO: Add message authentication to prevent malicious peers from sending
//! forged ephemeral messages.
//!
//! TODO: Add timestamp validation to prevent replay attacks (reject messages
//! with timestamps too far in the past or future).

use ahash::RandomState;
use anyhow::Result;
use async_tungstenite::{tokio::accept_async, tungstenite::Message as WsMessage};
use ciborium::value::Value as CborValue;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher, Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

/// Default maximum size for ephemeral messages (1 MB)
const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Sanitize a string for logging by truncating and removing control characters
fn sanitize_for_log(s: &str, max_len: usize) -> String {
    let truncated = if s.len() > max_len {
        format!("{}...", &s[..max_len])
    } else {
        s.to_string()
    };

    // Remove control characters (0x00-0x1F except \t, \n, \r)
    truncated
        .chars()
        .filter(|&c| {
            let code = c as u32;
            code >= 0x20 || c == '\t' || c == '\n' || c == '\r'
        })
        .collect()
}

// FIXME
/// Peer identifier (newtype for type safety)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PeerId(String);

impl std::ops::Deref for PeerId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for PeerId {
    fn from(s: String) -> Self {
        PeerId(s)
    }
}

/// Channel sender for peer messages (uses Arc for zero-copy broadcast)
#[derive(Debug, Clone)]
struct PeerSender(tokio::sync::mpsc::Sender<Arc<[u8]>>);

impl std::ops::Deref for PeerSender {
    type Target = tokio::sync::mpsc::Sender<Arc<[u8]>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Thread-safe peer connection map
#[derive(Debug, Clone)]
struct PeerConnections(Arc<RwLock<HashMap<PeerId, PeerSender>>>);

impl PeerConnections {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl std::ops::Deref for PeerConnections {
    type Target = Arc<RwLock<HashMap<PeerId, PeerSender>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Message deduplication key (newtype for type safety)
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct MessageKey(u64);

impl From<u64> for MessageKey {
    fn from(k: u64) -> Self {
        MessageKey(k)
    }
}

/// Ring buffer for tracking recently seen messages to prevent duplicates
struct MessageDeduplicator {
    seen: HashSet<MessageKey>,
    order: VecDeque<MessageKey>,
    capacity: usize,
}

impl MessageDeduplicator {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Returns true if this is a new message (not seen before)
    fn add(&mut self, message_key: MessageKey) -> bool {
        if self.seen.contains(&message_key) {
            return false;
        }

        // Add to seen set and order queue
        self.seen.insert(message_key);
        self.order.push_back(message_key);

        // Evict oldest if over capacity
        if self.order.len() > self.capacity
            && let Some(old_key) = self.order.pop_front() {
                self.seen.remove(&old_key);
            }

        true
    }
}

/// Sharded deduplicator using keyed hashing to distribute load across shards
struct ShardedDeduplicator {
    shards: Vec<Mutex<MessageDeduplicator>>,
    hasher_state: RandomState, // Keyed hasher state for AHash
    shard_mask: usize,         // S - 1 where S is power of 2
}

impl ShardedDeduplicator {
    fn new(num_shards: usize, capacity_per_shard: usize) -> Self {
        assert!(
            num_shards.is_power_of_two(),
            "num_shards must be power of 2"
        );

        // Generate random 128-bit secret key (as 4 x u64 for ahash RandomState)
        let hasher_state = RandomState::with_seeds(
            rand::random::<u64>(),
            rand::random::<u64>(),
            rand::random::<u64>(),
            rand::random::<u64>(),
        );

        let shards = (0..num_shards)
            .map(|_| Mutex::new(MessageDeduplicator::new(capacity_per_shard)))
            .collect();

        Self {
            shards,
            hasher_state,
            shard_mask: num_shards - 1,
        }
    }

    /// Compute keyed hash for a message without allocating
    ///
    /// Uses delimiters between fields to prevent hash collisions from
    /// concatenation (e.g., "ab"+"c" vs "a"+"bc").
    fn msg_key(&self, sender_id: &str, session_id: &str, count: u64) -> MessageKey {
        let mut hasher = self.hasher_state.build_hasher();
        hasher.write_u8(0xFF);
        hasher.write(sender_id.as_bytes());
        hasher.write_u8(0x00);
        hasher.write(session_id.as_bytes());
        hasher.write_u8(0x00);
        hasher.write_u64(count);
        MessageKey(hasher.finish())
    }

    /// Compute shard index from message key
    const fn get_shard_index(&self, message_key: MessageKey) -> usize {
        #[allow(clippy::cast_possible_truncation)]
        {
            (message_key.0 as usize) & self.shard_mask
        }
    }

    /// Returns true if this is a new message (not seen before)
    async fn add(&self, message_key: MessageKey) -> bool {
        let shard_index = self.get_shard_index(message_key);
        #[allow(clippy::expect_used)]
        let mut shard = self.shards.get(shard_index).expect("shard index should exist").lock().await;
        shard.add(message_key)
    }
}

type Deduplicator = Arc<ShardedDeduplicator>;

/// Join message from client (automerge-repo protocol)
#[derive(Debug, Deserialize)]
struct JoinMessage {
    #[serde(rename = "type")]
    msg_type: String,

    #[serde(rename = "senderId")]
    sender_id: String,

    #[serde(rename = "peerMetadata")]
    peer_metadata: CborValue,
}

/// Peer message to send back to client (automerge-repo protocol)
#[derive(Debug, Serialize)]
struct PeerMessage {
    #[serde(rename = "type")]
    msg_type: String,

    #[serde(rename = "senderId")]
    sender_id: String,

    #[serde(rename = "peerMetadata")]
    peer_metadata: CborValue,
}

/// Ephemeral message for extracting deduplication info
#[derive(Debug, Deserialize)]
struct EphemeralMessageHeader {
    #[serde(rename = "senderId")]
    sender_id: String,

    #[serde(rename = "sessionId")]
    session_id: String,

    count: u64,
}

/// Arguments for the ephemeral relay server.
#[derive(Debug, clap::Parser)]
pub(crate) struct EphemeralRelayArgs {
    /// Socket address to bind to
    #[arg(short, long, default_value = "0.0.0.0:8081")]
    pub(crate) socket: String,

    /// Maximum message size in bytes (default: 1 MB)
    #[arg(short, long, default_value_t = DEFAULT_MAX_MESSAGE_SIZE)]
    pub(crate) max_message_size: usize,
}

/// Run the ephemeral message relay server.
pub(crate) async fn run(args: EphemeralRelayArgs, token: CancellationToken) -> Result<()> {
    let addr: SocketAddr = args.socket.parse()?;
    let listener = TcpListener::bind(addr).await?;
    let max_message_size = args.max_message_size;

    tracing::info!("Ephemeral relay server listening on {}", addr);
    tracing::info!("This server relays presence/awareness messages between peers");
    tracing::info!("Maximum message size: {} bytes ({} MB)", max_message_size, max_message_size / (1024 * 1024));

    let peers = PeerConnections::new();

    // 16 shards, 256 messages per shard = 4096 total message history
    // 16 is a good balance for typical 4-16 core systems
    let deduplicator: Deduplicator = Arc::new(ShardedDeduplicator::new(16, 256));

    loop {
        tokio::select! {
            () = token.cancelled() => {
                tracing::info!("Ephemeral relay shutting down...");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        tracing::info!("New ephemeral connection from {}", addr);
                        let peers = peers.clone();
                        let deduplicator = deduplicator.clone();
                        tokio::spawn(handle_connection(stream, addr, peers, deduplicator, max_message_size));
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

#[allow(clippy::too_many_lines)]
async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    peers: PeerConnections,
    deduplicator: Deduplicator,
    max_message_size: usize,
) -> Result<()> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Create a channel for sending messages to this peer (uses Arc for zero-copy broadcast)
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<[u8]>>(100);

    // Wait for the first message which should be a join message
    let peer_id = if let Some(Ok(WsMessage::Binary(data))) = ws_receiver.next().await {
        // Try to parse as a join message
        match ciborium::from_reader::<JoinMessage, _>(data.as_ref()) {
            Ok(join_msg) if join_msg.msg_type == "join" => {
                let peer_id = PeerId::from(join_msg.sender_id.clone());
                tracing::info!("Peer {} joined from {}", peer_id.0, addr);

                // Send peer message back
                let peer_msg = PeerMessage {
                    msg_type: "peer".to_string(),
                    sender_id: "ephemeral-relay".to_string(),
                    peer_metadata: join_msg.peer_metadata,
                };

                let mut response = Vec::new();
                ciborium::into_writer(&peer_msg, &mut response)?;
                ws_sender.send(WsMessage::Binary(response.into())).await?;

                peer_id
            }
            _ => {
                tracing::warn!("First message from {} was not a join message", addr);
                return Ok(());
            }
        }
    } else {
        tracing::warn!("Connection from {} closed before join", addr);
        return Ok(());
    };

    // Register this peer
    peers.write().await.insert(peer_id.clone(), PeerSender(tx));
    tracing::info!("Registered ephemeral peer: {}", peer_id.0);

    // Spawn sender task to forward messages to this peer
    let peer_id_for_sender = peer_id.clone();
    let sender_task = tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if let Err(e) = ws_sender
                .send(WsMessage::Binary(data.to_vec().into()))
                .await
            {
                tracing::error!("Failed to send to peer {}: {}", peer_id_for_sender.0, e);
                break;
            }
        }
    });

    // Receiver task - relay incoming messages to all other peers
    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(WsMessage::Binary(data)) => {
                // Reject messages that are too large
                if data.len() > max_message_size {
                    tracing::warn!(
                        "Dropping oversized message from {}: {} bytes (max: {} bytes)",
                        peer_id.0,
                        data.len(),
                        max_message_size
                    );
                    continue;
                }

                // Try to parse message header for deduplication
                let should_relay = if let Ok(header) =
                    ciborium::from_reader::<EphemeralMessageHeader, _>(data.as_ref())
                {
                    let message_key =
                        deduplicator.msg_key(&header.sender_id, &header.session_id, header.count);
                    let is_new = deduplicator.add(message_key).await;

                    if is_new {
                        tracing::debug!(
                            "Relaying new message {}:{}:{} ({} bytes)",
                            sanitize_for_log(&header.sender_id, 64),
                            sanitize_for_log(&header.session_id, 64),
                            header.count,
                            data.len()
                        );
                    } else {
                        tracing::debug!(
                            "Dropping duplicate message {}:{}:{}",
                            sanitize_for_log(&header.sender_id, 64),
                            sanitize_for_log(&header.session_id, 64),
                            header.count
                        );
                    }

                    is_new
                } else {
                    // If we can't parse it, relay it anyway (might not be ephemeral message)
                    tracing::debug!("Relaying non-ephemeral message ({} bytes)", data.len());
                    true
                };

                if should_relay {
                    // Convert to Arc for zero-copy broadcast
                    let payload: Arc<[u8]> = data.to_vec().into();

                    // Clone senders first, drop lock, then broadcast
                    let targets: Vec<PeerSender> = {
                        let peer_map = peers.read().await;
                        peer_map
                            .iter()
                            .filter(|(id, _)| *id != &peer_id)
                            .map(|(_, sender)| sender.clone())
                            .collect()
                    };

                    for sender in targets {
                        // Fire and forget - if channel is full or closed, skip
                        drop(sender.try_send(payload.clone()));
                    }
                }
            }
            Ok(WsMessage::Text(text)) => {
                tracing::warn!(
                    "Received unexpected text message: {}",
                    sanitize_for_log(&text, 256)
                );
            }
            Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_)) => {
                // Ping/pong are handled automatically by the WebSocket library
            }
            Ok(WsMessage::Close(_)) => {
                tracing::info!("Peer {} requested close", peer_id.0);
                break;
            }
            Err(e) => {
                tracing::error!("WebSocket error from {}: {}", peer_id.0, e);
                break;
            }
        }
    }

    // Cleanup
    peers.write().await.remove(&peer_id);
    sender_task.abort();
    tracing::info!("Peer disconnected: {}", peer_id.0);

    Ok(())
}
