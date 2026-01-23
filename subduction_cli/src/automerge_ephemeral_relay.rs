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
use async_tungstenite::{
    tokio::accept_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message as WsMessage},
};
use futures_util::StreamExt;
use std::{
    collections::{HashSet, VecDeque},
    hash::{BuildHasher, Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};
use subduction_core::sharded_map::ShardedMap;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

/// Default maximum size for ephemeral messages (1 MB)
const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Maximum length for an automerge peer ID string (256 bytes)
const MAX_PEER_ID_LEN: usize = 256;

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

/// Automerge-repo peer identifier (newtype for type safety).
///
/// This is a string-based peer ID as used by the automerge-repo protocol,
/// distinct from the cryptographic [`subduction_core::peer::id::PeerId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AutomergePeerId(String);

/// Error returned when an automerge peer ID exceeds the maximum length.
#[derive(Debug, Clone, Copy)]
struct AutomergePeerIdTooLong;

impl AutomergePeerId {
    /// Create a new `AutomergePeerId` from a string.
    ///
    /// Returns an error if the string exceeds [`MAX_PEER_ID_LEN`] bytes.
    fn new(s: String) -> Result<Self, AutomergePeerIdTooLong> {
        if s.len() > MAX_PEER_ID_LEN {
            return Err(AutomergePeerIdTooLong);
        }
        Ok(Self(s))
    }
}

impl std::ops::Deref for AutomergePeerId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
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

/// Sharded peer connection map to reduce lock contention.
///
/// Wraps [`ShardedMap`] with peer-specific convenience methods.
#[derive(Clone)]
struct PeerConnections(Arc<ShardedMap<AutomergePeerId, PeerSender, 16>>);

impl PeerConnections {
    fn new() -> Self {
        Self(Arc::new(ShardedMap::new()))
    }

    async fn insert(&self, peer_id: AutomergePeerId, sender: PeerSender) {
        self.0.insert(peer_id, sender).await;
    }

    async fn remove(&self, peer_id: &AutomergePeerId) {
        self.0.remove(peer_id).await;
    }

    /// Collect all senders except the given peer, releasing locks quickly.
    async fn collect_other_senders(&self, exclude: &AutomergePeerId) -> Vec<PeerSender> {
        let mut senders = Vec::new();
        for idx in self.0.shard_indices() {
            #[allow(clippy::expect_used)]
            let shard = self.0.shard_at(idx).expect("shard index valid");
            let guard = shard.lock().await;
            for (id, sender) in guard.iter() {
                if id != exclude {
                    senders.push(sender.clone());
                }
            }
        }
        senders
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

        self.seen.insert(message_key);
        self.order.push_back(message_key);

        if self.order.len() > self.capacity
            && let Some(old_key) = self.order.pop_front() {
                self.seen.remove(&old_key);
            }

        true
    }
}

/// Compute shard index from message key.
///
/// Uses [Lemire's "fast range" method][post] to map hash to [0, N) without modulo bias.
///
/// [post]: https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction
fn get_shard_index<const N: usize>(message_key: MessageKey) -> usize {
    #[allow(clippy::expect_used)]
    usize::try_from((u128::from(message_key.0) * (N as u128)) >> 64).expect("N fits in usize")
}

/// Sharded deduplicator using keyed hashing to distribute load across shards.
struct ShardedDeduplicator<const N: usize> {
    shards: [Mutex<MessageDeduplicator>; N],
    hasher_state: RandomState, // Keyed hasher state for AHash
}

impl<const N: usize> ShardedDeduplicator<N> {
    fn new(capacity_per_shard: usize) -> Self {
        const { assert!(N > 0, "N must be greater than 0") };

        // Generate random 128-bit secret key (as 4 x u64 for ahash RandomState)
        let hasher_state = RandomState::with_seeds(
            rand::random::<u64>(),
            rand::random::<u64>(),
            rand::random::<u64>(),
            rand::random::<u64>(),
        );

        let shards = core::array::from_fn(|_| Mutex::new(MessageDeduplicator::new(capacity_per_shard)));

        Self {
            shards,
            hasher_state,
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

    /// Returns true if this is a new message (not seen before)
    async fn add(&self, message_key: MessageKey) -> bool {
        let shard_index = get_shard_index::<N>(message_key);
        #[allow(clippy::indexing_slicing)] // shard_index is always < N
        let mut shard = self.shards[shard_index].lock().await;
        shard.add(message_key)
    }
}


/// Join message from client (automerge-repo protocol)
#[derive(Debug)]
struct JoinMessage {
    msg_type: String,
    sender_id: String,
    /// Raw CBOR bytes for peer_metadata (passed through without interpretation)
    peer_metadata_raw: Vec<u8>,
}

impl<'b, C> minicbor::Decode<'b, C> for JoinMessage {
    fn decode(d: &mut minicbor::Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let len = d.map()?.ok_or_else(|| minicbor::decode::Error::message("expected definite-length map"))?;

        let mut msg_type = None;
        let mut sender_id = None;
        let mut peer_metadata_raw = None;

        for _ in 0..len {
            let key = d.str()?;
            match key {
                "type" => msg_type = Some(d.str()?.to_string()),
                "senderId" => sender_id = Some(d.str()?.to_string()),
                "peerMetadata" => {
                    // Capture raw CBOR bytes for passthrough
                    let start = d.position();
                    d.skip()?;
                    let end = d.position();
                    peer_metadata_raw = Some(d.input()[start..end].to_vec());
                }
                _ => d.skip()?,
            }
        }

        Ok(JoinMessage {
            msg_type: msg_type.ok_or_else(|| minicbor::decode::Error::message("missing 'type' field"))?,
            sender_id: sender_id.ok_or_else(|| minicbor::decode::Error::message("missing 'senderId' field"))?,
            peer_metadata_raw: peer_metadata_raw.unwrap_or_default(),
        })
    }
}

/// Peer message to send back to client (automerge-repo protocol)
#[derive(Debug)]
struct PeerMessage {
    msg_type: String,
    sender_id: String,
    /// Raw CBOR bytes for peer_metadata (embedded without interpretation)
    peer_metadata_raw: Vec<u8>,
}

impl<C> minicbor::Encode<C> for PeerMessage {
    fn encode<W: minicbor::encode::Write>(&self, e: &mut minicbor::Encoder<W>, _ctx: &mut C) -> Result<(), minicbor::encode::Error<W::Error>> {
        e.map(3)?;
        e.str("type")?.str(&self.msg_type)?;
        e.str("senderId")?.str(&self.sender_id)?;
        e.str("peerMetadata")?;
        // Embed raw CBOR bytes directly
        e.writer_mut().write_all(&self.peer_metadata_raw).map_err(minicbor::encode::Error::write)?;
        Ok(())
    }
}

/// Ephemeral message header for extracting deduplication info
#[derive(Debug)]
struct EphemeralMessageHeader {
    sender_id: String,
    session_id: String,
    count: u64,
}

impl<'b, C> minicbor::Decode<'b, C> for EphemeralMessageHeader {
    fn decode(d: &mut minicbor::Decoder<'b>, _ctx: &mut C) -> Result<Self, minicbor::decode::Error> {
        let len = d.map()?.ok_or_else(|| minicbor::decode::Error::message("expected definite-length map"))?;

        let mut sender_id = None;
        let mut session_id = None;
        let mut count = None;

        for _ in 0..len {
            let key = d.str()?;
            match key {
                "senderId" => sender_id = Some(d.str()?.to_string()),
                "sessionId" => session_id = Some(d.str()?.to_string()),
                "count" => count = Some(d.u64()?),
                _ => d.skip()?,
            }
        }

        Ok(EphemeralMessageHeader {
            sender_id: sender_id.ok_or_else(|| minicbor::decode::Error::message("missing 'senderId' field"))?,
            session_id: session_id.ok_or_else(|| minicbor::decode::Error::message("missing 'sessionId' field"))?,
            count: count.ok_or_else(|| minicbor::decode::Error::message("missing 'count' field"))?,
        })
    }
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
    // 16 shards, 256 messages per shard = 4096 total message history
    let deduplicator: Arc<ShardedDeduplicator<16>> = Arc::new(ShardedDeduplicator::new(256));

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
    deduplicator: Arc<ShardedDeduplicator<16>>,
    max_message_size: usize,
) -> Result<()> {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(max_message_size);
    let ws_stream = accept_async_with_config(stream, Some(config)).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Create a channel for sending messages to this peer (uses Arc for zero-copy broadcast)
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Arc<[u8]>>(100);

    // Wait for the first message which should be a join message
    let peer_id = if let Some(Ok(WsMessage::Binary(data))) = ws_receiver.next().await {
        // Try to parse as a join message
        match minicbor::decode::<JoinMessage>(&data) {
            Ok(join_msg) if join_msg.msg_type == "join" => {
                let Ok(peer_id) = AutomergePeerId::new(join_msg.sender_id.clone()) else {
                    tracing::warn!(
                        "Peer ID from {} exceeds max length ({} bytes)",
                        addr,
                        MAX_PEER_ID_LEN
                    );
                    return Ok(());
                };
                tracing::info!("Peer {} joined from {}", peer_id.0, addr);

                // Send peer message back (echo peer_metadata as raw bytes)
                let peer_msg = PeerMessage {
                    msg_type: "peer".to_string(),
                    sender_id: "ephemeral-relay".to_string(),
                    peer_metadata_raw: join_msg.peer_metadata_raw,
                };

                let response = minicbor::to_vec(&peer_msg).map_err(|e| anyhow::anyhow!("failed to encode peer message: {e}"))?;
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
    peers.insert(peer_id.clone(), PeerSender(tx)).await;
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
                // Try to parse message header for deduplication
                let should_relay = if let Ok(header) =
                    minicbor::decode::<EphemeralMessageHeader>(&data)
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

                    // Collect senders from shards, then broadcast
                    let targets = peers.collect_other_senders(&peer_id).await;

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
    peers.remove(&peer_id).await;
    sender_task.abort();
    tracing::info!("Peer disconnected: {}", peer_id.0);

    Ok(())
}
