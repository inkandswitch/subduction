// FIXME mod black_box;

// FIXME use self::black_box::BlackBox;
use clap::Parser;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Digest, LooseCommit, Sedimentree, SedimentreeId,
};
use sedimentree_sync_core::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionId, Message, Reconnection,
        RequestId,
    },
    peer::id::PeerId,
    SedimentreeSync,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot, Mutex, RwLock,
    },
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

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
            )
            .await;

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
            )
            .await;

            syncer.register(ws).await?;
            let listen = syncer.run();
            syncer.request_all_batch_sync_all(None).await?;
            listen.await?;
            syncer.run().await?;
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

#[derive(Debug, Clone)]
pub struct WebSocket {
    conn_id: ConnectionId,
    peer_id: PeerId,

    req_id_counter: Arc<Mutex<u128>>,
    timeout: Duration,

    ws_reader: Arc<Mutex<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    outbound:
        Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>>>,

    pending: Arc<RwLock<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    inbound_writer: UnboundedSender<Message>, // NOTE mspc
    inbound_reader: Arc<Mutex<UnboundedReceiver<Message>>>,
}

impl WebSocket {
    pub async fn new(
        ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Self {
        let (ws_writer, ws_reader_owned) = ws.split();
        let pending = Arc::new(RwLock::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
        let ws_reader = Arc::new(Mutex::new(ws_reader_owned));
        let starting_counter = rand::random::<u128>();

        Self {
            conn_id,
            peer_id,

            req_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            ws_reader,
            outbound: Arc::new(Mutex::new(ws_writer)),
            pending,
            inbound_writer,
            inbound_reader: Arc::new(Mutex::new(inbound_rx)),
        }
    }
}

impl Connection for WebSocket {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn connection_id(&self) -> ConnectionId {
        self.conn_id
    }

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    async fn next_request_id(&self) -> RequestId {
        let mut counter = self.req_id_counter.lock().await;
        *counter = counter.wrapping_add(1);
        tracing::info!("generated message id {:?}", *counter);
        RequestId {
            requestor: self.peer_id,
            nonce: *counter,
        }
    }

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: Message) -> Result<(), SendError> {
        self.outbound
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn recv(&self) -> Result<Message, Self::RecvError> {
        tracing::debug!("waiting for inbound message");
        let mut chan = self.inbound_reader.lock().await;
        let msg = chan.recv().await.ok_or(RecvError::ReadFromClosed)?;
        tracing::info!("received inbound message id {:?}", msg.request_id());
        Ok(msg)
    }

    #[tracing::instrument(skip(self, req), fields(req_id = ?req.req_id))]
    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
        let req_id = req.req_id;

        // Pre-register channel
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(req_id, tx);

        self.outbound
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(
                    Message::BatchSyncRequest(req),
                    bincode::config::standard(),
                )?
                .into(),
            ))
            .await?;

        let req_timeout = override_timeout.unwrap_or(self.timeout);

        // await response with timeout & cleanup
        match timeout(req_timeout, rx).await {
            Ok(Ok(resp)) => {
                tracing::info!("request {:?} completed", req_id);
                Ok(resp)
            }
            Ok(Err(e)) => {
                tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                Err(CallError::ChanError(e))
            }
            Err(elapsed) => {
                tracing::error!("request {:?} timed out", req_id);
                Err(CallError::Timeout(elapsed))
            }
        }
    }
}

impl Reconnection for WebSocket {
    type Address = String;
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    async fn connect(
        addr: Self::Address,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Result<Box<Self>, Self::ConnectError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(addr).await?;
        Ok(Box::new(
            Self::new(ws_stream, timeout, peer_id, conn_id).await,
        ))
    }

    async fn run(&self) -> Result<(), RunError> {
        let pending = self.pending.clone();
        while let Some(msg) = self.ws_reader.lock().await.next().await {
            tracing::debug!("received ws message");
            match msg {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    let (msg, _size): (Message, usize) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;

                    match msg {
                        Message::BatchSyncResponse(resp) => {
                            let req_id = resp.req_id;
                            tracing::info!("dispatching to waiter {:?}", req_id);
                            if let Some(waiting) = pending.write().await.remove(&req_id) {
                                tracing::info!("dispatching to waiter {:?}", req_id);
                                let result = waiting.send(resp);
                                debug_assert!(result.is_ok());
                                if result.is_err() {
                                    tracing::error!(
                                        "oneshot channel closed before sending response for req_id {:?}",
                                        req_id
                                    );
                                }
                            } else {
                                tracing::info!("dispatching to inbound channel {:?}", resp.req_id);
                                self.inbound_writer.send(Message::BatchSyncResponse(resp))?;
                            }
                        }
                        other => {
                            self.inbound_writer.send(other)?;
                        }
                    }
                }
                Ok(tungstenite::Message::Text(text)) => {
                    tracing::warn!("unexpected text message: {}", text);
                }
                Ok(tungstenite::Message::Ping(p)) => {
                    tracing::info!("received ping: {:x?}", p);
                    self.outbound
                        .lock()
                        .await
                        .send(tungstenite::Message::Pong(p))
                        .await
                        .unwrap_or_else(|_| {
                            tracing::error!("failed to send pong");
                        });
                }
                Ok(tungstenite::Message::Pong(p)) => {
                    tracing::warn!("unexpected pong message: {:x?}", p);
                }
                Ok(tungstenite::Message::Frame(f)) => {
                    tracing::warn!("unexpected frame: {:x?}", f);
                }
                Ok(tungstenite::Message::Close(_)) => {
                    // fail all pending
                    std::mem::take(&mut *pending.write().await);
                    break;
                }
                Err(e) => Err(e)?,
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    chunks: Arc<RwLock<HashMap<Digest, Chunk>>>,
    commits: Arc<RwLock<HashMap<Digest, LooseCommit>>>,
    blobs: Arc<RwLock<HashMap<Digest, Blob>>>,
}

impl Storage for MemoryStorage {
    type Error = std::convert::Infallible;

    async fn load_loose_commits(&self) -> Result<Vec<LooseCommit>, Self::Error> {
        let commits = self.commits.read().await.values().cloned().collect();
        Ok(commits)
    }

    async fn save_loose_commit(&self, loose_commit: LooseCommit) -> Result<(), Self::Error> {
        let digest = loose_commit.blob().digest();
        self.commits.write().await.insert(digest, loose_commit);
        Ok(())
    }

    async fn save_chunk(&self, chunk: Chunk) -> Result<(), Self::Error> {
        let digest = chunk.summary().blob_meta().digest();
        self.chunks.write().await.insert(digest, chunk);
        Ok(())
    }

    async fn load_chunks(&self) -> Result<Vec<Chunk>, Self::Error> {
        let chunks = self.chunks.read().await.values().cloned().collect();
        Ok(chunks)
    }

    async fn save_blob(&self, blob: Blob) -> Result<Digest, Self::Error> {
        let digest = Digest::hash(blob.contents());
        self.blobs.write().await.insert(digest, blob);
        Ok(digest)
    }

    async fn load_blob(&self, blob_digest: Digest) -> Result<Option<Blob>, Self::Error> {
        let maybe_blob = self.blobs.read().await.get(&blob_digest).cloned();
        Ok(maybe_blob)
    }
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::error::EncodeError),
}

#[derive(Debug, Error)]
pub enum CallError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::error::EncodeError),

    #[error("Channel receive error: {0}")]
    ChanError(#[from] oneshot::error::RecvError),

    #[error("Timed out waiting for response: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("Channel receive error: {0}")]
    ChanError(#[from] oneshot::error::RecvError),

    #[error("Attempted to read from closed channel")]
    ReadFromClosed,
}

#[derive(Debug, Error)]
#[error("Disconnected")]
pub struct DisconnectionError;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Channel send error: {0}")]
    ChanSend(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("Bincode deserialize error: {0}")]
    Deserialize(#[from] bincode::error::DecodeError),
}
