mod black_box;

use self::black_box::BlackBox;
use clap::Parser;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Digest, LooseCommit, Sedimentree, SedimentreeId,
};
use sedimentree_sync_core::{
    connection::{BatchSyncRequest, BatchSyncResponse, Connection, Message, RequestId},
    peer::{id::PeerId, metadata::PeerMetadata},
    SedimentreeSync,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc::UnboundedReceiver, oneshot, Mutex, RwLock},
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[tokio::main]
async fn main() {
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

            let listener = TcpListener::bind(args.ws.clone()).await.expect("FIXME");
            let (tcp, _peer) = listener.accept().await.expect("FIXME");
            let stream = MaybeTlsStream::Plain(tcp);
            let ws_stream = tokio_tungstenite::accept_async(stream)
                .await
                .expect("FIXME");
            tracing::info!("WebSocket server listening on {}", args.ws);

            let ws =
                WebSocket::new(ws_stream, Duration::from_secs(5), PeerId::new([0u8; 32]), 0).await;

            syncer.register(ws).await.expect("FIXME");
            loop {
                syncer.listen().await.expect("FIXME");
            }

            // for i in 0..99 {
            //     let blob = Blob::new([0, i].to_vec());
            //     let commit = LooseCommit::new(blob.meta().digest(), vec![], blob.meta().clone());

            //     syncer
            //         .add_commit(sed_id, &commit, blob)
            //         .await
            //         .expect("FIXME");
            // }
        }
        Some("connect") => {
            tracing::info!("Connecting to WebSocket server at {}", args.ws);

            let (ws_stream, _) = tokio_tungstenite::connect_async(&args.ws)
                .await
                .expect("FIXME");

            tracing::info!("WebSocket server listening on {}", args.ws);

            let ws =
                WebSocket::new(ws_stream, Duration::from_secs(5), PeerId::new([1u8; 32]), 0).await;

            syncer.register(ws).await.expect("FIXME");
            let listen = syncer.listen();
            syncer
                .request_all_batch_sync_all(None)
                .await
                .expect("FIXME");
            listen.await.expect("FIXME");
            loop {
                syncer.listen().await.expect("FIXME");
            }
        }
        _ => {
            eprintln!("Please specify either 'start' or 'connect' command");
            std::process::exit(1);
        }
    }
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
    conn_id: usize,
    peer_id: PeerId,

    req_id_counter: Arc<Mutex<u32>>,
    timeout: Duration,

    outbound:
        Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>>>,
    pending: Arc<RwLock<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound: Arc<Mutex<UnboundedReceiver<Message>>>,
}

impl WebSocket {
    pub async fn new(
        ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: usize,
    ) -> Self {
        let (ws_writer, mut ws_reader) = ws.split();
        let pending = Arc::new(RwLock::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let loop_pending = pending.clone();

        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
        let outbound = Arc::new(Mutex::new(ws_writer));
        let loop_outbound = outbound.clone();

        tokio::spawn(async move {
            while let Some(msg) = ws_reader.next().await {
                tracing::debug!("received ws message");
                match msg {
                    Ok(tungstenite::Message::Binary(bytes)) => {
                        let result: Result<(Message, usize), _> =
                            bincode::serde::decode_from_slice(&bytes, bincode::config::standard());

                        match result {
                            Ok((msg, _size)) => {
                                match msg {
                                    Message::BatchSyncResponse(resp) => {
                                        tracing::info!("dispatching to waiter {:?}", resp.req_id);
                                        if let Some(waiting) =
                                            loop_pending.write().await.remove(&resp.req_id)
                                        {
                                            tracing::info!(
                                                "dispatching to waiter {:?}",
                                                resp.req_id
                                            );
                                            waiting.send(resp).expect("FIXME");
                                        } else {
                                            tracing::info!(
                                                "dispatching to inbound channel {:?}",
                                                resp.req_id
                                            );
                                            inbound_tx
                                                .send(Message::BatchSyncResponse(resp))
                                                .expect("FIXME");
                                        }
                                    }
                                    other => {
                                        inbound_tx.send(other).expect("FIXME");
                                    } // FIXME
                                }
                            }
                            Err(e) => {
                                tracing::error!("failed to decode message: {}", e);
                            }
                        }
                    }
                    Ok(tungstenite::Message::Text(text)) => {
                        tracing::warn!("unexpected text message: {}", text);
                    }
                    Ok(tungstenite::Message::Ping(p)) => {
                        tracing::info!("received ping: {:x?}", p);
                        loop_outbound
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
                        std::mem::take(&mut *loop_pending.write().await);
                        break;
                    }
                    Err(e) => {
                        // FIXME err chan?
                        tracing::error!("WebSocket error: {}", e);
                    }
                }
            }
        });

        let starting_counter = rand::random::<u32>();

        Self {
            conn_id,
            peer_id,

            req_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            outbound,
            pending,
            inbound: Arc::new(Mutex::new(inbound_rx)),
        }
    }
}

impl Connection for WebSocket {
    type Error = FixmeErr;
    type DisconnectionError = FixmeErr;

    fn connection_id(&self) -> usize {
        self.conn_id
    }

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn peer_metadata(&self) -> Option<PeerMetadata> {
        None
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
    async fn send(&self, message: Message) -> Result<(), Self::Error> {
        self.outbound
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(&message, bincode::config::standard())
                    .expect("FIXME")
                    .into(),
            ))
            .await
            .expect("FIXME");

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn recv(&self) -> Result<Message, Self::Error> {
        tracing::debug!("waiting for inbound message");
        let mut chan = self.inbound.lock().await;
        let msg = chan.recv().await.ok_or(FixmeErr)?;
        tracing::info!("received inbound message id {:?}", msg.request_id());
        Ok(msg)
    }

    #[tracing::instrument(skip(self, req), fields(req_id = ?req.req_id))]
    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::Error> {
        let req_id = req.req_id;

        // Pre-register channel
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(req_id, tx);

        self.outbound
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(
                    &Message::BatchSyncRequest(req),
                    bincode::config::standard(),
                )
                .expect("FIXME")
                .into(),
            ))
            .await
            .expect("FIXME");

        let req_timeout = override_timeout.unwrap_or(self.timeout);

        // await response with timeout & cleanup
        // FIXME make timeout adjustable
        match timeout(req_timeout, rx).await {
            Ok(Ok(resp)) => {
                tracing::info!("request {:?} completed", req_id);
                Ok(resp)
            }
            Ok(Err(e)) => {
                tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                Err(FixmeErr)
            }
            Err(_elapsed) => {
                tracing::error!("request {:?} timed out", req_id);
                Err(FixmeErr)
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("FIXME Error")]
pub struct FixmeErr;

#[derive(Debug, Clone, Default)]
pub struct MemoryStorage {
    chunks: Arc<RwLock<HashMap<Digest, Chunk>>>,
    commits: Arc<RwLock<HashMap<Digest, LooseCommit>>>,
    blobs: Arc<RwLock<HashMap<Digest, Blob>>>,
}

impl Storage for MemoryStorage {
    type Error = std::convert::Infallible;

    fn load_loose_commits(&self) -> impl Future<Output = Result<Vec<LooseCommit>, Self::Error>> {
        async {
            let commits = self.commits.read().await.values().cloned().collect();
            Ok(commits)
        }
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

    fn load_chunks(&self) -> impl Future<Output = Result<Vec<Chunk>, Self::Error>> {
        async {
            let chunks = self.chunks.read().await.values().cloned().collect();
            Ok(chunks)
        }
    }

    async fn save_blob(&self, blob: Blob) -> Result<Digest, Self::Error> {
        let digest = Digest::hash(blob.contents()); // FIXME hash should take anything that can be serialized
        self.blobs.write().await.insert(digest, blob);
        Ok(digest)
    }

    async fn load_blob(&self, blob_digest: Digest) -> Result<Option<Blob>, Self::Error> {
        let maybe_blob = self.blobs.read().await.get(&blob_digest).cloned();
        Ok(maybe_blob)
    }
}
