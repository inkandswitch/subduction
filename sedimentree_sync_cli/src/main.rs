use clap::Parser;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Digest, LooseCommit, Sedimentree, SedimentreeId,
    SedimentreeSummary,
};
use sedimentree_sync_core::{
    connection::{Connection, Receive, SyncDiff, ToSend},
    peer::{id::PeerId, metadata::PeerMetadata},
    SedimentreeSync,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc::UnboundedReceiver, oneshot, Mutex},
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
    let mut syncer = Arc::new(Mutex::new(SedimentreeSync::new(
        HashMap::from_iter([(sed_id, sed)]),
        MemoryStorage::default(),
        HashMap::new(),
    )));

    // let tcp_listener = TcpListener::bind(&args.ws).await.expect("FIXME");

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

            let s2 = syncer.clone();
            tokio::spawn(async move { s2.lock().await.listen().await });

            tracing::info!("Attaching connection to syncer");
            syncer
                .lock()
                .await
                .attach_connection(ws) // FIXME rename to attach?
                .await
                .expect("FIXME"); // FIXME renmae to just attach?
                                  // s2.lock().await.listen().await.expect("FIXME");
        }
        Some("connect") => {
            tracing::info!("Connecting to WebSocket server at {}", args.ws);

            let (ws_stream, _) = tokio_tungstenite::connect_async(&args.ws)
                .await
                .expect("FIXME");

            tracing::info!("WebSocket server listening on {}", args.ws);

            let ws =
                WebSocket::new(ws_stream, Duration::from_secs(5), PeerId::new([1u8; 32]), 0).await;

            let s2 = syncer.clone();

            tokio::spawn(async move { s2.lock().await.listen().await });

            tracing::info!("Attaching connection to syncer");
            syncer
                .lock()
                .await
                .attach_connection(ws)
                .await
                .expect("FIXME"); // FIXME renmae to just attach?

            // s2.lock().await.listen().await.expect("FIXME");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MsgId(usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage<T> {
    msg_id: MsgId,
    payload: T,
}

#[derive(Debug, Clone)]
pub struct WebSocket {
    conn_id: usize,
    peer_id: PeerId,

    msg_id_counter: Arc<Mutex<usize>>,
    timeout: Duration,

    // FIXME renae ws_writer
    writer: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>>>,
    pending: Arc<RwLock<HashMap<MsgId, oneshot::Sender<WsMessage<Receive>>>>>,

    inbound: Arc<Mutex<UnboundedReceiver<WsMessage<Receive>>>>,
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
            MsgId,
            oneshot::Sender<WsMessage<Receive>>,
        >::new()));

        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();

        let writer = Arc::new(Mutex::new(ws_writer));
        let loop_writer = writer.clone();

        // Reader task: demux frames -> pending or events
        {
            let pending = Arc::clone(&pending);
            // let events_tx = events_tx.clone();
            tokio::spawn(async move {
                // FIXME arc the reader?
                while let Some(msg) = ws_reader.next().await {
                    match msg {
                        Ok(tungstenite::Message::Binary(bytes)) => {
                            match bincode::serde::decode_from_slice(
                                &bytes,
                                bincode::config::standard(),
                            ) {
                                Ok((WsMessage { msg_id, payload }, _size)) => {
                                    tracing::info!("received message id {:?}", msg_id);
                                    if let Some(waiting) =
                                        pending.write().expect("FIXME").remove(&msg_id)
                                    {
                                        tracing::info!("dispatching to waiter {:?}", msg_id);
                                        waiting.send(WsMessage { msg_id, payload }).expect("FIXME");
                                    } else {
                                        tracing::info!(
                                            "dispatching to inbound channel {:?}",
                                            msg_id
                                        );
                                        inbound_tx
                                            .send(WsMessage { msg_id, payload })
                                            .expect("FIXME");
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
                            loop_writer
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
                            let muts = std::mem::take(&mut *pending.write().expect("FIXME"));
                            for (_msg_id, _tx) in muts {
                                todo!()
                                // let _ = tx.send(WsMessage {
                                //     req_id: 0, /* mark error */
                                // });
                            }
                            break;
                        }
                        Err(e) => {
                            // FIXME err chan?
                            tracing::error!("WebSocket error: {}", e);
                        }
                    }
                }
            });
        }

        let starting_counter = rand::random::<u32>() as usize;

        Self {
            conn_id,
            peer_id,

            msg_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            writer,
            pending,
            inbound: Arc::new(Mutex::new(inbound_rx)),
        }
    }

    async fn get_msg_id(&self) -> MsgId {
        let mut counter = self.msg_id_counter.lock().await;
        *counter = counter.wrapping_add(1);
        tracing::info!("generated message id {:?}", *counter);
        MsgId(*counter)
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

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        Ok(())
    }

    async fn send<'a>(&self, req: ToSend<'a>) -> Result<(), Self::Error> {
        // FIXME still use this: let msg_id = self.get_msg_id().await;

        self.writer
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(&req, bincode::config::standard())
                    .expect("FIXME")
                    .into(),
            ))
            .await
            .expect("FIXME");

        Ok(())
    }

    async fn recv(&self) -> Result<Receive, Self::Error> {
        tracing::debug!(">>>>>>>>>>>>>>>> 1");
        let mut chan = self.inbound.lock().await;
        tracing::debug!(">>>>>>>>>>>>>>>> 2");
        let ws_msg = chan.recv().await.ok_or(FixmeErr)?;
        tracing::info!("received inbound message id {:?}", ws_msg.msg_id);
        match ws_msg.payload {
            Receive::BatchSyncRequest { .. } => {
                tracing::info!("sync REQUEST");
            }
            Receive::BatchSyncResponse { .. } => {
                tracing::info!("sync RESPONSE");
            }
            _ => {
                tracing::error!("unexpected message on recv: {:?}", ws_msg.payload);
            } // FIXME
        }
        tracing::info!("dispatching to caller {:?}", ws_msg.msg_id);
        Ok(ws_msg.payload)
    }

    // FIXME rename call or ask?
    // FIXME include timeout field?
    async fn request_batch_sync(
        &self,
        id: SedimentreeId,
        our_sedimentree_summary: &SedimentreeSummary,
    ) -> Result<SyncDiff, Self::Error> {
        tracing::debug!("requesting batch sync");
        let msg_id = self.get_msg_id().await;

        // Pre-register waiter to avoid races
        let (tx, rx) = oneshot::channel();
        self.pending.write().expect("FIXME").insert(msg_id, tx);

        tracing::debug!("INSIDE");

        let mut w = self.writer.lock().await;
        w.send(tungstenite::Message::Binary(
            bincode::serde::encode_to_vec(
                &WsMessage {
                    msg_id,
                    payload: ToSend::BatchSyncRequest {
                        id,
                        sedimentree_summary: our_sedimentree_summary,
                    },
                },
                bincode::config::standard(),
            )
            .expect("FIXME")
            .into(),
        ))
        // .send(WsMessage {
        //     msg_id: msg_id,
        //     payload: b"FIXME".to_vec(), // FIXME serialize req
        // })
        .await
        .expect("FIXME");
        drop(w);

        tracing::debug!("OUTSIDE");
        tracing::debug!("sent batch sync request, waiting for response");

        // await response with timeout & cleanup
        // FIXME make timeout adjustable
        // match timeout(self.timeout, rx).await {
        match timeout(self.timeout, rx).await {
            Ok(Ok(WsMessage { msg_id, payload })) => {
                match payload {
                    Receive::BatchSyncResponse { diff, .. } => Ok(diff),
                    Receive::BatchSyncRequest { .. } => {
                        tracing::error!("just a request again");
                        todo!();
                    }
                    _ => {
                        tracing::error!("unexpected response to batch sync request: {:?}", payload);
                        todo!("FIXME");
                    } // FIXME
                }
            }
            Ok(Err(e)) => {
                tracing::error!("request {:?} oneshot recv error: {:?}", msg_id, e);
                todo!("fixme");
            }
            Err(_elapsed) => {
                tracing::error!("request {:?} timed out", msg_id);
                // self.pending.write().expect("FIXME").remove(&msg_id);
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
        let commits = self
            .commits
            .read()
            .expect("FIXME")
            .values()
            .cloned()
            .collect();
        async move { Ok(commits) }
    }

    async fn save_loose_commit(&self, loose_commit: LooseCommit) -> Result<(), Self::Error> {
        let digest = loose_commit.blob().digest();
        self.commits
            .write()
            .expect("FIXME")
            .insert(digest, loose_commit);
        Ok(())
    }

    async fn save_chunk(&self, chunk: Chunk) -> Result<(), Self::Error> {
        let digest = chunk.summary().blob_meta().digest();
        self.chunks.write().expect("FIXME").insert(digest, chunk);
        Ok(())
    }

    fn load_chunks(&self) -> impl Future<Output = Result<Vec<Chunk>, Self::Error>> {
        let chunks = self
            .chunks
            .read()
            .expect("FIXME")
            .values()
            .cloned()
            .collect();
        async move { Ok(chunks) }
    }

    async fn save_blob(&self, blob: Blob) -> Result<Digest, Self::Error> {
        let digest = Digest::hash(blob.contents()); // FIXME hash should take anything that can be serialized
        self.blobs.write().expect("FIXME").insert(digest, blob);
        Ok(digest)
    }

    async fn load_blob(&self, blob_digest: Digest) -> Result<Option<Blob>, Self::Error> {
        let maybe_blob = self.blobs.read().expect("FIXME").get(&blob_digest).cloned();
        Ok(maybe_blob)
    }
}
