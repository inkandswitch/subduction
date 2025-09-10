use clap::Parser;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use sedimentree_core::{SedimentreeId, SedimentreeSummary};
use sedimentree_sync_core::{
    connection::{Connection, Receive, SyncDiff, ToSend},
    peer::{id::PeerId, metadata::PeerMetadata},
};
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
use tokio_tungstenite::{accept_async, WebSocketStream};

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    let listener = TcpListener::bind(&args.ws).await.expect("Failed to bind");

    println!("WebSocket server listening on ws://{}", args.ws);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let ws_stream = accept_async(stream)
                .await
                .expect("WebSocket handshake failed");

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MsgId(usize);

#[derive(Debug, Clone)]
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
    writer: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>>>,
    pending: Arc<RwLock<HashMap<MsgId, oneshot::Sender<WsMessage<Receive>>>>>,

    inbound: Arc<Mutex<UnboundedReceiver<WsMessage<Receive>>>>,
}

impl WebSocket {
    pub async fn new(
        ws: WebSocketStream<TcpStream>,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: usize,
    ) -> Self {
        let (ws_writer, mut ws_reader) = ws.split();
        let pending = Arc::new(RwLock::new(HashMap::<
            MsgId,
            oneshot::Sender<WsMessage<Receive>>,
        >::new()));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let writer = Arc::new(Mutex::new(ws_writer));
        // Reader task: demux frames -> pending or events
        {
            let pending = Arc::clone(&pending);
            // let events_tx = events_tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = ws_reader.next().await {
                    match msg {
                        Ok(tungstenite::Message::Binary(bytes)) => {
                            let WsMessage { msg_id, payload } =
                                parse_response(&bytes).expect("FIXME");

                            if let Some(waiting) = pending.write().expect("FIXME").remove(&msg_id) {
                                waiting.send(WsMessage { msg_id, payload }).expect("FIXME");
                            } else {
                                tx.send(WsMessage { msg_id, payload }).expect("FIXME");
                            }
                        }
                        Ok(tungstenite::Message::Text(text)) => {
                            todo!("return error")
                        }
                        Ok(tungstenite::Message::Ping(p)) => {
                            ()
                            // FIXME? include ping? writer.lock().await.send(Message::Pong(p)).await.ok();
                        }
                        Ok(tungstenite::Message::Close(_)) | Err(_) => {
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
                        _ => todo!(),
                    }
                }
            });
        }

        Self {
            conn_id,
            peer_id,

            msg_id_counter: Arc::new(Mutex::new(0)),
            timeout,

            writer,
            pending,
            inbound: Arc::new(Mutex::new(rx)),
        }
    }

    async fn get_msg_id(&self) -> MsgId {
        let mut counter = self.msg_id_counter.lock().await;
        *counter = counter.wrapping_add(1);
        MsgId(*counter)
    }
}

fn parse_response(_bytes: &impl AsRef<[u8]>) -> Option<WsMessage<Receive>> {
    todo!()
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
            .send(tungstenite::Message::Binary(b"FIXME".to_vec().into()))
            .await
            .expect("FIXME");

        Ok(())
    }

    async fn recv(&self) -> Result<Receive, Self::Error> {
        let mut chan = self.inbound.lock().await;
        chan.recv()
            .await
            .map(|ws_msg| ws_msg.payload)
            .ok_or(FixmeErr)
    }

    // FIXME rename call or ask?
    // FIXME include timeout field?
    async fn request_batch_sync(
        &self,
        _id: SedimentreeId,
        _our_sedimentree_summary: &SedimentreeSummary,
    ) -> Result<SyncDiff<'_>, Self::Error> {
        let msg_id = self.get_msg_id().await;

        // Pre-register waiter to avoid races
        let (tx, rx) = oneshot::channel();
        self.pending.write().expect("FIXME").insert(msg_id, tx);

        self.writer
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                b"FIXME".to_vec().into(), // FIXME serialize req
            ))
            // .send(WsMessage {
            //     msg_id: msg_id,
            //     payload: b"FIXME".to_vec(), // FIXME serialize req
            // })
            .await
            .expect("FIXME");

        // await response with timeout & cleanup
        // FIXME make timeout adjustable
        match timeout(self.timeout, rx).await {
            Ok(Ok(resp)) => todo!(),                          // FIXME Ok(resp),
            Ok(Err(_canceled)) => todo!("connection closed"), // FIXME use anyhow
            Err(_elapsed) => {
                self.pending.write().expect("FIXME").remove(&msg_id);
                todo!()
                // anyhow::bail!("request {} timed out", req_id)
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("FIXME Error")]
pub struct FixmeErr;
