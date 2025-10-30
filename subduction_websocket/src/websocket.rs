//! # Generic WebSocket connection for Subduction

use crate::error::{CallError, DisconnectionError, RecvError, RunError, SendError};
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use futures::{
    channel::oneshot,
    future::{self, BoxFuture, LocalBoxFuture},
    lock::Mutex,
    FutureExt,
};
use futures_timer::Delay;
use futures_util::stream::TryStreamExt;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use sedimentree_core::future::{Local, Sendable};
use std::{collections::HashMap, sync::Arc, time::Duration};
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::{self, id::PeerId},
};

#[derive(Debug)]
pub struct Inbound {
    writer: async_channel::Sender<Message>,
    reader: Mutex<async_channel::Receiver<Message>>,
}

/// A WebSocket implementation for [`Connection`].
#[derive(Debug)]
pub struct WebSocket<T: AsyncRead + AsyncWrite + Unpin> {
    peer_id: PeerId,
    req_id_counter: Arc<Mutex<u128>>, // FIXME use cooler atomic types
    timeout: Duration,
    pub chan_id: u64,

    ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,
    outbound: Arc<Mutex<WebSocketSender<T>>>,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    pub inbound_writer: async_channel::Sender<Message>, // FIXME make private
    inbound_reader: Arc<Mutex<async_channel::Receiver<Message>>>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> WebSocket<T>
where
    WebSocket<T>: Connection<Sendable>, // FIXME remove
{
    /// Create a new WebSocket connection.
    pub fn new(ws: WebSocketStream<T>, timeout: Duration, peer_id: PeerId) -> Self {
        tracing::info!("Creating new WebSocket connection for peer {:?}", peer_id);
        let (ws_writer, ws_reader) = ws.split();
        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_reader) = async_channel::unbounded();
        let starting_counter = rand::random::<u128>();
        let chan_id = rand::random::<u64>();

        Self {
            peer_id,
            chan_id,

            req_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound: Arc::new(Mutex::new(ws_writer)),
            pending,
            inbound_writer,
            inbound_reader: Arc::new(Mutex::new(inbound_reader)),
        }
    }

    /// Listen for incoming messages and dispatch them appropriately.
    ///
    /// # Errors
    ///
    /// If there is an error reading from the WebSocket or processing messages.
    pub async fn listen(&self) -> Result<(), RunError> {
        tracing::info!("Starting WebSocket listener for peer {:?}", self.peer_id);
        let mut locked = self.ws_reader.lock().await;
        while let Some(ws_msg) = locked.next().await {
            tracing::error!("received ws message");
            match ws_msg {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    let (msg, _size): (Message, usize) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;

                    tracing::error!(
                        "decoded inbound message id {:?} from peer {:?}, message: {:?}",
                        msg.request_id(),
                        self.peer_id,
                        &msg
                    );

                    match msg {
                        Message::BatchSyncResponse(resp) => {
                            tracing::info!(
                                "received BatchSyncResponse for req_id {:?}",
                                resp.req_id
                            );
                            let req_id = resp.req_id;
                            if let Some(waiting) = self.pending.lock().await.remove(&req_id) {
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
                                tracing::error!("dispatching to inbound channel {:?}", resp.req_id);
                                self.inbound_writer
                                    .send(Message::BatchSyncResponse(resp))
                                    .await
                                    .expect("FIXME");
                                tracing::error!("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                            }
                        }
                        other => {
                            tracing::error!(
                                "dispatching to inbound channel {:?}",
                                other.request_id()
                            );
                            // let mut counter = 0;
                            // for i in 1..=10 {
                            //     // if counter % 2 == 0 {
                            //     // counter += 1;
                            //     tracing::error!(
                            //         "SEND LOOP\n count: {}\nchan_id: {}",
                            //         i,
                            //         self.chan_id
                            //     );
                            //     let _ = self
                            //         .inbound_writer
                            //         // .try_send(other.clone())
                            //         // .await
                            //         .expect("FIXME");
                            //     tracing::error!("9999999999999999999");
                            //     // }
                            // }
                            // self.recv().await.expect("FIXME");
                            // tracing::info!("000000000000000000000");
                            let _ = self.inbound_writer.send(other).await.expect("FIXME");
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
                    tracing::info!("received close message, shutting down listener");
                    // FIXME
                    // // fail all pending
                    // std::mem::take(&mut *self.pending);
                    break;
                }
                Err(e) => {
                    tracing::error!("error reading from websocket: {}", e);
                    Err(e)?
                }
            }
            tracing::info!("DONE");
            tracing::error!(chan_id = self.chan_id, "send result {:?}", self.peer_id);
        }

        Ok(())
    }
}

// impl<T: AsyncRead + AsyncWrite + Unpin> Clone for WebSocket<T> {
//     fn clone(&self) -> Self {
//         Self {
//             peer_id: self.peer_id,
//             req_id_counter: self.req_id_counter.clone(),
//             timeout: self.timeout,
//             ws_reader: self.ws_reader.clone(),
//             outbound: self.outbound.clone(),
//             pending: self.pending.clone(),
//             inbound_writer: self.inbound_writer.clone(),
//             inbound_reader: self.inbound_reader.clone(),
//         }
//     }
// }
//
// impl<T: AsyncRead + AsyncWrite + Unpin> PartialEq for WebSocket<T> {
//     fn eq(&self, other: &Self) -> bool {
//         self.peer_id == other.peer_id
//             && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
//             && Arc::ptr_eq(&self.outbound, &other.outbound)
//     }
// }

// FIXME just commentng out to debug the sendable case
// impl<T: AsyncRead + AsyncWrite + Unpin> Connection<Local> for WebSocket<T> {
//     type SendError = SendError;
//     type RecvError = RecvError;
//     type CallError = CallError;
//     type DisconnectionError = DisconnectionError;
//
//     fn peer_id(&self) -> PeerId {
//         self.peer_id
//     }
//
//     fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
//         async {
//             let mut counter = self.req_id_counter.lock().await;
//             *counter = counter.wrapping_add(1);
//             tracing::debug!("generated message id {:?}", *counter);
//             RequestId {
//                 requestor: self.peer_id,
//                 nonce: *counter,
//             }
//         }
//         .boxed_local()
//     }
//
//     fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
//         async { Ok(()) }.boxed_local()
//     }
//
//     fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
//         async move {
//             tracing::debug!("sending outbound message id {:?}", message.request_id());
//             self.outbound
//                 .lock()
//                 .await
//                 .send(tungstenite::Message::Binary(
//                     bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
//                 ))
//                 .await?;
//
//             Ok(())
//         }
//         .boxed_local()
//     }
//
//     fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
//         async {
//             tracing::debug!("Waiting for inbound message for peer {:?}", self.peer_id);
//             let msg = self
//                 .inbound_reader
//                 .recv()
//                 .await
//                 .map_err(|_| RecvError::ReadFromClosed)?;
//             tracing::info!("Received inbound message id {:?}", msg.request_id());
//             Ok(msg)
//         }
//         .boxed_local()
//     }
//
//     fn call(
//         &self,
//         req: BatchSyncRequest,
//         override_timeout: Option<Duration>,
//     ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
//         async move {
//             tracing::debug!("making call with request id {:?}", req.req_id);
//             let req_id = req.req_id;
//
//             // Pre-register channel
//             let (tx, rx) = oneshot::channel();
//             self.pending.lock().await.insert(req_id, tx);
//
//             self.outbound
//                 .lock()
//                 .await
//                 .send(tungstenite::Message::Binary(
//                     bincode::serde::encode_to_vec(
//                         Message::BatchSyncRequest(req),
//                         bincode::config::standard(),
//                     )
//                     .map_err(CallError::Serialization)?
//                     .into(),
//                 ))
//                 .await?;
//
//             tracing::info!("sent request {:?}", req_id);
//
//             let req_timeout = override_timeout.unwrap_or(self.timeout);
//
//             // await response with timeout & cleanup
//             match timeout(req_timeout, rx).await {
//                 Ok(Ok(resp)) => {
//                     tracing::info!("request {:?} completed", req_id);
//                     Ok(resp)
//                 }
//                 Ok(Err(e)) => {
//                     tracing::error!("request {:?} failed to receive response: {}", req_id, e);
//                     panic!("FIXME");
//                     // Err(CallError::ChanCanceled(e))
//                 }
//                 Err(TimedOut) => {
//                     tracing::error!("request {:?} timed out", req_id);
//                     Err(CallError::Timeout)
//                 }
//             }
//         }
//         .boxed_local()
//     }
// }

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection<Sendable> for WebSocket<T> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async {
            let mut counter = self.req_id_counter.lock().await;
            *counter = counter.wrapping_add(1);
            tracing::debug!("generated message id {:?}", *counter);
            RequestId {
                requestor: self.peer_id,
                nonce: *counter,
            }
        }
        .boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            tracing::debug!("sending outbound message id {:?}", message.request_id());
            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
                ))
                .await?;

            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        tracing::error!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);
        async move {
            tracing::error!(
                "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Waiting for inbound message for peer {:?}",
                self.peer_id
            );

            let locked = chan.lock().await;
            tracing::info!(
                len = locked.len(),
                ":::::::::::::::::: before recv for peer {:?}",
                self.peer_id
            );

            // let msg: Message;
            // let mut counter = 0;
            // loop {
            //     if counter % 100_000_000 == 0 {
            //         tracing::info!(
            //             "........................................ Waiting for inbound message for peer {:?} ({} loops)",
            //             self.peer_id,
            //             counter
            //         );
            //     }
            //     if let Ok(m) = locked.try_recv().inspect_err(|e| {
            //         if counter % 100_000_000 == 0 {
            //             tracing::error!("try_recv error:\n{}\nchan {}", e, self.chan_id);
            //         }
            //     }) {
            //         msg = m;
            //         tracing::error!("{counter} SSSSSSSSSSSSSSSSSSSSSSUUUUUUUUUUUUUUUUCCCCCCCCCCCCCEEEEEEEEEEESSSSSSSSSSSSSSSSSSSSSSSS!!");
            //         break;
            //     }
            //     counter += 1;
            // }

            let msg = locked.recv().await.map_err(|_| {
                tracing::error!(">>>>>>>>>>>>>>>>> inbound channel closed unexpectedly");
                RecvError::ReadFromClosed
            })?;
            tracing::error!("SSSSSSSSSSSSSSSSSSSSSSUUUUUUUUUUUUUUUUCCCCCCCCCCCCCEEEEEEEEEEESSSSSSSSSSSSSSSSSSSSSSSS!!");
            tracing::info!(
                "::::::::::::::::::::: Received inbound message id {:?}",
                msg.request_id()
            );
            Ok(msg)
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(req_id, tx);

            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(
                        Message::BatchSyncRequest(req),
                        bincode::config::standard(),
                    )
                    .map_err(CallError::Serialization)?
                    .into(),
                ))
                .await?;

            tracing::info!("sent request {:?}", req_id);

            let req_timeout = override_timeout.unwrap_or(self.timeout);

            // await response with timeout & cleanup
            match timeout(req_timeout, rx).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    panic!("FIXME");
                    // Err(CallError::ChanCanceled(e))
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Clone for WebSocket<T> {
    fn clone(&self) -> Self {
        Self {
            chan_id: self.chan_id,
            peer_id: self.peer_id,
            req_id_counter: self.req_id_counter.clone(),
            timeout: self.timeout,
            ws_reader: self.ws_reader.clone(),
            outbound: self.outbound.clone(),
            pending: self.pending.clone(),
            inbound_writer: self.inbound_writer.clone(),
            inbound_reader: self.inbound_reader.clone(),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> PartialEq for WebSocket<T> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
            && Arc::ptr_eq(&self.outbound, &other.outbound)
            && Arc::ptr_eq(&self.pending, &other.pending)
            && self.inbound_writer.same_channel(&other.inbound_writer)
            && Arc::ptr_eq(&self.inbound_reader, &other.inbound_reader)
    }
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    match future::select(fut, Delay::new(dur)).await {
        future::Either::Left((val, _delay)) => Ok(val),
        future::Either::Right(_) => Err(TimedOut),
    }
}

// impl<T: AsyncRead + AsyncWrite + Unpin> Drop for WebSocket<T> {
//     fn drop(&mut self) {
//         tracing::error!(
//             "-------------------- Dropping WebSocket connection for peer {:?} with count: {}",
//             self.peer_id,
//             Arc::strong_count(&self.req_id_counter)
//         );
//     }
// }
