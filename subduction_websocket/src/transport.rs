//! The WebSocket transport: channel-fronted, pumped by a caller-spawned
//! future.
//!
//! A raw WebSocket stream is neither `Clone` nor shareable by `&self`,
//! so the transport is split powerbox-style: [`WebSocketTransport`] is a
//! pair of channel handles (cheaply clonable, lock-free), and
//! [`attach`] returns the pump future that owns the socket and shuttles
//! frames:
//!
//! ```text
//!  driver ── send_bytes ──▶ outgoing ──┐
//!                                      ├─ pump (caller-spawned) ⇄ WebSocket
//!  driver ◀─ recv_bytes ─── incoming ──┘
//! ```
//!
//! One complete Subduction wire message rides in one binary WebSocket
//! frame. Pings are answered by the WebSocket layer inside the pump;
//! text frames are a protocol violation and close the connection.

use async_channel::{Receiver, Sender};
use async_tungstenite::WebSocketStream;
use future_form::{future_form, FutureForm, Local, Sendable};
use futures::{
    future::{self, Either},
    pin_mut, AsyncRead, AsyncWrite, StreamExt as _,
};
use subduction_runtime::transport::Transport;
use thiserror::Error;
use tungstenite::Message;

/// One end of a WebSocket connection, as the driver sees it. See the
/// [module docs](self).
#[derive(Debug, Clone)]
pub struct WebSocketTransport {
    outgoing: Sender<Vec<u8>>,
    incoming: Receiver<Vec<u8>>,
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Transport<Async> for WebSocketTransport {
    type Error = WebSocketClosed;

    fn send_bytes(&self, bytes: Vec<u8>) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(
            async move { self.outgoing.send(bytes).await.map_err(|_| WebSocketClosed) },
        )
    }

    fn recv_bytes(&self) -> Async::Future<'_, Result<Option<Vec<u8>>, Self::Error>> {
        Async::from_future(async move {
            // A closed channel is a clean close, not an error.
            Ok(self.incoming.recv().await.ok())
        })
    }

    fn disconnect(&self) -> Async::Future<'_, ()> {
        Async::from_future(async move {
            let _was_open = self.outgoing.close();
            let _was_open = self.incoming.close();
        })
    }
}

/// Front a WebSocket stream with a [`WebSocketTransport`], returning the
/// pump future the caller must spawn. The pump owns the socket: it
/// shuttles binary frames both ways, answers pings, and sends a `Close`
/// frame when the transport is disconnected locally.
pub fn attach<S>(ws: WebSocketStream<S>) -> (WebSocketTransport, impl Future<Output = ()> + use<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (outgoing_tx, outgoing_rx) = async_channel::unbounded::<Vec<u8>>();
    let (incoming_tx, incoming_rx) = async_channel::unbounded::<Vec<u8>>();
    let transport = WebSocketTransport {
        outgoing: outgoing_tx,
        incoming: incoming_rx,
    };
    (transport, pump(ws, outgoing_rx, incoming_tx))
}

/// Shuttle frames between the channels and the socket until either side
/// goes away.
async fn pump<S>(mut ws: WebSocketStream<S>, outgoing: Receiver<Vec<u8>>, incoming: Sender<Vec<u8>>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let send = outgoing.recv();
        let recv = ws.next();
        pin_mut!(send, recv);
        match future::select(send, recv).await {
            // Locally-queued message → binary frame.
            Either::Left((Ok(bytes), _)) => {
                if ws.send(Message::Binary(bytes.into())).await.is_err() {
                    break;
                }
            }
            // Transport disconnected locally: close politely.
            Either::Left((Err(_closed), _)) => {
                let _result = ws.close(None).await;
                break;
            }
            // Inbound binary frame → driver. (Ping/pong are handled by
            // tungstenite internally as part of reading.)
            Either::Right((Some(Ok(Message::Binary(bytes))), _)) => {
                if incoming.send(bytes.into()).await.is_err() {
                    let _result = ws.close(None).await;
                    break;
                }
            }
            Either::Right((Some(Ok(Message::Ping(_) | Message::Pong(_))), _)) => {}
            // Peer closed, or the stream ended.
            Either::Right((Some(Ok(Message::Close(_))) | None, _)) => break,
            // Text (or raw frames) are a protocol violation.
            Either::Right((Some(Ok(Message::Text(_) | Message::Frame(_))), _)) => {
                tracing::warn!("non-binary WebSocket message; closing");
                let _result = ws.close(None).await;
                break;
            }
            Either::Right((Some(Err(error)), _)) => {
                tracing::debug!(%error, "WebSocket read failed; closing");
                break;
            }
        }
    }
    // Either side ended: tear both channel directions down so the
    // driver's read loop observes a clean close.
    let _was_open = outgoing.close();
    let _was_open = incoming.close();
}

/// The connection is gone (closed locally or by the peer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("websocket closed")]
pub struct WebSocketClosed;
