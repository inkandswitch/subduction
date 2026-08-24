//! Channel-backed in-memory transport.

use async_channel::{Receiver, Sender};
use future_form::{future_form, FutureForm, Local, Sendable};
use thiserror::Error;

use crate::transport::Transport;

/// One end of an in-memory framed connection.
#[derive(Debug, Clone)]
pub struct MemoryTransport {
    tx: Sender<Vec<u8>>,
    rx: Receiver<Vec<u8>>,
}

impl MemoryTransport {
    /// A connected pair of transport ends.
    #[must_use]
    pub fn pair() -> (Self, Self) {
        let (a_tx, a_rx) = async_channel::unbounded();
        let (b_tx, b_rx) = async_channel::unbounded();
        (Self { tx: a_tx, rx: b_rx }, Self { tx: b_tx, rx: a_rx })
    }

    /// One end over externally-owned channels — for wiring a node to a
    /// foreign stack (e.g. interop tests against another implementation
    /// sharing the same byte duct).
    #[must_use]
    pub const fn from_channels(tx: Sender<Vec<u8>>, rx: Receiver<Vec<u8>>) -> Self {
        Self { tx, rx }
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Transport<Async> for MemoryTransport {
    type Error = MemoryTransportClosed;

    fn send_bytes(&self, bytes: Vec<u8>) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(
            async move { self.tx.send(bytes).await.map_err(|_| MemoryTransportClosed) },
        )
    }

    fn recv_bytes(&self) -> Async::Future<'_, Result<Option<Vec<u8>>, Self::Error>> {
        Async::from_future(async move {
            // A closed channel is a clean close, not an error.
            Ok(self.rx.recv().await.ok())
        })
    }

    fn disconnect(&self) -> Async::Future<'_, ()> {
        Async::from_future(async move {
            let _was_open = self.tx.close();
            let _was_open = self.rx.close();
        })
    }
}

/// The peer end of a memory transport is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("memory transport closed")]
pub struct MemoryTransportClosed;
