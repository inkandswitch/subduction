//! Handshake adapter for Iroh QUIC streams.
//!
//! Wraps a pair of QUIC [`SendStream`] / [`RecvStream`] into a type that
//! implements [`Handshake<Sendable>`], allowing Subduction's standard
//! challenge-response protocol to run over the QUIC bi-directional stream.

use alloc::vec::Vec;

use future_form::Sendable;
use futures::FutureExt;
use iroh::endpoint::{RecvStream, SendStream};
use subduction_core::connection::handshake::Handshake;

use crate::error::RunError;

/// A handshake adapter over a QUIC bi-directional stream.
///
/// Implements [`Handshake<Sendable>`] using the same length-prefixed framing
/// as the main connection. After the handshake completes, call
/// [`into_parts`](Self::into_parts) to recover the stream halves for use
/// by the listener and sender tasks.
pub struct IrohHandshake {
    send: SendStream,
    recv: RecvStream,
}

impl core::fmt::Debug for IrohHandshake {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IrohHandshake").finish_non_exhaustive()
    }
}

impl IrohHandshake {
    /// Wrap a QUIC bi-directional stream pair for handshake exchange.
    #[must_use]
    pub const fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }

    /// Consume the handshake adapter and return the stream halves.
    #[must_use]
    pub fn into_parts(self) -> (SendStream, RecvStream) {
        (self.send, self.recv)
    }
}

impl Handshake<Sendable> for IrohHandshake {
    type Error = RunError;

    fn send(
        &mut self,
        bytes: Vec<u8>,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, Result<(), Self::Error>> {
        async move { crate::tasks::write_framed(&mut self.send, &bytes).await }.boxed()
    }

    fn recv(
        &mut self,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, Result<Vec<u8>, Self::Error>> {
        async move { crate::tasks::read_framed(&mut self.recv).await }.boxed()
    }
}
