//! Byte transport capability: framed send/receive over one connection.
//!
//! # Contract
//!
//! - [`send_bytes`](Transport::send_bytes) delivers the entire message
//!   atomically (no partial sends).
//! - [`recv_bytes`](Transport::recv_bytes) returns one complete message
//!   frame (no fragmentation), or `None` when the connection is cleanly
//!   closed.
//! - A receive error is terminal: the read loop treats any error as the
//!   end of the connection. Transports that can recover from transient
//!   failures must do so internally and surface an error only when the
//!   connection is truly gone.

use future_form::FutureForm;

/// One framed, bidirectional byte connection.
pub trait Transport<Async: FutureForm>: Clone {
    /// A terminal transport failure.
    type Error: core::error::Error;

    /// Send one complete message.
    fn send_bytes(&self, bytes: Vec<u8>) -> Async::Future<'_, Result<(), Self::Error>>;

    /// Receive the next complete message; `None` on clean close.
    fn recv_bytes(&self) -> Async::Future<'_, Result<Option<Vec<u8>>, Self::Error>>;

    /// Close the connection gracefully.
    fn disconnect(&self) -> Async::Future<'_, ()>;
}
