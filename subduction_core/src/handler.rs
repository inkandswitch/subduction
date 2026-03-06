//! Message handler trait for the Subduction sync protocol.
//!
//! The [`Handler`] trait decouples _what to do with a message_ from
//! _how messages arrive_. It receives decoded messages from authenticated
//! peers and processes them, returning success or an error that signals
//! the connection should be dropped.
//!
//! # Design
//!
//! A handler is a standalone type that owns whatever state it needs
//! (storage, connections, subscriptions, etc.) via [`Arc`]s passed at
//! construction time. The [`Subduction`] listen loop calls
//! [`Handler::handle`] for each message received from the wire.
//!
//! The message type must implement [`Decode`] so that it can be
//! deserialized from the wire. The handler only _receives_ decoded
//! messages — encoding of outgoing responses is handled by
//! [`Connection::send`].
//!
//! # Example
//!
//! ```ignore
//! struct MyHandler {
//!     storage: Arc<MyStorage>,
//! }
//!
//! impl<K, C> Handler<K, C> for MyHandler
//! where
//!     K: FutureForm,
//!     C: Connection<K>,
//! {
//!     type Message = Message;
//!     type HandlerError = MyError;
//!
//!     fn handle(
//!         &self,
//!         conn: &Authenticated<C, K>,
//!         message: Message,
//!     ) -> K::Future<'_, Result<(), MyError>> {
//!         K::from_future(async move {
//!             // process message ...
//!             Ok(())
//!         })
//!     }
//! }
//! ```
//!
//! [`Arc`]: alloc::sync::Arc
//! [`Subduction`]: crate::subduction::Subduction
//! [`Decode`]: sedimentree_core::codec::decode::Decode
//! [`Connection::send`]: crate::connection::Connection::send

use future_form::FutureForm;
use sedimentree_core::codec::decode::Decode;

use crate::connection::{authenticated::Authenticated, Connection};

/// A handler for messages received from authenticated peers.
///
/// Implementors define what to do when a message arrives on a connection.
/// Any resources the handler needs (storage, sedimentrees, peer state, etc.)
/// must be provided at construction time.
///
/// # Wire Compatibility
///
/// The message type [`M`](Handler::Message) must implement [`Decode`]
/// so it can be deserialized from the wire. Encoding of outgoing
/// responses is handled by the [`Connection`] layer, not the handler.
///
/// # Error Semantics
///
/// Returning `Err` from [`handle`](Handler::handle) signals that the
/// connection is broken and should be dropped. If a handler wants to
/// be lenient about a particular message, it should return `Ok(())`
/// and log the issue internally.
pub trait Handler<K: FutureForm, C: Connection<K>> {
    /// The message type this handler processes.
    ///
    /// Must support wire decoding. For the standard Subduction
    /// protocol, this is [`Message`](crate::connection::message::Message).
    type Message: Decode;

    /// Error type returned by the handler.
    type HandlerError: core::error::Error;

    /// Handle a single message from an authenticated peer.
    ///
    /// The `conn` parameter is the authenticated connection the message
    /// arrived on. The handler may use it to send responses, but should
    /// not call `recv()` — the listen loop owns the receive side.
    fn handle(
        &self,
        conn: &Authenticated<C, K>,
        message: Self::Message,
    ) -> K::Future<'_, Result<(), Self::HandlerError>>;
}
