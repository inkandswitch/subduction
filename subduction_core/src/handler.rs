//! Message handler trait for the Subduction sync protocol.
//!
//! The [`Handler`] trait decouples _what to do with a message_ from
//! _how messages arrive_. It receives messages from authenticated
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
//!     C: Clone,
//! {
//!     type Message = SyncMessage;
//!     type HandlerError = MyError;
//!
//!     fn handle<'a>(
//!         &'a self,
//!         conn: &'a Authenticated<C, K>,
//!         message: SyncMessage,
//!     ) -> K::Future<'a, Result<(), MyError>> {
//!         K::from_future(async move {
//!             // process message ...
//!             Ok(())
//!         })
//!     }
//!
//!     fn on_peer_disconnect(&self, _peer: PeerId) -> K::Future<'_, ()> {
//!         K::from_future(async {})
//!     }
//! }
//! ```
//!
//! [`Arc`]: alloc::sync::Arc
//! [`Subduction`]: crate::subduction::Subduction
//! [`Connection::send`]: crate::connection::Connection::send

pub mod sync;

use future_form::FutureForm;

use crate::{connection::authenticated::Authenticated, peer::id::PeerId};

/// A handler for messages received from authenticated peers.
///
/// Implementors define what to do when a message arrives on a connection.
/// Any resources the handler needs (storage, sedimentrees, peer state, etc.)
/// must be provided at construction time.
///
/// # Error Semantics
///
/// Returning `Err` from [`handle`](Handler::handle) signals that the
/// connection is broken and should be dropped. If a handler wants to
/// be lenient about a particular message, it should return `Ok(())`
/// and log the issue internally.
///
/// # Type Parameters
///
/// `C` is minimally bounded (`Clone`) because [`Authenticated<C, K>`]
/// requires it. Individual impls specify their own additional bounds
/// (e.g., `C: Connection<K, SyncMessage>`).
///
/// [`Authenticated<C, K>`]: crate::connection::authenticated::Authenticated
pub trait Handler<K: FutureForm, C: Clone> {
    /// The message type this handler processes.
    ///
    /// For the standard Subduction protocol, this is
    /// [`SyncMessage`](crate::connection::message::SyncMessage).
    /// Composed handlers typically use a wire envelope type (e.g.,
    /// `WireMessage`) and dispatch to sub-handlers via pattern matching.
    type Message;

    /// Error type returned by the handler.
    type HandlerError: core::error::Error;

    /// Handle a single message from an authenticated peer.
    ///
    /// The `conn` parameter is the authenticated connection the message
    /// arrived on. The handler may use it to send responses, but should
    /// not call `recv()` — the listen loop owns the receive side.
    ///
    /// The returned future borrows both `self` and `conn` for lifetime `'a`.
    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, K>,
        message: Self::Message,
    ) -> K::Future<'a, Result<(), Self::HandlerError>>;

    /// Called when a peer's last connection drops.
    ///
    /// Use this hook to clean up per-peer state such as subscription
    /// maps, presence records, or other resources scoped to a peer's
    /// session lifetime.
    ///
    /// The listen loop calls this after [`remove_connection`] reports
    /// that the peer has no remaining connections.
    ///
    /// [`remove_connection`]: crate::subduction::Subduction::remove_connection
    fn on_peer_disconnect(&self, peer: PeerId) -> K::Future<'_, ()>;
}
