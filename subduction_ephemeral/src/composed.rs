//! Composed handler that dispatches [`WireMessage`] to sub-handlers.
//!
//! [`ComposedHandler`] receives a [`WireMessage`] from the connection and
//! routes it to the appropriate sub-handler based on the variant:
//!
//! - [`WireMessage::Sync`] → first handler (e.g., `SyncHandler`)
//! - [`WireMessage::Ephemeral`] → second handler (e.g., `EphemeralHandler`)
//!
//! Both sub-handlers share the same underlying `Connection<K, WireMessage>`,
//! and the connection's `recv()` returns `WireMessage` values.

use alloc::sync::Arc;

use future_form::{FutureForm, Local, Sendable};
use subduction_core::{connection::authenticated::Authenticated, handler::Handler, peer::id::PeerId};
use thiserror::Error;

use crate::wire::WireMessage;

/// Composed handler that dispatches [`WireMessage`] variants to sub-handlers.
///
/// `A` handles sync messages, `B` handles ephemeral messages.
pub struct ComposedHandler<A, B> {
    sync: Arc<A>,
    ephemeral: Arc<B>,
}

impl<A, B> core::fmt::Debug for ComposedHandler<A, B> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ComposedHandler").finish_non_exhaustive()
    }
}

impl<A, B> ComposedHandler<A, B> {
    /// Create a new composed handler from two sub-handlers.
    pub const fn new(sync: Arc<A>, ephemeral: Arc<B>) -> Self {
        Self { sync, ephemeral }
    }

    /// Access the sync sub-handler.
    #[must_use]
    pub fn sync(&self) -> &A {
        &self.sync
    }

    /// Access the ephemeral sub-handler.
    #[must_use]
    pub fn ephemeral(&self) -> &B {
        &self.ephemeral
    }
}

/// Error from a composed handler, indicating which sub-handler failed.
#[derive(Debug, Error)]
pub enum ComposedError<A: core::error::Error, B: core::error::Error> {
    /// The sync sub-handler returned an error.
    #[error(transparent)]
    Sync(A),

    /// The ephemeral sub-handler returned an error.
    #[error(transparent)]
    Ephemeral(B),
}

#[future_form::future_form(
    Sendable where
        A: Handler<Sendable, C> + Send + Sync,
        B: Handler<Sendable, C> + Send + Sync,
        C: Clone + Send + Sync + 'static,
        A::HandlerError: Send + 'static,
        B::HandlerError: Send + 'static,
    Local where
        A: Handler<Local, C>,
        B: Handler<Local, C>,
        C: Clone + 'static
)]
impl<K: FutureForm, C, A, B> Handler<K, C> for ComposedHandler<A, B>
where
    A: Handler<K, C, Message = subduction_core::connection::message::SyncMessage>,
    B: Handler<K, C, Message = crate::message::EphemeralMessage>,
{
    type Message = WireMessage;
    type HandlerError = ComposedError<A::HandlerError, B::HandlerError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, K>,
        message: WireMessage,
    ) -> K::Future<'a, Result<(), Self::HandlerError>> {
        K::from_future(async move {
            match message {
                WireMessage::Sync(m) => self
                    .sync
                    .handle(conn, *m)
                    .await
                    .map_err(ComposedError::Sync),
                WireMessage::Ephemeral(m) => self
                    .ephemeral
                    .handle(conn, m)
                    .await
                    .map_err(ComposedError::Ephemeral),
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> K::Future<'_, ()> {
        K::from_future(async move {
            self.sync.on_peer_disconnect(peer).await;
            self.ephemeral.on_peer_disconnect(peer).await;
        })
    }
}
