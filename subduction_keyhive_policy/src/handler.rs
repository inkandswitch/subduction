//! Keyhive message handler for Subduction.
//!
//! Provides [`KeyhiveProtocolHandle`], a `Send + Sync` handle that
//! bridges the [`Handler`] trait to an existing [`KeyhiveProtocol`]
//! running on a `!Send` task (e.g., a `tokio::task::LocalSet`).
//!
//! # Architecture
//!
//! ```text
//! KeyhiveProtocolHandle (Send + Sync, Clone)
//!   |  impl Handler<Sendable, C>
//!   |
//!   v  async_channel
//! run_actor (runs on LocalSet, calls !Send KeyhiveProtocol)
//!   |  loops on KeyhiveCommand
//!   v
//! KeyhiveProtocol::handle_message(...)
//! ```
//!
//! When `keyhive_core` becomes `Send`, the actor can be removed and
//! the handle can call `KeyhiveProtocol` directly.
//!
//! [`Handler`]: subduction_core::handler::Handler
//! [`KeyhiveProtocol`]: subduction_keyhive::KeyhiveProtocol

extern crate alloc;

use alloc::string::String;

use async_channel::{Receiver, Sender};
use futures::{FutureExt, future::BoxFuture};
use subduction_core::{authenticated::Authenticated, handler::Handler, peer::id::PeerId};
use subduction_keyhive::{KeyhiveMessage, SignedMessage, signed_message::CborError};

// ── Command enum ────────────────────────────────────────────────────────

/// Commands sent from the handle to the actor.
#[allow(missing_debug_implementations)]
pub enum KeyhiveCommand {
    /// Handle an inbound keyhive message from a peer.
    HandleInbound {
        /// Subduction peer ID of the sender.
        peer_id: PeerId,
        /// The keyhive wire message.
        message: KeyhiveMessage,
        /// Reply channel for the result.
        reply: Sender<Result<(), HandleError>>,
    },

    /// A peer disconnected.
    PeerDisconnect {
        /// Subduction peer ID.
        peer_id: PeerId,
    },
}

// ── Error type ──────────────────────────────────────────────────────────

/// Errors from handling a keyhive message.
#[derive(Debug, thiserror::Error)]
pub enum HandleError {
    /// Failed to decode the wire message payload into a `SignedMessage`.
    #[error("failed to decode keyhive payload: {0}")]
    Decode(#[from] CborError),

    /// The keyhive protocol returned an error.
    #[error("keyhive protocol error: {0}")]
    Protocol(String),

    /// The actor is gone (channel closed).
    #[error("keyhive actor has shut down")]
    ActorGone,
}

// ── Handle ──────────────────────────────────────────────────────────────

/// `Send + Sync` handle to a keyhive protocol actor.
///
/// Implements [`Handler<Sendable, C>`] by forwarding messages to the
/// actor via an async channel. Clone is cheap (just an `Arc` bump on
/// the channel internals).
#[derive(Clone)]
pub struct KeyhiveProtocolHandle {
    tx: Sender<KeyhiveCommand>,
}

impl core::fmt::Debug for KeyhiveProtocolHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveProtocolHandle")
            .finish_non_exhaustive()
    }
}

impl KeyhiveProtocolHandle {
    /// Create a new handle from a command sender.
    ///
    /// The corresponding [`Receiver<KeyhiveCommand>`] should be consumed
    /// by [`run_actor`].
    #[must_use]
    pub const fn new(tx: Sender<KeyhiveCommand>) -> Self {
        Self { tx }
    }

    /// Create a (handle, receiver) pair.
    ///
    /// Pass the receiver to [`run_actor`] on a `LocalSet`.
    #[must_use]
    pub fn channel() -> (Self, Receiver<KeyhiveCommand>) {
        let (tx, rx) = async_channel::unbounded();
        (Self { tx }, rx)
    }
}

// ── Actor ───────────────────────────────────────────────────────────────

/// Run the keyhive actor loop.
///
/// Spawn this on a `tokio::task::LocalSet` (or equivalent `!Send`
/// executor). It receives commands from a [`KeyhiveProtocolHandle`]
/// and calls the provided callbacks.
///
/// # Arguments
///
/// * `rx` — command receiver from [`KeyhiveProtocolHandle::channel`]
/// * `process` — async callback `(PeerId, SignedMessage) -> Result<(), String>`
///   that calls [`KeyhiveProtocol::handle_message`] internally
/// * `on_disconnect` — callback for peer cleanup (calls
///   [`KeyhiveProtocol::remove_peer`])
///
/// # Example
///
/// ```ignore
/// let (handle, rx) = KeyhiveProtocolHandle::channel();
/// let protocol: Arc<KeyhiveProtocol<...>> = ...;
///
/// local_set.spawn_local(run_actor(
///     rx,
///     |peer_id, signed_msg| {
///         let proto = protocol.clone();
///         let khid = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
///         async move {
///             proto.handle_message(&khid, signed_msg)
///                 .await
///                 .map_err(|e| e.to_string())
///         }
///     },
///     |peer_id| {
///         let proto = protocol.clone();
///         let khid = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
///         async move { proto.remove_peer(&khid).await; }
///     },
/// ));
/// ```
///
/// [`KeyhiveProtocol::handle_message`]: subduction_keyhive::KeyhiveProtocol::handle_message
/// [`KeyhiveProtocol::remove_peer`]: subduction_keyhive::KeyhiveProtocol::remove_peer
pub async fn run_actor<F, Fut, D, DFut>(rx: Receiver<KeyhiveCommand>, process: F, on_disconnect: D)
where
    F: Fn(PeerId, SignedMessage) -> Fut,
    Fut: core::future::Future<Output = Result<(), String>>,
    D: Fn(PeerId) -> DFut,
    DFut: core::future::Future<Output = ()>,
{
    while let Ok(cmd) = rx.recv().await {
        match cmd {
            KeyhiveCommand::HandleInbound {
                peer_id,
                message,
                reply,
            } => {
                let result = match message.into_signed() {
                    Ok(signed_msg) => process(peer_id, signed_msg)
                        .await
                        .map_err(HandleError::Protocol),
                    Err(e) => Err(HandleError::Decode(e)),
                };
                // Best-effort reply; handle may have timed out.
                drop(reply.send(result).await);
            }
            KeyhiveCommand::PeerDisconnect { peer_id } => {
                on_disconnect(peer_id).await;
            }
        }
    }

    tracing::debug!("keyhive actor shutting down (channel closed)");
}

// ── Handler impl ────────────────────────────────────────────────────────

impl<C> Handler<future_form::Sendable, C> for KeyhiveProtocolHandle
where
    C: Clone + Send + Sync + 'static,
{
    type Message = KeyhiveMessage;
    type HandlerError = HandleError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, future_form::Sendable>,
        message: KeyhiveMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        let peer_id = conn.peer_id();
        let tx = self.tx.clone();

        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);

            tx.send(KeyhiveCommand::HandleInbound {
                peer_id,
                message,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HandleError::ActorGone)?;

            reply_rx.recv().await.map_err(|_| HandleError::ActorGone)?
        }
        .boxed()
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        let tx = self.tx.clone();
        async move {
            drop(
                tx.send(KeyhiveCommand::PeerDisconnect { peer_id: peer })
                    .await,
            );
        }
        .boxed()
    }
}
