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
//! KeyhiveProtocol::handle_inbound(...)
//! ```
//!
//! When `keyhive_core` becomes `Send`, the actor can be removed and
//! the handle can call `KeyhiveProtocol` directly.
//!
//! [`Handler`]: subduction_core::handler::Handler
//! [`KeyhiveProtocol`]: crate::KeyhiveProtocol

extern crate alloc;

use alloc::string::String;

use async_channel::{Receiver, Sender};
use futures::{FutureExt, future::BoxFuture};
use subduction_core::{authenticated::Authenticated, handler::Handler, peer::id::PeerId};

use crate::{KeyhiveMessage, SignedMessage, signed_message::CborError};

// ── Command enum ────────────────────────────────────────────────────────

/// Commands sent from the handle to the actor.
///
/// Two connection generics:
///
/// * `C` is the [`Handler`] trait's connection payload, forwarded over
///   `HandleInbound` so the actor can auto-register unknown peers.
/// * `Conn` is the per-peer keyhive connection passed to
///   [`KeyhiveProtocol::add_peer`](crate::KeyhiveProtocol::add_peer).
#[allow(missing_debug_implementations)]
pub enum KeyhiveCommand<C, Conn> {
    /// Handle an inbound keyhive message from a peer.
    HandleInbound {
        /// Subduction peer ID of the sender.
        peer_id: PeerId,
        /// Connection payload from the inbound `Authenticated<C>`. The
        /// orchestrator converts this to a `Conn` and uses it to
        /// auto-register the peer when a sync-check arrives before any
        /// explicit `AddPeer`.
        c: C,
        /// The keyhive wire message.
        message: KeyhiveMessage,
        /// Reply channel for the result.
        reply: Sender<Result<(), HandleError>>,
    },

    /// A peer connected; register it with the keyhive protocol.
    AddPeer {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Connection handle the actor should hand to
        /// `KeyhiveProtocol::add_peer`.
        conn: Conn,
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
pub struct KeyhiveProtocolHandle<C, Conn> {
    tx: Sender<KeyhiveCommand<C, Conn>>,
}

impl<C, Conn> Clone for KeyhiveProtocolHandle<C, Conn> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<C, Conn> core::fmt::Debug for KeyhiveProtocolHandle<C, Conn> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveProtocolHandle")
            .finish_non_exhaustive()
    }
}

impl<C, Conn> KeyhiveProtocolHandle<C, Conn> {
    /// Create a new handle from a command sender.
    ///
    /// The corresponding [`Receiver<KeyhiveCommand<C, Conn>>`] should be
    /// consumed by [`run_actor`].
    #[must_use]
    pub const fn new(tx: Sender<KeyhiveCommand<C, Conn>>) -> Self {
        Self { tx }
    }

    /// Create a (handle, receiver) pair.
    ///
    /// Pass the receiver to [`run_actor`] on a `LocalSet`.
    #[must_use]
    pub fn channel() -> (Self, Receiver<KeyhiveCommand<C, Conn>>) {
        let (tx, rx) = async_channel::bounded(1024);
        (Self { tx }, rx)
    }

    /// Register a newly-connected peer with the keyhive actor.
    ///
    /// Fire-and-forget: if the actor has shut down, the command is
    /// silently dropped (same semantics as [`on_peer_disconnect`]).
    ///
    /// [`on_peer_disconnect`]: subduction_core::handler::Handler::on_peer_disconnect
    pub async fn on_peer_connect(&self, peer_id: PeerId, conn: Conn) {
        drop(
            self.tx
                .send(KeyhiveCommand::AddPeer { peer_id, conn })
                .await,
        );
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
/// * `process` — async callback `(PeerId, Conn, SignedMessage) -> Result<(), String>`
///   that calls [`KeyhiveProtocol::handle_message`] internally and may
///   auto-register `Conn` on sync-check from unknown peers
/// * `on_connect` — callback for peer registration (calls
///   [`KeyhiveProtocol::add_peer`])
/// * `on_disconnect` — callback for peer cleanup (calls
///   [`KeyhiveProtocol::remove_peer`])
///
/// [`KeyhiveProtocol::handle_message`]: crate::KeyhiveProtocol::handle_message
/// [`KeyhiveProtocol::add_peer`]: crate::KeyhiveProtocol::add_peer
/// [`KeyhiveProtocol::remove_peer`]: crate::KeyhiveProtocol::remove_peer
pub async fn run_actor<C, Conn, F, Fut, A, AFut, D, DFut>(
    rx: Receiver<KeyhiveCommand<C, Conn>>,
    process: F,
    on_connect: A,
    on_disconnect: D,
) where
    F: Fn(PeerId, C, SignedMessage) -> Fut,
    Fut: core::future::Future<Output = Result<(), String>>,
    A: Fn(PeerId, Conn) -> AFut,
    AFut: core::future::Future<Output = ()>,
    D: Fn(PeerId) -> DFut,
    DFut: core::future::Future<Output = ()>,
{
    while let Ok(cmd) = rx.recv().await {
        match cmd {
            KeyhiveCommand::HandleInbound {
                peer_id,
                c,
                message,
                reply,
            } => {
                let result = match message.into_signed() {
                    Ok(signed_msg) => process(peer_id, c, signed_msg)
                        .await
                        .map_err(HandleError::Protocol),
                    Err(e) => Err(HandleError::Decode(e)),
                };
                // Best-effort reply; handle may have been dropped.
                drop(reply.send(result).await);
            }
            KeyhiveCommand::AddPeer { peer_id, conn } => {
                on_connect(peer_id, conn).await;
            }
            KeyhiveCommand::PeerDisconnect { peer_id } => {
                on_disconnect(peer_id).await;
            }
        }
    }

    tracing::debug!("keyhive actor shutting down (channel closed)");
}

// ── Handler impl ────────────────────────────────────────────────────────

impl<C, Conn> Handler<future_form::Sendable, C> for KeyhiveProtocolHandle<C, Conn>
where
    C: Clone + Send + Sync + 'static,
    Conn: Send + 'static,
{
    type Message = KeyhiveMessage;
    type HandlerError = HandleError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, future_form::Sendable>,
        message: KeyhiveMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        let peer_id = conn.peer_id();
        let c = conn.inner().clone();
        let tx = self.tx.clone();

        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);

            tx.send(KeyhiveCommand::HandleInbound {
                peer_id,
                c,
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use alloc::{string::ToString, vec};

    use crate::SignedMessage;
    use testresult::TestResult;

    use super::*;

    #[derive(Clone, PartialEq)]
    struct DummyConn;

    fn test_peer_id() -> PeerId {
        PeerId::new([0xAB; 32])
    }

    fn valid_keyhive_message() -> KeyhiveMessage {
        // The actor-decoding tests only need a syntactically-valid
        // SignedMessage. They don't exercise contact-card behaviour.
        let signed = SignedMessage::new(vec![1, 2, 3]);
        let cbor = signed.to_cbor().expect("CBOR serialization");
        KeyhiveMessage::new(cbor)
    }

    fn test_conn() -> Authenticated<DummyConn, future_form::Sendable> {
        Authenticated::new_for_test(DummyConn, test_peer_id())
    }

    #[tokio::test]
    async fn actor_processes_handle_inbound() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _conn, _msg| async { Ok(()) },
            |_peer_id, _conn| async {},
            |_peer_id| async {},
        ));

        let conn = test_conn();
        let msg = valid_keyhive_message();
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn actor_processes_peer_connect() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        let (done_tx, done_rx) = async_channel::bounded::<PeerId>(1);

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _conn, _msg| async { Ok(()) },
            move |peer_id, _conn| {
                let done_tx = done_tx.clone();
                async move {
                    let _ = done_tx.send(peer_id).await;
                }
            },
            |_peer_id| async {},
        ));

        handle.on_peer_connect(test_peer_id(), DummyConn).await;

        let connected_peer = done_rx.recv().await?;
        assert_eq!(connected_peer, test_peer_id());

        Ok(())
    }

    #[tokio::test]
    async fn actor_processes_peer_disconnect() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        let (done_tx, done_rx) = async_channel::bounded::<PeerId>(1);

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _conn, _msg| async { Ok(()) },
            |_peer_id, _conn| async {},
            move |peer_id| {
                let done_tx = done_tx.clone();
                async move {
                    let _ = done_tx.send(peer_id).await;
                }
            },
        ));

        Handler::<future_form::Sendable, DummyConn>::on_peer_disconnect(&handle, test_peer_id())
            .await;

        let disconnected_peer = done_rx.recv().await?;
        assert_eq!(disconnected_peer, test_peer_id());

        Ok(())
    }

    #[tokio::test]
    async fn handle_returns_actor_gone_when_receiver_dropped() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();
        drop(rx);

        let conn = test_conn();
        let msg = valid_keyhive_message();
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;

        assert!(matches!(result, Err(HandleError::ActorGone)));

        Ok(())
    }

    #[tokio::test]
    async fn handle_returns_decode_error_for_invalid_cbor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _conn, _msg| async { Ok(()) },
            |_peer_id, _conn| async {},
            |_peer_id| async {},
        ));

        let conn = test_conn();
        let msg = KeyhiveMessage::new(vec![0xFF, 0xFF]);
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;

        assert!(matches!(result, Err(HandleError::Decode(_))));

        Ok(())
    }

    #[tokio::test]
    async fn handle_returns_protocol_error_from_process() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _conn, _msg| async { Err("boom".to_string()) },
            |_peer_id, _conn| async {},
            |_peer_id| async {},
        ));

        let conn = test_conn();
        let msg = valid_keyhive_message();
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;

        match result {
            Err(HandleError::Protocol(s)) => assert_eq!(s, "boom"),
            other => panic!("expected Protocol(\"boom\"), got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn disconnect_is_fire_and_forget_when_actor_gone() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();
        drop(rx);

        // Should not panic even though the actor is gone.
        Handler::<future_form::Sendable, DummyConn>::on_peer_disconnect(&handle, test_peer_id())
            .await;

        Ok(())
    }

    #[tokio::test]
    async fn actor_shuts_down_when_all_handles_dropped() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::<DummyConn, DummyConn>::channel();

        let (done_tx, done_rx) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            run_actor(
                rx,
                |_peer_id, _conn, _msg| async { Ok(()) },
                |_peer_id, _conn| async {},
                |_peer_id| async {},
            )
            .await;
            let _ = done_tx.send(()).await;
        });

        drop(handle);

        // Actor exits when all senders (handles) are dropped.
        done_rx.recv().await?;

        Ok(())
    }
}
