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

use alloc::{string::String, vec::Vec};

use async_channel::{Receiver, Sender};
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    authenticated::Authenticated,
    handler::Handler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_ephemeral::policy::EphemeralPolicy;
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

    // ── ConnectionPolicy commands ───────────────────────────────────
    /// Authorize a peer connection.
    AuthorizeConnect {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Reply channel for the authorization result.
        reply: Sender<Result<(), String>>,
    },

    // ── StoragePolicy commands ──────────────────────────────────────
    /// Authorize fetching a sedimentree.
    AuthorizeFetch {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Target sedimentree.
        sedimentree_id: SedimentreeId,
        /// Reply channel for the authorization result.
        reply: Sender<Result<(), String>>,
    },

    /// Authorize putting data into a sedimentree.
    AuthorizePut {
        /// The peer requesting the put.
        requestor: PeerId,
        /// The peer that authored the data.
        author: PeerId,
        /// Target sedimentree.
        sedimentree_id: SedimentreeId,
        /// Reply channel for the authorization result.
        reply: Sender<Result<(), String>>,
    },

    /// Filter a set of sedimentree IDs to those the peer may fetch.
    FilterAuthorizedFetch {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Candidate sedimentree IDs.
        ids: Vec<SedimentreeId>,
        /// Reply channel for the filtered IDs.
        reply: Sender<Vec<SedimentreeId>>,
    },

    // ── EphemeralPolicy commands ────────────────────────────────────
    /// Authorize subscribing to ephemeral messages on a sedimentree.
    AuthorizeSubscribe {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Target sedimentree.
        id: SedimentreeId,
        /// Reply channel for the authorization result.
        reply: Sender<Result<(), String>>,
    },

    /// Authorize publishing ephemeral messages to a sedimentree.
    AuthorizePublish {
        /// Subduction peer ID.
        peer_id: PeerId,
        /// Target sedimentree.
        id: SedimentreeId,
        /// Reply channel for the authorization result.
        reply: Sender<Result<(), String>>,
    },

    /// Filter a set of peers to those authorized to receive ephemeral messages.
    FilterAuthorizedSubscribers {
        /// Target sedimentree.
        id: SedimentreeId,
        /// Candidate peers.
        peers: Vec<PeerId>,
        /// Reply channel for the filtered peers.
        reply: Sender<Vec<PeerId>>,
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

/// Error returned by policy trait impls on [`KeyhiveProtocolHandle`].
///
/// Uses `String` to avoid exposing complex keyhive-internal error types
/// through the actor channel.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct KeyhivePolicyError(String);

impl KeyhivePolicyError {
    /// Create a new policy error from any displayable value.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
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
        let (tx, rx) = async_channel::bounded(1024);
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
                // Best-effort reply; handle may have been dropped.
                drop(reply.send(result).await);
            }
            KeyhiveCommand::PeerDisconnect { peer_id } => {
                on_disconnect(peer_id).await;
            }

            // Policy commands are handled by a dedicated keyhive actor
            // that holds a SubductionKeyhive instance. In the generic
            // run_actor, these allow-all as a safe fallback.
            KeyhiveCommand::AuthorizeConnect { reply, .. }
            | KeyhiveCommand::AuthorizeFetch { reply, .. }
            | KeyhiveCommand::AuthorizePut { reply, .. }
            | KeyhiveCommand::AuthorizeSubscribe { reply, .. }
            | KeyhiveCommand::AuthorizePublish { reply, .. } => {
                drop(reply.send(Ok(())).await);
            }

            KeyhiveCommand::FilterAuthorizedFetch { ids, reply, .. } => {
                drop(reply.send(ids).await);
            }
            KeyhiveCommand::FilterAuthorizedSubscribers { peers, reply, .. } => {
                drop(reply.send(peers).await);
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

// ── ConnectionPolicy impl ──────────────────────────────────────────────

impl ConnectionPolicy<future_form::Sendable> for KeyhiveProtocolHandle {
    type ConnectionDisallowed = KeyhivePolicyError;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(KeyhiveCommand::AuthorizeConnect {
                peer_id: peer,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KeyhivePolicyError::new("actor gone"))?;

            reply_rx
                .recv()
                .await
                .map_err(|_| KeyhivePolicyError::new("actor gone"))?
                .map_err(KeyhivePolicyError)
        }
        .boxed()
    }
}

// ── StoragePolicy impl ─────────────────────────────────────────────────

impl StoragePolicy<future_form::Sendable> for KeyhiveProtocolHandle {
    type FetchDisallowed = KeyhivePolicyError;
    type PutDisallowed = KeyhivePolicyError;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(KeyhiveCommand::AuthorizeFetch {
                peer_id: peer,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KeyhivePolicyError::new("actor gone"))?;

            reply_rx
                .recv()
                .await
                .map_err(|_| KeyhivePolicyError::new("actor gone"))?
                .map_err(KeyhivePolicyError)
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(KeyhiveCommand::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KeyhivePolicyError::new("actor gone"))?;

            reply_rx
                .recv()
                .await
                .map_err(|_| KeyhivePolicyError::new("actor gone"))?
                .map_err(KeyhivePolicyError)
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            let send_result = tx
                .send(KeyhiveCommand::FilterAuthorizedFetch {
                    peer_id: peer,
                    ids: ids.clone(),
                    reply: reply_tx,
                })
                .await;

            if send_result.is_err() {
                tracing::warn!("keyhive actor gone during filter_authorized_fetch; denying all");
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        }
        .boxed()
    }
}

// ── EphemeralPolicy impl ───────────────────────────────────────────────

impl EphemeralPolicy<future_form::Sendable> for KeyhiveProtocolHandle {
    type SubscribeDisallowed = KeyhivePolicyError;
    type PublishDisallowed = KeyhivePolicyError;

    fn authorize_subscribe(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::SubscribeDisallowed>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(KeyhiveCommand::AuthorizeSubscribe {
                peer_id: peer,
                id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KeyhivePolicyError::new("actor gone"))?;

            reply_rx
                .recv()
                .await
                .map_err(|_| KeyhivePolicyError::new("actor gone"))?
                .map_err(KeyhivePolicyError)
        }
        .boxed()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PublishDisallowed>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(KeyhiveCommand::AuthorizePublish {
                peer_id: peer,
                id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| KeyhivePolicyError::new("actor gone"))?;

            reply_rx
                .recv()
                .await
                .map_err(|_| KeyhivePolicyError::new("actor gone"))?
                .map_err(KeyhivePolicyError)
        }
        .boxed()
    }

    fn filter_authorized_subscribers(
        &self,
        id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> BoxFuture<'_, Vec<PeerId>> {
        let tx = self.tx.clone();
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            let send_result = tx
                .send(KeyhiveCommand::FilterAuthorizedSubscribers {
                    id,
                    peers: peers.clone(),
                    reply: reply_tx,
                })
                .await;

            if send_result.is_err() {
                tracing::warn!(
                    "keyhive actor gone during filter_authorized_subscribers; denying all"
                );
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        }
        .boxed()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use alloc::{string::ToString, vec};

    use future_form::Sendable;
    use subduction_keyhive::SignedMessage;
    use testresult::TestResult;

    use super::*;

    #[derive(Clone, PartialEq)]
    struct DummyConn;

    fn test_peer_id() -> PeerId {
        PeerId::new([0xAB; 32])
    }

    fn valid_keyhive_message() -> KeyhiveMessage {
        let signed = SignedMessage::with_contact_card(vec![1, 2, 3], vec![4, 5, 6]);
        let cbor = signed.to_cbor().expect("CBOR serialization");
        KeyhiveMessage::new(cbor)
    }

    fn test_conn() -> Authenticated<DummyConn, future_form::Sendable> {
        Authenticated::new_for_test(DummyConn, test_peer_id())
    }

    #[tokio::test]
    async fn actor_processes_handle_inbound() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
            |_peer_id| async {},
        ));

        let conn = test_conn();
        let msg = valid_keyhive_message();
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn actor_processes_peer_disconnect() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        let (done_tx, done_rx) = async_channel::bounded::<PeerId>(1);

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
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
        let (handle, rx) = KeyhiveProtocolHandle::channel();
        drop(rx);

        let conn = test_conn();
        let msg = valid_keyhive_message();
        let result = Handler::<future_form::Sendable, DummyConn>::handle(&handle, &conn, msg).await;

        assert!(matches!(result, Err(HandleError::ActorGone)));

        Ok(())
    }

    #[tokio::test]
    async fn handle_returns_decode_error_for_invalid_cbor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
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
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Err("boom".to_string()) },
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
        let (handle, rx) = KeyhiveProtocolHandle::channel();
        drop(rx);

        // Should not panic even though the actor is gone.
        Handler::<future_form::Sendable, DummyConn>::on_peer_disconnect(&handle, test_peer_id())
            .await;

        Ok(())
    }

    // ── Policy trait integration tests ────────────────────────────────

    #[tokio::test]
    async fn policy_authorize_connect_via_actor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
            |_peer_id| async {},
        ));

        let result = ConnectionPolicy::<Sendable>::authorize_connect(&handle, test_peer_id()).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn policy_authorize_fetch_via_actor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
            |_peer_id| async {},
        ));

        let sid = SedimentreeId::new([0xCC; 32]);
        let result = StoragePolicy::<Sendable>::authorize_fetch(&handle, test_peer_id(), sid).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn policy_filter_authorized_fetch_via_actor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
            |_peer_id| async {},
        ));

        let ids = vec![
            SedimentreeId::new([0x01; 32]),
            SedimentreeId::new([0x02; 32]),
        ];
        let filtered = StoragePolicy::<Sendable>::filter_authorized_fetch(
            &handle,
            test_peer_id(),
            ids.clone(),
        )
        .await;
        assert_eq!(filtered, ids);

        Ok(())
    }

    #[tokio::test]
    async fn policy_authorize_subscribe_via_actor() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        tokio::spawn(run_actor(
            rx,
            |_peer_id, _msg| async { Ok(()) },
            |_peer_id| async {},
        ));

        let sid = SedimentreeId::new([0xDD; 32]);
        let result =
            EphemeralPolicy::<Sendable>::authorize_subscribe(&handle, test_peer_id(), sid).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn actor_shuts_down_when_all_handles_dropped() -> TestResult {
        let (handle, rx) = KeyhiveProtocolHandle::channel();

        let (done_tx, done_rx) = async_channel::bounded::<()>(1);

        tokio::spawn(async move {
            run_actor(rx, |_peer_id, _msg| async { Ok(()) }, |_peer_id| async {}).await;
            let _ = done_tx.send(()).await;
        });

        drop(handle);

        // Actor exits when all senders (handles) are dropped.
        done_rx.recv().await?;

        Ok(())
    }
}
