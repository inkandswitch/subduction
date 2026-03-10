//! Relay actor for bridging `!Send` keyhive handler operations to a
//! `Send`-safe handle.
//!
//! The [`KeyhiveHandler`](crate::handler::KeyhiveHandler) only implements
//! `Handler<Local, _>` because `keyhive_core`'s async traits produce
//! `!Send` futures. On the CLI (which uses [`Sendable`]), the composed
//! handler runs on a `Send` executor and cannot call keyhive directly.
//!
//! This module provides [`KeyhiveHandlerRelay`], a `Send + Sync + Clone`
//! handle that forwards inbound keyhive messages and disconnect events to
//! an actor future. The actor runs on a `!Send` executor (e.g., a
//! `tokio::task::LocalSet`) and processes requests sequentially using the
//! real `KeyhiveSyncManager`.
//!
//! ```text
//!                      Send boundary
//!                          │
//!  KeyhiveHandlerRelay     │   Actor future (!Send)
//!  ────────────────────    │   ─────────────────────
//!  handle_inbound()        │
//!    → RelayRequest  ────► │ ──► KeyhiveSyncManager
//!    ← Vec<Vec<u8>>  ◄──  │       ::handle_inbound()
//!                          │         calls send_fn
//!  on_peer_disconnect()    │         → reply channel
//!    → RelayRequest  ────► │ ──► KSM::remove_peer()
//! ```
//!
//! # Outbound model
//!
//! `handle_inbound` may call its send function multiple times per
//! invocation (e.g., sync response + contact card). The relay collects
//! _all_ outbound messages into a `Vec<Vec<u8>>` and returns them to
//! the caller. The caller is responsible for sending each one through
//! the real connection as `WireMessage::Keyhive(bytes)`.
//!
//! [`Sendable`]: future_form::Sendable

extern crate alloc;

use alloc::{sync::Arc, vec::Vec};

use keyhive_core::{
    content::reference::ContentRef, crypto::signer::async_signer::AsyncSigner,
    listener::membership::MembershipListener, store::ciphertext::CiphertextStore,
};
use subduction_core::peer::id::PeerId;
use subduction_keyhive::{
    KeyhivePeerId, KeyhiveSyncManager, SyncManagerError, storage::KeyhiveStorage,
};

use future_form::Local;

/// A request sent from the relay handle to the actor loop.
enum RelayRequest {
    /// Process an inbound keyhive message from a peer.
    ///
    /// The actor calls `handle_inbound` with a send function that
    /// collects outbound bytes into a channel. After completion, all
    /// collected outbound messages are sent back through `reply`.
    Inbound {
        peer_id: PeerId,
        wire_bytes: Vec<u8>,
        reply: async_channel::Sender<Result<Vec<Vec<u8>>, RelayError>>,
    },

    /// Notify the actor that a peer disconnected.
    Disconnect(PeerId),
}

/// Errors from relay operations.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    /// The relay actor has shut down.
    #[error("keyhive relay actor has shut down")]
    ActorGone,

    /// The keyhive sync manager returned an error.
    #[error(transparent)]
    SyncManager(SyncManagerError<CollectError>),
}

/// `Send`-safe handle to the keyhive handler relay actor.
///
/// Each method sends a request through a channel to the `!Send` actor
/// and awaits the reply. The futures returned are `Send`.
#[derive(Debug, Clone)]
pub struct KeyhiveHandlerRelay {
    tx: async_channel::Sender<RelayRequest>,
}

/// Receiver half of the relay channel.
///
/// Created by [`KeyhiveHandlerRelay::channel`] and consumed by
/// [`RelayReceiver::run`] to start the `!Send` actor loop.
///
/// This type is `Send`, so it can be moved into a dedicated thread
/// where the keyhive `KeyhiveSyncManager` is constructed, and then
/// [`run`](Self::run) is called with the `!Send` sync manager.
#[derive(Debug)]
pub struct RelayReceiver {
    rx: async_channel::Receiver<RelayRequest>,
}

impl RelayReceiver {
    /// Start the relay actor loop with the given sync manager.
    ///
    /// This consumes the receiver and runs until all
    /// [`KeyhiveHandlerRelay`] handles are dropped.
    ///
    /// The returned future is `!Send` — the caller must run it on an
    /// executor that supports `!Send` futures (e.g., a `LocalSet` or
    /// `wasm_bindgen_futures::spawn_local`).
    pub async fn run<Signer, T, P, C, L, R, Store>(
        self,
        sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
    ) where
        Signer: AsyncSigner + Clone + Send + 'static,
        T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: CiphertextStore<T, P> + Clone + 'static,
        L: MembershipListener<Signer, T> + Send + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
        Store: KeyhiveStorage<Local> + 'static,
        Store::Error: Send + Sync + 'static,
    {
        run_relay_actor(sync_manager, self.rx).await;
    }
}

impl KeyhiveHandlerRelay {
    /// Create a relay handle and its matching receiver.
    ///
    /// The receiver is `Send` and should be moved into the dedicated
    /// `!Send` executor thread. Call [`RelayReceiver::run`] there with
    /// the `KeyhiveSyncManager` to start the actor loop.
    ///
    /// `buffer` controls the request channel capacity.
    #[must_use]
    pub fn channel(buffer: usize) -> (Self, RelayReceiver) {
        let (tx, rx) = async_channel::bounded(buffer);
        (Self { tx }, RelayReceiver { rx })
    }

    /// Create a new relay handle and its corresponding actor future.
    ///
    /// The `sync_manager` is moved into the actor future. All handler
    /// operations go through the returned handle via channels.
    ///
    /// `buffer` controls the request channel capacity.
    ///
    /// The returned future is `!Send` — the caller must spawn it on an
    /// executor that supports `!Send` futures (e.g., a `LocalSet` or
    /// `wasm_bindgen_futures::spawn_local`).
    pub fn new<Signer, T, P, C, L, R, Store>(
        sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
        buffer: usize,
    ) -> (Self, impl core::future::Future<Output = ()>)
    where
        Signer: AsyncSigner + Clone + Send + 'static,
        T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: CiphertextStore<T, P> + Clone + 'static,
        L: MembershipListener<Signer, T> + Send + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
        Store: KeyhiveStorage<Local> + 'static,
        Store::Error: Send + Sync + 'static,
    {
        let (tx, rx) = async_channel::bounded(buffer);
        let handle = Self { tx };
        let actor_fut = run_relay_actor(sync_manager, rx);
        (handle, actor_fut)
    }

    /// Forward an inbound keyhive message and collect outbound responses.
    ///
    /// Returns the raw `SUK\x00`-framed bytes for each outbound message
    /// that the sync manager wants to send back. The caller wraps each
    /// one in the application's wire message type and sends it through
    /// the real connection.
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::ActorGone`] if the actor has shut down, or
    /// [`RelayError::Sync`] if the sync manager returns an error.
    pub async fn handle_inbound(
        &self,
        peer_id: PeerId,
        wire_bytes: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, RelayError> {
        let (reply_tx, reply_rx) = async_channel::bounded(1);
        self.tx
            .send(RelayRequest::Inbound {
                peer_id,
                wire_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RelayError::ActorGone)?;

        reply_rx.recv().await.map_err(|_| RelayError::ActorGone)?
    }

    /// Notify the relay that a peer disconnected.
    ///
    /// This removes the peer's keyhive mapping from the sync manager.
    /// Fire-and-forget: if the actor is gone, the disconnect is silently
    /// dropped.
    pub async fn on_peer_disconnect(&self, peer_id: PeerId) {
        drop(self.tx.send(RelayRequest::Disconnect(peer_id)).await);
    }
}

/// The relay actor loop.
///
/// Processes handler requests sequentially using the `!Send`
/// `KeyhiveSyncManager`. This future is `!Send` and must be spawned
/// on an appropriate executor.
async fn run_relay_actor<Signer, T, P, C, L, R, Store>(
    sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
    rx: async_channel::Receiver<RelayRequest>,
) where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: CiphertextStore<T, P> + Clone + 'static,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: KeyhiveStorage<Local> + 'static,
    Store::Error: Send + Sync + 'static,
{
    while let Ok(req) = rx.recv().await {
        match req {
            RelayRequest::Inbound {
                peer_id,
                wire_bytes,
                reply,
            } => {
                let result = handle_inbound_request(&sync_manager, peer_id, wire_bytes).await;
                drop(reply.send(result).await);
            }

            RelayRequest::Disconnect(peer_id) => {
                sync_manager.remove_peer(peer_id.as_bytes()).await;
                tracing::debug!(peer = %peer_id, "relay: removed keyhive peer on disconnect");
            }
        }
    }

    tracing::debug!("keyhive relay actor shutting down (all handles dropped)");
}

/// Process a single inbound keyhive message.
///
/// Resolves the peer's keyhive ID, constructs a collecting send function,
/// calls `handle_inbound`, and returns all collected outbound messages.
async fn handle_inbound_request<Signer, T, P, C, L, R, Store>(
    sync_manager: &KeyhiveSyncManager<Signer, T, P, C, L, R, Store>,
    peer_id: PeerId,
    wire_bytes: Vec<u8>,
) -> Result<Vec<Vec<u8>>, RelayError>
where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: CiphertextStore<T, P> + Clone + 'static,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: KeyhiveStorage<Local> + 'static,
    Store::Error: Send + Sync + 'static,
{
    let from_keyhive_id = resolve_keyhive_peer_id(sync_manager, peer_id).await;

    // Collecting send function: accumulates outbound bytes in a RefCell
    // so that handle_inbound can call it multiple times.
    let outbound: core::cell::RefCell<Vec<Vec<u8>>> = core::cell::RefCell::new(Vec::new());

    let send_fn = |bytes: Vec<u8>| {
        outbound.borrow_mut().push(bytes);
        async { Ok::<(), CollectError>(()) }
    };

    sync_manager
        .handle_inbound(&from_keyhive_id, wire_bytes, send_fn)
        .await
        .map_err(RelayError::SyncManager)?;

    Ok(outbound.into_inner())
}

/// Resolve or auto-register a [`KeyhivePeerId`] for a subduction [`PeerId`].
///
/// Delegates to the shared helper in [`crate::handler`] logic: if the peer
/// is already registered, returns the existing mapping. Otherwise,
/// auto-registers using the Ed25519 public key bytes from the `PeerId`.
async fn resolve_keyhive_peer_id<Signer, T, P, C, L, R, Store>(
    mgr: &KeyhiveSyncManager<Signer, T, P, C, L, R, Store>,
    peer_id: PeerId,
) -> KeyhivePeerId
where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
    Store::Error: Send + Sync + 'static,
{
    if let Some(khid) = mgr.keyhive_peer_id_for(peer_id.as_bytes()).await {
        return khid;
    }

    let khid = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
    tracing::debug!(
        peer = %peer_id,
        "relay: auto-registering keyhive peer mapping"
    );
    mgr.register_peer(*peer_id.as_bytes(), khid.clone()).await;
    khid
}

/// Error type for the collecting send function.
///
/// The collecting send function never actually fails (it just pushes to
/// a `Vec`), but `AsyncSendFn` requires an error type. This is public
/// so that [`RelayError::SyncManager`] can name the concrete
/// `SyncManagerError<CollectError>` without type erasure.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("collecting send (unreachable)")]
pub struct CollectError;
