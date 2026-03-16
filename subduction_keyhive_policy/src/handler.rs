//! Keyhive message handler that bridges the [`Handler`] trait to
//! [`KeyhiveSyncManager`].
//!
//! [`KeyhiveHandler`] receives raw keyhive wire bytes from the application's
//! composed handler, maps the subduction [`PeerId`] to a [`KeyhivePeerId`],
//! constructs a send callback from the authenticated connection, and
//! delegates to the sync manager.
//!
//! # Message type
//!
//! The handler's `Message` type is `Vec<u8>` — the raw `SUK\x00`-framed
//! wire bytes. The application's wire message enum (e.g., `WireMessage`)
//! should extract the keyhive bytes from its `Keyhive` variant and pass
//! them to this handler.
//!
//! # Send callback
//!
//! The `SendFn` type parameter provides a closure that wraps outbound
//! keyhive bytes into the application's wire message type and sends them.
//! This keeps the handler decoupled from any specific wire enum.
//!
//! [`Handler`]: subduction_core::handler::Handler
//! [`PeerId`]: subduction_core::peer::id::PeerId

use alloc::{sync::Arc, vec::Vec};

use future_form::Local;
use futures::FutureExt;
use keyhive_core::{
    content::reference::ContentRef, crypto::signer::async_signer::AsyncSigner,
    listener::membership::MembershipListener, store::ciphertext::CiphertextStore,
};
use subduction_core::{authenticated::Authenticated, handler::Handler, peer::id::PeerId};
use subduction_keyhive::{storage::KeyhiveStorage, KeyhivePeerId, KeyhiveSyncManager};

/// Keyhive message handler.
///
/// Implements [`Handler<Local, Conn>`] for raw keyhive wire bytes
/// (`Vec<u8>`). When a keyhive message arrives, the application's
/// composed handler extracts the raw bytes and passes them here.
///
/// The `M` parameter is the application's wire message type (e.g.,
/// `WireMessage`). The handler wraps outbound keyhive bytes via
/// `M: From<Vec<u8>>` to send responses back through the connection.
///
/// The handler:
///
/// 1. Looks up the sender's [`KeyhivePeerId`] from the peer map
/// 2. Constructs a send callback that wraps bytes in `M`
/// 3. Delegates to [`KeyhiveSyncManager::handle_inbound`]
///
/// If the sender's keyhive peer ID is not registered, the handler logs a
/// warning and registers a mapping from the subduction peer bytes. This
/// happens automatically when a keyhive message arrives from a peer we
/// haven't seen before — the verifying key bytes in the [`PeerId`] are
/// used to construct a [`KeyhivePeerId`].
#[allow(clippy::type_complexity)]
pub struct KeyhiveHandler<Signer, T, P, C, L, R, Store, M>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
    _wire: core::marker::PhantomData<fn(M) -> M>,
}

impl<Signer, T, P, C, L, R, Store, M> core::fmt::Debug
    for KeyhiveHandler<Signer, T, P, C, L, R, Store, M>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveHandler")
            .field("sync_manager", &self.sync_manager)
            .finish()
    }
}

impl<Signer, T, P, C, L, R, Store, M> KeyhiveHandler<Signer, T, P, C, L, R, Store, M>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    /// Create a new keyhive handler wrapping a shared sync manager.
    pub fn new(sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>) -> Self {
        Self {
            sync_manager,
            _wire: core::marker::PhantomData,
        }
    }

    /// Access the underlying sync manager.
    #[must_use]
    pub const fn sync_manager(&self) -> &Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>> {
        &self.sync_manager
    }
}

/// Errors from [`KeyhiveHandler`].
#[derive(Debug, thiserror::Error)]
pub enum KeyhiveHandlerError<SendErr: core::error::Error + 'static> {
    /// The keyhive sync manager returned an error.
    #[error("keyhive sync error: {0}")]
    Sync(subduction_keyhive::SyncManagerError<SendErr>),
}

// ── Handler impl (Local only — sendable is broken upstream) ─────────────

impl<Conn, Signer, T, P, C, L, R, Store, M> Handler<Local, Conn>
    for KeyhiveHandler<Signer, T, P, C, L, R, Store, M>
where
    Conn: subduction_core::connection::Connection<Local, M>
        + subduction_core::connection::Connection<
            Local,
            subduction_core::connection::message::SyncMessage,
        > + Clone
        + 'static,
    M: sedimentree_core::codec::encode::Encode
        + sedimentree_core::codec::decode::Decode
        + From<Vec<u8>>
        + 'static,
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: CiphertextStore<T, P> + Clone + 'static,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: KeyhiveStorage<Local> + 'static,
    Store::Error: Send + Sync + 'static,
{
    type Message = subduction_keyhive::KeyhiveMessage;
    type HandlerError =
        KeyhiveHandlerError<<Conn as subduction_core::connection::Connection<Local, M>>::SendError>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Local>,
        message: subduction_keyhive::KeyhiveMessage,
    ) -> futures::future::LocalBoxFuture<'a, Result<(), Self::HandlerError>> {
        let peer_id = conn.peer_id();
        let mgr = self.sync_manager.clone();

        async move {
            let from_keyhive_id = resolve_keyhive_peer_id(&mgr, peer_id).await;

            let send_fn = make_send_fn::<Conn, M>(conn);

            // Extract the raw CBOR payload from the wire frame.
            let wire_bytes = message.into_payload();

            mgr.handle_inbound(&from_keyhive_id, wire_bytes, send_fn)
                .await
                .map_err(KeyhiveHandlerError::Sync)
        }
        .boxed_local()
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> futures::future::LocalBoxFuture<'_, ()> {
        let mgr = self.sync_manager.clone();
        async move {
            mgr.remove_peer(peer.as_bytes()).await;
            tracing::debug!(peer = %peer, "removed keyhive peer mapping on disconnect");
        }
        .boxed_local()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Resolve or auto-register a [`KeyhivePeerId`] for a subduction [`PeerId`].
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

    // Auto-register: the verifying key bytes from PeerId are the same bytes
    // used for KeyhivePeerId (both are raw Ed25519 public keys).
    let khid = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
    tracing::debug!(
        peer = %peer_id,
        "auto-registering keyhive peer mapping"
    );
    mgr.register_peer(*peer_id.as_bytes(), khid.clone()).await;
    khid
}

/// Construct an [`AsyncSendFn`]-compatible closure from an authenticated
/// connection.
///
/// The closure wraps raw `SUK\x00`-framed bytes via `M: From<Vec<u8>>`
/// and sends them through the connection. Each invocation clones the inner
/// connection so the returned future is `'static`.
fn make_send_fn<Conn, M>(
    conn: &Authenticated<Conn, Local>,
) -> impl subduction_keyhive::AsyncSendFn<
    <Conn as subduction_core::connection::Connection<Local, M>>::SendError,
> + '_
where
    Conn: subduction_core::connection::Connection<Local, M>
        + subduction_core::connection::Connection<
            Local,
            subduction_core::connection::message::SyncMessage,
        > + Clone
        + 'static,
    M: sedimentree_core::codec::encode::Encode
        + sedimentree_core::codec::decode::Decode
        + From<Vec<u8>>
        + 'static,
{
    let inner = conn.inner().clone();
    move |wire_bytes: alloc::vec::Vec<u8>| {
        let conn = inner.clone();
        let msg = M::from(wire_bytes);
        async move { subduction_core::connection::Connection::<Local, M>::send(&conn, &msg).await }
    }
}
