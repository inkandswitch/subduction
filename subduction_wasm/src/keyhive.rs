//! Subduction's keyhive sync protocol, run for the optional keyhive
//! passed into the `keyhive` constructor.

pub mod access;

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::fmt::Display;

use async_lock::{Mutex, OnceCell};
use future_form::{FutureForm, Local};
use futures::FutureExt;
use subduction_core::{authenticated::Authenticated, connection::Connection, peer::id::PeerId};
use subduction_keyhive::{
    KeyhiveConnection, KeyhiveMessage, KeyhivePeerId, KeyhiveProtocol, KeyhiveStorage,
    MessageOutcome, SignedMessage, StorageHash,
};
use wasm_bindgen::JsValue;

use crate::{subduction::NodeConnection, wire::WireMessage};
use access::{JsKeyhiveAccess, WasmKeyhive};

/// An error from the keyhive protocol or the connection it sends on, as a
/// message.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct KeyhiveError(String);

impl KeyhiveError {
    fn from_display(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

/// Sends keyhive frames on a peer's Subduction connection.
#[derive(Debug, Clone)]
struct PeerConnection {
    peer: KeyhivePeerId,
    conn: Authenticated<NodeConnection, Local>,
}

impl KeyhiveConnection<Local> for PeerConnection {
    type SendError = KeyhiveError;
    type RecvError = KeyhiveError;
    type DisconnectError = KeyhiveError;

    fn peer_id(&self) -> KeyhivePeerId {
        self.peer.clone()
    }

    fn send(
        &self,
        message: SignedMessage,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async move {
            let frame = message.to_cbor().map_err(KeyhiveError::from_display)?;
            self.conn
                .send(&WireMessage::Keyhive(KeyhiveMessage::new(frame)))
                .await
                .map_err(KeyhiveError::from_display)
        }
        .boxed_local()
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<SignedMessage, KeyhiveError>> {
        async {
            Err(KeyhiveError(
                "keyhive frames arrive through the Subduction handler".into(),
            ))
        }
        .boxed_local()
    }

    fn disconnect(&self) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }
}

/// Stores and loads nothing.
///
/// The keyhive's event listener already reports every event it applies and
/// the JavaScript side persists from that listener.
#[derive(Debug, Clone, Copy)]
struct ListenerPersists;

impl KeyhiveStorage<Local> for ListenerPersists {
    type Error = KeyhiveError;

    fn save_archive(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_archives(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, KeyhiveError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_archive(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn save_event(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_events(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, KeyhiveError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_event(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }
}

type Protocol = KeyhiveProtocol<WasmKeyhive, PeerConnection, ListenerPersists, Local>;

/// The keyhive sync protocol for a keyhive that JavaScript owns.
///
/// The protocol is created on first use, because creating it reads the
/// keyhive's contact card, which is asynchronous. It stores nothing. The
/// caller persists keyhive state from the keyhive's event listener, which
/// reports every event the protocol applies. Received events still waiting
/// for their dependencies are not reported, so they are lost on reload and
/// arrive again in the next full sync.
///
/// Nothing serializes calls JavaScript makes on the keyhive against calls
/// the protocol makes, so the two must not overlap.
pub(crate) struct KeyhiveSync {
    keyhive: WasmKeyhive,
    protocol: OnceCell<Protocol>,
    on_outcome: Option<js_sys::Function>,
}

impl core::fmt::Debug for KeyhiveSync {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveSync").finish_non_exhaustive()
    }
}

impl KeyhiveSync {
    pub(crate) fn new(access: JsKeyhiveAccess, on_outcome: Option<js_sys::Function>) -> Self {
        Self {
            keyhive: WasmKeyhive::from(access),
            protocol: OnceCell::new(),
            on_outcome,
        }
    }

    async fn protocol(&self) -> Result<&Protocol, KeyhiveError> {
        self.protocol
            .get_or_try_init(|| async {
                let peer = KeyhivePeerId::from_bytes(
                    self.keyhive.id().map_err(KeyhiveError::from_display)?,
                );
                let contact_card = self
                    .keyhive
                    .contact_card()
                    .await
                    .map_err(KeyhiveError::from_display)?;
                Ok(KeyhiveProtocol::new(
                    Arc::new(Mutex::new(self.keyhive.clone())),
                    ListenerPersists,
                    peer,
                    contact_card,
                ))
            })
            .await
    }

    /// Register `conn` as the connection for its peer, replacing any earlier
    /// one.
    async fn register(
        &self,
        protocol: &Protocol,
        conn: &Authenticated<NodeConnection, Local>,
    ) -> KeyhivePeerId {
        let peer = KeyhivePeerId::from_bytes(*conn.peer_id().as_bytes());
        protocol
            .add_peer(
                peer.clone(),
                PeerConnection {
                    peer: peer.clone(),
                    conn: conn.clone(),
                },
            )
            .await;
        peer
    }

    /// Handle a keyhive frame from `conn`'s peer, and report the outcome to
    /// the `onKeyhiveOutcome` callback. Events can already be ingested when
    /// an error is returned, and the outcome is then not reported.
    pub(crate) async fn handle_frame(
        &self,
        conn: &Authenticated<NodeConnection, Local>,
        frame: &[u8],
    ) -> Result<(), KeyhiveError> {
        let protocol = self.protocol().await?;
        let peer = self.register(protocol, conn).await;
        let message = SignedMessage::from_cbor(frame).map_err(KeyhiveError::from_display)?;
        let outcome = protocol
            .handle_message(&peer, message, None)
            .await
            .map_err(KeyhiveError::from_display)?;
        self.report(conn.peer_id(), outcome);
        Ok(())
    }

    fn report(&self, peer: PeerId, outcome: MessageOutcome) {
        let Some(callback) = &self.on_outcome else {
            return;
        };
        let peer = JsValue::from(sedimentree_wasm::pojo::hex(peer.as_bytes()));
        if let Err(e) = callback.call3(
            &JsValue::NULL,
            &peer,
            &JsValue::from_bool(outcome.ingested),
            &JsValue::from_bool(outcome.confirmed),
        ) {
            tracing::warn!("onKeyhiveOutcome callback error: {e:?}");
        }
    }

    /// Start a round with `conn`'s peer. This sends a digest check if the two
    /// have a syncpoint from an earlier round and `full` is false. Otherwise,
    /// it requests a full sync.
    pub(crate) async fn sync_with_peer(
        &self,
        conn: &Authenticated<NodeConnection, Local>,
        full: bool,
    ) -> Result<(), KeyhiveError> {
        let protocol = self.protocol().await?;
        let peer = self.register(protocol, conn).await;
        if full {
            protocol.sync_keyhive(Some(&peer)).await
        } else {
            protocol.initiate_sync_with_peer(&peer).await
        }
        .map_err(KeyhiveError::from_display)
    }

    /// Rebuild the event cache if keyhive's op count has changed.
    pub(crate) async fn refresh_cache(&self) -> Result<(), KeyhiveError> {
        self.protocol()
            .await?
            .refresh_cache()
            .await
            .map_err(KeyhiveError::from_display)
    }

    /// Forget a disconnected peer.
    pub(crate) async fn remove_peer(&self, peer: PeerId) {
        if let Some(protocol) = self.protocol.get() {
            protocol
                .remove_peer(&KeyhivePeerId::from_bytes(*peer.as_bytes()))
                .await;
        }
    }
}
