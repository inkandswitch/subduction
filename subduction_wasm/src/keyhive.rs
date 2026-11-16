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
use nonempty::NonEmpty;
use sedimentree_core::collections::Map;
use subduction_core::{authenticated::Authenticated, connection::Connection, peer::id::PeerId};
use subduction_keyhive::{
    KeyhiveConnection, KeyhiveMessage, KeyhivePeerId, KeyhiveProtocol, KeyhiveStorage,
    MessageOutcome, SignedMessage, StorageHash, SyncMode,
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

/// Every connection to each peer.
pub(crate) type Connections =
    Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<NodeConnection, Local>>>>>;

/// Sends keyhive frames on one of a peer's open connections.
#[derive(Clone)]
struct PeerConnection {
    peer: PeerId,
    connections: Connections,
}

impl core::fmt::Debug for PeerConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PeerConnection")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl KeyhiveConnection<Local> for PeerConnection {
    type SendError = KeyhiveError;
    type RecvError = KeyhiveError;
    type DisconnectError = KeyhiveError;

    fn peer_id(&self) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes(*self.peer.as_bytes())
    }

    fn send(
        &self,
        message: SignedMessage,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async move {
            let frame = WireMessage::Keyhive(KeyhiveMessage::new(
                message.to_cbor().map_err(KeyhiveError::from_display)?,
            ));
            let conns: Vec<_> = self
                .connections
                .lock()
                .await
                .get(&self.peer)
                .map(|conns| conns.iter().cloned().collect())
                .unwrap_or_default();
            let mut error = KeyhiveError("the peer is not connected".into());
            for conn in conns {
                match conn.send(&frame).await {
                    Ok(()) => return Ok(()),
                    Err(e) => error = KeyhiveError::from_display(e),
                }
            }
            Err(error)
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
/// The protocol is created on first use since creating it reads the
/// keyhive's contact card, which is asynchronous. Each peer is registered
/// when it connects and keyhive frames are sent on whichever of its
/// connections accepts them.
///
/// Nothing serializes calls JavaScript makes on the keyhive against calls
/// the protocol makes, so the two must not overlap.
pub(crate) struct KeyhiveSync {
    keyhive: WasmKeyhive,
    protocol: OnceCell<Protocol>,
    on_outcome: Option<js_sys::Function>,
    connections: Connections,
}

impl core::fmt::Debug for KeyhiveSync {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveSync").finish_non_exhaustive()
    }
}

impl KeyhiveSync {
    pub(crate) fn new(
        access: JsKeyhiveAccess,
        on_outcome: Option<js_sys::Function>,
        connections: Connections,
    ) -> Self {
        Self {
            keyhive: WasmKeyhive::from(access),
            protocol: OnceCell::new(),
            on_outcome,
            connections,
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
                let protocol = KeyhiveProtocol::new(
                    Arc::new(Mutex::new(self.keyhive.clone())),
                    ListenerPersists,
                    peer,
                    contact_card,
                );
                // Peers that connected earlier, when `add_peer` could not
                // create the protocol.
                let connected: Vec<PeerId> =
                    self.connections.lock().await.keys().copied().collect();
                for peer in connected {
                    self.register(&protocol, peer).await;
                }
                Ok(protocol)
            })
            .await
    }

    /// Register a newly connected peer with the protocol, creating the
    /// protocol if this is the first.
    pub(crate) async fn add_peer(&self, peer: PeerId) -> Result<(), KeyhiveError> {
        self.register(self.protocol().await?, peer).await;
        Ok(())
    }

    async fn register(&self, protocol: &Protocol, peer: PeerId) {
        protocol
            .add_peer(
                KeyhivePeerId::from_bytes(*peer.as_bytes()),
                PeerConnection {
                    peer,
                    connections: self.connections.clone(),
                },
            )
            .await;
    }

    /// Handle a keyhive frame from `peer` and report the outcome to the
    /// `onKeyhiveOutcome` callback. Events can already be ingested when an
    /// error is returned, in which case the outcome is not reported.
    pub(crate) async fn handle_frame(
        &self,
        peer: PeerId,
        frame: &[u8],
    ) -> Result<(), KeyhiveError> {
        let protocol = self.protocol().await?;
        let message = SignedMessage::from_cbor(frame).map_err(KeyhiveError::from_display)?;
        let outcome = protocol
            .handle_message(&KeyhivePeerId::from_bytes(*peer.as_bytes()), message, None)
            .await
            .map_err(KeyhiveError::from_display)?;
        self.report(peer, outcome);
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

    /// Start a round with `peer`, as `mode` says.
    pub(crate) async fn sync_with_peer(
        &self,
        peer: PeerId,
        mode: SyncMode,
    ) -> Result<(), KeyhiveError> {
        self.protocol()
            .await?
            .sync_with_peer(&KeyhivePeerId::from_bytes(*peer.as_bytes()), mode)
            .await
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

    /// Forget a disconnected peer, unless it has connected again since.
    pub(crate) async fn remove_peer(&self, peer: PeerId) {
        // Subduction reports a disconnect after removing the peer's last
        // connection, so the peer may have connected again, and been
        // registered, before this runs.
        if self.connections.lock().await.contains_key(&peer) {
            return;
        }
        if let Some(protocol) = self.protocol.get() {
            protocol
                .remove_peer(&KeyhivePeerId::from_bytes(*peer.as_bytes()))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_errors_are_unwind_safe() {
        fn unwind_safe<T: core::panic::UnwindSafe + core::panic::RefUnwindSafe>() {}
        unwind_safe::<subduction_keyhive::KeyhiveProtocolError<WasmKeyhive, PeerConnection, Local>>(
        );
    }
}
