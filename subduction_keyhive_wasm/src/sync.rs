//! The [`KeyhiveSync`] class: Subduction's keyhive sync protocol for a
//! keyhive in JavaScript.

use alloc::{
    format,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::fmt::Display;

use async_lock::Mutex;
use future_form::{FutureForm, Local};
use futures::FutureExt;
use js_sys::{Function, Uint8Array};
use subduction_keyhive::{
    KeyhiveConnection, KeyhivePeerId, KeyhiveProtocol, KeyhiveStorage, MessageOutcome,
    SignedMessage, StorageHash,
};
use wasm_bindgen::prelude::*;

use crate::access::{JsKeyhiveAccess, WasmKeyhive};

/// An error from the JavaScript connection, as a message.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BridgeError(String);

/// Sends a peer's frames through a JavaScript callback.
#[derive(Debug, Clone)]
struct JsConnection {
    peer: KeyhivePeerId,
    send: Function,
}

impl KeyhiveConnection<Local> for JsConnection {
    type SendError = BridgeError;
    type RecvError = BridgeError;
    type DisconnectError = BridgeError;

    fn peer_id(&self) -> KeyhivePeerId {
        self.peer.clone()
    }

    fn send(
        &self,
        message: SignedMessage,
    ) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async move {
            let frame = message.to_cbor().map_err(|e| BridgeError(e.to_string()))?;
            self.send
                .call2(
                    &JsValue::NULL,
                    &Uint8Array::from(self.peer.verifying_key().as_slice()).into(),
                    &Uint8Array::from(frame.as_slice()).into(),
                )
                .map(|_| ())
                .map_err(|e| BridgeError(format!("{e:?}")))
        }
        .boxed_local()
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<SignedMessage, BridgeError>> {
        async { Err(BridgeError("frames arrive through handleFrame".into())) }.boxed_local()
    }

    fn disconnect(&self) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async { Ok(()) }.boxed_local()
    }
}

/// Stores and loads nothing.
///
/// The keyhive's event listener already reports every event it applies, and
/// the JavaScript side persists from that listener.
#[derive(Debug, Clone, Copy)]
struct ListenerPersists;

impl KeyhiveStorage<Local> for ListenerPersists {
    type Error = BridgeError;

    fn save_archive(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_archives(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, BridgeError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_archive(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async { Ok(()) }.boxed_local()
    }

    fn save_event(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_events(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, BridgeError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_event(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), BridgeError>> {
        async { Ok(()) }.boxed_local()
    }
}

type Protocol = KeyhiveProtocol<WasmKeyhive, JsConnection, ListenerPersists, Local>;

fn js_error(error: impl Display) -> JsValue {
    js_sys::Error::new(&error.to_string()).into()
}

fn peer_id(verifying_key: &[u8]) -> Result<KeyhivePeerId, JsValue> {
    let bytes: [u8; 32] = verifying_key
        .try_into()
        .map_err(|_| js_error("a peer's verifying key must be 32 bytes"))?;
    Ok(KeyhivePeerId::from_bytes(bytes))
}

/// The outcome of handling a frame.
#[wasm_bindgen(js_name = FrameOutcome)]
#[derive(Debug, Clone, Copy)]
pub struct JsFrameOutcome {
    /// Sync ops in the frame increased keyhive's op count. A contact card in
    /// the frame is not counted.
    pub ingested: bool,

    /// The sender confirmed the end of a sync round, having received the
    /// events this side sent in it. A sync check that finds the two sides
    /// in sync ends without a confirmation.
    pub confirmed: bool,
}

impl From<MessageOutcome> for JsFrameOutcome {
    fn from(outcome: MessageOutcome) -> Self {
        Self {
            ingested: outcome.ingested,
            confirmed: outcome.confirmed,
        }
    }
}

/// Runs the keyhive sync protocol for a keyhive that JavaScript owns.
///
/// Nothing serializes calls JavaScript makes on the keyhive against calls
/// these methods make, so the two must not overlap.
///
/// The protocol stores nothing. The caller persists keyhive state from the
/// keyhive's event listener, which reports every event the protocol applies.
/// Received events still waiting for their dependencies are not reported, so
/// they are lost on reload and arrive again in the next full sync.
#[wasm_bindgen]
#[derive(Debug)]
pub struct KeyhiveSync {
    protocol: Protocol,
    send: Function,
}

#[wasm_bindgen]
impl KeyhiveSync {
    /// Create the protocol for the keyhive behind `access`.
    ///
    /// `send` is called synchronously as `send(peerVerifyingKey, frame)`, both
    /// `Uint8Array`, for each outgoing frame. Its return value is ignored, so
    /// it must start delivery and return.
    ///
    /// # Errors
    ///
    /// Returns an error if `access` cannot produce the keyhive's identifier or
    /// contact card.
    pub async fn create(access: JsKeyhiveAccess, send: Function) -> Result<KeyhiveSync, JsValue> {
        let keyhive = WasmKeyhive::from(access);
        let peer = KeyhivePeerId::from_bytes(keyhive.id().map_err(js_error)?);
        let contact_card = keyhive.contact_card().await.map_err(js_error)?;
        let protocol = KeyhiveProtocol::new(
            Arc::new(Mutex::new(keyhive)),
            ListenerPersists,
            peer,
            contact_card,
        );
        Ok(Self { protocol, send })
    }

    /// Register a connected peer.
    ///
    /// # Errors
    ///
    /// Returns an error if `verifying_key` is not 32 bytes.
    #[wasm_bindgen(js_name = addPeer)]
    pub async fn add_peer(&self, verifying_key: Vec<u8>) -> Result<(), JsValue> {
        self.register(&verifying_key).await.map(|_| ())
    }

    /// Forget a disconnected peer.
    ///
    /// # Errors
    ///
    /// Returns an error if `verifying_key` is not 32 bytes.
    #[wasm_bindgen(js_name = removePeer)]
    pub async fn remove_peer(&self, verifying_key: Vec<u8>) -> Result<(), JsValue> {
        self.protocol.remove_peer(&peer_id(&verifying_key)?).await;
        Ok(())
    }

    /// Start a round with a peer. This sends a digest check if the two have a
    /// syncpoint from an earlier round, and a full sync otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if `verifying_key` is not 32 bytes, if keyhive cannot
    /// list its events or sign the message, or if the message cannot be sent.
    #[wasm_bindgen(js_name = syncWithPeer)]
    pub async fn sync_with_peer(&self, verifying_key: Vec<u8>) -> Result<(), JsValue> {
        self.protocol
            .initiate_sync_with_peer(&peer_id(&verifying_key)?)
            .await
            .map_err(js_error)
    }

    /// Start a round with a peer from a full comparison of keyhive state.
    ///
    /// # Errors
    ///
    /// Returns an error if `verifying_key` is not 32 bytes, if keyhive cannot
    /// list its events or sign the message or if the message cannot be sent.
    #[wasm_bindgen(js_name = fullSyncWithPeer)]
    pub async fn full_sync_with_peer(&self, verifying_key: Vec<u8>) -> Result<(), JsValue> {
        self.protocol
            .sync_keyhive(Some(&peer_id(&verifying_key)?))
            .await
            .map_err(js_error)
    }

    /// Handle a frame received from a peer, registering the peer first if it
    /// is not registered.
    ///
    /// Replies come from the event cache, which this method does not refresh.
    /// Call `refreshCache` after a frame whose outcome has `ingested` set.
    ///
    /// # Errors
    ///
    /// Returns an error if `verifying_key` is not 32 bytes, if the frame is
    /// invalid, if a keyhive call fails, or if a reply cannot be sent. Events
    /// can already be ingested when an error is returned.
    #[wasm_bindgen(js_name = handleFrame)]
    pub async fn handle_frame(
        &self,
        verifying_key: Vec<u8>,
        frame: Vec<u8>,
    ) -> Result<JsFrameOutcome, JsValue> {
        let peer = self.register(&verifying_key).await?;
        let message = SignedMessage::from_cbor(&frame).map_err(js_error)?;
        self.protocol
            .handle_message(&peer, message, None)
            .await
            .map(JsFrameOutcome::from)
            .map_err(js_error)
    }

    /// Rebuild the event cache if keyhive's op count has changed. Sync rounds
    /// read this cache, so call it after the keyhive changes.
    ///
    /// # Errors
    ///
    /// Returns an error if keyhive's events cannot be read.
    #[wasm_bindgen(js_name = refreshCache)]
    pub async fn refresh_cache(&self) -> Result<(), JsValue> {
        self.protocol.refresh_cache().await.map_err(js_error)
    }
}

impl KeyhiveSync {
    async fn register(&self, verifying_key: &[u8]) -> Result<KeyhivePeerId, JsValue> {
        let peer = peer_id(verifying_key)?;
        if !self.protocol.has_peer(&peer).await {
            let conn = JsConnection {
                peer: peer.clone(),
                send: self.send.clone(),
            };
            self.protocol.add_peer(peer.clone(), conn).await;
        }
        Ok(peer)
    }
}
