//! Keyhive sync protocol handler.
//!
//! This module implements the keyhive synchronization protocol, which enables
//! peers to reconcile their keyhive operations (delegations, revocations,
//! prekey operations) through a three-phase exchange:
//!
//! 1. **Sync Request**: The initiator sends hashes of operations accessible to
//!    both peers, plus any pending operation hashes.
//! 2. **Sync Response**: The responder computes set differences and sends back
//!    operations the initiator is missing, along with hashes it wants from the
//!    initiator.
//! 3. **Sync Ops**: The initiator sends the requested operations.
//!
//! Contact card exchange is also handled for cases where peers don't yet know
//! each other's identity.

use alloc::{string::ToString, sync::Arc, vec, vec::Vec};

use async_lock::Mutex;
use keyhive_core::{
    contact_card::ContactCard,
    content::reference::ContentRef,
    crypto::{digest::Digest, signed::Signed, signer::async_signer::AsyncSigner},
    event::{Event, static_event::StaticEvent},
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::agent::Agent,
    store::ciphertext::CiphertextStore,
};

use crate::{
    collections::{Map, Set},
    connection::KeyhiveConnection,
    error::{ProtocolError, SigningError, StorageError, VerificationError},
    message::{EventBytes, EventHash, Message},
    peer_id::KeyhivePeerId,
    signed_message::SignedMessage,
    storage::KeyhiveStorage,
    storage_ops,
};

/// Shared keyhive instance behind a mutex.
type SharedKeyhive<Signer, T, P, C, L, R> = Arc<Mutex<Keyhive<Signer, T, P, C, L, R>>>;

/// Main keyhive sync protocol handler.
///
/// Manages peer connections and implements the keyhive sync protocol for
/// reconciling operations between peers. All keyhive access is serialized
/// through an `Arc<Mutex<Keyhive>>`.
pub struct KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Store: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    keyhive: SharedKeyhive<Signer, T, P, C, L, R>,
    storage: Store,
    peer_id: KeyhivePeerId,
    peers: Mutex<Map<KeyhivePeerId, Conn>>,
    contact_card_bytes: Vec<u8>,
    _marker: core::marker::PhantomData<K>,
}

impl<Signer, T, P, C, L, R, Conn, Store, K> core::fmt::Debug
    for KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Store: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveProtocol")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl<Signer, T, P, C, L, R, Conn, Store, K> KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Conn::SendError: 'static,
    Conn::RecvError: 'static,
    Conn::DisconnectError: 'static,
    Store: KeyhiveStorage<K>,
    K: futures_kind::FutureKind + ?Sized,
{
    /// Create a new protocol handler.
    pub fn new(
        keyhive: SharedKeyhive<Signer, T, P, C, L, R>,
        storage: Store,
        peer_id: KeyhivePeerId,
        contact_card_bytes: Vec<u8>,
    ) -> Self {
        Self {
            keyhive,
            storage,
            peer_id,
            peers: Mutex::new(Map::new()),
            contact_card_bytes,
            _marker: core::marker::PhantomData,
        }
    }

    /// Register a peer connection.
    pub async fn add_peer(&self, peer_id: KeyhivePeerId, conn: Conn) {
        self.peers.lock().await.insert(peer_id, conn);
    }

    /// Unregister a peer connection.
    pub async fn remove_peer(&self, peer_id: &KeyhivePeerId) {
        self.peers.lock().await.remove(peer_id);
    }

    /// Get the set of connected peer IDs.
    pub async fn peer_ids(&self) -> Vec<KeyhivePeerId> {
        self.peers.lock().await.keys().cloned().collect()
    }

    /// Initiate a keyhive sync with connected peers.
    ///
    /// If `target` is `Some`, only that peer is synced. Otherwise all connected
    /// peers are synced.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if peer ID conversion, message signing, or
    /// sending fails.
    pub async fn sync_keyhive(
        &self,
        target: Option<&KeyhivePeerId>,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let peer_ids = match target {
            Some(t) => vec![t.clone()],
            None => self.peer_ids().await,
        };

        for target_id in &peer_ids {
            if target_id.same_identity(&self.peer_id) {
                continue;
            }

            match self.get_hashes_for_peer_pair(target_id).await {
                Ok(Some(hashes)) => {
                    let found: Vec<EventHash> = hashes.keys().map(digest_to_bytes).collect();
                    let pending = self.get_pending_hashes().await?;

                    let message = Message::SyncRequest {
                        sender_id: self.peer_id.clone(),
                        target_id: target_id.clone(),
                        found,
                        pending,
                    };

                    tracing::debug!(
                        target = %target_id,
                        "sending sync request"
                    );

                    self.sign_and_send(target_id, message, false).await?;
                }
                Ok(None) => {
                    // We don't have an agent for this peer. Request their
                    // contact card so we can sync next time.
                    tracing::debug!(
                        target = %target_id,
                        "requesting contact card"
                    );

                    let message = Message::RequestContactCard {
                        sender_id: self.peer_id.clone(),
                        target_id: target_id.clone(),
                    };

                    self.sign_and_send(target_id, message, true).await?;
                }
                Err(e) => {
                    tracing::warn!(
                        target = %target_id,
                        error = %e,
                        "failed to get hashes for peer pair, skipping"
                    );
                }
            }
        }

        Ok(())
    }

    /// Handle an incoming signed message from a peer.
    ///
    /// Verifies the message signature, optionally ingests a contact card,
    /// and dispatches to the appropriate handler based on message type.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if signature verification, contact card
    /// ingestion, or message handling fails.
    pub async fn handle_message(
        &self,
        from: &KeyhivePeerId,
        signed_msg: SignedMessage,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let (message, contact_card_bytes) = Self::verify_and_decode(from, &signed_msg)?;

        if let Some(cc_bytes) = contact_card_bytes {
            self.ingest_contact_card(&cc_bytes).await?;
        }

        match &message {
            Message::SyncRequest { .. } => self.handle_sync_request(message).await,
            Message::SyncResponse { .. } => self.handle_sync_response(message).await,
            Message::SyncOps { .. } => self.handle_sync_ops(message).await,
            Message::RequestContactCard { .. } => self.handle_request_contact_card(message).await,
            Message::MissingContactCard { .. } => self.handle_missing_contact_card(message).await,
        }
    }

    /// Handle a `SyncRequest`: compute which ops to send and which to request.
    async fn handle_sync_request(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let Message::SyncRequest {
            sender_id,
            found: peer_found,
            pending: peer_pending,
            ..
        } = message
        else {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: "SyncRequest",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            found = peer_found.len(),
            pending = peer_pending.len(),
            "handling sync request"
        );

        let Some(local_hashes) = self.get_hashes_for_peer_pair(&sender_id).await? else {
            // We don't know this peer — request their contact card.
            tracing::debug!(from = %sender_id, "no agent found, requesting contact card");
            let msg = Message::RequestContactCard {
                sender_id: self.peer_id.clone(),
                target_id: sender_id.clone(),
            };
            return self.sign_and_send(&sender_id, msg, true).await;
        };

        let our_pending_hashes = self.get_pending_hashes().await?;

        // Build sets for comparison.
        let peer_found_set: Set<EventHash> = peer_found.iter().copied().collect();
        let peer_pending_set: Set<EventHash> = peer_pending.iter().copied().collect();
        let local_set: Set<EventHash> = local_hashes.keys().map(digest_to_bytes).collect();
        let our_pending_set: Set<EventHash> = our_pending_hashes.iter().copied().collect();

        // Ops to send = local - (peer_found U peer_pending)
        let mut found_ops: Vec<EventBytes> = Vec::new();
        for (digest, event) in &local_hashes {
            let h = digest_to_bytes(digest);
            if !peer_found_set.contains(&h) && !peer_pending_set.contains(&h) {
                let bytes = cbor_serialize(event).map_err(ProtocolError::keyhive)?;
                found_ops.push(bytes);
            }
        }

        // Ops to request = peer_found - (local U our_pending)
        let requested: Vec<EventHash> = peer_found
            .into_iter()
            .filter(|h| !local_set.contains(h) && !our_pending_set.contains(h))
            .collect();

        tracing::debug!(
            from = %sender_id,
            sending = found_ops.len(),
            requesting = requested.len(),
            "sending sync response"
        );

        let response = Message::SyncResponse {
            sender_id: self.peer_id.clone(),
            target_id: sender_id.clone(),
            requested,
            found: found_ops,
        };

        self.sign_and_send(&sender_id, response, false).await
    }

    /// Handle a `SyncResponse`: ingest events we received and send any
    /// requested ops back.
    async fn handle_sync_response(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let Message::SyncResponse {
            sender_id,
            requested: requested_hashes,
            found: found_events,
            ..
        } = message
        else {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: "SyncResponse",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            received = found_events.len(),
            requested = requested_hashes.len(),
            "handling sync response"
        );

        if !found_events.is_empty() {
            self.ingest_events(&found_events).await?;
        }

        // Send requested ops.
        if !requested_hashes.is_empty() {
            let ops = self.get_event_bytes_for_hashes(&requested_hashes).await?;

            if !ops.is_empty() {
                tracing::debug!(
                    to = %sender_id,
                    count = ops.len(),
                    "sending requested ops"
                );

                let msg = Message::SyncOps {
                    sender_id: self.peer_id.clone(),
                    target_id: sender_id.clone(),
                    ops,
                };

                self.sign_and_send(&sender_id, msg, false).await?;
            }
        }

        Ok(())
    }

    /// Handle `SyncOps`: ingest received operations.
    async fn handle_sync_ops(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let Message::SyncOps { sender_id, ops, .. } = message else {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: "SyncOps",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            count = ops.len(),
            "handling sync ops"
        );

        if !ops.is_empty() {
            self.ingest_events(&ops).await?;
        }

        Ok(())
    }

    /// Handle `RequestContactCard`: send our contact card to the requesting
    /// peer.
    async fn handle_request_contact_card(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let Message::RequestContactCard { sender_id, .. } = message else {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: "RequestContactCard",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            "sending missing contact card"
        );

        let msg = Message::MissingContactCard {
            sender_id: self.peer_id.clone(),
            target_id: sender_id.clone(),
        };

        self.sign_and_send(&sender_id, msg, true).await
    }

    /// Handle `MissingContactCard`: the contact card was already ingested in
    /// `handle_message`, so now we trigger a sync with the peer.
    async fn handle_missing_contact_card(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let Message::MissingContactCard { sender_id, .. } = message else {
            return Err(ProtocolError::UnexpectedMessageType {
                expected: "MissingContactCard",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            "received contact card, initiating sync"
        );

        self.sync_keyhive(Some(&sender_id)).await
    }

    /// Sign a message and send it to a peer.
    async fn sign_and_send(
        &self,
        target_peer_id: &KeyhivePeerId,
        message: Message,
        include_contact_card: bool,
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let msg_bytes =
            cbor_serialize(&message).map_err(|e| SigningError::Serialization(e.to_string()))?;

        let signed: Signed<Vec<u8>> = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .try_sign(msg_bytes)
                .await
                .map_err(|e| SigningError::SigningFailed(e.to_string()))?
        };

        let signed_bytes =
            cbor_serialize(&signed).map_err(|e| SigningError::Serialization(e.to_string()))?;

        let signed_message = if include_contact_card {
            SignedMessage::with_contact_card(signed_bytes, self.contact_card_bytes.clone())
        } else {
            SignedMessage::new(signed_bytes)
        };

        let conn = {
            let peers = self.peers.lock().await;
            peers
                .get(target_peer_id)
                .cloned()
                .ok_or_else(|| ProtocolError::UnknownPeer(target_peer_id.clone()))?
        };

        conn.send(signed_message).await.map_err(ProtocolError::Send)
    }

    /// Verify a signed message and decode the inner protocol message.
    #[allow(clippy::type_complexity)]
    fn verify_and_decode(
        from: &KeyhivePeerId,
        signed_msg: &SignedMessage,
    ) -> Result<(Message, Option<Vec<u8>>), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let signed: Signed<Vec<u8>> = cbor_deserialize(&signed_msg.signed)
            .map_err(|e| VerificationError::Deserialization(e.to_string()))?;

        signed
            .try_verify()
            .map_err(|_| VerificationError::InvalidSignature)?;

        // Check that the signer matches the expected sender.
        let sender_peer_id = KeyhivePeerId::from_bytes(*signed.issuer().as_bytes());
        if !sender_peer_id.same_identity(from) {
            return Err(VerificationError::SenderMismatch {
                expected: from.clone(),
                actual: sender_peer_id,
            }
            .into());
        }

        let message: Message = cbor_deserialize(signed.payload())
            .map_err(|e| VerificationError::Deserialization(e.to_string()))?;

        Ok((message, signed_msg.contact_card.clone()))
    }

    /// Get the intersection of event hashes accessible to both us and a peer.
    ///
    /// Returns `None` if the peer is unknown (no agent found in keyhive).
    async fn get_hashes_for_peer_pair(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<
        Option<Map<Digest<StaticEvent<T>>, StaticEvent<T>>>,
        ProtocolError<Conn::SendError, Conn::RecvError>,
    > {
        let our_id = self
            .peer_id
            .to_identifier()
            .map_err(ProtocolError::keyhive)?;
        let their_id = peer_id.to_identifier().map_err(ProtocolError::keyhive)?;

        let keyhive = self.keyhive.lock().await;

        let our_agent = keyhive.get_agent(our_id).await;
        let their_agent = keyhive.get_agent(their_id).await;

        match (our_agent, their_agent) {
            (Some(ref our_agent), Some(ref their_agent)) => {
                let our_events = sync_events_for_agent(&keyhive, our_agent).await;
                let their_events = sync_events_for_agent(&keyhive, their_agent).await;

                // Get events accessible to both peers.
                let intersection = our_events
                    .into_iter()
                    .filter(|(digest, _)| their_events.contains_key(digest))
                    .collect();

                Ok(Some(intersection))
            }
            _ => Ok(None),
        }
    }

    /// Get pending event hashes as `Vec<EventHash>`.
    async fn get_pending_hashes(
        &self,
    ) -> Result<Vec<EventHash>, ProtocolError<Conn::SendError, Conn::RecvError>> {
        let keyhive = self.keyhive.lock().await;
        let digests = keyhive.pending_event_hashes().await;
        Ok(digests.into_iter().map(|d| digest_to_bytes(&d)).collect())
    }

    /// Fetch serialized event bytes for the given hashes.
    async fn get_event_bytes_for_hashes(
        &self,
        hashes: &[EventHash],
    ) -> Result<Vec<EventBytes>, ProtocolError<Conn::SendError, Conn::RecvError>> {
        let requested_set: Set<EventHash> = hashes.iter().copied().collect();

        let peer_id = self
            .peer_id
            .to_identifier()
            .map_err(ProtocolError::keyhive)?;

        let keyhive = self.keyhive.lock().await;

        let Some(agent) = keyhive.get_agent(peer_id).await else {
            return Ok(Vec::new());
        };

        let events = sync_events_for_agent(&keyhive, &agent).await;

        let mut result = Vec::new();
        for (digest, event) in &events {
            let h = digest_to_bytes(digest);
            if requested_set.contains(&h) {
                let bytes = cbor_serialize(event).map_err(ProtocolError::keyhive)?;
                result.push(bytes);
            }
        }

        Ok(result)
    }

    /// Ingest received event bytes into keyhive and persist them to storage.
    ///
    /// If some events remain pending (missing dependencies), attempts recovery
    /// by loading from storage and retrying.
    async fn ingest_events(
        &self,
        event_bytes_list: &[EventBytes],
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let events: Vec<StaticEvent<T>> = event_bytes_list
            .iter()
            .map(|bytes| cbor_deserialize(bytes))
            .collect::<Result<_, _>>()
            .map_err(ProtocolError::keyhive)?;

        let pending = {
            let keyhive = self.keyhive.lock().await;
            keyhive.ingest_unsorted_static_events(events).await
        };

        if !pending.is_empty() {
            tracing::warn!(
                count = pending.len(),
                "some events pending after ingestion, attempting storage recovery"
            );

            if let Err(e) = self.try_storage_recovery(event_bytes_list).await {
                tracing::warn!(
                    error = %e,
                    "storage recovery failed"
                );
            }
        }

        for event_bytes in event_bytes_list {
            if let Err(e) = storage_ops::save_event_bytes(&self.storage, event_bytes.clone()).await
            {
                tracing::warn!(error = %e, "failed to save received event to storage");
            }
        }

        Ok(())
    }

    /// Attempt to recover by ingesting from storage and retrying.
    async fn try_storage_recovery(
        &self,
        event_bytes_list: &[EventBytes],
    ) -> Result<(), StorageError> {
        {
            let keyhive = self.keyhive.lock().await;
            storage_ops::ingest_from_storage(&keyhive, &self.storage).await?;
        }

        let events: Vec<StaticEvent<T>> = event_bytes_list
            .iter()
            .filter_map(|bytes| cbor_deserialize(bytes).ok())
            .collect();

        if !events.is_empty() {
            let keyhive = self.keyhive.lock().await;
            let retry_pending = keyhive.ingest_unsorted_static_events(events).await;
            if retry_pending.is_empty() {
                tracing::debug!("all events ingested after storage recovery");
            } else {
                tracing::warn!(
                    count = retry_pending.len(),
                    "events still pending after storage recovery"
                );
            }
        }

        Ok(())
    }

    /// Deserialize and ingest a contact card into keyhive.
    async fn ingest_contact_card(
        &self,
        cc_bytes: &[u8],
    ) -> Result<(), ProtocolError<Conn::SendError, Conn::RecvError>> {
        let contact_card: ContactCard =
            cbor_deserialize(cc_bytes).map_err(ProtocolError::keyhive)?;

        let keyhive = self.keyhive.lock().await;
        keyhive
            .receive_contact_card(&contact_card)
            .await
            .map_err(ProtocolError::keyhive)?;

        tracing::debug!("ingested contact card");
        Ok(())
    }

    /// Compact keyhive storage.
    ///
    /// Consolidates archives and removes processed events. This should be
    /// called periodically by the owner of the protocol handler.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if any storage operation, serialization, or
    /// deserialization fails.
    pub async fn compact(
        &self,
        storage_id: crate::storage::StorageHash,
    ) -> Result<(), StorageError> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::compact(&keyhive, &self.storage, storage_id).await
    }

    /// Load and ingest all stored archives and events.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if loading or ingestion fails.
    pub async fn ingest_from_storage(&self) -> Result<(), StorageError> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::ingest_from_storage(&keyhive, &self.storage).await?;
        Ok(())
    }
}

/// Get sync-relevant events for an agent, excluding CGKA operations.
async fn sync_events_for_agent<Signer, T, P, C, L, R>(
    keyhive: &Keyhive<Signer, T, P, C, L, R>,
    agent: &Agent<Signer, T, L>,
) -> Map<Digest<StaticEvent<T>>, StaticEvent<T>>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
{
    // Membership ops
    let mut ops: Map<Digest<Event<Signer, T, L>>, Event<Signer, T, L>> = keyhive
        .membership_ops_for_agent(agent)
        .await
        .into_iter()
        .map(|(digest, op)| (digest.into(), op.into()))
        .collect();

    // Prekey ops
    for key_ops in keyhive.reachable_prekey_ops_for_agent(agent).await.values() {
        for key_op in key_ops.iter() {
            let op = Event::<Signer, T, L>::from(key_op.as_ref().clone());
            ops.insert(Digest::hash(&op), op);
        }
    }

    // TODO: CGKA ops are currently excluded but will need to be added back in
    // to support encryption.

    ops.into_iter()
        .map(|(digest, event)| (digest.into(), event.into()))
        .collect()
}

/// Serialize a value to CBOR bytes.
fn cbor_serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Deserialize a value from CBOR bytes.
fn cbor_deserialize<V: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<V, StorageError> {
    ciborium::from_reader(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

/// Convert a `Digest` to a 32-byte array.
const fn digest_to_bytes<U: serde::Serialize>(digest: &Digest<U>) -> [u8; 32] {
    *digest.raw.as_bytes()
}
