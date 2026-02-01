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
        let verified = signed_msg.verify(from)?;

        if let Some(cc_bytes) = &verified.contact_card {
            self.ingest_contact_card(cc_bytes).await?;
        }

        let message: Message = cbor_deserialize(&verified.payload)
            .map_err(|e| VerificationError::Deserialization(e.to_string()))?;

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
                let bytes =
                    cbor_serialize(event).map_err(|e| ProtocolError::Keyhive(e.to_string()))?;
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
            .map_err(|e| ProtocolError::Keyhive(e.to_string()))?;
        let their_id = peer_id
            .to_identifier()
            .map_err(|e| ProtocolError::Keyhive(e.to_string()))?;

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
            .map_err(|e| ProtocolError::Keyhive(e.to_string()))?;

        let keyhive = self.keyhive.lock().await;

        let Some(agent) = keyhive.get_agent(peer_id).await else {
            return Ok(Vec::new());
        };

        let events = sync_events_for_agent(&keyhive, &agent).await;

        let mut result = Vec::new();
        for (digest, event) in &events {
            let h = digest_to_bytes(digest);
            if requested_set.contains(&h) {
                let bytes =
                    cbor_serialize(event).map_err(|e| ProtocolError::Keyhive(e.to_string()))?;
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
            .map_err(|e| ProtocolError::Keyhive(e.to_string()))?;

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
            cbor_deserialize(cc_bytes).map_err(|e| ProtocolError::Keyhive(e.to_string()))?;

        let keyhive = self.keyhive.lock().await;
        keyhive
            .receive_contact_card(&contact_card)
            .await
            .map_err(|e| ProtocolError::Keyhive(e.to_string()))?;

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
    #[allow(clippy::type_complexity)]
    let mut ops: Map<Digest<Event<Signer, T, L>>, Event<Signer, T, L>> = keyhive
        .membership_ops_for_agent(agent)
        .await
        .into_iter()
        .map(|(digest, op)| (digest.into(), op.into()))
        .collect();

    // Prekey ops
    for key_ops in keyhive.reachable_prekey_ops_for_agent(agent).await.values() {
        for key_op in key_ops {
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::{
        storage::MemoryKeyhiveStorage,
        test_utils::{
            TestProtocol, TwoPeerHarness, create_channel_pair, create_group_with_read_members,
            exchange_all_contact_cards, exchange_contact_cards_and_setup, keyhive_peer_id,
            make_keyhive, make_protocol_with_shared_keyhive, run_sync_round,
            serialize_contact_card,
        },
    };
    use futures_kind::Local;
    use keyhive_core::{
        access::Access,
        principal::{identifier::Identifier, membered::Membered, peer::Peer},
    };
    use nonempty::nonempty;

    /// Helper to create a test protocol instance.
    async fn make_protocol() -> (TestProtocol, crate::test_utils::SimpleKeyhive) {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.unwrap();
        let cc_bytes = serialize_contact_card(&cc);
        let storage = MemoryKeyhiveStorage::new();
        let shared = Arc::new(Mutex::new(keyhive.clone()));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc_bytes);
        (protocol, keyhive)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sign_and_verify_roundtrip() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        // Create a peer so we can send to them
        let other = make_keyhive().await;
        let other_id = keyhive_peer_id(&other);

        let (conn_to_other, conn_to_us) = create_channel_pair(peer_id.clone(), &other_id);
        protocol.add_peer(other_id.clone(), conn_to_other).await;

        // Send a message
        let message = Message::RequestContactCard {
            sender_id: peer_id.clone(),
            target_id: other_id.clone(),
        };
        protocol
            .sign_and_send(&other_id, message, true)
            .await
            .unwrap();

        // Read the signed message from the channel
        let signed_msg = conn_to_us.inbound_rx.recv().await.unwrap();

        // Verify and decode
        let verified = signed_msg.verify(&peer_id).unwrap();
        let decoded_msg: Message = cbor_deserialize(&verified.payload).unwrap();

        // The decoded message should match
        assert!(matches!(decoded_msg, Message::RequestContactCard { .. }));
        assert_eq!(decoded_msg.sender_id(), &peer_id);
        assert_eq!(decoded_msg.target_id(), &other_id);

        // Contact card should be present since we set include_contact_card=true
        assert!(verified.contact_card.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verify_rejects_wrong_sender() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        let other = make_keyhive().await;
        let other_id = keyhive_peer_id(&other);

        let (conn_to_other, conn_to_us) = create_channel_pair(peer_id.clone(), &other_id);
        protocol.add_peer(other_id.clone(), conn_to_other).await;

        let message = Message::RequestContactCard {
            sender_id: peer_id.clone(),
            target_id: other_id.clone(),
        };
        protocol
            .sign_and_send(&other_id, message, false)
            .await
            .unwrap();

        let signed_msg = conn_to_us.inbound_rx.recv().await.unwrap();

        // Try to verify as if it came from the wrong sender
        let wrong_sender = keyhive_peer_id(&make_keyhive().await);
        let result = signed_msg.verify(&wrong_sender);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::error::VerificationError::SenderMismatch { .. }),
            "expected SenderMismatch, got: {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verify_rejects_tampered_message() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        let other = make_keyhive().await;
        let other_id = keyhive_peer_id(&other);

        let (conn_to_other, conn_to_us) = create_channel_pair(peer_id.clone(), &other_id);
        protocol.add_peer(other_id.clone(), conn_to_other).await;

        let message = Message::RequestContactCard {
            sender_id: peer_id.clone(),
            target_id: other_id.clone(),
        };
        protocol
            .sign_and_send(&other_id, message, false)
            .await
            .unwrap();

        let mut signed_msg = conn_to_us.inbound_rx.recv().await.unwrap();

        // Tamper with the signed bytes
        if let Some(byte) = signed_msg.signed_bytes_mut().last_mut() {
            *byte ^= 0xFF;
        }

        let result = signed_msg.verify(&peer_id);
        assert!(result.is_err(), "tampered message should fail verification");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn peer_management() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        let peer1 = keyhive_peer_id(&make_keyhive().await);
        let peer2 = keyhive_peer_id(&make_keyhive().await);

        let (conn1, _) = create_channel_pair(peer_id.clone(), &peer1);
        let (conn2, _) = create_channel_pair(peer_id.clone(), &peer2);

        assert!(protocol.peer_ids().await.is_empty());

        protocol.add_peer(peer1.clone(), conn1).await;
        protocol.add_peer(peer2.clone(), conn2).await;
        assert_eq!(protocol.peer_ids().await.len(), 2);

        protocol.remove_peer(&peer1).await;
        let remaining = protocol.peer_ids().await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0], peer2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_requests_contact_card_for_unknown_peer() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        // Create a peer that our keyhive doesn't know about
        let other = make_keyhive().await;
        let other_id = keyhive_peer_id(&other);

        let (conn_to_other, conn_to_us) = create_channel_pair(peer_id.clone(), &other_id);
        protocol.add_peer(other_id.clone(), conn_to_other).await;

        // Sync should request a contact card since we don't know the peer
        protocol.sync_keyhive(Some(&other_id)).await.unwrap();

        // Verify we got a RequestContactCard
        let signed_msg = conn_to_us.inbound_rx.recv().await.unwrap();
        let verified = signed_msg.verify(&peer_id).unwrap();
        let decoded: Message = cbor_deserialize(&verified.payload).unwrap();

        assert!(matches!(decoded, Message::RequestContactCard { .. }));
        // Should include our contact card so the peer can learn about us
        assert!(verified.contact_card.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contact_card_exchange_flow() {
        // Alice doesn't know Bob, and vice versa
        let (alice_proto, _alice_kh) = make_protocol().await;
        let alice_id = alice_proto.peer_id.clone();

        let (bob_proto, _bob_kh) = make_protocol().await;
        let bob_id = bob_proto.peer_id.clone();

        // Set up channels
        let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);
        alice_proto
            .add_peer(bob_id.clone(), alice_conn.clone())
            .await;
        bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

        // Alice tries to sync with Bob (she doesn't know him)
        alice_proto.sync_keyhive(Some(&bob_id)).await.unwrap();

        // Alice should have sent RequestContactCard
        let signed_msg1 = bob_conn.inbound_rx.recv().await.unwrap();

        let verified1 = signed_msg1.clone().verify(&alice_id).unwrap();
        let decoded1: Message = cbor_deserialize(&verified1.payload).unwrap();
        assert!(
            matches!(decoded1, Message::RequestContactCard { .. }),
            "alice should request bob's contact card"
        );

        // Bob handles alice's RequestContactCard
        bob_proto
            .handle_message(&alice_id, signed_msg1)
            .await
            .unwrap();

        // Bob should have sent MissingContactCard back
        let signed_msg2 = alice_conn.inbound_rx.recv().await.unwrap();

        let verified2 = signed_msg2.clone().verify(&bob_id).unwrap();
        let decoded2: Message = cbor_deserialize(&verified2.payload).unwrap();
        assert!(
            matches!(decoded2, Message::MissingContactCard { .. }),
            "bob should respond with MissingContactCard"
        );
        // Bob's response should include his contact card
        assert!(
            verified2.contact_card.is_some(),
            "bob's response should include contact card"
        );

        // Alice handles Bob's MissingContactCard (which includes Bob's contact card)
        // This should trigger Alice to initiate a sync
        alice_proto
            .handle_message(&bob_id, signed_msg2)
            .await
            .unwrap();

        // Alice should now have sent either a SyncRequest or another RequestContactCard
        // to Bob (depending on whether she successfully ingested Bob's CC)
        let signed_msg3 = bob_conn.inbound_rx.recv().await.unwrap();
        let verified3 = signed_msg3.verify(&alice_id).unwrap();
        let decoded3: Message = cbor_deserialize(&verified3.payload).unwrap();

        // After ingesting Bob's contact card, Alice should send a SyncRequest
        assert!(
            matches!(decoded3, Message::SyncRequest { .. }),
            "alice should now be able to send a sync request, got: {decoded3:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_request_response_flow_with_known_peers() {
        // Set up Alice and Bob who know each other
        let (alice_proto, alice_kh) = make_protocol().await;
        let alice_id = alice_proto.peer_id.clone();

        let (bob_proto, bob_kh) = make_protocol().await;
        let bob_id = bob_proto.peer_id.clone();

        // Exchange contact cards at the keyhive level
        let alice_cc = alice_kh.contact_card().await.unwrap();
        let bob_cc = bob_kh.contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        // Set up channels
        let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);
        alice_proto
            .add_peer(bob_id.clone(), alice_conn.clone())
            .await;
        bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

        // Alice initiates sync
        alice_proto.sync_keyhive(Some(&bob_id)).await.unwrap();

        // Read Alice's SyncRequest from the channel
        let signed_msg1 = bob_conn.inbound_rx.recv().await.unwrap();
        let verified1 = signed_msg1.clone().verify(&alice_id).unwrap();
        let decoded1: Message = cbor_deserialize(&verified1.payload).unwrap();

        assert!(
            matches!(decoded1, Message::SyncRequest { .. }),
            "alice should send SyncRequest when she knows bob"
        );

        // Bob handles the SyncRequest
        bob_proto
            .handle_message(&alice_id, signed_msg1)
            .await
            .unwrap();

        // Bob should respond with SyncResponse
        let signed_msg2 = alice_conn.inbound_rx.recv().await.unwrap();
        let verified2 = signed_msg2.clone().verify(&bob_id).unwrap();
        let decoded2: Message = cbor_deserialize(&verified2.payload).unwrap();

        assert!(
            matches!(decoded2, Message::SyncResponse { .. }),
            "bob should respond with SyncResponse, got: {decoded2:?}"
        );

        // Alice handles the SyncResponse
        alice_proto
            .handle_message(&bob_id, signed_msg2)
            .await
            .unwrap();

        // Depending on set differences, Alice might send SyncOps or nothing
        // At minimum, the protocol should complete without errors
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_bidirectional_sync() {
        // Create Alice and Bob, exchange contact cards, then sync both ways
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;

        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        // Exchange contact cards
        let alice_cc = alice_kh.contact_card().await.unwrap();
        let bob_cc = bob_kh.contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        let alice_cc_bytes = serialize_contact_card(&alice_cc);
        let bob_cc_bytes = serialize_contact_card(&bob_cc);

        let alice_storage = MemoryKeyhiveStorage::new();
        let bob_storage = MemoryKeyhiveStorage::new();

        let alice_shared = Arc::new(Mutex::new(alice_kh));
        let bob_shared = Arc::new(Mutex::new(bob_kh));

        let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);

        let alice_proto = TestProtocol::new(
            alice_shared.clone(),
            alice_storage.clone(),
            alice_id.clone(),
            alice_cc_bytes,
        );
        let bob_proto = TestProtocol::new(
            bob_shared.clone(),
            bob_storage.clone(),
            bob_id.clone(),
            bob_cc_bytes,
        );

        alice_proto
            .add_peer(bob_id.clone(), alice_conn.clone())
            .await;
        bob_proto.add_peer(alice_id.clone(), bob_conn.clone()).await;

        // Alice → Bob sync
        alice_proto.sync_keyhive(Some(&bob_id)).await.unwrap();

        // Forward SyncRequest to Bob
        let a_to_b_rx = bob_conn.inbound_rx.clone();
        let b_to_a_rx = alice_conn.inbound_rx.clone();

        let sync_request = a_to_b_rx.recv().await.unwrap();
        bob_proto
            .handle_message(&alice_id, sync_request)
            .await
            .unwrap();

        // Forward SyncResponse to Alice
        let sync_response = b_to_a_rx.recv().await.unwrap();
        alice_proto
            .handle_message(&bob_id, sync_response)
            .await
            .unwrap();

        // If Alice sent SyncOps, forward those too
        if let Ok(sync_ops) = a_to_b_rx.try_recv() {
            bob_proto.handle_message(&alice_id, sync_ops).await.unwrap();
        }

        // Now Bob → Alice sync
        bob_proto.sync_keyhive(Some(&alice_id)).await.unwrap();

        let sync_request2 = b_to_a_rx.recv().await.unwrap();
        alice_proto
            .handle_message(&bob_id, sync_request2)
            .await
            .unwrap();

        let sync_response2 = a_to_b_rx.recv().await.unwrap();
        bob_proto
            .handle_message(&alice_id, sync_response2)
            .await
            .unwrap();

        if let Ok(sync_ops2) = b_to_a_rx.try_recv() {
            alice_proto
                .handle_message(&bob_id, sync_ops2)
                .await
                .unwrap();
        }

        // After bidirectional sync, both should have the same pending state
        let alice_pending = {
            let kh = alice_shared.lock().await;
            kh.pending_event_hashes().await
        };
        let bob_pending = {
            let kh = bob_shared.lock().await;
            kh.pending_event_hashes().await
        };

        // Both should have the same pending hashes (possibly empty)
        assert_eq!(
            alice_pending, bob_pending,
            "after bidirectional sync, pending hashes should match"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_skips_self_peer() {
        let (protocol, _keyhive) = make_protocol().await;
        let peer_id = protocol.peer_id.clone();

        // Add ourselves as a peer (edge case)
        let (conn, _) = create_channel_pair(peer_id.clone(), &peer_id);
        protocol.add_peer(peer_id.clone(), conn).await;

        // Sync should skip ourselves
        protocol.sync_keyhive(None).await.unwrap();

        // No messages should have been sent (we skipped ourselves)
        // The protocol should complete without error
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_message_rejects_unsigned_junk() {
        let (protocol, _keyhive) = make_protocol().await;

        let fake_sender = keyhive_peer_id(&make_keyhive().await);

        // Create a SignedMessage with junk data
        let junk = SignedMessage::new(vec![0xFF, 0xFE, 0xFD]);

        let result = protocol.handle_message(&fake_sender, junk).await;
        assert!(result.is_err(), "junk data should fail verification");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_via_protocol() {
        let (protocol, keyhive) = make_protocol().await;
        let storage_id = crate::storage::StorageHash::new([1u8; 32]);

        // Save an archive to storage via storage_ops
        let archive = keyhive.into_archive().await;
        crate::storage_ops::save_keyhive_archive::<_, _, Local>(
            &MemoryKeyhiveStorage::new(),
            storage_id,
            &archive,
        )
        .await
        .unwrap();

        // Compact via protocol should work
        let result = protocol.compact(storage_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingest_from_storage_via_protocol() {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.unwrap();
        let cc_bytes = serialize_contact_card(&cc);

        let storage = MemoryKeyhiveStorage::new();

        // Save an archive
        let archive = keyhive.into_archive().await;
        let storage_id = crate::storage::StorageHash::new([1u8; 32]);
        crate::storage_ops::save_keyhive_archive::<_, _, Local>(&storage, storage_id, &archive)
            .await
            .unwrap();

        let shared = Arc::new(Mutex::new(keyhive));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc_bytes);

        // Ingest from storage should work
        let result = protocol.ingest_from_storage().await;
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------
    // Integration tests: divergent ops sync convergence
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "current_thread")]
    async fn sync_group_membership_converges() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
        } = exchange_contact_cards_and_setup().await;

        // Alice creates a group and adds Bob as a Read member
        let (group_id, bob_individual_id) = {
            let kh = alice_kh.lock().await;
            let group = kh.generate_group(vec![]).await.unwrap();
            let group_id = group.lock().await.group_id();

            // Get Bob's Individual on Alice's keyhive
            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = kh.get_agent(bob_identifier).await.unwrap();

            kh.add_member(
                bob_agent,
                &Membered::Group(group_id, group.clone()),
                Access::Read,
                &[],
            )
            .await
            .unwrap();

            let bob_identifier: Identifier = bob_identifier;
            (group_id, bob_identifier)
        };

        // Before sync: Bob should not have the group
        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Bob should not have the group before sync"
            );
        }

        // Sync Alice → Bob
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // After sync: Bob should have the group and his membership
        {
            let kh = bob_kh.lock().await;
            let group = kh.get_group(group_id).await;
            assert!(group.is_some(), "Bob should have the group after sync");

            let group_ref = group.unwrap();
            let members = kh
                .reachable_members(Membered::Group(group_id, group_ref))
                .await;
            assert!(
                members.contains_key(&bob_individual_id),
                "Bob should be a member of the group"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_document_access_converges() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
        } = exchange_contact_cards_and_setup().await;

        // Alice creates group, adds Bob, creates doc owned by group
        let doc_id = {
            let kh = alice_kh.lock().await;
            let group = kh.generate_group(vec![]).await.unwrap();
            let group_id = group.lock().await.group_id();

            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = kh.get_agent(bob_identifier).await.unwrap();

            kh.add_member(
                bob_agent,
                &Membered::Group(group_id, group.clone()),
                Access::Read,
                &[],
            )
            .await
            .unwrap();

            let doc = kh
                .generate_doc(
                    vec![Peer::Group(group_id, group.clone())],
                    nonempty![[0u8; 32]],
                )
                .await
                .unwrap();
            doc.lock().await.doc_id()
        };

        // Before sync: Bob should not have the document
        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_document(doc_id).await.is_none(),
                "Bob should not have the document before sync"
            );
        }

        // Sync Alice → Bob
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // After sync: Bob should have the document and it should be reachable
        {
            let kh = bob_kh.lock().await;
            let doc = kh.get_document(doc_id).await;
            assert!(doc.is_some(), "Bob should have the document after sync");

            let reachable = kh.reachable_docs().await;
            assert!(
                reachable.contains_key(&doc_id),
                "Bob's reachable docs should include the synced document"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bidirectional_sync_with_divergent_ops_converges() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
        } = exchange_contact_cards_and_setup().await;

        // Alice creates her group and adds Bob
        let alice_group_id = {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await
        };

        // Bob creates his group and adds Alice
        let bob_group_id = {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await
        };

        // Before sync: each peer only has their own group
        {
            let alice = alice_kh.lock().await;
            assert!(alice.get_group(alice_group_id).await.is_some());
            assert!(
                alice.get_group(bob_group_id).await.is_none(),
                "Alice should not have Bob's group before sync"
            );
        }
        {
            let bob = bob_kh.lock().await;
            assert!(bob.get_group(bob_group_id).await.is_some());
            assert!(
                bob.get_group(alice_group_id).await.is_none(),
                "Bob should not have Alice's group before sync"
            );
        }

        // Bidirectional sync: Alice → Bob, then Bob → Alice
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;
        run_sync_round(
            &bob_proto,
            &alice_proto,
            &bob_id,
            &alice_id,
            &bob_conn,
            &alice_conn,
        )
        .await;

        // Verify both keyhives have both groups
        {
            let alice = alice_kh.lock().await;
            assert!(alice.get_group(alice_group_id).await.is_some());
            assert!(alice.get_group(bob_group_id).await.is_some());
        }
        {
            let bob = bob_kh.lock().await;
            assert!(bob.get_group(alice_group_id).await.is_some());
            assert!(bob.get_group(bob_group_id).await.is_some());
        }

        // Verify membership op counts match
        {
            let alice = alice_kh.lock().await;
            let bob = bob_kh.lock().await;

            let alice_self = alice
                .get_agent(alice_id.to_identifier().unwrap())
                .await
                .unwrap();
            let bob_self = bob
                .get_agent(bob_id.to_identifier().unwrap())
                .await
                .unwrap();

            let alice_ops = alice.membership_ops_for_agent(&alice_self).await;
            let bob_ops = bob.membership_ops_for_agent(&bob_self).await;
            assert_eq!(
                alice_ops.len(),
                bob_ops.len(),
                "membership op counts should match after bidirectional sync"
            );
        }

        // Verify pending state is empty
        {
            let alice = alice_kh.lock().await;
            let bob = bob_kh.lock().await;
            assert!(alice.pending_event_hashes().await.is_empty());
            assert!(bob.pending_event_hashes().await.is_empty());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_revocation_propagates() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
        } = exchange_contact_cards_and_setup().await;

        // Alice creates group and adds Bob
        let group_id = {
            let kh = alice_kh.lock().await;
            let group = kh.generate_group(vec![]).await.unwrap();
            let group_id = group.lock().await.group_id();

            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = kh.get_agent(bob_identifier).await.unwrap();

            kh.add_member(
                bob_agent,
                &Membered::Group(group_id, group.clone()),
                Access::Read,
                &[],
            )
            .await
            .unwrap();

            group_id
        };

        // Before first sync: Bob should not have the group
        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Bob should not have the group before sync"
            );
        }

        // First sync: Alice → Bob — Bob gets the group
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Bob should now have the group, but no revocations yet
        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_some(),
                "Bob should have the group after first sync"
            );

            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = kh.get_agent(bob_identifier).await.unwrap();
            let group = kh.get_group(group_id).await.unwrap();
            let revocations = Membered::Group(group_id, group)
                .get_agent_revocations(&bob_agent)
                .await;
            assert!(
                revocations.is_empty(),
                "Bob should have no revocations before Alice revokes"
            );
        }

        // Alice revokes Bob from the group
        {
            let kh = alice_kh.lock().await;
            let group = kh.get_group(group_id).await.unwrap();
            let bob_identifier = bob_id.to_identifier().unwrap();

            kh.revoke_member(bob_identifier, true, &Membered::Group(group_id, group))
                .await
                .unwrap();
        };

        // Second sync: Alice → Bob — Bob receives the revocation
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Verify Bob's keyhive has the revocation
        {
            let bob = bob_kh.lock().await;
            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = bob.get_agent(bob_identifier).await.unwrap();

            let group = bob.get_group(group_id).await.unwrap();
            let revocations = Membered::Group(group_id, group)
                .get_agent_revocations(&bob_agent)
                .await;

            assert!(
                !revocations.is_empty(),
                "Bob should have received the revocation"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn three_peer_transitive_sync() {
        let alice_keyhive = make_keyhive().await;
        let bob_keyhive = make_keyhive().await;
        let carol_keyhive = make_keyhive().await;

        exchange_all_contact_cards(&[&alice_keyhive, &bob_keyhive, &carol_keyhive]).await;

        let alice_id = keyhive_peer_id(&alice_keyhive);
        let bob_id = keyhive_peer_id(&bob_keyhive);
        let carol_id = keyhive_peer_id(&carol_keyhive);

        let (alice_proto, alice_kh, _) = make_protocol_with_shared_keyhive(alice_keyhive).await;
        let (bob_proto, bob_kh, _) = make_protocol_with_shared_keyhive(bob_keyhive).await;
        let (carol_proto, carol_kh, _) = make_protocol_with_shared_keyhive(carol_keyhive).await;

        let (ab_conn_a, ab_conn_b) = create_channel_pair(alice_id.clone(), &bob_id);
        let (bc_conn_b, bc_conn_c) = create_channel_pair(bob_id.clone(), &carol_id);

        alice_proto
            .add_peer(bob_id.clone(), ab_conn_a.clone())
            .await;
        bob_proto
            .add_peer(alice_id.clone(), ab_conn_b.clone())
            .await;
        bob_proto
            .add_peer(carol_id.clone(), bc_conn_b.clone())
            .await;
        carol_proto
            .add_peer(bob_id.clone(), bc_conn_c.clone())
            .await;

        // Alice creates a group and adds Bob and Carol
        let group_id = {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id, &carol_id]).await
        };

        // Before any sync: neither Bob nor Carol has the group
        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Bob should not have the group before sync"
            );
        }
        {
            let kh = carol_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Carol should not have the group before sync"
            );
        }

        // Alice syncs to Bob only
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &ab_conn_a,
            &ab_conn_b,
        )
        .await;

        {
            let kh = bob_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_some(),
                "Bob should have the group after Alice→Bob sync"
            );
        }
        // Carol still shouldn't have it
        {
            let kh = carol_kh.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Carol should not have the group before Bob→Carol sync"
            );
        }

        // Bob syncs to Carol
        run_sync_round(
            &bob_proto,
            &carol_proto,
            &bob_id,
            &carol_id,
            &bc_conn_b,
            &bc_conn_c,
        )
        .await;

        // Verify Carol got the group and her membership
        {
            let kh = carol_kh.lock().await;
            let group = kh.get_group(group_id).await;
            assert!(
                group.is_some(),
                "Carol should have the group after transitive sync"
            );

            let carol_identifier = carol_id.to_identifier().unwrap();
            let members = kh
                .reachable_members(Membered::Group(group_id, group.unwrap()))
                .await;
            assert!(
                members.contains_key(&carol_identifier),
                "Carol should be a member of the group"
            );
        }
    }
}
