//! Keyhive sync protocol integration.
//!
//! This module integrates keyhive synchronization into the Subduction protocol,
//! reusing Subduction's existing connection infrastructure to exchange keyhive
//! operations between peers.
//!
//! The sync protocol follows a three-phase exchange:
//!
//! 1. **Sync Request**: The initiator sends hashes of operations accessible to
//!    both peers, plus pending operation hashes.
//! 2. **Sync Response**: The responder computes set differences and sends back
//!    operations the initiator is missing, along with hashes it wants.
//! 3. **Sync Ops**: The initiator sends the requested operations.

use alloc::{collections::BTreeSet, string::ToString, vec, vec::Vec};

use keyhive_core::{
    content::reference::ContentRef,
    crypto::{digest::Digest, signed::Signed, signer::async_signer::AsyncSigner},
    event::{Event, static_event::StaticEvent},
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::agent::Agent,
    store::ciphertext::CiphertextStore,
};
use rand::{CryptoRng, RngCore};
use sedimentree_core::{collections::Map, depth::DepthMetric};
use subduction_keyhive::{
    KeyhivePeerId, KeyhiveStorage, SignedMessage as KeyhiveSignedMessage, StorageError,
    message::{EventBytes, EventHash, Message as KeyhiveMessage},
    storage_ops,
};

use super::{Subduction, SubductionFutureForm};
use crate::{
    connection::{Connection, message::Message},
    crypto::signer::Signer,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    subduction::error::KeyhiveSyncError,
};

type Set<T> = BTreeSet<T>;

/// Convert Subduction `PeerId` to Keyhive `KeyhivePeerId`.
fn to_keyhive_peer_id(peer_id: &PeerId) -> KeyhivePeerId {
    KeyhivePeerId::from_bytes(*peer_id.as_bytes())
}

/// Convert a keyhive `Digest` to an `EventHash` (32-byte array).
fn digest_to_bytes<T: serde::Serialize>(digest: &Digest<T>) -> EventHash {
    let bytes = digest.as_slice();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes[..32]);
    arr
}

/// Serialize a value to CBOR bytes using minicbor-serde (for keyhive_core types).
fn cbor_serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, StorageError> {
    minicbor_serde::to_vec(value).map_err(|e| StorageError::Serialization(e.to_string()))
}

/// Deserialize a value from CBOR bytes using minicbor-serde (for keyhive_core types).
fn cbor_deserialize<V: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<V, StorageError> {
    minicbor_serde::from_slice(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

/// Encode a keyhive protocol [`KeyhiveMessage`] to bytes using minicbor.
fn encode_message(msg: &KeyhiveMessage) -> Result<Vec<u8>, StorageError> {
    minicbor::to_vec(msg).map_err(|e| StorageError::Serialization(e.to_string()))
}

/// Decode a keyhive protocol [`KeyhiveMessage`] from bytes using minicbor.
fn decode_message(bytes: &[u8]) -> Result<KeyhiveMessage, StorageError> {
    minicbor::decode(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

impl<
    'a,
    F: SubductionFutureForm<
            'a,
            S,
            C,
            P,
            Sig,
            M,
            N,
            KContentRef,
            KPayload,
            KCiphertextStore,
            KListener,
            KRng,
            KStore,
        > + 'static,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F> + AsyncSigner + Clone,
    M: DepthMetric,
    const N: usize,
    KContentRef: ContentRef + serde::de::DeserializeOwned,
    KPayload: for<'de> serde::Deserialize<'de>,
    KCiphertextStore: CiphertextStore<KContentRef, KPayload> + Clone,
    KListener: MembershipListener<Sig, KContentRef>,
    KRng: CryptoRng + RngCore,
    KStore: KeyhiveStorage<F>,
>
    Subduction<
        'a,
        F,
        S,
        C,
        P,
        Sig,
        M,
        N,
        KContentRef,
        KPayload,
        KCiphertextStore,
        KListener,
        KRng,
        KStore,
    >
{
    /// Initiate a keyhive sync with connected peers.
    ///
    /// If `target` is `Some`, only that peer is synced. Otherwise all connected
    /// peers are synced.
    ///
    /// # Errors
    ///
    /// Returns [`KeyhiveSyncError`] if peer ID conversion, message signing, or
    /// sending fails.
    pub async fn sync_keyhive(
        &self,
        target: Option<&PeerId>,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let peer_ids: Vec<PeerId> = match target {
            Some(t) => vec![*t],
            None => self.connected_peer_ids().await.into_iter().collect(),
        };

        for peer_id in &peer_ids {
            let target_keyhive_id = to_keyhive_peer_id(peer_id);

            if target_keyhive_id.same_identity(&self.keyhive_peer_id) {
                continue;
            }

            match self.get_hashes_for_peer_pair(&target_keyhive_id).await {
                Ok(Some(hashes)) => {
                    let found: Vec<EventHash> = hashes.keys().map(digest_to_bytes).collect();
                    let pending = self.get_pending_hashes().await?;

                    let message = KeyhiveMessage::SyncRequest {
                        sender_id: self.keyhive_peer_id.clone(),
                        target_id: target_keyhive_id.clone(),
                        found,
                        pending,
                    };

                    tracing::debug!(
                        target = %target_keyhive_id,
                        "sending keyhive sync request"
                    );

                    self.sign_and_send_keyhive(peer_id, message, false).await?;
                }
                Ok(None) => {
                    // We don't have an agent for this peer. Request their
                    // contact card so we can sync next time.
                    tracing::debug!(
                        target = %target_keyhive_id,
                        "requesting contact card"
                    );

                    let message = KeyhiveMessage::RequestContactCard {
                        sender_id: self.keyhive_peer_id.clone(),
                        target_id: target_keyhive_id,
                    };

                    self.sign_and_send_keyhive(peer_id, message, true).await?;
                }
                Err(e) => {
                    tracing::warn!(
                        target = %target_keyhive_id,
                        error = %e,
                        "failed to get hashes for peer pair, skipping"
                    );
                }
            }
        }

        Ok(())
    }

    /// Handle an incoming keyhive signed message from a peer.
    ///
    /// Verifies the message signature, optionally ingests a contact card,
    /// and dispatches to the appropriate handler based on message type.
    ///
    /// # Errors
    ///
    /// Returns [`KeyhiveSyncError`] if signature verification, contact card
    /// ingestion, or message handling fails.
    pub async fn handle_keyhive_message(
        &self,
        from: &PeerId,
        signed_msg: KeyhiveSignedMessage,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let from_keyhive_id = to_keyhive_peer_id(from);
        let verified = signed_msg
            .verify(&from_keyhive_id)
            .map_err(KeyhiveSyncError::Verification)?;

        if let Some(cc_bytes) = &verified.contact_card {
            self.ingest_contact_card(cc_bytes).await?;
        }

        let message: KeyhiveMessage = decode_message(&verified.payload)
            .map_err(|e| KeyhiveSyncError::Deserialization(e.to_string()))?;

        match &message {
            KeyhiveMessage::SyncRequest { .. } => self.handle_sync_request(from, message).await,
            KeyhiveMessage::SyncResponse { .. } => self.handle_sync_response(from, message).await,
            KeyhiveMessage::SyncOps { .. } => self.handle_sync_ops(message).await,
            KeyhiveMessage::RequestContactCard { .. } => {
                self.handle_request_contact_card(from, message).await
            }
            KeyhiveMessage::MissingContactCard { .. } => {
                self.handle_missing_contact_card(from, message).await
            }
        }
    }

    /// Handle a `SyncRequest`: compute which ops to send and which to request.
    async fn handle_sync_request(
        &self,
        from: &PeerId,
        message: KeyhiveMessage,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let KeyhiveMessage::SyncRequest {
            sender_id,
            found: peer_found,
            pending: peer_pending,
            ..
        } = message
        else {
            return Err(KeyhiveSyncError::UnexpectedMessageType {
                expected: "SyncRequest",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            found = peer_found.len(),
            pending = peer_pending.len(),
            "handling keyhive sync request"
        );

        let Some(local_hashes) = self.get_hashes_for_peer_pair(&sender_id).await? else {
            // We don't know this peer — request their contact card.
            tracing::debug!(from = %sender_id, "no agent found, requesting contact card");
            let msg = KeyhiveMessage::RequestContactCard {
                sender_id: self.keyhive_peer_id.clone(),
                target_id: sender_id,
            };
            return self.sign_and_send_keyhive(from, msg, true).await;
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
                let bytes = cbor_serialize(event)
                    .map_err(|e| KeyhiveSyncError::Serialization(e.to_string()))?;
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
            "sending keyhive sync response"
        );

        let response = KeyhiveMessage::SyncResponse {
            sender_id: self.keyhive_peer_id.clone(),
            target_id: sender_id,
            requested,
            found: found_ops,
        };

        self.sign_and_send_keyhive(from, response, false).await
    }

    /// Handle a `SyncResponse`: ingest events we received and send any
    /// requested ops back.
    async fn handle_sync_response(
        &self,
        from: &PeerId,
        message: KeyhiveMessage,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let KeyhiveMessage::SyncResponse {
            sender_id,
            requested: requested_hashes,
            found: found_events,
            ..
        } = message
        else {
            return Err(KeyhiveSyncError::UnexpectedMessageType {
                expected: "SyncResponse",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            received = found_events.len(),
            requested = requested_hashes.len(),
            "handling keyhive sync response"
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
                    "sending requested keyhive ops"
                );

                let msg = KeyhiveMessage::SyncOps {
                    sender_id: self.keyhive_peer_id.clone(),
                    target_id: sender_id,
                    ops,
                };

                self.sign_and_send_keyhive(from, msg, false).await?;
            }
        }

        Ok(())
    }

    /// Handle `SyncOps`: ingest received operations.
    async fn handle_sync_ops(&self, message: KeyhiveMessage) -> Result<(), KeyhiveSyncError<F, C>> {
        let KeyhiveMessage::SyncOps { sender_id, ops, .. } = message else {
            return Err(KeyhiveSyncError::UnexpectedMessageType {
                expected: "SyncOps",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            count = ops.len(),
            "handling keyhive sync ops"
        );

        if !ops.is_empty() {
            self.ingest_events(&ops).await?;
        }

        Ok(())
    }

    /// Handle `RequestContactCard`: send our contact card to the requesting peer.
    async fn handle_request_contact_card(
        &self,
        from: &PeerId,
        message: KeyhiveMessage,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let KeyhiveMessage::RequestContactCard { sender_id, .. } = message else {
            return Err(KeyhiveSyncError::UnexpectedMessageType {
                expected: "RequestContactCard",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            "sending missing contact card"
        );

        let msg = KeyhiveMessage::MissingContactCard {
            sender_id: self.keyhive_peer_id.clone(),
            target_id: sender_id,
        };

        self.sign_and_send_keyhive(from, msg, true).await
    }

    /// Handle `MissingContactCard`: the contact card was already ingested in
    /// `handle_keyhive_message`, so now we trigger a sync with the peer.
    async fn handle_missing_contact_card(
        &self,
        from: &PeerId,
        message: KeyhiveMessage,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let KeyhiveMessage::MissingContactCard { sender_id, .. } = message else {
            return Err(KeyhiveSyncError::UnexpectedMessageType {
                expected: "MissingContactCard",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            "received contact card, initiating keyhive sync"
        );

        self.sync_keyhive(Some(from)).await
    }

    /// Sign a keyhive message and send it to a peer.
    async fn sign_and_send_keyhive(
        &self,
        target_peer_id: &PeerId,
        message: KeyhiveMessage,
        include_contact_card: bool,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let msg_bytes =
            encode_message(&message).map_err(|e| KeyhiveSyncError::Serialization(e.to_string()))?;

        let signed: Signed<Vec<u8>> = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .try_sign(msg_bytes)
                .await
                .map_err(KeyhiveSyncError::Signing)?
        };

        let signed_bytes =
            cbor_serialize(&signed).map_err(|e| KeyhiveSyncError::Serialization(e.to_string()))?;

        let signed_message = if include_contact_card {
            KeyhiveSignedMessage::with_contact_card(
                signed_bytes,
                self.keyhive_contact_card_bytes.clone(),
            )
        } else {
            KeyhiveSignedMessage::new(signed_bytes)
        };

        let conn = self
            .get_connection(target_peer_id)
            .await
            .ok_or_else(|| KeyhiveSyncError::PeerNotConnected(*target_peer_id))?;

        conn.send(&Message::Keyhive(signed_message))
            .await
            .map_err(KeyhiveSyncError::Send)
    }

    /// Get the intersection of event hashes accessible to both us and a peer.
    ///
    /// Returns `None` if the peer is unknown (no agent found in keyhive).
    async fn get_hashes_for_peer_pair(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<
        Option<Map<Digest<StaticEvent<KContentRef>>, StaticEvent<KContentRef>>>,
        KeyhiveSyncError<F, C>,
    > {
        let our_id = self
            .keyhive_peer_id
            .to_identifier()
            .map_err(KeyhiveSyncError::InvalidIdentifier)?;
        let their_id = peer_id
            .to_identifier()
            .map_err(KeyhiveSyncError::InvalidIdentifier)?;

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
    async fn get_pending_hashes(&self) -> Result<Vec<EventHash>, KeyhiveSyncError<F, C>> {
        let keyhive = self.keyhive.lock().await;
        let digests = keyhive.pending_event_hashes().await;
        Ok(digests.into_iter().map(|d| digest_to_bytes(&d)).collect())
    }

    /// Fetch serialized event bytes for the given hashes.
    async fn get_event_bytes_for_hashes(
        &self,
        hashes: &[EventHash],
    ) -> Result<Vec<EventBytes>, KeyhiveSyncError<F, C>> {
        let requested_set: Set<EventHash> = hashes.iter().copied().collect();

        let peer_id = self
            .keyhive_peer_id
            .to_identifier()
            .map_err(KeyhiveSyncError::InvalidIdentifier)?;

        let keyhive = self.keyhive.lock().await;

        let Some(agent) = keyhive.get_agent(peer_id).await else {
            return Ok(Vec::new());
        };

        let events = sync_events_for_agent(&keyhive, &agent).await;

        let mut result = Vec::new();
        for (digest, event) in &events {
            let h = digest_to_bytes(digest);
            if requested_set.contains(&h) {
                let bytes = cbor_serialize(event)
                    .map_err(|e| KeyhiveSyncError::Serialization(e.to_string()))?;
                result.push(bytes);
            }
        }

        Ok(result)
    }

    /// Ingest received event bytes into keyhive and persist them to storage.
    async fn ingest_events(
        &self,
        event_bytes_list: &[EventBytes],
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let events: Vec<StaticEvent<KContentRef>> = event_bytes_list
            .iter()
            .map(|bytes| cbor_deserialize(bytes))
            .collect::<Result<_, _>>()
            .map_err(|e| KeyhiveSyncError::Deserialization(e.to_string()))?;

        let pending = {
            let keyhive = self.keyhive.lock().await;
            keyhive.ingest_unsorted_static_events(events).await
        };

        if !pending.is_empty() {
            tracing::warn!(
                count = pending.len(),
                "some keyhive events pending after ingestion, attempting storage recovery"
            );

            if let Err(e) = self.try_storage_recovery(event_bytes_list).await {
                tracing::warn!(
                    error = %e,
                    "keyhive storage recovery failed"
                );
            }
        }

        for event_bytes in event_bytes_list {
            if let Err(e) =
                storage_ops::save_event_bytes(&self.keyhive_storage, event_bytes.clone()).await
            {
                tracing::warn!(error = %e, "failed to save received keyhive event to storage");
            }
        }

        Ok(())
    }

    /// Attempt to recover by ingesting from storage and retrying.
    async fn try_storage_recovery(
        &self,
        event_bytes_list: &[EventBytes],
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        {
            let keyhive = self.keyhive.lock().await;
            storage_ops::ingest_from_storage(&keyhive, &self.keyhive_storage)
                .await
                .map_err(KeyhiveSyncError::Storage)?;
        }

        let events: Vec<StaticEvent<KContentRef>> = event_bytes_list
            .iter()
            .filter_map(|bytes| cbor_deserialize(bytes).ok())
            .collect();

        if !events.is_empty() {
            let keyhive = self.keyhive.lock().await;
            let retry_pending = keyhive.ingest_unsorted_static_events(events).await;
            if retry_pending.is_empty() {
                tracing::debug!("all keyhive events ingested after storage recovery");
            } else {
                tracing::warn!(
                    count = retry_pending.len(),
                    "keyhive events still pending after storage recovery"
                );
            }
        }

        Ok(())
    }

    /// Deserialize and ingest a contact card into keyhive.
    async fn ingest_contact_card(&self, cc_bytes: &[u8]) -> Result<(), KeyhiveSyncError<F, C>> {
        let contact_card = cbor_deserialize(cc_bytes)
            .map_err(|e| KeyhiveSyncError::Deserialization(e.to_string()))?;

        let keyhive = self.keyhive.lock().await;
        keyhive
            .receive_contact_card(&contact_card)
            .await
            .map_err(KeyhiveSyncError::ReceiveContactCard)?;

        tracing::debug!("ingested keyhive contact card");
        Ok(())
    }

    /// Compact keyhive storage.
    ///
    /// Consolidates archives and removes processed events. This should be
    /// called periodically.
    ///
    /// # Errors
    ///
    /// Returns [`KeyhiveSyncError`] if any storage operation fails.
    pub async fn compact_keyhive(
        &self,
        storage_id: subduction_keyhive::storage::StorageHash,
    ) -> Result<(), KeyhiveSyncError<F, C>> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::compact(&keyhive, &self.keyhive_storage, storage_id)
            .await
            .map_err(KeyhiveSyncError::Storage)
    }

    /// Load and ingest all stored keyhive archives and events.
    ///
    /// # Errors
    ///
    /// Returns [`KeyhiveSyncError`] if loading or ingestion fails.
    pub async fn ingest_keyhive_from_storage(&self) -> Result<(), KeyhiveSyncError<F, C>> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::ingest_from_storage(&keyhive, &self.keyhive_storage)
            .await
            .map_err(KeyhiveSyncError::Storage)?;
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
