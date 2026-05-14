//! Keyhive sync protocol handler.
//!
//! This module implements the keyhive synchronization protocol, which enables
//! peers to reconcile their keyhive operations (delegations, revocations,
//! prekey operations, CGKA operations). T
//!
//! ## Full sync exchange
//!
//! 1. **Sync Request**: The initiator sends hashes of operations accessible to
//!    both peers, plus any pending operation hashes.
//! 2. **Sync Response**: The responder computes set differences and sends back
//!    operations the initiator is missing, along with hashes it wants from the
//!    initiator. Includes metadata totals for both sides.
//! 3. **Sync Ops**: The initiator sends the requested operations (if any).
//! 4. **Sync Confirmation**: Whichever side finishes last sends a confirmation
//!    with its total, establishing a syncpoint for future lightweight checks.
//!
//! ## Lightweight sync check
//!
//! When we have an established syncpoint for a peer, we can send
//! a **Sync Check** instead of a full request. The check carries the sender's
//! total and its syncpoint for the target. If both sides' totals match their
//! respective syncpoints, no full sync is needed. Otherwise the protocol falls
//! back to a full sync request.
//!
//! ## Contact card exchange
//!
//! When peers don't yet know each other's identity, the protocol handles
//! requesting and sending contact cards before initiating sync.

use alloc::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    string::ToString,
    sync::Arc,
    vec,
    vec::Vec,
};

use async_lock::Mutex;
use keyhive_core::{
    contact_card::ContactCard,
    event::{Event, static_event::StaticEvent},
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::{
        agent::Agent, group::membership_operation::MembershipOperation, identifier::Identifier,
        individual::op::KeyOp, public::Public,
    },
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{
    content::reference::ContentRef, digest::Digest, signed::Signed,
    signer::async_signer::AsyncSigner,
};
use std::collections::{HashMap, HashSet};

use crate::{
    cache::PeriodicEventCache,
    collections::{Map, Set},
    connection::KeyhiveConnection,
    error::{ProtocolError, SigningError, StorageError, VerificationError},
    message::{AgentHashMap, CborBytes, EventBytes, EventHash, Message},
    peer_id::KeyhivePeerId,
    signed_message::SignedMessage,
    storage::{KeyhiveStorage, StorageHash},
    storage_ops,
    storage_ops::{bincode_deserialize, bincode_serialize},
    syncpoints::SyncpointMap,
};

/// Shared keyhive instance behind a mutex.
type SharedKeyhive<Async, Signer, T, P, C, L, R> = Arc<Mutex<Keyhive<Async, Signer, T, P, C, L, R>>>;

/// Keyhive sync protocol handler.
///
/// Manages peer connections and implements the keyhive sync protocol for
/// reconciling operations between peers. All keyhive access is serialized
/// through an `Arc<Mutex<Keyhive>>`.
pub struct KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, Async>
where
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<Async>,
    Store: KeyhiveStorage<Async>,
    Async: future_form::FutureForm,
{
    keyhive: SharedKeyhive<Async, Signer, T, P, C, L, R>,
    storage: Store,
    peer_id: KeyhivePeerId,
    peers: Mutex<Map<KeyhivePeerId, Conn>>,
    contact_card: ContactCard,
    archive_config: Option<(usize, StorageHash)>,
    syncpoints: Mutex<SyncpointMap>,
    cache: Mutex<PeriodicEventCache>,
    _marker: core::marker::PhantomData<Async>,
}

impl<Signer, T, P, C, L, R, Conn, Store, Async> core::fmt::Debug
    for KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, Async>
where
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<Async>,
    Store: KeyhiveStorage<Async>,
    Async: future_form::FutureForm,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveProtocol")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl<Signer, T, P, C, L, R, Conn, Store, Async>
    KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, Async>
where
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<Async>,
    Conn::SendError: 'static,
    Conn::DisconnectError: 'static,
    Store: KeyhiveStorage<Async>,
    Async: future_form::FutureForm,
{
    /// Create a new protocol handler.
    pub fn new(
        keyhive: SharedKeyhive<Async, Signer, T, P, C, L, R>,
        storage: Store,
        peer_id: KeyhivePeerId,
        contact_card: ContactCard,
    ) -> Self {
        Self {
            keyhive,
            storage,
            peer_id,
            peers: Mutex::new(Map::new()),
            contact_card,
            archive_config: None,
            syncpoints: Mutex::new(SyncpointMap::new()),
            cache: Mutex::new(PeriodicEventCache::new()),
            _marker: core::marker::PhantomData,
        }
    }

    /// Write a single archive instead of N event files when ingesting more
    /// than `threshold` events.
    #[must_use]
    pub const fn with_archive_threshold(
        mut self,
        threshold: usize,
        storage_id: StorageHash,
    ) -> Self {
        self.archive_config = Some((threshold, storage_id));
        self
    }

    /// Register a peer connection.
    pub async fn add_peer(&self, peer_id: KeyhivePeerId, conn: Conn) {
        self.peers.lock().await.insert(peer_id, conn);
    }

    /// Unregister a peer connection and drop its syncpoint.
    pub async fn remove_peer(&self, peer_id: &KeyhivePeerId) {
        self.syncpoints.lock().await.remove(peer_id);
        self.peers.lock().await.remove(peer_id);
    }

    /// Connected peer IDs.
    pub async fn peer_ids(&self) -> Vec<KeyhivePeerId> {
        self.peers.lock().await.keys().cloned().collect()
    }

    /// The local peer's keyhive ID.
    #[must_use]
    pub fn peer_id(&self) -> KeyhivePeerId {
        self.peer_id.clone()
    }

    /// Whether the provided peer is registered.
    pub async fn has_peer(&self, peer_id: &KeyhivePeerId) -> bool {
        self.peers.lock().await.contains_key(peer_id)
    }

    /// Sum of all keyhive op counters.
    pub async fn total_ops(&self) -> u64 {
        let stats = self.keyhive.lock().await.stats().await;
        stats.delegations
            + stats.revocations
            + stats.prekeys_expanded
            + stats.prekey_rotations
            + stats.cgka_operations
    }

    /// Serialized events reachable by `peer_id`.
    ///
    /// # Errors
    /// Returns [`ProtocolError`] on serialization failure.
    pub async fn get_events_for_agent(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<Option<BTreeMap<EventHash, (EventBytes, CborBytes)>>, ProtocolError<Conn::SendError>>
    {
        let id = peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;
        let events = {
            let keyhive = self.keyhive.lock().await;
            let Some(agent) = keyhive.get_agent(id).await else {
                return Ok(None);
            };
            sync_events_for_agent(&keyhive, &agent).await
        };

        collect_serialized_events(events).map(Some)
    }

    /// Serialized events reachable by the public agent.
    ///
    /// # Errors
    /// Returns [`ProtocolError`] on serialization failure.
    pub async fn get_events_for_public_agent(
        &self,
    ) -> Result<BTreeMap<EventHash, (EventBytes, CborBytes)>, ProtocolError<Conn::SendError>> {
        let events = {
            let keyhive = self.keyhive.lock().await;
            let Some(agent) = keyhive.get_agent(Public.id()).await else {
                return Ok(BTreeMap::new());
            };
            sync_events_for_agent(&keyhive, &agent).await
        };

        collect_serialized_events(events)
    }

    /// Bulk snapshot of every agent's reachable events, deduplicated.
    ///
    /// Hashes in `skip_serialization` are still recorded in the per-agent
    /// hash sets but their bytes are not serialized (the caller already
    /// has them cached).
    ///
    /// # Errors
    /// Returns [`ProtocolError`] on serialization failure.
    #[allow(clippy::too_many_lines)]
    pub async fn all_agent_events(
        &self,
        skip_serialization: &BTreeSet<EventHash>,
    ) -> Result<crate::all_agent_events::AllAgentEvents, ProtocolError<Conn::SendError>> {
        let (all_membership, all_prekey, all_cgka) = {
            let keyhive = self.keyhive.lock().await;
            let all_membership = keyhive.membership_ops_for_all_agents().await;
            let all_prekey = keyhive.reachable_prekey_ops_for_all_agents().await;
            let all_cgka = keyhive.cgka_ops_for_all_agents().await;
            (all_membership, all_prekey, all_cgka)
        };

        let mut event_data: BTreeMap<EventHash, (EventBytes, CborBytes)> = BTreeMap::new();

        // Phase 1: serialize every distinct op once, skipping hashes
        // the caller already has cached.
        let mut membership_source_hashes: BTreeMap<Identifier, Vec<EventHash>> = BTreeMap::new();
        let mut delegate_ids_per_source: BTreeMap<Identifier, Vec<Identifier>> = BTreeMap::new();
        for (source_id, source_ops) in &all_membership.ops {
            let mut hashes = Vec::with_capacity(source_ops.len());
            let mut delegates = Vec::new();
            for (digest, op) in source_ops {
                let h = digest_to_bytes(digest);
                hashes.push(h);
                if let MembershipOperation::Delegation(dlg) = op {
                    delegates.push(dlg.payload.delegate().id());
                }
                if !skip_serialization.contains(&h)
                    && let Entry::Vacant(e) = event_data.entry(h)
                {
                    let event: Event<Async, Signer, T, L> = op.clone().into();
                    let static_event = StaticEvent::from(event);
                    e.insert(serialize_event_pair(&static_event)?);
                }
            }
            membership_source_hashes.insert(*source_id, hashes);
            if !delegates.is_empty() {
                delegate_ids_per_source.insert(*source_id, delegates);
            }
        }

        let mut prekey_source_hashes: BTreeMap<Identifier, Vec<EventHash>> = BTreeMap::new();
        for (source_id, key_ops) in &all_prekey.ops {
            let events = key_ops
                .iter()
                .map(|op| -> Event<Async, Signer, T, L> { Event::from(op.as_ref().clone()) });
            prekey_source_hashes.insert(
                *source_id,
                hash_and_insert_events(&mut event_data, events, skip_serialization)?,
            );
        }

        let mut cgka_source_hashes: BTreeMap<Identifier, Vec<EventHash>> = BTreeMap::new();
        for (source_id, cgka_ops) in &all_cgka.ops {
            let events = cgka_ops
                .iter()
                .map(|op| -> Event<Async, Signer, T, L> { Event::from(op.clone()) });
            cgka_source_hashes.insert(
                *source_id,
                hash_and_insert_events(&mut event_data, events, skip_serialization)?,
            );
        }

        // Phase 2: union of all agent IDs across the three indices.
        let mut all_agent_ids: BTreeSet<Identifier> =
            all_membership.index.keys().copied().collect();
        all_agent_ids.extend(all_prekey.index.keys().copied());
        all_agent_ids.extend(all_cgka.index.keys().copied());

        // Phase 3: roll source-keyed hashes up to per-agent sets,
        // including prekey ops for revoked delegates referenced by
        // membership delegations.
        let mut source_delegate_prekeys: BTreeMap<Identifier, BTreeSet<EventHash>> =
            BTreeMap::new();
        for (source_id, delegates) in &delegate_ids_per_source {
            let mut combined = BTreeSet::new();
            for delegate_id in delegates {
                if let Some(ph) = prekey_source_hashes.get(delegate_id) {
                    combined.extend(ph);
                }
            }
            source_delegate_prekeys.insert(*source_id, combined);
        }

        let mut agent_hashes: BTreeMap<KeyhivePeerId, BTreeSet<EventHash>> = BTreeMap::new();
        for agent_id in &all_agent_ids {
            let mut hash_set: BTreeSet<EventHash> = BTreeSet::new();
            extend_from_index(
                &mut hash_set,
                &all_membership.index,
                &membership_source_hashes,
                agent_id,
            );
            extend_from_index(
                &mut hash_set,
                &all_prekey.index,
                &prekey_source_hashes,
                agent_id,
            );
            extend_from_index(
                &mut hash_set,
                &all_cgka.index,
                &cgka_source_hashes,
                agent_id,
            );
            if let Some(sources) = all_membership.index.get(agent_id) {
                for source in sources {
                    if let Some(delegate_hashes) = source_delegate_prekeys.get(source) {
                        hash_set.extend(delegate_hashes);
                    }
                }
            }
            let peer = KeyhivePeerId::from_identifier(agent_id);
            agent_hashes.insert(peer, hash_set);
        }

        Ok(crate::all_agent_events::AllAgentEvents {
            agent_hashes,
            event_data,
        })
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
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let peer_ids = match target {
            Some(t) => vec![t.clone()],
            None => self.peer_ids().await,
        };

        for target_id in &peer_ids {
            if target_id.same_identity(&self.peer_id) {
                continue;
            }

            let cached = self.cached_events_for_pair_with_peer(target_id).await;
            let computed;
            let pair = if let Some(ref c) = cached {
                Some(c)
            } else {
                match self.get_events_for_peer_pair(target_id).await {
                    Ok(p) => {
                        computed = p;
                        computed.as_ref()
                    }
                    Err(e) => {
                        tracing::warn!(
                            target = %target_id,
                            error = %e,
                            "failed to get hashes for peer pair, skipping"
                        );
                        continue;
                    }
                }
            };
            if let Some(hashes) = pair {
                let found: Vec<EventHash> = hashes.keys().copied().collect();
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
            } else {
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
        }

        Ok(())
    }

    /// Send a lightweight sync check to a peer.
    ///
    /// This is called instead of [`sync_keyhive`](Self::sync_keyhive)
    /// when there is an established syncpoint for the target. Computes the local
    /// total and sends a [`Message::SyncCheck`].
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if total computation, signing, or sending fails.
    pub async fn sync_check_keyhive(
        &self,
        target: &KeyhivePeerId,
        our_syncpoint_for_target: u64,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let our_total = self.compute_total_for_peer(target).await?;
        let msg = Message::SyncCheck {
            sender_id: self.peer_id.clone(),
            target_id: target.clone(),
            sender_total: our_total,
            sender_syncpoint: our_syncpoint_for_target,
        };
        self.sign_and_send(target, msg, false).await
    }

    async fn resolve_sync_check(
        &self,
        peer: &KeyhivePeerId,
        sender_total: u64,
        sender_syncpoint: u64,
        local_syncpoint_for_sender: u64,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let our_total = self.compute_total_for_peer(peer).await?;

        let in_sync = local_syncpoint_for_sender == sender_total && sender_syncpoint == our_total;

        if in_sync {
            tracing::debug!(
                peer = %peer,
                "sync check passed, peers are in sync"
            );
        } else {
            tracing::debug!(
                peer = %peer,
                our_total,
                sender_total,
                sender_syncpoint,
                local_syncpoint_for_sender,
                "sync check mismatch, falling back to full sync"
            );
            self.sync_keyhive(Some(peer)).await?;
        }
        Ok(())
    }

    /// Handle an incoming signed message from a peer.
    ///
    /// Verifies the message signature, optionally ingests a contact card,
    /// and dispatches to the appropriate handler based on message type.
    /// Updates syncpoints internally.
    ///
    /// The optional `conn` is used to auto-register unknown peers when a
    /// `SyncCheck` arrives before any explicit `add_peer` call.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if signature verification, contact card
    /// ingestion, or message handling fails.
    pub async fn handle_message(
        &self,
        from: &KeyhivePeerId,
        signed_msg: SignedMessage,
        conn: Option<Conn>,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let cached_sender_pair = self.cached_events_for_pair_with_peer(from).await;
        let verified = signed_msg.verify(from)?;

        if let Some(contact_card) = &verified.contact_card {
            self.ingest_contact_card(contact_card).await?;
        }

        let message: Message = cbor_deserialize(&verified.payload)
            .map_err(|e| VerificationError::Deserialization(e.to_string()))?;

        // Reject messages where the inner sender_id doesn't match the signer.
        if !message.sender_id().same_identity(&verified.sender_id) {
            return Err(VerificationError::SenderMismatch {
                expected: verified.sender_id.clone(),
                actual: message.sender_id().clone(),
            }
            .into());
        }

        match &message {
            Message::SyncRequest { .. } => {
                self.handle_sync_request(message, cached_sender_pair.as_ref())
                    .await
            }
            Message::SyncResponse { .. } => {
                self.handle_sync_response(message, cached_sender_pair.as_ref())
                    .await
            }
            Message::SyncOps { .. } => self.handle_sync_ops(message).await,
            Message::RequestContactCard { .. } => self.handle_request_contact_card(message).await,
            Message::MissingContactCard { .. } => self.handle_missing_contact_card(message).await,
            Message::SyncCheck {
                sender_id,
                sender_total,
                sender_syncpoint,
                ..
            } => {
                if !self.has_peer(sender_id).await
                    && let Some(c) = conn
                {
                    self.add_peer(sender_id.clone(), c).await;
                }
                let local_syncpoint_for_sender =
                    self.syncpoints.lock().await.get(sender_id).unwrap_or(0);
                self.resolve_sync_check(
                    sender_id,
                    *sender_total,
                    *sender_syncpoint,
                    local_syncpoint_for_sender,
                )
                .await
            }
            Message::SyncConfirmation {
                sender_id,
                confirmer_total,
                ..
            } => {
                self.syncpoints
                    .lock()
                    .await
                    .set(sender_id.clone(), *confirmer_total);
                Ok(())
            }
        }
    }

    /// Handle a `SyncRequest`: compute which ops to send and which to request.
    async fn handle_sync_request(
        &self,
        message: Message,
        cached_sender_pair: Option<&AgentHashMap>,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
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

        let computed;
        let local_events = if let Some(c) = cached_sender_pair {
            c
        } else {
            let Some(c) = self.get_events_for_peer_pair(&sender_id).await? else {
                // We don't know this peer. Request their contact card.
                tracing::debug!(from = %sender_id, "no agent found, requesting contact card");
                let msg = Message::RequestContactCard {
                    sender_id: self.peer_id.clone(),
                    target_id: sender_id.clone(),
                };
                self.sign_and_send(&sender_id, msg, true).await?;
                return Ok(());
            };
            computed = c;
            &computed
        };

        let our_pending_hashes = self.get_pending_hashes().await?;

        // Compute totals from the original collections before building sets.
        let sync_responder_total = (local_events.len() + our_pending_hashes.len()) as u64;
        let sync_requester_total = (peer_found.len() + peer_pending.len()) as u64;

        // Build sets for comparison.
        let peer_found_set: Set<EventHash> = peer_found.iter().copied().collect();
        let peer_pending_set: Set<EventHash> = peer_pending.iter().copied().collect();
        let local_set: Set<EventHash> = local_events.keys().copied().collect();
        let our_pending_set: Set<EventHash> = our_pending_hashes.iter().copied().collect();

        // Ops to send = local - (peer_found U peer_pending)
        let mut found_ops: Vec<EventBytes> = Vec::new();
        for (h, (event_bytes, _)) in local_events {
            if !peer_found_set.contains(h) && !peer_pending_set.contains(h) {
                found_ops.push(event_bytes.clone());
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
            sync_responder_total,
            sync_requester_total,
        };

        self.sign_and_send(&sender_id, response, false).await?;
        Ok(())
    }

    /// Handle a `SyncResponse`: ingest events we received and send any
    /// requested ops back.
    ///
    /// If ops are sent back, the other side will confirm in `handle_sync_ops`.
    /// If no ops are sent, we send a confirmation and establish a syncpoint.
    async fn handle_sync_response(
        &self,
        message: Message,
        cached_sender_pair: Option<&AgentHashMap>,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let Message::SyncResponse {
            sender_id,
            requested: requested_hashes,
            found: found_events,
            sync_responder_total,
            sync_requester_total,
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

        let total_before = self.total_ops().await;

        let ingested = !found_events.is_empty();
        if ingested {
            self.ingest_events(&found_events).await?;
        }

        let total_after = self.total_ops().await;
        let advanced = total_after != total_before;

        // Send requested ops.
        if !requested_hashes.is_empty() {
            let ops = self
                .get_event_bytes_for_requested(&sender_id, &requested_hashes, cached_sender_pair)
                .await?;

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
                    sync_responder_total,
                    sync_requester_total,
                };

                self.sign_and_send(&sender_id, msg, false).await?;
                // The other side will send a confirmation after ingesting.
                if advanced {
                    self.syncpoints.lock().await.invalidate_all();
                }
                return Ok(());
            }
        }

        // No ops to send back. Send confirmation and establish syncpoint.
        // Our total is sync_requester_total (we are the requester).
        let confirmation = Message::SyncConfirmation {
            sender_id: self.peer_id.clone(),
            target_id: sender_id.clone(),
            confirmer_total: sync_requester_total,
        };
        self.sign_and_send(&sender_id, confirmation, false).await?;

        {
            let mut map = self.syncpoints.lock().await;
            if advanced {
                map.invalidate_all();
            }
            map.set(sender_id, sync_responder_total);
        }

        Ok(())
    }

    /// Handle `SyncOps`: ingest received operations and send confirmation.
    async fn handle_sync_ops(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let Message::SyncOps {
            sender_id,
            ops,
            sync_responder_total,
            sync_requester_total,
            ..
        } = message
        else {
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

        let total_before = self.total_ops().await;

        if !ops.is_empty() {
            self.ingest_events(&ops).await?;
        }

        let total_after = self.total_ops().await;
        let advanced = total_after != total_before;

        // Send confirmation after ingesting ops.
        // Our total is sync_responder_total (we are the responder).
        let confirmation = Message::SyncConfirmation {
            sender_id: self.peer_id.clone(),
            target_id: sender_id.clone(),
            confirmer_total: sync_responder_total,
        };
        self.sign_and_send(&sender_id, confirmation, false).await?;

        {
            let mut map = self.syncpoints.lock().await;
            if advanced {
                map.invalidate_all();
            }
            map.set(sender_id, sync_requester_total);
        }

        Ok(())
    }

    /// Handle `RequestContactCard`: send our contact card to the requesting
    /// peer.
    async fn handle_request_contact_card(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
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

        self.sign_and_send(&sender_id, msg, true).await?;
        Ok(())
    }

    /// Handle `MissingContactCard`: the contact card was already ingested in
    /// `handle_message`, so now we trigger a sync with the peer.
    async fn handle_missing_contact_card(
        &self,
        message: Message,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
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

        self.sync_keyhive(Some(&sender_id)).await?;
        Ok(())
    }

    /// Sign a message and send it to a peer.
    async fn sign_and_send(
        &self,
        target_peer_id: &KeyhivePeerId,
        message: Message,
        include_contact_card: bool,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let msg_bytes =
            cbor_serialize(&message).map_err(|e| SigningError::Serialization(e.to_string()))?;

        let signed: Signed<Vec<u8>> = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .try_sign(msg_bytes)
                .await
                .map_err(SigningError::SigningFailed)?
        };

        // Wire format: bincode `Signed<Vec<u8>>` (matches `Signed.toBytes()`
        // from keyhive-wasm; see `keyhive_wasm/src/js/signed.rs`).
        let signed_bytes =
            bincode::serialize(&signed).map_err(|e| SigningError::Serialization(e.to_string()))?;

        let signed_message = if include_contact_card {
            SignedMessage::with_contact_card(signed_bytes, self.contact_card.clone())
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
    async fn get_events_for_peer_pair(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<Option<AgentHashMap>, ProtocolError<Conn::SendError>> {
        let our_id = self
            .peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;
        let their_id = peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;

        let (our_events, their_events, public_events) = {
            let keyhive = self.keyhive.lock().await;

            let our_agent = keyhive.get_agent(our_id).await;
            let their_agent = keyhive.get_agent(their_id).await;

            match (our_agent, their_agent) {
                (Some(ref our_agent), Some(ref their_agent)) => {
                    let our_events = sync_events_for_agent(&keyhive, our_agent).await;
                    let their_events = sync_events_for_agent(&keyhive, their_agent).await;
                    let public_events: Map<Digest<StaticEvent<T>>, StaticEvent<T>> =
                        if let Some(ref public_agent) = keyhive.get_agent(Public.id()).await {
                            sync_events_for_agent(&keyhive, public_agent).await
                        } else {
                            Map::new()
                        };
                    (our_events, their_events, public_events)
                }
                _ => return Ok(None),
            }
        };

        let mut result = AgentHashMap::new();
        for (digest, event) in public_events {
            let h = digest_to_bytes(&digest);
            let pair = serialize_event_pair(&event)?;
            result.insert(h, pair);
        }

        for (digest, event) in our_events {
            if their_events.contains_key(&digest) {
                let h = digest_to_bytes(&digest);
                if let Entry::Vacant(entry) = result.entry(h) {
                    let pair = serialize_event_pair(&event)?;
                    entry.insert(pair);
                }
            }
        }

        Ok(Some(result))
    }

    /// Get pending event hashes as `Vec<EventHash>`.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the keyhive lock or hash computation fails.
    pub async fn get_pending_hashes(
        &self,
    ) -> Result<Vec<EventHash>, ProtocolError<Conn::SendError>> {
        let keyhive = self.keyhive.lock().await;
        let digests = keyhive.pending_event_hashes().await;
        Ok(digests.into_iter().map(|d| digest_to_bytes(&d)).collect())
    }

    /// Fetch serialized event bytes for the given hashes.
    async fn get_event_bytes_for_requested(
        &self,
        sender_id: &KeyhivePeerId,
        requested: &[EventHash],
        cached_pair: Option<&AgentHashMap>,
    ) -> Result<Vec<EventBytes>, ProtocolError<Conn::SendError>> {
        let computed;
        let pair = if let Some(c) = cached_pair {
            c
        } else {
            let Some(c) = self.get_events_for_peer_pair(sender_id).await? else {
                return Ok(Vec::new());
            };
            computed = c;
            &computed
        };

        let mut result = Vec::with_capacity(requested.len().min(pair.len()));
        for h in requested {
            if let Some((bytes, _)) = pair.get(h) {
                result.push(bytes.clone());
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
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let mut events: Vec<StaticEvent<T>> = Vec::with_capacity(event_bytes_list.len());
        for (idx, bytes) in event_bytes_list.iter().enumerate() {
            match bincode_deserialize::<StaticEvent<T>>(bytes) {
                Ok(ev) => events.push(ev),
                Err(e) => {
                    let head_len = core::cmp::min(bytes.len(), 96);
                    let mut head_hex = alloc::string::String::with_capacity(head_len * 2);
                    for b in bytes.get(..head_len).unwrap_or(bytes) {
                        use core::fmt::Write;
                        let _ = write!(head_hex, "{b:02x}");
                    }
                    tracing::warn!(
                        index = idx,
                        total = event_bytes_list.len(),
                        len = bytes.len(),
                        head = %head_hex,
                        error = ?e,
                        "failed to deserialize incoming event"
                    );
                    return Err(ProtocolError::Deserialization(e.to_string()));
                }
            }
        }

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

        let use_archive = self
            .archive_config
            .filter(|(threshold, _)| event_bytes_list.len() > *threshold);

        if let Some((_, storage_id)) = use_archive {
            let archive = {
                let keyhive = self.keyhive.lock().await;
                keyhive.into_archive().await
            };
            storage_ops::save_keyhive_archive(&self.storage, storage_id, &archive).await?;
        } else {
            for event_bytes in event_bytes_list {
                if let Err(e) =
                    storage_ops::save_event_bytes(&self.storage, event_bytes.clone()).await
                {
                    tracing::warn!(error = %e, "failed to persist event to storage");
                }
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
            .filter_map(|bytes| match bincode_deserialize(bytes) {
                Ok(ev) => Some(ev),
                Err(e) => {
                    tracing::warn!(error = %e, "storage recovery: failed to deserialize event");
                    None
                }
            })
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

    /// Ingest a contact card into keyhive and persist its prekey op to
    /// storage.
    async fn ingest_contact_card(
        &self,
        contact_card: &ContactCard,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        // Validate first: only persist after keyhive accepts the card.
        {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .receive_contact_card(contact_card)
                .await
                .map_err(ProtocolError::ReceiveContactCard)?;
        }

        let event: StaticEvent<T> = match contact_card.op() {
            KeyOp::Add(add) => StaticEvent::PrekeysExpanded(Box::new(add.as_ref().clone())),
            KeyOp::Rotate(rot) => StaticEvent::PrekeyRotated(Box::new(rot.as_ref().clone())),
        };
        if let Err(e) = storage_ops::save_event(&self.storage, &event).await {
            tracing::error!(error = %e, "failed to save contact card op to storage. Card will be lost on restart");
        }

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
    pub async fn compact(&self, storage_id: StorageHash) -> Result<(), StorageError> {
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

    /// Look up cached events for us paired with the provided peer
    async fn cached_events_for_pair_with_peer(&self, peer: &KeyhivePeerId) -> Option<AgentHashMap> {
        let cache = self.cache.lock().await;
        let local = self.peer_id();
        let map = cache.events_for_peer_pair(&local, peer);
        (!map.is_empty()).then_some(map)
    }

    /// Compute the total operation count for a peer pair.
    ///
    /// This is the number of intersection hashes plus pending hashes,
    /// used for sync check/confirmation metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if hash or pending computation fails.
    async fn compute_total_for_peer(
        &self,
        peer: &KeyhivePeerId,
    ) -> Result<u64, ProtocolError<Conn::SendError>> {
        let hash_count = if let Some(cached) = self.cached_events_for_pair_with_peer(peer).await {
            cached.len()
        } else {
            self.get_events_for_peer_pair(peer)
                .await?
                .map_or(0, |h| h.len())
        };
        let pending_count = self.get_pending_hashes().await?.len();
        Ok((hash_count + pending_count) as u64)
    }

    /// Sync with a peer using the best available strategy.
    ///
    /// Sends a lightweight sync check when a syncpoint exists for the
    /// peer; otherwise falls back to a full sync.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the protocol call fails.
    pub async fn initiate_sync_with_peer(
        &self,
        peer: &KeyhivePeerId,
    ) -> Result<(), ProtocolError<Conn::SendError>> {
        let syncpoint = self.syncpoints.lock().await.get(peer);

        match syncpoint {
            Some(sp) => self.sync_check_keyhive(peer, sp).await,
            None => self.sync_keyhive(Some(peer)).await,
        }
    }

    /// Refresh the periodic event cache from the underlying keyhive.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if any per-agent walk fails.
    pub async fn refresh_cache(&self) -> Result<(), ProtocolError<Conn::SendError>> {
        let mut cache = self.cache.lock().await;
        cache.refresh(self).await.map(|_| ())
    }
}

/// Get sync-relevant events for an agent: membership, prekey, and CGKA operations.
async fn sync_events_for_agent<Async, Signer, T, P, C, L, R>(
    keyhive: &Keyhive<Async, Signer, T, P, C, L, R>,
    agent: &Agent<Async, Signer, T, L>,
) -> Map<Digest<StaticEvent<T>>, StaticEvent<T>>
where
    Async: future_form::FutureForm,
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<Async, T, P> + CiphertextStoreExt<Async, T, P> + Clone,
    L: MembershipListener<Async, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
{
    keyhive
        .static_events_for_agent(agent)
        .await
        .into_iter()
        .collect()
}

/// Hash, serialize, and deduplicate events into `event_data`, returning hashes.
///
/// Hashes present in `skip_serialization` are recorded but not serialized.
fn hash_and_insert_events<Async, Signer, T, L, E>(
    event_data: &mut BTreeMap<EventHash, (EventBytes, CborBytes)>,
    events: impl Iterator<Item = Event<Async, Signer, T, L>>,
    skip_serialization: &BTreeSet<EventHash>,
) -> Result<Vec<EventHash>, ProtocolError<E>>
where
    Async: future_form::FutureForm,
    Signer: AsyncSigner<Async> + Clone,
    T: ContentRef,
    L: MembershipListener<Async, Signer, T>,
    E: core::error::Error + 'static,
{
    let mut hashes = Vec::new();
    for event in events {
        let digest = Digest::hash(&event);
        let h = digest_to_bytes(&digest);
        hashes.push(h);
        if !skip_serialization.contains(&h)
            && let Entry::Vacant(e) = event_data.entry(h)
        {
            let static_event = StaticEvent::from(event);
            e.insert(serialize_event_pair(&static_event)?);
        }
    }
    Ok(hashes)
}

/// Serialize a map of digested events into a hash-keyed byte map.
fn collect_serialized_events<T: ContentRef, E: core::error::Error + 'static>(
    events: Map<Digest<StaticEvent<T>>, StaticEvent<T>>,
) -> Result<BTreeMap<EventHash, (EventBytes, CborBytes)>, ProtocolError<E>> {
    let mut out = BTreeMap::new();
    for (digest, event) in events {
        let h = digest_to_bytes(&digest);
        let (event_bytes, cbor_bytes) = serialize_event_pair(&event)?;
        out.insert(h, (event_bytes, cbor_bytes));
    }
    Ok(out)
}

/// Serialize a `StaticEvent` to bincode bytes and a CBOR byte-string wrapper.
fn serialize_event_pair<T: ContentRef, E: core::error::Error + 'static>(
    event: &StaticEvent<T>,
) -> Result<(EventBytes, CborBytes), ProtocolError<E>> {
    let bytes =
        bincode_serialize(event).map_err(|e| ProtocolError::Serialization(e.to_string()))?;
    let cbor = wrap_as_cbor_byte_string(&bytes);
    Ok((bytes, cbor))
}

/// Collect hashes reachable from `agent_id` through `index` into `hash_set`.
fn extend_from_index(
    hash_set: &mut BTreeSet<EventHash>,
    index: &HashMap<Identifier, HashSet<Identifier>>,
    source_hashes: &BTreeMap<Identifier, Vec<EventHash>>,
    agent_id: &Identifier,
) {
    if let Some(sources) = index.get(agent_id) {
        for s in sources {
            if let Some(hs) = source_hashes.get(s) {
                hash_set.extend(hs);
            }
        }
    }
}

/// Serialize a value to CBOR bytes.
fn cbor_serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Wrap raw bytes in a CBOR byte-string (major type 2) header.
///
/// Lets the cache hand pre-encoded fragments straight to a hand-rolled
/// wire path without re-running ciborium's array serializer.
#[allow(clippy::cast_possible_truncation)] // match arms guarantee range
fn wrap_as_cbor_byte_string(bytes: &[u8]) -> Vec<u8> {
    let len = bytes.len();
    let mut out = match len {
        0..=23 => {
            let mut v = Vec::with_capacity(1 + len);
            v.push(0x40 | len as u8);
            v
        }
        24..=255 => {
            let mut v = Vec::with_capacity(2 + len);
            v.push(0x58);
            v.push(len as u8);
            v
        }
        256..=65535 => {
            let mut v = Vec::with_capacity(3 + len);
            v.push(0x59);
            v.extend_from_slice(&(len as u16).to_be_bytes());
            v
        }
        65536..=4_294_967_295 => {
            let mut v = Vec::with_capacity(5 + len);
            v.push(0x5A);
            v.extend_from_slice(&(len as u32).to_be_bytes());
            v
        }
        _ => {
            let mut v = Vec::with_capacity(9 + len);
            v.push(0x5B);
            v.extend_from_slice(&(len as u64).to_be_bytes());
            v
        }
    };
    out.extend_from_slice(bytes);
    out
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
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::wildcard_enum_match_arm,
    clippy::panic,
    clippy::too_many_lines
)]
mod tests {
    use super::*;
    use crate::{
        storage::MemoryKeyhiveStorage,
        test_utils::{
            SimpleKeyhive, TestProtocol, TwoPeerHarness, create_channel_pair,
            create_group_with_read_members, exchange_all_contact_cards,
            exchange_contact_cards_and_setup, keyhive_peer_id, make_keyhive,
            make_protocol_with_shared_keyhive, run_sync_round, sync_pair_rounds,
        },
    };
    use future_form::Local;
    use keyhive_core::{
        access::Access,
        principal::{identifier::Identifier, membered::Membered, peer::Peer},
    };
    use nonempty::nonempty;

    /// Helper to create a test protocol instance.
    async fn make_protocol() -> (TestProtocol, SimpleKeyhive) {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.unwrap();
        let storage = MemoryKeyhiveStorage::new();
        let shared = Arc::new(Mutex::new(keyhive.clone()));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc);
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
    async fn handle_message_rejects_forged_inner_sender_id() {
        // Alice signs a message that *claims* (via the inner Message
        // payload's `sender_id`) to come from Bob. The signature
        // verifies (Alice really did sign), but the cross-check
        // between `verified.sender_id` (from the signing key) and the
        // inner payload's `sender_id` should reject.
        let (alice, _alice_kh) = make_protocol().await;
        let alice_id = alice.peer_id.clone();
        let bob_id = keyhive_peer_id(&make_keyhive().await);
        let (conn_alice_to_bob, _conn_bob_to_alice) =
            create_channel_pair(alice_id.clone(), &bob_id);
        alice.add_peer(bob_id.clone(), conn_alice_to_bob).await;

        // Construct a Message whose inner sender_id is Bob, even though
        // Alice will sign it.
        let forged = Message::RequestContactCard {
            sender_id: bob_id.clone(),
            target_id: alice_id.clone(),
        };
        let payload = cbor_serialize(&forged).expect("forged payload should serialize");
        let signed: keyhive_crypto::signed::Signed<Vec<u8>> = {
            let kh = alice.keyhive.lock().await;
            kh.try_sign(payload).await.expect("alice signs")
        };
        let signed_bytes = bincode::serialize(&signed).expect("bincode encode");
        let signed_msg = SignedMessage::new(signed_bytes);

        let err = alice
            .handle_message(&alice_id, signed_msg, None)
            .await
            .expect_err("should reject forged inner sender_id");
        assert!(
            matches!(
                err,
                ProtocolError::Verification(VerificationError::SenderMismatch { .. })
            ),
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
            .handle_message(&alice_id, signed_msg1, None)
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
            .handle_message(&bob_id, signed_msg2, None)
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
            .handle_message(&alice_id, signed_msg1, None)
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
            .handle_message(&bob_id, signed_msg2, None)
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

        let alice_storage = MemoryKeyhiveStorage::new();
        let bob_storage = MemoryKeyhiveStorage::new();

        let alice_shared = Arc::new(Mutex::new(alice_kh));
        let bob_shared = Arc::new(Mutex::new(bob_kh));

        let (alice_conn, bob_conn) = create_channel_pair(alice_id.clone(), &bob_id);

        let alice_proto = TestProtocol::new(
            alice_shared.clone(),
            alice_storage.clone(),
            alice_id.clone(),
            alice_cc.clone(),
        );
        let bob_proto = TestProtocol::new(
            bob_shared.clone(),
            bob_storage.clone(),
            bob_id.clone(),
            bob_cc.clone(),
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
            .handle_message(&alice_id, sync_request, None)
            .await
            .unwrap();

        // Forward SyncResponse to Alice
        let sync_response = b_to_a_rx.recv().await.unwrap();
        alice_proto
            .handle_message(&bob_id, sync_response, None)
            .await
            .unwrap();

        // If Alice sent SyncOps, forward those too
        if let Ok(sync_ops) = a_to_b_rx.try_recv() {
            bob_proto
                .handle_message(&alice_id, sync_ops, None)
                .await
                .unwrap();
        }

        // Handle any confirmation messages before next round
        while let Ok(msg) = a_to_b_rx.try_recv() {
            bob_proto
                .handle_message(&alice_id, msg, None)
                .await
                .unwrap();
        }
        while let Ok(msg) = b_to_a_rx.try_recv() {
            alice_proto
                .handle_message(&bob_id, msg, None)
                .await
                .unwrap();
        }

        // Now Bob → Alice sync
        bob_proto.sync_keyhive(Some(&alice_id)).await.unwrap();

        let sync_request2 = b_to_a_rx.recv().await.unwrap();
        alice_proto
            .handle_message(&bob_id, sync_request2, None)
            .await
            .unwrap();

        let sync_response2 = a_to_b_rx.recv().await.unwrap();
        bob_proto
            .handle_message(&alice_id, sync_response2, None)
            .await
            .unwrap();

        if let Ok(sync_ops2) = b_to_a_rx.try_recv() {
            alice_proto
                .handle_message(&bob_id, sync_ops2, None)
                .await
                .unwrap();
        }

        // Handle any remaining confirmation messages
        while let Ok(msg) = a_to_b_rx.try_recv() {
            bob_proto
                .handle_message(&alice_id, msg, None)
                .await
                .unwrap();
        }
        while let Ok(msg) = b_to_a_rx.try_recv() {
            alice_proto
                .handle_message(&bob_id, msg, None)
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
    async fn ingest_events_writes_archive_when_threshold_exceeded() {
        // Two peers exchange contact cards so Alice's keyhive ends up
        // with several real events (delegations + prekey ops). Then
        // feed those events back through `ingest_events` on a fresh
        // protocol configured with a low `archive_threshold` and
        // verify exactly one archive lands in storage and no
        // individual event files.
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;
        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        let alice_cc = alice_kh.contact_card().await.unwrap();
        let bob_cc = bob_kh.contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();
        let storage_id = crate::storage::StorageHash::new([42u8; 32]);
        let shared = Arc::new(Mutex::new(alice_kh.clone()));
        let protocol = TestProtocol::new(shared, storage.clone(), alice_id.clone(), alice_cc)
            .with_archive_threshold(2, storage_id);

        // Pull real event bytes off Bob's agent (these are the kind of
        // events that arrive over the wire from a real peer).
        let pair = protocol
            .get_events_for_agent(&bob_id)
            .await
            .unwrap()
            .expect("bob should resolve to an agent");
        let event_bytes: Vec<EventBytes> = pair.values().map(|(b, _)| b.clone()).collect();
        assert!(
            event_bytes.len() > 2,
            "need >2 events to exceed threshold of 2 (got {})",
            event_bytes.len()
        );

        protocol.ingest_events(&event_bytes).await.unwrap();

        let archives = crate::storage_ops::load_archives::<[u8; 32], _, Local>(&storage)
            .await
            .unwrap();
        let events = crate::storage_ops::load_events::<[u8; 32], _, Local>(&storage)
            .await
            .unwrap();
        assert_eq!(archives.len(), 1, "expected one archive write");
        assert_eq!(archives[0].0, storage_id, "archive at expected storage_id");
        assert!(
            events.is_empty(),
            "expected no individual event files when threshold path fires (got {})",
            events.len()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingest_events_writes_individual_files_below_threshold() {
        // Mirror of the above with a high threshold: confirm the
        // existing per-event write path still fires when the threshold
        // isn't crossed.
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;
        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        let alice_cc = alice_kh.contact_card().await.unwrap();
        let bob_cc = bob_kh.contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();
        let storage_id = crate::storage::StorageHash::new([42u8; 32]);
        let shared = Arc::new(Mutex::new(alice_kh.clone()));
        let protocol = TestProtocol::new(shared, storage.clone(), alice_id.clone(), alice_cc)
            .with_archive_threshold(10_000, storage_id);

        let pair = protocol
            .get_events_for_agent(&bob_id)
            .await
            .unwrap()
            .unwrap();
        let event_bytes: Vec<EventBytes> = pair.values().map(|(b, _)| b.clone()).collect();
        assert!(!event_bytes.is_empty());

        protocol.ingest_events(&event_bytes).await.unwrap();

        let archives = crate::storage_ops::load_archives::<[u8; 32], _, Local>(&storage)
            .await
            .unwrap();
        let events = crate::storage_ops::load_events::<[u8; 32], _, Local>(&storage)
            .await
            .unwrap();
        assert!(archives.is_empty(), "no archive expected below threshold");
        assert_eq!(
            events.len(),
            event_bytes.len(),
            "expected one event file per ingested event"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_sync_response_serves_requested_from_cached_pair() {
        // Regression test for the byte-fetch divergence between ARK and
        // Subduction (see
        // `~/dev/prg/sync-server/keyhive-sync-protocol-ts-vs-rust-comparison.md`).
        //
        // Pre-fix, `handle_sync_response` ignored the protocol's
        // cached pair entirely and walked our own active agent's
        // reachable set via `sync_events_for_agent`. That silently
        // dropped any hash advertised via public-agent reachability,
        // any hash whose direct reachability had since narrowed
        // (revocation/expansion), and any hash present in the cached
        // bytes store but not in the live walk, leaving the requesting
        // peer stuck on dependent pending events.
        //
        // We exercise the cache-hit path directly by handing the
        // handler a cached pair containing a synthetic hash → bytes
        // mapping that is *not* in Alice's keyhive at all. Pre-fix,
        // Alice would walk her own agent, find nothing, and send a
        // confirmation (no SyncOps). Post-fix, Alice serves the bytes
        // straight from the cached pair and replies with SyncOps.
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let synthetic_hash: EventHash = [0xAB; 32];
        let synthetic_bytes: EventBytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let synthetic_cbor = wrap_as_cbor_byte_string(&synthetic_bytes);

        let mut cached_pair = AgentHashMap::new();
        cached_pair.insert(synthetic_hash, (synthetic_bytes.clone(), synthetic_cbor));

        // SyncResponse from Bob asking Alice for the synthetic hash.
        // Bob's `requested` always lists hashes Alice advertised, so
        // they're in Alice's pair set with Bob, exactly the case the
        // fix targets.
        let msg = Message::SyncResponse {
            sender_id: bob_id.clone(),
            target_id: alice_id.clone(),
            requested: vec![synthetic_hash],
            found: vec![],
            sync_responder_total: 0,
            sync_requester_total: 0,
        };

        alice_proto
            .handle_sync_response(msg, Some(&cached_pair))
            .await
            .expect("alice handle_sync_response with cached pair");

        // Drain Bob's inbound channel and verify Alice sent SyncOps
        // carrying the cached bytes.
        let signed = bob_conn
            .inbound_rx
            .try_recv()
            .expect("bob should have received a SyncOps from alice");
        let verified = signed
            .verify(&alice_id)
            .expect("alice's signature verifies");
        let outbound: Message =
            cbor_deserialize(&verified.payload).expect("decode alice's outbound payload");

        let Message::SyncOps { ops, .. } = outbound else {
            panic!(
                "expected SyncOps from alice (carrying cached bytes), got: {:?}",
                outbound.variant_name()
            );
        };
        assert_eq!(
            ops.len(),
            1,
            "alice should have served exactly one op (the synthetic one)"
        );
        assert_eq!(
            ops[0], synthetic_bytes,
            "served bytes should match the cached pair entry, \
             not anything from a live keyhive walk"
        );

        // And Bob shouldn't have received anything else.
        assert!(
            bob_conn.inbound_rx.try_recv().is_err(),
            "no further messages expected from alice"
        );
        // Symmetric: alice didn't loop a message to herself.
        assert!(
            alice_conn.inbound_rx.try_recv().is_err(),
            "no inbound message expected on alice's side"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_message_rejects_unsigned_junk() {
        let (protocol, _keyhive) = make_protocol().await;

        let fake_sender = keyhive_peer_id(&make_keyhive().await);

        // Create a SignedMessage with junk data
        let junk = SignedMessage::new(vec![0xFF, 0xFE, 0xFD]);

        let result = protocol.handle_message(&fake_sender, junk, None).await;
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

        let storage = MemoryKeyhiveStorage::new();

        // Save an archive
        let archive = keyhive.into_archive().await;
        let storage_id = crate::storage::StorageHash::new([1u8; 32]);
        crate::storage_ops::save_keyhive_archive::<_, _, Local>(&storage, storage_id, &archive)
            .await
            .unwrap();

        let shared = Arc::new(Mutex::new(keyhive));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc);

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

    #[tokio::test(flavor = "current_thread")]
    async fn sync_round_produces_confirmation_with_correct_totals() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Alice creates a group so there are ops to sync
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }

        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Both sides should have established syncpoints with non-zero totals
        // (there are ops from the group creation).
        let alice_sp = alice_proto.syncpoints.lock().await.get(&bob_id);
        let bob_sp = bob_proto.syncpoints.lock().await.get(&alice_id);
        assert!(
            alice_sp.is_some_and(|v| v > 0),
            "alice should have a non-zero syncpoint for bob, got: {alice_sp:?}"
        );
        assert!(
            bob_sp.is_some_and(|v| v > 0),
            "bob should have a non-zero syncpoint for alice, got: {bob_sp:?}"
        );
    }

    /// Reproduces the e2e cross-stack scenario where the responder owns
    /// every op and the requester has none to send back. The responder's
    /// `SyncResponse` carries `requested=[]` and `found=many`, so the
    /// requester must close the round directly with `SyncConfirmation`
    /// (no intervening `SyncOps` step).
    ///
    /// Setup runs an initial alignment round so that both peers share the
    /// baseline (prekey / contact-card) history; then Bob creates a group,
    /// and a second round exercises the asymmetric "responder has more,
    /// requester has nothing new" path that the e2e periodic sync hits.
    #[tokio::test(flavor = "current_thread")]
    async fn sync_round_with_empty_requested_returns_confirmation_directly() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Initial alignment round: drains each side's baseline history so
        // a subsequent round with one-sided new ops actually has an empty
        // `requested` field. Without this, each peer's prekey/contact-card
        // history that the other doesn't yet know about causes the round
        // to take the SyncOps path instead.
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Now Bob alone gets new ops. Alice has no new ops to offer back.
        {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await;
        }

        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Both sides should have established syncpoints with non-zero totals.
        // The empty-requested path (Alice has nothing to send back) should
        // close via direct SyncConfirmation, establishing syncpoints.
        let alice_sp = alice_proto.syncpoints.lock().await.get(&bob_id);
        let bob_sp = bob_proto.syncpoints.lock().await.get(&alice_id);
        assert!(
            alice_sp.is_some_and(|v| v > 0),
            "alice should have a non-zero syncpoint for bob after the \
             empty-requested round, got: {alice_sp:?}"
        );
        assert!(
            bob_sp.is_some_and(|v| v > 0),
            "bob should have a non-zero syncpoint for alice, got: {bob_sp:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_hashes_for_agent_returns_serialised_events_for_known_peer() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Give Alice some membership ops so the per-agent walk has content.
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }

        // Run a full sync round so Bob's keyhive learns about Alice's ops.
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        let alice_hashes = alice_proto
            .get_events_for_agent(&alice_id)
            .await
            .expect("alice get_hashes_for_agent")
            .expect("alice agent exists");
        assert!(
            !alice_hashes.is_empty(),
            "alice's agent should have membership/cgka ops after the group was created"
        );

        // Event bytes round-trip via bincode (matching keyhive-wasm); the
        // pre-encoded form wraps them in a CBOR byte-string header for the
        // outer envelope.
        for (event_bytes, cbor_bytes) in alice_hashes.values() {
            bincode_deserialize::<StaticEvent<[u8; 32]>>(event_bytes)
                .expect("event_bytes round-trips");
            assert!(
                cbor_bytes.len() > event_bytes.len(),
                "cbor_bytes carries at least a 1-byte byte-string header"
            );
            // Major-type-2 (byte-string) marker check.
            assert!(
                cbor_bytes[0] & 0xE0 == 0x40,
                "cbor_bytes[0] should be a CBOR byte-string major type"
            );
        }

        // Symmetric: querying for a peer keyhive doesn't know about returns None.
        let unknown = KeyhivePeerId::from_bytes([0xAA; 32]);
        assert!(
            alice_proto
                .get_events_for_agent(&unknown)
                .await
                .expect("get_hashes_for_agent for unknown peer")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn total_ops_increases_after_local_mutation() {
        let (protocol, _) = make_protocol().await;
        let before = protocol.total_ops().await;

        // Generate some keyhive activity by creating a group.
        {
            let kh = protocol.keyhive.lock().await;
            kh.generate_group(vec![]).await.expect("generate_group");
        }

        let after = protocol.total_ops().await;
        assert!(
            after > before,
            "total_ops should increase after creating a group: {before} -> {after}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_returns_in_sync_when_totals_match() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Alice creates a group and syncs with Bob
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }

        // First sync round to get both sides aligned (exchanges ops)
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Second sync round with no new ops — establishes stable syncpoints.
        // The first round ingested ops (invalidating syncpoints), so the
        // second round's zero-op exchange produces usable syncpoints.
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        let alice_sp = alice_proto
            .syncpoints
            .lock()
            .await
            .get(&bob_id)
            .expect("should have Alice's syncpoint for Bob");

        // Now Alice sends a sync check to Bob
        alice_proto
            .sync_check_keyhive(&bob_id, alice_sp)
            .await
            .expect("sync_check_keyhive failed");

        // Bob receives the sync check — handle_message resolves it internally
        let check_msg = bob_conn
            .inbound_rx
            .recv()
            .await
            .expect("failed to receive sync check");
        bob_proto
            .handle_message(&alice_id, check_msg, None)
            .await
            .expect("bob failed to handle sync check");

        // In-sync means no fallback SyncRequest was sent
        assert!(
            alice_conn.inbound_rx.try_recv().is_err(),
            "no outbound messages expected when peers are in sync"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_falls_back_when_out_of_sync() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Alice creates a group and syncs with Bob
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }

        // First sync to exchange ops
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        // Second sync to establish stable syncpoints (no ops exchanged)
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        let alice_sp = alice_proto
            .syncpoints
            .lock()
            .await
            .get(&bob_id)
            .expect("should have Alice's syncpoint for Bob");

        // Alice creates ANOTHER group with Bob (changes her state without syncing)
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }

        // Alice sends sync check — her total has changed but she passes
        // the stale syncpoint she got from the last round
        alice_proto
            .sync_check_keyhive(&bob_id, alice_sp)
            .await
            .expect("sync_check_keyhive failed");

        let check_msg = bob_conn
            .inbound_rx
            .recv()
            .await
            .expect("failed to receive sync check");

        // handle_message resolves the sync check internally and falls back
        bob_proto
            .handle_message(&alice_id, check_msg, None)
            .await
            .expect("bob failed to handle sync check");

        // Bob should have sent a full SyncRequest (fallback).
        // Use try_recv so the test fails fast instead of hanging if
        // the fallback didn't fire.
        let fallback_msg = alice_conn
            .inbound_rx
            .try_recv()
            .expect("should have received fallback SyncRequest from bob");
        alice_proto
            .handle_message(&bob_id, fallback_msg, None)
            .await
            .expect("alice failed to handle fallback message");
    }

    // ── Revocation chain regression tests ──────────────────────────────
    //
    // Scenario: A creates doc → make public → revoke public → add B →
    // revoke B → make public again. A and B sync incrementally with the
    // server after each step. Then fresh client C connects.
    //
    // These tests verify that:
    // 1. C receives all events with complete dependencies (no stuck pending)
    // 2. The `all_agent_events()` cache path includes delegate prekey ops
    //    for revoked agents (the root cause of the production bug)

    fn assert_event_deps_complete(
        hashes: &alloc::collections::BTreeSet<[u8; 32]>,
        events: &alloc::collections::BTreeMap<[u8; 32], StaticEvent<[u8; 32]>>,
        builtin_agents: &[keyhive_core::principal::identifier::Identifier],
        label: &str,
    ) {
        use core::fmt::Write;
        use keyhive_core::principal::identifier::Identifier;

        fn to_hex(bytes: &[u8]) -> String {
            bytes.iter().fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            })
        }

        let mut missing: Vec<String> = Vec::new();
        for (h, event) in events {
            let hh = to_hex(&h[..8]);
            match event {
                StaticEvent::Delegated(signed_dlg) => {
                    let dlg = &signed_dlg.payload;
                    if let Some(proof) = &dlg.proof {
                        let ph = digest_to_bytes(proof);
                        if !hashes.contains(&ph) {
                            missing.push(format!(
                                "delegation {hh} missing proof_dlg:{}",
                                to_hex(&ph[..8])
                            ));
                        }
                    }
                    for rev in &dlg.after_revocations {
                        let rh = digest_to_bytes(rev);
                        if !hashes.contains(&rh) {
                            missing.push(format!(
                                "delegation {hh} missing after_rev:{}",
                                to_hex(&rh[..8])
                            ));
                        }
                    }
                    let delegate_id = dlg.delegate;
                    let has_prekey = events.values().any(|e| match e {
                        StaticEvent::PrekeysExpanded(op) => Identifier(*op.issuer()) == delegate_id,
                        StaticEvent::PrekeyRotated(op) => Identifier(*op.issuer()) == delegate_id,
                        _ => false,
                    });
                    if !has_prekey && !builtin_agents.contains(&delegate_id) {
                        missing.push(format!(
                            "delegation {hh} missing prekey for delegate {delegate_id:?}",
                        ));
                    }
                }
                StaticEvent::Revoked(signed_rev) => {
                    let rev = &signed_rev.payload;
                    let rh = digest_to_bytes(&rev.revoke);
                    if !hashes.contains(&rh) {
                        missing.push(format!(
                            "revocation {hh} missing revoke_dlg:{}",
                            to_hex(&rh[..8])
                        ));
                    }
                    if let Some(proof) = &rev.proof {
                        let ph = digest_to_bytes(proof);
                        if !hashes.contains(&ph) {
                            missing.push(format!(
                                "revocation {hh} missing proof_dlg:{}",
                                to_hex(&ph[..8])
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        assert!(
            missing.is_empty(),
            "{label} has {} events but missing deps:\n{}",
            events.len(),
            missing.join("\n")
        );
    }

    /// A → server (with B syncing) → fresh C. Verifies C has no
    /// pending events after the revocation chain.
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_client_gets_complete_deps_after_revocation_chain() {
        use keyhive_core::{
            access::Access,
            principal::{agent::Agent, membered::Membered, public::Public},
        };

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_server = make_keyhive().await;
        let kh_c = make_keyhive().await;

        exchange_all_contact_cards(&[&kh_a, &kh_b]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_b, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_server, &kh_c]).await;

        let a_id = keyhive_peer_id(&kh_a);
        let b_id = keyhive_peer_id(&kh_b);
        let server_id = keyhive_peer_id(&kh_server);
        let c_id = keyhive_peer_id(&kh_c);

        let b_identifier = kh_b.id().into();
        let b_agent = kh_a.get_agent(b_identifier).await.unwrap();

        let doc = kh_a
            .generate_doc(vec![], nonempty![[0u8; 32]])
            .await
            .unwrap();
        let doc_id = doc.lock().await.doc_id();
        let membered = Membered::Document(doc_id, doc.clone());
        let public_agent: Agent<_, _, _, _> = Public.individual().into();

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (server_proto, server_kh, _) = make_protocol_with_shared_keyhive(kh_server).await;
        let (c_proto, c_kh, _) = make_protocol_with_shared_keyhive(kh_c).await;

        let (a_conn, server_conn_a) = create_channel_pair(a_id.clone(), &server_id);
        a_proto.add_peer(server_id.clone(), a_conn.clone()).await;
        server_proto
            .add_peer(a_id.clone(), server_conn_a.clone())
            .await;

        let (b_conn, server_conn_b) = create_channel_pair(b_id.clone(), &server_id);
        b_proto.add_peer(server_id.clone(), b_conn.clone()).await;
        server_proto
            .add_peer(b_id.clone(), server_conn_b.clone())
            .await;

        macro_rules! sync_a_server {
            () => {
                sync_pair_rounds(
                    &a_proto,
                    &server_proto,
                    &a_id,
                    &server_id,
                    &a_conn,
                    &server_conn_a,
                    3,
                )
                .await
            };
        }
        macro_rules! sync_b_server {
            () => {
                sync_pair_rounds(
                    &b_proto,
                    &server_proto,
                    &b_id,
                    &server_id,
                    &b_conn,
                    &server_conn_b,
                    3,
                )
                .await
            };
        }

        sync_a_server!();

        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(Public.id(), true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(b_agent, &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(b_identifier, true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        assert!(server_kh.lock().await.get_document(doc_id).await.is_some());

        let (server_conn_c, c_conn) = create_channel_pair(server_id.clone(), &c_id);
        server_proto
            .add_peer(c_id.clone(), server_conn_c.clone())
            .await;
        c_proto.add_peer(server_id.clone(), c_conn.clone()).await;

        for _ in 0..3 {
            run_sync_round(
                &server_proto,
                &c_proto,
                &server_id,
                &c_id,
                &server_conn_c,
                &c_conn,
            )
            .await;
            run_sync_round(
                &c_proto,
                &server_proto,
                &c_id,
                &server_id,
                &c_conn,
                &server_conn_c,
            )
            .await;
        }

        let c = c_kh.lock().await;
        assert!(
            c.get_document(doc_id).await.is_some(),
            "C should have the doc"
        );
        drop(c);

        let snapshot = c_proto.all_agent_events(&BTreeSet::new()).await.unwrap();
        let public_peer = KeyhivePeerId::from_identifier(&Public.id());
        let public_hashes = snapshot.hashes_for(&public_peer).unwrap();

        let mut events: alloc::collections::BTreeMap<[u8; 32], StaticEvent<[u8; 32]>> =
            alloc::collections::BTreeMap::new();
        for h in public_hashes {
            if let Some((bytes, _)) = snapshot.event_data.get(h) {
                events.insert(*h, bincode_deserialize(bytes).unwrap());
            }
        }

        let hash_set: alloc::collections::BTreeSet<[u8; 32]> =
            public_hashes.iter().copied().collect();
        let builtin = [Public.id()];
        assert_event_deps_complete(
            &hash_set,
            &events,
            &builtin,
            "fresh client C after revocation chain",
        );
    }

    /// Verifies the `all_agent_events()` cache path (used by the
    /// `PeriodicEventCache`) produces a dependency-
    /// complete public hash set after the revocation chain synced
    /// through the protocol.
    #[tokio::test(flavor = "current_thread")]
    async fn all_agent_events_complete_after_protocol_sync() {
        use keyhive_core::principal::{
            agent::Agent, identifier::Identifier, membered::Membered, public::Public,
        };

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_server = make_keyhive().await;

        exchange_all_contact_cards(&[&kh_a, &kh_b]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_b, &kh_server]).await;

        let a_id = keyhive_peer_id(&kh_a);
        let b_id = keyhive_peer_id(&kh_b);
        let server_id = keyhive_peer_id(&kh_server);

        let b_identifier: Identifier = kh_b.id().into();
        let b_agent = kh_a.get_agent(b_identifier).await.unwrap();

        let doc = kh_a
            .generate_doc(vec![], nonempty![[0u8; 32]])
            .await
            .unwrap();
        let doc_id = doc.lock().await.doc_id();
        let membered = Membered::Document(doc_id, doc.clone());
        let public_agent: Agent<_, _, _, _> = Public.individual().into();

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (server_proto, _server_kh, _) = make_protocol_with_shared_keyhive(kh_server).await;

        let (a_conn, server_conn_a) = create_channel_pair(a_id.clone(), &server_id);
        a_proto.add_peer(server_id.clone(), a_conn.clone()).await;
        server_proto
            .add_peer(a_id.clone(), server_conn_a.clone())
            .await;

        let (b_conn, server_conn_b) = create_channel_pair(b_id.clone(), &server_id);
        b_proto.add_peer(server_id.clone(), b_conn.clone()).await;
        server_proto
            .add_peer(b_id.clone(), server_conn_b.clone())
            .await;

        macro_rules! sync_a_server {
            () => {
                sync_pair_rounds(
                    &a_proto,
                    &server_proto,
                    &a_id,
                    &server_id,
                    &a_conn,
                    &server_conn_a,
                    3,
                )
                .await
            };
        }
        macro_rules! sync_b_server {
            () => {
                sync_pair_rounds(
                    &b_proto,
                    &server_proto,
                    &b_id,
                    &server_id,
                    &b_conn,
                    &server_conn_b,
                    3,
                )
                .await
            };
        }

        sync_a_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(Public.id(), true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(b_agent, &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(b_identifier, true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        let snapshot = server_proto
            .all_agent_events(&BTreeSet::new())
            .await
            .unwrap();
        let public_peer = KeyhivePeerId::from_identifier(&Public.id());
        let public_hashes = snapshot.hashes_for(&public_peer).unwrap();

        let mut events: alloc::collections::BTreeMap<[u8; 32], StaticEvent<[u8; 32]>> =
            alloc::collections::BTreeMap::new();
        for h in public_hashes {
            if let Some((bytes, _)) = snapshot.event_data.get(h) {
                events.insert(*h, bincode_deserialize(bytes).unwrap());
            }
        }

        let hash_set: alloc::collections::BTreeSet<[u8; 32]> =
            public_hashes.iter().copied().collect();
        let builtin = [Public.id()];
        assert_event_deps_complete(
            &hash_set,
            &events,
            &builtin,
            "all_agent_events (after protocol sync)",
        );
    }

    /// `all_agent_events()` public hash set is dep-complete across four docs
    /// with varied delegation patterns (matches typical real-world TPW scenario).
    ///
    /// Doc 1: no extra members (baseline).
    /// Doc 2: made public, never revoked.
    /// Doc 3: made public then revoked.
    /// Doc 4: full chain: make public → revoke → add B → revoke B → make public.
    #[tokio::test(flavor = "current_thread")]
    async fn all_agent_events_complete_multiple_docs() {
        use keyhive_core::principal::{
            agent::Agent, identifier::Identifier, membered::Membered, public::Public,
        };

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_server = make_keyhive().await;

        exchange_all_contact_cards(&[&kh_a, &kh_b]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_b, &kh_server]).await;

        let a_id = keyhive_peer_id(&kh_a);
        let b_id = keyhive_peer_id(&kh_b);
        let server_id = keyhive_peer_id(&kh_server);

        let b_identifier: Identifier = kh_b.id().into();
        let b_agent = kh_a.get_agent(b_identifier).await.unwrap();

        // Create all four docs before wrapping in protocols.
        let _doc1 = kh_a
            .generate_doc(vec![], nonempty![[1u8; 32]])
            .await
            .unwrap();
        let doc2 = kh_a
            .generate_doc(vec![], nonempty![[2u8; 32]])
            .await
            .unwrap();
        let doc3 = kh_a
            .generate_doc(vec![], nonempty![[3u8; 32]])
            .await
            .unwrap();
        let doc4 = kh_a
            .generate_doc(vec![], nonempty![[4u8; 32]])
            .await
            .unwrap();

        let doc2_id = doc2.lock().await.doc_id();
        let doc3_id = doc3.lock().await.doc_id();
        let doc4_id = doc4.lock().await.doc_id();

        let membered2 = Membered::Document(doc2_id, doc2.clone());
        let membered3 = Membered::Document(doc3_id, doc3.clone());
        let membered4 = Membered::Document(doc4_id, doc4.clone());

        let public_agent: Agent<_, _, _, _> = Public.individual().into();

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (server_proto, _server_kh, _) = make_protocol_with_shared_keyhive(kh_server).await;

        let (a_conn, server_conn_a) = create_channel_pair(a_id.clone(), &server_id);
        a_proto.add_peer(server_id.clone(), a_conn.clone()).await;
        server_proto
            .add_peer(a_id.clone(), server_conn_a.clone())
            .await;

        let (b_conn, server_conn_b) = create_channel_pair(b_id.clone(), &server_id);
        b_proto.add_peer(server_id.clone(), b_conn.clone()).await;
        server_proto
            .add_peer(b_id.clone(), server_conn_b.clone())
            .await;

        // Doc 1: no changes — just sync the baseline.
        macro_rules! sync_a_server {
            () => {
                sync_pair_rounds(
                    &a_proto,
                    &server_proto,
                    &a_id,
                    &server_id,
                    &a_conn,
                    &server_conn_a,
                    3,
                )
                .await
            };
        }
        macro_rules! sync_b_server {
            () => {
                sync_pair_rounds(
                    &b_proto,
                    &server_proto,
                    &b_id,
                    &server_id,
                    &b_conn,
                    &server_conn_b,
                    3,
                )
                .await
            };
        }

        sync_a_server!();

        // Doc 2: make public, never revoke.
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered2, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        // Doc 3: make public then revoke.
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered3, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(Public.id(), true, &membered3)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        // Doc 4: full chain.
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered4, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(Public.id(), true, &membered4)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(b_agent, &membered4, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(b_identifier, true, &membered4)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered4, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        let snapshot = server_proto
            .all_agent_events(&BTreeSet::new())
            .await
            .unwrap();
        let public_peer = KeyhivePeerId::from_identifier(&Public.id());
        let public_hashes = snapshot.hashes_for(&public_peer).unwrap();

        let mut events: alloc::collections::BTreeMap<[u8; 32], StaticEvent<[u8; 32]>> =
            alloc::collections::BTreeMap::new();
        for h in public_hashes {
            if let Some((bytes, _)) = snapshot.event_data.get(h) {
                events.insert(*h, bincode_deserialize(bytes).unwrap());
            }
        }

        let hash_set: alloc::collections::BTreeSet<[u8; 32]> =
            public_hashes.iter().copied().collect();
        let builtin = [Public.id()];
        assert_event_deps_complete(&hash_set, &events, &builtin, "all_agent_events multi-doc");
    }

    /// CGKA ops generated by the delegation chain (add/revoke members) are
    /// present and dependency-complete in the `all_agent_events()` public hash
    /// set after sync through the protocol. This is a regression test for the
    /// class of bugs where per-agent event sets silently drop CGKA ops
    /// associated with revoked agents.
    #[tokio::test(flavor = "current_thread")]
    async fn all_agent_events_includes_cgka_ops_after_revocation() {
        use keyhive_core::principal::{
            agent::Agent, identifier::Identifier, membered::Membered, public::Public,
        };

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_server = make_keyhive().await;

        exchange_all_contact_cards(&[&kh_a, &kh_b]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_b, &kh_server]).await;

        let a_id = keyhive_peer_id(&kh_a);
        let b_id = keyhive_peer_id(&kh_b);
        let server_id = keyhive_peer_id(&kh_server);

        let b_identifier: Identifier = kh_b.id().into();
        let b_agent = kh_a.get_agent(b_identifier).await.unwrap();

        let doc = kh_a
            .generate_doc(vec![], nonempty![[0u8; 32]])
            .await
            .unwrap();
        let doc_id = doc.lock().await.doc_id();
        let membered = Membered::Document(doc_id, doc.clone());
        let public_agent: Agent<_, _, _, _> = Public.individual().into();

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (server_proto, _server_kh, _) = make_protocol_with_shared_keyhive(kh_server).await;

        let (a_conn, server_conn_a) = create_channel_pair(a_id.clone(), &server_id);
        a_proto.add_peer(server_id.clone(), a_conn.clone()).await;
        server_proto
            .add_peer(a_id.clone(), server_conn_a.clone())
            .await;

        let (b_conn, server_conn_b) = create_channel_pair(b_id.clone(), &server_id);
        b_proto.add_peer(server_id.clone(), b_conn.clone()).await;
        server_proto
            .add_peer(b_id.clone(), server_conn_b.clone())
            .await;

        // Full delegation chain — each step generates CGKA Add/Remove ops implicitly.
        // Adding and revoking members creates CgkaOperation::Add and ::Remove events
        // in the document's CGKA tree.
        macro_rules! sync_a_server {
            () => {
                sync_pair_rounds(
                    &a_proto,
                    &server_proto,
                    &a_id,
                    &server_id,
                    &a_conn,
                    &server_conn_a,
                    3,
                )
                .await
            };
        }
        macro_rules! sync_b_server {
            () => {
                sync_pair_rounds(
                    &b_proto,
                    &server_proto,
                    &b_id,
                    &server_id,
                    &b_conn,
                    &server_conn_b,
                    3,
                )
                .await
            };
        }

        sync_a_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(Public.id(), true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(b_agent, &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .revoke_member(b_identifier, true, &membered)
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();
        {
            a_kh.lock()
                .await
                .add_member(public_agent.clone(), &membered, Access::Edit, &[])
                .await
                .unwrap();
        }
        sync_a_server!();
        sync_b_server!();

        let snapshot = server_proto
            .all_agent_events(&BTreeSet::new())
            .await
            .unwrap();
        let public_peer = KeyhivePeerId::from_identifier(&Public.id());
        let public_hashes = snapshot.hashes_for(&public_peer).unwrap();

        let mut events: alloc::collections::BTreeMap<[u8; 32], StaticEvent<[u8; 32]>> =
            alloc::collections::BTreeMap::new();
        for h in public_hashes {
            if let Some((bytes, _)) = snapshot.event_data.get(h) {
                events.insert(*h, bincode_deserialize(bytes).unwrap());
            }
        }

        let hash_set: alloc::collections::BTreeSet<[u8; 32]> =
            public_hashes.iter().copied().collect();
        let builtin = [Public.id()];

        // Standard delegation/revocation dep check.
        assert_event_deps_complete(
            &hash_set,
            &events,
            &builtin,
            "all_agent_events (cgka regression)",
        );

        // Additional check: every CgkaOperation event in the public set must
        // have at least one Delegated event also present in the set (confirming
        // the doc was delegated and the CGKA tree was bootstrapped). This guards
        // against the class of bug where CGKA ops exist in the keyhive but are
        // dropped from the per-agent event set during serialization.
        let cgka_count = events
            .values()
            .filter(|e| matches!(e, StaticEvent::CgkaOperation(_)))
            .count();
        if cgka_count > 0 {
            let has_any_delegation = events
                .values()
                .any(|e| matches!(e, StaticEvent::Delegated(_)));
            assert!(
                has_any_delegation,
                "public set has {cgka_count} CgkaOperation event(s) but no Delegated event — \
                 the CGKA ops are present but their bootstrapping delegation is missing",
            );
        }
        // At least some CGKA ops must be present after the delegation chain
        // (guards against the set being silently empty).
        assert!(
            cgka_count > 0,
            "expected at least one CgkaOperation in the public event set after \
             the add/revoke delegation chain, but found none"
        );
    }

    // ── wrap_as_cbor_byte_string boundary tests ──────────────────────────

    /// Helper: decode a CBOR byte string using ciborium and return the inner bytes.
    fn ciborium_decode_byte_string(cbor: &[u8]) -> Vec<u8> {
        let value: ciborium::Value = ciborium::from_reader(cbor).expect("ciborium decode");
        match value {
            ciborium::Value::Bytes(b) => b,
            other => panic!("expected CBOR Bytes, got: {other:?}"),
        }
    }

    #[test]
    fn wrap_cbor_len_0() {
        let input = vec![];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x40, "len 0: initial byte should be 0x40");
        assert_eq!(cbor.len(), 1);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_23() {
        let input = vec![0xAB; 23];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x40 | 23, "len 23: initial byte should be 0x57");
        assert_eq!(cbor.len(), 1 + 23);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_24() {
        let input = vec![0xCD; 24];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x58, "len 24: initial byte should be 0x58");
        assert_eq!(cbor[1], 24);
        assert_eq!(cbor.len(), 2 + 24);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_255() {
        let input = vec![0xEF; 255];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x58, "len 255: initial byte should be 0x58");
        assert_eq!(cbor[1], 255);
        assert_eq!(cbor.len(), 2 + 255);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_256() {
        let input = vec![0x11; 256];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x59, "len 256: initial byte should be 0x59");
        let encoded_len = u16::from_be_bytes([cbor[1], cbor[2]]);
        assert_eq!(encoded_len, 256);
        assert_eq!(cbor.len(), 3 + 256);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_65535() {
        let input = vec![0x22; 65535];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x59, "len 65535: initial byte should be 0x59");
        let encoded_len = u16::from_be_bytes([cbor[1], cbor[2]]);
        assert_eq!(encoded_len, 65535);
        assert_eq!(cbor.len(), 3 + 65535);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }

    #[test]
    fn wrap_cbor_len_65536() {
        let input = vec![0x33; 65536];
        let cbor = wrap_as_cbor_byte_string(&input);
        assert_eq!(cbor[0], 0x5A, "len 65536: initial byte should be 0x5A");
        let encoded_len = u32::from_be_bytes([cbor[1], cbor[2], cbor[3], cbor[4]]);
        assert_eq!(encoded_len, 65536);
        assert_eq!(cbor.len(), 5 + 65536);
        assert_eq!(ciborium_decode_byte_string(&cbor), input);
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::missing_panics_doc
)]
mod protocol_behavioural {
    use alloc::{sync::Arc, vec::Vec};

    use super::*;
    use crate::test_utils::{
        ChannelConnection, TwoPeerHarness, create_group_with_read_members,
        exchange_contact_cards_and_setup, run_sync_round,
    };

    fn deserialize_message(signed: &SignedMessage, expected_sender: &KeyhivePeerId) -> Message {
        let verified = signed
            .clone()
            .verify(expected_sender)
            .expect("verify signed message");
        ciborium::from_reader(verified.payload.as_slice()).expect("cbor decode message")
    }

    fn drain_channel(conn: &ChannelConnection, expected_sender: &KeyhivePeerId) -> Vec<Message> {
        core::iter::from_fn(|| conn.inbound_rx.try_recv().ok())
            .map(|msg| deserialize_message(&msg, expected_sender))
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn initiate_sync_with_peer_with_no_syncpoint_does_full_sync() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_proto = Arc::new(alice_proto);

        alice_proto
            .initiate_sync_with_peer(&bob_id)
            .await
            .expect("initiate_sync_with_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncRequest { .. })),
            "expected SyncRequest, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn initiate_sync_with_peer_with_syncpoint_does_sync_check() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_proto = Arc::new(alice_proto);

        alice_proto.syncpoints.lock().await.set(bob_id.clone(), 0);

        alice_proto
            .initiate_sync_with_peer(&bob_id)
            .await
            .expect("initiate_sync_with_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncCheck { .. })),
            "expected SyncCheck, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_received_resolves_via_protocol() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        let alice_proto = Arc::new(alice_proto);
        let alice_total = alice_proto.total_ops().await;
        alice_proto
            .syncpoints
            .lock()
            .await
            .set(bob_id.clone(), alice_total);

        bob_proto
            .sync_check_keyhive(&alice_id, alice_total)
            .await
            .expect("bob sync_check_keyhive");

        let inbound = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync-check");

        alice_proto
            .handle_message(&bob_id, inbound, Some(bob_conn.clone()))
            .await
            .expect("handle_message sync check");

        let alice_messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            alice_messages.is_empty(),
            "expected no outbound messages (in-sync), got {alice_messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auto_registers_unknown_peer_on_sync_check() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_id,
            bob_id,
            alice_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_proto = Arc::new(alice_proto);

        alice_proto.remove_peer(&bob_id).await;
        assert!(!alice_proto.has_peer(&bob_id).await);

        bob_proto
            .sync_check_keyhive(&alice_id, 0)
            .await
            .expect("bob sync_check_keyhive");
        let inbound = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync-check");

        alice_proto
            .handle_message(&bob_id, inbound, Some(alice_conn.clone()))
            .await
            .expect("handle_message sync check");

        assert!(
            alice_proto.has_peer(&bob_id).await,
            "auto-register should have added bob to the protocol's peer registry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cache_refresh_populates_then_early_returns_on_unchanged() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }
        run_sync_round(
            &alice_proto,
            &bob_proto,
            &alice_id,
            &bob_id,
            &alice_conn,
            &bob_conn,
        )
        .await;

        let alice_proto = Arc::new(alice_proto);

        alice_proto.refresh_cache().await.expect("first refresh");
        let (total_after_first, agent_count_after_first, public_hash_count_after_first) = {
            let cache = alice_proto.cache.lock().await;
            assert!(!cache.is_empty(), "cache populated after first refresh");
            let agent_count = cache.agent_count();
            assert!(agent_count >= 1, "at least the local agent is tracked");
            let total = cache
                .last_total_ops()
                .expect("last_total_ops should be Some after refresh");
            let public_count = cache.public_hashes().len();
            (total, agent_count, public_count)
        };

        alice_proto.refresh_cache().await.expect("second refresh");
        let cache = alice_proto.cache.lock().await;
        assert_eq!(cache.last_total_ops(), Some(total_after_first));
        assert_eq!(
            cache.agent_count(),
            agent_count_after_first,
            "agent count unchanged"
        );
        assert_eq!(
            cache.public_hashes().len(),
            public_hash_count_after_first,
            "public hash count unchanged"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preserves_other_peers_when_ingest_is_noop() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await;
        }
        run_sync_round(
            &bob_proto,
            &alice_proto,
            &bob_id,
            &alice_id,
            &bob_conn,
            &alice_conn,
        )
        .await;
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        let alice_proto = Arc::new(alice_proto);

        let carol_id = KeyhivePeerId::from_bytes([0xCC; 32]);
        alice_proto
            .syncpoints
            .lock()
            .await
            .set(carol_id.clone(), 42);

        alice_proto
            .sync_keyhive(Some(&bob_id))
            .await
            .expect("alice sync_keyhive");

        let sync_request = bob_conn
            .inbound_rx
            .try_recv()
            .expect("bob should receive sync request");
        bob_proto
            .handle_message(&alice_id, sync_request, None)
            .await
            .expect("bob handle sync request");

        let sync_response = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync response");
        alice_proto
            .handle_message(&bob_id, sync_response, None)
            .await
            .expect("alice handle sync response");

        drop(drain_channel(&bob_conn, &alice_id));
        drop(drain_channel(&alice_conn, &bob_id));

        let map = alice_proto.syncpoints.lock().await;
        assert!(
            map.get(&bob_id).is_some(),
            "alice should record a syncpoint for bob after the round"
        );
        assert_eq!(
            map.get(&carol_id),
            Some(42),
            "carol's syncpoint should survive: ingest was a no-op so \
             total_ops didn't advance and invalidation should be skipped"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalidates_other_peers_when_ops_ingested() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            bob_kh,
            alice_id,
            bob_id,
            alice_conn,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Initial sync so both sides share the baseline.
        run_sync_round(
            &bob_proto,
            &alice_proto,
            &bob_id,
            &alice_id,
            &bob_conn,
            &alice_conn,
        )
        .await;
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        // Bob creates new ops AFTER the initial sync.
        {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await;
        }

        let alice_proto = Arc::new(alice_proto);

        let carol_id = KeyhivePeerId::from_bytes([0xCC; 32]);
        alice_proto
            .syncpoints
            .lock()
            .await
            .set(carol_id.clone(), 42);

        // Alice initiates sync — Bob has new ops so Alice will ingest,
        // advancing total_ops.
        alice_proto
            .sync_keyhive(Some(&bob_id))
            .await
            .expect("alice sync_keyhive");

        let sync_request = bob_conn
            .inbound_rx
            .try_recv()
            .expect("bob should receive sync request");
        bob_proto
            .handle_message(&alice_id, sync_request, None)
            .await
            .expect("bob handle sync request");

        let sync_response = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync response");
        alice_proto
            .handle_message(&bob_id, sync_response, None)
            .await
            .expect("alice handle sync response");

        drop(drain_channel(&bob_conn, &alice_id));
        drop(drain_channel(&alice_conn, &bob_id));

        let map = alice_proto.syncpoints.lock().await;
        assert!(
            map.get(&bob_id).is_some(),
            "alice should record a syncpoint for bob after the round"
        );
        assert_eq!(
            map.get(&carol_id),
            None,
            "carol's syncpoint should be invalidated: alice ingested new \
             ops so total_ops advanced and all stale syncpoints are cleared"
        );
    }
}
