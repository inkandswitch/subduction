//! Keyhive sync protocol handler.
//!
//! This module implements the keyhive synchronization protocol, which enables
//! peers to reconcile their keyhive operations (delegations, revocations,
//! prekey operations, CGKA operations).
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
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec,
    vec::Vec,
};

use async_lock::Mutex;
use dupe::Dupe;
use keyhive_crypto::identifier::Identifier;
use sedimentree_core::codec::encode::Encode;

use crate::{
    access::KeyhiveAccess,
    all_agent_events::AllAgentEvents,
    awaiting_confirmation::AwaitingConfirmation,
    cache::PeriodicEventCache,
    collections::{Map, Set},
    connection::KeyhiveConnection,
    error::{ProtocolError, SigningError, StorageError, VerificationError},
    message::{AgentHashMap, EventHash, Message},
    peer_id::KeyhivePeerId,
    signed_message::SignedMessage,
    storage::{KeyhiveStorage, StorageHash},
    storage_ops,
    syncpoints::SyncpointMap,
};

/// The [`ProtocolError`] of a [`KeyhiveProtocol`] over the keyhive `K` and the
/// connection `Conn`.
pub type KeyhiveProtocolError<K, Conn, Async> = ProtocolError<
    <Conn as KeyhiveConnection<Async>>::SendError,
    <K as KeyhiveAccess<Async>>::Error,
>;

/// XOR a set of 32-byte op hashes into a single 32-byte digest.
///
/// An empty set yields all zeros, matching a peer that has not yet sent a
/// digest.
fn pair_set_digest<'a>(hashes: impl IntoIterator<Item = &'a EventHash>) -> EventHash {
    let mut out = [0u8; 32];
    for h in hashes {
        for (o, c) in out.iter_mut().zip(h.iter()) {
            *o ^= *c;
        }
    }
    out
}

/// How [`KeyhiveProtocol::sync_with_peer`] starts a round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// A digest check if an earlier round with the peer left a syncpoint.
    /// Otherwise, a full sync.
    Check,

    /// A full sync, comparing every event the two share.
    Full,
}

/// The outcome of handling a message, for callers that act on sync progress.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessageOutcome {
    /// Sync ops in the message increased keyhive's op count. A contact card in
    /// the message is not counted.
    pub ingested: bool,

    /// The sender showed it ingested events this side sent it, by confirming
    /// a round or, as requester, by answering this side's `SyncResponse` with
    /// `SyncOps`. Rounds in which this side sent no events are not reported
    /// and neither is a sync check that determines the two sides are in sync.
    pub confirmed: bool,
}

/// Keyhive sync protocol handler.
///
/// Manages peer connections and implements the keyhive sync protocol for
/// reconciling operations between peers. Keyhive is shared as an
/// `Arc<Mutex<K>>`, but some calls clone `K` and run after the lock is
/// released, so clones of `K` must share one keyhive's state.
pub struct KeyhiveProtocol<K, Conn, Store, Async>
where
    K: KeyhiveAccess<Async>,
    Conn: KeyhiveConnection<Async>,
    Store: KeyhiveStorage<Async>,
    Async: future_form::FutureForm,
{
    keyhive: Arc<Mutex<K>>,
    storage: Store,
    peer_id: KeyhivePeerId,
    peers: Mutex<Map<KeyhivePeerId, Conn>>,
    /// The `KH` frame of the local peer's contact card.
    contact_card: Vec<u8>,
    archive_config: Option<(usize, StorageHash)>,
    /// Whether to reload and re-ingest the store when events remain pending
    /// after ingestion.
    ///
    /// False by default. Enable it with [`Self::with_storage_recovery`].
    attempt_storage_recovery: bool,
    syncpoints: Mutex<SyncpointMap>,
    awaiting_confirmation: Mutex<AwaitingConfirmation>,
    cache: Mutex<PeriodicEventCache>,
    _marker: core::marker::PhantomData<Async>,
}

impl<K, Conn, Store, Async> core::fmt::Debug for KeyhiveProtocol<K, Conn, Store, Async>
where
    K: KeyhiveAccess<Async>,
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

impl<K, Conn, Store, Async> KeyhiveProtocol<K, Conn, Store, Async>
where
    K: KeyhiveAccess<Async> + Dupe,
    Conn: KeyhiveConnection<Async>,
    Conn::SendError: 'static,
    Store: KeyhiveStorage<Async>,
    Async: future_form::FutureForm,
{
    /// Create a new protocol handler.
    ///
    /// `contact_card` is the `KH` frame of the local peer's contact card.
    pub fn new(
        keyhive: Arc<Mutex<K>>,
        storage: Store,
        peer_id: KeyhivePeerId,
        contact_card: Vec<u8>,
    ) -> Self {
        Self {
            keyhive,
            storage,
            peer_id,
            peers: Mutex::new(Map::new()),
            contact_card,
            archive_config: None,
            attempt_storage_recovery: false,
            syncpoints: Mutex::new(SyncpointMap::new()),
            awaiting_confirmation: Mutex::new(AwaitingConfirmation::new()),
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

    /// Enable storage recovery when events remain pending after ingestion.
    #[must_use]
    pub const fn with_storage_recovery(mut self) -> Self {
        self.attempt_storage_recovery = true;
        self
    }

    /// Register a peer connection.
    pub async fn add_peer(&self, peer_id: KeyhivePeerId, conn: Conn) {
        self.peers.lock().await.insert(peer_id, conn);
    }

    /// Unregister a peer connection and drop its syncpoint and its rounds
    /// awaiting confirmation.
    pub async fn remove_peer(&self, peer_id: &KeyhivePeerId) {
        self.syncpoints.lock().await.remove(peer_id);
        self.awaiting_confirmation.lock().await.remove(peer_id);
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

    /// The `KH` frame of the local peer's contact card.
    #[must_use]
    pub fn contact_card(&self) -> &[u8] {
        &self.contact_card
    }

    /// Whether the provided peer is registered.
    pub async fn has_peer(&self, peer_id: &KeyhivePeerId) -> bool {
        self.peers.lock().await.contains_key(peer_id)
    }

    /// Sum of all keyhive op counters.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Keyhive`] if keyhive cannot count its ops.
    pub async fn total_ops(&self) -> Result<u64, KeyhiveProtocolError<K, Conn, Async>> {
        self.keyhive
            .lock()
            .await
            .total_ops()
            .await
            .map_err(ProtocolError::Keyhive)
    }

    /// Serialized events reachable by `peer_id`.
    ///
    /// Values are [`Arc`]-shared.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if `peer_id` is not a valid identifier or
    /// if keyhive cannot look up the agent or list its events.
    pub async fn get_events_for_agent(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<Option<AgentHashMap>, KeyhiveProtocolError<K, Conn, Async>> {
        let id = peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;
        let events = {
            let keyhive = self.keyhive.lock().await;
            if !keyhive
                .is_known_agent(id)
                .await
                .map_err(ProtocolError::Keyhive)?
            {
                return Ok(None);
            }
            keyhive
                .events_for_agent(id, None)
                .await
                .map_err(ProtocolError::Keyhive)?
        };

        // Shared, so the periodic cache and every served response reference one
        // copy of each event's bytes.
        Ok(Some(
            events
                .into_iter()
                .map(|(h, bytes)| (h, Arc::from(bytes)))
                .collect(),
        ))
    }

    /// Bulk snapshot of every agent's reachable events, deduplicated.
    ///
    /// Hashes in `skip_serialization` are still recorded in the per-agent
    /// hash sets but their bytes are not serialized (the caller already
    /// has them cached).
    ///
    /// # Errors
    /// Returns [`ProtocolError::Keyhive`] if keyhive cannot list its events.
    #[allow(clippy::too_many_lines)]
    pub async fn all_agent_events(
        &self,
        skip_serialization: &BTreeSet<EventHash>,
    ) -> Result<AllAgentEvents, KeyhiveProtocolError<K, Conn, Async>> {
        let keyhive = { self.keyhive.lock().await.dupe() };

        // Keyhive computes the per-agent event sets itself, including the
        // explanations a revoked member is sent. It leaves out the events
        // whose hashes the caller already has cached.
        let all = keyhive
            .all_agent_events(skip_serialization)
            .await
            .map_err(ProtocolError::Keyhive)?;

        let event_data: BTreeMap<EventHash, Arc<[u8]>> = all
            .events
            .into_iter()
            .map(|(h, bytes)| (h, Arc::from(bytes)))
            .collect();

        let agent_hashes: BTreeMap<KeyhivePeerId, BTreeSet<EventHash>> = all
            .agent_hashes
            .into_iter()
            .map(|(agent, hashes)| (KeyhivePeerId::from_bytes(agent), hashes))
            .collect();

        Ok(AllAgentEvents {
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
    #[tracing::instrument(name = "keyhive_sync", level = "debug", skip_all, fields(target = ?target))]
    pub async fn sync_keyhive(
        &self,
        target: Option<&KeyhivePeerId>,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
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
            let pair = if let Some(c) = cached.as_deref() {
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
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let (our_total, our_digest) = self.compute_total_and_digest_for_peer(target).await?;
        let msg = Message::SyncCheck {
            sender_id: self.peer_id.clone(),
            target_id: target.clone(),
            sender_total: our_total,
            sender_syncpoint: our_syncpoint_for_target,
            sender_digest: our_digest,
        };
        self.sign_and_send(target, msg, false).await
    }

    async fn resolve_sync_check(
        &self,
        peer: &KeyhivePeerId,
        sender_total: u64,
        sender_syncpoint: u64,
        sender_digest: EventHash,
        local_syncpoint_for_sender: u64,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let (our_total, our_digest) = self.compute_total_and_digest_for_peer(peer).await?;

        let digests_match = sender_digest == our_digest;
        let in_sync = local_syncpoint_for_sender == sender_total
            && sender_syncpoint == our_total
            && digests_match;

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
                digests_match,
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
    /// ingestion, or message handling fails. Events can already be ingested
    /// when an error is returned, e.g., when the reply cannot be sent.
    #[tracing::instrument(name = "keyhive_handle_message", level = "debug", skip_all, fields(peer = %from))]
    pub async fn handle_message(
        &self,
        from: &KeyhivePeerId,
        signed_msg: SignedMessage,
        conn: Option<Conn>,
    ) -> Result<MessageOutcome, KeyhiveProtocolError<K, Conn, Async>> {
        let cached_sender_pair = self.cached_events_for_pair_with_peer(from).await;
        let verified = signed_msg.verify(from)?;

        if let Some(contact_card) = &verified.contact_card {
            self.ingest_contact_card(contact_card).await?;
        }

        let message = Message::try_decode(&verified.payload).map_err(VerificationError::Payload)?;

        // Reject messages where the inner sender_id doesn't match the signer.
        if !message.sender_id().same_identity(&verified.sender_id) {
            return Err(VerificationError::SenderMismatch {
                expected: verified.sender_id.clone(),
                actual: message.sender_id().clone(),
            }
            .into());
        }

        match &message {
            Message::SyncRequest { .. } => self
                .handle_sync_request(message, cached_sender_pair.as_deref())
                .await
                .map(|()| MessageOutcome::default()),
            Message::SyncResponse { .. } => {
                self.handle_sync_response(message, cached_sender_pair.as_deref())
                    .await
            }
            Message::SyncOps { .. } => self.handle_sync_ops(message).await,
            Message::RequestContactCard { .. } => self
                .handle_request_contact_card(message)
                .await
                .map(|()| MessageOutcome::default()),
            Message::MissingContactCard { .. } => self
                .handle_missing_contact_card(message)
                .await
                .map(|()| MessageOutcome::default()),
            Message::SyncCheck {
                sender_id,
                sender_total,
                sender_syncpoint,
                sender_digest,
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
                    *sender_digest,
                    local_syncpoint_for_sender,
                )
                .await
                .map(|()| MessageOutcome::default())
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
                let confirmed = self.awaiting_confirmation.lock().await.end_round(sender_id);
                Ok(MessageOutcome {
                    ingested: false,
                    confirmed,
                })
            }
        }
    }

    /// Handle a `SyncRequest`: compute which ops to send and which to request.
    async fn handle_sync_request(
        &self,
        message: Message,
        cached_sender_pair: Option<&AgentHashMap>,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
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
        let our_pending_set: Set<EventHash> = our_pending_hashes.iter().copied().collect();

        // Ops to send = local - (peer_found U peer_pending)
        let mut found_ops: Vec<Arc<[u8]>> = Vec::new();
        for (h, event_bytes) in local_events {
            if !peer_found_set.contains(h) && !peer_pending_set.contains(h) {
                found_ops.push(event_bytes.dupe());
            }
        }

        // Ops to request = peer_found - (local U our_pending)
        let requested: Vec<EventHash> = peer_found
            .into_iter()
            .filter(|h| !local_events.contains_key(h) && !our_pending_set.contains(h))
            .collect();

        tracing::debug!(
            from = %sender_id,
            sending = found_ops.len(),
            requesting = requested.len(),
            "sending sync response"
        );

        let sends_events = !found_ops.is_empty();
        let response = Message::SyncResponse {
            sender_id: self.peer_id.clone(),
            target_id: sender_id.clone(),
            requested,
            found: found_ops,
            sync_responder_total,
            sync_requester_total,
        };

        if sends_events {
            self.send_events(&sender_id, response).await
        } else {
            self.sign_and_send(&sender_id, response, false).await
        }
    }

    async fn send_events(
        &self,
        peer: &KeyhivePeerId,
        message: Message,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        self.awaiting_confirmation
            .lock()
            .await
            .record_round(peer.clone());
        let sent = self.sign_and_send(peer, message, false).await;
        if sent.is_err() {
            self.awaiting_confirmation.lock().await.end_round(peer);
        }
        sent
    }

    /// Handle a `SyncResponse`, ingest events we received, and send any
    /// requested ops back.
    ///
    /// If ops are sent back, the other side will confirm in `handle_sync_ops`.
    /// If no ops are sent, we send a confirmation and establish a syncpoint.
    async fn handle_sync_response(
        &self,
        message: Message,
        cached_sender_pair: Option<&AgentHashMap>,
    ) -> Result<MessageOutcome, KeyhiveProtocolError<K, Conn, Async>> {
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

        let advanced = self.ingest_and_compare(&found_events).await?;
        let outcome = MessageOutcome {
            ingested: advanced,
            confirmed: false,
        };

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

                self.send_events(&sender_id, msg).await?;
                // The other side will send a confirmation after ingesting.
                if advanced {
                    self.syncpoints.lock().await.invalidate_all();
                }
                return Ok(outcome);
            }
        }

        // No ops to send back. Send confirmation and establish syncpoint.
        // Our total is `sync_requester_total` (we are the requester).
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

        Ok(outcome)
    }

    /// Handle `SyncOps`, ingest received operations, and send confirmation.
    ///
    /// The requester sends `SyncOps` only after ingesting this side's
    /// `SyncResponse`, so it also confirms that response's round.
    async fn handle_sync_ops(
        &self,
        message: Message,
    ) -> Result<MessageOutcome, KeyhiveProtocolError<K, Conn, Async>> {
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

        let confirmed = self
            .awaiting_confirmation
            .lock()
            .await
            .end_round(&sender_id);
        let advanced = self.ingest_and_compare(&ops).await?;
        let outcome = MessageOutcome {
            ingested: advanced,
            confirmed,
        };

        // Send confirmation after ingesting ops.
        // Our total is `sync_responder_total` (we are the responder).
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

        Ok(outcome)
    }

    /// Handle `RequestContactCard` and send our contact card to the requesting
    /// peer.
    async fn handle_request_contact_card(
        &self,
        message: Message,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
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

    /// Handle `MissingContactCard`. The contact card was already ingested in
    /// `handle_message`, so now we trigger a sync with the peer.
    async fn handle_missing_contact_card(
        &self,
        message: Message,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
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

    /// Encode a message, sign it, and wrap it as a [`SignedMessage`].
    pub(crate) async fn build_signed_message(
        &self,
        message: Message,
        include_contact_card: bool,
    ) -> Result<SignedMessage, KeyhiveProtocolError<K, Conn, Async>> {
        let msg_bytes = message.encode();

        let signed_bytes = {
            let keyhive = { self.keyhive.lock().await.dupe() };
            keyhive
                .sign_bytes(msg_bytes)
                .await
                .map_err(SigningError::SigningFailed)?
        };

        Ok(if include_contact_card {
            SignedMessage::with_contact_card(signed_bytes, self.contact_card.clone())
        } else {
            SignedMessage::new(signed_bytes)
        })
    }

    /// Sign a message and send it to a peer.
    async fn sign_and_send(
        &self,
        target_peer_id: &KeyhivePeerId,
        message: Message,
        include_contact_card: bool,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let signed_message = self
            .build_signed_message(message, include_contact_card)
            .await?;

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
    ) -> Result<Option<AgentHashMap>, KeyhiveProtocolError<K, Conn, Async>> {
        let our_id = self
            .peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;
        let their_id = peer_id
            .to_identifier()
            .map_err(ProtocolError::InvalidIdentifier)?;

        let shared = {
            let keyhive = { self.keyhive.lock().await.dupe() };

            if !keyhive
                .is_known_agent(our_id)
                .await
                .map_err(ProtocolError::Keyhive)?
                || !keyhive
                    .is_known_agent(their_id)
                    .await
                    .map_err(ProtocolError::Keyhive)?
            {
                return Ok(None);
            }

            // Only the events both are sent are returned, so an implementation
            // need not encode or copy the rest.
            let their_hashes = keyhive
                .event_hashes_for_agent(their_id)
                .await
                .map_err(ProtocolError::Keyhive)?;
            keyhive
                .events_for_agent(our_id, Some(&their_hashes))
                .await
                .map_err(ProtocolError::Keyhive)?
        };

        Ok(Some(
            shared
                .into_iter()
                .map(|(h, bytes)| (h, Arc::from(bytes)))
                .collect(),
        ))
    }

    /// Get pending event hashes as `Vec<EventHash>`.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the keyhive lock or hash computation fails.
    pub async fn get_pending_hashes(
        &self,
    ) -> Result<Vec<EventHash>, KeyhiveProtocolError<K, Conn, Async>> {
        let keyhive = self.keyhive.lock().await;
        keyhive
            .pending_event_hashes()
            .await
            .map_err(ProtocolError::Keyhive)
    }

    /// Fetch serialized event bytes for the given hashes.
    async fn get_event_bytes_for_requested(
        &self,
        sender_id: &KeyhivePeerId,
        requested: &[EventHash],
        cached_pair: Option<&AgentHashMap>,
    ) -> Result<Vec<Arc<[u8]>>, KeyhiveProtocolError<K, Conn, Async>> {
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
            if let Some(bytes) = pair.get(h) {
                result.push(bytes.dupe());
            }
        }
        Ok(result)
    }

    /// Ingest `events`, returning whether keyhive's op count changed.
    async fn ingest_and_compare<B: AsRef<[u8]>>(
        &self,
        events: &[B],
    ) -> Result<bool, KeyhiveProtocolError<K, Conn, Async>> {
        if events.is_empty() {
            return Ok(false);
        }
        let total_before = self.total_ops().await?;
        self.ingest_events(events).await?;
        Ok(self.total_ops().await? != total_before)
    }

    /// Ingest received event bytes into keyhive and persist them to storage.
    ///
    /// If some events remain pending (missing dependencies), attempts recovery
    /// by loading from storage and retrying. If an event does not decode, the
    /// call fails. When keyhive reports such an event instead of failing on
    /// it, the other events are ingested and persisted before the call fails.
    async fn ingest_events<B: AsRef<[u8]>>(
        &self,
        event_bytes_list: &[B],
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let not_ingested = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .ingest_events(
                    event_bytes_list
                        .iter()
                        .map(|b| b.as_ref().to_vec())
                        .collect(),
                )
                .await
                .map_err(ProtocolError::Keyhive)?
        };
        for bytes in &not_ingested.undecodable {
            let head_len = core::cmp::min(bytes.len(), 96);
            let mut head_hex = alloc::string::String::with_capacity(head_len * 2);
            for b in bytes.get(..head_len).unwrap_or(bytes) {
                use core::fmt::Write;
                let _ = write!(head_hex, "{b:02x}");
            }
            tracing::warn!(
                total = event_bytes_list.len(),
                len = bytes.len(),
                head = %head_hex,
                "failed to deserialize incoming event"
            );
        }
        let undecodable: Set<&[u8]> = not_ingested.undecodable.iter().map(Vec::as_slice).collect();
        let decoded: Vec<&[u8]> = event_bytes_list
            .iter()
            .map(AsRef::as_ref)
            .filter(|bytes| !undecodable.contains(bytes))
            .collect();

        let pending = not_ingested.pending;
        if !pending.is_empty() {
            if self.attempt_storage_recovery {
                tracing::warn!(
                    count = pending.len(),
                    "events pending after ingestion, attempting storage recovery"
                );

                if let Err(e) = self.try_storage_recovery(&decoded).await {
                    tracing::warn!(
                        error = %e,
                        "storage recovery failed"
                    );
                }
            } else {
                tracing::warn!(
                    count = pending.len(),
                    "events pending after ingestion; storage recovery disabled"
                );
            }
        }

        let use_archive = self
            .archive_config
            .filter(|(threshold, _)| decoded.len() > *threshold);

        // The events are already applied in memory, so persist them one way or
        // another: an archive when the batch is large, otherwise (or if the
        // archive cannot be exported or saved) each event's bytes.
        let archived = match use_archive {
            Some((_, storage_id)) => self.save_archive(storage_id).await,
            None => false,
        };

        if !archived {
            for event_bytes in &decoded {
                if let Err(e) =
                    storage_ops::save_event_bytes(&self.storage, event_bytes.to_vec()).await
                {
                    tracing::warn!(error = %e, "failed to persist event to storage");
                }
            }
        }

        if undecodable.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::UndecodableEvents {
                undecodable: not_ingested.undecodable.len(),
                total: event_bytes_list.len(),
            })
        }
    }

    /// Export the Keyhive as an archive and save it at `storage_id`.
    ///
    /// Returns whether it was saved. A failure is logged rather than
    /// returned, so the caller can persist the events another way.
    ///
    /// The Keyhive stays locked until the save finishes, as in
    /// [`Self::compact`], so archives are saved in the order they were
    /// exported. Otherwise an older export could be saved over a newer one,
    /// and the events only the newer one holds (or that compaction deleted
    /// after folding them into it) would be lost.
    async fn save_archive(&self, storage_id: StorageHash) -> bool {
        let keyhive = self.keyhive.lock().await;
        let archive = match keyhive.to_archive().await {
            Ok(archive) => archive,
            Err(error) => {
                tracing::warn!(%error, "failed to export keyhive archive; saving events individually");
                return false;
            }
        };
        match storage_ops::replace_keyhive_archive(&*keyhive, &self.storage, storage_id, archive)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "failed to save keyhive archive; saving events individually");
                false
            }
        }
    }

    /// Attempt to recover by ingesting from storage and retrying.
    async fn try_storage_recovery(&self, event_bytes_list: &[&[u8]]) -> Result<(), StorageError> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::ingest_from_storage(&*keyhive, &self.storage).await?;

        if !event_bytes_list.is_empty() {
            let retry_pending = keyhive
                .ingest_events(event_bytes_list.iter().map(|b| b.to_vec()).collect())
                .await
                .map_err(storage_ops::event_ingestion_error)?
                .pending;
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

    /// Ingest a contact card's `KH` frame into keyhive and persist its
    /// prekey op to storage.
    async fn ingest_contact_card(
        &self,
        contact_card: &[u8],
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        // Validate first: only persist after keyhive accepts the card.
        let event = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .receive_contact_card(contact_card)
                .await
                .map_err(ProtocolError::ReceiveContactCard)?
        };

        if let Err(e) = storage_ops::save_event_bytes(&self.storage, event).await {
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
    /// Returns [`StorageError`] if a storage operation, ingestion, or the
    /// archive export fails.
    pub async fn compact(&self, storage_id: StorageHash) -> Result<(), StorageError> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::compact(&*keyhive, &self.storage, storage_id).await
    }

    /// Load and ingest all stored archives and events.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if loading or ingestion fails.
    pub async fn ingest_from_storage(&self) -> Result<(), StorageError> {
        let keyhive = self.keyhive.lock().await;
        storage_ops::ingest_from_storage(&*keyhive, &self.storage).await?;
        Ok(())
    }

    /// Look up cached events for us paired with the provided peer
    async fn cached_events_for_pair_with_peer(
        &self,
        peer: &KeyhivePeerId,
    ) -> Option<Arc<AgentHashMap>> {
        let cache = self.cache.lock().await;
        let local = self.peer_id();
        let map = cache.events_for_peer_pair(&local, peer);
        (!map.is_empty()).then_some(map)
    }

    /// Compute the per-pair total op count and a digest of the pair's op-hash
    /// set.
    ///
    /// The total is the number of intersection hashes plus pending hashes. The
    /// digest is an order-independent XOR of the intersection hashes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if hash or pending computation fails.
    async fn compute_total_and_digest_for_peer(
        &self,
        peer: &KeyhivePeerId,
    ) -> Result<(u64, EventHash), KeyhiveProtocolError<K, Conn, Async>> {
        let (hash_count, digest) =
            if let Some(cached) = self.cached_events_for_pair_with_peer(peer).await {
                (cached.len(), pair_set_digest(cached.keys()))
            } else if let Some(pair) = self.get_events_for_peer_pair(peer).await? {
                (pair.len(), pair_set_digest(pair.keys()))
            } else {
                (0, [0u8; 32])
            };
        let pending_count = self.get_pending_hashes().await?.len();
        Ok(((hash_count + pending_count) as u64, digest))
    }

    /// Start a round with `peer`, as `mode` says.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] if the protocol call fails.
    #[tracing::instrument(name = "keyhive_sync_with_peer", level = "debug", skip_all, fields(peer = %peer))]
    pub async fn sync_with_peer(
        &self,
        peer: &KeyhivePeerId,
        mode: SyncMode,
    ) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let syncpoint = match mode {
            SyncMode::Check => self.syncpoints.lock().await.get(peer),
            SyncMode::Full => None,
        };
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
    pub async fn refresh_cache(&self) -> Result<(), KeyhiveProtocolError<K, Conn, Async>> {
        let mut cache = self.cache.lock().await;
        cache.refresh(self).await.map(|_| ())
    }
}

/// Whether `keyhive` knows `id`, as [`KeyhiveAccess::is_known_agent`] says.
///
/// # Errors
///
/// Returns the keyhive's error if the lookup fails.
pub async fn is_known_agent<F, K>(keyhive: &K, id: Identifier) -> Result<bool, K::Error>
where
    F: future_form::FutureForm,
    K: KeyhiveAccess<F>,
{
    keyhive.is_known_agent(id).await
}

#[cfg(all(test, feature = "test-utils"))]
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
    use keyhive_codec::traits::{Decode, Encode};
    use keyhive_core::event::static_event::StaticEvent;
    use sedimentree_core::codec::encode::Encode as _;

    use crate::{
        message::EventBytes,
        storage::MemoryKeyhiveStorage,
        test_utils::{
            SimpleKeyhive, TestProtocol, TwoPeerHarness, create_channel_pair,
            create_group_with_read_members, exchange_all_contact_cards,
            exchange_contact_cards_and_setup, keyhive_peer_id, make_keyhive,
            make_protocol_with_shared_keyhive, run_sync_round, sync_pair_rounds,
        },
    };
    use future_form::Local;
    use keyhive_core::principal::document::id::DocumentId;
    use keyline::power::Power;
    use nonempty::nonempty;
    use testresult::TestResult;

    /// Helper to create a test protocol instance.
    async fn make_protocol() -> (TestProtocol, SimpleKeyhive) {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.generate_contact_card().await.unwrap();
        let storage = MemoryKeyhiveStorage::new();
        let shared = Arc::new(Mutex::new(keyhive.clone()));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc.to_bytes());
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
        let decoded_msg = Message::try_decode(&verified.payload).unwrap();

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
        let payload = forged.encode();
        let signed = {
            let kh = alice.keyhive.lock().await.dupe();
            kh.sign_bytes(payload).await.expect("alice signs")
        };
        let signed_bytes = signed.signed().as_ref().encode().into_bytes();
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
    async fn handle_message_rejects_malformed_contact_card() {
        let (alice, _alice_kh) = make_protocol().await;
        let (bob, _bob_kh) = make_protocol().await;
        let alice_id = alice.peer_id.clone();

        let request = Message::RequestContactCard {
            sender_id: alice_id.clone(),
            target_id: bob.peer_id.clone(),
        };
        let payload = request.encode();
        let signed = {
            let kh = alice.keyhive.lock().await.dupe();
            kh.sign_bytes(payload).await.expect("alice signs")
        };
        let signed_msg = SignedMessage::with_contact_card(
            signed.signed().as_ref().encode().into_bytes(),
            b"not a contact card".to_vec(),
        );

        let err = bob
            .handle_message(&alice_id, signed_msg, None)
            .await
            .expect_err("should reject a malformed contact card");
        assert!(
            matches!(err, ProtocolError::ReceiveContactCard(_)),
            "expected ReceiveContactCard, got: {err:?}"
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
        let decoded = Message::try_decode(&verified.payload).unwrap();

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
        let decoded1 = Message::try_decode(&verified1.payload).unwrap();
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
        let decoded2 = Message::try_decode(&verified2.payload).unwrap();
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
        let decoded3 = Message::try_decode(&verified3.payload).unwrap();

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
        let alice_cc = alice_kh.generate_contact_card().await.unwrap();
        let bob_cc = bob_kh.generate_contact_card().await.unwrap();
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
        let decoded1 = Message::try_decode(&verified1.payload).unwrap();

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
        let decoded2 = Message::try_decode(&verified2.payload).unwrap();

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
        let alice_cc = alice_kh.generate_contact_card().await.unwrap();
        let bob_cc = bob_kh.generate_contact_card().await.unwrap();
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
            alice_cc.to_bytes(),
        );
        let bob_proto = TestProtocol::new(
            bob_shared.clone(),
            bob_storage.clone(),
            bob_id.clone(),
            bob_cc.to_bytes(),
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

        let alice_cc = alice_kh.generate_contact_card().await.unwrap();
        let bob_cc = bob_kh.generate_contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        // Contact cards alone share nothing under Keyline; a shared group
        // gives Bob's agent delegations and both peers' prekey ops.
        create_group_with_read_members(&alice_kh, &[&bob_id]).await;

        let storage = MemoryKeyhiveStorage::new();
        let storage_id = crate::storage::StorageHash::new([42u8; 32]);
        let shared = Arc::new(Mutex::new(alice_kh.clone()));
        let protocol = TestProtocol::new(
            shared,
            storage.clone(),
            alice_id.clone(),
            alice_cc.to_bytes(),
        )
        .with_archive_threshold(2, storage_id);

        // Pull real event bytes off Bob's agent (these are the kind of
        // events that arrive over the wire from a real peer).
        let pair = protocol
            .get_events_for_agent(&bob_id)
            .await
            .unwrap()
            .expect("bob should resolve to an agent");
        let event_bytes: Vec<Arc<[u8]>> = pair.values().map(Dupe::dupe).collect();
        assert!(
            event_bytes.len() > 2,
            "need >2 events to exceed threshold of 2 (got {})",
            event_bytes.len()
        );

        protocol.ingest_events(&event_bytes).await.unwrap();

        let archives = crate::storage_ops::load_archives::<_, Local>(&storage)
            .await
            .unwrap();
        let events = crate::storage_ops::load_event_bytes::<_, Local>(&storage)
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

    /// How `ingest_events` persists what it applied when a batch is large
    /// enough for the archive path.
    mod archive_saves {
        use alloc::{sync::Arc, vec, vec::Vec};
        use core::{
            convert::Infallible,
            sync::atomic::{AtomicBool, Ordering},
        };

        use async_lock::Mutex;
        use dupe::Dupe;
        use future_form::Local;
        use futures::{FutureExt, future::LocalBoxFuture};
        use testresult::TestResult;

        use crate::{
            protocol::KeyhiveProtocol,
            storage::{KeyhiveStorage, MemoryKeyhiveStorage, StorageHash},
            storage_ops,
            test_utils::{
                ChannelConnection, SimpleKeyhive, create_group_with_read_members, keyhive_peer_id,
                make_keyhive,
            },
        };

        #[derive(Debug, thiserror::Error)]
        #[error("archives refused")]
        struct ArchivesRefused;

        /// What [`ArchiveTestStorage`] does when asked to save an archive.
        #[derive(Debug, Clone)]
        enum OnSaveArchive {
            /// Refuse every archive.
            Refuse,

            /// Signal `entered` and wait for `release` before saving the
            /// first archive; save the rest at once.
            HoldFirst {
                entered: async_channel::Sender<()>,
                release: async_channel::Receiver<()>,
                held: Arc<AtomicBool>,
            },
        }

        /// Storage that keeps everything, except as [`OnSaveArchive`] says.
        #[derive(Debug, Clone)]
        struct ArchiveTestStorage {
            inner: MemoryKeyhiveStorage,
            on_save_archive: OnSaveArchive,
        }

        impl ArchiveTestStorage {
            fn new(on_save_archive: OnSaveArchive) -> Self {
                Self {
                    inner: MemoryKeyhiveStorage::new(),
                    on_save_archive,
                }
            }
        }

        const fn never<T>(never: Infallible) -> T {
            match never {}
        }

        impl KeyhiveStorage<Local> for ArchiveTestStorage {
            type Error = ArchivesRefused;

            fn save_archive(
                &self,
                hash: StorageHash,
                data: Vec<u8>,
            ) -> LocalBoxFuture<'_, Result<(), ArchivesRefused>> {
                async move {
                    match &self.on_save_archive {
                        OnSaveArchive::Refuse => return Err(ArchivesRefused),
                        OnSaveArchive::HoldFirst {
                            entered,
                            release,
                            held,
                        } => {
                            if !held.swap(true, Ordering::SeqCst) {
                                entered.send(()).await.expect("the test waits for it");
                                release.recv().await.expect("the test releases it");
                            }
                        }
                    }
                    KeyhiveStorage::<Local>::save_archive(&self.inner, hash, data)
                        .await
                        .unwrap_or_else(never);
                    Ok(())
                }
                .boxed_local()
            }

            fn load_archives(
                &self,
            ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, ArchivesRefused>>
            {
                async move {
                    Ok(KeyhiveStorage::<Local>::load_archives(&self.inner)
                        .await
                        .unwrap_or_else(never))
                }
                .boxed_local()
            }

            fn delete_archive(
                &self,
                hash: StorageHash,
            ) -> LocalBoxFuture<'_, Result<(), ArchivesRefused>> {
                async move {
                    KeyhiveStorage::<Local>::delete_archive(&self.inner, hash)
                        .await
                        .unwrap_or_else(never);
                    Ok(())
                }
                .boxed_local()
            }

            fn save_event(
                &self,
                hash: StorageHash,
                data: Vec<u8>,
            ) -> LocalBoxFuture<'_, Result<(), ArchivesRefused>> {
                async move {
                    KeyhiveStorage::<Local>::save_event(&self.inner, hash, data)
                        .await
                        .unwrap_or_else(never);
                    Ok(())
                }
                .boxed_local()
            }

            fn load_events(
                &self,
            ) -> LocalBoxFuture<'_, Result<Vec<(StorageHash, Vec<u8>)>, ArchivesRefused>>
            {
                async move {
                    Ok(KeyhiveStorage::<Local>::load_events(&self.inner)
                        .await
                        .unwrap_or_else(never))
                }
                .boxed_local()
            }

            fn delete_event(
                &self,
                hash: StorageHash,
            ) -> LocalBoxFuture<'_, Result<(), ArchivesRefused>> {
                async move {
                    KeyhiveStorage::<Local>::delete_event(&self.inner, hash)
                        .await
                        .unwrap_or_else(never);
                    Ok(())
                }
                .boxed_local()
            }
        }

        type TestProtocol =
            KeyhiveProtocol<SimpleKeyhive, ChannelConnection, ArchiveTestStorage, Local>;

        /// Alice, with a group shared with Bob, and the events Bob's agent
        /// is sent: a batch over a threshold of 2.
        async fn alice_and_a_batch()
        -> TestResult<(SimpleKeyhive, TestProtocolParts, Vec<Arc<[u8]>>)> {
            let alice_kh = make_keyhive().await;
            let bob_kh = make_keyhive().await;
            let alice_id = keyhive_peer_id(&alice_kh);
            let bob_id = keyhive_peer_id(&bob_kh);
            let alice_cc = alice_kh.generate_contact_card().await?;
            let bob_cc = bob_kh.generate_contact_card().await?;
            alice_kh.receive_contact_card(&bob_cc).await?;
            bob_kh.receive_contact_card(&alice_cc).await?;
            create_group_with_read_members(&alice_kh, &[&bob_id]).await;

            let probe = TestProtocol::new(
                Arc::new(Mutex::new(alice_kh.clone())),
                ArchiveTestStorage::new(OnSaveArchive::Refuse),
                alice_id.clone(),
                alice_cc.to_bytes(),
            );
            let batch: Vec<Arc<[u8]>> = probe
                .get_events_for_agent(&bob_id)
                .await?
                .ok_or("bob should resolve to an agent")?
                .values()
                .map(Dupe::dupe)
                .collect();
            assert!(batch.len() > 2, "need a batch over the threshold");
            Ok((alice_kh, (alice_id, alice_cc.to_bytes()), batch))
        }

        type TestProtocolParts = (crate::peer_id::KeyhivePeerId, Vec<u8>);

        const STORAGE_ID: StorageHash = StorageHash::new([42u8; 32]);

        fn protocol(
            alice_kh: &SimpleKeyhive,
            (alice_id, alice_cc): TestProtocolParts,
            storage: ArchiveTestStorage,
        ) -> TestProtocol {
            TestProtocol::new(
                Arc::new(Mutex::new(alice_kh.clone())),
                storage,
                alice_id,
                alice_cc,
            )
            .with_archive_threshold(2, STORAGE_ID)
        }

        #[tokio::test(flavor = "current_thread")]
        async fn a_refused_archive_falls_back_to_saving_each_event() -> TestResult {
            let (alice_kh, parts, batch) = alice_and_a_batch().await?;
            let storage = ArchiveTestStorage::new(OnSaveArchive::Refuse);
            let protocol = protocol(&alice_kh, parts, storage.clone());

            protocol.ingest_events(&batch).await?;

            let saved = storage_ops::load_event_bytes::<_, Local>(&storage.inner).await?;
            assert_eq!(
                saved.len(),
                batch.len(),
                "every applied event is saved when the archive is refused"
            );
            Ok(())
        }

        /// Two large batches ingested at once save their archives in the
        /// order they were exported, so the newest is what storage keeps.
        #[tokio::test(flavor = "current_thread")]
        async fn concurrent_batches_keep_the_newest_archive() -> TestResult {
            let (alice_kh, parts, batch) = alice_and_a_batch().await?;
            let (entered_tx, entered) = async_channel::bounded(1);
            let (release, release_rx) = async_channel::bounded(1);
            let storage = ArchiveTestStorage::new(OnSaveArchive::HoldFirst {
                entered: entered_tx,
                release: release_rx,
                held: Arc::new(AtomicBool::new(false)),
            });
            let protocol = protocol(&alice_kh, parts, storage.clone());

            let first = protocol.ingest_events(&batch);
            let second = async {
                entered.recv().await?;
                // The Keyhive changes while the first archive is being saved,
                // so the second batch exports a newer one.
                alice_kh.generate_group(vec![]).await?;
                let release_later = async {
                    for _ in 0..100 {
                        tokio::task::yield_now().await;
                    }
                    release.send(()).await
                };
                let (ingested, released) =
                    futures::join!(protocol.ingest_events(&batch), release_later);
                released?;
                ingested?;
                TestResult::Ok(())
            };
            let (first, second) = futures::join!(first, second);
            first?;
            second?;

            let newest = alice_kh.into_archive().await?.encode_zeroizing().to_vec();
            let stored = KeyhiveStorage::<Local>::load_archives(&storage.inner)
                .await
                .unwrap_or_else(never);
            assert_eq!(stored, vec![(STORAGE_ID, newest)]);
            Ok(())
        }
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

        let alice_cc = alice_kh.generate_contact_card().await.unwrap();
        let bob_cc = bob_kh.generate_contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        let storage = MemoryKeyhiveStorage::new();
        let storage_id = crate::storage::StorageHash::new([42u8; 32]);
        let shared = Arc::new(Mutex::new(alice_kh.clone()));
        let protocol = TestProtocol::new(
            shared,
            storage.clone(),
            alice_id.clone(),
            alice_cc.to_bytes(),
        )
        .with_archive_threshold(10_000, storage_id);

        let pair = protocol
            .get_events_for_agent(&bob_id)
            .await
            .unwrap()
            .unwrap();
        let event_bytes: Vec<Arc<[u8]>> = pair.values().map(Dupe::dupe).collect();
        assert!(!event_bytes.is_empty());

        protocol.ingest_events(&event_bytes).await.unwrap();

        let archives = crate::storage_ops::load_archives::<_, Local>(&storage)
            .await
            .unwrap();
        let events = crate::storage_ops::load_event_bytes::<_, Local>(&storage)
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
    async fn ingest_events_fails_on_an_undecodable_event_and_stores_only_the_others() {
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;
        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);
        let alice_cc = alice_kh.generate_contact_card().await.unwrap();
        alice_kh
            .receive_contact_card(&bob_kh.generate_contact_card().await.unwrap())
            .await
            .unwrap();

        let storage = MemoryKeyhiveStorage::new();
        let protocol = TestProtocol::new(
            Arc::new(Mutex::new(alice_kh)),
            storage.clone(),
            alice_id,
            alice_cc.to_bytes(),
        );
        let mut event_bytes: Vec<Arc<[u8]>> = protocol
            .get_events_for_agent(&bob_id)
            .await
            .unwrap()
            .unwrap()
            .into_values()
            .collect();
        let readable: Set<Vec<u8>> = event_bytes.iter().map(|b| b.to_vec()).collect();
        assert!(!readable.is_empty());
        event_bytes.push(Arc::from(&b"not an event"[..]));

        assert!(matches!(
            protocol.ingest_events(&event_bytes).await,
            Err(ProtocolError::UndecodableEvents { .. })
        ));

        let stored: Set<Vec<u8>> = crate::storage_ops::load_event_bytes::<_, Local>(&storage)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, bytes)| bytes)
            .collect();
        assert_eq!(stored, readable);
    }

    /// Pending events reload storage only when recovery is enabled.
    ///
    /// `other` creates a document and adds the destination as a reader. The
    /// destination's storage already holds the document's certificates, but
    /// its in-memory Keyhive does not (say, after they were persisted and the
    /// process restarted). The document's CGKA ops then arrive on their own.
    /// They depend on those certificates, so they pend, and only a reload of
    /// storage resolves them. This pins both the `with_storage_recovery`
    /// wiring and the pending guard in `ingest_events`.
    #[tokio::test(flavor = "current_thread")]
    async fn ingest_events_triggers_recovery_only_when_enabled() -> TestResult {
        /// Whether `bytes` is a CGKA op.
        fn is_cgka_op(bytes: &[u8]) -> bool {
            matches!(
                StaticEvent::<[u8; 32]>::decode(bytes),
                Ok(StaticEvent::CgkaOperation(_))
            )
        }

        /// A destination that is a reader of a fresh document of `other`,
        /// with the document's certificates in storage only, returning the
        /// CGKA ops still to arrive.
        async fn reader_with_stored_certificates(
            other: &SimpleKeyhive,
            dst: &SimpleKeyhive,
            storage: &MemoryKeyhiveStorage,
        ) -> TestResult<(DocumentId, Vec<Arc<[u8]>>)> {
            exchange_all_contact_cards(&[other, dst]).await;
            let doc = other.generate_doc(vec![], nonempty![[7u8; 32]]).await?;
            let reader: Identifier = dst.id().into();
            other.add_member(reader, doc, Power::Read).await?;

            let other_id = keyhive_peer_id(other);
            let other_proto = TestProtocol::new(
                Arc::new(Mutex::new(other.clone())),
                MemoryKeyhiveStorage::new(),
                other_id.clone(),
                other.generate_contact_card().await?.to_bytes(),
            );
            let (cgka_ops, certificates): (Vec<_>, Vec<_>) = other_proto
                .get_events_for_agent(&keyhive_peer_id(dst))
                .await?
                .ok_or("the reader resolves to an agent")?
                .into_values()
                .partition(|bytes| is_cgka_op(bytes));
            for bytes in certificates {
                crate::storage_ops::save_event_bytes::<_, Local>(storage, bytes.to_vec()).await?;
            }
            Ok((doc, cgka_ops))
        }

        let other = make_keyhive().await;

        // Recovery disabled: the ops stay pending and the document stays
        // unknown.
        let dst_off = make_keyhive().await;
        let storage_off = MemoryKeyhiveStorage::new();
        let (doc_off, ops_off) =
            reader_with_stored_certificates(&other, &dst_off, &storage_off).await?;
        assert!(!ops_off.is_empty(), "a new reader should get CGKA ops");
        let proto_off = TestProtocol::new(
            Arc::new(Mutex::new(dst_off.clone())),
            storage_off,
            keyhive_peer_id(&dst_off),
            dst_off.generate_contact_card().await?.to_bytes(),
        );
        proto_off.ingest_events(&ops_off).await?;
        assert!(
            !dst_off.pending_event_hashes().await.is_empty(),
            "CGKA ops without their certificates should pend"
        );
        assert!(
            !dst_off.has_document(doc_off).await,
            "with recovery off, a pending event must not reload storage"
        );

        // Recovery enabled: the pending ops trigger a reload, which brings in
        // the certificates and resolves them.
        let dst_on = make_keyhive().await;
        let storage_on = MemoryKeyhiveStorage::new();
        let (doc_on, ops_on) =
            reader_with_stored_certificates(&other, &dst_on, &storage_on).await?;
        let proto_on = TestProtocol::new(
            Arc::new(Mutex::new(dst_on.clone())),
            storage_on,
            keyhive_peer_id(&dst_on),
            dst_on.generate_contact_card().await?.to_bytes(),
        )
        .with_storage_recovery();
        proto_on.ingest_events(&ops_on).await?;
        assert!(
            dst_on.has_document(doc_on).await,
            "with recovery on, a pending event must reload the certificates"
        );
        assert!(
            dst_on.pending_event_hashes().await.is_empty(),
            "the reload should resolve the pending ops"
        );
        Ok(())
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

        let mut cached_pair = AgentHashMap::new();
        cached_pair.insert(synthetic_hash, synthetic_bytes.clone().into());

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
            Message::try_decode(&verified.payload).expect("decode alice's outbound payload");

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
            ops[0].as_ref(),
            synthetic_bytes.as_slice(),
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
    async fn ingest_from_storage_via_protocol() {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.generate_contact_card().await.unwrap();

        let storage = MemoryKeyhiveStorage::new();

        // Save an archive
        let archive = KeyhiveAccess::<Local>::to_archive(&keyhive).await.unwrap();
        let storage_id = crate::storage::StorageHash::new([1u8; 32]);
        crate::storage_ops::save_keyhive_archive::<_, Local>(&storage, storage_id, archive)
            .await
            .unwrap();

        let shared = Arc::new(Mutex::new(keyhive));
        let protocol = TestProtocol::new(shared, storage, peer_id, cc.to_bytes());

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
            let group_id = kh.generate_group(vec![]).await.unwrap();

            let bob_identifier = bob_id.to_identifier().unwrap();
            kh.add_member(bob_identifier, group_id, Power::Read)
                .await
                .unwrap();

            (group_id, bob_identifier)
        };

        // Before sync: Bob should not have the group
        {
            let kh = bob_kh.lock().await;
            assert!(
                !kh.has_group(group_id).await.unwrap(),
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
            assert!(
                kh.has_group(group_id).await.unwrap(),
                "Bob should have the group after sync"
            );

            let members = kh.reachable_members(group_id).await.unwrap();
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
            let group_id = kh.generate_group(vec![]).await.unwrap();

            let bob_identifier = bob_id.to_identifier().unwrap();
            kh.add_member(bob_identifier, group_id, Power::Read)
                .await
                .unwrap();

            kh.generate_doc(vec![group_id.into()], nonempty![[0u8; 32]])
                .await
                .unwrap()
        };

        // Before sync: Bob should not have the document
        {
            let kh = bob_kh.lock().await;
            assert!(
                !kh.has_document(doc_id).await,
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
            assert!(
                kh.has_document(doc_id).await,
                "Bob should have the document after sync"
            );

            let reachable = kh.reachable_docs().await.unwrap();
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
            assert!(alice.has_group(alice_group_id).await.unwrap());
            assert!(
                !alice.has_group(bob_group_id).await.unwrap(),
                "Alice should not have Bob's group before sync"
            );
        }
        {
            let bob = bob_kh.lock().await;
            assert!(bob.has_group(bob_group_id).await.unwrap());
            assert!(
                !bob.has_group(alice_group_id).await.unwrap(),
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
            assert!(alice.has_group(alice_group_id).await.unwrap());
            assert!(alice.has_group(bob_group_id).await.unwrap());
        }
        {
            let bob = bob_kh.lock().await;
            assert!(bob.has_group(alice_group_id).await.unwrap());
            assert!(bob.has_group(bob_group_id).await.unwrap());
        }

        // Verify membership op counts match
        {
            let alice = alice_kh.lock().await;
            let bob = bob_kh.lock().await;

            let alice_self = alice_id.to_identifier().unwrap();
            let bob_self = bob_id.to_identifier().unwrap();

            let alice_ops = alice.certificates_for_agent(alice_self).await.unwrap();
            let bob_ops = bob.certificates_for_agent(bob_self).await.unwrap();
            assert!(
                !alice_ops.is_empty(),
                "alice should reach membership ops for both groups after sync"
            );
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
            let group_id = kh.generate_group(vec![]).await.unwrap();

            let bob_identifier = bob_id.to_identifier().unwrap();
            kh.add_member(bob_identifier, group_id, Power::Read)
                .await
                .unwrap();

            group_id
        };

        // Before first sync: Bob should not have the group
        {
            let kh = bob_kh.lock().await;
            assert!(
                !kh.has_group(group_id).await.unwrap(),
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
                kh.has_group(group_id).await.unwrap(),
                "Bob should have the group after first sync"
            );

            let bob_identifier = bob_id.to_identifier().unwrap();
            let revoked = kh.revoked_members(group_id).await.unwrap();
            assert!(
                !revoked.contains_key(&bob_identifier),
                "Bob should not be revoked before Alice revokes"
            );
        }

        // Alice revokes Bob from the group
        {
            let kh = alice_kh.lock().await;
            let bob_identifier = bob_id.to_identifier().unwrap();

            kh.revoke_member(bob_identifier, true, group_id)
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
            let revoked = bob.revoked_members(group_id).await.unwrap();

            assert!(
                revoked.contains_key(&bob_identifier),
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
                !kh.has_group(group_id).await.unwrap(),
                "Bob should not have the group before sync"
            );
        }
        {
            let kh = carol_kh.lock().await;
            assert!(
                !kh.has_group(group_id).await.unwrap(),
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
                kh.has_group(group_id).await.unwrap(),
                "Bob should have the group after Alice→Bob sync"
            );
        }
        // Carol still shouldn't have it
        {
            let kh = carol_kh.lock().await;
            assert!(
                !kh.has_group(group_id).await.unwrap(),
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
            assert!(
                kh.has_group(group_id).await.unwrap(),
                "Carol should have the group after transitive sync"
            );

            let carol_identifier = carol_id.to_identifier().unwrap();
            let members = kh.reachable_members(group_id).await.unwrap();
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

        // Both sides should have established syncpoints recording the
        // other's total from the start of the round. Alice's is non-zero
        // (the group's ops). Bob's is zero: he shared no events with Alice
        // before learning of the group, since contact cards alone share
        // nothing under Keyline.
        let alice_sp = alice_proto.syncpoints.lock().await.get(&bob_id);
        let bob_sp = bob_proto.syncpoints.lock().await.get(&alice_id);
        assert_eq!(
            alice_sp,
            Some(0),
            "alice's syncpoint for bob should be his total before the round"
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

        // The empty-requested path (Alice has nothing to send back) should
        // close via direct SyncConfirmation, establishing syncpoints on both
        // sides. Each records the other's total from the start of the round:
        // Bob's is non-zero, and Alice's is zero, because peers that have
        // only exchanged contact cards share no events under Keyline (there
        // is no public agent whose prekeys everyone sees).
        let alice_sp = alice_proto.syncpoints.lock().await.get(&bob_id);
        let bob_sp = bob_proto.syncpoints.lock().await.get(&alice_id);
        assert!(
            alice_sp.is_some_and(|v| v > 0),
            "alice should have a non-zero syncpoint for bob after the \
             empty-requested round, got: {alice_sp:?}"
        );
        assert_eq!(
            bob_sp,
            Some(0),
            "bob's syncpoint for alice should be her total before the round"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn confirmation_is_reported_only_for_a_round_in_which_this_side_sent_events() {
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
        let round = || {
            run_sync_round(
                &alice_proto,
                &bob_proto,
                &alice_id,
                &bob_id,
                &alice_conn,
                &bob_conn,
            )
        };
        round().await;
        {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await;
        }

        let (_, at_bob) = round().await;
        assert!(at_bob.confirmed, "bob sent his new ops in this round");

        // Alice sends back the ops that ingesting Bob's group produced.
        round().await;
        let (_, at_bob) = round().await;
        assert!(!at_bob.confirmed, "bob sent nothing in this round");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_events_for_agent_returns_decodable_events_for_known_peer() {
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
            .expect("alice get_events_for_agent")
            .expect("alice agent exists");
        assert!(
            !alice_hashes.is_empty(),
            "alice's agent should have membership/cgka ops after the group was created"
        );

        let not_ingested = KeyhiveAccess::<Local>::ingest_events(
            &make_keyhive().await,
            alice_hashes.values().map(|bytes| bytes.to_vec()).collect(),
        )
        .await
        .expect("ingest_events");
        assert!(not_ingested.undecodable.is_empty());

        // Symmetric: querying for a peer keyhive doesn't know about returns None.
        let unknown = KeyhivePeerId::from_bytes([0xAA; 32]);
        assert!(
            alice_proto
                .get_events_for_agent(&unknown)
                .await
                .expect("get_events_for_agent for unknown peer")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn total_ops_increases_after_local_mutation() {
        let (protocol, _) = make_protocol().await;
        let before = protocol.total_ops().await.unwrap();

        // Generate some keyhive activity by creating a group.
        {
            let kh = protocol.keyhive.lock().await;
            kh.generate_group(vec![]).await.expect("generate_group");
        }

        let after = protocol.total_ops().await.unwrap();
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

        // Under Keyline, peers share nothing until they share a group, so
        // Bob can only offer his own prekey ops once he has learned of it:
        // the second round still moves ops. The third, zero-op round
        // establishes stable syncpoints.
        for _ in 0..2 {
            run_sync_round(
                &alice_proto,
                &bob_proto,
                &alice_id,
                &bob_id,
                &alice_conn,
                &bob_conn,
            )
            .await;
        }

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

    /// `pair_set_digest` must be order-independent, reduce
    /// an empty set to all zeros, and distinguish different op-sets.
    #[test]
    fn pair_set_digest_properties() {
        let mut a: EventHash = [0u8; 32];
        a[0] = 1;
        let mut b: EventHash = [0u8; 32];
        b[1] = 1;

        // An empty set yields all zeros, matching a peer that sends no digest.
        assert_eq!(
            pair_set_digest(core::iter::empty::<&EventHash>()),
            [0u8; 32]
        );

        // Iteration order does not change the digest.
        assert_eq!(pair_set_digest([&a, &b]), pair_set_digest([&b, &a]));

        // Different op-sets produce different digests.
        assert_ne!(pair_set_digest([&a, &b]), pair_set_digest([&a]));
    }

    /// When the counts cross-match on crossed syncpoints and the digests also
    /// match, `resolve_sync_check` short-circuits without a fallback sync.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_sync_check_in_sync_when_counts_and_digest_match() {
        let TwoPeerHarness {
            alice_proto,
            alice_kh,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        // Give Alice some pair state with Bob so the digest is non-trivial.
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }
        while bob_conn.inbound_rx.try_recv().is_ok() {}

        let (our_total, our_digest) = alice_proto
            .compute_total_and_digest_for_peer(&bob_id)
            .await
            .expect("compute total and digest");

        // local_syncpoint_for_sender == sender_total and sender_syncpoint ==
        // our_total, so the counts pass; the digest matches too.
        alice_proto
            .resolve_sync_check(&bob_id, our_total, our_total, our_digest, our_total)
            .await
            .expect("resolve_sync_check");

        assert!(
            bob_conn.inbound_rx.try_recv().is_err(),
            "no fallback expected when counts and digest match"
        );
    }

    /// A digest mismatch forces a full sync even when the counts cross-match.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_sync_check_falls_back_on_digest_mismatch_despite_matching_counts() {
        let TwoPeerHarness {
            alice_proto,
            alice_kh,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }
        while bob_conn.inbound_rx.try_recv().is_ok() {}

        let (our_total, our_digest) = alice_proto
            .compute_total_and_digest_for_peer(&bob_id)
            .await
            .expect("compute total and digest");

        // Identical crossed-syncpoint counts as the in-sync case, but a digest
        // that differs by one byte.
        let mut wrong_digest = our_digest;
        wrong_digest[0] ^= 0xFF;

        alice_proto
            .resolve_sync_check(&bob_id, our_total, our_total, wrong_digest, our_total)
            .await
            .expect("resolve_sync_check");

        assert!(
            bob_conn.inbound_rx.try_recv().is_ok(),
            "digest mismatch should trigger a fallback sync"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn message_outcome_reports_new_state_and_confirmation() {
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

        create_group_with_read_members(&*alice_kh.lock().await, &[&bob_id]).await;
        create_group_with_read_members(&*bob_kh.lock().await, &[&alice_id]).await;

        let ingested_and_confirmed = MessageOutcome {
            ingested: true,
            confirmed: true,
        };
        for (expected_at_bob, expected_at_alice) in [
            (ingested_and_confirmed, ingested_and_confirmed),
            (MessageOutcome::default(), MessageOutcome::default()),
        ] {
            let (at_bob, at_alice) = run_sync_round(
                &bob_proto,
                &alice_proto,
                &bob_id,
                &alice_id,
                &bob_conn,
                &alice_conn,
            )
            .await;
            assert_eq!(at_bob, expected_at_bob);
            assert_eq!(at_alice, expected_at_alice);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn removing_a_peer_drops_its_rounds_awaiting_confirmation() {
        let (protocol, _) = make_protocol().await;
        let peer = KeyhivePeerId::from_bytes([7; 32]);
        protocol
            .awaiting_confirmation
            .lock()
            .await
            .record_round(peer.clone());
        protocol
            .awaiting_confirmation
            .lock()
            .await
            .record_round(peer.clone());

        protocol.remove_peer(&peer).await;

        assert!(!protocol.awaiting_confirmation.lock().await.end_round(&peer));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_send_leaves_no_round_awaiting_confirmation() {
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
        create_group_with_read_members(&*alice_kh.lock().await, &[&bob_id]).await;
        bob_proto.sync_keyhive(Some(&alice_id)).await.unwrap();
        let request = alice_conn.inbound_rx.recv().await.unwrap();
        bob_conn.inbound_rx.close();

        assert!(
            alice_proto
                .handle_message(&bob_id, request, None)
                .await
                .is_err()
        );

        assert!(
            !alice_proto
                .awaiting_confirmation
                .lock()
                .await
                .end_round(&bob_id)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn message_outcome_does_not_report_events_already_known() {
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
        create_group_with_read_members(&*alice_kh.lock().await, &[&bob_id]).await;

        bob_proto.sync_keyhive(Some(&alice_id)).await.unwrap();
        let request = alice_conn.inbound_rx.recv().await.unwrap();
        alice_proto
            .handle_message(&bob_id, request, None)
            .await
            .unwrap();
        let response = bob_conn.inbound_rx.recv().await.unwrap();

        let first = bob_proto
            .handle_message(&alice_id, response.clone(), None)
            .await
            .unwrap();
        let again = bob_proto
            .handle_message(&alice_id, response, None)
            .await
            .unwrap();
        assert!(first.ingested);
        assert!(!again.ingested);
    }

    // ── Revocation chain regression test ──────────────────────────────
    //
    // Scenario: A creates doc → add C → revoke C → add B → revoke B →
    // add C again. A and B sync incrementally with the server after each
    // step. Then C, a fresh client, connects to the server.
    //
    // Keyline certificates carry no proofs, so a delegation never waits on
    // a missing dependency. What remains to check is that C ends up with
    // the document and nothing left pending.
    //
    // The member-side checks after this test replace the pre-Keyline ones on
    // the public agent's served set: completeness across several documents,
    // revocations reaching the revoked, and CGKA ops reaching a late reader.

    /// A → server (with B syncing) → fresh C. Verifies C has the document
    /// and no pending events after the revocation chain.
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_client_gets_complete_deps_after_revocation_chain() -> TestResult {
        use keyhive_crypto::identifier::Identifier;
        use keyline::power::Power;

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_server = make_keyhive().await;
        let kh_c = make_keyhive().await;

        exchange_all_contact_cards(&[&kh_a, &kh_b]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_c]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_b, &kh_server]).await;
        exchange_all_contact_cards(&[&kh_server, &kh_c]).await;

        let a_id = keyhive_peer_id(&kh_a);
        let b_id = keyhive_peer_id(&kh_b);
        let server_id = keyhive_peer_id(&kh_server);
        let c_id = keyhive_peer_id(&kh_c);

        let b_identifier: Identifier = kh_b.id().into();
        let c_identifier: Identifier = kh_c.id().into();
        let server_identifier: Identifier = kh_server.id().into();

        let doc_id = kh_a.generate_doc(vec![], nonempty![[0u8; 32]]).await?;
        // A relay, as the CLI server is: it serves the document's ops without
        // holding the document, so C gets everything from a peer that can't read.
        kh_a.add_member(server_identifier, doc_id, Power::Relay)
            .await?;

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (server_proto, _, _) = make_protocol_with_shared_keyhive(kh_server).await;
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

        for (who, add) in [
            (c_identifier, true),
            (c_identifier, false),
            (b_identifier, true),
            (b_identifier, false),
            (c_identifier, true),
        ] {
            {
                let a = a_kh.lock().await.dupe();
                if add {
                    a.add_member(who, doc_id, Power::Edit).await?;
                } else {
                    a.revoke_member(who, true, doc_id).await?;
                }
            }
            sync_a_server!();
            sync_b_server!();
        }

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

        let c = c_kh.lock().await.dupe();
        assert!(c.has_document(doc_id).await, "C should have the doc");
        assert!(
            c.pending_event_hashes().await.is_empty(),
            "C should have nothing pending after the revocation chain"
        );
        Ok(())
    }

    /// A peer and a relay connected over the test channel.
    struct Link {
        peer_id: KeyhivePeerId,
        relay_id: KeyhivePeerId,
        peer_conn: crate::test_utils::ChannelConnection,
        relay_conn: crate::test_utils::ChannelConnection,
    }

    impl Link {
        async fn connect(
            peer: &TestProtocol,
            peer_id: &KeyhivePeerId,
            relay: &TestProtocol,
            relay_id: &KeyhivePeerId,
        ) -> Self {
            let (peer_conn, relay_conn) = create_channel_pair(peer_id.clone(), relay_id);
            peer.add_peer(relay_id.clone(), peer_conn.clone()).await;
            relay.add_peer(peer_id.clone(), relay_conn.clone()).await;
            Self {
                peer_id: peer_id.clone(),
                relay_id: relay_id.clone(),
                peer_conn,
                relay_conn,
            }
        }

        async fn sync(&self, peer: &TestProtocol, relay: &TestProtocol) {
            sync_pair_rounds(
                peer,
                relay,
                &self.peer_id,
                &self.relay_id,
                &self.peer_conn,
                &self.relay_conn,
                3,
            )
            .await;
        }
    }

    /// A member learns each of several documents' membership histories
    /// through a relay: nothing is left pending, and its access matches each
    /// history. Reading no access to the document it was revoked from means
    /// the revocation reached it.
    #[tokio::test(flavor = "current_thread")]
    async fn member_sees_each_document_history_through_a_relay() -> TestResult {
        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_relay = make_keyhive().await;
        exchange_all_contact_cards(&[&kh_a, &kh_b, &kh_relay]).await;
        let (a_id, b_id, relay_id) = (
            keyhive_peer_id(&kh_a),
            keyhive_peer_id(&kh_b),
            keyhive_peer_id(&kh_relay),
        );
        let b: Identifier = kh_b.id().into();
        let relay: Identifier = kh_relay.id().into();

        let new_doc = async |seed: u8| -> TestResult<DocumentId> {
            let doc = kh_a.generate_doc(vec![], nonempty![[seed; 32]]).await?;
            // A relay, as the CLI server is: it carries ops it can't read.
            kh_a.add_member(relay, doc, Power::Relay).await?;
            Ok(doc)
        };
        let never = new_doc(1).await?;
        let added = new_doc(2).await?;
        let revoked = new_doc(3).await?;
        let readded = new_doc(4).await?;

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, b_kh, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (relay_proto, _, _) = make_protocol_with_shared_keyhive(kh_relay).await;
        let a_link = Link::connect(&a_proto, &a_id, &relay_proto, &relay_id).await;
        let b_link = Link::connect(&b_proto, &b_id, &relay_proto, &relay_id).await;
        a_link.sync(&a_proto, &relay_proto).await;

        for (doc, add) in [
            (added, true),
            (revoked, true),
            (revoked, false),
            (readded, true),
            (readded, false),
            (readded, true),
        ] {
            {
                let a = a_kh.lock().await.dupe();
                if add {
                    a.add_member(b, doc, Power::Read).await?;
                } else {
                    a.revoke_member(b, true, doc).await?;
                }
            }
            a_link.sync(&a_proto, &relay_proto).await;
            b_link.sync(&b_proto, &relay_proto).await;
        }

        let b_kh = b_kh.lock().await.dupe();
        assert!(
            b_kh.pending_event_hashes().await.is_empty(),
            "B should have nothing pending"
        );
        for (doc, expected, history) in [
            (never, None, "never added"),
            (added, Some(Power::Read), "added"),
            (revoked, None, "added then revoked"),
            (readded, Some(Power::Read), "added, revoked, re-added"),
        ] {
            assert_eq!(
                b_kh.access_for_doc(b, doc).await?,
                expected,
                "B's access to the document it was {history}"
            );
        }
        Ok(())
    }

    /// After members come and go, a reader that has never synced gets every
    /// CGKA op it needs from a relay, and decrypts what was written after it
    /// joined. A served set missing any of them leaves it unable to decrypt.
    #[tokio::test(flavor = "current_thread")]
    async fn late_reader_decrypts_after_membership_churn() -> TestResult {
        const PLAINTEXT: &[u8] = b"after the churn";

        let kh_a = make_keyhive().await;
        let kh_b = make_keyhive().await;
        let kh_relay = make_keyhive().await;
        let kh_late = make_keyhive().await;
        exchange_all_contact_cards(&[&kh_a, &kh_b, &kh_relay]).await;
        exchange_all_contact_cards(&[&kh_a, &kh_late]).await;
        exchange_all_contact_cards(&[&kh_relay, &kh_late]).await;
        let (a_id, b_id, relay_id, late_id) = (
            keyhive_peer_id(&kh_a),
            keyhive_peer_id(&kh_b),
            keyhive_peer_id(&kh_relay),
            keyhive_peer_id(&kh_late),
        );
        let b: Identifier = kh_b.id().into();
        let late: Identifier = kh_late.id().into();
        let relay: Identifier = kh_relay.id().into();

        let doc = kh_a.generate_doc(vec![], nonempty![[0u8; 32]]).await?;
        kh_a.add_member(relay, doc, Power::Relay).await?;

        let (a_proto, a_kh, _) = make_protocol_with_shared_keyhive(kh_a).await;
        let (b_proto, _, _) = make_protocol_with_shared_keyhive(kh_b).await;
        let (relay_proto, _, _) = make_protocol_with_shared_keyhive(kh_relay).await;
        let (late_proto, late_kh, _) = make_protocol_with_shared_keyhive(kh_late).await;
        let a_link = Link::connect(&a_proto, &a_id, &relay_proto, &relay_id).await;
        let b_link = Link::connect(&b_proto, &b_id, &relay_proto, &relay_id).await;
        a_link.sync(&a_proto, &relay_proto).await;

        // Each add and revoke changes the document's CGKA tree.
        for add in [true, false, true, false] {
            {
                let a = a_kh.lock().await.dupe();
                if add {
                    a.add_member(b, doc, Power::Read).await?;
                } else {
                    a.revoke_member(b, true, doc).await?;
                }
            }
            a_link.sync(&a_proto, &relay_proto).await;
            b_link.sync(&b_proto, &relay_proto).await;
        }

        let sealed = {
            let a = a_kh.lock().await.dupe();
            a.add_member(late, doc, Power::Read).await?;
            a.try_encrypt_content(doc, &[9u8; 32], &[], PLAINTEXT)
                .await?
        };
        a_link.sync(&a_proto, &relay_proto).await;

        let late_link = Link::connect(&late_proto, &late_id, &relay_proto, &relay_id).await;
        late_link.sync(&late_proto, &relay_proto).await;

        let late_kh = late_kh.lock().await.dupe();
        assert!(
            late_kh.pending_event_hashes().await.is_empty(),
            "the late reader should have nothing pending"
        );
        let opened = late_kh
            .try_decrypt_content(doc, sealed.encrypted_content())
            .await?;
        assert_eq!(opened.plaintext, PLAINTEXT);
        Ok(())
    }
}

#[cfg(all(test, feature = "test-utils"))]
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
        Message::try_decode(&verified.payload).expect("decode message")
    }

    fn drain_channel(conn: &ChannelConnection, expected_sender: &KeyhivePeerId) -> Vec<Message> {
        core::iter::from_fn(|| conn.inbound_rx.try_recv().ok())
            .map(|msg| deserialize_message(&msg, expected_sender))
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_with_no_syncpoint_does_full_sync() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        alice_proto
            .sync_with_peer(&bob_id, SyncMode::Check)
            .await
            .expect("sync_with_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncRequest { .. })),
            "expected SyncRequest, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_with_syncpoint_does_sync_check() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        alice_proto.syncpoints.lock().await.set(bob_id.clone(), 0);

        alice_proto
            .sync_with_peer(&bob_id, SyncMode::Check)
            .await
            .expect("sync_with_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncCheck { .. })),
            "expected SyncCheck, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_sync_ignores_a_syncpoint() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;
        alice_proto.syncpoints.lock().await.set(bob_id.clone(), 0);

        alice_proto
            .sync_with_peer(&bob_id, SyncMode::Full)
            .await
            .expect("sync_with_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncRequest { .. })),
            "expected SyncRequest, got {messages:?}"
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

        let kh = alice_kh.lock().await.dupe();
        create_group_with_read_members(&kh, &[&bob_id]).await;
        // Two rounds: Bob can only offer his prekey ops once he has learned
        // of the shared group (Keyline shares nothing before that).
        for _ in 0..2 {
            run_sync_round(
                &alice_proto,
                &bob_proto,
                &alice_id,
                &bob_id,
                &alice_conn,
                &bob_conn,
            )
            .await;
        }
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        let alice_proto = Arc::new(alice_proto);
        let alice_total = alice_proto.total_ops().await.unwrap();
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
        let (total_after_first, agent_count_after_first) = {
            let cache = alice_proto.cache.lock().await;
            assert!(!cache.is_empty(), "cache populated after first refresh");
            let agent_count = cache.agent_count();
            assert!(agent_count >= 1, "at least the local agent is tracked");
            let total = cache
                .last_total_ops()
                .expect("last_total_ops should be Some after refresh");
            (total, agent_count)
        };

        alice_proto.refresh_cache().await.expect("second refresh");
        let cache = alice_proto.cache.lock().await;
        assert_eq!(cache.last_total_ops(), Some(total_after_first));
        assert_eq!(
            cache.agent_count(),
            agent_count_after_first,
            "agent count unchanged"
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
