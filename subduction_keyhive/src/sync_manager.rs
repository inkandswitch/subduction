//! Keyhive sync manager.
//!
//! [`KeyhiveSyncManager`] bridges keyhive's operation-level sync protocol with
//! subduction's connection and wire infrastructure. It wraps a shared
//! `Arc<Mutex<Keyhive>>` (co-owned with the application) and uses a
//! callback-based send function to exchange signed keyhive messages with peers.
//!
//! # Protocol
//!
//! The three-phase set-reconciliation protocol:
//!
//! 1. **`SyncRequest`** — initiator sends hashes of operations accessible to
//!    both peers, plus any pending hashes.
//! 2. **`SyncResponse`** — responder computes set differences, sends missing
//!    operations and a list of hashes it needs.
//! 3. **`SyncOps`** — initiator sends the requested operations.
//!
//! Contact card exchange is handled when peers don't yet know each other.
//!
//! # Mutex Discipline
//!
//! Lock keyhive, perform reads/writes, drop the lock, _then_ do connection
//! I/O. Never hold the keyhive lock across a `.send()` or `.recv()` await.

use alloc::{string::ToString, sync::Arc, vec::Vec};

use async_lock::Mutex;
use future_form::Local;
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
    error::{SigningError, StorageError, VerificationError},
    message::{EventBytes, EventHash, Message},
    peer_id::KeyhivePeerId,
    signed_message::SignedMessage,
    storage::KeyhiveStorage,
    storage_ops,
    wire::KeyhiveMessage,
};

/// Keyhive sync manager.
///
/// Bridges keyhive's operation sync protocol with subduction's transport.
/// Holds a shared `Arc<Mutex<Keyhive>>` and sends/receives keyhive messages
/// through a callback-based send function, decoupled from any specific
/// connection type.
///
/// # Ownership
///
/// The application retains its own `Arc<Mutex<Keyhive>>` clone for direct
/// keyhive mutations (create groups, add members, etc.). The sync manager
/// holds another clone for peer reconciliation and policy checks.
#[allow(clippy::type_complexity)]
pub struct KeyhiveSyncManager<Signer, T, P, C, L, R, Store>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    keyhive: Arc<Mutex<Keyhive<Signer, T, P, C, L, R>>>,
    storage: Store,
    peer_id: KeyhivePeerId,
    contact_card_bytes: Vec<u8>,

    /// Bidirectional mapping between subduction `PeerId`s (raw `[u8; 32]`) and
    /// keyhive `KeyhivePeerId`s. The raw bytes are the verifying key, which is
    /// exactly what `PeerId::as_bytes()` returns.
    peer_map: Mutex<Map<[u8; 32], KeyhivePeerId>>,
}

impl<Signer, T, P, C, L, R, Store> core::fmt::Debug
    for KeyhiveSyncManager<Signer, T, P, C, L, R, Store>
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
        f.debug_struct("KeyhiveSyncManager")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl<Signer, T, P, C, L, R, Store> KeyhiveSyncManager<Signer, T, P, C, L, R, Store>
where
    Signer: AsyncSigner + Clone,
    T: ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    /// Create a new keyhive sync manager.
    ///
    /// The caller retains their own `Arc<Mutex<Keyhive>>` clone for direct
    /// keyhive mutations.
    #[allow(clippy::type_complexity)]
    pub fn new(
        keyhive: Arc<Mutex<Keyhive<Signer, T, P, C, L, R>>>,
        storage: Store,
        peer_id: KeyhivePeerId,
        contact_card_bytes: Vec<u8>,
    ) -> Self {
        Self {
            keyhive,
            storage,
            peer_id,
            contact_card_bytes,
            peer_map: Mutex::new(Map::new()),
        }
    }

    /// Our keyhive peer ID.
    #[must_use]
    pub const fn peer_id(&self) -> &KeyhivePeerId {
        &self.peer_id
    }

    /// A reference to the shared keyhive instance.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub const fn keyhive(&self) -> &Arc<Mutex<Keyhive<Signer, T, P, C, L, R>>> {
        &self.keyhive
    }

    // ── Peer tracking ───────────────────────────────────────────────────

    /// Register a peer identity mapping.
    ///
    /// Maps a subduction `PeerId` (raw 32-byte verifying key) to a
    /// [`KeyhivePeerId`].
    pub async fn register_peer(
        &self,
        subduction_peer_bytes: [u8; 32],
        keyhive_peer_id: KeyhivePeerId,
    ) {
        self.peer_map
            .lock()
            .await
            .insert(subduction_peer_bytes, keyhive_peer_id);
    }

    /// Unregister a peer identity mapping.
    pub async fn remove_peer(&self, subduction_peer_bytes: &[u8; 32]) {
        self.peer_map.lock().await.remove(subduction_peer_bytes);
    }

    /// Look up a keyhive peer ID from a subduction peer's raw bytes.
    pub async fn keyhive_peer_id_for(
        &self,
        subduction_peer_bytes: &[u8; 32],
    ) -> Option<KeyhivePeerId> {
        self.peer_map
            .lock()
            .await
            .get(subduction_peer_bytes)
            .cloned()
    }

    // ── Outbound sync ───────────────────────────────────────────────────

    /// Initiate keyhive sync with a specific peer.
    ///
    /// The `send_fn` callback sends encoded `SUK\x00`-framed wire bytes.
    /// The caller wraps the raw bytes in `WireMessage::Keyhive` and sends
    /// them through the appropriate subduction connection.
    ///
    /// # Errors
    ///
    /// Returns [`SyncManagerError`] on signing, serialization, or send failure.
    pub async fn sync_with_peer<F, E>(
        &self,
        target_keyhive_id: &KeyhivePeerId,
        send_fn: &F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        if target_keyhive_id.same_identity(&self.peer_id) {
            return Ok(());
        }

        if let Some(hashes) = self
            .get_hashes_for_peer_pair(target_keyhive_id)
            .await
            .map_err(SyncManagerError::InvalidIdentifier)?
        {
            let found: Vec<EventHash> = hashes.keys().map(digest_to_bytes).collect();
            let pending = self.get_pending_hashes().await;

            let message = Message::SyncRequest {
                sender_id: self.peer_id.clone(),
                target_id: target_keyhive_id.clone(),
                found,
                pending,
            };

            tracing::debug!(
                target = %target_keyhive_id,
                "sending keyhive sync request"
            );

            self.sign_and_send(send_fn, message, false).await
        } else {
            tracing::debug!(
                target = %target_keyhive_id,
                "requesting contact card from peer"
            );

            let message = Message::RequestContactCard {
                sender_id: self.peer_id.clone(),
                target_id: target_keyhive_id.clone(),
            };

            self.sign_and_send(send_fn, message, true).await
        }
    }

    // ── Inbound dispatch ────────────────────────────────────────────────

    /// Handle an inbound keyhive message from a peer.
    ///
    /// `raw_wire_bytes` are the complete `SUK\x00`-framed bytes extracted
    /// from `WireMessage::Keyhive(bytes)`. The `from_keyhive_id` is the
    /// keyhive peer identity of the sender (looked up from the subduction
    /// `PeerId` via the peer map). The `send_fn` callback sends reply
    /// messages back to the peer.
    ///
    /// # Errors
    ///
    /// Returns [`SyncManagerError`] on verification, ingestion, or send
    /// failure.
    pub async fn handle_inbound<F, E>(
        &self,
        from_keyhive_id: &KeyhivePeerId,
        raw_wire_bytes: Vec<u8>,
        send_fn: F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let keyhive_msg = <KeyhiveMessage as sedimentree_core::codec::decode::Decode>::try_decode(
            &raw_wire_bytes,
        )
        .map_err(|e| SyncManagerError::Decode(e.to_string()))?;

        let signed_msg = keyhive_msg
            .into_signed()
            .map_err(|e| SyncManagerError::Decode(e.to_string()))?;

        let verified = signed_msg
            .verify(from_keyhive_id)
            .map_err(SyncManagerError::Verification)?;

        if let Some(cc_bytes) = &verified.contact_card {
            self.ingest_contact_card(cc_bytes)
                .await
                .map_err(InternalError::into_sync_manager_error)?;
        }

        let message: Message = cbor_deserialize(&verified.payload)
            .map_err(|e| SyncManagerError::Decode(e.to_string()))?;

        match &message {
            Message::SyncRequest { .. } => self.handle_sync_request(message, &send_fn).await,
            Message::SyncResponse { .. } => self.handle_sync_response(message, &send_fn).await,
            Message::SyncOps { .. } => self.handle_sync_ops(message).await,
            Message::RequestContactCard { .. } => {
                self.handle_request_contact_card(message, &send_fn).await
            }
            Message::MissingContactCard { .. } => {
                self.handle_missing_contact_card(message, &send_fn).await
            }
        }
    }

    // ── Protocol handlers ───────────────────────────────────────────────

    async fn handle_sync_request<F, E>(
        &self,
        message: Message,
        send_fn: &F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let Message::SyncRequest {
            sender_id,
            found: peer_found,
            pending: peer_pending,
            ..
        } = message
        else {
            return Err(SyncManagerError::UnexpectedMessageType {
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

        let Some(local_hashes) = self
            .get_hashes_for_peer_pair(&sender_id)
            .await
            .map_err(SyncManagerError::InvalidIdentifier)?
        else {
            tracing::debug!(from = %sender_id, "no agent found, requesting contact card");
            let msg = Message::RequestContactCard {
                sender_id: self.peer_id.clone(),
                target_id: sender_id,
            };
            return self.sign_and_send(send_fn, msg, true).await;
        };

        let our_pending_hashes = self.get_pending_hashes().await;

        let peer_found_set: Set<EventHash> = peer_found.iter().copied().collect();
        let peer_pending_set: Set<EventHash> = peer_pending.iter().copied().collect();
        let local_set: Set<EventHash> = local_hashes.keys().map(digest_to_bytes).collect();
        let our_pending_set: Set<EventHash> = our_pending_hashes.iter().copied().collect();

        // Ops to send = local - (peer_found ∪ peer_pending)
        let mut found_ops: Vec<EventBytes> = Vec::new();
        for (digest, event) in &local_hashes {
            let h = digest_to_bytes(digest);
            if !peer_found_set.contains(&h) && !peer_pending_set.contains(&h) {
                let bytes = cbor_serialize(event)
                    .map_err(|e| SyncManagerError::Serialization(e.to_string()))?;
                found_ops.push(bytes);
            }
        }

        // Ops to request = peer_found - (local ∪ our_pending)
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

        let response = Message::SyncResponse {
            sender_id: self.peer_id.clone(),
            target_id: sender_id,
            requested,
            found: found_ops,
        };

        self.sign_and_send(send_fn, response, false).await
    }

    async fn handle_sync_response<F, E>(
        &self,
        message: Message,
        send_fn: &F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let Message::SyncResponse {
            sender_id,
            requested: requested_hashes,
            found: found_events,
            ..
        } = message
        else {
            return Err(SyncManagerError::UnexpectedMessageType {
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
            self.ingest_events(&found_events)
                .await
                .map_err(|e| SyncManagerError::Decode(e.to_string()))?;
        }

        if !requested_hashes.is_empty() {
            let ops = self
                .get_event_bytes_for_hashes(&requested_hashes)
                .await
                .map_err(InternalError::into_sync_manager_error)?;

            if !ops.is_empty() {
                tracing::debug!(
                    to = %sender_id,
                    count = ops.len(),
                    "sending requested keyhive ops"
                );

                let msg = Message::SyncOps {
                    sender_id: self.peer_id.clone(),
                    target_id: sender_id,
                    ops,
                };

                self.sign_and_send(send_fn, msg, false).await?;
            }
        }

        Ok(())
    }

    async fn handle_sync_ops<E>(&self, message: Message) -> Result<(), SyncManagerError<E>>
    where
        E: core::error::Error + 'static,
    {
        let Message::SyncOps { sender_id, ops, .. } = message else {
            return Err(SyncManagerError::UnexpectedMessageType {
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
            self.ingest_events(&ops)
                .await
                .map_err(|e| SyncManagerError::Decode(e.to_string()))?;
        }

        Ok(())
    }

    async fn handle_request_contact_card<F, E>(
        &self,
        message: Message,
        send_fn: &F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let Message::RequestContactCard { sender_id, .. } = message else {
            return Err(SyncManagerError::UnexpectedMessageType {
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
            target_id: sender_id,
        };

        self.sign_and_send(send_fn, msg, true).await
    }

    async fn handle_missing_contact_card<F, E>(
        &self,
        message: Message,
        send_fn: &F,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let Message::MissingContactCard { sender_id, .. } = message else {
            return Err(SyncManagerError::UnexpectedMessageType {
                expected: "MissingContactCard",
                actual: message.variant_name(),
            });
        };

        tracing::debug!(
            from = %sender_id,
            "received contact card, initiating sync"
        );

        self.sync_with_peer(&sender_id, send_fn).await
    }

    // ── Signing + wire framing ──────────────────────────────────────────

    async fn sign_and_send<F, E>(
        &self,
        send_fn: &F,
        message: Message,
        include_contact_card: bool,
    ) -> Result<(), SyncManagerError<E>>
    where
        F: AsyncSendFn<E>,
        E: core::error::Error + 'static,
    {
        let msg_bytes =
            cbor_serialize(&message).map_err(|e| SyncManagerError::Serialization(e.to_string()))?;

        // Lock keyhive, sign, drop lock — then do I/O
        let signed: Signed<Vec<u8>> = {
            let keyhive = self.keyhive.lock().await;
            keyhive
                .try_sign(msg_bytes)
                .await
                .map_err(|e| SyncManagerError::Signing(SigningError::SigningFailed(e)))?
        };

        let signed_bytes =
            cbor_serialize(&signed).map_err(|e| SyncManagerError::Serialization(e.to_string()))?;

        let signed_message = if include_contact_card {
            SignedMessage::with_contact_card(signed_bytes, self.contact_card_bytes.clone())
        } else {
            SignedMessage::new(signed_bytes)
        };

        let keyhive_msg = KeyhiveMessage::from_signed(&signed_message)
            .map_err(|e| SyncManagerError::Serialization(e.to_string()))?;

        let wire_bytes = sedimentree_core::codec::encode::Encode::encode(&keyhive_msg);

        send_fn
            .send(wire_bytes)
            .await
            .map_err(SyncManagerError::Send)
    }

    // ── Keyhive data access ─────────────────────────────────────────────

    async fn get_hashes_for_peer_pair(
        &self,
        peer_id: &KeyhivePeerId,
    ) -> Result<Option<Map<Digest<StaticEvent<T>>, StaticEvent<T>>>, ed25519_dalek::SignatureError>
    {
        let our_id = self.peer_id.to_identifier()?;
        let their_id = peer_id.to_identifier()?;

        let keyhive = self.keyhive.lock().await;

        let our_agent = keyhive.get_agent(our_id).await;
        let their_agent = keyhive.get_agent(their_id).await;

        match (our_agent, their_agent) {
            (Some(ref our_agent), Some(ref their_agent)) => {
                let our_events = sync_events_for_agent(&keyhive, our_agent).await;
                let their_events = sync_events_for_agent(&keyhive, their_agent).await;

                let intersection = our_events
                    .into_iter()
                    .filter(|(digest, _)| their_events.contains_key(digest))
                    .collect();

                Ok(Some(intersection))
            }
            _ => Ok(None),
        }
    }

    async fn get_pending_hashes(&self) -> Vec<EventHash> {
        let keyhive = self.keyhive.lock().await;
        let digests = keyhive.pending_event_hashes().await;
        digests.into_iter().map(|d| digest_to_bytes(&d)).collect()
    }

    async fn get_event_bytes_for_hashes(
        &self,
        hashes: &[EventHash],
    ) -> Result<Vec<EventBytes>, InternalError> {
        let requested_set: Set<EventHash> = hashes.iter().copied().collect();

        let peer_id = self
            .peer_id
            .to_identifier()
            .map_err(InternalError::InvalidIdentifier)?;

        let keyhive = self.keyhive.lock().await;

        let Some(agent) = keyhive.get_agent(peer_id).await else {
            return Ok(Vec::new());
        };

        let events = sync_events_for_agent(&keyhive, &agent).await;

        let mut result = Vec::new();
        for (digest, event) in &events {
            let h = digest_to_bytes(digest);
            if requested_set.contains(&h) {
                let bytes = cbor_serialize(event).map_err(InternalError::Storage)?;
                result.push(bytes);
            }
        }

        Ok(result)
    }

    async fn ingest_events(&self, event_bytes_list: &[EventBytes]) -> Result<(), StorageError> {
        let events: Vec<StaticEvent<T>> = event_bytes_list
            .iter()
            .map(|bytes| cbor_deserialize(bytes))
            .collect::<Result<_, _>>()?;

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
            if let Err(e) = storage_ops::save_event_bytes(&self.storage, event_bytes.clone()).await
            {
                tracing::warn!(error = %e, "failed to save received keyhive event to storage");
            }
        }

        Ok(())
    }

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

    async fn ingest_contact_card(&self, cc_bytes: &[u8]) -> Result<(), InternalError> {
        let contact_card: ContactCard =
            cbor_deserialize(cc_bytes).map_err(InternalError::Storage)?;

        let keyhive = self.keyhive.lock().await;
        keyhive
            .receive_contact_card(&contact_card)
            .await
            .map_err(InternalError::ReceiveContactCard)?;

        tracing::debug!("ingested keyhive contact card");
        Ok(())
    }

    /// Compact keyhive storage.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if any storage operation fails.
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

// ── Send abstraction ────────────────────────────────────────────────────

/// Trait for async send callbacks.
///
/// The sync manager is decoupled from any specific connection type. The
/// caller provides a send function that wraps the raw `SUK\x00`-framed
/// wire bytes in `WireMessage::Keyhive` and sends them through the
/// appropriate subduction connection.
///
/// Implemented for closures `Fn(Vec<u8>) -> Future<Result<(), E>>`.
pub trait AsyncSendFn<E: core::error::Error + 'static> {
    /// Send encoded wire bytes to the peer.
    fn send(&self, wire_bytes: Vec<u8>) -> futures::future::LocalBoxFuture<'_, Result<(), E>>;
}

impl<F, Fut, E> AsyncSendFn<E> for F
where
    F: Fn(Vec<u8>) -> Fut,
    Fut: core::future::Future<Output = Result<(), E>> + 'static,
    E: core::error::Error + 'static,
{
    fn send(&self, wire_bytes: Vec<u8>) -> futures::future::LocalBoxFuture<'_, Result<(), E>> {
        use futures::FutureExt;
        (self)(wire_bytes).boxed_local()
    }
}

// ── Error types ─────────────────────────────────────────────────────────

/// Internal error used by non-generic helper methods.
///
/// Converted to [`SyncManagerError`] at call sites via
/// [`into_sync_manager_error`](InternalError::into_sync_manager_error).
#[derive(Debug, thiserror::Error)]
enum InternalError {
    #[error("invalid peer identifier")]
    InvalidIdentifier(#[source] ed25519_dalek::SignatureError),

    #[error(transparent)]
    Storage(StorageError),

    #[error("failed to receive contact card")]
    ReceiveContactCard(#[source] keyhive_core::principal::individual::ReceivePrekeyOpError),
}

impl InternalError {
    fn into_sync_manager_error<E: core::error::Error + 'static>(self) -> SyncManagerError<E> {
        match self {
            Self::InvalidIdentifier(e) => SyncManagerError::InvalidIdentifier(e),
            Self::Storage(e) => SyncManagerError::Decode(e.to_string()),
            Self::ReceiveContactCard(e) => SyncManagerError::ReceiveContactCard(e),
        }
    }
}

/// Errors from [`KeyhiveSyncManager`] operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncManagerError<E: core::error::Error + 'static> {
    /// Failed to send a message via the transport.
    #[error("send error")]
    Send(#[source] E),

    /// Message signing failed.
    #[error("signing error")]
    Signing(#[from] SigningError),

    /// Message verification failed.
    #[error("verification error")]
    Verification(#[from] VerificationError),

    /// Failed to decode a message.
    #[error("decode error: {0}")]
    Decode(alloc::string::String),

    /// Failed to serialize a message.
    #[error("serialization error: {0}")]
    Serialization(alloc::string::String),

    /// Failed to convert peer ID to identifier.
    #[error("invalid peer identifier")]
    InvalidIdentifier(#[source] ed25519_dalek::SignatureError),

    /// Failed to receive a contact card.
    #[error("failed to receive contact card")]
    ReceiveContactCard(#[source] keyhive_core::principal::individual::ReceivePrekeyOpError),

    /// Unexpected message type received.
    #[error("unexpected message type: expected {expected}, got {actual}")]
    UnexpectedMessageType {
        /// Expected variant.
        expected: &'static str,
        /// Actual variant received.
        actual: &'static str,
    },
}

// ── Helpers ─────────────────────────────────────────────────────────────

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
    #[allow(clippy::type_complexity)]
    let mut ops: Map<Digest<Event<Signer, T, L>>, Event<Signer, T, L>> = keyhive
        .membership_ops_for_agent(agent)
        .await
        .into_iter()
        .map(|(digest, op)| (digest.into(), op.into()))
        .collect();

    for key_ops in keyhive.reachable_prekey_ops_for_agent(agent).await.values() {
        for key_op in key_ops {
            let op = Event::<Signer, T, L>::from(key_op.as_ref().clone());
            ops.insert(Digest::hash(&op), op);
        }
    }

    ops.into_iter()
        .map(|(digest, event)| (digest.into(), event.into()))
        .collect()
}

fn cbor_serialize<V: serde::Serialize>(value: &V) -> Result<Vec<u8>, StorageError> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| StorageError::Serialization(e.to_string()))?;
    Ok(buf)
}

fn cbor_deserialize<V: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<V, StorageError> {
    ciborium::from_reader(bytes).map_err(|e| StorageError::Deserialization(e.to_string()))
}

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
            SimpleKeyhive, exchange_all_contact_cards, keyhive_peer_id, make_keyhive,
            serialize_contact_card,
        },
    };
    use alloc::vec;
    use async_channel::{Receiver, Sender};

    /// Test send error type.
    #[derive(Debug, Clone, thiserror::Error)]
    #[error("test send error")]
    struct TestSendError;

    /// Type alias for the test sync manager.
    type TestSyncManager = KeyhiveSyncManager<
        keyhive_core::crypto::signer::memory::MemorySigner,
        [u8; 32],
        Vec<u8>,
        keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<[u8; 32], Vec<u8>>,
        keyhive_core::listener::no_listener::NoListener,
        rand::rngs::OsRng,
        MemoryKeyhiveStorage,
    >;

    /// Build a sync manager from a keyhive instance.
    async fn make_sync_manager(
        keyhive: SimpleKeyhive,
    ) -> (TestSyncManager, Arc<Mutex<SimpleKeyhive>>) {
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.unwrap();
        let cc_bytes = serialize_contact_card(&cc);
        let storage = MemoryKeyhiveStorage::new();
        let shared = Arc::new(Mutex::new(keyhive));
        let manager = TestSyncManager::new(shared.clone(), storage, peer_id, cc_bytes);
        (manager, shared)
    }

    /// Create a send function that captures outgoing wire bytes into a channel.
    fn channel_send_fn(
        tx: Sender<Vec<u8>>,
    ) -> impl Fn(Vec<u8>) -> futures::future::LocalBoxFuture<'static, Result<(), TestSendError>>
    {
        move |bytes: Vec<u8>| {
            let tx = tx.clone();
            use futures::FutureExt;
            async move {
                tx.send(bytes).await.map_err(|_| TestSendError)?;
                Ok(())
            }
            .boxed_local()
        }
    }

    /// Create a (send_fn, receiver) pair for capturing outbound messages.
    fn make_outbound_channel() -> (
        impl Fn(Vec<u8>) -> futures::future::LocalBoxFuture<'static, Result<(), TestSendError>>,
        Receiver<Vec<u8>>,
    ) {
        let (tx, rx) = async_channel::unbounded();
        (channel_send_fn(tx), rx)
    }

    /// Run a full three-phase sync round between an initiator and a responder.
    ///
    /// Returns the number of messages exchanged.
    async fn run_sync_manager_round(
        initiator: &TestSyncManager,
        responder: &TestSyncManager,
        initiator_id: &KeyhivePeerId,
        responder_id: &KeyhivePeerId,
    ) -> usize {
        let mut messages = 0;

        // 1. Initiator sends SyncRequest (or RequestContactCard)
        let (init_send, init_outbound) = make_outbound_channel();
        initiator
            .sync_with_peer(responder_id, &init_send)
            .await
            .unwrap();

        // 2. Forward to responder
        let msg1 = init_outbound.recv().await.unwrap();
        messages += 1;

        let (resp_send, resp_outbound) = make_outbound_channel();
        responder
            .handle_inbound(initiator_id, msg1, &resp_send)
            .await
            .unwrap();

        // 3. Forward SyncResponse to initiator
        if let Ok(msg2) = resp_outbound.try_recv() {
            messages += 1;

            let (init_send2, init_outbound2) = make_outbound_channel();
            initiator
                .handle_inbound(responder_id, msg2, &init_send2)
                .await
                .unwrap();

            // 4. If initiator sent SyncOps, forward them
            if let Ok(msg3) = init_outbound2.try_recv() {
                messages += 1;
                let (resp_send2, _) = make_outbound_channel();
                responder
                    .handle_inbound(initiator_id, msg3, &resp_send2)
                    .await
                    .unwrap();
            }
        }

        messages
    }

    // ── Basic tests ─────────────────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn peer_registration_and_removal() {
        let keyhive = make_keyhive().await;
        let (manager, _) = make_sync_manager(keyhive).await;

        let bytes = [42u8; 32];
        let kh_id = KeyhivePeerId::from_bytes(bytes);

        assert!(manager.keyhive_peer_id_for(&bytes).await.is_none());

        manager.register_peer(bytes, kh_id.clone()).await;
        assert_eq!(manager.keyhive_peer_id_for(&bytes).await, Some(kh_id));

        manager.remove_peer(&bytes).await;
        assert!(manager.keyhive_peer_id_for(&bytes).await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_with_self_is_noop() {
        let keyhive = make_keyhive().await;
        let (manager, _) = make_sync_manager(keyhive).await;
        let own_id = manager.peer_id().clone();

        let (send_fn, rx) = make_outbound_channel();
        manager.sync_with_peer(&own_id, &send_fn).await.unwrap();

        assert!(rx.try_recv().is_err(), "no messages should be sent to self");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_with_unknown_peer_sends_request_contact_card() {
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;
        let bob_id = keyhive_peer_id(&bob_kh);

        let (alice_mgr, _) = make_sync_manager(alice_kh).await;

        let (send_fn, rx) = make_outbound_channel();
        alice_mgr.sync_with_peer(&bob_id, &send_fn).await.unwrap();

        // Should have sent exactly one message
        let wire_bytes = rx.recv().await.unwrap();

        // Decode the wire frame
        let keyhive_msg =
            <KeyhiveMessage as sedimentree_core::codec::decode::Decode>::try_decode(&wire_bytes)
                .unwrap();
        let signed_msg = keyhive_msg.into_signed().unwrap();

        // Should include our contact card (RequestContactCard always does)
        assert!(
            signed_msg.has_contact_card(),
            "RequestContactCard should include contact card"
        );

        // Verify and decode the inner message
        let verified = signed_msg.verify(alice_mgr.peer_id()).unwrap();
        let message: Message = ciborium::from_reader(verified.payload.as_slice()).unwrap();
        assert!(
            matches!(message, Message::RequestContactCard { .. }),
            "expected RequestContactCard, got: {message:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_with_known_peer_sends_sync_request() {
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;

        // Exchange contact cards so they know each other
        let alice_cc = alice_kh.contact_card().await.unwrap();
        let bob_cc = bob_kh.contact_card().await.unwrap();
        alice_kh.receive_contact_card(&bob_cc).await.unwrap();
        bob_kh.receive_contact_card(&alice_cc).await.unwrap();

        let bob_id = keyhive_peer_id(&bob_kh);
        let (alice_mgr, _) = make_sync_manager(alice_kh).await;

        let (send_fn, rx) = make_outbound_channel();
        alice_mgr.sync_with_peer(&bob_id, &send_fn).await.unwrap();

        let wire_bytes = rx.recv().await.unwrap();
        let keyhive_msg =
            <KeyhiveMessage as sedimentree_core::codec::decode::Decode>::try_decode(&wire_bytes)
                .unwrap();
        let signed_msg = keyhive_msg.into_signed().unwrap();
        let verified = signed_msg.verify(alice_mgr.peer_id()).unwrap();
        let message: Message = ciborium::from_reader(verified.payload.as_slice()).unwrap();

        assert!(
            matches!(message, Message::SyncRequest { .. }),
            "expected SyncRequest, got: {message:?}"
        );
    }

    // ── Contact card exchange ───────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn contact_card_exchange_enables_sync() {
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;
        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        let (alice_mgr, _) = make_sync_manager(alice_kh).await;
        let (bob_mgr, _) = make_sync_manager(bob_kh).await;

        // Alice tries to sync with Bob (doesn't know him)
        let (alice_send, alice_out) = make_outbound_channel();
        alice_mgr
            .sync_with_peer(&bob_id, &alice_send)
            .await
            .unwrap();

        // Alice sends RequestContactCard
        let msg1 = alice_out.recv().await.unwrap();

        // Bob handles it, sends MissingContactCard back
        let (bob_send, bob_out) = make_outbound_channel();
        bob_mgr
            .handle_inbound(&alice_id, msg1, &bob_send)
            .await
            .unwrap();

        let msg2 = bob_out.recv().await.unwrap();

        // Alice handles Bob's MissingContactCard (which includes his CC).
        // This triggers Alice to initiate sync, sending a SyncRequest.
        let (alice_send2, alice_out2) = make_outbound_channel();
        alice_mgr
            .handle_inbound(&bob_id, msg2, &alice_send2)
            .await
            .unwrap();

        let msg3 = alice_out2.recv().await.unwrap();

        // Verify the third message is a SyncRequest (Alice now knows Bob)
        let keyhive_msg =
            <KeyhiveMessage as sedimentree_core::codec::decode::Decode>::try_decode(&msg3).unwrap();
        let signed_msg = keyhive_msg.into_signed().unwrap();
        let verified = signed_msg.verify(&alice_id).unwrap();
        let message: Message = ciborium::from_reader(verified.payload.as_slice()).unwrap();

        assert!(
            matches!(message, Message::SyncRequest { .. }),
            "after contact card exchange, Alice should send SyncRequest, got: {message:?}"
        );
    }

    // ── Full sync convergence ───────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn bidirectional_sync_converges() {
        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;

        // Exchange CCs so they know each other
        exchange_all_contact_cards(&[&alice_kh, &bob_kh]).await;

        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        let (alice_mgr, alice_shared) = make_sync_manager(alice_kh).await;
        let (bob_mgr, bob_shared) = make_sync_manager(bob_kh).await;

        // Alice → Bob
        run_sync_manager_round(&alice_mgr, &bob_mgr, &alice_id, &bob_id).await;

        // Bob → Alice
        run_sync_manager_round(&bob_mgr, &alice_mgr, &bob_id, &alice_id).await;

        // Pending hashes should match
        let alice_pending = {
            let kh = alice_shared.lock().await;
            kh.pending_event_hashes().await
        };
        let bob_pending = {
            let kh = bob_shared.lock().await;
            kh.pending_event_hashes().await
        };
        assert_eq!(
            alice_pending, bob_pending,
            "after bidirectional sync, pending hashes should match"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_group_membership_converges() {
        use keyhive_core::{access::Access, principal::membered::Membered};

        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;

        exchange_all_contact_cards(&[&alice_kh, &bob_kh]).await;

        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        // Alice creates a group and adds Bob as a Read member
        let group_id = {
            let group = alice_kh.generate_group(vec![]).await.unwrap();
            let gid = group.lock().await.group_id();

            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = alice_kh.get_agent(bob_identifier).await.unwrap();

            alice_kh
                .add_member(
                    bob_agent,
                    &Membered::Group(gid, group.clone()),
                    Access::Read,
                    &[],
                )
                .await
                .unwrap();

            gid
        };

        let (alice_mgr, _) = make_sync_manager(alice_kh).await;
        let (bob_mgr, bob_shared) = make_sync_manager(bob_kh).await;

        // Before sync: Bob should not have the group
        {
            let kh = bob_shared.lock().await;
            assert!(
                kh.get_group(group_id).await.is_none(),
                "Bob should not have the group before sync"
            );
        }

        // Sync Alice → Bob
        run_sync_manager_round(&alice_mgr, &bob_mgr, &alice_id, &bob_id).await;

        // After sync: Bob should have the group
        {
            let kh = bob_shared.lock().await;
            let group = kh.get_group(group_id).await;
            assert!(group.is_some(), "Bob should have the group after sync");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_and_ingest_from_storage() {
        let keyhive = make_keyhive().await;
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.unwrap();
        let cc_bytes = serialize_contact_card(&cc);
        let storage = MemoryKeyhiveStorage::new();

        // Save an archive first
        let archive = keyhive.into_archive().await;
        let storage_id = crate::storage::StorageHash::new([1u8; 32]);
        crate::storage_ops::save_keyhive_archive::<_, _, Local>(&storage, storage_id, &archive)
            .await
            .unwrap();

        let shared = Arc::new(Mutex::new(keyhive));
        let manager = TestSyncManager::new(shared, storage, peer_id, cc_bytes);

        // Both operations should succeed
        assert!(manager.compact(storage_id).await.is_ok());
        assert!(manager.ingest_from_storage().await.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn divergent_groups_converge_after_bidirectional_sync() {
        use keyhive_core::{access::Access, principal::membered::Membered};

        let alice_kh = make_keyhive().await;
        let bob_kh = make_keyhive().await;

        exchange_all_contact_cards(&[&alice_kh, &bob_kh]).await;

        let alice_id = keyhive_peer_id(&alice_kh);
        let bob_id = keyhive_peer_id(&bob_kh);

        // Alice creates her group and adds Bob
        let alice_group_id = {
            let group = alice_kh.generate_group(vec![]).await.unwrap();
            let gid = group.lock().await.group_id();
            let bob_identifier = bob_id.to_identifier().unwrap();
            let bob_agent = alice_kh.get_agent(bob_identifier).await.unwrap();
            alice_kh
                .add_member(
                    bob_agent,
                    &Membered::Group(gid, group.clone()),
                    Access::Read,
                    &[],
                )
                .await
                .unwrap();
            gid
        };

        // Bob creates his group and adds Alice
        let bob_group_id = {
            let group = bob_kh.generate_group(vec![]).await.unwrap();
            let gid = group.lock().await.group_id();
            let alice_identifier = alice_id.to_identifier().unwrap();
            let alice_agent = bob_kh.get_agent(alice_identifier).await.unwrap();
            bob_kh
                .add_member(
                    alice_agent,
                    &Membered::Group(gid, group.clone()),
                    Access::Read,
                    &[],
                )
                .await
                .unwrap();
            gid
        };

        let (alice_mgr, alice_shared) = make_sync_manager(alice_kh).await;
        let (bob_mgr, bob_shared) = make_sync_manager(bob_kh).await;

        // Bidirectional sync
        run_sync_manager_round(&alice_mgr, &bob_mgr, &alice_id, &bob_id).await;
        run_sync_manager_round(&bob_mgr, &alice_mgr, &bob_id, &alice_id).await;

        // Both should have both groups
        {
            let alice = alice_shared.lock().await;
            assert!(alice.get_group(alice_group_id).await.is_some());
            assert!(alice.get_group(bob_group_id).await.is_some());
        }
        {
            let bob = bob_shared.lock().await;
            assert!(bob.get_group(alice_group_id).await.is_some());
            assert!(bob.get_group(bob_group_id).await.is_some());
        }
    }
}
