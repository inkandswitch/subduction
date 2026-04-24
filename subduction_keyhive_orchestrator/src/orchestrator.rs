//! [`SubductionKeyhiveOrchestrator`]: passive orchestration layer above
//! [`KeyhiveProtocol`].
//!
//! Wraps an `Arc<KeyhiveProtocol<...>>` together with syncpoint
//! tracking, a periodic event cache, and configuration. Drivers
//! own all concurrency primitives and call into the orchestrator.

use alloc::{
    string::{String, ToString},
    sync::Arc,
};
use core::fmt;

use async_lock::Mutex;
use keyhive_core::{
    listener::membership::MembershipListener,
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{content::reference::ContentRef, signer::async_signer::AsyncSigner};
use serde::Deserialize;
use subduction_core::peer::id::PeerId;
use subduction_keyhive::{
    KeyhiveConnection, KeyhivePeerId, KeyhiveProtocol, SignedMessage, SyncOutcome,
    message::AgentHashMap,
    storage::{KeyhiveStorage, StorageHash},
};

use crate::{cache::PeriodicEventCache, config::OrchestratorConfig, syncpoints::SyncpointMap};

/// Error returned from orchestrator methods.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OrchestratorError {
    /// The wrapped [`KeyhiveProtocol`] returned an error.
    #[error("keyhive protocol error: {0}")]
    Protocol(String),

    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Passive orchestrator for the keyhive sync protocol.
#[allow(clippy::type_complexity)]
pub struct SubductionKeyhiveOrchestrator<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner<K> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<K, T, P> + CiphertextStoreExt<K, T, P> + Clone,
    L: MembershipListener<K, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Store: KeyhiveStorage<K>,
    K: future_form::FutureForm,
{
    protocol: Arc<KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>>,
    syncpoints: Mutex<SyncpointMap>,
    cache: Mutex<PeriodicEventCache>,
    config: OrchestratorConfig,
}

impl<Signer, T, P, C, L, R, Conn, Store, K> fmt::Debug
    for SubductionKeyhiveOrchestrator<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner<K> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<K, T, P> + CiphertextStoreExt<K, T, P> + Clone,
    L: MembershipListener<K, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Store: KeyhiveStorage<K>,
    K: future_form::FutureForm,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubductionKeyhiveOrchestrator")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<Signer, T, P, C, L, R, Conn, Store, K>
    SubductionKeyhiveOrchestrator<Signer, T, P, C, L, R, Conn, Store, K>
where
    Signer: AsyncSigner<K> + Clone,
    T: ContentRef + serde::de::DeserializeOwned,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<K, T, P> + CiphertextStoreExt<K, T, P> + Clone,
    L: MembershipListener<K, Signer, T>,
    R: rand::CryptoRng + rand::RngCore,
    Conn: KeyhiveConnection<K>,
    Conn::SendError: 'static,
    Conn::DisconnectError: 'static,
    Store: KeyhiveStorage<K>,
    K: future_form::FutureForm,
{
    /// Construct a new orchestrator around an existing [`KeyhiveProtocol`].
    #[allow(clippy::type_complexity)]
    pub const fn new(
        protocol: Arc<KeyhiveProtocol<Signer, T, P, C, L, R, Conn, Store, K>>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            protocol,
            syncpoints: Mutex::new(SyncpointMap::new()),
            cache: Mutex::new(PeriodicEventCache::new()),
            config,
        }
    }

    /// Reference to the orchestrator's configuration.
    #[must_use]
    pub const fn config(&self) -> &OrchestratorConfig {
        &self.config
    }

    /// Cached per-agent hashes for the local/peer pair, or `None` if empty.
    async fn cached_pair_for(&self, peer: &KeyhivePeerId) -> Option<AgentHashMap> {
        let cache = self.cache.lock().await;
        let local = self.protocol.peer_id();
        let map = cache.hashes_for_peer_pair(&local, peer);
        (!map.is_empty()).then_some(map)
    }

    /// Handle an inbound keyhive message from a peer.
    ///
    /// Delegates to [`KeyhiveProtocol::handle_message`] and threads the
    /// resulting [`SyncOutcome`] into syncpoint bookkeeping:
    ///
    /// - [`ConfirmationReceived`][SyncOutcome::ConfirmationReceived]: record
    ///   the peer's reported total as our new syncpoint for them.
    /// - [`ConfirmationSent`][SyncOutcome::ConfirmationSent]: record the
    ///   fresh total for this peer. If local state actually advanced via
    ///   ingest (checked based on [`KeyhiveProtocol::total_ops`] changing),
    ///   also invalidate every other peer's syncpoint since their cross-pair
    ///   totals may now be stale.
    /// - [`OpsIngested`][SyncOutcome::OpsIngested] -> local state advanced but
    ///   the round is not yet closed. Invalidate all syncpoints when
    ///   `total_ops` advanced.
    /// - [`SyncCheckReceived`][SyncOutcome::SyncCheckReceived] -> call
    ///   [`KeyhiveProtocol::resolve_sync_check`] with our syncpoint for the
    ///   sender.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Protocol`] if the wrapped protocol
    /// rejects the message or if the follow-up sync-check resolution fails.
    pub async fn handle_inbound(
        &self,
        peer: PeerId,
        conn: Conn,
        msg: SignedMessage,
    ) -> Result<(), OrchestratorError> {
        let khid = peer_id_to_keyhive_peer_id(peer);

        // Snapshot pair hashes from the cache so the protocol's
        // SyncRequest handler doesn't re-walk the keyhive event graph.
        let cached_pair = self.cached_pair_for(&khid).await;

        // Snapshot total_ops before handling so we can detect whether
        // ingest actually advanced local state.
        let total_before = self.protocol.total_ops().await;

        let outcome = self
            .protocol
            .handle_message_with_cache(&khid, msg, cached_pair.as_ref())
            .await
            .map_err(|e| OrchestratorError::Protocol(e.to_string()))?;

        if let SyncOutcome::SyncCheckReceived {
            peer,
            sender_total,
            sender_syncpoint,
        } = &outcome
        {
            if !self.protocol.has_peer(peer).await {
                self.protocol.add_peer(peer.clone(), conn).await;
            }
            return self
                .handle_sync_check_received(
                    peer,
                    *sender_total,
                    *sender_syncpoint,
                    cached_pair.as_ref(),
                )
                .await;
        }

        let total_after = self.protocol.total_ops().await;
        let advanced = total_after != total_before;

        let mut map = self.syncpoints.lock().await;
        apply_outcome_to_syncpoints(outcome, &mut map, advanced);

        Ok(())
    }

    /// Drive [`KeyhiveProtocol::resolve_sync_check_with_cache`] after
    /// [`SyncOutcome::SyncCheckReceived`].
    ///
    /// Reads our local syncpoint for `peer` (defaulting to `0` when
    /// absent, falling back to a full sync).
    async fn handle_sync_check_received(
        &self,
        peer: &KeyhivePeerId,
        sender_total: u64,
        sender_syncpoint: u64,
        cached_pair: Option<&AgentHashMap>,
    ) -> Result<(), OrchestratorError> {
        let local_syncpoint_for_sender = self.syncpoints.lock().await.get(peer).unwrap_or(0);

        let outcome = self
            .protocol
            .resolve_sync_check_with_cache(
                peer,
                sender_total,
                sender_syncpoint,
                local_syncpoint_for_sender,
                cached_pair,
            )
            .await
            .map_err(|e| OrchestratorError::Protocol(e.to_string()))?;

        let mut map = self.syncpoints.lock().await;
        // SyncCheck resolution doesn't ingest events, so advanced is always false.
        apply_outcome_to_syncpoints(outcome, &mut map, false);
        Ok(())
    }

    /// Register a peer connection.
    pub async fn on_peer_connect(&self, peer: PeerId, conn: Conn) {
        let khid = peer_id_to_keyhive_peer_id(peer);
        self.protocol.add_peer(khid, conn).await;
    }

    /// Unregister a peer connection.
    ///
    /// Drops any per-peer syncpoint.
    pub async fn on_peer_disconnect(&self, peer: PeerId) {
        let khid = peer_id_to_keyhive_peer_id(peer);
        self.syncpoints.lock().await.remove(&khid);
        self.protocol.remove_peer(&khid).await;
    }

    /// Initiate a full keyhive sync with the given peer.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Protocol`] if the wrapped protocol
    /// fails to initiate the exchange.
    pub async fn sync_with_peer(&self, peer: PeerId) -> Result<(), OrchestratorError> {
        let khid = peer_id_to_keyhive_peer_id(peer);
        let cached_pair = self.cached_pair_for(&khid).await;
        self.protocol
            .sync_keyhive_with_cache(Some(&khid), cached_pair.as_ref())
            .await
            .map_err(|e| OrchestratorError::Protocol(e.to_string()))
    }

    /// Send a lightweight sync check to the given peer.
    ///
    /// If we have a recorded syncpoint for the peer, call
    /// [`KeyhiveProtocol::sync_check_keyhive`] with it. Otherwise fall
    /// through to a full sync.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Protocol`] if the wrapped protocol
    /// call fails.
    pub async fn sync_check_peer(&self, peer: PeerId) -> Result<(), OrchestratorError> {
        let khid = peer_id_to_keyhive_peer_id(peer);
        let syncpoint = self.syncpoints.lock().await.get(&khid);

        match syncpoint {
            Some(sp) => {
                let cached_pair = self.cached_pair_for(&khid).await;
                self.protocol
                    .sync_check_keyhive_with_cache(&khid, sp, cached_pair.as_ref())
                    .await
                    .map_err(|e| OrchestratorError::Protocol(e.to_string()))
            }
            None => self.sync_with_peer(peer).await,
        }
    }

    /// Refresh the periodic event cache from the underlying keyhive.
    ///
    /// Skips work when the keyhive's total op count is unchanged since
    /// the last refresh. Otherwise rebuilds per-agent hashes for the
    /// local peer, every connected peer, and the public agent.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Protocol`] if any per-agent walk
    /// fails (peer-id conversion, serialization).
    pub async fn refresh_cache(&self) -> Result<(), OrchestratorError> {
        let mut cache = self.cache.lock().await;
        cache
            .refresh(&self.protocol)
            .await
            .map(|_| ())
            .map_err(|e| OrchestratorError::Protocol(e.to_string()))
    }

    /// Ingest events from storage.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Storage`] if the storage read or
    /// replay fails.
    pub async fn ingest_from_storage(&self) -> Result<(), OrchestratorError> {
        self.protocol
            .ingest_from_storage()
            .await
            .map_err(|e| OrchestratorError::Storage(e.to_string()))
    }

    /// Compact keyhive storage.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::Storage`] if the compaction fails.
    pub async fn compact(&self, storage_id: StorageHash) -> Result<(), OrchestratorError> {
        self.protocol
            .compact(storage_id)
            .await
            .map_err(|e| OrchestratorError::Storage(e.to_string()))
    }
}

/// Convert a subduction [`PeerId`] into a [`KeyhivePeerId`].
///
/// The conversion is infallible: both types are 32-byte arrays
/// representing an Ed25519 verifying key; no suffix is attached.
const fn peer_id_to_keyhive_peer_id(peer: PeerId) -> KeyhivePeerId {
    KeyhivePeerId::from_bytes(*peer.as_bytes())
}

/// Update our syncpoints based on a [`SyncOutcome`].
fn apply_outcome_to_syncpoints(outcome: SyncOutcome, map: &mut SyncpointMap, advanced: bool) {
    match outcome {
        SyncOutcome::ConfirmationReceived {
            peer,
            confirmer_total,
        } => {
            map.set(peer, confirmer_total);
        }
        SyncOutcome::ConfirmationSent { peer, peer_total } => {
            if advanced {
                map.invalidate_all();
            }
            map.set(peer, peer_total);
        }
        SyncOutcome::OpsIngested { .. } => {
            if advanced {
                map.invalidate_all();
            }
        }
        SyncOutcome::Ok
        | SyncOutcome::InSync { .. }
        | SyncOutcome::SyncCheckFallback { .. }
        | SyncOutcome::SyncCheckReceived { .. } => {}
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    //! Unit tests for the syncpoint-effect helper.
    //!
    //! Behavioural tests that drive the full orchestrator + protocol
    //! live in `behavioural` below; they use
    //! [`subduction_keyhive::test_utils`] (gated behind the
    //! `test-utils` feature, enabled via this crate's dev-dependency).

    use super::*;

    /// Assert that the error type is `Send + Sync + 'static`, which is
    /// what `thiserror::Error` users expect for cross-task propagation.
    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<Tp: Send + Sync + 'static>() {}
        assert_send_sync::<OrchestratorError>();
    }

    fn peer(seed: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([seed; 32])
    }

    fn populated_map() -> SyncpointMap {
        let mut map = SyncpointMap::new();
        map.set(peer(1), 1);
        map.set(peer(2), 2);
        map.set(peer(3), 3);
        map
    }

    #[test]
    fn apply_outcome_confirmation_received_sets_syncpoint() {
        let mut map = SyncpointMap::new();
        apply_outcome_to_syncpoints(
            SyncOutcome::ConfirmationReceived {
                peer: peer(1),
                confirmer_total: 42,
            },
            &mut map,
            false,
        );
        assert_eq!(map.get(&peer(1)), Some(42));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn apply_outcome_confirmation_sent_when_advanced_invalidates_then_sets() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(
            SyncOutcome::ConfirmationSent {
                peer: peer(1),
                peer_total: 99,
            },
            &mut map,
            true,
        );
        assert_eq!(map.get(&peer(1)), Some(99));
        assert_eq!(map.get(&peer(2)), None);
        assert_eq!(map.get(&peer(3)), None);
        assert_eq!(map.len(), 1);
    }

    /// Two peers' confirmations arriving back-to-back must each leave
    /// the other's syncpoint intact when ingestion was a no-op (i.e.
    /// duplicate events that didn't advance `total_ops`). This is the
    /// e2e steady-state pattern: peer's view of our agent is stale, so
    /// it keeps re-sending the same already-known events; we keep
    /// emitting `ConfirmationSent` but `total_ops` doesn't change.
    #[test]
    fn apply_outcome_confirmation_sent_when_not_advanced_keeps_other_peers() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(
            SyncOutcome::ConfirmationSent {
                peer: peer(1),
                peer_total: 99,
            },
            &mut map,
            false,
        );
        assert_eq!(map.get(&peer(1)), Some(99), "current peer's syncpoint set");
        assert_eq!(map.get(&peer(2)), Some(2), "peer 2's syncpoint preserved");
        assert_eq!(map.get(&peer(3)), Some(3), "peer 3's syncpoint preserved");
    }

    #[test]
    fn apply_outcome_ops_ingested_when_advanced_invalidates_all() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(SyncOutcome::OpsIngested { peer: peer(1) }, &mut map, true);
        assert!(map.is_empty());
    }

    #[test]
    fn apply_outcome_ops_ingested_when_not_advanced_is_noop() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(SyncOutcome::OpsIngested { peer: peer(1) }, &mut map, false);
        assert_eq!(map.len(), 3, "no-op when total_ops didn't advance");
    }

    #[test]
    fn apply_outcome_ok_is_noop() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(SyncOutcome::Ok, &mut map, true);
        assert_eq!(map.get(&peer(1)), Some(1));
        assert_eq!(map.get(&peer(2)), Some(2));
        assert_eq!(map.get(&peer(3)), Some(3));
    }

    #[test]
    fn apply_outcome_in_sync_is_noop() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(SyncOutcome::InSync { peer: peer(1) }, &mut map, true);
        assert_eq!(map.get(&peer(1)), Some(1));
        assert_eq!(map.get(&peer(2)), Some(2));
        assert_eq!(map.get(&peer(3)), Some(3));
    }

    #[test]
    fn apply_outcome_sync_check_fallback_is_noop() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(
            SyncOutcome::SyncCheckFallback { peer: peer(1) },
            &mut map,
            true,
        );
        assert_eq!(map.get(&peer(1)), Some(1));
        assert_eq!(map.get(&peer(2)), Some(2));
        assert_eq!(map.get(&peer(3)), Some(3));
    }

    #[test]
    fn apply_outcome_sync_check_received_is_noop() {
        let mut map = populated_map();
        apply_outcome_to_syncpoints(
            SyncOutcome::SyncCheckReceived {
                peer: peer(1),
                sender_total: 10,
                sender_syncpoint: 5,
            },
            &mut map,
            true,
        );
        assert_eq!(map.get(&peer(1)), Some(1));
        assert_eq!(map.get(&peer(2)), Some(2));
        assert_eq!(map.get(&peer(3)), Some(3));
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    clippy::missing_panics_doc
)]
mod behavioural {
    //! Behavioural coverage that drives a real `KeyhiveProtocol` through
    //! the orchestrator. Uses the harness from
    //! `subduction_keyhive::test_utils` (made available by enabling the
    //! `test-utils` feature in this crate's dev-dependencies).

    use alloc::{sync::Arc, vec::Vec};

    use subduction_core::peer::id::PeerId;
    use subduction_keyhive::{
        Message, SignedMessage,
        test_utils::{
            ChannelConnection, TwoPeerHarness, create_group_with_read_members,
            exchange_contact_cards_and_setup, run_sync_round,
        },
    };

    use super::*;

    fn subduction_peer(keyhive_peer: &KeyhivePeerId) -> PeerId {
        PeerId::new(*keyhive_peer.verifying_key())
    }

    fn deserialize_message(signed: &SignedMessage, expected_sender: &KeyhivePeerId) -> Message {
        let verified = signed
            .clone()
            .verify(expected_sender)
            .expect("verify signed message");
        ciborium::from_reader(verified.payload.as_slice()).expect("cbor decode message")
    }

    /// Drain the channel into a vec of decoded messages.
    fn drain_channel(conn: &ChannelConnection, expected_sender: &KeyhivePeerId) -> Vec<Message> {
        core::iter::from_fn(|| conn.inbound_rx.try_recv().ok())
            .map(|msg| deserialize_message(&msg, expected_sender))
            .collect()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_peer_with_no_syncpoint_does_full_sync() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));

        // No syncpoint seeded → orchestrator falls through to a full sync.
        alice_orch
            .sync_check_peer(subduction_peer(&bob_id))
            .await
            .expect("sync_check_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncRequest { .. })),
            "expected SyncRequest, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_check_peer_with_syncpoint_does_sync_check() {
        let TwoPeerHarness {
            alice_proto,
            alice_id,
            bob_id,
            bob_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));

        // Manually seed a syncpoint for bob so the orchestrator picks the
        // light-weight SyncCheck path.
        alice_orch.syncpoints.lock().await.set(bob_id.clone(), 0);

        alice_orch
            .sync_check_peer(subduction_peer(&bob_id))
            .await
            .expect("sync_check_peer");

        let messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            matches!(messages.first(), Some(Message::SyncCheck { .. })),
            "expected SyncCheck, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_inbound_sync_check_received_resolves_via_protocol() {
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

        // Give Alice some membership ops then run a sync so both sides
        // end up in agreement and bob has a syncpoint for alice.
        {
            let kh = alice_kh.lock().await;
            create_group_with_read_members(&kh, &[&bob_id]).await;
        }
        drop(
            run_sync_round(
                &alice_proto,
                &bob_proto,
                &alice_id,
                &bob_id,
                &alice_conn,
                &bob_conn,
            )
            .await,
        );
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));
        let alice_total = alice_orch.protocol.total_ops().await;
        // Seed alice's syncpoint for bob so the in-sync test passes
        // when totals match.
        alice_orch
            .syncpoints
            .lock()
            .await
            .set(bob_id.clone(), alice_total);

        // Bob crafts and sends a SyncCheck targeting alice.
        bob_proto
            .sync_check_keyhive(&alice_id, alice_total)
            .await
            .expect("bob sync_check_keyhive");

        let inbound = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync-check");

        alice_orch
            .handle_inbound(subduction_peer(&bob_id), bob_conn.clone(), inbound)
            .await
            .expect("handle_inbound sync check");

        // Both totals match and alice has a syncpoint for bob, so
        // resolve_sync_check should take the InSync branch and send
        // nothing back.
        let alice_messages = drain_channel(&bob_conn, &alice_id);
        assert!(
            alice_messages.is_empty(),
            "expected no outbound messages (in-sync), got {alice_messages:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_inbound_auto_registers_unknown_peer_on_sync_check() {
        let TwoPeerHarness {
            alice_proto,
            bob_proto,
            alice_id,
            bob_id,
            alice_conn,
            ..
        } = exchange_contact_cards_and_setup().await;

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));

        // Drop bob from alice's protocol peer registry so the inbound
        // sync-check arrives from an "unknown" peer.
        alice_orch.protocol.remove_peer(&bob_id).await;
        assert!(!alice_orch.protocol.has_peer(&bob_id).await);

        // Bob sends a SyncCheck to alice (lands on `alice_conn.inbound_rx`).
        bob_proto
            .sync_check_keyhive(&alice_id, 0)
            .await
            .expect("bob sync_check_keyhive");
        let inbound = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync-check");

        // alice_conn is alice's outbound channel to bob — exactly the
        // conn auto-register should hand to add_peer.
        alice_orch
            .handle_inbound(subduction_peer(&bob_id), alice_conn.clone(), inbound)
            .await
            .expect("handle_inbound sync check");

        assert!(
            alice_orch.protocol.has_peer(&bob_id).await,
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
        drop(
            run_sync_round(
                &alice_proto,
                &bob_proto,
                &alice_id,
                &bob_id,
                &alice_conn,
                &bob_conn,
            )
            .await,
        );

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));

        // First refresh rebuilds.
        alice_orch.refresh_cache().await.expect("first refresh");
        let (total_after_first, agent_count_after_first, public_hash_count_after_first) = {
            let cache = alice_orch.cache.lock().await;
            assert!(!cache.is_empty(), "cache populated after first refresh");
            let agent_count = cache.agent_count();
            assert!(agent_count >= 1, "at least the local agent is tracked");
            let total = cache
                .last_total_ops()
                .expect("last_total_ops should be Some after refresh");
            let public_count = cache.public_hashes().len();
            (total, agent_count, public_count)
        };

        // Second refresh, no keyhive mutation in between → early-return,
        // cache contents unchanged.
        alice_orch.refresh_cache().await.expect("second refresh");
        let cache = alice_orch.cache.lock().await;
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

    /// Reproduces the e2e steady-state pattern: peer's view of our agent
    /// is stale, so it keeps sending us already-known events. Ingestion
    /// is a no-op (`total_ops` doesn't change), but the protocol still
    /// returns `ConfirmationSent`. Without the `total_ops`-gated
    /// invalidation fix, this would wipe every other peer's syncpoint
    /// on every round, defeating the sync-check shortcut.
    #[tokio::test(flavor = "current_thread")]
    async fn handle_inbound_preserves_other_peers_when_ingest_is_noop() {
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

        // Bob has group ops; run a direct sync round so alice ingests
        // them. After this, alice and bob share the full event set.
        {
            let kh = bob_kh.lock().await;
            create_group_with_read_members(&kh, &[&alice_id]).await;
        }
        drop(
            run_sync_round(
                &bob_proto,
                &alice_proto,
                &bob_id,
                &alice_id,
                &bob_conn,
                &alice_conn,
            )
            .await,
        );
        drop(drain_channel(&alice_conn, &bob_id));
        drop(drain_channel(&bob_conn, &alice_id));

        let alice_orch = Arc::new(SubductionKeyhiveOrchestrator::new(
            Arc::new(alice_proto),
            OrchestratorConfig::default(),
        ));

        // Pre-seed a syncpoint for an unrelated peer (carol). With the
        // unconditional `invalidate_all` bug, the next ConfirmationSent
        // would clear this; with the fix, it survives because ingestion
        // doesn't advance `total_ops`.
        let carol_id = KeyhivePeerId::from_bytes([0xCC; 32]);
        alice_orch.syncpoints.lock().await.set(carol_id.clone(), 42);

        // Alice initiates a fresh sync with bob. Both sides already
        // share the events, so any ingestion is a no-op.
        alice_orch
            .sync_with_peer(subduction_peer(&bob_id))
            .await
            .expect("alice sync_with_peer");

        // Bob receives the SyncRequest and replies via direct
        // handle_message (bob has no orchestrator in this test).
        let sync_request = bob_conn
            .inbound_rx
            .try_recv()
            .expect("bob should receive sync request");
        bob_proto
            .handle_message(&alice_id, sync_request)
            .await
            .expect("bob handle sync request");

        // Alice receives the SyncResponse and drives it through her
        // orchestrator. With found=[] and requested=[], handle_sync_response
        // falls through to the confirmation branch, returning
        // ConfirmationSent. Ingestion is a no-op so advanced=false.
        let sync_response = alice_conn
            .inbound_rx
            .try_recv()
            .expect("alice should receive sync response");
        alice_orch
            .handle_inbound(subduction_peer(&bob_id), bob_conn.clone(), sync_response)
            .await
            .expect("alice handle sync response");

        // Possible follow-up traffic (the confirmation alice just sent
        // to bob) is bob's responsibility; drain so subsequent runs
        // aren't confused by leftovers.
        drop(drain_channel(&bob_conn, &alice_id));
        drop(drain_channel(&alice_conn, &bob_id));

        let map = alice_orch.syncpoints.lock().await;
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
}
