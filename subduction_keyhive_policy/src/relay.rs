//! Relay actor for bridging `!Send` keyhive handler operations to a
//! `Send`-safe handle.
//!
//! The [`KeyhiveHandler`](crate::handler::KeyhiveHandler) only implements
//! `Handler<Local, _>` because `keyhive_core`'s async traits produce
//! `!Send` futures. On the CLI (which uses [`Sendable`]), the composed
//! handler runs on a `Send` executor and cannot call keyhive directly.
//!
//! This module provides [`KeyhiveHandlerRelay`], a `Send + Sync + Clone`
//! handle that forwards inbound keyhive messages and disconnect events to
//! an actor future. The actor runs on a `!Send` executor (e.g., a
//! `tokio::task::LocalSet`) and processes requests sequentially using the
//! real `KeyhiveSyncManager`.
//!
//! ```text
//!                      Send boundary
//!                          │
//!  KeyhiveHandlerRelay     │   Actor future (!Send)
//!  ────────────────────    │   ─────────────────────
//!  handle_inbound()        │
//!    → RelayRequest  ────► │ ──► KeyhiveSyncManager
//!    ← Vec<Vec<u8>>  ◄──  │       ::handle_inbound()
//!                          │         calls send_fn
//!  on_peer_disconnect()    │         → reply channel
//!    → RelayRequest  ────► │ ──► KSM::remove_peer()
//! ```
//!
//! # Outbound model
//!
//! `handle_inbound` may call its send function multiple times per
//! invocation (e.g., sync response + contact card). The relay collects
//! _all_ outbound messages into a `Vec<Vec<u8>>` and returns them to
//! the caller. The caller is responsible for sending each one through
//! the real connection as `WireMessage::Keyhive(bytes)`.
//!
//! [`Sendable`]: future_form::Sendable

extern crate alloc;

use alloc::{sync::Arc, vec::Vec};

use keyhive_core::{
    content::reference::ContentRef, crypto::signer::async_signer::AsyncSigner,
    listener::membership::MembershipListener, store::ciphertext::CiphertextStore,
};
use subduction_core::peer::id::PeerId;
use subduction_keyhive::{
    storage::KeyhiveStorage, KeyhivePeerId, KeyhiveSyncManager, SyncManagerError,
};

use future_form::Local;

/// A request sent from the relay handle to the actor loop.
enum RelayRequest {
    /// Process an inbound keyhive message from a peer.
    ///
    /// The actor calls `handle_inbound` with a send function that
    /// collects outbound bytes into a channel. After completion, all
    /// collected outbound messages are sent back through `reply`.
    Inbound {
        peer_id: PeerId,
        wire_bytes: Vec<u8>,
        reply: async_channel::Sender<Result<Vec<Vec<u8>>, RelayError>>,
    },

    /// Notify the actor that a peer disconnected.
    Disconnect(PeerId),
}

/// Errors from relay operations.
#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    /// The relay actor has shut down.
    #[error("keyhive relay actor has shut down")]
    ActorGone,

    /// The keyhive sync manager returned an error.
    #[error(transparent)]
    SyncManager(SyncManagerError<CollectError>),
}

/// `Send`-safe handle to the keyhive handler relay actor.
///
/// Each method sends a request through a channel to the `!Send` actor
/// and awaits the reply. The futures returned are `Send`.
#[derive(Debug, Clone)]
pub struct KeyhiveHandlerRelay {
    tx: async_channel::Sender<RelayRequest>,
}

/// Receiver half of the relay channel.
///
/// Created by [`KeyhiveHandlerRelay::channel`] and consumed by
/// [`RelayReceiver::run`] to start the `!Send` actor loop.
///
/// This type is `Send`, so it can be moved into a dedicated thread
/// where the keyhive `KeyhiveSyncManager` is constructed, and then
/// [`run`](Self::run) is called with the `!Send` sync manager.
#[derive(Debug)]
pub struct RelayReceiver {
    rx: async_channel::Receiver<RelayRequest>,
}

impl RelayReceiver {
    /// Start the relay actor loop with the given sync manager.
    ///
    /// This consumes the receiver and runs until all
    /// [`KeyhiveHandlerRelay`] handles are dropped.
    ///
    /// The returned future is `!Send` — the caller must run it on an
    /// executor that supports `!Send` futures (e.g., a `LocalSet` or
    /// `wasm_bindgen_futures::spawn_local`).
    pub async fn run<Signer, T, P, C, L, R, Store>(
        self,
        sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
    ) where
        Signer: AsyncSigner + Clone + Send + 'static,
        T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: CiphertextStore<T, P> + Clone + 'static,
        L: MembershipListener<Signer, T> + Send + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
        Store: KeyhiveStorage<Local> + 'static,
        Store::Error: Send + Sync + 'static,
    {
        run_relay_actor(sync_manager, self.rx).await;
    }
}

impl KeyhiveHandlerRelay {
    /// Create a relay handle and its matching receiver.
    ///
    /// The receiver is `Send` and should be moved into the dedicated
    /// `!Send` executor thread. Call [`RelayReceiver::run`] there with
    /// the `KeyhiveSyncManager` to start the actor loop.
    ///
    /// `buffer` controls the request channel capacity.
    #[must_use]
    pub fn channel(buffer: usize) -> (Self, RelayReceiver) {
        let (tx, rx) = async_channel::bounded(buffer);
        (Self { tx }, RelayReceiver { rx })
    }

    /// Create a new relay handle and its corresponding actor future.
    ///
    /// The `sync_manager` is moved into the actor future. All handler
    /// operations go through the returned handle via channels.
    ///
    /// `buffer` controls the request channel capacity.
    ///
    /// The returned future is `!Send` — the caller must spawn it on an
    /// executor that supports `!Send` futures (e.g., a `LocalSet` or
    /// `wasm_bindgen_futures::spawn_local`).
    pub fn new<Signer, T, P, C, L, R, Store>(
        sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
        buffer: usize,
    ) -> (Self, impl core::future::Future<Output = ()>)
    where
        Signer: AsyncSigner + Clone + Send + 'static,
        T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: CiphertextStore<T, P> + Clone + 'static,
        L: MembershipListener<Signer, T> + Send + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
        Store: KeyhiveStorage<Local> + 'static,
        Store::Error: Send + Sync + 'static,
    {
        let (tx, rx) = async_channel::bounded(buffer);
        let handle = Self { tx };
        let actor_fut = run_relay_actor(sync_manager, rx);
        (handle, actor_fut)
    }

    /// Forward an inbound keyhive message and collect outbound responses.
    ///
    /// Returns the raw `SUK\x00`-framed bytes for each outbound message
    /// that the sync manager wants to send back. The caller wraps each
    /// one in the application's wire message type and sends it through
    /// the real connection.
    ///
    /// # Errors
    ///
    /// Returns [`RelayError::ActorGone`] if the actor has shut down, or
    /// [`RelayError::Sync`] if the sync manager returns an error.
    pub async fn handle_inbound(
        &self,
        peer_id: PeerId,
        wire_bytes: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, RelayError> {
        let (reply_tx, reply_rx) = async_channel::bounded(1);
        self.tx
            .send(RelayRequest::Inbound {
                peer_id,
                wire_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RelayError::ActorGone)?;

        reply_rx.recv().await.map_err(|_| RelayError::ActorGone)?
    }

    /// Notify the relay that a peer disconnected.
    ///
    /// This removes the peer's keyhive mapping from the sync manager.
    /// Fire-and-forget: if the actor is gone, the disconnect is silently
    /// dropped.
    pub async fn on_peer_disconnect(&self, peer_id: PeerId) {
        drop(self.tx.send(RelayRequest::Disconnect(peer_id)).await);
    }
}

/// The relay actor loop.
///
/// Processes handler requests sequentially using the `!Send`
/// `KeyhiveSyncManager`. This future is `!Send` and must be spawned
/// on an appropriate executor.
async fn run_relay_actor<Signer, T, P, C, L, R, Store>(
    sync_manager: Arc<KeyhiveSyncManager<Signer, T, P, C, L, R, Store>>,
    rx: async_channel::Receiver<RelayRequest>,
) where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: CiphertextStore<T, P> + Clone + 'static,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: KeyhiveStorage<Local> + 'static,
    Store::Error: Send + Sync + 'static,
{
    while let Ok(req) = rx.recv().await {
        match req {
            RelayRequest::Inbound {
                peer_id,
                wire_bytes,
                reply,
            } => {
                let result = handle_inbound_request(&sync_manager, peer_id, wire_bytes).await;
                drop(reply.send(result).await);
            }

            RelayRequest::Disconnect(peer_id) => {
                sync_manager.remove_peer(peer_id.as_bytes()).await;
                tracing::debug!(peer = %peer_id, "relay: removed keyhive peer on disconnect");
            }
        }
    }

    tracing::debug!("keyhive relay actor shutting down (all handles dropped)");
}

/// Process a single inbound keyhive message.
///
/// Resolves the peer's keyhive ID, constructs a collecting send function,
/// calls `handle_inbound`, and returns all collected outbound messages.
async fn handle_inbound_request<Signer, T, P, C, L, R, Store>(
    sync_manager: &KeyhiveSyncManager<Signer, T, P, C, L, R, Store>,
    peer_id: PeerId,
    wire_bytes: Vec<u8>,
) -> Result<Vec<Vec<u8>>, RelayError>
where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: CiphertextStore<T, P> + Clone + 'static,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: KeyhiveStorage<Local> + 'static,
    Store::Error: Send + Sync + 'static,
{
    let from_keyhive_id = resolve_keyhive_peer_id(sync_manager, peer_id).await;

    // Collecting send function: accumulates outbound bytes in a RefCell
    // so that handle_inbound can call it multiple times.
    let outbound: core::cell::RefCell<Vec<Vec<u8>>> = core::cell::RefCell::new(Vec::new());

    let send_fn = |bytes: Vec<u8>| {
        outbound.borrow_mut().push(bytes);
        async { Ok::<(), CollectError>(()) }
    };

    sync_manager
        .handle_inbound(&from_keyhive_id, wire_bytes, send_fn)
        .await
        .map_err(RelayError::SyncManager)?;

    Ok(outbound.into_inner())
}

/// Resolve or auto-register a [`KeyhivePeerId`] for a subduction [`PeerId`].
///
/// Delegates to the shared helper in [`crate::handler`] logic: if the peer
/// is already registered, returns the existing mapping. Otherwise,
/// auto-registers using the Ed25519 public key bytes from the `PeerId`.
async fn resolve_keyhive_peer_id<Signer, T, P, C, L, R, Store>(
    mgr: &KeyhiveSyncManager<Signer, T, P, C, L, R, Store>,
    peer_id: PeerId,
) -> KeyhivePeerId
where
    Signer: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<Signer, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
    Store::Error: Send + Sync + 'static,
{
    if let Some(khid) = mgr.keyhive_peer_id_for(peer_id.as_bytes()).await {
        return khid;
    }

    let khid = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
    tracing::debug!(
        peer = %peer_id,
        "relay: auto-registering keyhive peer mapping"
    );
    mgr.register_peer(*peer_id.as_bytes(), khid.clone()).await;
    khid
}

/// Error type for the collecting send function.
///
/// The collecting send function never actually fails (it just pushes to
/// a `Vec`), but `AsyncSendFn` requires an error type. This is public
/// so that [`RelayError::SyncManager`] can name the concrete
/// `SyncManagerError<CollectError>` without type erasure.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("collecting send (unreachable)")]
pub struct CollectError;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloc::{sync::Arc, vec, vec::Vec};
    use async_lock::Mutex;
    use keyhive_core::{
        access::Access, crypto::signer::memory::MemorySigner, keyhive::Keyhive,
        listener::no_listener::NoListener, principal::membered::Membered,
        store::ciphertext::memory::MemoryCiphertextStore,
    };
    use rand::rngs::OsRng;
    use subduction_keyhive::{KeyhivePeerId, KeyhiveSyncManager, MemoryKeyhiveStorage};

    type SimpleKeyhive = Keyhive<
        MemorySigner,
        [u8; 32],
        Vec<u8>,
        MemoryCiphertextStore<[u8; 32], Vec<u8>>,
        NoListener,
        OsRng,
    >;

    type TestSyncManager = KeyhiveSyncManager<
        MemorySigner,
        [u8; 32],
        Vec<u8>,
        MemoryCiphertextStore<[u8; 32], Vec<u8>>,
        NoListener,
        OsRng,
        MemoryKeyhiveStorage,
    >;

    async fn make_keyhive() -> SimpleKeyhive {
        let mut csprng = OsRng;
        let sk = MemorySigner::generate(&mut csprng);
        Keyhive::generate(sk, MemoryCiphertextStore::new(), NoListener, csprng)
            .await
            .expect("keyhive generation failed")
    }

    fn keyhive_peer_id(kh: &SimpleKeyhive) -> KeyhivePeerId {
        let id: keyhive_core::principal::identifier::Identifier = kh.id().into();
        KeyhivePeerId::from_bytes(id.to_bytes())
    }

    /// Convert a [`KeyhivePeerId`] to a subduction [`PeerId`].
    fn to_subduction_peer_id(khid: &KeyhivePeerId) -> PeerId {
        PeerId::new(*khid.verifying_key())
    }

    async fn make_sync_manager(
        keyhive: SimpleKeyhive,
    ) -> (Arc<TestSyncManager>, Arc<Mutex<SimpleKeyhive>>) {
        let peer_id = keyhive_peer_id(&keyhive);
        let cc = keyhive.contact_card().await.expect("contact card");
        let mut cc_bytes = Vec::new();
        ciborium::into_writer(&cc, &mut cc_bytes).expect("serialize contact card");
        let storage = MemoryKeyhiveStorage::new();
        let shared = Arc::new(Mutex::new(keyhive));
        let manager = Arc::new(TestSyncManager::new(
            shared.clone(),
            storage,
            peer_id,
            cc_bytes,
        ));
        (manager, shared)
    }

    /// Run a full relay sync round between two peers.
    ///
    /// The initiator sends a sync request through its relay, the responder
    /// handles it through its relay, and responses are forwarded back.
    async fn relay_sync_round(
        alice_relay: &KeyhiveHandlerRelay,
        bob_relay: &KeyhiveHandlerRelay,
        alice_sub_id: PeerId,
        bob_sub_id: PeerId,
        alice_mgr: &TestSyncManager,
    ) -> usize {
        let mut messages = 0;

        // Initiator: generate outbound sync request via the sync manager
        let (send_fn, init_out) = {
            let (tx, rx) = async_channel::unbounded::<Vec<u8>>();
            let send_fn = move |bytes: Vec<u8>| {
                let tx = tx.clone();
                async move {
                    tx.send(bytes).await.map_err(|_| CollectError)?;
                    Ok::<(), CollectError>(())
                }
            };
            (send_fn, rx)
        };

        let bob_khid = {
            if let Some(khid) = alice_mgr.keyhive_peer_id_for(bob_sub_id.as_bytes()).await {
                khid
            } else {
                let khid = KeyhivePeerId::from_bytes(*bob_sub_id.as_bytes());
                alice_mgr
                    .register_peer(*bob_sub_id.as_bytes(), khid.clone())
                    .await;
                khid
            }
        };

        alice_mgr
            .sync_with_peer(&bob_khid, &send_fn)
            .await
            .expect("alice sync_with_peer failed");

        // Forward all outbound messages from Alice through Bob's relay
        while let Ok(wire_bytes) = init_out.try_recv() {
            messages += 1;

            // Bob handles via relay
            let responses = bob_relay
                .handle_inbound(alice_sub_id, wire_bytes)
                .await
                .expect("bob relay handle_inbound failed");

            // Forward Bob's responses through Alice's relay
            for response_bytes in responses {
                messages += 1;
                let alice_responses = alice_relay
                    .handle_inbound(bob_sub_id, response_bytes)
                    .await
                    .expect("alice relay handle_inbound failed");

                // Alice may send further messages (e.g., SyncOps)
                for further_bytes in alice_responses {
                    messages += 1;
                    drop(
                        bob_relay
                            .handle_inbound(alice_sub_id, further_bytes)
                            .await
                            .expect("bob relay handle further failed"),
                    );
                }
            }
        }

        messages
    }

    /// Two peers sync keyhive state through the relay actor, verifying
    /// that group membership converges end-to-end.
    #[tokio::test(flavor = "current_thread")]
    async fn relay_e2e_two_peers_group_convergence() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let alice_kh = make_keyhive().await;
                let bob_kh = make_keyhive().await;

                // Exchange contact cards at the keyhive level
                let alice_cc = alice_kh.contact_card().await.expect("alice cc");
                let bob_cc = bob_kh.contact_card().await.expect("bob cc");
                alice_kh
                    .receive_contact_card(&bob_cc)
                    .await
                    .expect("alice receive bob cc");
                bob_kh
                    .receive_contact_card(&alice_cc)
                    .await
                    .expect("bob receive alice cc");

                let alice_khid = keyhive_peer_id(&alice_kh);
                let bob_khid = keyhive_peer_id(&bob_kh);
                let alice_sub_id = to_subduction_peer_id(&alice_khid);
                let bob_sub_id = to_subduction_peer_id(&bob_khid);

                // Alice creates a group and adds Bob
                let group_id = {
                    let group = alice_kh.generate_group(vec![]).await.expect("gen group");
                    let gid = group.lock().await.group_id();
                    let bob_identifier = bob_khid.to_identifier().expect("bob identifier");
                    let bob_agent = alice_kh.get_agent(bob_identifier).await.expect("bob agent");
                    alice_kh
                        .add_member(
                            bob_agent,
                            &Membered::Group(gid, group.clone()),
                            Access::Read,
                            &[],
                        )
                        .await
                        .expect("add bob to group");
                    gid
                };

                // Build sync managers
                let (alice_mgr, _alice_shared) = make_sync_manager(alice_kh).await;
                let (bob_mgr, bob_shared) = make_sync_manager(bob_kh).await;

                // Register peer mappings
                alice_mgr
                    .register_peer(*bob_sub_id.as_bytes(), bob_khid.clone())
                    .await;
                bob_mgr
                    .register_peer(*alice_sub_id.as_bytes(), alice_khid.clone())
                    .await;

                // Create relay actors
                let (alice_relay, alice_receiver) = KeyhiveHandlerRelay::channel(64);
                let (bob_relay, bob_receiver) = KeyhiveHandlerRelay::channel(64);

                // Spawn relay actors on the local executor
                let alice_mgr_clone = alice_mgr.clone();
                let bob_mgr_clone = bob_mgr.clone();
                tokio::task::spawn_local(async move {
                    alice_receiver.run(alice_mgr_clone).await;
                });
                tokio::task::spawn_local(async move {
                    bob_receiver.run(bob_mgr_clone).await;
                });

                // Before sync: Bob should not have the group
                {
                    let kh = bob_shared.lock().await;
                    assert!(
                        kh.get_group(group_id).await.is_none(),
                        "Bob should not have the group before sync"
                    );
                }

                // Alice → Bob sync through relays
                let msgs = relay_sync_round(
                    &alice_relay,
                    &bob_relay,
                    alice_sub_id,
                    bob_sub_id,
                    &alice_mgr,
                )
                .await;

                assert!(msgs > 0, "at least one message should have been exchanged");

                // After sync: Bob should have the group
                {
                    let kh = bob_shared.lock().await;
                    assert!(
                        kh.get_group(group_id).await.is_some(),
                        "Bob should have the group after relay sync"
                    );
                }
            })
            .await;
    }

    /// Two peers with divergent groups both converge after bidirectional
    /// relay sync.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn relay_e2e_bidirectional_convergence() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let alice_kh = make_keyhive().await;
                let bob_kh = make_keyhive().await;

                // Exchange contact cards
                let alice_cc = alice_kh.contact_card().await.expect("alice cc");
                let bob_cc = bob_kh.contact_card().await.expect("bob cc");
                alice_kh
                    .receive_contact_card(&bob_cc)
                    .await
                    .expect("alice receive bob cc");
                bob_kh
                    .receive_contact_card(&alice_cc)
                    .await
                    .expect("bob receive alice cc");

                let alice_khid = keyhive_peer_id(&alice_kh);
                let bob_khid = keyhive_peer_id(&bob_kh);
                let alice_sub_id = to_subduction_peer_id(&alice_khid);
                let bob_sub_id = to_subduction_peer_id(&bob_khid);

                // Alice creates her group with Bob
                let alice_group_id = {
                    let group = alice_kh.generate_group(vec![]).await.expect("gen group");
                    let gid = group.lock().await.group_id();
                    let bob_identifier = bob_khid.to_identifier().expect("bob id");
                    let bob_agent = alice_kh.get_agent(bob_identifier).await.expect("bob agent");
                    alice_kh
                        .add_member(
                            bob_agent,
                            &Membered::Group(gid, group.clone()),
                            Access::Read,
                            &[],
                        )
                        .await
                        .expect("add bob");
                    gid
                };

                // Bob creates his group with Alice
                let bob_group_id = {
                    let group = bob_kh.generate_group(vec![]).await.expect("gen group");
                    let gid = group.lock().await.group_id();
                    let alice_identifier = alice_khid.to_identifier().expect("alice id");
                    let alice_agent = bob_kh
                        .get_agent(alice_identifier)
                        .await
                        .expect("alice agent");
                    bob_kh
                        .add_member(
                            alice_agent,
                            &Membered::Group(gid, group.clone()),
                            Access::Read,
                            &[],
                        )
                        .await
                        .expect("add alice");
                    gid
                };

                // Build sync managers
                let (alice_mgr, alice_shared) = make_sync_manager(alice_kh).await;
                let (bob_mgr, bob_shared) = make_sync_manager(bob_kh).await;

                // Register peer mappings
                alice_mgr
                    .register_peer(*bob_sub_id.as_bytes(), bob_khid.clone())
                    .await;
                bob_mgr
                    .register_peer(*alice_sub_id.as_bytes(), alice_khid.clone())
                    .await;

                // Create and spawn relay actors
                let (alice_relay, alice_receiver) = KeyhiveHandlerRelay::channel(64);
                let (bob_relay, bob_receiver) = KeyhiveHandlerRelay::channel(64);

                let alice_mgr_clone = alice_mgr.clone();
                let bob_mgr_clone = bob_mgr.clone();
                tokio::task::spawn_local(async move {
                    alice_receiver.run(alice_mgr_clone).await;
                });
                tokio::task::spawn_local(async move {
                    bob_receiver.run(bob_mgr_clone).await;
                });

                // Alice → Bob
                relay_sync_round(
                    &alice_relay,
                    &bob_relay,
                    alice_sub_id,
                    bob_sub_id,
                    &alice_mgr,
                )
                .await;

                // Bob → Alice
                relay_sync_round(&bob_relay, &alice_relay, bob_sub_id, alice_sub_id, &bob_mgr)
                    .await;

                // Both should have both groups
                {
                    let alice = alice_shared.lock().await;
                    assert!(
                        alice.get_group(alice_group_id).await.is_some(),
                        "Alice should have her own group"
                    );
                    assert!(
                        alice.get_group(bob_group_id).await.is_some(),
                        "Alice should have Bob's group after sync"
                    );
                }
                {
                    let bob = bob_shared.lock().await;
                    assert!(
                        bob.get_group(bob_group_id).await.is_some(),
                        "Bob should have his own group"
                    );
                    assert!(
                        bob.get_group(alice_group_id).await.is_some(),
                        "Bob should have Alice's group after sync"
                    );
                }
            })
            .await;
    }

    /// Relay correctly handles peer disconnect.
    #[tokio::test(flavor = "current_thread")]
    async fn relay_peer_disconnect_removes_mapping() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let alice_kh = make_keyhive().await;
                let (alice_mgr, _) = make_sync_manager(alice_kh).await;

                let (relay, receiver) = KeyhiveHandlerRelay::channel(64);
                let mgr = alice_mgr.clone();
                tokio::task::spawn_local(async move {
                    receiver.run(mgr).await;
                });

                // Register a fake peer
                let fake_bytes = [42u8; 32];
                let fake_khid = KeyhivePeerId::from_bytes(fake_bytes);
                let fake_sub_id = PeerId::new(fake_bytes);
                alice_mgr.register_peer(fake_bytes, fake_khid).await;

                assert!(alice_mgr.keyhive_peer_id_for(&fake_bytes).await.is_some());

                // Disconnect through relay
                relay.on_peer_disconnect(fake_sub_id).await;

                // Give the actor a moment to process
                tokio::task::yield_now().await;

                assert!(
                    alice_mgr.keyhive_peer_id_for(&fake_bytes).await.is_none(),
                    "peer mapping should be removed after disconnect"
                );
            })
            .await;
    }
}
