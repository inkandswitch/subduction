//! Tests for keyhive sync operations integrated with Subduction.
//!
//! These tests verify that keyhive operations (contact card exchange, group membership,
//! document access) sync correctly through Subduction's message dispatch system.

#![allow(clippy::expect_used, clippy::panic)]

use alloc::{collections::BTreeSet, sync::Arc, vec::Vec};
use core::time::Duration;
use future_form::Sendable;
use keyhive_core::{
    access::Access,
    crypto::digest::Digest,
    event::Event,
    keyhive::Keyhive,
    listener::no_listener::NoListener,
    principal::{group::id::GroupId, identifier::Identifier, membered::Membered},
    store::ciphertext::memory::MemoryCiphertextStore,
};
use rand::rngs::OsRng;
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_keyhive::{peer_id::KeyhivePeerId, storage::MemoryKeyhiveStorage};
use testresult::TestResult;

use super::common::TokioSpawn;
use crate::{
    connection::{message::Message, nonce_cache::NonceCache, test_utils::ChannelMockConnection},
    crypto::signer::MemorySigner,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};

/// Type alias for keyhive used in tests.
type TestKeyhive = Keyhive<
    MemorySigner,
    [u8; 32],
    Vec<u8>,
    MemoryCiphertextStore<[u8; 32], Vec<u8>>,
    NoListener,
    OsRng,
>;

/// Create a unique signer for a peer.
fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

/// Create a keyhive with a specific signer.
async fn make_keyhive_with_signer(signer: MemorySigner) -> TestKeyhive {
    Keyhive::generate(signer, MemoryCiphertextStore::new(), NoListener, OsRng)
        .await
        .expect("failed to create keyhive")
}

/// Get the keyhive peer ID from a keyhive instance.
fn keyhive_peer_id(keyhive: &TestKeyhive) -> KeyhivePeerId {
    let id: keyhive_core::principal::identifier::Identifier = keyhive.id().into();
    KeyhivePeerId::from_bytes(id.to_bytes())
}

/// Type alias for the Subduction type used in tests.
type TestSubduction = Subduction<
    'static,
    Sendable,
    MemoryStorage,
    ChannelMockConnection,
    OpenPolicy,
    MemorySigner,
    CountLeadingZeroBytes,
>;

/// A test harness for two peers with Subduction instances.
///
/// This harness creates two Subduction instances (Alice and Bob) with their
/// keyhives pre-configured with exchanged contact cards. The keyhives are
/// accessed via `subduction.keyhive()` to ensure we're testing the actual
/// internal state that sync operations modify.
struct TwoPeerSubductionHarness {
    alice: Arc<TestSubduction>,
    bob: Arc<TestSubduction>,
    alice_peer_id: PeerId,
    bob_peer_id: PeerId,
    alice_keyhive_id: KeyhivePeerId,
    bob_keyhive_id: KeyhivePeerId,
    alice_handle: crate::connection::test_utils::ChannelMockConnectionHandle,
    bob_handle: crate::connection::test_utils::ChannelMockConnectionHandle,
    // Abort handles for cleanup
    _alice_actor: tokio::task::JoinHandle<()>,
    _alice_listener: tokio::task::JoinHandle<()>,
    _bob_actor: tokio::task::JoinHandle<()>,
    _bob_listener: tokio::task::JoinHandle<()>,
}

impl TwoPeerSubductionHarness {
    /// Create a new two-peer harness with contact cards already exchanged.
    ///
    /// The keyhives are owned by their respective Subduction instances.
    /// Use `self.alice.keyhive()` and `self.bob.keyhive()` to access them.
    async fn new() -> Self {
        let alice_signer = make_signer(1);
        let bob_signer = make_signer(2);

        let alice_peer_id = alice_signer.peer_id();
        let bob_peer_id = bob_signer.peer_id();

        // Create keyhives
        let alice_keyhive = make_keyhive_with_signer(alice_signer.clone()).await;
        let bob_keyhive = make_keyhive_with_signer(bob_signer.clone()).await;

        // Exchange contact cards
        let alice_cc = alice_keyhive.contact_card().await.expect("alice cc");
        let bob_cc = bob_keyhive.contact_card().await.expect("bob cc");
        alice_keyhive
            .receive_contact_card(&bob_cc)
            .await
            .expect("alice recv bob cc");
        bob_keyhive
            .receive_contact_card(&alice_cc)
            .await
            .expect("bob recv alice cc");

        let alice_keyhive_id = keyhive_peer_id(&alice_keyhive);
        let bob_keyhive_id = keyhive_peer_id(&bob_keyhive);

        // Create Subduction instances - keyhives are moved into Subduction
        let (alice, alice_listener, alice_actor) =
            Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
                None,
                alice_signer,
                MemoryStorage::new(),
                OpenPolicy,
                NonceCache::default(),
                CountLeadingZeroBytes,
                ShardedMap::with_key(0, 0),
                TokioSpawn,
                DEFAULT_MAX_PENDING_BLOB_REQUESTS,
                alice_keyhive,
                MemoryKeyhiveStorage::default(),
                alice_cc,
            );

        let (bob, bob_listener, bob_actor) =
            Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
                None,
                bob_signer,
                MemoryStorage::new(),
                OpenPolicy,
                NonceCache::default(),
                CountLeadingZeroBytes,
                ShardedMap::with_key(0, 0),
                TokioSpawn,
                DEFAULT_MAX_PENDING_BLOB_REQUESTS,
                bob_keyhive,
                MemoryKeyhiveStorage::default(),
                bob_cc,
            );

        // Create channel connections
        let (alice_conn, alice_handle) = ChannelMockConnection::new_with_handle(bob_peer_id);
        let (bob_conn, bob_handle) = ChannelMockConnection::new_with_handle(alice_peer_id);

        // Register connections
        alice
            .register(alice_conn.authenticated())
            .await
            .expect("alice register");
        bob.register(bob_conn.authenticated())
            .await
            .expect("bob register");

        // Spawn background tasks
        let alice_actor_task = tokio::spawn(async move {
            let _ = alice_actor.await;
        });
        let alice_listener_task = tokio::spawn(async move {
            let _ = alice_listener.await;
        });
        let bob_actor_task = tokio::spawn(async move {
            let _ = bob_actor.await;
        });
        let bob_listener_task = tokio::spawn(async move {
            let _ = bob_listener.await;
        });

        // Give tasks time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        Self {
            alice,
            bob,
            alice_peer_id,
            bob_peer_id,
            alice_keyhive_id,
            bob_keyhive_id,
            alice_handle,
            bob_handle,
            _alice_actor: alice_actor_task,
            _alice_listener: alice_listener_task,
            _bob_actor: bob_actor_task,
            _bob_listener: bob_listener_task,
        }
    }

    /// Create a group on Alice's keyhive and add Bob as a read member.
    ///
    /// Uses Alice's internal keyhive (via `subduction.keyhive()`).
    async fn alice_creates_group_with_bob(&self) -> GroupId {
        let kh = self.alice.keyhive().lock().await;
        let group = kh.generate_group(vec![]).await.expect("generate_group");
        let group_id = group.lock().await.group_id();

        let bob_identifier = self.bob_keyhive_id.to_identifier().expect("bob identifier");
        let bob_agent = kh.get_agent(bob_identifier).await.expect("get bob agent");

        kh.add_member(
            bob_agent,
            &Membered::Group(group_id, group.clone()),
            Access::Read,
            &[],
        )
        .await
        .expect("add bob to group");

        group_id
    }

    /// Run a keyhive sync round from Alice to Bob.
    ///
    /// Manually forwards messages between the peers since we're using mock connections.
    async fn run_keyhive_sync_alice_to_bob(&self) -> TestResult {
        // Alice initiates sync
        self.alice.sync_keyhive(Some(&self.bob_peer_id)).await?;

        // Forward the keyhive message from Alice to Bob
        let msg = tokio::time::timeout(
            Duration::from_millis(100),
            self.alice_handle.outbound_rx.recv(),
        )
        .await?
        .expect("alice should send keyhive message");

        // Extract the keyhive signed message and forward to Bob
        let Message::Keyhive(signed_msg) = msg else {
            panic!("Expected Keyhive message, got {msg:?}");
        };

        // Bob receives and handles the message
        self.bob
            .handle_keyhive_message(&self.alice_peer_id, signed_msg)
            .await?;

        // Forward Bob's response back to Alice (if any)
        if let Ok(Ok(Message::Keyhive(response_msg))) = tokio::time::timeout(
            Duration::from_millis(50),
            self.bob_handle.outbound_rx.recv(),
        )
        .await
        {
            self.alice
                .handle_keyhive_message(&self.bob_peer_id, response_msg)
                .await?;
        }

        // Forward any SyncOps from Alice to Bob
        if let Ok(Ok(Message::Keyhive(ops_msg))) = tokio::time::timeout(
            Duration::from_millis(50),
            self.alice_handle.outbound_rx.recv(),
        )
        .await
        {
            self.bob
                .handle_keyhive_message(&self.alice_peer_id, ops_msg)
                .await?;
        }

        Ok(())
    }

    /// Run a full bidirectional keyhive sync (Alice → Bob, then Bob → Alice).
    async fn run_bidirectional_keyhive_sync(&self) -> TestResult {
        self.run_keyhive_sync_alice_to_bob().await?;

        // Bob initiates sync back to Alice
        self.bob.sync_keyhive(Some(&self.alice_peer_id)).await?;

        // Forward Bob's message to Alice
        if let Ok(Ok(Message::Keyhive(signed_msg))) = tokio::time::timeout(
            Duration::from_millis(100),
            self.bob_handle.outbound_rx.recv(),
        )
        .await
        {
            self.alice
                .handle_keyhive_message(&self.bob_peer_id, signed_msg)
                .await?;
        }

        // Forward Alice's response to Bob
        if let Ok(Ok(Message::Keyhive(response_msg))) = tokio::time::timeout(
            Duration::from_millis(50),
            self.alice_handle.outbound_rx.recv(),
        )
        .await
        {
            self.bob
                .handle_keyhive_message(&self.alice_peer_id, response_msg)
                .await?;
        }

        // Forward any SyncOps from Bob to Alice
        if let Ok(Ok(Message::Keyhive(ops_msg))) = tokio::time::timeout(
            Duration::from_millis(50),
            self.bob_handle.outbound_rx.recv(),
        )
        .await
        {
            self.alice
                .handle_keyhive_message(&self.bob_peer_id, ops_msg)
                .await?;
        }

        Ok(())
    }
}

impl Drop for TwoPeerSubductionHarness {
    fn drop(&mut self) {
        self._alice_actor.abort();
        self._alice_listener.abort();
        self._bob_actor.abort();
        self._bob_listener.abort();
    }
}

/// Test that keyhive sync can be initiated through Subduction.
#[tokio::test]
async fn test_keyhive_sync_initiates() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice initiates keyhive sync
    harness
        .alice
        .sync_keyhive(Some(&harness.bob_peer_id))
        .await?;

    // Should have sent a keyhive message
    let msg = tokio::time::timeout(
        Duration::from_millis(100),
        harness.alice_handle.outbound_rx.recv(),
    )
    .await?
    .expect("should send keyhive message");

    assert!(
        matches!(msg, Message::Keyhive(_)),
        "Expected Keyhive message, got {msg:?}"
    );

    Ok(())
}

/// Test that keyhive messages are handled correctly through Subduction dispatch.
#[tokio::test]
async fn test_keyhive_message_dispatch() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice initiates sync
    harness
        .alice
        .sync_keyhive(Some(&harness.bob_peer_id))
        .await?;

    // Get the message Alice sent
    let msg = tokio::time::timeout(
        Duration::from_millis(100),
        harness.alice_handle.outbound_rx.recv(),
    )
    .await?
    .expect("alice should send message");

    let Message::Keyhive(signed_msg) = msg else {
        panic!("Expected Keyhive message");
    };

    // Bob handles the message
    harness
        .bob
        .handle_keyhive_message(&harness.alice_peer_id, signed_msg)
        .await?;

    // Bob should respond
    let response = tokio::time::timeout(
        Duration::from_millis(100),
        harness.bob_handle.outbound_rx.recv(),
    )
    .await?
    .expect("bob should respond");

    assert!(
        matches!(response, Message::Keyhive(_)),
        "Expected Keyhive response, got {response:?}"
    );

    Ok(())
}

/// Test that group membership syncs between peers.
#[tokio::test]
async fn test_group_membership_syncs() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice creates a group and adds Bob
    let group_id = harness.alice_creates_group_with_bob().await;

    // Before sync: Bob should NOT have the group
    {
        let bob_kh = harness.bob.keyhive().lock().await;
        assert!(
            bob_kh.get_group(group_id).await.is_none(),
            "Bob should not have the group before sync"
        );
    }

    // Run bidirectional sync
    harness.run_bidirectional_keyhive_sync().await?;

    // Give time for events to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // After sync: Bob should have the group
    {
        let bob_kh = harness.bob.keyhive().lock().await;
        let group = bob_kh.get_group(group_id).await;
        assert!(group.is_some(), "Bob should have the group after sync");
    }

    Ok(())
}

/// Test bidirectional sync with divergent operations.
///
/// Alice and Bob each create their own groups and add each other.
/// After sync, both should have both groups.
#[tokio::test]
async fn test_bidirectional_sync_with_divergent_keyhive_ops() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice creates her group and adds Bob
    let alice_group_id = harness.alice_creates_group_with_bob().await;

    // Bob creates his group and adds Alice
    let bob_group_id = {
        let kh = harness.bob.keyhive().lock().await;
        let group = kh.generate_group(vec![]).await.expect("generate_group");
        let group_id = group.lock().await.group_id();

        let alice_identifier = harness
            .alice_keyhive_id
            .to_identifier()
            .expect("alice identifier");
        let alice_agent = kh
            .get_agent(alice_identifier)
            .await
            .expect("get alice agent");

        kh.add_member(
            alice_agent,
            &Membered::Group(group_id, group.clone()),
            Access::Read,
            &[],
        )
        .await
        .expect("add alice to group");

        group_id
    };

    // Before sync: each peer only has their own group
    {
        let alice_kh = harness.alice.keyhive().lock().await;
        assert!(alice_kh.get_group(alice_group_id).await.is_some());
        assert!(
            alice_kh.get_group(bob_group_id).await.is_none(),
            "Alice should not have Bob's group before sync"
        );
    }
    {
        let bob_kh = harness.bob.keyhive().lock().await;
        assert!(bob_kh.get_group(bob_group_id).await.is_some());
        assert!(
            bob_kh.get_group(alice_group_id).await.is_none(),
            "Bob should not have Alice's group before sync"
        );
    }

    // Run bidirectional sync
    harness.run_bidirectional_keyhive_sync().await?;

    // Give time for events to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // After sync: both peers should have both groups
    {
        let alice_kh = harness.alice.keyhive().lock().await;
        assert!(
            alice_kh.get_group(alice_group_id).await.is_some(),
            "Alice should still have her group"
        );
        assert!(
            alice_kh.get_group(bob_group_id).await.is_some(),
            "Alice should have Bob's group after sync"
        );
    }
    {
        let bob_kh = harness.bob.keyhive().lock().await;
        assert!(
            bob_kh.get_group(bob_group_id).await.is_some(),
            "Bob should still have his group"
        );
        assert!(
            bob_kh.get_group(alice_group_id).await.is_some(),
            "Bob should have Alice's group after sync"
        );
    }

    Ok(())
}

/// Collect all sync-relevant op digests for an agent as raw bytes.
///
/// Includes membership ops and prekey ops. CGKA ops are excluded as they
/// aren't synced yet (see TODO in `sync_events_for_agent`).
async fn collect_sync_op_digests(kh: &TestKeyhive, identifier: Identifier) -> BTreeSet<[u8; 32]> {
    let agent = kh.get_agent(identifier).await.expect("agent should exist");
    let mut digests = BTreeSet::new();

    // Membership ops
    for digest in kh.membership_ops_for_agent(&agent).await.keys() {
        digests.insert(*digest.raw.as_bytes());
    }

    // Prekey ops
    for key_ops in kh.reachable_prekey_ops_for_agent(&agent).await.values() {
        for key_op in key_ops {
            let op = Event::<MemorySigner, [u8; 32], NoListener>::from(key_op.as_ref().clone());
            let digest = Digest::hash(&op);
            digests.insert(*digest.raw.as_bytes());
        }
    }

    digests
}

/// Test that pending keyhive events are cleared after sync.
#[tokio::test]
async fn test_pending_events_cleared_after_sync() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice creates a group (generates events)
    let _group_id = harness.alice_creates_group_with_bob().await;

    // Run bidirectional sync
    harness.run_bidirectional_keyhive_sync().await?;

    // Give time for events to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both peers should have no pending events
    {
        let alice_kh = harness.alice.keyhive().lock().await;
        let pending = alice_kh.pending_event_hashes().await;
        assert!(
            pending.is_empty(),
            "Alice should have no pending events, got {}",
            pending.len()
        );
    }
    {
        let bob_kh = harness.bob.keyhive().lock().await;
        let pending = bob_kh.pending_event_hashes().await;
        assert!(
            pending.is_empty(),
            "Bob should have no pending events, got {}",
            pending.len()
        );
    }

    Ok(())
}

/// Test that keyhive ops fully converge after bidirectional sync.
///
/// Verifies that both peers have identical StaticEvent digest sets for all agents,
/// not just that groups exist. This catches issues where sync delivers incomplete
/// or inconsistent op sets.
#[tokio::test]
async fn test_keyhive_ops_fully_converge() -> TestResult {
    let harness = TwoPeerSubductionHarness::new().await;

    // Alice creates group and adds Bob (generates ops)
    let _group_id = harness.alice_creates_group_with_bob().await;

    // Run bidirectional sync
    harness.run_bidirectional_keyhive_sync().await?;

    // Give time for events to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get identifiers
    let alice_id = harness.alice_keyhive_id.to_identifier().expect("alice id");
    let bob_id = harness.bob_keyhive_id.to_identifier().expect("bob id");

    // Lock both keyhives
    let alice_kh = harness.alice.keyhive().lock().await;
    let bob_kh = harness.bob.keyhive().lock().await;

    // Verify Alice's ops match on both keyhives
    let alice_ops_on_alice = collect_sync_op_digests(&alice_kh, alice_id).await;
    let alice_ops_on_bob = collect_sync_op_digests(&bob_kh, alice_id).await;
    assert_eq!(
        alice_ops_on_alice, alice_ops_on_bob,
        "Alice's ops should be identical on both peers"
    );

    // Verify Bob's ops match on both keyhives
    let bob_ops_on_alice = collect_sync_op_digests(&alice_kh, bob_id).await;
    let bob_ops_on_bob = collect_sync_op_digests(&bob_kh, bob_id).await;
    assert_eq!(
        bob_ops_on_alice, bob_ops_on_bob,
        "Bob's ops should be identical on both peers"
    );

    // Verify pending events are empty on both
    assert!(
        alice_kh.pending_event_hashes().await.is_empty(),
        "Alice should have no pending events"
    );
    assert!(
        bob_kh.pending_event_hashes().await.is_empty(),
        "Bob should have no pending events"
    );

    Ok(())
}
