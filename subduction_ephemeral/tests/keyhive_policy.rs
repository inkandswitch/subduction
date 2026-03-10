//! Tests for keyhive-backed [`EphemeralPolicy`] on [`KeyhiveSyncManager`]
//! (both `Local` and `Sendable` via [`SyncManagerActorHandle`]), plus
//! `Sendable` tests for [`ConnectionPolicy`] and [`StoragePolicy`] on the
//! actor handle.

use std::{sync::Arc, vec, vec::Vec};

use async_lock::Mutex;
use future_form::{Local, Sendable};
use keyhive_core::{
    access::Access,
    crypto::signer::memory::MemorySigner,
    keyhive::Keyhive,
    listener::no_listener::NoListener,
    principal::{document::id::DocumentId, identifier::Identifier, membered::Membered},
    store::ciphertext::memory::MemoryCiphertextStore,
};
use rand::rngs::OsRng;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_ephemeral::policy::{
    keyhive::{PublishDisallowedError, SubscribeDisallowedError},
    keyhive_actor::{self, SyncManagerActorHandle},
    EphemeralPolicy,
};
use subduction_keyhive::{KeyhivePeerId, KeyhiveSyncManager, MemoryKeyhiveStorage};
use testresult::TestResult;

// ── Type aliases ────────────────────────────────────────────────────────

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

// ── Helpers ─────────────────────────────────────────────────────────────

async fn make_keyhive() -> SimpleKeyhive {
    let mut csprng = OsRng;
    let sk = MemorySigner::generate(&mut csprng);
    Keyhive::generate(sk, MemoryCiphertextStore::new(), NoListener, csprng)
        .await
        .expect("keyhive generation should not fail")
}

fn keyhive_peer_id(kh: &SimpleKeyhive) -> KeyhivePeerId {
    let id: Identifier = kh.id().into();
    KeyhivePeerId::from_bytes(id.to_bytes())
}

fn peer_id_from_keyhive(kh: &SimpleKeyhive) -> PeerId {
    let id: Identifier = kh.id().into();
    PeerId::new(id.to_bytes())
}

fn sedimentree_id_from_doc_id(doc_id: DocumentId) -> SedimentreeId {
    let identifier: Identifier = doc_id.into();
    SedimentreeId::new(identifier.to_bytes())
}

async fn make_sync_manager(kh: SimpleKeyhive) -> (TestSyncManager, Arc<Mutex<SimpleKeyhive>>) {
    let peer_id = keyhive_peer_id(&kh);
    let cc = kh
        .contact_card()
        .await
        .expect("contact card should not fail");
    let mut buf = Vec::new();
    ciborium::into_writer(&cc, &mut buf).expect("serialization should not fail");
    let shared = Arc::new(Mutex::new(kh));
    let sm = TestSyncManager::new(shared.clone(), MemoryKeyhiveStorage::new(), peer_id, buf);
    (sm, shared)
}

struct TestFixture {
    sm: TestSyncManager,
    bob_peer_id: PeerId,
    sedimentree_id: SedimentreeId,
    _kh: Arc<Mutex<SimpleKeyhive>>,
}

async fn setup_with_member(bob_access: Access) -> TestFixture {
    let alice_kh = make_keyhive().await;
    let bob_kh = make_keyhive().await;

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

    let bob_peer_id = peer_id_from_keyhive(&bob_kh);
    let bob_identifier: Identifier = bob_kh.id().into();
    let bob_agent = alice_kh
        .get_agent(bob_identifier)
        .await
        .expect("bob should be known after cc exchange");

    let doc = alice_kh
        .generate_doc(
            vec![],
            nonempty_0_10::NonEmpty {
                head: [0u8; 32],
                tail: vec![],
            },
        )
        .await
        .expect("generate_doc");

    let doc_id = doc.lock().await.doc_id();
    let membered = Membered::Document(doc_id, doc.clone());
    alice_kh
        .add_member(bob_agent, &membered, bob_access, &[])
        .await
        .expect("add_member");

    let sedimentree_id = sedimentree_id_from_doc_id(doc_id);
    let (sm, shared) = make_sync_manager(alice_kh).await;

    TestFixture {
        sm,
        bob_peer_id,
        sedimentree_id,
        _kh: shared,
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  EphemeralPolicy<Local> tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "current_thread")]
async fn local_member_with_pull_can_subscribe() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let result =
        EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, f.bob_peer_id, f.sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_member_with_write_can_subscribe() -> TestResult {
    let f = setup_with_member(Access::Write).await;

    let result =
        EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, f.bob_peer_id, f.sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_non_member_cannot_subscribe() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let stranger_kh = make_keyhive().await;
    let stranger_cc = stranger_kh.contact_card().await.expect("stranger cc");
    {
        let kh = f.sm.keyhive().lock().await;
        kh.receive_contact_card(&stranger_cc)
            .await
            .expect("receive stranger cc");
    }
    let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

    let result =
        EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, stranger_peer_id, f.sedimentree_id)
            .await;
    assert_eq!(
        result.expect_err("non-member should be denied subscribe"),
        SubscribeDisallowedError::InsufficientAccess
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_subscribe_invalid_peer_fails() -> TestResult {
    let f = setup_with_member(Access::Pull).await;
    let invalid = PeerId::new([2u8; 32]);

    let result =
        EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, invalid, f.sedimentree_id).await;
    assert_eq!(
        result.expect_err("invalid peer should fail"),
        SubscribeDisallowedError::InvalidPeerId
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_subscribe_invalid_sedimentree_fails() -> TestResult {
    let f = setup_with_member(Access::Pull).await;
    let invalid = SedimentreeId::new([2u8; 32]);

    let result = EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, f.bob_peer_id, invalid).await;
    assert_eq!(
        result.expect_err("invalid sedimentree should fail"),
        SubscribeDisallowedError::InvalidSedimentreeId
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_subscribe_nonexistent_doc_fails() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let other_kh = make_keyhive().await;
    let other_doc = other_kh
        .generate_doc(
            vec![],
            nonempty_0_10::NonEmpty {
                head: [1u8; 32],
                tail: vec![],
            },
        )
        .await
        .expect("generate_doc");
    let other_sid = sedimentree_id_from_doc_id(other_doc.lock().await.doc_id());

    let result =
        EphemeralPolicy::<Local>::authorize_subscribe(&f.sm, f.bob_peer_id, other_sid).await;
    assert_eq!(
        result.expect_err("nonexistent doc should fail"),
        SubscribeDisallowedError::DocumentNotFound
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_member_with_write_can_publish() -> TestResult {
    let f = setup_with_member(Access::Write).await;

    let result =
        EphemeralPolicy::<Local>::authorize_publish(&f.sm, f.bob_peer_id, f.sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_member_with_pull_cannot_publish() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let result =
        EphemeralPolicy::<Local>::authorize_publish(&f.sm, f.bob_peer_id, f.sedimentree_id).await;
    assert_eq!(
        result.expect_err("Pull-only should not be able to publish"),
        PublishDisallowedError::InsufficientAccess
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_publish_invalid_peer_fails() -> TestResult {
    let f = setup_with_member(Access::Write).await;
    let invalid = PeerId::new([2u8; 32]);

    let result =
        EphemeralPolicy::<Local>::authorize_publish(&f.sm, invalid, f.sedimentree_id).await;
    assert_eq!(
        result.expect_err("invalid peer should fail"),
        PublishDisallowedError::InvalidPeerId
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_filter_authorized_subscribers() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let stranger_kh = make_keyhive().await;
    let stranger_cc = stranger_kh.contact_card().await.expect("stranger cc");
    {
        let kh = f.sm.keyhive().lock().await;
        kh.receive_contact_card(&stranger_cc)
            .await
            .expect("receive stranger cc");
    }
    let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

    let peers = vec![f.bob_peer_id, stranger_peer_id];
    let authorized =
        EphemeralPolicy::<Local>::filter_authorized_subscribers(&f.sm, f.sedimentree_id, peers)
            .await;

    assert_eq!(authorized.len(), 1);
    assert_eq!(authorized[0], f.bob_peer_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn local_filter_returns_empty_for_invalid_sedimentree() -> TestResult {
    let f = setup_with_member(Access::Pull).await;

    let invalid = SedimentreeId::new([2u8; 32]);
    let authorized = EphemeralPolicy::<Local>::filter_authorized_subscribers(
        &f.sm,
        invalid,
        vec![f.bob_peer_id],
    )
    .await;

    assert!(authorized.is_empty());
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
//  SyncManagerActorHandle — Sendable policy tests
// ═══════════════════════════════════════════════════════════════════════

/// Spawn the actor on a `current_thread` runtime and return the handle.
///
/// The actor future is `!Send`, so it runs on the same thread via
/// `tokio::task::spawn_local`. The returned handle is `Send + Sync`.
fn spawn_actor(sm: TestSyncManager) -> SyncManagerActorHandle {
    let (handle, actor_fut) = SyncManagerActorHandle::new(sm, 16);
    tokio::task::spawn_local(actor_fut);
    handle
}

// ── ConnectionPolicy<Sendable> ──────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn sendable_known_agent_can_connect() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let handle = spawn_actor(f.sm);

            let result =
                ConnectionPolicy::<Sendable>::authorize_connect(&handle, f.bob_peer_id).await;
            assert!(result.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_unknown_peer_rejected() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let alice_kh = make_keyhive().await;
            let (sm, _shared) = make_sync_manager(alice_kh).await;
            let handle = spawn_actor(sm);

            let stranger_kh = make_keyhive().await;
            let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

            let result =
                ConnectionPolicy::<Sendable>::authorize_connect(&handle, stranger_peer_id).await;
            assert_eq!(
                result.expect_err("unknown agent should be rejected"),
                keyhive_actor::ConnectionDisallowedError::UnknownAgent
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_invalid_peer_rejected_at_connect() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let alice_kh = make_keyhive().await;
            let (sm, _shared) = make_sync_manager(alice_kh).await;
            let handle = spawn_actor(sm);

            let invalid = PeerId::new([2u8; 32]);
            let result = ConnectionPolicy::<Sendable>::authorize_connect(&handle, invalid).await;
            assert_eq!(
                result.expect_err("invalid peer should be rejected"),
                keyhive_actor::ConnectionDisallowedError::InvalidPeerId
            );
            Ok(())
        })
        .await
}

// ── StoragePolicy<Sendable> ─────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_pull_can_fetch() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let handle = spawn_actor(f.sm);

            let result = StoragePolicy::<Sendable>::authorize_fetch(
                &handle,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert!(result.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_non_member_cannot_fetch() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;

            let stranger_kh = make_keyhive().await;
            let stranger_cc = stranger_kh.contact_card().await.expect("stranger cc");
            {
                let kh = f.sm.keyhive().lock().await;
                kh.receive_contact_card(&stranger_cc)
                    .await
                    .expect("receive stranger cc");
            }
            let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

            let handle = spawn_actor(f.sm);

            let result = StoragePolicy::<Sendable>::authorize_fetch(
                &handle,
                stranger_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert_eq!(
                result.expect_err("non-member should be denied"),
                keyhive_actor::FetchDisallowedError::InsufficientAccess
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_write_can_put() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Write).await;
            let handle = spawn_actor(f.sm);

            let result = StoragePolicy::<Sendable>::authorize_put(
                &handle,
                f.bob_peer_id,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert!(result.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_pull_cannot_put() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let handle = spawn_actor(f.sm);

            let result = StoragePolicy::<Sendable>::authorize_put(
                &handle,
                f.bob_peer_id,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert_eq!(
                result.expect_err("Pull-only should not be able to put"),
                keyhive_actor::PutDisallowedError::InsufficientAccess
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_filter_authorized_fetch() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;

            let other_kh = make_keyhive().await;
            let other_doc = other_kh
                .generate_doc(
                    vec![],
                    nonempty_0_10::NonEmpty {
                        head: [1u8; 32],
                        tail: vec![],
                    },
                )
                .await
                .expect("generate_doc");
            let other_sid = sedimentree_id_from_doc_id(other_doc.lock().await.doc_id());

            let handle = spawn_actor(f.sm);

            let ids = vec![f.sedimentree_id, other_sid];
            let authorized =
                StoragePolicy::<Sendable>::filter_authorized_fetch(&handle, f.bob_peer_id, ids)
                    .await;

            assert_eq!(authorized.len(), 1);
            assert_eq!(authorized[0], f.sedimentree_id);
            Ok(())
        })
        .await
}

// ── EphemeralPolicy<Sendable> ───────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_pull_can_subscribe() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let handle = spawn_actor(f.sm);

            let result = EphemeralPolicy::<Sendable>::authorize_subscribe(
                &handle,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert!(result.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_non_member_cannot_subscribe() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;

            let stranger_kh = make_keyhive().await;
            let stranger_cc = stranger_kh.contact_card().await.expect("stranger cc");
            {
                let kh = f.sm.keyhive().lock().await;
                kh.receive_contact_card(&stranger_cc)
                    .await
                    .expect("receive stranger cc");
            }
            let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

            let handle = spawn_actor(f.sm);

            let result = EphemeralPolicy::<Sendable>::authorize_subscribe(
                &handle,
                stranger_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert_eq!(
                result.expect_err("non-member should be denied"),
                keyhive_actor::SubscribeDisallowedError::InsufficientAccess
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_write_can_publish() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Write).await;
            let handle = spawn_actor(f.sm);

            let result = EphemeralPolicy::<Sendable>::authorize_publish(
                &handle,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert!(result.is_ok());
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_member_with_pull_cannot_publish() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let handle = spawn_actor(f.sm);

            let result = EphemeralPolicy::<Sendable>::authorize_publish(
                &handle,
                f.bob_peer_id,
                f.sedimentree_id,
            )
            .await;
            assert_eq!(
                result.expect_err("Pull-only should not be able to publish"),
                keyhive_actor::PublishDisallowedError::InsufficientAccess
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn sendable_filter_authorized_subscribers() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;

            let stranger_kh = make_keyhive().await;
            let stranger_cc = stranger_kh.contact_card().await.expect("stranger cc");
            {
                let kh = f.sm.keyhive().lock().await;
                kh.receive_contact_card(&stranger_cc)
                    .await
                    .expect("receive stranger cc");
            }
            let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

            let handle = spawn_actor(f.sm);

            let peers = vec![f.bob_peer_id, stranger_peer_id];
            let authorized = EphemeralPolicy::<Sendable>::filter_authorized_subscribers(
                &handle,
                f.sedimentree_id,
                peers,
            )
            .await;

            assert_eq!(authorized.len(), 1);
            assert_eq!(authorized[0], f.bob_peer_id);
            Ok(())
        })
        .await
}

// ── Actor lifecycle tests ───────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn actor_gone_returns_error() -> TestResult {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let f = setup_with_member(Access::Pull).await;
            let (handle, actor_fut) = SyncManagerActorHandle::new(f.sm, 16);

            // Drop the actor future without spawning it — the channel
            // closes immediately when the receiver is dropped.
            drop(actor_fut);

            let result =
                ConnectionPolicy::<Sendable>::authorize_connect(&handle, f.bob_peer_id).await;
            assert_eq!(
                result.expect_err("should return ActorGone"),
                keyhive_actor::ConnectionDisallowedError::ActorGone
            );
            Ok(())
        })
        .await
}
