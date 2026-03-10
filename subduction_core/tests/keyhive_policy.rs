//! Tests for keyhive-backed [`ConnectionPolicy`] and [`StoragePolicy`] on
//! [`KeyhiveSyncManager`].
//!
//! These tests exercise the `Local` policy impls defined in
//! `subduction_core::policy::keyhive`.

use std::{sync::Arc, vec, vec::Vec};

use async_lock::Mutex;
use future_form::Local;
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
    policy::{
        connection::ConnectionPolicy,
        keyhive::{
            ConnectionDisallowedError, FetchDisallowedError, PutDisallowedError,
            try_peer_id_to_identifier, try_sedimentree_id_to_document_id,
        },
        storage::StoragePolicy,
    },
};
use subduction_keyhive::{KeyhivePeerId, KeyhiveSyncManager, MemoryKeyhiveStorage};
use testresult::TestResult;

// ── Test-local type aliases ─────────────────────────────────────────────

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

/// Create two keyhives that have exchanged contact cards, a document owned
/// by `alice` with `bob` as a member at the given access level, and a
/// `KeyhiveSyncManager` for alice.
async fn setup_with_member(
    bob_access: Access,
) -> (
    TestSyncManager,
    PeerId,
    SedimentreeId,
    Arc<Mutex<SimpleKeyhive>>,
) {
    let alice_kh = make_keyhive().await;
    let bob_kh = make_keyhive().await;

    // Exchange contact cards
    let alice_cc = alice_kh.contact_card().await.expect("alice contact card");
    let bob_cc = bob_kh.contact_card().await.expect("bob contact card");
    alice_kh
        .receive_contact_card(&bob_cc)
        .await
        .expect("alice receive bob cc");
    bob_kh
        .receive_contact_card(&alice_cc)
        .await
        .expect("bob receive alice cc");

    let bob_peer_id = peer_id_from_keyhive(&bob_kh);

    // Create document with bob as member
    let bob_identifier: Identifier = bob_kh.id().into();
    let bob_agent = alice_kh
        .get_agent(bob_identifier)
        .await
        .expect("bob should be a known agent after cc exchange");

    let doc = alice_kh
        .generate_doc(
            vec![],
            nonempty_0_10::NonEmpty {
                head: [0u8; 32],
                tail: vec![],
            },
        )
        .await
        .expect("generate_doc should not fail");

    let doc_id = doc.lock().await.doc_id();
    let membered = Membered::Document(doc_id, doc.clone());

    alice_kh
        .add_member(bob_agent, &membered, bob_access, &[])
        .await
        .expect("add_member should not fail");

    let sedimentree_id = sedimentree_id_from_doc_id(doc_id);

    let (sm, shared) = make_sync_manager(alice_kh).await;
    (sm, bob_peer_id, sedimentree_id, shared)
}

// ── Conversion helper tests ─────────────────────────────────────────────

#[test]
fn valid_ed25519_peer_id_converts_to_identifier() {
    let kh = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(make_keyhive());
    let peer_id = peer_id_from_keyhive(&kh);
    assert!(try_peer_id_to_identifier(peer_id).is_some());
}

#[test]
fn invalid_peer_id_bytes_return_none() {
    // [2u8; 32] is not a valid Ed25519 curve point
    let peer_id = PeerId::new([2u8; 32]);
    assert!(try_peer_id_to_identifier(peer_id).is_none());
}

#[test]
fn valid_doc_id_converts_to_sedimentree_id_and_back() -> TestResult {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let kh = make_keyhive().await;
        let doc = kh
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
        let sedimentree_id = sedimentree_id_from_doc_id(doc_id);
        let roundtripped = try_sedimentree_id_to_document_id(sedimentree_id);
        assert!(roundtripped.is_some(), "roundtrip should succeed");
    });
    Ok(())
}

#[test]
fn invalid_sedimentree_id_returns_none() {
    let sedimentree_id = SedimentreeId::new([2u8; 32]);
    assert!(try_sedimentree_id_to_document_id(sedimentree_id).is_none());
}

// ── ConnectionPolicy<Local> tests ───────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn known_agent_is_allowed_to_connect() -> TestResult {
    let (sm, bob_peer_id, _sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let result = ConnectionPolicy::<Local>::authorize_connect(&sm, bob_peer_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_peer_is_rejected() -> TestResult {
    let alice_kh = make_keyhive().await;
    let (sm, _shared) = make_sync_manager(alice_kh).await;

    // A stranger whose contact card was never exchanged
    let stranger_kh = make_keyhive().await;
    let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

    let result = ConnectionPolicy::<Local>::authorize_connect(&sm, stranger_peer_id).await;
    assert_eq!(
        result.expect_err("should reject unknown agent"),
        ConnectionDisallowedError::UnknownAgent
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_peer_id_bytes_rejected_at_connect() -> TestResult {
    let alice_kh = make_keyhive().await;
    let (sm, _shared) = make_sync_manager(alice_kh).await;

    let invalid_peer = PeerId::new([2u8; 32]);

    let result = ConnectionPolicy::<Local>::authorize_connect(&sm, invalid_peer).await;
    assert_eq!(
        result.expect_err("should reject invalid peer id"),
        ConnectionDisallowedError::InvalidPeerId
    );
    Ok(())
}

// ── StoragePolicy<Local>::authorize_fetch tests ─────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn member_with_pull_access_can_fetch() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let result = StoragePolicy::<Local>::authorize_fetch(&sm, bob_peer_id, sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn member_with_write_access_can_fetch() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Write).await;

    let result = StoragePolicy::<Local>::authorize_fetch(&sm, bob_peer_id, sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn non_member_cannot_fetch() -> TestResult {
    let (sm, _bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    // Create a third peer who is NOT a member
    let stranger_kh = make_keyhive().await;
    let stranger_peer_id = peer_id_from_keyhive(&stranger_kh);

    // Exchange contact cards so the stranger is a known agent but NOT a doc member
    let stranger_cc = stranger_kh
        .contact_card()
        .await
        .expect("stranger contact card");
    {
        let kh = sm.keyhive().lock().await;
        kh.receive_contact_card(&stranger_cc)
            .await
            .expect("receive stranger cc");
    }

    let result =
        StoragePolicy::<Local>::authorize_fetch(&sm, stranger_peer_id, sedimentree_id).await;
    assert_eq!(
        result.expect_err("non-member should be denied fetch"),
        FetchDisallowedError::InsufficientAccess
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_nonexistent_document_fails() -> TestResult {
    let (sm, bob_peer_id, _sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    // Use a valid but non-existent sedimentree ID (from a different keyhive)
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
    let other_doc_id = other_doc.lock().await.doc_id();
    let other_sedimentree_id = sedimentree_id_from_doc_id(other_doc_id);

    let result =
        StoragePolicy::<Local>::authorize_fetch(&sm, bob_peer_id, other_sedimentree_id).await;
    assert_eq!(
        result.expect_err("nonexistent doc should fail"),
        FetchDisallowedError::DocumentNotFound
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_with_invalid_peer_id_fails() -> TestResult {
    let (sm, _bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let invalid_peer = PeerId::new([2u8; 32]);
    let result = StoragePolicy::<Local>::authorize_fetch(&sm, invalid_peer, sedimentree_id).await;
    assert_eq!(
        result.expect_err("invalid peer ID should fail"),
        FetchDisallowedError::InvalidPeerId
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn fetch_with_invalid_sedimentree_id_fails() -> TestResult {
    let (sm, bob_peer_id, _sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let invalid_sedimentree = SedimentreeId::new([2u8; 32]);
    let result =
        StoragePolicy::<Local>::authorize_fetch(&sm, bob_peer_id, invalid_sedimentree).await;
    assert_eq!(
        result.expect_err("invalid sedimentree ID should fail"),
        FetchDisallowedError::InvalidSedimentreeId
    );
    Ok(())
}

// ── StoragePolicy<Local>::authorize_put tests ───────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn member_with_write_access_can_put() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Write).await;

    let result =
        StoragePolicy::<Local>::authorize_put(&sm, bob_peer_id, bob_peer_id, sedimentree_id).await;
    assert!(result.is_ok());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn member_with_pull_only_cannot_put() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let result =
        StoragePolicy::<Local>::authorize_put(&sm, bob_peer_id, bob_peer_id, sedimentree_id).await;
    assert_eq!(
        result.expect_err("Pull-only member should not be able to put"),
        PutDisallowedError::InsufficientAccess
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn member_with_read_only_cannot_put() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Read).await;

    let result =
        StoragePolicy::<Local>::authorize_put(&sm, bob_peer_id, bob_peer_id, sedimentree_id).await;
    assert_eq!(
        result.expect_err("Read-only member should not be able to put"),
        PutDisallowedError::InsufficientAccess
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn put_with_invalid_author_fails() -> TestResult {
    let (sm, _bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Write).await;

    let invalid_author = PeerId::new([2u8; 32]);
    let result =
        StoragePolicy::<Local>::authorize_put(&sm, invalid_author, invalid_author, sedimentree_id)
            .await;
    assert_eq!(
        result.expect_err("invalid author should fail"),
        PutDisallowedError::InvalidAuthorId
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn put_to_nonexistent_document_fails() -> TestResult {
    let (sm, bob_peer_id, _sedimentree_id, _kh) = setup_with_member(Access::Write).await;

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
    let other_sedimentree_id = sedimentree_id_from_doc_id(other_doc.lock().await.doc_id());

    let result =
        StoragePolicy::<Local>::authorize_put(&sm, bob_peer_id, bob_peer_id, other_sedimentree_id)
            .await;
    assert_eq!(
        result.expect_err("nonexistent doc should fail for put"),
        PutDisallowedError::DocumentNotFound
    );
    Ok(())
}

// ── StoragePolicy<Local>::filter_authorized_fetch tests ─────────────────

#[tokio::test(flavor = "current_thread")]
async fn filter_returns_only_authorized_ids() -> TestResult {
    let (sm, bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    // Create a second valid but non-existent (from alice's perspective) sedimentree ID
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
    let other_sedimentree_id = sedimentree_id_from_doc_id(other_doc.lock().await.doc_id());

    let ids = vec![sedimentree_id, other_sedimentree_id];
    let authorized = StoragePolicy::<Local>::filter_authorized_fetch(&sm, bob_peer_id, ids).await;

    assert_eq!(authorized.len(), 1);
    assert_eq!(authorized[0], sedimentree_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn filter_returns_empty_for_invalid_peer() -> TestResult {
    let (sm, _bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let invalid_peer = PeerId::new([2u8; 32]);
    let authorized =
        StoragePolicy::<Local>::filter_authorized_fetch(&sm, invalid_peer, vec![sedimentree_id])
            .await;

    assert!(authorized.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn filter_returns_empty_for_invalid_sedimentree_ids() -> TestResult {
    let (sm, bob_peer_id, _sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    let invalid_id = SedimentreeId::new([2u8; 32]);
    let authorized =
        StoragePolicy::<Local>::filter_authorized_fetch(&sm, bob_peer_id, vec![invalid_id]).await;

    assert!(authorized.is_empty());
    Ok(())
}

// ── Owner access tests ──────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn document_owner_can_fetch_and_put() -> TestResult {
    let (sm, _bob_peer_id, sedimentree_id, _kh) = setup_with_member(Access::Pull).await;

    // Alice is the owner — use her PeerId
    let alice_peer_id = PeerId::new(sm.peer_id().verifying_key().to_owned());

    let fetch_result =
        StoragePolicy::<Local>::authorize_fetch(&sm, alice_peer_id, sedimentree_id).await;
    assert!(fetch_result.is_ok(), "owner should be able to fetch");

    let put_result =
        StoragePolicy::<Local>::authorize_put(&sm, alice_peer_id, alice_peer_id, sedimentree_id)
            .await;
    assert!(put_result.is_ok(), "owner should be able to put");

    Ok(())
}
