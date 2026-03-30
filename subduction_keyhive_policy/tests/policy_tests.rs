//! Integration tests for `SubductionKeyhive` policy implementations.
//!
//! These tests create real `Keyhive` instances, generate documents and groups
//! with specific access grants, and verify that the `ConnectionPolicy` and
//! `StoragePolicy` impls return the correct `Ok`/`Err` results.
//!
//! Only the `Local` impls are tested — the `Sendable` impls have identical
//! logic (just different future boxing) and the actor bridge is separately
//! tested in `handler.rs`.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::type_complexity,
    clippy::unwrap_used,
    clippy::doc_markdown
)]

use future_form::{Local, Sendable};
use keyhive_core::{
    access::Access,
    principal::{membered::Membered, peer::Peer},
};
use nonempty::nonempty;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_keyhive::test_utils::{keyhive_peer_id, make_keyhive, SimpleKeyhive};
use subduction_keyhive_policy::SubductionKeyhive;

// ── Helpers ─────────────────────────────────────────────────────────────

/// Wrap a `SimpleKeyhive` into a `SubductionKeyhive` for policy testing.
fn wrap_policy(
    keyhive: SimpleKeyhive,
) -> SubductionKeyhive<
    keyhive_crypto::signer::memory::MemorySigner,
    [u8; 32],
    Vec<u8>,
    keyhive_core::store::ciphertext::memory::MemoryCiphertextStore<[u8; 32], Vec<u8>>,
    keyhive_core::listener::no_listener::NoListener,
    rand::rngs::OsRng,
> {
    SubductionKeyhive::from(keyhive)
}

/// Convert a keyhive `DocumentId` to a `SedimentreeId`.
fn doc_id_to_sedimentree_id(
    doc_id: keyhive_core::principal::document::id::DocumentId,
) -> SedimentreeId {
    let identifier: keyhive_core::principal::identifier::Identifier = doc_id.into();
    SedimentreeId::new(identifier.to_bytes())
}

/// Convert a keyhive peer ID to a subduction `PeerId`.
const fn to_peer_id(kh_peer_id: &subduction_keyhive::KeyhivePeerId) -> PeerId {
    PeerId::new(*kh_peer_id.verifying_key())
}

/// Create a `VerifiedAuthor` for a given signer by sealing a dummy payload.
///
/// This exercises the real verification path: seal → extract `verified_author()`.
async fn make_verified_author(
    signer: &MemorySigner,
) -> subduction_crypto::verified_author::VerifiedAuthor {
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        id::SedimentreeId as SedId,
        loose_commit::LooseCommit,
    };
    use std::collections::BTreeSet;

    let blob = Blob::new(b"dummy".to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(SedId::new([0; 32]), BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(signer, commit).await;
    verified.verified_author()
}

/// Create a second keyhive, exchange contact cards so they know each other,
/// and return the second keyhive and its peer identity.
async fn create_known_peer(
    host: &SimpleKeyhive,
) -> Result<(SimpleKeyhive, subduction_keyhive::KeyhivePeerId), Box<dyn core::error::Error>> {
    let peer_kh = make_keyhive().await?;
    let peer_id = keyhive_peer_id(&peer_kh);

    let host_cc = host.contact_card().await?;
    let peer_cc = peer_kh.contact_card().await?;

    host.receive_contact_card(&peer_cc).await?;
    peer_kh.receive_contact_card(&host_cc).await?;

    Ok((peer_kh, peer_id))
}

/// Create a keyhive from known signing key bytes and exchange contact cards
/// with the host so the host recognizes it. Returns the keyhive and its peer ID.
///
/// This allows creating a subduction_crypto `MemorySigner` from the same bytes,
/// so the verifying keys match between keyhive identity and subduction signer.
async fn create_known_peer_from_signing_key(
    host: &SimpleKeyhive,
    signing_key_bytes: &[u8; 32],
) -> (SimpleKeyhive, subduction_keyhive::KeyhivePeerId) {
    let kh_signer = keyhive_crypto::signer::memory::MemorySigner::from(
        ed25519_dalek::SigningKey::from_bytes(signing_key_bytes),
    );
    let peer_kh: SimpleKeyhive = keyhive_core::keyhive::Keyhive::generate(
        kh_signer,
        keyhive_core::store::ciphertext::memory::MemoryCiphertextStore::new(),
        keyhive_core::listener::no_listener::NoListener,
        rand::rngs::OsRng,
    )
    .await
    .expect("failed to create keyhive from signing key");

    let peer_id = keyhive_peer_id(&peer_kh);

    let host_cc = host.contact_card().await.unwrap();
    let peer_cc = peer_kh.contact_card().await.unwrap();
    host.receive_contact_card(&peer_cc).await.unwrap();
    peer_kh.receive_contact_card(&host_cc).await.unwrap();

    (peer_kh, peer_id)
}

/// Create a group, add the peer to it with the given access, and create a
/// document owned by that group. Returns the document ID as a `SedimentreeId`.
async fn create_doc_with_peer_access(
    host: &SimpleKeyhive,
    peer_id: &subduction_keyhive::KeyhivePeerId,
    access: Access,
) -> SedimentreeId {
    let group = host.generate_group(vec![]).await.unwrap();
    let group_id = group.lock().await.group_id();

    let peer_identifier = peer_id.to_identifier().unwrap();
    let peer_agent = host.get_agent(peer_identifier).await.unwrap();

    host.add_member(
        peer_agent,
        &Membered::Group(group_id, group.clone()),
        access,
        &[],
    )
    .await
    .unwrap();

    let doc = host
        .generate_doc(
            vec![Peer::Group(group_id, group.clone())],
            nonempty![[0u8; 32]],
        )
        .await
        .unwrap();

    let doc_id = doc.lock().await.doc_id();
    doc_id_to_sedimentree_id(doc_id)
}

// ── ConnectionPolicy Tests ──────────────────────────────────────────────

#[tokio::test]
async fn authorize_connect_known_agent() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;
    let policy = wrap_policy(host_kh);

    let result = ConnectionPolicy::<Local>::authorize_connect(&policy, to_peer_id(&peer_id)).await;
    assert!(result.is_ok(), "known agent should be allowed to connect");

    Ok(())
}

#[tokio::test]
async fn authorize_connect_unknown_agent() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    // Create a peer but DON'T exchange contact cards
    let unknown_kh = make_keyhive().await?;
    let unknown_id = keyhive_peer_id(&unknown_kh);
    let policy = wrap_policy(host_kh);

    let result =
        ConnectionPolicy::<Local>::authorize_connect(&policy, to_peer_id(&unknown_id)).await;
    assert!(result.is_err(), "unknown agent should be rejected");

    Ok(())
}

#[tokio::test]
async fn authorize_connect_invalid_peer_id() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let policy = wrap_policy(host_kh);

    // All-zero bytes is not a valid Ed25519 public key
    let invalid_id = PeerId::new([0u8; 32]);
    let result = ConnectionPolicy::<Local>::authorize_connect(&policy, invalid_id).await;
    assert!(result.is_err(), "invalid peer ID should be rejected");

    Ok(())
}

// ── StoragePolicy::authorize_fetch Tests ────────────────────────────────

#[tokio::test]
async fn authorize_fetch_with_relay_access() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    let sed_id = create_doc_with_peer_access(&host_kh, &peer_id, Access::Relay).await;

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_fetch(&policy, to_peer_id(&peer_id), sed_id).await;
    assert!(
        result.is_ok(),
        "peer with Relay access should be allowed to fetch"
    );

    Ok(())
}

#[tokio::test]
async fn authorize_fetch_without_access() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    // Create a document WITHOUT granting the peer any access
    let doc = host_kh.generate_doc(vec![], nonempty![[0u8; 32]]).await?;
    let doc_id = doc.lock().await.doc_id();
    let sed_id = doc_id_to_sedimentree_id(doc_id);

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_fetch(&policy, to_peer_id(&peer_id), sed_id).await;
    assert!(
        result.is_err(),
        "peer without access should be rejected for fetch"
    );

    Ok(())
}

#[tokio::test]
async fn authorize_fetch_unknown_document() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    // Use a valid SedimentreeId that doesn't correspond to any document
    let signer = MemorySigner::generate();
    let fake_sed_id = SedimentreeId::new(signer.verifying_key().to_bytes());

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_fetch(&policy, to_peer_id(&peer_id), fake_sed_id).await;
    assert!(
        result.is_err(),
        "fetch for unknown document should be rejected"
    );

    Ok(())
}

// ── StoragePolicy::authorize_put Tests ──────────────────────────────────

#[tokio::test]
async fn authorize_put_with_edit_access() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    // Pick signing key bytes we control, then create both a keyhive identity
    // and a subduction_crypto signer from the same bytes so verifying keys match.
    let signing_key_bytes: [u8; 32] = {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes
    };

    let sub_signer = MemorySigner::from_bytes(&signing_key_bytes);
    let (_edit_peer_kh, edit_peer_id) =
        create_known_peer_from_signing_key(&host_kh, &signing_key_bytes).await;

    let sed_id = create_doc_with_peer_access(&host_kh, &edit_peer_id, Access::Edit).await;
    let author = make_verified_author(&sub_signer).await;

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_put(&policy, to_peer_id(&peer_id), author, sed_id).await;
    assert!(
        result.is_ok(),
        "author with Edit access should be allowed to put"
    );

    Ok(())
}

#[tokio::test]
async fn authorize_put_with_only_relay_access() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    // Same technique but with Relay access (not Edit)
    let signing_key_bytes: [u8; 32] = {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        bytes
    };

    let sub_signer = MemorySigner::from_bytes(&signing_key_bytes);
    let (_relay_peer_kh, relay_peer_id) =
        create_known_peer_from_signing_key(&host_kh, &signing_key_bytes).await;

    let sed_id = create_doc_with_peer_access(&host_kh, &relay_peer_id, Access::Relay).await;
    let author = make_verified_author(&sub_signer).await;

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_put(&policy, to_peer_id(&peer_id), author, sed_id).await;
    assert!(
        result.is_err(),
        "author with only Relay access should be rejected for put"
    );

    Ok(())
}

#[tokio::test]
async fn authorize_put_unknown_document() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    let signer = MemorySigner::generate();
    let fake_sed_id = SedimentreeId::new(signer.verifying_key().to_bytes());
    let author = make_verified_author(&signer).await;

    let policy = wrap_policy(host_kh);
    let result =
        StoragePolicy::<Local>::authorize_put(&policy, to_peer_id(&peer_id), author, fake_sed_id)
            .await;
    assert!(
        result.is_err(),
        "put for unknown document should be rejected"
    );

    Ok(())
}

// ── StoragePolicy::filter_authorized_fetch Tests ────────────────────────

#[tokio::test]
async fn filter_authorized_fetch_mixed_access() -> Result<(), Box<dyn core::error::Error>> {
    let host_kh = make_keyhive().await?;
    let (_peer_kh, peer_id) = create_known_peer(&host_kh).await?;

    // Create one document with Relay access for the peer
    let sed_id_with = create_doc_with_peer_access(&host_kh, &peer_id, Access::Relay).await;

    // Create another document WITHOUT granting the peer any access
    let doc_without_access = host_kh.generate_doc(vec![], nonempty![[2u8; 32]]).await?;
    let sed_id_without = doc_id_to_sedimentree_id(doc_without_access.lock().await.doc_id());

    let policy = wrap_policy(host_kh);
    let filtered = StoragePolicy::<Local>::filter_authorized_fetch(
        &policy,
        to_peer_id(&peer_id),
        vec![sed_id_with, sed_id_without],
    )
    .await;

    assert_eq!(filtered.len(), 1, "only one document should be authorized");
    assert_eq!(filtered[0], sed_id_with);

    Ok(())
}
