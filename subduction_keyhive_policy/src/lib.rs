//! Keyhive-based authorization policy for Subduction connections.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

use ed25519_dalek::VerifyingKey;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures_kind::Sendable;
use keyhive_core::{
    access::Access,
    content::reference::ContentRef,
    crypto::signer::async_signer::AsyncSigner,
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::{document::id::DocumentId, identifier::Identifier, individual::id::IndividualId},
    store::ciphertext::CiphertextStore,
};
use sedimentree_core::id::SedimentreeId;
use serde::Deserialize;
use subduction_core::{peer::id::PeerId, policy::{ConnectionPolicy, StoragePolicy}};

/// A wrapper around [`Keyhive`] that implements [`ConnectionPolicy`] and [`StoragePolicy`] for Subduction.
pub struct SubductionKeyhive<
    S: AsyncSigner + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<S, T>,
    R: rand::CryptoRng + rand::RngCore,
>(Keyhive<S, T, P, C, L, R>);

impl<
        S: AsyncSigner + Clone,
        T: ContentRef,
        P: for<'de> Deserialize<'de>,
        C: CiphertextStore<T, P> + Clone,
        L: MembershipListener<S, T>,
        R: rand::CryptoRng + rand::RngCore,
    > SubductionKeyhive<S, T, P, C, L, R>
{
    /// Create a new [`SubductionKeyhive`] from a [`Keyhive`].
    pub fn new(keyhive: Keyhive<S, T, P, C, L, R>) -> Self {
        Self(keyhive)
    }

    /// Get a reference to the inner [`Keyhive`].
    pub fn keyhive(&self) -> &Keyhive<S, T, P, C, L, R> {
        &self.0
    }
}

impl<S, T, P, C, L, R> ConnectionPolicy<Sendable> for SubductionKeyhive<S, T, P, C, L, R>
where
    S: AsyncSigner + Clone + Send + Sync,
    T: ContentRef + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<T, P> + Clone + Send + Sync,
    L: MembershipListener<S, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
{
    fn is_connect_allowed(&self, peer_id: PeerId) -> BoxFuture<'_, bool> {
        async move {
            let Some(identifier) = try_peer_id_to_identifier(peer_id) else {
                // FIXME add errors to the policy
                return false;
            };

            if self.0.get_agent(identifier).await.is_some() {
                return true;
            }

            false
        }
        .boxed()
    }
}

impl<S, T, P, C, L, R> StoragePolicy<Sendable> for SubductionKeyhive<S, T, P, C, L, R>
where
    S: AsyncSigner + Clone + Send + Sync,
    T: ContentRef + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<T, P> + Clone + Send + Sync,
    L: MembershipListener<S, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
{
    fn is_fetch_allowed(&self, peer: PeerId, sedimentree_id: SedimentreeId) -> BoxFuture<'_, bool> {
        async move {
            let Some(identifier) = try_peer_id_to_identifier(peer) else {
                return false;
            };

            let Some(doc_id) = try_sedimentree_id_to_document_id(sedimentree_id) else {
                return false;
            };

            let Some(doc) = self.0.get_document(doc_id).await else {
                return false;
            };

            let members = doc.lock().await.transitive_members().await;

            // Check if the peer has at least Pull access
            members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Pull)
        }
        .boxed()
    }

    fn is_put_allowed(
        &self,
        _requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, bool> {
        async move {
            let Some(identifier) = try_peer_id_to_identifier(author) else {
                return false;
            };

            let Some(doc_id) = try_sedimentree_id_to_document_id(sedimentree_id) else {
                return false;
            };

            let Some(doc) = self.0.get_document(doc_id).await else {
                return false;
            };

            let members = doc.lock().await.transitive_members().await;

            // Check if the author has at least Write access
            members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Write)
        }
        .boxed()
    }
}

impl<
        S: AsyncSigner + Clone,
        T: ContentRef,
        P: for<'de> Deserialize<'de>,
        C: CiphertextStore<T, P> + Clone,
        L: MembershipListener<S, T>,
        R: rand::CryptoRng + rand::RngCore,
    > From<Keyhive<S, T, P, C, L, R>> for SubductionKeyhive<S, T, P, C, L, R>
{
    fn from(keyhive: Keyhive<S, T, P, C, L, R>) -> Self {
        SubductionKeyhive(keyhive)
    }
}

impl<
        S: AsyncSigner + Clone,
        T: ContentRef,
        P: for<'de> Deserialize<'de>,
        C: CiphertextStore<T, P> + Clone,
        L: MembershipListener<S, T>,
        R: rand::CryptoRng + rand::RngCore,
    > From<SubductionKeyhive<S, T, P, C, L, R>> for Keyhive<S, T, P, C, L, R>
{
    fn from(subduction_keyhive: SubductionKeyhive<S, T, P, C, L, R>) -> Self {
        subduction_keyhive.0
    }
}

/// Try to convert a [`PeerId`] to an [`Identifier`].
///
/// This conversion may fail because [`Identifier`] wraps an Ed25519 public key,
/// while [`PeerId`] is arbitrary bytes that may not represent a valid key.
pub fn try_peer_id_to_identifier(peer_id: PeerId) -> Option<Identifier> {
    let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).ok()?;
    Some(Identifier::from(verifying_key))
}

/// Try to convert a [`PeerId`] to an [`IndividualId`].
///
/// This conversion may fail because [`IndividualId`] wraps an Ed25519 public key,
/// while [`PeerId`] is arbitrary bytes that may not represent a valid key.
pub fn try_peer_id_to_individual_id(peer_id: PeerId) -> Option<IndividualId> {
    let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).ok()?;
    Some(IndividualId::from(verifying_key))
}

/// Try to convert a [`SedimentreeId`] to a [`DocumentId`].
///
/// This conversion may fail because [`DocumentId`] wraps an Ed25519 public key,
/// while [`SedimentreeId`] is arbitrary bytes that may not represent a valid key.
pub fn try_sedimentree_id_to_document_id(sedimentree_id: SedimentreeId) -> Option<DocumentId> {
    let verifying_key = VerifyingKey::from_bytes(sedimentree_id.as_bytes()).ok()?;
    Some(DocumentId::from(Identifier::from(verifying_key)))
}
