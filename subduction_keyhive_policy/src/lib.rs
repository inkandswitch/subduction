//! Keyhive-based authorization policy for Subduction connections.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

use alloc::vec::Vec;

use ed25519_dalek::VerifyingKey;
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
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
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};

/// Error returned when a connection is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The peer is not a known agent in the Keyhive.
    #[error("peer is not a known agent")]
    UnknownAgent,
}

/// Error returned when a fetch operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FetchDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The sedimentree ID is not a valid Ed25519 public key (document ID).
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist.
    #[error("document not found")]
    DocumentNotFound,

    /// The peer does not have sufficient access to fetch from this document.
    #[error("peer does not have Pull access")]
    InsufficientAccess,
}

/// Error returned when a put operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutDisallowedError {
    /// The author peer ID is not a valid Ed25519 public key.
    #[error("author ID is not a valid Ed25519 public key")]
    InvalidAuthorId,

    /// The sedimentree ID is not a valid Ed25519 public key (document ID).
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist.
    #[error("document not found")]
    DocumentNotFound,

    /// The author does not have sufficient access to write to this document.
    #[error("author does not have Write access")]
    InsufficientAccess,
}

/// A wrapper around [`Keyhive`] that implements [`ConnectionPolicy`] and [`StoragePolicy`] for Subduction.
#[allow(missing_debug_implementations)]
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
    #[must_use]
    pub const fn new(keyhive: Keyhive<S, T, P, C, L, R>) -> Self {
        Self(keyhive)
    }

    /// Get a reference to the inner [`Keyhive`].
    #[must_use]
    pub const fn keyhive(&self) -> &Keyhive<S, T, P, C, L, R> {
        &self.0
    }
}

impl<
    S: AsyncSigner + Clone + Send + Sync,
    T: ContentRef + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<T, P> + Clone + Send + Sync,
    L: MembershipListener<S, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
> ConnectionPolicy<Sendable> for SubductionKeyhive<S, T, P, C, L, R>
{
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async move {
            let identifier = try_peer_id_to_identifier(peer_id)
                .ok_or(ConnectionDisallowedError::InvalidPeerId)?;

            if self.0.get_agent(identifier).await.is_some() {
                return Ok(());
            }

            Err(ConnectionDisallowedError::UnknownAgent)
        }
        .boxed()
    }
}

impl<
    S: AsyncSigner + Clone + Send + Sync,
    T: ContentRef + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<T, P> + Clone + Send + Sync,
    L: MembershipListener<S, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
> StoragePolicy<Sendable> for SubductionKeyhive<S, T, P, C, L, R>
{
    type FetchDisallowed = FetchDisallowedError;
    type PutDisallowed = PutDisallowedError;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(peer).ok_or(FetchDisallowedError::InvalidPeerId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(FetchDisallowedError::InvalidSedimentreeId)?;

            let doc = self
                .0
                .get_document(doc_id)
                .await
                .ok_or(FetchDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;

            // Check if the peer has at least Pull access
            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Pull)
            {
                Ok(())
            } else {
                Err(FetchDisallowedError::InsufficientAccess)
            }
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(author).ok_or(PutDisallowedError::InvalidAuthorId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(PutDisallowedError::InvalidSedimentreeId)?;

            let doc = self
                .0
                .get_document(doc_id)
                .await
                .ok_or(PutDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;

            // Check if the author has at least Write access
            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Write)
            {
                Ok(())
            } else {
                Err(PutDisallowedError::InsufficientAccess)
            }
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move {
            let Some(identifier) = try_peer_id_to_identifier(peer) else {
                return Vec::new();
            };

            let mut authorized = Vec::new();
            for sedimentree_id in ids {
                let Some(doc_id) = try_sedimentree_id_to_document_id(sedimentree_id) else {
                    continue;
                };

                let Some(doc) = self.0.get_document(doc_id).await else {
                    continue;
                };

                let members = doc.lock().await.transitive_members().await;

                if members
                    .get(&identifier)
                    .is_some_and(|(_, access)| *access >= Access::Pull)
                {
                    authorized.push(sedimentree_id);
                }
            }

            authorized
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
#[must_use]
pub fn try_peer_id_to_identifier(peer_id: PeerId) -> Option<Identifier> {
    let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).ok()?;
    Some(Identifier::from(verifying_key))
}

/// Try to convert a [`PeerId`] to an [`IndividualId`].
///
/// This conversion may fail because [`IndividualId`] wraps an Ed25519 public key,
/// while [`PeerId`] is arbitrary bytes that may not represent a valid key.
#[must_use]
pub fn try_peer_id_to_individual_id(peer_id: PeerId) -> Option<IndividualId> {
    let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).ok()?;
    Some(IndividualId::from(verifying_key))
}

/// Try to convert a [`SedimentreeId`] to a [`DocumentId`].
///
/// This conversion may fail because [`DocumentId`] wraps an Ed25519 public key,
/// while [`SedimentreeId`] is arbitrary bytes that may not represent a valid key.
#[must_use]
pub fn try_sedimentree_id_to_document_id(sedimentree_id: SedimentreeId) -> Option<DocumentId> {
    let verifying_key = VerifyingKey::from_bytes(sedimentree_id.as_bytes()).ok()?;
    Some(DocumentId::from(Identifier::from(verifying_key)))
}

#[cfg(test)]
mod tests {

    use super::*;
    use ed25519_dalek::SigningKey;

    /// Generate a valid peer ID from a signing key.
    fn valid_peer_id(seed: u8) -> PeerId {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        PeerId::new(*signing_key.verifying_key().as_bytes())
    }

    /// Generate an invalid peer ID (not a valid Ed25519 point).
    ///
    /// Finding truly invalid Ed25519 public key bytes is non-trivial since
    /// ed25519-dalek accepts many byte patterns. This uses a pattern with
    /// specific bytes known to not represent a valid curve point.
    fn invalid_peer_id() -> Option<PeerId> {
        // Try to find bytes that don't represent a valid Ed25519 point
        // by checking various patterns with the high bit manipulated
        for i in 0..=255u8 {
            let mut bytes = [i; 32];
            // Manipulate to try to create invalid point
            bytes[0] = 0xFE;
            bytes[31] = 0x80 | i;
            if VerifyingKey::from_bytes(&bytes).is_err() {
                return Some(PeerId::new(bytes));
            }
        }
        None
    }

    /// Generate a valid sedimentree ID from a signing key.
    fn valid_sedimentree_id(seed: u8) -> SedimentreeId {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        SedimentreeId::new(*signing_key.verifying_key().as_bytes())
    }

    /// Generate an invalid sedimentree ID (not a valid Ed25519 point).
    fn invalid_sedimentree_id() -> Option<SedimentreeId> {
        // Reuse the same pattern search as invalid_peer_id
        for i in 0..=255u8 {
            let mut bytes = [i; 32];
            bytes[0] = 0xFE;
            bytes[31] = 0x80 | i;
            if VerifyingKey::from_bytes(&bytes).is_err() {
                return Some(SedimentreeId::new(bytes));
            }
        }
        None
    }

    mod conversion_functions {
        use super::*;

        #[test]
        fn try_peer_id_to_identifier_succeeds_for_valid_key() {
            let peer_id = valid_peer_id(42);
            let result = try_peer_id_to_identifier(peer_id);
            assert!(result.is_some());
        }

        #[test]
        fn try_peer_id_to_identifier_fails_for_invalid_key() {
            let Some(peer_id) = invalid_peer_id() else {
                // If we can't find an invalid pattern, skip this test
                // (ed25519-dalek may accept all patterns in some configurations)
                return;
            };
            let result = try_peer_id_to_identifier(peer_id);
            assert!(result.is_none());
        }

        #[test]
        fn try_peer_id_to_individual_id_succeeds_for_valid_key() {
            let peer_id = valid_peer_id(42);
            let result = try_peer_id_to_individual_id(peer_id);
            assert!(result.is_some());
        }

        #[test]
        fn try_peer_id_to_individual_id_fails_for_invalid_key() {
            let Some(peer_id) = invalid_peer_id() else {
                return;
            };
            let result = try_peer_id_to_individual_id(peer_id);
            assert!(result.is_none());
        }

        #[test]
        fn try_sedimentree_id_to_document_id_succeeds_for_valid_key() {
            let sed_id = valid_sedimentree_id(42);
            let result = try_sedimentree_id_to_document_id(sed_id);
            assert!(result.is_some());
        }

        #[test]
        fn try_sedimentree_id_to_document_id_fails_for_invalid_key() {
            let Some(sed_id) = invalid_sedimentree_id() else {
                return;
            };
            let result = try_sedimentree_id_to_document_id(sed_id);
            assert!(result.is_none());
        }

        #[test]
        fn different_seeds_produce_different_identifiers() {
            let peer1 = valid_peer_id(1);
            let peer2 = valid_peer_id(2);

            let id1 = try_peer_id_to_identifier(peer1);
            let id2 = try_peer_id_to_identifier(peer2);

            assert!(id1.is_some());
            assert!(id2.is_some());
            assert_ne!(id1, id2);
        }

        #[test]
        fn same_seed_produces_same_identifier() {
            let peer1 = valid_peer_id(42);
            let peer2 = valid_peer_id(42);

            let id1 = try_peer_id_to_identifier(peer1);
            let id2 = try_peer_id_to_identifier(peer2);

            assert_eq!(id1, id2);
        }
    }
}
