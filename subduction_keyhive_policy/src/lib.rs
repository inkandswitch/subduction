//! Keyhive-based authorization policy for Subduction connections.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

use core::fmt;

use ed25519_dalek::VerifyingKey;
use futures::FutureExt;
use futures::future::BoxFuture;
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
use subduction_core::{
    peer::id::PeerId,
    policy::{ConnectionPolicy, Generation, StoragePolicy},
};

/// Error returned when a connection is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    InvalidPeerId,
    /// The peer is not a known agent in the Keyhive.
    UnknownAgent,
}

impl fmt::Display for ConnectionDisallowedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPeerId => write!(f, "peer ID is not a valid Ed25519 public key"),
            Self::UnknownAgent => write!(f, "peer is not a known agent"),
        }
    }
}

impl core::error::Error for ConnectionDisallowedError {}

/// Error returned when a fetch operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    InvalidPeerId,
    /// The sedimentree ID is not a valid Ed25519 public key (document ID).
    InvalidSedimentreeId,
    /// The document does not exist.
    DocumentNotFound,
    /// The peer does not have sufficient access to fetch from this document.
    InsufficientAccess,
}

impl fmt::Display for FetchDisallowedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPeerId => write!(f, "peer ID is not a valid Ed25519 public key"),
            Self::InvalidSedimentreeId => write!(f, "sedimentree ID is not a valid document ID"),
            Self::DocumentNotFound => write!(f, "document not found"),
            Self::InsufficientAccess => write!(f, "peer does not have Pull access"),
        }
    }
}

impl core::error::Error for FetchDisallowedError {}

/// Error returned when a put operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutDisallowedError {
    /// The author peer ID is not a valid Ed25519 public key.
    InvalidAuthorId,
    /// The sedimentree ID is not a valid Ed25519 public key (document ID).
    InvalidSedimentreeId,
    /// The document does not exist.
    DocumentNotFound,
    /// The author does not have sufficient access to write to this document.
    InsufficientAccess,
}

impl fmt::Display for PutDisallowedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAuthorId => write!(f, "author ID is not a valid Ed25519 public key"),
            Self::InvalidSedimentreeId => write!(f, "sedimentree ID is not a valid document ID"),
            Self::DocumentNotFound => write!(f, "document not found"),
            Self::InsufficientAccess => write!(f, "author does not have Write access"),
        }
    }
}

impl core::error::Error for PutDisallowedError {}

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

    fn generation(&self, _sedimentree_id: SedimentreeId) -> BoxFuture<'_, Generation> {
        // TODO: Return the actual membership version from Keyhive document
        // For now, return 0 (no revocation checking)
        async { Generation::default() }.boxed()
    }

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
