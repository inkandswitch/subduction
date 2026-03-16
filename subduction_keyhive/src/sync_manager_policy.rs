//! Keyhive-backed [`ConnectionPolicy`] and [`StoragePolicy`] for
//! [`KeyhiveSyncManager`].
//!
//! Requires the `keyhive` feature.
//!
//! Authorization checks lock the shared `Keyhive`, read membership/access
//! data, drop the lock, and return the result. The lock is never held
//! across I/O or channel operations.
//!
//! [`KeyhiveSyncManager`]: subduction_keyhive::KeyhiveSyncManager

use alloc::vec::Vec;

use ed25519_dalek::VerifyingKey;
use future_form::Local;
use futures::FutureExt;
use keyhive_core::{
    access::Access,
    content::reference::ContentRef,
    crypto::signer::async_signer::AsyncSigner,
    listener::membership::MembershipListener,
    principal::{document::id::DocumentId, identifier::Identifier},
    store::ciphertext::CiphertextStore,
};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};

use crate::{storage::KeyhiveStorage, sync_manager::KeyhiveSyncManager};

// ── Conversion helpers ──────────────────────────────────────────────────

/// Try to convert a [`PeerId`] to a keyhive [`Identifier`].
///
/// Fails if the peer ID bytes are not a valid Ed25519 curve point.
#[must_use]
pub fn try_peer_id_to_identifier(peer_id: PeerId) -> Option<Identifier> {
    let verifying_key = VerifyingKey::from_bytes(peer_id.as_bytes()).ok()?;
    Some(Identifier::from(verifying_key))
}

/// Try to convert a [`SedimentreeId`] to a keyhive [`DocumentId`].
///
/// Fails if the sedimentree ID bytes are not a valid Ed25519 curve point.
#[must_use]
pub fn try_sedimentree_id_to_document_id(sedimentree_id: SedimentreeId) -> Option<DocumentId> {
    let verifying_key = VerifyingKey::from_bytes(sedimentree_id.as_bytes()).ok()?;
    Some(DocumentId::from(Identifier::from(verifying_key)))
}

// ── Error types ─────────────────────────────────────────────────────────

/// Error returned when a connection is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The peer is not a known agent in the keyhive.
    #[error("peer is not a known agent")]
    UnknownAgent,
}

/// Error returned when a fetch operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FetchDisallowedError {
    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The peer does not have sufficient access.
    #[error("peer does not have Pull access")]
    InsufficientAccess,
}

/// Error returned when a put operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutDisallowedError {
    /// The author peer ID is not a valid Ed25519 public key.
    #[error("author ID is not a valid Ed25519 public key")]
    InvalidAuthorId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The author does not have sufficient access.
    #[error("author does not have Write access")]
    InsufficientAccess,
}

// ── ConnectionPolicy ────────────────────────────────────────────────────

impl<S, T, P, C, L, R, Store> ConnectionPolicy<Local>
    for KeyhiveSyncManager<S, T, P, C, L, R, Store>
where
    S: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<S, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
    Store::Error: Send + Sync + 'static,
{
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> futures::future::LocalBoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(peer).ok_or(ConnectionDisallowedError::InvalidPeerId)?;

            let keyhive = self.keyhive().lock().await;
            let is_known = keyhive.get_agent(identifier).await.is_some();
            drop(keyhive);

            if is_known {
                Ok(())
            } else {
                Err(ConnectionDisallowedError::UnknownAgent)
            }
        }
        .boxed_local()
    }
}

// ── StoragePolicy ───────────────────────────────────────────────────────

impl<S, T, P, C, L, R, Store> StoragePolicy<Local> for KeyhiveSyncManager<S, T, P, C, L, R, Store>
where
    S: AsyncSigner + Clone + Send + 'static,
    T: ContentRef + serde::de::DeserializeOwned + Send + Sync + 'static,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<S, T> + Send + 'static,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
    Store::Error: Send + Sync + 'static,
{
    type FetchDisallowed = FetchDisallowedError;
    type PutDisallowed = PutDisallowedError;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> futures::future::LocalBoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(peer).ok_or(FetchDisallowedError::InvalidPeerId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(FetchDisallowedError::InvalidSedimentreeId)?;

            let keyhive = self.keyhive().lock().await;

            let doc = keyhive
                .get_document(doc_id)
                .await
                .ok_or(FetchDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;
            drop(keyhive);

            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Pull)
            {
                Ok(())
            } else {
                Err(FetchDisallowedError::InsufficientAccess)
            }
        }
        .boxed_local()
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> futures::future::LocalBoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(author).ok_or(PutDisallowedError::InvalidAuthorId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(PutDisallowedError::InvalidSedimentreeId)?;

            let keyhive = self.keyhive().lock().await;

            let doc = keyhive
                .get_document(doc_id)
                .await
                .ok_or(PutDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;
            drop(keyhive);

            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Write)
            {
                Ok(())
            } else {
                Err(PutDisallowedError::InsufficientAccess)
            }
        }
        .boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> futures::future::LocalBoxFuture<'_, Vec<SedimentreeId>> {
        async move {
            let Some(identifier) = try_peer_id_to_identifier(peer) else {
                return Vec::new();
            };

            let keyhive = self.keyhive().lock().await;
            let mut authorized = Vec::new();

            for sedimentree_id in ids {
                let Some(doc_id) = try_sedimentree_id_to_document_id(sedimentree_id) else {
                    continue;
                };

                let Some(doc) = keyhive.get_document(doc_id).await else {
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

            drop(keyhive);
            authorized
        }
        .boxed_local()
    }
}
