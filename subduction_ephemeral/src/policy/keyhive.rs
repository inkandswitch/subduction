//! Keyhive-backed [`EphemeralPolicy`] for [`KeyhiveSyncManager`].
//!
//! Requires the `keyhive` feature.
//!
//! Authorization checks lock the shared `Keyhive`, read membership/access
//! data, drop the lock, and return the result. The lock is never held
//! across I/O or channel operations.
//!
//! [`KeyhiveSyncManager`]: subduction_keyhive::KeyhiveSyncManager

use alloc::vec::Vec;

use future_form::Local;
use futures::FutureExt;
use keyhive_core::{
    access::Access, content::reference::ContentRef, crypto::signer::async_signer::AsyncSigner,
    listener::membership::MembershipListener, store::ciphertext::CiphertextStore,
};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::keyhive::{try_peer_id_to_identifier, try_sedimentree_id_to_document_id},
};
use subduction_keyhive::{KeyhiveStorage, KeyhiveSyncManager};

use super::EphemeralPolicy;

// ── Error types ─────────────────────────────────────────────────────────

/// Error returned when an ephemeral subscribe is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubscribeDisallowedError {
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

/// Error returned when an ephemeral publish is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublishDisallowedError {
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
    #[error("peer does not have Write access")]
    InsufficientAccess,
}

// ── EphemeralPolicy ─────────────────────────────────────────────────────

impl<S, T, P, C, L, R, Store> EphemeralPolicy<Local> for KeyhiveSyncManager<S, T, P, C, L, R, Store>
where
    S: AsyncSigner + Clone,
    T: ContentRef + serde::de::DeserializeOwned,
    P: for<'de> serde::Deserialize<'de>,
    C: CiphertextStore<T, P> + Clone,
    L: MembershipListener<S, T>,
    R: rand::CryptoRng + rand::RngCore,
    Store: KeyhiveStorage<Local>,
{
    type SubscribeDisallowed = SubscribeDisallowedError;
    type PublishDisallowed = PublishDisallowedError;

    fn authorize_subscribe(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> futures::future::LocalBoxFuture<'_, Result<(), Self::SubscribeDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(peer).ok_or(SubscribeDisallowedError::InvalidPeerId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(SubscribeDisallowedError::InvalidSedimentreeId)?;

            let keyhive = self.keyhive().lock().await;

            let doc = keyhive
                .get_document(doc_id)
                .await
                .ok_or(SubscribeDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;
            drop(keyhive);

            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Pull)
            {
                Ok(())
            } else {
                Err(SubscribeDisallowedError::InsufficientAccess)
            }
        }
        .boxed_local()
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> futures::future::LocalBoxFuture<'_, Result<(), Self::PublishDisallowed>> {
        async move {
            let identifier =
                try_peer_id_to_identifier(peer).ok_or(PublishDisallowedError::InvalidPeerId)?;

            let doc_id = try_sedimentree_id_to_document_id(sedimentree_id)
                .ok_or(PublishDisallowedError::InvalidSedimentreeId)?;

            let keyhive = self.keyhive().lock().await;

            let doc = keyhive
                .get_document(doc_id)
                .await
                .ok_or(PublishDisallowedError::DocumentNotFound)?;

            let members = doc.lock().await.transitive_members().await;
            drop(keyhive);

            if members
                .get(&identifier)
                .is_some_and(|(_, access)| *access >= Access::Write)
            {
                Ok(())
            } else {
                Err(PublishDisallowedError::InsufficientAccess)
            }
        }
        .boxed_local()
    }

    fn filter_authorized_subscribers(
        &self,
        sedimentree_id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> futures::future::LocalBoxFuture<'_, Vec<PeerId>> {
        async move {
            let Some(doc_id) = try_sedimentree_id_to_document_id(sedimentree_id) else {
                return Vec::new();
            };

            let keyhive = self.keyhive().lock().await;

            let Some(doc) = keyhive.get_document(doc_id).await else {
                drop(keyhive);
                return Vec::new();
            };

            let members = doc.lock().await.transitive_members().await;
            drop(keyhive);

            peers
                .into_iter()
                .filter(|peer| {
                    try_peer_id_to_identifier(*peer).is_some_and(|identifier| {
                        members
                            .get(&identifier)
                            .is_some_and(|(_, access)| *access >= Access::Pull)
                    })
                })
                .collect()
        }
        .boxed_local()
    }
}
