//! Keyhive-based authorization policy for Subduction connections.
//!
//! Provides [`SubductionKeyhive`], a wrapper around [`Keyhive`] that implements
//! [`ConnectionPolicy`] and [`StoragePolicy`], plus standalone `authorize_*`
//! functions for callers that hold a `&Keyhive` directly (e.g. an actor on a
//! `LocalSet`).
//!
//! Access is Keyhive's Keyline membership: the effective [`Power`] a peer
//! holds over a document ([`Keyhive::access_for_doc`]).
//!
//! | Check   | Rule                                              |
//! |---------|---------------------------------------------------|
//! | Connect | The peer ID is a valid identifier                 |
//! | Fetch   | The peer has at least [`FETCH_POWER`] (`Relay`)   |
//! | Put     | The _requestor_ has at least [`PUT_POWER`] (`Relay`) |
//!
//! Put checks the connected peer, not the commit's author. Whether an
//! author's write counts, including after a revocation, is the
//! materializer's decision. Checking the author against current state would
//! make acceptance depend on arrival order, and replicas would diverge. See
//! [`subduction_keyline::policy`] for the full argument.
//!
//! There is no public access: Keyline has no public agent.

extern crate alloc;

use alloc::vec::Vec;

use future_form::{FutureForm, Local, Sendable};
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};
use keyhive_codec::traits::{Encode, StreamDecode};
use keyhive_core::{
    certificate::KeylineStorageError,
    content::heads::ContentHeads,
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::{document::id::DocumentId, individual::id::IndividualId},
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{
    content::reference::ContentRef, identifier::Identifier, signer::async_signer::AsyncSigner,
};
use keyline::{contract::Keyline, power::Power};
use sedimentree_core::id::SedimentreeId;
use serde::Deserialize;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
pub use subduction_keyline::policy::{
    ConnectionDisallowedError, FETCH_POWER, FetchDisallowedError, InsufficientPowerError,
    PUT_POWER, PutDisallowedError,
};
use subduction_keyline::policy::{peer_identifier, sedimentree_identifier};

/// The error a Keyhive membership query fails with, for store `K`.
pub type AccessError<F, K> = KeylineStorageError<<K as Keyline<F>>::Error>;

/// A wrapper around [`Keyhive`] that implements [`ConnectionPolicy`] and [`StoragePolicy`] for Subduction.
#[allow(missing_debug_implementations)]
pub struct SubductionKeyhive<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
>(Keyhive<F, S, T, P, C, L, R, K>);

impl<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
> SubductionKeyhive<F, S, T, P, C, L, R, K>
{
    /// Create a new [`SubductionKeyhive`] from a [`Keyhive`].
    #[must_use]
    pub const fn new(keyhive: Keyhive<F, S, T, P, C, L, R, K>) -> Self {
        Self(keyhive)
    }

    /// Get a reference to the inner [`Keyhive`].
    #[must_use]
    pub const fn keyhive(&self) -> &Keyhive<F, S, T, P, C, L, R, K> {
        &self.0
    }
}

impl<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
> From<Keyhive<F, S, T, P, C, L, R, K>> for SubductionKeyhive<F, S, T, P, C, L, R, K>
{
    fn from(keyhive: Keyhive<F, S, T, P, C, L, R, K>) -> Self {
        SubductionKeyhive(keyhive)
    }
}

impl<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
> From<SubductionKeyhive<F, S, T, P, C, L, R, K>> for Keyhive<F, S, T, P, C, L, R, K>
{
    fn from(subduction_keyhive: SubductionKeyhive<F, S, T, P, C, L, R, K>) -> Self {
        subduction_keyhive.0
    }
}

impl<
    S: AsyncSigner<Sendable> + Clone + Send + Sync,
    T: ContentRef + Encode + StreamDecode + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<Sendable, T, P> + CiphertextStoreExt<Sendable, T, P> + Clone + Send + Sync,
    L: MembershipListener<Sendable, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
    K: Keyline<Sendable, RetentionWatermark = ContentHeads<T>> + Send + Sync,
> ConnectionPolicy<Sendable> for SubductionKeyhive<Sendable, S, T, P, C, L, R, K>
{
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        authorize_connect_with(&self.0, peer_id).boxed()
    }
}

impl<
    S: AsyncSigner<Sendable> + Clone + Send + Sync,
    T: ContentRef + Encode + StreamDecode + Send + Sync,
    P: for<'de> Deserialize<'de> + Send + Sync,
    C: CiphertextStore<Sendable, T, P> + CiphertextStoreExt<Sendable, T, P> + Clone + Send + Sync,
    L: MembershipListener<Sendable, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
    K: Keyline<Sendable, RetentionWatermark = ContentHeads<T>> + Send + Sync,
> StoragePolicy<Sendable> for SubductionKeyhive<Sendable, S, T, P, C, L, R, K>
where
    K::Error: Send + Sync + 'static,
{
    type FetchDisallowed = FetchDisallowedError<AccessError<Sendable, K>>;
    type PutDisallowed = PutDisallowedError<AccessError<Sendable, K>>;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        authorize_fetch_with(&self.0, peer, sedimentree_id).boxed()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        authorize_put_with(&self.0, requestor, author, sedimentree_id).boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        filter_authorized_fetch_with(&self.0, peer, ids).boxed()
    }
}

// ── Local policy impls ──────────────────────────────────────────────────
//
// Natural form for `!Send` keyhive_core. Used in Wasm and other
// single-threaded contexts.

impl<
    S: AsyncSigner<Local> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<Local, T, P> + CiphertextStoreExt<Local, T, P> + Clone,
    L: MembershipListener<Local, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<Local, RetentionWatermark = ContentHeads<T>>,
> ConnectionPolicy<Local> for SubductionKeyhive<Local, S, T, P, C, L, R, K>
{
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> LocalBoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        authorize_connect_with(&self.0, peer_id).boxed_local()
    }
}

impl<
    S: AsyncSigner<Local> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<Local, T, P> + CiphertextStoreExt<Local, T, P> + Clone,
    L: MembershipListener<Local, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<Local, RetentionWatermark = ContentHeads<T>>,
> StoragePolicy<Local> for SubductionKeyhive<Local, S, T, P, C, L, R, K>
where
    K::Error: Send + Sync + 'static,
{
    type FetchDisallowed = FetchDisallowedError<AccessError<Local, K>>;
    type PutDisallowed = PutDisallowedError<AccessError<Local, K>>;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        authorize_fetch_with(&self.0, peer, sedimentree_id).boxed_local()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::PutDisallowed>> {
        authorize_put_with(&self.0, requestor, author, sedimentree_id).boxed_local()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> LocalBoxFuture<'_, Vec<SedimentreeId>> {
        filter_authorized_fetch_with(&self.0, peer, ids).boxed_local()
    }
}

/// Authorize a connection from `peer_id`.
///
/// Connect is permissive: any peer whose ID is a valid identifier may
/// connect, whether or not it is a known agent. Per-document fetch and put
/// checks gate actual access. Without this, a fresh peer could never
/// onboard: the server learns about new agents over a connection.
///
/// # Errors
///
/// Returns [`ConnectionDisallowedError::InvalidPeerId`] if `peer_id` is not
/// a valid identifier.
#[allow(clippy::unused_async)]
pub async fn authorize_connect_with<F, S, T, P, C, L, R, K>(
    _keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    peer_id: PeerId,
) -> Result<(), ConnectionDisallowedError>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
{
    subduction_keyline::policy::authorize_connect(peer_id)
}

/// Authorize a fetch of `sedimentree_id` by `peer`: at least
/// [`FETCH_POWER`] over the document.
///
/// # Errors
///
/// Returns [`FetchDisallowedError`] for invalid IDs, too little power, or a
/// failed membership query.
pub async fn authorize_fetch_with<F, S, T, P, C, L, R, K>(
    keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    peer: PeerId,
    sedimentree_id: SedimentreeId,
) -> Result<(), FetchDisallowedError<AccessError<F, K>>>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
{
    let peer = peer_identifier(peer).map_err(FetchDisallowedError::InvalidPeerId)?;
    let doc = sedimentree_identifier(sedimentree_id)
        .map_err(FetchDisallowedError::InvalidSedimentreeId)?;
    let actual = access(keyhive, peer, doc)
        .await
        .map_err(FetchDisallowedError::Storage)?;
    InsufficientPowerError::require(FETCH_POWER, actual)?;
    Ok(())
}

/// Authorize a put to `sedimentree_id` by `requestor`: at least
/// [`PUT_POWER`] over the document. The commit's `author` is not checked;
/// see the [module documentation](self).
///
/// # Errors
///
/// Returns [`PutDisallowedError`] for invalid IDs, too little power, or a
/// failed membership query.
pub async fn authorize_put_with<F, S, T, P, C, L, R, K>(
    keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    requestor: PeerId,
    _author: VerifiedAuthor,
    sedimentree_id: SedimentreeId,
) -> Result<(), PutDisallowedError<AccessError<F, K>>>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
{
    let requestor = peer_identifier(requestor).map_err(PutDisallowedError::InvalidRequestorId)?;
    let doc =
        sedimentree_identifier(sedimentree_id).map_err(PutDisallowedError::InvalidSedimentreeId)?;
    let actual = access(keyhive, requestor, doc)
        .await
        .map_err(PutDisallowedError::Storage)?;
    InsufficientPowerError::require(PUT_POWER, actual)?;
    Ok(())
}

/// The subset of `ids` that `peer` may fetch, in order. A failed membership
/// query denies that ID and is logged.
pub async fn filter_authorized_fetch_with<F, S, T, P, C, L, R, K>(
    keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    peer: PeerId,
    ids: Vec<SedimentreeId>,
) -> Vec<SedimentreeId>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
{
    let mut authorized = Vec::with_capacity(ids.len());
    for id in ids {
        match authorize_fetch_with(keyhive, peer, id).await {
            Ok(()) => authorized.push(id),
            Err(FetchDisallowedError::Storage(e)) => {
                tracing::warn!(sedimentree_id = %id, error = %e, "keyhive membership query failed; denying fetch");
            }
            Err(
                FetchDisallowedError::InvalidPeerId(_)
                | FetchDisallowedError::InvalidSedimentreeId(_)
                | FetchDisallowedError::InsufficientPower(_),
            ) => {}
        }
    }
    authorized
}

/// `who`'s effective power over `doc`, or `None` for no access.
async fn access<F, S, T, P, C, L, R, K>(
    keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    who: Identifier,
    doc: Identifier,
) -> Result<Option<Power>, AccessError<F, K>>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef + Encode + StreamDecode,
    P: for<'de> Deserialize<'de>,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
{
    keyhive.access_for_doc(who, DocumentId::from(doc)).await
}

/// Try to convert a [`PeerId`] to an [`Identifier`].
///
/// Fails if the bytes are not a canonical Ed25519 point of large order.
#[must_use]
pub fn try_peer_id_to_identifier(peer_id: PeerId) -> Option<Identifier> {
    peer_identifier(peer_id).ok()
}

/// Try to convert a [`PeerId`] to an [`IndividualId`].
///
/// Fails if the bytes are not a canonical Ed25519 point of large order.
#[must_use]
pub fn try_peer_id_to_individual_id(peer_id: PeerId) -> Option<IndividualId> {
    peer_identifier(peer_id).ok().map(IndividualId::from)
}

/// Try to convert a [`SedimentreeId`] to a [`DocumentId`].
///
/// Fails if the bytes are not a canonical Ed25519 point of large order.
#[must_use]
pub fn try_sedimentree_id_to_document_id(sedimentree_id: SedimentreeId) -> Option<DocumentId> {
    sedimentree_identifier(sedimentree_id)
        .ok()
        .map(DocumentId::from)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    /// The identity point: small order, so never a valid identifier.
    const SMALL_ORDER: [u8; 32] = {
        let mut bytes = [0; 32];
        bytes[0] = 1;
        bytes
    };

    fn valid_peer_id(seed: u8) -> PeerId {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        PeerId::new(*signing_key.verifying_key().as_bytes())
    }

    fn valid_sedimentree_id(seed: u8) -> SedimentreeId {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        SedimentreeId::new(*signing_key.verifying_key().as_bytes())
    }

    #[test]
    fn conversions_accept_valid_keys() {
        assert!(try_peer_id_to_identifier(valid_peer_id(42)).is_some());
        assert!(try_peer_id_to_individual_id(valid_peer_id(42)).is_some());
        assert!(try_sedimentree_id_to_document_id(valid_sedimentree_id(42)).is_some());
    }

    #[test]
    fn conversions_reject_small_order_points() {
        assert!(try_peer_id_to_identifier(PeerId::new(SMALL_ORDER)).is_none());
        assert!(try_peer_id_to_individual_id(PeerId::new(SMALL_ORDER)).is_none());
        assert!(try_sedimentree_id_to_document_id(SedimentreeId::new(SMALL_ORDER)).is_none());
    }

    #[test]
    fn conversions_preserve_identity() {
        assert_eq!(
            try_peer_id_to_identifier(valid_peer_id(1)),
            try_peer_id_to_identifier(valid_peer_id(1))
        );
        assert_ne!(
            try_peer_id_to_identifier(valid_peer_id(1)),
            try_peer_id_to_identifier(valid_peer_id(2))
        );
    }
}
