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
//! A sedimentree's ID is the document's [`Identifier`], and a peer's ID is
//! the member's.
//!
//! | Check   | Rule                                                 |
//! |---------|------------------------------------------------------|
//! | Connect | The peer ID is a valid identifier                    |
//! | Fetch   | The peer has at least [`FETCH_POWER`] (`Relay`)      |
//! | Put     | The _requestor_ has at least [`PUT_POWER`] (`Relay`) |
//!
//! There is no public access: Keyline has no public agent.
//!
//! # Why put checks the requestor, not the author
//!
//! Checking the author against the current membership makes acceptance
//! depend on arrival order. A replica that hears of a revocation first would
//! reject a commit that another replica already accepted, and sedimentree
//! only grows, so they would never converge. Checking the connected peer
//! instead only ever _delays_ a commit: any other authorized holder can push
//! it later.
//!
//! Whether an author's write counts (including revocation retention
//! watermarks) is the materializer's decision, made at read time from the
//! commits and certificates. Both only grow, so every replica renders the
//! same result from the same sets.
//!
//! Connect is permissive beyond a well-formed ID: a fresh peer has to be
//! able to connect before it can learn the certificates that grant it
//! anything.

extern crate alloc;

use alloc::vec::Vec;

use future_form::{FutureForm, Local, Sendable};
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};
use keyhive_codec::traits::Decode;
use keyhive_core::{
    certificate::KeylineStorageError,
    content::heads::ContentHeads,
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::{document::id::DocumentId, individual::id::IndividualId},
    store::ciphertext::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_crypto::{
    content::reference::ContentRef,
    identifier::{Identifier, InvalidIdentifier},
    signer::async_signer::AsyncSigner,
};
use keyline::{contract::Keyline, power::Power};
use sedimentree_core::id::SedimentreeId;

use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;

/// The least power a peer needs over a document to fetch it.
pub const FETCH_POWER: Power = Power::Relay;

/// The least power a requestor needs over a document to push data into it.
///
/// `Relay` rather than `Edit`: servers push content they did not write, and
/// readers push CGKA operations.
pub const PUT_POWER: Power = Power::Relay;

/// The error a Keyhive membership query fails with, for store `K`.
pub type AccessError<F, K> = KeylineStorageError<<K as Keyline<F>>::Error>;

/// A wrapper around [`Keyhive`] that implements [`ConnectionPolicy`] and [`StoragePolicy`] for Subduction.
#[allow(missing_debug_implementations)]
pub struct SubductionKeyhive<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: Decode,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
>(Keyhive<F, S, T, P, C, L, R, K>);

impl<
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: Decode,
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
    P: Decode,
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
    P: Decode,
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
    T: ContentRef + Send + Sync,
    P: Decode + Send + Sync,
    C: CiphertextStore<Sendable, T, P> + CiphertextStoreExt<Sendable, T, P> + Clone + Send + Sync,
    L: MembershipListener<Sendable, T> + Send + Sync,
    R: rand::CryptoRng + rand::RngCore + Send + Sync,
    K: Keyline<Sendable, RetentionWatermark = ContentHeads<T>> + Send + Sync,
> ConnectionPolicy<Sendable> for SubductionKeyhive<Sendable, S, T, P, C, L, R, K>
{
    type ConnectionDisallowed = InvalidIdentifier;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        authorize_connect_with(&self.0, peer_id).boxed()
    }
}

impl<
    S: AsyncSigner<Sendable> + Clone + Send + Sync,
    T: ContentRef + Send + Sync,
    P: Decode + Send + Sync,
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
    T: ContentRef,
    P: Decode,
    C: CiphertextStore<Local, T, P> + CiphertextStoreExt<Local, T, P> + Clone,
    L: MembershipListener<Local, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<Local, RetentionWatermark = ContentHeads<T>>,
> ConnectionPolicy<Local> for SubductionKeyhive<Local, S, T, P, C, L, R, K>
{
    type ConnectionDisallowed = InvalidIdentifier;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> LocalBoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        authorize_connect_with(&self.0, peer_id).boxed_local()
    }
}

impl<
    S: AsyncSigner<Local> + Clone,
    T: ContentRef,
    P: Decode,
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
/// Returns [`InvalidIdentifier`] if `peer_id` is not
/// a valid identifier.
#[allow(clippy::unused_async)]
pub async fn authorize_connect_with<F, S, T, P, C, L, R, K>(
    _keyhive: &Keyhive<F, S, T, P, C, L, R, K>,
    peer_id: PeerId,
) -> Result<(), InvalidIdentifier>
where
    F: FutureForm,
    S: AsyncSigner<F> + Clone,
    T: ContentRef,
    P: Decode,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
{
    peer_identifier(peer_id).map(|_| ())
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
    T: ContentRef,
    P: Decode,
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
    T: ContentRef,
    P: Decode,
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
    T: ContentRef,
    P: Decode,
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
    T: ContentRef,
    P: Decode,
    C: CiphertextStore<F, T, P> + CiphertextStoreExt<F, T, P> + Clone,
    L: MembershipListener<F, T>,
    R: rand::CryptoRng + rand::RngCore,
    K: Keyline<F, RetentionWatermark = ContentHeads<T>>,
    K::Error: Send + Sync + 'static,
{
    keyhive.access_for_doc(who, DocumentId::from(doc)).await
}

/// Parse a [`PeerId`] as an [`Identifier`].
///
/// # Errors
///
/// Returns [`InvalidIdentifier`] if the bytes are not a canonical Ed25519
/// point of large order. Such a key can never appear in Keyline.
pub fn peer_identifier(peer: PeerId) -> Result<Identifier, InvalidIdentifier> {
    Identifier::from_bytes(*peer.as_bytes())
}

/// Parse a [`SedimentreeId`] as the [`Identifier`] of its document.
///
/// # Errors
///
/// Returns [`InvalidIdentifier`] if the bytes are not a canonical Ed25519
/// point of large order.
pub fn sedimentree_identifier(id: SedimentreeId) -> Result<Identifier, InvalidIdentifier> {
    Identifier::from_bytes(*id.as_bytes())
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

/// Why a fetch was denied.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchDisallowedError<E> {
    /// The peer ID is not a valid identifier.
    #[error("peer ID is not a valid identifier")]
    InvalidPeerId(#[source] InvalidIdentifier),

    /// The sedimentree ID is not a valid identifier.
    #[error("sedimentree ID is not a valid identifier")]
    InvalidSedimentreeId(#[source] InvalidIdentifier),

    /// The peer's power over the document is too low.
    #[error(transparent)]
    InsufficientPower(#[from] InsufficientPowerError),

    /// The membership query failed.
    #[error("keyhive membership query failed")]
    Storage(#[source] E),
}

/// Why a put was denied.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PutDisallowedError<E> {
    /// The requestor's peer ID is not a valid identifier.
    #[error("requestor ID is not a valid identifier")]
    InvalidRequestorId(#[source] InvalidIdentifier),

    /// The sedimentree ID is not a valid identifier.
    #[error("sedimentree ID is not a valid identifier")]
    InvalidSedimentreeId(#[source] InvalidIdentifier),

    /// The requestor's power over the document is too low.
    #[error(transparent)]
    InsufficientPower(#[from] InsufficientPowerError),

    /// The membership query failed.
    #[error("keyhive membership query failed")]
    Storage(#[source] E),
}

/// A peer's power over a document is below what the operation requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("requires at least {required}, has {}", describe(*actual))]
pub struct InsufficientPowerError {
    required: Power,
    actual: Option<Power>,
}

impl InsufficientPowerError {
    /// The power the operation requires.
    #[must_use]
    pub const fn required(&self) -> Power {
        self.required
    }

    /// The peer's effective power, or `None` for no access.
    #[must_use]
    pub const fn actual(&self) -> Option<Power> {
        self.actual
    }

    /// The granted power if `actual` is at least `required`.
    ///
    /// # Errors
    ///
    /// Returns the shortfall if `actual` is below `required` or `None`.
    pub fn require(required: Power, actual: Option<Power>) -> Result<Power, Self> {
        actual
            .filter(|power| *power >= required)
            .ok_or(Self { required, actual })
    }
}

const fn describe(power: Option<Power>) -> &'static str {
    match power {
        None => "no access",
        Some(Power::Relay) => "Relay",
        Some(Power::Read) => "Read",
        Some(Power::Edit) => "Edit",
        Some(Power::Admin) => "Admin",
    }
}

#[cfg(test)]
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

    /// The authorization rules, against a real [`Keyhive`].
    #[cfg(feature = "serde")]
    mod with_keyhive {
        use super::*;

        use alloc::{collections::BTreeSet, vec};

        use future_form::Local;
        use keyhive_core::principal::group::id::GroupId;
        use nonempty::nonempty;
        use sedimentree_core::{
            blob::{Blob, BlobMeta},
            loose_commit::{LooseCommit, id::CommitId},
        };
        use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
        use testresult::TestResult;

        use crate::test_utils::{SimpleKeyhive, exchange_all_contact_cards, make_keyhive};

        fn identifier(kh: &SimpleKeyhive) -> Identifier {
            kh.id().into()
        }

        fn peer(kh: &SimpleKeyhive) -> PeerId {
            PeerId::new(identifier(kh).to_bytes())
        }

        fn tree(doc: DocumentId) -> SedimentreeId {
            SedimentreeId::new(Identifier::from(doc).to_bytes())
        }

        /// Some verified author. Put never checks who.
        async fn author(seed: u8) -> VerifiedAuthor {
            let signer = MemorySigner::from_bytes(&[seed; 32]);
            let commit = LooseCommit::new(
                SedimentreeId::new([seed; 32]),
                CommitId::new([seed; 32]),
                BTreeSet::new(),
                BlobMeta::new(&Blob::new(vec![seed])),
            );
            Signed::seal::<Local, _>(&signer, commit)
                .await
                .verified_author()
        }

        /// An owner with a fresh document, and `others` who know the
        /// owner (and so can be added as members).
        async fn setup(
            others: usize,
        ) -> TestResult<(SimpleKeyhive, DocumentId, Vec<SimpleKeyhive>)> {
            let owner = make_keyhive().await;
            let mut peers = Vec::with_capacity(others);
            for _ in 0..others {
                let peer = make_keyhive().await;
                exchange_all_contact_cards(&[&owner, &peer]).await;
                peers.push(peer);
            }
            let doc = owner.generate_doc(vec![], nonempty![[0u8; 32]]).await?;
            Ok((owner, doc, peers))
        }

        #[tokio::test(flavor = "current_thread")]
        async fn fetch_needs_relay() -> TestResult {
            let (owner, doc, peers) = setup(2).await?;
            let [relay, stranger] = peers.as_slice() else {
                return Err("two peers".into());
            };
            owner
                .add_member(identifier(relay), doc, Power::Relay)
                .await?;

            authorize_fetch_with(&owner, peer(relay), tree(doc)).await?;
            let denied = authorize_fetch_with(&owner, peer(stranger), tree(doc)).await;
            assert!(matches!(
                denied,
                Err(FetchDisallowedError::InsufficientPower(e)) if e.actual().is_none()
            ));
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn put_checks_the_requestor_not_the_author() -> TestResult {
            let (owner, doc, peers) = setup(2).await?;
            let [relay, stranger] = peers.as_slice() else {
                return Err("two peers".into());
            };
            owner
                .add_member(identifier(relay), doc, Power::Relay)
                .await?;

            // An authorized relay may push anyone's commit.
            authorize_put_with(&owner, peer(relay), author(7).await, tree(doc)).await?;

            // A stranger may push no one's.
            let denied =
                authorize_put_with(&owner, peer(stranger), author(8).await, tree(doc)).await;
            assert!(matches!(
                denied,
                Err(PutDisallowedError::InsufficientPower(_))
            ));
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn revocation_removes_access() -> TestResult {
            let (owner, doc, peers) = setup(1).await?;
            let [reader] = peers.as_slice() else {
                return Err("one peer".into());
            };
            owner
                .add_member(identifier(reader), doc, Power::Read)
                .await?;
            authorize_fetch_with(&owner, peer(reader), tree(doc)).await?;

            owner.revoke_member(identifier(reader), true, doc).await?;
            assert!(matches!(
                authorize_fetch_with(&owner, peer(reader), tree(doc)).await,
                Err(FetchDisallowedError::InsufficientPower(_))
            ));
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn access_flows_through_a_group() -> TestResult {
            let (owner, doc, peers) = setup(1).await?;
            let [member] = peers.as_slice() else {
                return Err("one peer".into());
            };
            let group: GroupId = owner.generate_group(vec![]).await?;
            owner
                .add_member(identifier(member), group, Power::Read)
                .await?;
            assert!(
                authorize_fetch_with(&owner, peer(member), tree(doc))
                    .await
                    .is_err()
            );

            owner
                .add_member(Identifier::from(group), doc, Power::Read)
                .await?;
            authorize_fetch_with(&owner, peer(member), tree(doc)).await?;
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn filter_keeps_authorized_ids_in_order() -> TestResult {
            let (owner, shared, peers) = setup(1).await?;
            let [relay] = peers.as_slice() else {
                return Err("one peer".into());
            };
            let private = owner.generate_doc(vec![], nonempty![[1u8; 32]]).await?;
            owner
                .add_member(identifier(relay), shared, Power::Relay)
                .await?;

            let ids = vec![
                tree(private),
                tree(shared),
                SedimentreeId::new(SMALL_ORDER),
                tree(private),
                tree(shared),
            ];
            assert_eq!(
                filter_authorized_fetch_with(&owner, peer(relay), ids).await,
                vec![tree(shared), tree(shared)]
            );
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn invalid_identifiers_are_refused() -> TestResult {
            let (owner, doc, _) = setup(0).await?;
            let bad_peer = PeerId::new(SMALL_ORDER);
            let bad_tree = SedimentreeId::new(SMALL_ORDER);

            assert_eq!(
                authorize_connect_with(&owner, bad_peer).await,
                Err(InvalidIdentifier::SmallOrder)
            );
            authorize_connect_with(&owner, peer(&owner)).await?;

            assert!(matches!(
                authorize_fetch_with(&owner, bad_peer, tree(doc)).await,
                Err(FetchDisallowedError::InvalidPeerId(_))
            ));
            assert!(matches!(
                authorize_fetch_with(&owner, peer(&owner), bad_tree).await,
                Err(FetchDisallowedError::InvalidSedimentreeId(_))
            ));
            assert!(matches!(
                authorize_put_with(&owner, bad_peer, author(1).await, tree(doc)).await,
                Err(PutDisallowedError::InvalidRequestorId(_))
            ));
            assert!(matches!(
                authorize_put_with(&owner, peer(&owner), author(1).await, bad_tree).await,
                Err(PutDisallowedError::InvalidSedimentreeId(_))
            ));
            Ok(())
        }
    }
}
