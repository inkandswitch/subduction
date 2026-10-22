//! [`KeylinePolicy`]: Subduction's connection and storage policy over a
//! [`Keyline`] store.
//!
//! A sedimentree's ID is the Keyline subject (the document's
//! [`Identifier`]), and a peer's ID is the audience.
//!
//! | Check   | Rule                                                        |
//! |---------|-------------------------------------------------------------|
//! | Connect | The peer ID parses as an [`Identifier`]                     |
//! | Fetch   | `effective_power(doc, peer) >= FETCH_POWER`                 |
//! | Put     | `effective_power(doc, requestor) >= PUT_POWER`              |
//!
//! # Why put checks the requestor, not the author
//!
//! Checking the author against the store's current state makes acceptance
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
//! Connect is permissive: a fresh peer has to be able to connect before it
//! can learn the certificates that grant it anything.

use alloc::vec::Vec;

use future_form::{FutureForm, Local, Sendable, future_form};
use keyhive_crypto::identifier::{Identifier, InvalidIdentifier};
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

/// Subduction's [`ConnectionPolicy`] and [`StoragePolicy`] over a
/// [`Keyline`] store.
///
/// See the [module documentation](self) for the rules.
#[derive(Debug, Clone, Default)]
pub struct KeylinePolicy<K> {
    keyline: K,
}

impl<K> KeylinePolicy<K> {
    /// Authorize against `keyline`.
    #[must_use]
    pub const fn new(keyline: K) -> Self {
        Self { keyline }
    }

    /// The store this policy reads.
    #[must_use]
    pub const fn keyline(&self) -> &K {
        &self.keyline
    }

    /// Recover the store.
    #[must_use]
    pub fn into_keyline(self) -> K {
        self.keyline
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm, K> ConnectionPolicy<Async> for KeylinePolicy<K> {
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> Async::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        Async::from_future(async move {
            peer_identifier(peer).map_err(ConnectionDisallowedError::InvalidPeerId)?;
            Ok(())
        })
    }
}

#[future_form(Sendable where K: Sync, K::Error: Send, Local)]
impl<Async: FutureForm, K: Keyline<Async>> StoragePolicy<Async> for KeylinePolicy<K>
where
    K::Error: 'static,
{
    type FetchDisallowed = FetchDisallowedError<K::Error>;
    type PutDisallowed = PutDisallowedError<K::Error>;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::FetchDisallowed>> {
        Async::from_future(async move {
            let peer = peer_identifier(peer).map_err(FetchDisallowedError::InvalidPeerId)?;
            fetch(&self.keyline, peer, sedimentree_id).await?;
            Ok(())
        })
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        _author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::PutDisallowed>> {
        Async::from_future(async move {
            let requestor =
                peer_identifier(requestor).map_err(PutDisallowedError::InvalidRequestorId)?;
            let doc = sedimentree_identifier(sedimentree_id)
                .map_err(PutDisallowedError::InvalidSedimentreeId)?;
            let actual = effective_power(&self.keyline, doc, requestor)
                .await
                .map_err(PutDisallowedError::Storage)?;
            InsufficientPowerError::require(PUT_POWER, actual)?;
            Ok(())
        })
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> Async::Future<'_, Vec<SedimentreeId>> {
        Async::from_future(async move {
            let Ok(peer) = peer_identifier(peer) else {
                return Vec::new();
            };

            let mut authorized = Vec::with_capacity(ids.len());
            for id in ids {
                match fetch(&self.keyline, peer, id).await {
                    Ok(_) => authorized.push(id),
                    Err(FetchDisallowedError::Storage(e)) => {
                        tracing::warn!(sedimentree_id = %id, error = %e, "keyline store failed; denying fetch");
                    }
                    Err(
                        FetchDisallowedError::InvalidPeerId(_)
                        | FetchDisallowedError::InvalidSedimentreeId(_)
                        | FetchDisallowedError::InsufficientPower(_),
                    ) => {}
                }
            }
            authorized
        })
    }
}

/// Parse a [`PeerId`] as a Keyline [`Identifier`].
///
/// # Errors
///
/// Returns [`InvalidIdentifier`] if the bytes are not a canonical Ed25519
/// point of large order.
pub fn peer_identifier(peer: PeerId) -> Result<Identifier, InvalidIdentifier> {
    Identifier::from_bytes(*peer.as_bytes())
}

/// Parse a [`SedimentreeId`] as the Keyline [`Identifier`] of its document.
///
/// # Errors
///
/// Returns [`InvalidIdentifier`] if the bytes are not a canonical Ed25519
/// point of large order.
pub fn sedimentree_identifier(id: SedimentreeId) -> Result<Identifier, InvalidIdentifier> {
    Identifier::from_bytes(*id.as_bytes())
}

/// The connected peer's ID is not a valid identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionDisallowedError {
    /// The peer ID is not a valid identifier.
    #[error("peer ID is not a valid identifier")]
    InvalidPeerId(#[source] InvalidIdentifier),
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

    /// The Keyline store failed.
    #[error("keyline store failed")]
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

    /// The Keyline store failed.
    #[error("keyline store failed")]
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
    fn require(required: Power, actual: Option<Power>) -> Result<Power, Self> {
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

async fn effective_power<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    subject: Identifier,
    audience: Identifier,
) -> Result<Option<Power>, K::Error> {
    Ok(keyline.effective_power(subject, audience).await?.value)
}

async fn fetch<F: FutureForm, K: Keyline<F>>(
    keyline: &K,
    peer: Identifier,
    sedimentree_id: SedimentreeId,
) -> Result<Power, FetchDisallowedError<K::Error>> {
    let doc = sedimentree_identifier(sedimentree_id)
        .map_err(FetchDisallowedError::InvalidSedimentreeId)?;
    let actual = effective_power(keyline, doc, peer)
        .await
        .map_err(FetchDisallowedError::Storage)?;
    Ok(InsufficientPowerError::require(FETCH_POWER, actual)?)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]
mod tests {
    use super::*;

    use alloc::{collections::BTreeSet, vec};

    use futures::executor::block_on;
    use keyline::{
        certificate::VerifiedCertificate,
        delegation::Delegation,
        test_utils::{Issued, assume_verified, delegation, id, revocation},
    };
    use keyline_memory::MemoryKeyline;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        loose_commit::{LooseCommit, id::CommitId},
    };
    use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
    use testresult::TestResult;

    type Store = MemoryKeyline<()>;
    type Policy = KeylinePolicy<Store>;

    /// The identity point: small order, so never a valid identifier.
    const SMALL_ORDER: [u8; 32] = {
        let mut bytes = [0; 32];
        bytes[0] = 1;
        bytes
    };

    fn peer(n: u8) -> PeerId {
        PeerId::new(id(n).to_bytes())
    }

    fn doc(n: u8) -> SedimentreeId {
        SedimentreeId::new(id(n).to_bytes())
    }

    /// A verified author with fixture identity `n`, by sealing a commit.
    fn author(n: u8) -> VerifiedAuthor {
        let signer = MemorySigner::from_bytes(&[n; 32]);
        let commit = LooseCommit::new(
            doc(n),
            CommitId::new([n; 32]),
            BTreeSet::new(),
            BlobMeta::new(&Blob::new(vec![n])),
        );
        block_on(Signed::seal::<Sendable, _>(&signer, commit)).verified_author()
    }

    fn grant(issuer: u8, audience: u8, subject: u8, power: Power) -> Issued<Delegation> {
        delegation(id(issuer), id(audience), id(subject), power)
    }

    fn policy<I: IntoIterator<Item = VerifiedCertificate<()>>>(certs: I) -> Policy {
        let store = Store::new();
        block_on(<Store as Keyline<Sendable>>::insert(
            &store,
            certs.into_iter().collect(),
        ))
        .expect("the memory store is infallible");
        KeylinePolicy::new(store)
    }

    fn connect(policy: &Policy, peer: PeerId) -> Result<(), ConnectionDisallowedError> {
        block_on(<Policy as ConnectionPolicy<Sendable>>::authorize_connect(
            policy, peer,
        ))
    }

    fn fetch(
        policy: &Policy,
        peer: PeerId,
        doc: SedimentreeId,
    ) -> Result<(), FetchDisallowedError<core::convert::Infallible>> {
        block_on(<Policy as StoragePolicy<Sendable>>::authorize_fetch(
            policy, peer, doc,
        ))
    }

    fn put(
        policy: &Policy,
        requestor: PeerId,
        author: VerifiedAuthor,
        doc: SedimentreeId,
    ) -> Result<(), PutDisallowedError<core::convert::Infallible>> {
        block_on(<Policy as StoragePolicy<Sendable>>::authorize_put(
            policy, requestor, author, doc,
        ))
    }

    fn filter(policy: &Policy, peer: PeerId, ids: Vec<SedimentreeId>) -> Vec<SedimentreeId> {
        block_on(<Policy as StoragePolicy<Sendable>>::filter_authorized_fetch(policy, peer, ids))
    }

    #[test]
    fn denies_peer_without_route_with_its_power() {
        let policy = policy([assume_verified(grant(1, 2, 1, Power::Relay))]);

        assert_eq!(fetch(&policy, peer(2), doc(1)), Ok(()));
        assert_eq!(
            fetch(&policy, peer(3), doc(1)),
            Err(FetchDisallowedError::InsufficientPower(
                InsufficientPowerError {
                    required: FETCH_POWER,
                    actual: None,
                }
            ))
        );
    }

    #[test]
    fn grants_through_a_role_at_the_minimum_along_the_route() -> TestResult {
        const DOC: u8 = 1;
        const ROLE: u8 = 10;
        const ALICE: u8 = 2;

        let policy = policy([
            // Supply: the role holds Read over the document.
            assume_verified(grant(DOC, ROLE, DOC, Power::Read)),
            // Membership: Alice holds Edit in the role.
            assume_verified(grant(ROLE, ALICE, ROLE, Power::Edit)),
        ]);

        fetch(&policy, peer(ALICE), doc(DOC))?;
        put(&policy, peer(ALICE), author(ALICE), doc(DOC))?;

        let power = block_on(<Store as Keyline<Sendable>>::effective_power(
            policy.keyline(),
            id(DOC),
            id(ALICE),
        ))?
        .value;
        assert_eq!(power, Some(Power::Read));
        Ok(())
    }

    #[test]
    fn revocation_removes_access() {
        let granted = grant(1, 2, 1, Power::Edit);
        let revoked = revocation::<()>(id(1), granted.digest());
        let policy = policy([assume_verified(granted), assume_verified(revoked)]);

        assert!(matches!(
            fetch(&policy, peer(2), doc(1)),
            Err(FetchDisallowedError::InsufficientPower(_))
        ));
        assert!(matches!(
            put(&policy, peer(2), author(2), doc(1)),
            Err(PutDisallowedError::InsufficientPower(_))
        ));
    }

    #[test]
    fn put_gates_the_requestor_not_the_author() {
        const DOC: u8 = 1;
        const RELAY: u8 = 2;
        const STRANGER: u8 = 3;
        const OWNER: u8 = 4;

        let policy = policy([
            assume_verified(grant(DOC, RELAY, DOC, Power::Relay)),
            assume_verified(grant(DOC, OWNER, DOC, Power::Admin)),
        ]);

        // An authorized relay may push a stranger's commit.
        assert_eq!(
            put(&policy, peer(RELAY), author(STRANGER), doc(DOC)),
            Ok(())
        );

        // A stranger may not push even the owner's commit.
        assert!(matches!(
            put(&policy, peer(STRANGER), author(OWNER), doc(DOC)),
            Err(PutDisallowedError::InsufficientPower(_))
        ));
    }

    #[test]
    fn rejects_invalid_identifiers() {
        let policy = policy([assume_verified(grant(1, 2, 1, Power::Admin))]);
        let bad_peer = PeerId::new(SMALL_ORDER);
        let bad_doc = SedimentreeId::new(SMALL_ORDER);

        assert!(matches!(
            connect(&policy, bad_peer),
            Err(ConnectionDisallowedError::InvalidPeerId(_))
        ));
        assert_eq!(connect(&policy, peer(99)), Ok(()));

        assert!(matches!(
            fetch(&policy, bad_peer, doc(1)),
            Err(FetchDisallowedError::InvalidPeerId(_))
        ));
        assert!(matches!(
            fetch(&policy, peer(2), bad_doc),
            Err(FetchDisallowedError::InvalidSedimentreeId(_))
        ));
        assert!(matches!(
            put(&policy, bad_peer, author(2), doc(1)),
            Err(PutDisallowedError::InvalidRequestorId(_))
        ));
        assert!(matches!(
            put(&policy, peer(2), author(2), bad_doc),
            Err(PutDisallowedError::InvalidSedimentreeId(_))
        ));
    }

    #[test]
    fn filter_keeps_authorized_ids_in_order() {
        let policy = policy([
            assume_verified(grant(1, 9, 1, Power::Relay)),
            assume_verified(grant(3, 9, 3, Power::Read)),
        ]);
        let ids = vec![
            doc(3),
            doc(2),
            SedimentreeId::new(SMALL_ORDER),
            doc(1),
            doc(3),
        ];

        assert_eq!(
            filter(&policy, peer(9), ids.clone()),
            vec![doc(3), doc(1), doc(3)]
        );
        assert_eq!(filter(&policy, PeerId::new(SMALL_ORDER), ids), vec![]);
    }

    /// Over random certificate sets, fetch and put agree with
    /// `effective_power(doc, peer)` at their thresholds, whoever the author
    /// is. Catches a swapped `subject` / `audience`, which both are
    /// `Identifier`s and so type-check either way.
    #[test]
    fn decisions_match_effective_power() {
        /// Identities `1..=IDENTITIES`, small enough that routes form.
        const IDENTITIES: u8 = 5;
        let ident = |n: u8| n % IDENTITIES + 1;

        bolero::check!()
            .with_type::<(Vec<(u8, u8, u8, u8)>, Vec<(u8, u8)>, (u8, u8, u8))>()
            .for_each(|(grants, revokes, (p, d, a))| {
                let delegations: Vec<Issued<Delegation>> = grants
                    .iter()
                    .map(|&(issuer, audience, subject, power)| {
                        grant(
                            ident(issuer),
                            ident(audience),
                            ident(subject),
                            Power::ALL[usize::from(power % 4)],
                        )
                    })
                    .collect();

                let revocations = revokes.iter().filter_map(|&(issuer, target)| {
                    let target = delegations.get(usize::from(target) % delegations.len().max(1))?;
                    Some(assume_verified(revocation::<()>(
                        id(ident(issuer)),
                        target.digest(),
                    )))
                });

                let certs: Vec<_> = delegations
                    .iter()
                    .copied()
                    .map(assume_verified)
                    .chain(revocations)
                    .collect();
                let policy = policy(certs);

                let (p, d, a) = (ident(*p), ident(*d), ident(*a));
                let power = block_on(<Store as Keyline<Sendable>>::effective_power(
                    policy.keyline(),
                    id(d),
                    id(p),
                ))
                .expect("the memory store is infallible")
                .value;

                assert_eq!(
                    fetch(&policy, peer(p), doc(d)).is_ok(),
                    power >= Some(FETCH_POWER)
                );
                assert_eq!(
                    put(&policy, peer(p), author(a), doc(d)).is_ok(),
                    power >= Some(PUT_POWER)
                );
            });
    }
}
