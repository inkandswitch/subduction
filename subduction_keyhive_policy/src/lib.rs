//! Keyhive-based authorization policy for Subduction connections.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate std;

use futures::future::BoxFuture;
use futures::FutureExt;
use futures_kind::Sendable;
use keyhive_core::{
    content::reference::ContentRef, crypto::signer::async_signer::AsyncSigner, keyhive::Keyhive,
    listener::membership::MembershipListener, principal::agent::id::AgentId,
    store::ciphertext::CiphertextStore,
};
use sedimentree_core::id::SedimentreeId;
use serde::Deserialize;
use subduction_core::{peer::id::PeerId, policy::Policy};

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
    pub fn new(keyhive: Keyhive<S, T, P, C, L, R>) -> Self {
        Self(keyhive)
    }
}

impl<
        S: AsyncSigner + Clone,
        T: ContentRef,
        P: for<'de> Deserialize<'de>,
        C: CiphertextStore<T, P> + Clone,
        L: MembershipListener<S, T>,
        R: rand::CryptoRng + rand::RngCore,
    > Policy<Sendable> for SubductionKeyhive<S, T, P, C, L, R>
{
    fn is_connect_allowed(&self, _peer_id: PeerId) -> BoxFuture<'_, bool> {
        async {
            // TODO: Implement proper PeerId -> AgentId conversion
            // This requires the PeerId bytes to be a valid Ed25519 public key
            todo!("implement PeerId to AgentId conversion")
        }
        .boxed()
    }

    fn is_fetch_allowed(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, bool> {
        todo!()
    }

    fn is_put_allowed(
        &self,
        _requestor: PeerId,
        _author: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, bool> {
        todo!()
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

/// Try to convert a [`PeerId`] to an [`AgentId`].
///
/// This conversion may fail because `AgentId` requires a valid Ed25519 public key,
/// while `PeerId` is just arbitrary bytes.
pub fn try_peer_id_to_agent_id(_peer_id: PeerId) -> Option<AgentId> {
    // TODO: Implement proper conversion
    // Need to:
    // 1. Try to parse peer_id bytes as a VerifyingKey
    // 2. Convert to IndividualId
    // 3. Wrap in AgentId::IndividualId
    todo!("implement PeerId to AgentId conversion")
}

// FIXME split out connection policy and storage policy
