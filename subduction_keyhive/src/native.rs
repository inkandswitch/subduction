//! [`KeyhiveAccess`] for `keyhive_core`'s [`Keyhive`].

use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};
use core::{error::Error, fmt};

use future_form::{FutureForm, Local, Sendable, future_form};
use keyhive_codec::traits::{Decode, Encode};
use keyhive_content::{
    reference::ContentRef,
    store::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_core::{
    archive::Archive,
    contact_card::ContactCard,
    content::heads::ContentHeads,
    event::{Event, static_event::StaticEvent},
    keyhive::Keyhive,
    listener::membership::MembershipListener,
    principal::individual::id::IndividualId,
};
use keyhive_crypto::{digest::Digest, identifier::Identifier, signer::async_signer::AsyncSigner};
use keyline::contract::Keyline;

use crate::{
    access::{AgentEventIndex, ArchiveBytes, KeyhiveAccess, NotIngested, Undecodable},
    message::{EventBytes, EventHash},
};

/// An error from a [`Keyhive`] operation.
///
/// Displays, debug-formats, and reports its source as the underlying error
/// does.
pub struct NativeKeyhiveError(Box<dyn Error + Send + Sync + 'static>);

impl NativeKeyhiveError {
    fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(error))
    }
}

impl fmt::Debug for NativeKeyhiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for NativeKeyhiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Error for NativeKeyhiveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Encode a value with Keyhive's canonical codec.
fn encode_value<V: Encode>(value: &V) -> Vec<u8> {
    value.encode().into_bytes()
}

/// Decode a value with Keyhive's canonical codec, accepting only its
/// canonical encoding.
fn decode_value<V: Decode>(bytes: &[u8]) -> Result<V, Undecodable> {
    V::decode(bytes).map_err(|_| Undecodable)
}

const fn digest_to_bytes<U>(digest: &Digest<U>) -> EventHash {
    *digest.raw.as_bytes()
}

#[future_form(
    Sendable where
        S: Send + Sync,
        T: Send + Sync + 'static,
        P: Send + Sync,
        C: Send + Sync,
        L: Send + Sync,
        R: Send + Sync,
        K: Send + Sync,
    Local
)]
impl<F, S, T, P, C, L, R, K> KeyhiveAccess<F> for Keyhive<F, S, T, P, C, L, R, K>
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
    type Error = NativeKeyhiveError;

    fn total_ops(&self) -> F::Future<'_, Result<u64, NativeKeyhiveError>> {
        F::from_future(async move {
            let stats = Keyhive::stats(self).await;
            Ok(stats.delegations
                + stats.revocations
                + stats.prekeys_expanded
                + stats.prekey_rotations
                + stats.cgka_operations)
        })
    }

    fn pending_event_hashes(&self) -> F::Future<'_, Result<Vec<EventHash>, NativeKeyhiveError>> {
        F::from_future(async move {
            let digests = Keyhive::pending_event_hashes(self).await;
            Ok(digests.into_iter().map(|d| digest_to_bytes(&d)).collect())
        })
    }

    fn is_known_agent(&self, id: Identifier) -> F::Future<'_, Result<bool, NativeKeyhiveError>> {
        F::from_future(async move {
            Ok(Keyhive::id(self) == IndividualId::from(id)
                || self.has_individual(id.into()).await
                || self.has_document(id.into()).await
                || self
                    .has_group(id.into())
                    .await
                    .map_err(NativeKeyhiveError::new)?)
        })
    }

    fn events_for_agent(
        &self,
        id: Identifier,
    ) -> F::Future<'_, Result<BTreeMap<EventHash, EventBytes>, NativeKeyhiveError>> {
        F::from_future(async move {
            Ok(Keyhive::static_events_for_agent(self, id)
                .await
                .map_err(NativeKeyhiveError::new)?
                .into_iter()
                .map(|(digest, event)| (digest_to_bytes(&digest), encode_value(&event)))
                .collect())
        })
    }

    fn all_agent_events<'a>(
        &'a self,
        skip: &'a BTreeSet<EventHash>,
    ) -> F::Future<'a, Result<AgentEventIndex, NativeKeyhiveError>> {
        F::from_future(async move {
            let all = Keyhive::all_agent_events(self)
                .await
                .map_err(NativeKeyhiveError::new)?;

            let events = all
                .events
                .iter()
                .filter_map(|(digest, event)| {
                    let h = digest_to_bytes(digest);
                    (!skip.contains(&h))
                        .then(|| (h, encode_value(&StaticEvent::from(event.clone()))))
                })
                .collect();

            let agent_hashes = all
                .agents()
                .into_iter()
                .map(|agent| {
                    let hashes = all.digests_for(agent).iter().map(digest_to_bytes).collect();
                    (agent.to_bytes(), hashes)
                })
                .collect();

            Ok(AgentEventIndex {
                events,
                agent_hashes,
            })
        })
    }

    fn sign_bytes(&self, payload: Vec<u8>) -> F::Future<'_, Result<Vec<u8>, NativeKeyhiveError>> {
        F::from_future(async move {
            let signed = Keyhive::sign_bytes(self, payload)
                .await
                .map_err(NativeKeyhiveError::new)?;
            Ok(encode_value(signed.signed().as_ref()))
        })
    }

    fn ingest_events(
        &self,
        events: Vec<EventBytes>,
    ) -> F::Future<'_, Result<NotIngested, NativeKeyhiveError>> {
        F::from_future(async move {
            let mut decoded = Vec::with_capacity(events.len());
            let mut undecodable = Vec::new();
            for bytes in events {
                match decode_value::<StaticEvent<T>>(&bytes) {
                    Ok(event) => decoded.push(event),
                    Err(Undecodable) => undecodable.push(bytes),
                }
            }
            let pending = self
                .ingest_unsorted_static_events(decoded)
                .await
                .iter()
                .map(|event| encode_value(event.as_ref()))
                .collect();
            Ok(NotIngested {
                pending,
                undecodable,
            })
        })
    }

    fn receive_contact_card<'a>(
        &'a self,
        card: &'a [u8],
    ) -> F::Future<'a, Result<EventBytes, NativeKeyhiveError>> {
        F::from_future(async move {
            let card = ContactCard::from_bytes(card).map_err(NativeKeyhiveError::new)?;
            Keyhive::receive_contact_card(self, &card)
                .await
                .map_err(NativeKeyhiveError::new)?;
            Ok(encode_value(&StaticEvent::from(Event::<T>::from(
                card.op().clone(),
            ))))
        })
    }

    fn to_archive(&self) -> F::Future<'_, Result<ArchiveBytes, NativeKeyhiveError>> {
        F::from_future(async move {
            Keyhive::into_archive(self)
                .await
                .map(|archive| encode_value(&archive))
                .map_err(NativeKeyhiveError::new)
        })
    }

    fn ingest_archive<'a>(
        &'a self,
        archive: &'a [u8],
    ) -> F::Future<'a, Result<Result<(), Undecodable>, NativeKeyhiveError>> {
        F::from_future(async move {
            let Ok(archive) = decode_value::<Archive<T>>(archive) else {
                return Ok(Err(Undecodable));
            };
            Keyhive::ingest_archive(self, archive)
                .await
                .map(|_pending| Ok(()))
                .map_err(NativeKeyhiveError::new)
        })
    }
}
