//! [`KeyhiveAccess`] for `keyhive_core`'s [`Keyhive`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};
use core::error::Error;

use future_form::{FutureForm, Local, Sendable, future_form};
use keyhive_codec::traits::{Decode, Encode};
use keyhive_content::{
    reference::ContentRef,
    store::{CiphertextStore, CiphertextStoreExt},
};
use keyhive_core::{
    archive::Archive,
    certificate::KeylineStorageError,
    contact_card::{ContactCard, ContactCardError},
    content::heads::ContentHeads,
    event::{Event, static_event::StaticEvent},
    keyhive::{Keyhive, ReceiveStaticEventError},
    listener::membership::MembershipListener,
    principal::individual::ReceivePrekeyOpError,
};
use keyhive_crypto::{
    digest::Digest, identifier::Identifier, signer::async_signer::AsyncSigner, verified::SignError,
};
use keyline::contract::Keyline;

use crate::{
    access::{AgentEventIndex, ArchiveBytes, KeyhiveAccess, NotIngested, Undecodable},
    message::{EventBytes, EventHash},
};

/// An error from a [`Keyhive`] operation. `E` is the Keyline store's error.
///
/// Each variant displays and reports its source as the underlying error
/// does.
#[derive(Debug, thiserror::Error)]
pub enum NativeKeyhiveError<E: Error + 'static> {
    /// Reading or writing the Keyline store failed.
    #[error(transparent)]
    Storage(#[from] KeylineStorageError<E>),

    /// Signing failed.
    #[error(transparent)]
    Sign(#[from] SignError),

    /// A received contact card does not decode or verify.
    #[error(transparent)]
    ContactCard(#[from] ContactCardError),

    /// Keyhive rejected a received contact card.
    #[error(transparent)]
    ReceiveContactCard(#[from] ReceivePrekeyOpError),

    /// Keyhive rejected an archive.
    #[error(transparent)]
    IngestArchive(#[from] ReceiveStaticEventError<E>),
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
    type Error = NativeKeyhiveError<K::Error>;

    fn total_ops(&self) -> F::Future<'_, Result<u64, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move { Ok(Keyhive::stats(self).await.total_ops()) })
    }

    fn pending_event_hashes(
        &self,
    ) -> F::Future<'_, Result<Vec<EventHash>, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            let digests = Keyhive::pending_event_hashes(self).await;
            Ok(digests.into_iter().map(|d| digest_to_bytes(&d)).collect())
        })
    }

    fn is_known_agent(
        &self,
        id: Identifier,
    ) -> F::Future<'_, Result<bool, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move { Ok(self.has_agent(id).await?) })
    }

    fn events_for_agent<'a>(
        &'a self,
        id: Identifier,
        within: Option<&'a BTreeSet<EventHash>>,
    ) -> F::Future<'a, Result<BTreeMap<EventHash, EventBytes>, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            Ok(Keyhive::static_events_for_agent(self, id)
                .await
                .map_err(NativeKeyhiveError::from)?
                .into_iter()
                .filter_map(|(digest, event)| {
                    let hash = digest_to_bytes(&digest);
                    within
                        .is_none_or(|within| within.contains(&hash))
                        .then(|| (hash, encode_value(&event)))
                })
                .collect())
        })
    }

    fn event_hashes_for_agent(
        &self,
        id: Identifier,
    ) -> F::Future<'_, Result<BTreeSet<EventHash>, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            Ok(Keyhive::static_events_for_agent(self, id)
                .await
                .map_err(NativeKeyhiveError::from)?
                .keys()
                .map(digest_to_bytes)
                .collect())
        })
    }

    fn all_agent_events<'a>(
        &'a self,
        skip: &'a BTreeSet<EventHash>,
    ) -> F::Future<'a, Result<AgentEventIndex, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            let all = Keyhive::all_agent_events(self)
                .await
                .map_err(NativeKeyhiveError::from)?;

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

    fn sign_bytes(
        &self,
        payload: Vec<u8>,
    ) -> F::Future<'_, Result<Vec<u8>, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            let signed = Keyhive::sign_bytes(self, payload)
                .await
                .map_err(NativeKeyhiveError::from)?;
            Ok(encode_value(signed.signed().as_ref()))
        })
    }

    fn ingest_events(
        &self,
        events: Vec<EventBytes>,
    ) -> F::Future<'_, Result<NotIngested, NativeKeyhiveError<K::Error>>> {
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
    ) -> F::Future<'a, Result<EventBytes, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            let card = ContactCard::from_bytes(card).map_err(NativeKeyhiveError::from)?;
            Keyhive::receive_contact_card(self, &card)
                .await
                .map_err(NativeKeyhiveError::from)?;
            Ok(encode_value(&StaticEvent::from(Event::<T>::from(
                card.op().clone(),
            ))))
        })
    }

    fn to_archive(&self) -> F::Future<'_, Result<ArchiveBytes, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            // The archive holds this replica's secret keys.
            // `encode_zeroizing` writes them into one exact-sized buffer, so
            // no reallocation leaves a copy behind, and the buffer moves out
            // without another copy.
            Keyhive::into_archive(self)
                .await
                .map(|archive| core::mem::take(&mut *archive.encode_zeroizing()))
                .map_err(NativeKeyhiveError::from)
        })
    }

    fn is_current_archive(&self, archive: &[u8]) -> Result<bool, NativeKeyhiveError<K::Error>> {
        Ok(archive
            .split_first_chunk::<4>()
            .is_some_and(|(magic, rest)| {
                *magic == Archive::<T>::MAGIC && rest.first() == Some(&Archive::<T>::FORMAT)
            }))
    }

    fn ingest_archive<'a>(
        &'a self,
        archive: &'a [u8],
    ) -> F::Future<'a, Result<Result<(), Undecodable>, NativeKeyhiveError<K::Error>>> {
        F::from_future(async move {
            let Ok(archive) = decode_value::<Archive<T>>(archive) else {
                return Ok(Err(Undecodable));
            };
            Keyhive::ingest_archive(self, archive)
                .await
                .map(|_pending| Ok(()))
                .map_err(NativeKeyhiveError::from)
        })
    }
}
