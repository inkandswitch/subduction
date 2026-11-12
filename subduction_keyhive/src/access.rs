//! The keyhive operations the sync protocol uses.
//!
//! [`KeyhiveProtocol`](crate::KeyhiveProtocol) calls keyhive only through
//! [`KeyhiveAccess`] and [`KeyhiveCodec`]. With the `native` feature, both are
//! implemented for `keyhive_core`'s `Keyhive`.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec::Vec,
};

use future_form::FutureForm;
use keyhive_crypto::identifier::Identifier;

use crate::message::EventHash;

/// The 32 bytes of an agent's identifier.
pub type AgentId = [u8; 32];

/// Every event each agent is sent, as returned by
/// [`KeyhiveAccess::all_agent_events`].
#[derive(Debug, Clone)]
pub struct AgentEventIndex<E> {
    /// Distinct events by hash, leaving out the hashes the caller asked to
    /// skip.
    pub events: Vec<(EventHash, E)>,

    /// For each agent, the hashes of every event it is sent, skipped ones
    /// included.
    pub agent_hashes: BTreeMap<AgentId, BTreeSet<EventHash>>,
}

/// Conversion between keyhive values and the bytes the protocol stores and
/// sends.
///
/// An event's bytes are what goes on the wire and into storage. Its
/// [`EventHash`] is its digest according to keyhive.
pub trait KeyhiveCodec {
    /// A keyhive event.
    type Event;

    /// A snapshot of keyhive state.
    type Archive;

    /// The error for every operation of this trait and of [`KeyhiveAccess`].
    type Error: core::error::Error + Send + Sync + 'static;

    /// Encode an event.
    fn encode_event(event: &Self::Event) -> Vec<u8>;

    /// Decode an event.
    ///
    /// An implementation may accept any bytes here and leave the check to
    /// [`KeyhiveAccess::ingest_events`]. Loading from storage then fails on an
    /// unreadable stored event instead of skipping it.
    ///
    /// # Errors
    ///
    /// Returns an error if the implementation checks `bytes` here and they
    /// are not an encoded event.
    fn decode_event(bytes: &[u8]) -> Result<Self::Event, Self::Error>;

    /// Encode an archive.
    fn encode_archive(archive: &Self::Archive) -> Vec<u8>;

    /// Decode an archive.
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not an encoded archive.
    fn decode_archive(bytes: &[u8]) -> Result<Self::Archive, Self::Error>;
}

/// The keyhive operations the sync protocol uses.
pub trait KeyhiveAccess<F: FutureForm>: KeyhiveCodec {
    /// The number of delegations, revocations, prekey operations, and CGKA
    /// operations keyhive contains.
    fn total_ops(&self) -> F::Future<'_, Result<u64, Self::Error>>;

    /// Hashes of the events waiting for their dependencies.
    fn pending_event_hashes(&self) -> F::Future<'_, Result<Vec<EventHash>, Self::Error>>;

    /// Whether `id` is this keyhive itself or an individual, document, or
    /// group it knows.
    fn is_known_agent(&self, id: Identifier) -> F::Future<'_, Result<bool, Self::Error>>;

    /// The membership, prekey, and CGKA events `id` is sent.
    #[allow(clippy::type_complexity)]
    fn events_for_agent(
        &self,
        id: Identifier,
    ) -> F::Future<'_, Result<BTreeMap<EventHash, Self::Event>, Self::Error>>;

    /// Every event each agent is sent, with each event once.
    ///
    /// Any event whose hash is in `skip` is left out of
    /// [`AgentEventIndex::events`].
    #[allow(clippy::type_complexity)]
    fn all_agent_events<'a>(
        &'a self,
        skip: &'a BTreeSet<EventHash>,
    ) -> F::Future<'a, Result<AgentEventIndex<Self::Event>, Self::Error>>;

    /// Sign `payload` as this keyhive, returning the `KH` frame of the
    /// `Signed<Bytes>` which `SignedMessage::verify` decodes.
    fn sign_bytes(&self, payload: Vec<u8>) -> F::Future<'_, Result<Vec<u8>, Self::Error>>;

    /// Ingest events in any order, returning the ones still waiting for
    /// their dependencies.
    #[allow(clippy::type_complexity)]
    fn ingest_events(
        &self,
        events: Vec<Self::Event>,
    ) -> F::Future<'_, Result<Vec<Arc<Self::Event>>, Self::Error>>;

    /// Receive a contact card's `KH` frame, returning its prekey operation as
    /// an event to persist.
    fn receive_contact_card<'a>(
        &'a self,
        card: &'a [u8],
    ) -> F::Future<'a, Result<Self::Event, Self::Error>>;

    /// A snapshot of keyhive state.
    fn to_archive(&self) -> F::Future<'_, Result<Self::Archive, Self::Error>>;

    /// Merge an archive into keyhive.
    fn ingest_archive(&self, archive: Self::Archive) -> F::Future<'_, Result<(), Self::Error>>;
}
