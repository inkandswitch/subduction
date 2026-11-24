//! The keyhive operations the sync protocol uses.
//!
//! [`KeyhiveProtocol`](crate::KeyhiveProtocol) calls keyhive only through
//! [`KeyhiveAccess`]. With the `native` feature, it is implemented for
//! `keyhive_core`'s `Keyhive`.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};

use future_form::FutureForm;
use keyhive_crypto::identifier::Identifier;

use crate::message::{EventBytes, EventHash};

/// The 32 bytes of an agent's identifier.
pub type AgentId = [u8; 32];

/// Every event each agent is sent, as returned by
/// [`KeyhiveAccess::all_agent_events`].
#[derive(Debug, Clone)]
pub struct AgentEventIndex {
    /// Distinct events by hash, leaving out the hashes the caller asked to
    /// skip.
    pub events: Vec<(EventHash, EventBytes)>,

    /// For each agent, the hashes of every event it is sent, skipped ones
    /// included.
    pub agent_hashes: BTreeMap<AgentId, BTreeSet<EventHash>>,
}

/// A snapshot of keyhive state, in keyhive's canonical encoding.
pub type ArchiveBytes = Vec<u8>;

/// The events [`KeyhiveAccess::ingest_events`] did not ingest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotIngested {
    /// Events waiting for their dependencies.
    pub pending: Vec<EventBytes>,

    /// Events that do not decode.
    pub undecodable: Vec<EventBytes>,
}

/// Bytes that are not an encoded keyhive value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Undecodable;

/// The keyhive operations the sync protocol uses.
pub trait KeyhiveAccess<F: FutureForm> {
    /// The error for every operation.
    type Error: core::error::Error + Send + Sync + 'static;

    /// The number of delegations, revocations, prekey operations, and CGKA
    /// operations keyhive contains.
    fn total_ops(&self) -> F::Future<'_, Result<u64, Self::Error>>;

    /// Hashes of the events waiting for their dependencies.
    fn pending_event_hashes(&self) -> F::Future<'_, Result<Vec<EventHash>, Self::Error>>;

    /// Whether `id` is this keyhive itself or an individual, document, or
    /// group it knows.
    fn is_known_agent(&self, id: Identifier) -> F::Future<'_, Result<bool, Self::Error>>;

    /// The membership, prekey, and CGKA events `id` is sent.
    ///
    /// If `within` is not `None`, an event whose hash is not in `within`
    /// is excluded.
    #[allow(clippy::type_complexity)]
    fn events_for_agent<'a>(
        &'a self,
        id: Identifier,
        within: Option<&'a BTreeSet<EventHash>>,
    ) -> F::Future<'a, Result<BTreeMap<EventHash, EventBytes>, Self::Error>>;

    /// The hashes of the events [`Self::events_for_agent`] returns for `id`,
    /// without their bytes.
    fn event_hashes_for_agent(
        &self,
        id: Identifier,
    ) -> F::Future<'_, Result<BTreeSet<EventHash>, Self::Error>>;

    /// Every event each agent is sent, with each event once.
    ///
    /// Any event whose hash is in `skip` is excluded from
    /// [`AgentEventIndex::events`].
    #[allow(clippy::type_complexity)]
    fn all_agent_events<'a>(
        &'a self,
        skip: &'a BTreeSet<EventHash>,
    ) -> F::Future<'a, Result<AgentEventIndex, Self::Error>>;

    /// Sign `payload` as this keyhive, returning the `KH` frame of the
    /// `Signed<Bytes>` which `SignedMessage::verify` decodes.
    fn sign_bytes(&self, payload: Vec<u8>) -> F::Future<'_, Result<Vec<u8>, Self::Error>>;

    /// Ingest events in any order, returning the ones not ingested.
    ///
    /// Events that do not decode are skipped and returned as
    /// [`NotIngested::undecodable`]. An implementation may instead fail the
    /// whole call on them.
    fn ingest_events(
        &self,
        events: Vec<EventBytes>,
    ) -> F::Future<'_, Result<NotIngested, Self::Error>>;

    /// Receive a contact card's `KH` frame, returning its prekey operation as
    /// an event to persist.
    fn receive_contact_card<'a>(
        &'a self,
        card: &'a [u8],
    ) -> F::Future<'a, Result<EventBytes, Self::Error>>;

    /// A snapshot of keyhive state.
    fn to_archive(&self) -> F::Future<'_, Result<ArchiveBytes, Self::Error>>;

    /// Whether `archive` is in the archive format this keyhive writes.
    ///
    /// It judges by the header alone, so it is cheap. An archive from an older
    /// or newer version carries another header. Storage keeps such an
    /// archive under another key rather than overwriting it.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if the format cannot be judged.
    fn is_current_archive(&self, archive: &[u8]) -> Result<bool, Self::Error>;

    /// Merge an archive into keyhive.
    ///
    /// Returns `Ok(Err(Undecodable))`, leaving keyhive unchanged, if
    /// `archive` does not decode. An implementation may instead fail the call.
    #[allow(clippy::type_complexity)]
    fn ingest_archive<'a>(
        &'a self,
        archive: &'a [u8],
    ) -> F::Future<'a, Result<Result<(), Undecodable>, Self::Error>>;
}
