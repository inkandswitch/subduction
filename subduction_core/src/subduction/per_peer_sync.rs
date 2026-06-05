//! Per-peer outcome of a broadcast / sync round.
//!
//! [`PerPeerSync`] is the return type of
//! [`sync_with_all_peers`](super::Subduction::sync_with_all_peers) and the
//! round-trip `add_*` combinators, letting callers observe which peers acked and
//! which failed. It is a thin newtype over a [`Map`] keyed by [`PeerId`].

use alloc::vec::Vec;
use core::{fmt, ops::Deref};

use future_form::FutureForm;
use sedimentree_core::collections::Map;

use crate::{
    authenticated::Authenticated,
    connection::{managed::CallError, stats::SyncStats},
    peer::id::PeerId,
};

/// The per-peer value in a [`PerPeerSync`]: `(succeeded, stats, per-connection
/// call errors)`.
pub type PeerSyncEntry<Conn, Async, SendErr> = (
    bool,
    SyncStats,
    Vec<(Authenticated<Conn, Async>, CallError<SendErr>)>,
);

/// The underlying map of a [`PerPeerSync`], keyed by [`PeerId`].
pub type PerPeerSyncMap<Conn, Async, SendErr> = Map<PeerId, PeerSyncEntry<Conn, Async, SendErr>>;

/// Per-peer outcome of a broadcast / sync round, keyed by [`PeerId`].
///
/// Returned by [`sync_with_all_peers`](super::Subduction::sync_with_all_peers)
/// and the round-trip `add_*` combinators
/// ([`add_built_batch`](super::Subduction::add_built_batch),
/// [`add_sedimentree`](super::Subduction::add_sedimentree)) so callers can
/// observe which peers acked and which failed.
///
/// Each entry's value is a [`PeerSyncEntry`]. The newtype [`Deref`]s to the
/// underlying [`Map`], so `.get(&peer)`, `.iter()`, `.is_empty()`, etc. work
/// directly; it also implements [`IntoIterator`] and [`FromIterator`].
pub struct PerPeerSync<Conn: Clone, Async: FutureForm, SendErr: core::error::Error>(
    PerPeerSyncMap<Conn, Async, SendErr>,
);

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error>
    PerPeerSync<Conn, Async, SendErr>
{
    /// Wrap a per-peer map.
    #[must_use]
    pub(crate) const fn new(map: PerPeerSyncMap<Conn, Async, SendErr>) -> Self {
        Self(map)
    }

    /// The inner per-peer map.
    #[must_use]
    pub const fn as_map(&self) -> &PerPeerSyncMap<Conn, Async, SendErr> {
        &self.0
    }

    /// Consume into the inner per-peer map.
    #[must_use]
    pub fn into_map(self) -> PerPeerSyncMap<Conn, Async, SendErr> {
        self.0
    }
}

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error> fmt::Debug
    for PerPeerSync<Conn, Async, SendErr>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PerPeerSync")
            .field("peers", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error> Default
    for PerPeerSync<Conn, Async, SendErr>
{
    fn default() -> Self {
        Self(Map::new())
    }
}

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error> Deref
    for PerPeerSync<Conn, Async, SendErr>
{
    type Target = PerPeerSyncMap<Conn, Async, SendErr>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error> IntoIterator
    for PerPeerSync<Conn, Async, SendErr>
{
    type Item = (PeerId, PeerSyncEntry<Conn, Async, SendErr>);
    type IntoIter = <PerPeerSyncMap<Conn, Async, SendErr> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<Conn: Clone, Async: FutureForm, SendErr: core::error::Error>
    FromIterator<(PeerId, PeerSyncEntry<Conn, Async, SendErr>)>
    for PerPeerSync<Conn, Async, SendErr>
{
    fn from_iter<T: IntoIterator<Item = (PeerId, PeerSyncEntry<Conn, Async, SendErr>)>>(
        iter: T,
    ) -> Self {
        Self(iter.into_iter().collect())
    }
}
