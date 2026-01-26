//! An open policy that allows all connections and storage operations.

use alloc::vec::Vec;
use core::convert::Infallible;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::id::SedimentreeId;

use super::{ConnectionPolicy, StoragePolicy};
use crate::peer::id::PeerId;

/// An open policy that allows all connections and storage operations.
///
/// Suitable for testing and scenarios where authorization is handled externally.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenPolicy;

#[future_form(Sendable, Local)]
impl<K: FutureForm> ConnectionPolicy<K> for OpenPolicy {
    type ConnectionDisallowed = core::convert::Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        K::from_future(async { Ok(()) })
    }
}

#[future_form(Sendable, Local)]
impl<K: FutureForm> StoragePolicy<K> for OpenPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::FetchDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PutDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> K::Future<'_, Vec<SedimentreeId>> {
        // OpenPolicy allows everything
        K::from_future(async { ids })
    }
}
