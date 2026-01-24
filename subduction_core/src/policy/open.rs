//! An open policy that allows all connections and storage operations.

use core::convert::Infallible;
use futures_kind::FutureKind;
use sedimentree_core::id::SedimentreeId;

use super::{ConnectionPolicy, Generation, StoragePolicy};
use crate::peer::id::PeerId;

/// An open policy that allows all connections and storage operations.
///
/// Suitable for testing and scenarios where authorization is handled externally.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenPolicy;

#[futures_kind::kinds(Sendable, Local)]
impl<K: FutureKind> ConnectionPolicy<K> for OpenPolicy {
    type ConnectionDisallowed = core::convert::Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        K::into_kind(async { Ok(()) })
    }
}

#[futures_kind::kinds(Sendable, Local)]
impl<K: FutureKind> StoragePolicy<K> for OpenPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = Infallible;

    fn generation(&self, _sedimentree_id: SedimentreeId) -> K::Future<'_, Generation> {
        K::into_kind(async { Generation::default() })
    }

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::FetchDisallowed>> {
        K::into_kind(async { Ok(()) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PutDisallowed>> {
        K::into_kind(async { Ok(()) })
    }
}
