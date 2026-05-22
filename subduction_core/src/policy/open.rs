//! An open policy that allows all connections and storage operations.

use alloc::vec::Vec;
use core::convert::Infallible;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::id::SedimentreeId;

use super::{connection::ConnectionPolicy, storage::StoragePolicy};
use crate::peer::id::PeerId;
use subduction_crypto::verified_author::VerifiedAuthor;

/// An open policy that allows all connections and storage operations.
///
/// Suitable for testing and scenarios where authorization is handled externally.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenPolicy;

#[future_form(Sendable, Local)]
impl<Async: FutureForm> ConnectionPolicy<Async> for OpenPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(
        &self,
        _peer: PeerId,
    ) -> Async::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        Async::from_future(async { Ok(()) })
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> StoragePolicy<Async> for OpenPolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = Infallible;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::FetchDisallowed>> {
        Async::from_future(async { Ok(()) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::PutDisallowed>> {
        Async::from_future(async { Ok(()) })
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> Async::Future<'_, Vec<SedimentreeId>> {
        // OpenPolicy allows everything
        Async::from_future(async { ids })
    }
}
