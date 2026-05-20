//! Keyhive-backed authorization policy for the CLI server.
//!
//! Legacy (zero-padded) doc IDs are always allowed.

use std::sync::Arc;

use async_lock::Mutex as AsyncMutex;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
use subduction_keyhive::{
    policy::{
        ConnectionDisallowedError, FetchDisallowedError, PutDisallowedError,
        authorize_connect_with, authorize_fetch_with, authorize_put_with,
        filter_authorized_fetch_with,
    },
    runtime::SendableRuntimeKeyhive,
};

fn is_legacy(id: &SedimentreeId) -> bool {
    id.as_bytes()[16..].iter().all(|&b| b == 0)
}

#[derive(Clone, Debug)]
pub(crate) struct CliKeyhivePolicyHandle {
    keyhive: Option<Arc<AsyncMutex<SendableRuntimeKeyhive>>>,
}

impl CliKeyhivePolicyHandle {
    pub(crate) const fn new(keyhive: Arc<AsyncMutex<SendableRuntimeKeyhive>>) -> Self {
        Self {
            keyhive: Some(keyhive),
        }
    }

    pub(crate) const fn open() -> Self {
        Self { keyhive: None }
    }
}

impl ConnectionPolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        let Some(ref keyhive) = self.keyhive else {
            return async { Ok(()) }.boxed();
        };
        let keyhive = Arc::clone(keyhive);
        async move {
            let kh = keyhive.lock().await;
            authorize_connect_with(&*kh, peer).await
        }
        .boxed()
    }
}

impl StoragePolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type FetchDisallowed = FetchDisallowedError;
    type PutDisallowed = PutDisallowedError;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        let Some(ref keyhive) = self.keyhive else {
            return async { Ok(()) }.boxed();
        };
        if is_legacy(&sedimentree_id) {
            return async { Ok(()) }.boxed();
        }
        let keyhive = Arc::clone(keyhive);
        async move {
            let kh = keyhive.lock().await;
            authorize_fetch_with(&*kh, peer, sedimentree_id).await
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let Some(ref keyhive) = self.keyhive else {
            return async { Ok(()) }.boxed();
        };
        if is_legacy(&sedimentree_id) {
            return async { Ok(()) }.boxed();
        }
        let keyhive = Arc::clone(keyhive);
        async move {
            let kh = keyhive.lock().await;
            authorize_put_with(&*kh, requestor, author, sedimentree_id).await
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let Some(ref keyhive) = self.keyhive else {
            return async { ids }.boxed();
        };
        let (legacy, keyhive_ids): (Vec<_>, Vec<_>) = ids.into_iter().partition(is_legacy);
        let keyhive = Arc::clone(keyhive);
        async move {
            let kh = keyhive.lock().await;
            let mut allowed = filter_authorized_fetch_with(&*kh, peer, keyhive_ids).await;
            allowed.extend(legacy);
            allowed
        }
        .boxed()
    }
}
