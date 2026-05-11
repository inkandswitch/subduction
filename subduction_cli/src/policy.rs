//! Subduction policy for the CLI server.
//!
//! Same actor-bridge pattern as `keyhive.rs`: a `Send + Sync` handle forwards
//! `authorize_*` calls to the keyhive thread.

use std::sync::Arc;

use async_channel::{Receiver, Sender};
use async_lock::Mutex as AsyncMutex;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_crypto::verified_author::VerifiedAuthor;
use subduction_keyhive_policy::{
    ConnectionDisallowedError, FetchDisallowedError, PutDisallowedError, authorize_connect_with,
    authorize_fetch_with, authorize_put_with, filter_authorized_fetch_with,
};

use crate::keyhive::CliKeyhive;

const POLICY_COMMAND_CAPACITY: usize = 1024;

#[allow(missing_debug_implementations)]
pub(crate) enum PolicyCommand {
    AuthorizeConnect {
        peer: PeerId,
        reply: Sender<Result<(), ConnectionDisallowedError>>,
    },
    AuthorizeFetch {
        peer: PeerId,
        sedimentree_id: SedimentreeId,
        reply: Sender<Result<(), FetchDisallowedError>>,
    },
    AuthorizePut {
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
        reply: Sender<Result<(), PutDisallowedError>>,
    },
    FilterAuthorizedFetch {
        peer: PeerId,
        ids: Vec<SedimentreeId>,
        reply: Sender<Vec<SedimentreeId>>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CliKeyhivePolicyHandle {
    tx: Sender<PolicyCommand>,
}

impl CliKeyhivePolicyHandle {
    pub(crate) fn channel() -> (Self, Receiver<PolicyCommand>) {
        let (tx, rx) = async_channel::bounded(POLICY_COMMAND_CAPACITY);
        (Self { tx }, rx)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyConnectDisallowed {
    #[error(transparent)]
    Keyhive(#[from] ConnectionDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyFetchDisallowed {
    #[error(transparent)]
    Keyhive(#[from] FetchDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliPolicyPutDisallowed {
    #[error(transparent)]
    Keyhive(#[from] PutDisallowedError),
    #[error("keyhive policy actor has shut down")]
    ActorGone,
}

impl ConnectionPolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type ConnectionDisallowed = CliPolicyConnectDisallowed;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizeConnect {
                    peer,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyConnectDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyConnectDisallowed::ActorGone)?;
            result.map_err(CliPolicyConnectDisallowed::Keyhive)
        }
        .boxed()
    }
}

impl StoragePolicy<future_form::Sendable> for CliKeyhivePolicyHandle {
    type FetchDisallowed = CliPolicyFetchDisallowed;
    type PutDisallowed = CliPolicyPutDisallowed;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizeFetch {
                    peer,
                    sedimentree_id,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyFetchDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyFetchDisallowed::ActorGone)?;
            result.map_err(CliPolicyFetchDisallowed::Keyhive)
        }
        .boxed()
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            self.tx
                .send(PolicyCommand::AuthorizePut {
                    requestor,
                    author,
                    sedimentree_id,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| CliPolicyPutDisallowed::ActorGone)?;
            let result = reply_rx
                .recv()
                .await
                .map_err(|_| CliPolicyPutDisallowed::ActorGone)?;
            result.map_err(CliPolicyPutDisallowed::Keyhive)
        }
        .boxed()
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            if self
                .tx
                .send(PolicyCommand::FilterAuthorizedFetch {
                    peer,
                    ids,
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                return Vec::new();
            }
            reply_rx.recv().await.unwrap_or_default()
        }
        .boxed()
    }
}

pub(crate) async fn run_policy_actor(
    rx: Receiver<PolicyCommand>,
    keyhive: Arc<AsyncMutex<CliKeyhive>>,
) {
    while let Ok(cmd) = rx.recv().await {
        match cmd {
            PolicyCommand::AuthorizeConnect { peer, reply } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_connect_with(&*locked_keyhive, peer).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::AuthorizeFetch {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_fetch_with(&*locked_keyhive, peer, sedimentree_id).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply,
            } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    authorize_put_with(&*locked_keyhive, requestor, author, sedimentree_id).await
                };
                reply.send(result).await.ok();
            }
            PolicyCommand::FilterAuthorizedFetch { peer, ids, reply } => {
                let result = {
                    let locked_keyhive = keyhive.lock().await;
                    filter_authorized_fetch_with(&*locked_keyhive, peer, ids).await
                };
                reply.send(result).await.ok();
            }
        }
    }

    tracing::debug!("keyhive policy actor shutting down (channel closed)");
}
