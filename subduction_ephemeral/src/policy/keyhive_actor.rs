//! Actor pattern for `Send`-safe keyhive policy on [`KeyhiveSyncManager`].
//!
//! Same approach as `subduction_keyhive_policy`'s [`KeyhiveActorHandle`],
//! but wrapping a [`KeyhiveSyncManager`] instead of `SubductionKeyhive`.
//! The actor runs the `Local` policy impls on the sync manager inside a
//! `!Send` future, while the handle exposes `Send`-safe `Sendable` impls
//! via channels.
//!
//! This module implements all three policy traits —
//! [`ConnectionPolicy<Sendable>`], [`StoragePolicy<Sendable>`], and
//! [`EphemeralPolicy<Sendable>`] — on a single [`SyncManagerActorHandle`],
//! so the application only needs to spawn one actor.
//!
//! [`KeyhiveActorHandle`]: subduction_keyhive_policy::actor::KeyhiveActorHandle
//! [`KeyhiveSyncManager`]: subduction_keyhive::KeyhiveSyncManager

extern crate alloc;

use alloc::{boxed::Box, vec::Vec};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};

use super::EphemeralPolicy;

// ── Error types ─────────────────────────────────────────────────────────

/// Error returned when a connection is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionDisallowedError {
    /// The keyhive sync manager actor has shut down.
    #[error("sync manager actor has shut down")]
    ActorGone,

    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The peer is not a known agent in the keyhive.
    #[error("peer is not a known agent")]
    UnknownAgent,
}

/// Error returned when a fetch operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FetchDisallowedError {
    /// The keyhive sync manager actor has shut down.
    #[error("sync manager actor has shut down")]
    ActorGone,

    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The peer does not have sufficient access.
    #[error("peer does not have Pull access")]
    InsufficientAccess,
}

/// Error returned when a put operation is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PutDisallowedError {
    /// The keyhive sync manager actor has shut down.
    #[error("sync manager actor has shut down")]
    ActorGone,

    /// The author peer ID is not a valid Ed25519 public key.
    #[error("author ID is not a valid Ed25519 public key")]
    InvalidAuthorId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The author does not have sufficient access.
    #[error("author does not have Write access")]
    InsufficientAccess,
}

/// Error returned when an ephemeral subscribe is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubscribeDisallowedError {
    /// The keyhive sync manager actor has shut down.
    #[error("sync manager actor has shut down")]
    ActorGone,

    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The peer does not have sufficient access.
    #[error("peer does not have Pull access")]
    InsufficientAccess,
}

/// Error returned when an ephemeral publish is not allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublishDisallowedError {
    /// The keyhive sync manager actor has shut down.
    #[error("sync manager actor has shut down")]
    ActorGone,

    /// The peer ID is not a valid Ed25519 public key.
    #[error("peer ID is not a valid Ed25519 public key")]
    InvalidPeerId,

    /// The sedimentree ID is not a valid document ID.
    #[error("sedimentree ID is not a valid document ID")]
    InvalidSedimentreeId,

    /// The document does not exist in keyhive.
    #[error("document not found")]
    DocumentNotFound,

    /// The peer does not have sufficient access.
    #[error("peer does not have Write access")]
    InsufficientAccess,
}

// ── Request type ────────────────────────────────────────────────────────

/// A policy request sent from the handle to the actor loop.
enum PolicyRequest {
    AuthorizeConnect {
        peer: PeerId,
        reply: async_channel::Sender<Result<(), ConnectionDisallowedError>>,
    },

    AuthorizeFetch {
        peer: PeerId,
        sedimentree_id: SedimentreeId,
        reply: async_channel::Sender<Result<(), FetchDisallowedError>>,
    },

    AuthorizePut {
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
        reply: async_channel::Sender<Result<(), PutDisallowedError>>,
    },

    FilterAuthorizedFetch {
        peer: PeerId,
        ids: Vec<SedimentreeId>,
        reply: async_channel::Sender<Vec<SedimentreeId>>,
    },

    AuthorizeSubscribe {
        peer: PeerId,
        sedimentree_id: SedimentreeId,
        reply: async_channel::Sender<Result<(), SubscribeDisallowedError>>,
    },

    AuthorizePublish {
        peer: PeerId,
        sedimentree_id: SedimentreeId,
        reply: async_channel::Sender<Result<(), PublishDisallowedError>>,
    },

    FilterAuthorizedSubscribers {
        sedimentree_id: SedimentreeId,
        peers: Vec<PeerId>,
        reply: async_channel::Sender<Vec<PeerId>>,
    },
}

// ── Handle ──────────────────────────────────────────────────────────────

/// `Send`-safe handle to a [`KeyhiveSyncManager`] policy actor.
///
/// Implements [`ConnectionPolicy<Sendable>`], [`StoragePolicy<Sendable>`],
/// and [`EphemeralPolicy<Sendable>`] by sending requests through a channel
/// to an actor future that runs `!Send` keyhive operations on the sync
/// manager.
///
/// This type is [`Send`], [`Sync`], and [`Clone`]. The futures it returns
/// from policy methods are `Send` because they only capture channel types.
///
/// # Construction
///
/// Use [`SyncManagerActorHandle::new`] to create a handle and actor future.
/// The caller is responsible for spawning the actor future on an
/// appropriate executor.
///
/// [`KeyhiveSyncManager`]: subduction_keyhive::KeyhiveSyncManager
#[derive(Debug, Clone)]
pub struct SyncManagerActorHandle {
    tx: async_channel::Sender<PolicyRequest>,
}

impl SyncManagerActorHandle {
    /// Create a new actor handle and its corresponding actor future.
    ///
    /// The `sync_manager` is moved into the actor future. All policy
    /// queries go through the returned handle via channels.
    ///
    /// `buffer` controls the request channel capacity.
    ///
    /// The returned future is `!Send` (it captures keyhive types). The
    /// caller must spawn it on an executor that supports `!Send` futures.
    #[allow(clippy::type_complexity)]
    pub fn new<S, T, P, C, L, R, Store>(
        sync_manager: subduction_keyhive::KeyhiveSyncManager<S, T, P, C, L, R, Store>,
        buffer: usize,
    ) -> (Self, impl core::future::Future<Output = ()>)
    where
        S: keyhive_core::crypto::signer::async_signer::AsyncSigner + Clone + 'static,
        T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone + 'static,
        L: keyhive_core::listener::membership::MembershipListener<S, T> + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
        Store: subduction_keyhive::KeyhiveStorage<future_form::Local> + 'static,
    {
        let (tx, rx) = async_channel::bounded(buffer);
        let handle = Self { tx };
        let actor_fut = run_actor(sync_manager, rx);
        (handle, actor_fut)
    }
}

/// The actor loop. Processes policy requests sequentially using the
/// `!Send` [`KeyhiveSyncManager`].
///
/// [`KeyhiveSyncManager`]: subduction_keyhive::KeyhiveSyncManager
#[allow(clippy::too_many_lines)]
async fn run_actor<S, T, P, C, L, R, Store>(
    sync_manager: subduction_keyhive::KeyhiveSyncManager<S, T, P, C, L, R, Store>,
    rx: async_channel::Receiver<PolicyRequest>,
) where
    S: keyhive_core::crypto::signer::async_signer::AsyncSigner + Clone + 'static,
    T: keyhive_core::content::reference::ContentRef + serde::de::DeserializeOwned + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone + 'static,
    L: keyhive_core::listener::membership::MembershipListener<S, T> + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
    Store: subduction_keyhive::KeyhiveStorage<future_form::Local> + 'static,
{
    use future_form::Local;

    while let Ok(req) = rx.recv().await {
        match req {
            PolicyRequest::AuthorizeConnect { peer, reply } => {
                let result =
                    ConnectionPolicy::<Local>::authorize_connect(&sync_manager, peer).await;
                let result = result.map_err(|e| match e {
                    subduction_core::policy::keyhive::ConnectionDisallowedError::InvalidPeerId => {
                        ConnectionDisallowedError::InvalidPeerId
                    }
                    subduction_core::policy::keyhive::ConnectionDisallowedError::UnknownAgent => {
                        ConnectionDisallowedError::UnknownAgent
                    }
                });
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizeFetch {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result =
                    StoragePolicy::<Local>::authorize_fetch(&sync_manager, peer, sedimentree_id)
                        .await;
                let result = result.map_err(|e| match e {
                    subduction_core::policy::keyhive::FetchDisallowedError::InvalidPeerId => {
                        FetchDisallowedError::InvalidPeerId
                    }
                    subduction_core::policy::keyhive::FetchDisallowedError::InvalidSedimentreeId => {
                        FetchDisallowedError::InvalidSedimentreeId
                    }
                    subduction_core::policy::keyhive::FetchDisallowedError::DocumentNotFound => {
                        FetchDisallowedError::DocumentNotFound
                    }
                    subduction_core::policy::keyhive::FetchDisallowedError::InsufficientAccess => {
                        FetchDisallowedError::InsufficientAccess
                    }
                });
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply,
            } => {
                let result = StoragePolicy::<Local>::authorize_put(
                    &sync_manager,
                    requestor,
                    author,
                    sedimentree_id,
                )
                .await;
                let result = result.map_err(|e| match e {
                    subduction_core::policy::keyhive::PutDisallowedError::InvalidAuthorId => {
                        PutDisallowedError::InvalidAuthorId
                    }
                    subduction_core::policy::keyhive::PutDisallowedError::InvalidSedimentreeId => {
                        PutDisallowedError::InvalidSedimentreeId
                    }
                    subduction_core::policy::keyhive::PutDisallowedError::DocumentNotFound => {
                        PutDisallowedError::DocumentNotFound
                    }
                    subduction_core::policy::keyhive::PutDisallowedError::InsufficientAccess => {
                        PutDisallowedError::InsufficientAccess
                    }
                });
                let _sent = reply.send(result).await;
            }

            PolicyRequest::FilterAuthorizedFetch { peer, ids, reply } => {
                let result =
                    StoragePolicy::<Local>::filter_authorized_fetch(&sync_manager, peer, ids).await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizeSubscribe {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result = EphemeralPolicy::<Local>::authorize_subscribe(
                    &sync_manager,
                    peer,
                    sedimentree_id,
                )
                .await;
                let result = result.map_err(|e| match e {
                    super::keyhive::SubscribeDisallowedError::InvalidPeerId => {
                        SubscribeDisallowedError::InvalidPeerId
                    }
                    super::keyhive::SubscribeDisallowedError::InvalidSedimentreeId => {
                        SubscribeDisallowedError::InvalidSedimentreeId
                    }
                    super::keyhive::SubscribeDisallowedError::DocumentNotFound => {
                        SubscribeDisallowedError::DocumentNotFound
                    }
                    super::keyhive::SubscribeDisallowedError::InsufficientAccess => {
                        SubscribeDisallowedError::InsufficientAccess
                    }
                });
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizePublish {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result = EphemeralPolicy::<Local>::authorize_publish(
                    &sync_manager,
                    peer,
                    sedimentree_id,
                )
                .await;
                let result = result.map_err(|e| match e {
                    super::keyhive::PublishDisallowedError::InvalidPeerId => {
                        PublishDisallowedError::InvalidPeerId
                    }
                    super::keyhive::PublishDisallowedError::InvalidSedimentreeId => {
                        PublishDisallowedError::InvalidSedimentreeId
                    }
                    super::keyhive::PublishDisallowedError::DocumentNotFound => {
                        PublishDisallowedError::DocumentNotFound
                    }
                    super::keyhive::PublishDisallowedError::InsufficientAccess => {
                        PublishDisallowedError::InsufficientAccess
                    }
                });
                let _sent = reply.send(result).await;
            }

            PolicyRequest::FilterAuthorizedSubscribers {
                sedimentree_id,
                peers,
                reply,
            } => {
                let result = EphemeralPolicy::<Local>::filter_authorized_subscribers(
                    &sync_manager,
                    sedimentree_id,
                    peers,
                )
                .await;
                let _sent = reply.send(result).await;
            }
        }
    }

    tracing::debug!("sync manager policy actor shutting down (all handles dropped)");
}

// ── Sendable policy impls ───────────────────────────────────────────────

impl ConnectionPolicy<Sendable> for SyncManagerActorHandle {
    type ConnectionDisallowed = ConnectionDisallowedError;

    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> BoxFuture<'_, Result<(), Self::ConnectionDisallowed>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(PolicyRequest::AuthorizeConnect {
                peer,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ConnectionDisallowedError::ActorGone)?;

            reply_rx
                .recv()
                .await
                .map_err(|_| ConnectionDisallowedError::ActorGone)?
        })
    }
}

impl StoragePolicy<Sendable> for SyncManagerActorHandle {
    type FetchDisallowed = FetchDisallowedError;
    type PutDisallowed = PutDisallowedError;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::FetchDisallowed>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(PolicyRequest::AuthorizeFetch {
                peer,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| FetchDisallowedError::ActorGone)?;

            reply_rx
                .recv()
                .await
                .map_err(|_| FetchDisallowedError::ActorGone)?
        })
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PutDisallowed>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(PolicyRequest::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PutDisallowedError::ActorGone)?;

            reply_rx
                .recv()
                .await
                .map_err(|_| PutDisallowedError::ActorGone)?
        })
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            let ok = tx
                .send(PolicyRequest::FilterAuthorizedFetch {
                    peer,
                    ids,
                    reply: reply_tx,
                })
                .await;

            if ok.is_err() {
                tracing::warn!("sync manager actor gone during filter_authorized_fetch");
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        })
    }
}

impl EphemeralPolicy<Sendable> for SyncManagerActorHandle {
    type SubscribeDisallowed = SubscribeDisallowedError;
    type PublishDisallowed = PublishDisallowedError;

    fn authorize_subscribe(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::SubscribeDisallowed>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(PolicyRequest::AuthorizeSubscribe {
                peer,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| SubscribeDisallowedError::ActorGone)?;

            reply_rx
                .recv()
                .await
                .map_err(|_| SubscribeDisallowedError::ActorGone)?
        })
    }

    fn authorize_publish(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Self::PublishDisallowed>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            tx.send(PolicyRequest::AuthorizePublish {
                peer,
                sedimentree_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PublishDisallowedError::ActorGone)?;

            reply_rx
                .recv()
                .await
                .map_err(|_| PublishDisallowedError::ActorGone)?
        })
    }

    fn filter_authorized_subscribers(
        &self,
        sedimentree_id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> BoxFuture<'_, Vec<PeerId>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply_tx, reply_rx) = async_channel::bounded(1);
            let ok = tx
                .send(PolicyRequest::FilterAuthorizedSubscribers {
                    sedimentree_id,
                    peers,
                    reply: reply_tx,
                })
                .await;

            if ok.is_err() {
                tracing::warn!("sync manager actor gone during filter_authorized_subscribers");
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        })
    }
}
