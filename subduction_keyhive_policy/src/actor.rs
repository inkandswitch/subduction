//! Actor pattern for `Send`-safe keyhive policy access.
//!
//! `keyhive_core`'s async traits (`AsyncSigner`, `MembershipListener`,
//! `PrekeyListener`) use `async fn in trait` without `+ Send` bounds,
//! making all keyhive futures `!Send`. This prevents implementing policy
//! traits for [`Sendable`] directly on types that hold a `Keyhive`.
//!
//! The actor pattern solves this by isolating `!Send` keyhive work behind
//! a channel and exposing a [`Send`]-safe handle ([`KeyhiveActorHandle`]):
//!
//! ```text
//!                    Send boundary
//!                        │
//!   KeyhiveActorHandle   │   Actor future (!Send)
//!   ──────────────────   │   ─────────────────────
//!   Sender<Request>  ──► │ ──► Receiver<Request>
//!                        │        │
//!   bounded(1) reply  ◄──│────────┘  processes via
//!                        │           SubductionKeyhive
//! ```
//!
//! The future returned by each policy method on [`KeyhiveActorHandle`]
//! captures only `async_channel::Sender` + `async_channel::Receiver` —
//! both `Send`. The actual `!Send` keyhive work runs inside the actor
//! future, which the caller is responsible for spawning on an appropriate
//! executor (e.g., a `LocalSet` or `wasm_bindgen_futures::spawn_local`).
//!
//! [`Sendable`]: future_form::Sendable

extern crate alloc;

use alloc::vec::Vec;

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
};
use subduction_ephemeral::policy::EphemeralPolicy;

use crate::{
    ConnectionDisallowedError, FetchDisallowedError, PublishDisallowedError, PutDisallowedError,
    SubscribeDisallowedError,
};

/// A policy request sent from the handle to the actor loop.
///
/// Each variant bundles the request parameters with a reply channel
/// (a `bounded(1)` sender). The actor processes the request using the
/// `!Send` keyhive and sends the result back through the reply channel.
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

/// `Send`-safe handle to a keyhive policy actor.
///
/// Implements [`ConnectionPolicy<Sendable>`], [`StoragePolicy<Sendable>`],
/// and [`EphemeralPolicy<Sendable>`] by sending requests through a channel
/// to an actor future that runs `!Send` keyhive operations.
///
/// This type is [`Send`], [`Sync`], and [`Clone`]. The futures it returns
/// from policy methods are `Send` because they only capture channel types.
///
/// # Construction
///
/// Use [`KeyhiveActorHandle::new`] to create a handle and actor future.
/// The caller is responsible for spawning the actor future on an
/// appropriate executor:
///
/// ```ignore
/// let (handle, actor_fut) = KeyhiveActorHandle::new(policy, 64);
///
/// // On a tokio LocalSet:
/// tokio::task::spawn_local(actor_fut);
///
/// // Or on Wasm:
/// wasm_bindgen_futures::spawn_local(actor_fut);
/// ```
#[derive(Debug, Clone)]
pub struct KeyhiveActorHandle {
    tx: async_channel::Sender<PolicyRequest>,
}

impl KeyhiveActorHandle {
    /// Create a new actor handle and its corresponding actor future.
    ///
    /// The `policy` is moved into the actor future. All policy queries go
    /// through the returned handle via channels.
    ///
    /// `buffer` controls the request channel capacity.
    ///
    /// The returned future is `!Send` (it captures keyhive types). The
    /// caller must spawn it on an executor that supports `!Send` futures
    /// (e.g., a `LocalSet` or `wasm_bindgen_futures::spawn_local`).
    #[allow(clippy::type_complexity)]
    pub fn new<S, T, P, C, L, R>(
        policy: crate::SubductionKeyhive<S, T, P, C, L, R>,
        buffer: usize,
    ) -> (Self, impl core::future::Future<Output = ()>)
    where
        S: keyhive_core::crypto::signer::async_signer::AsyncSigner + Clone + 'static,
        T: keyhive_core::content::reference::ContentRef + 'static,
        P: for<'de> serde::Deserialize<'de> + 'static,
        C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone + 'static,
        L: keyhive_core::listener::membership::MembershipListener<S, T> + 'static,
        R: rand::CryptoRng + rand::RngCore + 'static,
    {
        let (tx, rx) = async_channel::bounded(buffer);
        let handle = Self { tx };
        let actor_fut = run_actor(policy, rx);
        (handle, actor_fut)
    }
}

/// The actor loop. Processes policy requests sequentially using the
/// `!Send` `SubductionKeyhive`.
///
/// This future is `!Send` — it must be spawned on an executor that
/// supports non-`Send` futures. It runs until all handles are dropped
/// (the request channel closes).
async fn run_actor<S, T, P, C, L, R>(
    policy: crate::SubductionKeyhive<S, T, P, C, L, R>,
    rx: async_channel::Receiver<PolicyRequest>,
) where
    S: keyhive_core::crypto::signer::async_signer::AsyncSigner + Clone + 'static,
    T: keyhive_core::content::reference::ContentRef + 'static,
    P: for<'de> serde::Deserialize<'de> + 'static,
    C: keyhive_core::store::ciphertext::CiphertextStore<T, P> + Clone + 'static,
    L: keyhive_core::listener::membership::MembershipListener<S, T> + 'static,
    R: rand::CryptoRng + rand::RngCore + 'static,
{
    use future_form::Local;

    while let Ok(req) = rx.recv().await {
        match req {
            PolicyRequest::AuthorizeConnect { peer, reply } => {
                let result = ConnectionPolicy::<Local>::authorize_connect(&policy, peer).await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizeFetch {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result =
                    StoragePolicy::<Local>::authorize_fetch(&policy, peer, sedimentree_id).await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizePut {
                requestor,
                author,
                sedimentree_id,
                reply,
            } => {
                let result = StoragePolicy::<Local>::authorize_put(
                    &policy,
                    requestor,
                    author,
                    sedimentree_id,
                )
                .await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::FilterAuthorizedFetch { peer, ids, reply } => {
                let result =
                    StoragePolicy::<Local>::filter_authorized_fetch(&policy, peer, ids).await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizeSubscribe {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result =
                    EphemeralPolicy::<Local>::authorize_subscribe(&policy, peer, sedimentree_id)
                        .await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::AuthorizePublish {
                peer,
                sedimentree_id,
                reply,
            } => {
                let result =
                    EphemeralPolicy::<Local>::authorize_publish(&policy, peer, sedimentree_id)
                        .await;
                let _sent = reply.send(result).await;
            }

            PolicyRequest::FilterAuthorizedSubscribers {
                sedimentree_id,
                peers,
                reply,
            } => {
                let result = EphemeralPolicy::<Local>::filter_authorized_subscribers(
                    &policy,
                    sedimentree_id,
                    peers,
                )
                .await;
                let _sent = reply.send(result).await;
            }
        }
    }

    tracing::debug!("keyhive policy actor shutting down (all handles dropped)");
}

// ── Sendable policy impls ───────────────────────────────────────────────
//
// Each method sends a request through the channel and awaits the reply.
// The future captures only `async_channel::Sender` + `async_channel::Receiver`,
// both `Send`, so the returned `BoxFuture` satisfies `Send`.

impl ConnectionPolicy<Sendable> for KeyhiveActorHandle {
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

impl StoragePolicy<Sendable> for KeyhiveActorHandle {
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
                tracing::warn!("keyhive actor gone during filter_authorized_fetch");
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        })
    }
}

impl EphemeralPolicy<Sendable> for KeyhiveActorHandle {
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
                tracing::warn!("keyhive actor gone during filter_authorized_subscribers");
                return Vec::new();
            }

            reply_rx.recv().await.unwrap_or_default()
        })
    }
}
