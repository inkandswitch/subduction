//! Fixtures for heads-observer and heads-watch tests: a recording observer,
//! a policy whose answers a test can flip at runtime, and a node builder
//! that takes both.

use alloc::{sync::Arc, vec::Vec};
use core::{
    convert::Infallible,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use std::sync::{Mutex, PoisonError};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_crypto::{signer::memory::MemorySigner, verified_author::VerifiedAuthor};

use crate::{
    connection::{
        Connection,
        message::SyncMessage,
        test_utils::{InstantTimeout, TokioSpawn},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::{RemoteHeads, RemoteHeadsObserver},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};

use super::make_signer;

/// Settling time for negative assertions on in-process channels.
pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Remembers every delivery.
#[derive(Clone, Debug, Default)]
pub struct RecordingObserver(Arc<Mutex<Vec<(SedimentreeId, PeerId, RemoteHeads)>>>);

impl RecordingObserver {
    /// Every delivery so far, in order.
    #[must_use]
    pub fn deliveries(&self) -> Vec<(SedimentreeId, PeerId, RemoteHeads)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Number of deliveries so far.
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// The heads of every delivery, in order.
    #[must_use]
    pub fn heads(&self) -> Vec<Vec<CommitId>> {
        self.deliveries()
            .into_iter()
            .map(|(_, _, h)| h.heads)
            .collect()
    }

    /// The heads of every delivery about `id`, in order.
    #[must_use]
    pub fn heads_for(&self, id: SedimentreeId) -> Vec<Vec<CommitId>> {
        self.deliveries()
            .into_iter()
            .filter(|(tree, _, _)| *tree == id)
            .map(|(_, _, h)| h.heads)
            .collect()
    }
}

impl RemoteHeadsObserver for RecordingObserver {
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, peer, heads));
    }
}

/// Allows every connection; fetches and puts are each governed by a flag the
/// test may flip while the node runs.
#[derive(Clone, Debug)]
pub struct FlagPolicy {
    fetch: Arc<AtomicBool>,
    put: Arc<AtomicBool>,
}

/// The answer [`FlagPolicy`] gives when its flag is off.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("refused by policy")]
pub struct Refused;

impl FlagPolicy {
    /// Fetches and puts allowed.
    #[must_use]
    pub fn allow_all() -> Self {
        Self::new(true, true)
    }

    /// Fetches refused, puts allowed.
    #[must_use]
    pub fn deny_fetch() -> Self {
        Self::new(false, true)
    }

    /// Puts refused, fetches allowed.
    #[must_use]
    pub fn deny_put() -> Self {
        Self::new(true, false)
    }

    /// A policy with the given initial answers.
    #[must_use]
    pub fn new(fetch: bool, put: bool) -> Self {
        Self {
            fetch: Arc::new(AtomicBool::new(fetch)),
            put: Arc::new(AtomicBool::new(put)),
        }
    }

    /// Allow or refuse fetches from now on.
    pub fn set_fetch(&self, allowed: bool) {
        self.fetch.store(allowed, Ordering::SeqCst);
    }
}

impl ConnectionPolicy<Sendable> for FlagPolicy {
    type ConnectionDisallowed = Infallible;

    fn authorize_connect(&self, _peer: PeerId) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
}

impl StoragePolicy<Sendable> for FlagPolicy {
    type FetchDisallowed = Refused;
    type PutDisallowed = Refused;

    fn authorize_fetch(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Refused>> {
        let ok = self.fetch.load(Ordering::SeqCst);
        Box::pin(async move { ok.then_some(()).ok_or(Refused) })
    }

    fn authorize_put(
        &self,
        _requestor: PeerId,
        _author: VerifiedAuthor,
        _id: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Refused>> {
        let ok = self.put.load(Ordering::SeqCst);
        Box::pin(async move { ok.then_some(()).ok_or(Refused) })
    }

    fn filter_authorized_fetch(
        &self,
        _peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        let ok = self.fetch.load(Ordering::SeqCst);
        Box::pin(async move { if ok { ids } else { Vec::new() } })
    }
}

/// A running node over connection type `Conn` with a [`FlagPolicy`] and a
/// [`RecordingObserver`].
pub type WatchedNode<Conn> = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        SyncHandler<
            Sendable,
            MemoryStorage,
            Conn,
            FlagPolicy,
            CountLeadingZeroBytes,
            TokioSpawn,
            256,
            RecordingObserver,
        >,
        FlagPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// A node whose heads observer records every delivery.
///
/// Returns the node, its observer, and its [`PeerId`].
#[must_use]
pub fn spawn_watched_node<Conn>(
    seed: u8,
    policy: FlagPolicy,
) -> (WatchedNode<Conn>, RecordingObserver, PeerId)
where
    Conn: Connection<Sendable, SyncMessage>
        + PartialEq
        + Clone
        + core::fmt::Debug
        + Send
        + Sync
        + 'static,
    Conn::SendError: Send + Sync + 'static,
    Conn::RecvError: Send + Sync + 'static,
    Conn::DisconnectionError: Send + Sync + 'static,
{
    let (node, _handler, observer, peer) = spawn_watched_node_with_handler(seed, policy);
    (node, observer, peer)
}

/// Connect two watched nodes over a channel pair; `dialer` dials `target`.
///
/// # Errors
///
/// Propagates either node's `add_connection` error.
pub async fn dial_watched(
    dialer: &WatchedNode<super::ChannelConn>,
    dialer_peer: PeerId,
    target: &WatchedNode<super::ChannelConn>,
    target_peer: PeerId,
) -> Result<(), Box<dyn core::error::Error + Send + Sync>> {
    use crate::{
        authenticated::{Authenticated, Direction},
        connection::test_utils::ChannelTransport,
        transport::message::MessageTransport,
    };

    let (out, inbound) = ChannelTransport::pair();
    dialer
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(out),
            target_peer,
            Direction::Dialed,
        ))
        .await?;
    target
        .add_connection(Authenticated::new_for_test(
            MessageTransport::new(inbound),
            dialer_peer,
            Direction::Accepted,
        ))
        .await?;
    Ok(())
}

/// The [`SyncHandler`] behind a [`WatchedNode`], for tests that drive
/// dispatch directly.
pub type WatchedHandler<Conn> = Arc<
    SyncHandler<
        Sendable,
        MemoryStorage,
        Conn,
        FlagPolicy,
        CountLeadingZeroBytes,
        TokioSpawn,
        256,
        RecordingObserver,
    >,
>;

/// [`spawn_watched_node`], also returning the handler.
#[must_use]
pub fn spawn_watched_node_with_handler<Conn>(
    seed: u8,
    policy: FlagPolicy,
) -> (
    WatchedNode<Conn>,
    WatchedHandler<Conn>,
    RecordingObserver,
    PeerId,
)
where
    Conn: Connection<Sendable, SyncMessage>
        + PartialEq
        + Clone
        + core::fmt::Debug
        + Send
        + Sync
        + 'static,
    Conn::SendError: Send + Sync + 'static,
    Conn::RecvError: Send + Sync + 'static,
    Conn::DisconnectionError: Send + Sync + 'static,
{
    let signer = make_signer(seed);
    let peer = PeerId::from(signer.verifying_key());
    let observer = RecordingObserver::default();

    let (node, handler, listener, manager) = SubductionBuilder::<_, _, _, _, _, _, 256>::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(policy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .heads_observer(observer.clone())
        .build::<Sendable, Conn>();

    tokio::spawn(listener);
    tokio::spawn(manager);

    (node, handler, observer, peer)
}
