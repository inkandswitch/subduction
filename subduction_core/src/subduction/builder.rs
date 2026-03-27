//! Type-safe builder for [`Subduction`] instances.
//!
//! Eliminates the boilerplate of constructing shared state, wiring up
//! a [`SyncHandler`], and passing 11 arguments to [`Subduction::new`].
//!
//! # Required Fields
//!
//! Three fields must be set before calling [`build`] or
//! [`build_with_handler`]. These are tracked at the type level —
//! calling `build` on a builder with missing fields is a compile error.
//!
//! | Field | Setter | Purpose |
//! |-------|--------|---------|
//! | `signer` | [`.signer()`] | Peer identity and handshake signing |
//! | `spawner` | [`.spawner()`] | Platform-specific task spawning |
//! | `storage` | [`.storage()`] | Storage backend + authorization policy |
//!
//! # Optional Fields
//!
//! | Field | Setter | Default |
//! |-------|--------|---------|
//! | `discovery_id` | [`.discovery_id()`] | `None` |
//! | `depth_metric` | [`.depth_metric()`] | [`CountLeadingZeroBytes`] |
//! | `nonce_cache` | [`.nonce_cache()`] | [`NonceCache::default()`] |
//! | `max_pending_blob_requests` | [`.max_pending_blob_requests()`] | `10_000` |
//! | `sedimentrees` | [`.sedimentrees()`] | Empty [`ShardedMap::new()`] |
//!
//! # Example
//!
//! ```ignore
//! use subduction_core::subduction::builder::SubductionBuilder;
//!
//! let (subduction, handler, listener, manager) = SubductionBuilder::new()
//!     .signer(signer)
//!     .storage(my_storage, Arc::new(policy))
//!     .spawner(TokioSpawn)
//!     .discovery_id(discovery_id)
//!     .build::<Sendable, MyConnection>();
//! ```
//!
//! [`Subduction`]: super::Subduction
//! [`Subduction::new`]: super::Subduction::new
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//! [`build`]: SubductionBuilder::build
//! [`build_with_handler`]: SubductionBuilder::build_with_handler
//! [`.signer()`]: SubductionBuilder::signer
//! [`.spawner()`]: SubductionBuilder::spawner
//! [`.storage()`]: SubductionBuilder::storage
//! [`.discovery_id()`]: SubductionBuilder::discovery_id
//! [`.depth_metric()`]: SubductionBuilder::depth_metric
//! [`.nonce_cache()`]: SubductionBuilder::nonce_cache
//! [`.max_pending_blob_requests()`]: SubductionBuilder::max_pending_blob_requests
//! [`.sedimentrees()`]: SubductionBuilder::sedimentrees
//! [`CountLeadingZeroBytes`]: sedimentree_core::commit::CountLeadingZeroBytes
//! [`NonceCache::default()`]: crate::nonce_cache::NonceCache
//! [`ShardedMap::new()`]: crate::sharded_map::ShardedMap::new

use alloc::sync::Arc;
use async_lock::Mutex;
use core::time::Duration;
use sedimentree_core::{
    collections::{Map, Set},
    commit::CountLeadingZeroBytes,
    depth::DepthMetric,
    id::SedimentreeId,
    sedimentree::Sedimentree,
};

use crate::{
    authenticated::Authenticated,
    connection::{Connection, manager::Spawn, message::SyncMessage},
    handler::{Handler, sync::SyncHandler},
    handshake::audience::DiscoveryId,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, traits::Storage},
    timeout::Timeout,
};
use nonempty::NonEmpty;
use subduction_crypto::signer::Signer;

use super::{
    ListenerFuture, StartListener, Subduction, SubductionFutureForm,
    error::ListenError,
    pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
};

/// Marker for a required builder field that hasn't been set yet.
///
/// Used as a type-level sentinel: [`SubductionBuilder`] methods that
/// require a field to be present are only available when the
/// corresponding generic parameter is _not_ `Unset`.
#[derive(Debug, Clone, Copy)]
pub struct Unset;

/// Type-safe builder for [`Subduction`] instances.
///
/// Required fields are tracked at the type level via [`Unset`]: the
/// [`build`](Self::build) method is only available once `Sig`, `Sp`,
/// and `Sto` have all been replaced with concrete types.
///
/// Optional fields can be set in any order and have sensible defaults.
///
/// See the [module documentation](self) for a full example.
#[derive(Debug)]
pub struct SubductionBuilder<
    Sig = Unset,
    Sp = Unset,
    Sto = Unset,
    Tmr = Unset,
    M = CountLeadingZeroBytes,
    const N: usize = 256,
> {
    signer: Sig,
    spawner: Sp,
    storage: Sto,
    timer: Tmr,

    discovery_id: Option<DiscoveryId>,
    default_call_timeout: Option<Duration>,
    depth_metric: M,
    nonce_cache: Option<NonceCache>,
    max_pending_blob_requests: usize,
    sedimentrees: SedimentreesOption<N>,
}

/// Internal helper: stores an optional pre-populated `ShardedMap`.
///
/// Using a wrapper struct avoids placing the const generic `N` on
/// fields that don't otherwise need it.
#[derive(Debug)]
struct SedimentreesOption<const N: usize>(Option<Arc<ShardedMap<SedimentreeId, Sedimentree, N>>>);

impl<const N: usize> Default for SedimentreesOption<N> {
    fn default() -> Self {
        Self(None)
    }
}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------

impl<const N: usize> SubductionBuilder<Unset, Unset, Unset, Unset, CountLeadingZeroBytes, N> {
    /// Create a new builder with all defaults.
    ///
    /// The four required fields — `signer`, `spawner`, `storage`, and
    /// `timer` — must be set via their respective methods before
    /// calling [`build`](SubductionBuilder::build).
    ///
    /// The const generic `N` controls the number of shards in the
    /// internal [`ShardedMap`]. Defaults to 256 if not specified.
    #[must_use]
    pub fn new() -> Self {
        SubductionBuilder {
            signer: Unset,
            spawner: Unset,
            storage: Unset,
            timer: Unset,
            discovery_id: None,
            default_call_timeout: None,
            depth_metric: CountLeadingZeroBytes,
            nonce_cache: None,
            max_pending_blob_requests: DEFAULT_MAX_PENDING_BLOB_REQUESTS,
            sedimentrees: SedimentreesOption::default(),
        }
    }
}

impl<const N: usize> Default
    for SubductionBuilder<Unset, Unset, Unset, Unset, CountLeadingZeroBytes, N>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Sp, Sto, Tmr, M, const N: usize> SubductionBuilder<Unset, Sp, Sto, Tmr, M, N> {
    /// Set the signer for peer identity and handshake authentication.
    ///
    /// This is a required field.
    pub fn signer<Sig>(self, signer: Sig) -> SubductionBuilder<Sig, Sp, Sto, Tmr, M, N> {
        SubductionBuilder {
            signer,
            spawner: self.spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_call_timeout: self.default_call_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_pending_blob_requests: self.max_pending_blob_requests,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sig, Sto, Tmr, M, const N: usize> SubductionBuilder<Sig, Unset, Sto, Tmr, M, N> {
    /// Set the task spawner for background work.
    ///
    /// This is a required field. Common implementations:
    /// - `TokioSpawn` for native async (requires `Sendable`)
    /// - `WasmSpawn` for browser environments (requires `Local`)
    pub fn spawner<Sp>(self, spawner: Sp) -> SubductionBuilder<Sig, Sp, Sto, Tmr, M, N> {
        SubductionBuilder {
            signer: self.signer,
            spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_call_timeout: self.default_call_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_pending_blob_requests: self.max_pending_blob_requests,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sig, Sp, Tmr, M, const N: usize> SubductionBuilder<Sig, Sp, Unset, Tmr, M, N> {
    /// Set the storage backend and authorization policy.
    ///
    /// This is a required field. The `storage` backend provides
    /// persistence, and the `policy` controls per-peer access.
    pub fn storage<S, P>(
        self,
        storage: S,
        policy: Arc<P>,
    ) -> SubductionBuilder<Sig, Sp, StoragePowerbox<S, P>, Tmr, M, N> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: StoragePowerbox::new(storage, policy),
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_call_timeout: self.default_call_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_pending_blob_requests: self.max_pending_blob_requests,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sig, Sp, Sto, M, const N: usize> SubductionBuilder<Sig, Sp, Sto, Unset, M, N> {
    /// Set the timeout strategy for roundtrip calls.
    ///
    /// This is a required field. Common implementations:
    /// - `TokioTimeout` for native async
    /// - `JsTimeout` for browser environments
    pub fn timer<O>(self, timer: O) -> SubductionBuilder<Sig, Sp, Sto, O, M, N> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: self.storage,
            timer,
            discovery_id: self.discovery_id,
            default_call_timeout: self.default_call_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_pending_blob_requests: self.max_pending_blob_requests,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sig, Sp, Sto, Tmr, M, const N: usize> SubductionBuilder<Sig, Sp, Sto, Tmr, M, N> {
    /// Set the discovery ID for discovery-mode connections.
    ///
    /// Defaults to `None` (peer-to-peer mode only).
    #[must_use]
    pub const fn discovery_id(mut self, id: DiscoveryId) -> Self {
        self.discovery_id = Some(id);
        self
    }

    /// Set the default timeout for sync roundtrips
    /// (`BatchSyncRequest` → `BatchSyncResponse`).
    ///
    /// Defaults to 30 seconds if not set. Individual calls to
    /// `sync_with_peer` can override this per-request.
    #[must_use]
    pub const fn roundtrip_timeout(mut self, timeout: Duration) -> Self {
        self.default_call_timeout = Some(timeout);
        self
    }

    /// Override the depth metric used to assign commit depths.
    ///
    /// Defaults to [`CountLeadingZeroBytes`].
    pub fn depth_metric<M2: DepthMetric>(
        self,
        metric: M2,
    ) -> SubductionBuilder<Sig, Sp, Sto, Tmr, M2, N> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_call_timeout: self.default_call_timeout,
            depth_metric: metric,
            nonce_cache: self.nonce_cache,
            max_pending_blob_requests: self.max_pending_blob_requests,
            sedimentrees: self.sedimentrees,
        }
    }

    /// Override the nonce cache for handshake replay protection.
    ///
    /// Defaults to [`NonceCache::default()`] (3-minute buckets).
    #[must_use]
    pub fn nonce_cache(mut self, cache: NonceCache) -> Self {
        self.nonce_cache = Some(cache);
        self
    }

    /// Override the maximum number of pending blob requests.
    ///
    /// Defaults to [`DEFAULT_MAX_PENDING_BLOB_REQUESTS`] (10,000).
    #[must_use]
    pub const fn max_pending_blob_requests(mut self, max: usize) -> Self {
        self.max_pending_blob_requests = max;
        self
    }

    /// Provide a pre-populated sedimentree map.
    ///
    /// Use this for hydration: load sedimentree state from storage,
    /// then pass the populated map here. Defaults to an empty
    /// [`ShardedMap::new()`](ShardedMap::new).
    #[must_use]
    pub fn sedimentrees(
        mut self,
        sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    ) -> Self {
        self.sedimentrees = SedimentreesOption(Some(sedimentrees));
        self
    }
}

// -----------------------------------------------------------------------
// Build methods (only available when all required fields are set)
// -----------------------------------------------------------------------

impl<Sig, Sp, S, P, Tmr, M: DepthMetric, const N: usize>
    SubductionBuilder<Sig, Sp, StoragePowerbox<S, P>, Tmr, M, N>
{
    /// Build a [`Subduction`] instance with the default [`SyncHandler`].
    ///
    /// The handler is auto-constructed from the shared state and
    /// returned alongside the `Subduction` instance for use with
    /// [`Subduction::listen`].
    ///
    /// Returns `(subduction, handler, listener_future, manager_future)`.
    ///
    /// # Type Parameters
    ///
    /// - `F` — Future form (`Sendable` or `Local`). Usually inferred
    ///   from the spawner and context.
    /// - `C` — Connection type. Inferred from usage or specified via
    ///   turbofish.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (sd, handler, listener, manager) = SubductionBuilder::new()
    ///     .signer(signer)
    ///     .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
    ///     .spawner(TokioSpawn)
    ///     .timer(TokioTimeout)
    ///     .build::<Sendable, MyConnection>();
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn build<'a, F, C>(
        self,
    ) -> (
        Arc<Subduction<'a, F, S, C, SyncHandler<F, S, C, P, M, N>, P, Sig, Tmr, M, N>>,
        Arc<SyncHandler<F, S, C, P, M, N>>,
        ListenerFuture<'a, F, S, C, SyncHandler<F, S, C, P, M, N>, P, Sig, Tmr, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    )
    where
        F: SubductionFutureForm<'a, S, C, SyncMessage, P, Sig, M, N> + 'static,
        F: StartListener<'a, S, C, SyncMessage, SyncHandler<F, S, C, P, M, N>, P, Sig, M, N>,
        S: Storage<F>,
        C: Connection<F, SyncMessage> + PartialEq + Clone + 'a,
        P: ConnectionPolicy<F> + StoragePolicy<F>,
        Sig: Signer<F>,
        Tmr: Timeout<F> + Clone + Send + Sync + 'a,
        Sp: Spawn<F> + Send + Sync + 'static,
        M: Clone,
        SyncHandler<F, S, C, P, M, N>: Handler<F, C, Message = SyncMessage>,
        <SyncHandler<F, S, C, P, M, N> as Handler<F, C>>::HandlerError:
            Into<ListenError<F, S, C, SyncMessage>>,
        crate::connection::managed::ManagedConnection<C, F, Tmr>:
            crate::connection::managed::ManagedCall<
                    F,
                    SyncMessage,
                    SendError = <C as Connection<F, SyncMessage>>::SendError,
                >,
    {
        let sedimentrees = self
            .sedimentrees
            .0
            .unwrap_or_else(|| Arc::new(ShardedMap::new()));

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(
            self.max_pending_blob_requests,
        )));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            self.storage.clone(),
            pending_blob_requests.clone(),
            self.depth_metric.clone(),
        ));

        let send_counter = handler.send_counter().clone();

        let (sd, listener, manager) = Subduction::new(
            handler.clone(),
            self.discovery_id,
            self.signer,
            sedimentrees,
            connections,
            subscriptions,
            self.storage,
            pending_blob_requests,
            send_counter,
            nonce_cache,
            self.timer,
            self.default_call_timeout.unwrap_or(Duration::from_secs(30)),
            self.depth_metric,
            self.spawner,
        );

        (sd, handler, listener, manager)
    }

    /// Build a [`Subduction`] instance with a custom [`Handler`].
    ///
    /// Use this when replacing the default [`SyncHandler`] with a
    /// custom handler implementation.
    ///
    /// Returns `(subduction, listener_future, manager_future)`.
    ///
    /// A fresh [`PeerCounter`] is created for `Subduction`. If your custom
    /// handler also stamps outgoing messages with counters (e.g., wraps a
    /// [`SyncHandler`]), you must share the same `PeerCounter` between
    /// the handler and `Subduction` — use [`build`] or [`build_composed`]
    /// instead, which handle this automatically.
    ///
    /// [`PeerCounter`]: crate::peer::counter::PeerCounter
    /// [`SyncHandler`]: crate::handler::sync::SyncHandler
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handler = Arc::new(MyCustomHandler::new(/* ... */));
    ///
    /// let (sd, listener, manager) = SubductionBuilder::new()
    ///     .signer(signer)
    ///     .storage(my_storage, Arc::new(policy))
    ///     .spawner(TokioSpawn)
    ///     .timer(TokioTimeout)
    ///     .build_with_handler::<Sendable, MyConnection, _>(handler);
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn build_with_handler<'a, F, C, H>(
        self,
        handler: Arc<H>,
    ) -> (
        Arc<Subduction<'a, F, S, C, H, P, Sig, Tmr, M, N>>,
        ListenerFuture<'a, F, S, C, H, P, Sig, Tmr, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    )
    where
        F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N> + 'static,
        F: StartListener<'a, S, C, H::Message, H, P, Sig, M, N>,
        S: Storage<F>,
        C: Connection<F, H::Message> + PartialEq + Clone + 'a,
        P: ConnectionPolicy<F> + StoragePolicy<F>,
        Sig: Signer<F>,
        Tmr: Timeout<F> + Clone + Send + Sync + 'a,
        Sp: Spawn<F> + Send + Sync + 'static,
        H: Handler<F, C>,
        H::Message: From<SyncMessage>,
        H::HandlerError: Into<ListenError<F, S, C, H::Message>>,
        crate::connection::managed::ManagedConnection<C, F, Tmr>:
            crate::connection::managed::ManagedCall<
                    F,
                    H::Message,
                    SendError = <C as Connection<F, H::Message>>::SendError,
                >,
    {
        let sedimentrees = self
            .sedimentrees
            .0
            .unwrap_or_else(|| Arc::new(ShardedMap::new()));

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(
            self.max_pending_blob_requests,
        )));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            sedimentrees,
            connections,
            subscriptions,
            self.storage,
            pending_blob_requests,
            PeerCounter::default(),
            nonce_cache,
            self.timer,
            self.default_call_timeout.unwrap_or(Duration::from_secs(30)),
            self.depth_metric,
            self.spawner,
        )
    }

    /// Build a [`Subduction`] instance with a composed [`Handler`].
    ///
    /// Like [`build_with_handler`](Self::build_with_handler), but
    /// creates the default [`SyncHandler`] internally and passes it to
    /// the `compose` closure. This lets you wrap the `SyncHandler` in a
    /// composed handler that also handles other message types.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (sd, listener, manager) = SubductionBuilder::new()
    ///     .signer(signer)
    ///     .storage(my_storage, Arc::new(policy))
    ///     .spawner(TokioSpawn)
    ///     .build_composed::<Sendable, MyConn, _>(|sync_handler| {
    ///         Arc::new(MyComposedHandler { sync: sync_handler, /* ... */ })
    ///     });
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn build_composed<'a, F, C, H>(
        self,
        compose: impl FnOnce(Arc<SyncHandler<F, S, C, P, M, N>>) -> Arc<H>,
    ) -> (
        Arc<Subduction<'a, F, S, C, H, P, Sig, Tmr, M, N>>,
        ListenerFuture<'a, F, S, C, H, P, Sig, Tmr, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    )
    where
        F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N> + 'static,
        F: StartListener<'a, S, C, H::Message, H, P, Sig, M, N>,
        S: Storage<F>,
        C: Connection<F, H::Message> + Connection<F, SyncMessage> + PartialEq + Clone + 'a,
        P: ConnectionPolicy<F> + StoragePolicy<F>,
        Sig: Signer<F>,
        Tmr: Timeout<F> + Clone + Send + Sync + 'a,
        Sp: Spawn<F> + Send + Sync + 'static,
        H: Handler<F, C>,
        H::Message: From<SyncMessage>,
        H::HandlerError: Into<ListenError<F, S, C, H::Message>>,
        M: Clone,
        SyncHandler<F, S, C, P, M, N>: Handler<F, C, Message = SyncMessage>,
        <SyncHandler<F, S, C, P, M, N> as Handler<F, C>>::HandlerError:
            Into<ListenError<F, S, C, SyncMessage>>,
        crate::connection::managed::ManagedConnection<C, F, Tmr>:
            crate::connection::managed::ManagedCall<
                    F,
                    H::Message,
                    SendError = <C as Connection<F, H::Message>>::SendError,
                >,
    {
        let sedimentrees = self
            .sedimentrees
            .0
            .unwrap_or_else(|| Arc::new(ShardedMap::new()));

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(
            self.max_pending_blob_requests,
        )));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let sync_handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            self.storage.clone(),
            pending_blob_requests.clone(),
            self.depth_metric.clone(),
        ));

        let send_counter = sync_handler.send_counter().clone();
        let handler = compose(sync_handler);

        Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            sedimentrees,
            connections,
            subscriptions,
            self.storage,
            pending_blob_requests,
            send_counter,
            nonce_cache,
            self.timer,
            self.default_call_timeout.unwrap_or(Duration::from_secs(30)),
            self.depth_metric,
            self.spawner,
        )
    }
}
