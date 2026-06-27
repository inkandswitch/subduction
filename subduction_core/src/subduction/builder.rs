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
//! | `sedimentrees` | [`.sedimentrees()`] | Empty [`BoundedShardedMap::new()`] |
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
//! [`.sedimentrees()`]: SubductionBuilder::sedimentrees
//! [`CountLeadingZeroBytes`]: sedimentree_core::depth::CountLeadingZeroBytes
//! [`NonceCache::default()`]: crate::nonce_cache::NonceCache
//! [`BoundedShardedMap::new()`]: crate::collections::bounded_sharded_map::BoundedShardedMap::new

use alloc::sync::Arc;
use async_lock::Mutex;
use core::time::Duration;
use sedimentree_core::{
    collections::{Map, Set},
    depth::{CountLeadingZeroBytes, DepthMetric},
    id::SedimentreeId,
    sedimentree::minimized::MinimizedSedimentree,
};

use crate::{
    authenticated::Authenticated,
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::{
        Connection,
        managed::{ManagedCall, ManagedConnection},
        message::SyncMessage,
    },
    handler::{Handler, sync::SyncHandler},
    handshake::audience::DiscoveryId,
    multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::RemoteHeadsNotifier,
    spawn::Spawn,
    storage::{powerbox::StoragePowerbox, traits::Storage},
    timeout::Timeout,
};
use nonempty::NonEmpty;
use subduction_crypto::signer::Signer;

use super::{
    StartListener, Subduction, SubductionFutureForm, error::ListenError,
    listener_future::ListenerFuture,
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
    Sign = Unset,
    Sp = Unset,
    Store = Unset,
    Timer = Unset,
    Metric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> {
    signer: Sign,
    spawner: Sp,
    storage: Store,
    timer: Timer,

    discovery_id: Option<DiscoveryId>,
    default_roundtrip_timeout: Option<Duration>,
    depth_metric: Metric,
    nonce_cache: Option<NonceCache>,
    max_resident_trees: Option<usize>,
    sedimentrees: SedimentreesOption<SHARDS>,
}

/// Internal helper: stores an optional pre-populated `BoundedShardedMap`.
///
/// Using a wrapper struct avoids placing the const generic `SHARDS` on
/// fields that don't otherwise need it.
#[derive(Debug)]
struct SedimentreesOption<const SHARDS: usize>(
    Option<Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>>,
);

impl<const SHARDS: usize> Default for SedimentreesOption<SHARDS> {
    fn default() -> Self {
        Self(None)
    }
}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------

impl<const SHARDS: usize>
    SubductionBuilder<Unset, Unset, Unset, Unset, CountLeadingZeroBytes, SHARDS>
{
    /// Create a new builder with all defaults.
    ///
    /// The four required fields — `signer`, `spawner`, `storage`, and
    /// `timer` — must be set via their respective methods before
    /// calling [`build`](SubductionBuilder::build).
    ///
    /// The const generic `SHARDS` controls the number of shards in the
    /// internal [`BoundedShardedMap`]. Defaults to 256 if not specified.
    #[must_use]
    pub fn new() -> Self {
        SubductionBuilder {
            signer: Unset,
            spawner: Unset,
            storage: Unset,
            timer: Unset,
            discovery_id: None,
            default_roundtrip_timeout: None,
            depth_metric: CountLeadingZeroBytes,
            nonce_cache: None,
            max_resident_trees: None,
            sedimentrees: SedimentreesOption::default(),
        }
    }
}

impl<const SHARDS: usize> Default
    for SubductionBuilder<Unset, Unset, Unset, Unset, CountLeadingZeroBytes, SHARDS>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Sp, Store, Timer, Metric, const SHARDS: usize>
    SubductionBuilder<Unset, Sp, Store, Timer, Metric, SHARDS>
{
    /// Set the signer for peer identity and handshake authentication.
    ///
    /// This is a required field.
    pub fn signer<Sign>(
        self,
        signer: Sign,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, SHARDS> {
        SubductionBuilder {
            signer,
            spawner: self.spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_roundtrip_timeout: self.default_roundtrip_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_resident_trees: self.max_resident_trees,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sign, Store, Timer, Metric, const SHARDS: usize>
    SubductionBuilder<Sign, Unset, Store, Timer, Metric, SHARDS>
{
    /// Set the task spawner for background work.
    ///
    /// This is a required field. Common implementations:
    /// - `TokioSpawn` for native async (requires `Sendable`)
    /// - `WasmSpawn` for browser environments (requires `Local`)
    pub fn spawner<Sp>(
        self,
        spawner: Sp,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, SHARDS> {
        SubductionBuilder {
            signer: self.signer,
            spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_roundtrip_timeout: self.default_roundtrip_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_resident_trees: self.max_resident_trees,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sign, Sp, Timer, Metric, const SHARDS: usize>
    SubductionBuilder<Sign, Sp, Unset, Timer, Metric, SHARDS>
{
    /// Set the storage backend and authorization policy.
    ///
    /// This is a required field. The `storage` backend provides
    /// persistence, and the `policy` controls per-peer access.
    pub fn storage<Store, Auth>(
        self,
        storage: Store,
        policy: Arc<Auth>,
    ) -> SubductionBuilder<Sign, Sp, StoragePowerbox<Store, Auth>, Timer, Metric, SHARDS> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: StoragePowerbox::new(storage, policy),
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_roundtrip_timeout: self.default_roundtrip_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_resident_trees: self.max_resident_trees,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sign, Sp, Store, Metric, const SHARDS: usize>
    SubductionBuilder<Sign, Sp, Store, Unset, Metric, SHARDS>
{
    /// Set the timeout strategy for roundtrip calls.
    ///
    /// This is a required field. Common implementations:
    /// - `TokioTimeout` for native async
    /// - `JsTimeout` for browser environments
    pub fn timer<O>(self, timer: O) -> SubductionBuilder<Sign, Sp, Store, O, Metric, SHARDS> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: self.storage,
            timer,
            discovery_id: self.discovery_id,
            default_roundtrip_timeout: self.default_roundtrip_timeout,
            depth_metric: self.depth_metric,
            nonce_cache: self.nonce_cache,
            max_resident_trees: self.max_resident_trees,
            sedimentrees: self.sedimentrees,
        }
    }
}

impl<Sign, Sp, Store, Timer, Met, const SHARDS: usize>
    SubductionBuilder<Sign, Sp, Store, Timer, Met, SHARDS>
{
    /// Set the discovery ID for discovery-mode connections.
    ///
    /// Defaults to `None` (peer-to-peer mode only).
    #[must_use]
    pub const fn discovery_id(mut self, id: DiscoveryId) -> Self {
        self.discovery_id = Some(id);
        self
    }

    /// Set the default per-call **total deadline** for sync roundtrips
    /// (`BatchSyncRequest` → `BatchSyncResponse`), resolved when a caller
    /// passes [`CallTimeout::Default`](crate::timeout::call::CallTimeout::Default).
    ///
    /// This is **caller-side policy** applied over a cancel-safe wait, not a
    /// transport-layer fuse and **not** an idle/progress timeout: the
    /// multiplexer holds no clock. A blocking `sync_with_peer` is bounded by
    /// this deadline unless it supplies its own. Bounding by default matches
    /// the Erlang/OTP `GenServer.call` convention and keeps calls finite even
    /// on transports (e.g. HTTP long-poll) that don't guarantee an eventual
    /// disconnect on a byte-alive but protocol-silent peer.
    ///
    /// Defaults to [`DEFAULT_ROUNDTRIP_TIMEOUT`](crate::multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT)
    /// (30 s) if not set. Individual calls can override this per-request via
    /// [`CallTimeout`](crate::timeout::call::CallTimeout).
    #[must_use]
    pub const fn roundtrip_timeout(mut self, timeout: Duration) -> Self {
        self.default_roundtrip_timeout = Some(timeout);
        self
    }

    /// Override the depth metric used to assign commit depths.
    ///
    /// Defaults to [`CountLeadingZeroBytes`].
    pub fn depth_metric<Metric: DepthMetric>(
        self,
        metric: Metric,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, SHARDS> {
        SubductionBuilder {
            signer: self.signer,
            spawner: self.spawner,
            storage: self.storage,
            timer: self.timer,
            discovery_id: self.discovery_id,
            default_roundtrip_timeout: self.default_roundtrip_timeout,
            depth_metric: metric,
            nonce_cache: self.nonce_cache,
            max_resident_trees: self.max_resident_trees,
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

    /// Bound the number of sedimentrees kept resident in memory.
    ///
    /// The in-memory sedimentree map is an LRU cache over durable storage:
    /// when the resident set exceeds this many trees, the least-recently-used
    /// trees are evicted and transparently re-hydrated from storage on next
    /// access. This bounds memory by the active working set rather than the
    /// total number of documents ever synced.
    ///
    /// # Approximate, not a strict global cap
    ///
    /// The bound is enforced **per shard**: `max` is divided across the
    /// map's shards (`ceil(max / SHARDS)` per shard, floored at 1). So the
    /// effective global ceiling is `ceil(max / SHARDS) * SHARDS`, which can
    /// exceed `max` and is **at least `SHARDS`** (256 by default). Setting a
    /// value smaller than the shard count therefore still permits up to one
    /// resident tree per shard. Treat `max` as an order-of-magnitude target,
    /// not an exact limit. See
    /// [`BoundedShardedMap::with_capacity`](crate::collections::bounded_sharded_map::BoundedShardedMap::with_capacity).
    ///
    /// Defaults to unbounded (no eviction) — set this on servers / clients
    /// that sync large numbers of documents. Ignored if a pre-populated
    /// [`sedimentrees`](Self::sedimentrees) map is supplied (configure the
    /// cap on that map directly via `with_capacity`).
    #[must_use]
    pub const fn max_resident_trees(mut self, max: usize) -> Self {
        self.max_resident_trees = Some(max);
        self
    }

    /// Provide a pre-populated sedimentree map.
    ///
    /// Use this for hydration: load sedimentree state from storage,
    /// then pass the populated map here. Defaults to an empty
    /// [`BoundedShardedMap::new()`](crate::collections::bounded_sharded_map::BoundedShardedMap::new).
    #[must_use]
    pub fn sedimentrees(
        mut self,
        sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
    ) -> Self {
        self.sedimentrees = SedimentreesOption(Some(sedimentrees));
        self
    }
}

// -----------------------------------------------------------------------
// Build methods (only available when all required fields are set)
// -----------------------------------------------------------------------

impl<Sign, Sp, Store, Auth, Timer, Metric: DepthMetric, const SHARDS: usize>
    SubductionBuilder<Sign, Sp, StoragePowerbox<Store, Auth>, Timer, Metric, SHARDS>
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
    pub fn build<'a, Async, Conn>(
        self,
    ) -> (
        Arc<Subduction<'a, Async, Store, Conn, SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>, Auth, Sign, Timer, Sp, Metric, SHARDS>>,
        Arc<SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>>,
        ListenerFuture<'a, Async, Store, Conn, SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: SubductionFutureForm<'a, Store, Conn, SyncMessage, Auth, Sign, Metric, SHARDS> + 'static,
        Async: StartListener<'a, Store, Conn, SyncMessage, SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>, Auth, Sign, Metric, SHARDS>,
        Store: Storage<Async>,
        Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'a,
        Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + Send + Sync + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        'a: 'static,
        Metric: Clone,
        SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>: Handler<Async, Conn, Message = SyncMessage>,
        <SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS> as Handler<Async, Conn>>::HandlerError:
            Into<ListenError<Async, Store, Conn, SyncMessage>>,
        ManagedConnection<Conn, Async, Timer>: ManagedCall<
                Async,
                SyncMessage,
                SendError = <Conn as Connection<Async, SyncMessage>>::SendError,
            >,
    {
        let sedimentrees = self.sedimentrees.0.unwrap_or_else(|| {
            let map = BoundedShardedMap::new();
            let map = match self.max_resident_trees {
                Some(cap) => map.with_capacity(cap),
                None => map,
            };
            Arc::new(map)
        });

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            self.storage.clone(),
            self.depth_metric.clone(),
            self.spawner.clone(),
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
            send_counter,
            nonce_cache,
            self.timer,
            self.default_roundtrip_timeout
                .unwrap_or(DEFAULT_ROUNDTRIP_TIMEOUT),
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
    pub fn build_with_handler<'a, Async, Conn, Hdl>(
        self,
        handler: Arc<Hdl>,
    ) -> (
        Arc<Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>>,
        ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>
            + 'static,
        Async: StartListener<'a, Store, Conn, Hdl::Message, Hdl, Auth, Sign, Metric, SHARDS>,
        Store: Storage<Async>,
        Conn: Connection<Async, Hdl::Message> + PartialEq + Clone + 'a,
        Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + Send + Sync + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        'a: 'static,
        Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
        Hdl::Message: From<SyncMessage>,
        Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
        ManagedConnection<Conn, Async, Timer>: ManagedCall<
                Async,
                Hdl::Message,
                SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
            >,
    {
        let sedimentrees = self.sedimentrees.0.unwrap_or_else(|| {
            let map = BoundedShardedMap::new();
            let map = match self.max_resident_trees {
                Some(cap) => map.with_capacity(cap),
                None => map,
            };
            Arc::new(map)
        });

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            sedimentrees,
            connections,
            subscriptions,
            self.storage,
            PeerCounter::default(),
            nonce_cache,
            self.timer,
            self.default_roundtrip_timeout
                .unwrap_or(DEFAULT_ROUNDTRIP_TIMEOUT),
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
    /// The closure returns `(Arc<Hdl>, X)` where `X` is any extra data
    /// the caller wants to extract from the composition step (e.g.,
    /// an [`EphemeralHandler`] that shares the same `connections` map).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (sd, listener, manager, ephemeral) = SubductionBuilder::new()
    ///     .signer(signer)
    ///     .storage(my_storage, Arc::new(policy))
    ///     .spawner(TokioSpawn)
    ///     .build_composed::<Sendable, MyConn, _, _>(|sync_handler| {
    ///         let eph = EphemeralHandler::new(sync_handler.connections(), ...);
    ///         (Arc::new(MyComposedHandler { sync: sync_handler, ... }), eph)
    ///     });
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn build_composed<'a, Async, Conn, Hdl, X>(
        self,
        compose: impl FnOnce(Arc<SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>>) -> (Arc<Hdl>, X),
    ) -> (
        Arc<Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>>,
        ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        crate::connection::manager::ManagerFuture<Async>,
        X,
    )
    where
        Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS> + 'static,
        Async: StartListener<'a, Store, Conn, Hdl::Message, Hdl, Auth, Sign, Metric, SHARDS>,
        Store: Storage<Async>,
        Conn: Connection<Async, Hdl::Message> + Connection<Async, SyncMessage> + PartialEq + Clone + 'a,
        Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + Send + Sync + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        'a: 'static,
        Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
        Hdl::Message: From<SyncMessage>,
        Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
        Metric: Clone,
        SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS>: Handler<Async, Conn, Message = SyncMessage>,
        <SyncHandler<Async, Store, Conn, Auth, Metric, Sp, SHARDS> as Handler<Async, Conn>>::HandlerError:
            Into<ListenError<Async, Store, Conn, SyncMessage>>,
        ManagedConnection<Conn, Async, Timer>: ManagedCall<
                Async,
                Hdl::Message,
                SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
            >,
    {
        let sedimentrees = self.sedimentrees.0.unwrap_or_else(|| {
            let map = BoundedShardedMap::new();
            let map = match self.max_resident_trees {
                Some(cap) => map.with_capacity(cap),
                None => map,
            };
            Arc::new(map)
        });

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let sync_handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            self.storage.clone(),
            self.depth_metric.clone(),
            self.spawner.clone(),
        ));

        let send_counter = sync_handler.send_counter().clone();
        let (handler, extra) = compose(sync_handler);

        let (subduction, listener, manager) = Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            sedimentrees,
            connections,
            subscriptions,
            self.storage,
            send_counter,
            nonce_cache,
            self.timer,
            self.default_roundtrip_timeout
                .unwrap_or(DEFAULT_ROUNDTRIP_TIMEOUT),
            self.depth_metric,
            self.spawner,
        );

        (subduction, listener, manager, extra)
    }
}
