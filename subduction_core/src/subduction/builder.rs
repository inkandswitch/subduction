//! Type-safe builder for [`Subduction`] instances.
//!
//! Eliminates the boilerplate of constructing shared state, wiring up
//! a [`SyncHandler`], and passing 13 arguments to [`Subduction::new`].
//!
//! # Required Fields
//!
//! Four fields must be set before calling [`build`] or
//! [`build_with_handler`]. These are tracked at the type level —
//! calling `build` on a builder with missing fields is a compile error.
//!
//! | Field | Setter | Purpose |
//! |-------|--------|---------|
//! | `signer` | [`.signer()`] | Peer identity and handshake signing |
//! | `spawner` | [`.spawner()`] | Platform-specific task spawning |
//! | `storage` | [`.storage()`] | Storage backend + authorization policy |
//! | `timer` | [`.timer()`] | Timeout strategy for roundtrip calls |
//!
//! # Optional Fields
//!
//! | Field | Setter | Default |
//! |-------|--------|---------|
//! | `discovery_id` | [`.discovery_id()`] | `None` |
//! | `depth_metric` | [`.depth_metric()`] | [`CountLeadingZeroBytes`] |
//! | `heads_observer` | [`.heads_observer()`] | [`NoRemoteHeadsObserver`] (discards updates) |
//! | `nonce_cache` | [`.nonce_cache()`] | [`NonceCache::default()`] |
//! | `minimized_tree_store` | [`.minimized_tree_store()`] | Unbounded [`MemoryMinimizedTreeStore`] |
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
//!     .timer(InstantTimeout)
//!     .discovery_id(discovery_id)
//!     .build::<Sendable, MyConnection>();
//! ```
//!
//! `TokioSpawn`, `InstantTimeout`, and the mock connections live in
//! `subduction_core::connection::test_utils`, behind the `test_utils`
//! feature. Production nodes supply their own.
//!
//! The builder always produces a [`Config`] node. For an effects type of
//! your own, use [`Subduction::with_effects`].
//!
//! [`Subduction`]: super::Subduction
//! [`Config`]: crate::effects::config::Config
//! [`Subduction::new`]: super::Subduction::new
//! [`Subduction::with_effects`]: super::Subduction::with_effects
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//! [`build`]: SubductionBuilder::build
//! [`build_with_handler`]: SubductionBuilder::build_with_handler
//! [`.signer()`]: SubductionBuilder::signer
//! [`.spawner()`]: SubductionBuilder::spawner
//! [`.storage()`]: SubductionBuilder::storage
//! [`.timer()`]: SubductionBuilder::timer
//! [`.heads_observer()`]: SubductionBuilder::heads_observer
//! [`NoRemoteHeadsObserver`]: crate::remote_heads::NoRemoteHeadsObserver
//! [`.discovery_id()`]: SubductionBuilder::discovery_id
//! [`.depth_metric()`]: SubductionBuilder::depth_metric
//! [`.nonce_cache()`]: SubductionBuilder::nonce_cache
//! [`.minimized_tree_store()`]: SubductionBuilder::minimized_tree_store
//! [`CountLeadingZeroBytes`]: sedimentree_core::depth::CountLeadingZeroBytes
//! [`NonceCache::default()`]: crate::nonce_cache::NonceCache
//! [`MemoryMinimizedTreeStore`]: crate::minimized_tree_store::memory::MemoryMinimizedTreeStore

use alloc::sync::Arc;
use async_lock::Mutex;
use core::time::Duration;
use sedimentree_core::{
    collections::{Map, Set},
    depth::{CountLeadingZeroBytes, DepthMetric},
    id::SedimentreeId,
};

use crate::{
    authenticated::Authenticated,
    connection::{
        Connection,
        managed::{ManagedCall, ManagedConnection},
        message::SyncMessage,
    },
    effects::config::Config,
    handler::{Handler, sync::SyncHandler},
    handshake::audience::DiscoveryId,
    minimized_tree_store::{MinimizedTreeStore, memory::MemoryMinimizedTreeStore},
    multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::{NoRemoteHeadsObserver, RemoteHeadsNotifier, RemoteHeadsObserver},
    spawn::Spawn,
    storage::traits::Storage,
    timeout::Timeout,
};
use nonempty::NonEmpty;
use subduction_crypto::signer::Signer;

use super::{
    NodeTasks, Subduction, SubductionFutureForm, error::ListenError,
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
/// [`build`](Self::build) method is only available once `Sign`, `Sp`,
/// `Store`, and `Timer` have all been replaced with concrete types.
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
    HeadsObserver = NoRemoteHeadsObserver,
    Trees = MemoryMinimizedTreeStore,
> {
    signer: Sign,
    spawner: Sp,
    storage: Store,
    timer: Timer,
    heads_observer: HeadsObserver,

    discovery_id: Option<DiscoveryId>,
    default_roundtrip_timeout: Option<Duration>,
    depth_metric: Metric,
    nonce_cache: Option<NonceCache>,
    send_counter: Option<PeerCounter>,
    minimized_tree_store: Trees,
}

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------

impl
    SubductionBuilder<
        Unset,
        Unset,
        Unset,
        Unset,
        CountLeadingZeroBytes,
        NoRemoteHeadsObserver,
        MemoryMinimizedTreeStore,
    >
{
    /// Create a new builder with all defaults.
    ///
    /// The four required fields — `signer`, `spawner`, `storage`, and
    /// `timer` — must be set via their respective methods before
    /// calling [`build`](SubductionBuilder::build).
    ///
    /// Trees are kept in an unbounded [`MemoryMinimizedTreeStore`] unless
    /// [`minimized_tree_store`](SubductionBuilder::minimized_tree_store)
    /// supplies another store.
    #[must_use]
    pub fn new() -> Self {
        SubductionBuilder {
            signer: Unset,
            spawner: Unset,
            storage: Unset,
            timer: Unset,
            heads_observer: NoRemoteHeadsObserver,
            discovery_id: None,
            default_roundtrip_timeout: None,
            depth_metric: CountLeadingZeroBytes,
            nonce_cache: None,
            send_counter: None,
            minimized_tree_store: MemoryMinimizedTreeStore::new(),
        }
    }
}

impl Default
    for SubductionBuilder<
        Unset,
        Unset,
        Unset,
        Unset,
        CountLeadingZeroBytes,
        NoRemoteHeadsObserver,
        MemoryMinimizedTreeStore,
    >
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Sp, Store, Timer, Metric, HeadsObserver, Trees>
    SubductionBuilder<Unset, Sp, Store, Timer, Metric, HeadsObserver, Trees>
{
    /// Set the signer for peer identity and handshake authentication.
    ///
    /// This is a required field.
    pub fn signer<Sign>(
        self,
        signer: Sign,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, HeadsObserver, Trees> {
        let Self {
            signer: Unset,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        }
    }
}

impl<Sign, Store, Timer, Metric, HeadsObserver, Trees>
    SubductionBuilder<Sign, Unset, Store, Timer, Metric, HeadsObserver, Trees>
{
    /// Set the task spawner for background work.
    ///
    /// This is a required field. Common implementations:
    /// - `subduction_websocket::tokio::TokioSpawn` for native async (`Sendable`)
    /// - `WasmSpawn` for browser environments (`Local` only)
    pub fn spawner<Sp>(
        self,
        spawner: Sp,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, HeadsObserver, Trees> {
        let Self {
            signer,
            spawner: Unset,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        }
    }
}

impl<Sign, Sp, Timer, Metric, HeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, Unset, Timer, Metric, HeadsObserver, Trees>
{
    /// Set the storage backend and authorization policy.
    ///
    /// This is a required field. The `storage` backend provides
    /// persistence, and the `policy` controls per-peer access.
    pub fn storage<Store, Policy>(
        self,
        storage: Store,
        policy: Arc<Policy>,
    ) -> SubductionBuilder<Sign, Sp, (Store, Arc<Policy>), Timer, Metric, HeadsObserver, Trees>
    {
        let Self {
            signer,
            spawner,
            storage: Unset,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage: (storage, policy),
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        }
    }
}

impl<Sign, Sp, Store, Metric, HeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, Store, Unset, Metric, HeadsObserver, Trees>
{
    /// Set the timeout strategy for roundtrip calls.
    ///
    /// This is a required field. Common implementations:
    /// - `TokioTimeout` for native async
    /// - `JsTimeout` for browser environments
    pub fn timer<O>(
        self,
        timer: O,
    ) -> SubductionBuilder<Sign, Sp, Store, O, Metric, HeadsObserver, Trees> {
        let Self {
            signer,
            spawner,
            storage,
            timer: Unset,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        }
    }
}

impl<Sign, Sp, Store, Timer, Metric, OldHeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, Store, Timer, Metric, OldHeadsObserver, Trees>
{
    /// Set the [`RemoteHeadsObserver`] invoked whenever a remote peer
    /// reports its heads for a sedimentree — via a `HeadsUpdate` message,
    /// `sender_heads` on subscription pushes, or `responder_heads` in a
    /// sync response. Updates pass through a per-peer staleness filter
    /// (monotonic counter) before reaching the observer.
    ///
    /// Defaults to [`NoRemoteHeadsObserver`], which discards all
    /// notifications.
    ///
    /// Only consumed by [`build`](Self::build) and
    /// [`build_composed`](Self::build_composed), which construct the
    /// [`SyncHandler`] internally.
    /// [`build_with_handler`](Self::build_with_handler) takes a pre-built
    /// handler, so it is only available while the observer is unset;
    /// attach an observer to the handler directly (e.g. via
    /// [`SyncHandler::with_remote_heads_observer`]).
    pub fn heads_observer<NewHeadsObserver>(
        self,
        heads_observer: NewHeadsObserver,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, NewHeadsObserver, Trees> {
        let Self {
            signer,
            spawner,
            storage,
            timer,
            heads_observer: _replaced,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        }
    }
}

impl<Sign, Sp, Store, Timer, Met, HeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, Store, Timer, Met, HeadsObserver, Trees>
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
    /// Responding to a subscribing request for a locally missing document
    /// also waits for upstream fetching, for at most half this duration.
    /// This bounded responder-side wait breaks relay cycles and leaves time
    /// to send the response; the fetch can continue in the background.
    /// A caller's per-request override is not transmitted to the responder.
    ///
    /// Defaults to [`DEFAULT_ROUNDTRIP_TIMEOUT`] (30 s) if not set. Individual calls can override this per-request via
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
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Metric, HeadsObserver, Trees> {
        let Self {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric: _replaced,
            nonce_cache,
            send_counter,
            minimized_tree_store,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric: metric,
            nonce_cache,
            send_counter,
            minimized_tree_store,
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

    /// Override the shared per-peer send counter. Defaults to an unseeded
    /// counter (in-process monotonicity only).
    ///
    /// Embedders with a wall clock should pass
    /// `PeerCounter::with_seed(wall_clock_seed)`; see the
    /// `peer::counter` module docs for why.
    #[must_use]
    pub fn send_counter(mut self, counter: PeerCounter) -> Self {
        self.send_counter = Some(counter);
        self
    }

    /// Keep minimized trees in `store` instead of the default unbounded
    /// [`MemoryMinimizedTreeStore`].
    ///
    /// To bound memory, pass [`MemoryMinimizedTreeStore::with_capacity`]: the
    /// least-recently-used trees are evicted and rebuilt from storage on next
    /// access. To keep trees outside this process, pass your own
    /// [`MinimizedTreeStore`](crate::minimized_tree_store::MinimizedTreeStore),
    /// after reading its contract on sharing a store between nodes.
    pub fn minimized_tree_store<NewTrees>(
        self,
        store: NewTrees,
    ) -> SubductionBuilder<Sign, Sp, Store, Timer, Met, HeadsObserver, NewTrees> {
        let Self {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store: _replaced,
        } = self;

        SubductionBuilder {
            signer,
            spawner,
            storage,
            timer,
            heads_observer,
            discovery_id,
            default_roundtrip_timeout,
            depth_metric,
            nonce_cache,
            send_counter,
            minimized_tree_store: store,
        }
    }
}

// -----------------------------------------------------------------------
// Build methods (only available when all required fields are set)
// -----------------------------------------------------------------------

impl<Sign, Sp, Store, Policy, Timer, Metric: DepthMetric, HeadsObserver: RemoteHeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, (Store, Arc<Policy>), Timer, Metric, HeadsObserver, Trees>
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
    /// - `Async` — Future form (`Sendable` or `Local`). Not inferred;
    ///   name it in the turbofish.
    /// - `Conn` — Connection type. Name it in the turbofish too.
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
        Arc<
            Subduction<
                'a,
                Async,
                Config<Store, Policy, Sign, Timer, Sp, Trees>,
                Conn,
                SyncHandler<
                    Async,
                    Config<Store, Policy, Sign, Timer, Sp, Trees>,
                    Conn,
                    Metric,
                    HeadsObserver,
                >,
                Metric,
            >,
        >,
        Arc<
            SyncHandler<
                Async,
                Config<Store, Policy, Sign, Timer, Sp, Trees>,
                Conn,
                Metric,
                HeadsObserver,
            >,
        >,
        ListenerFuture<'a, Async>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: SubductionFutureForm<'a, Conn, SyncMessage> + 'static,
        Async: NodeTasks<
                'a,
                Config<Store, Policy, Sign, Timer, Sp, Trees>,
                Conn,
                SyncMessage,
                SyncHandler<
                    Async,
                    Config<Store, Policy, Sign, Timer, Sp, Trees>,
                    Conn,
                    Metric,
                    HeadsObserver,
                >,
                Metric,
            >,
        Store: Storage<Async>,
        Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'a,
        Policy: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        Trees: MinimizedTreeStore<Async> + Send + Sync,
        'a: 'static,
        Metric: Clone,
        SyncHandler<
            Async,
            Config<Store, Policy, Sign, Timer, Sp, Trees>,
            Conn,
            Metric,
            HeadsObserver,
        >: Handler<Async, Conn, Message = SyncMessage>,
        <SyncHandler<
            Async,
            Config<Store, Policy, Sign, Timer, Sp, Trees>,
            Conn,
            Metric,
            HeadsObserver,
        > as Handler<Async, Conn>>::HandlerError:
            Into<ListenError<Async, Store, Conn, SyncMessage>>,
        ManagedConnection<Conn, Async, Timer>: ManagedCall<
                Async,
                SyncMessage,
                SendError = <Conn as Connection<Async, SyncMessage>>::SendError,
            >,
    {
        let minimized_tree_store = self.minimized_tree_store;

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let storage = Async::storage_powerbox(
            self.storage.0,
            self.storage.1,
            minimized_tree_store.clone(),
            self.spawner.clone(),
        );
        let mut handler = SyncHandler::with_remote_heads_observer(
            minimized_tree_store.clone(),
            connections.clone(),
            subscriptions.clone(),
            storage.clone(),
            self.depth_metric.clone(),
            self.heads_observer,
            self.spawner.clone(),
        );
        if let Some(counter) = self.send_counter {
            handler = handler.with_send_counter(counter);
        }
        let handler = Arc::new(handler);

        let send_counter = handler.send_counter().clone();

        let (sd, listener, manager) = Subduction::new(
            handler.clone(),
            self.discovery_id,
            self.signer,
            minimized_tree_store,
            connections,
            subscriptions,
            storage,
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
}

impl<Sign, Sp, Store, Policy, Timer, Metric: DepthMetric, Trees>
    SubductionBuilder<Sign, Sp, (Store, Arc<Policy>), Timer, Metric, NoRemoteHeadsObserver, Trees>
{
    /// Build a [`Subduction`] instance with a custom [`Handler`].
    ///
    /// Use this when replacing the default [`SyncHandler`] with a
    /// custom handler implementation.
    ///
    /// Returns `(subduction, listener_future, manager_future)`.
    ///
    /// Only available while no [`heads_observer`](Self::heads_observer)
    /// has been set: this method takes a pre-built handler, so a
    /// builder-held observer would be silently discarded. Attach an
    /// observer to the handler itself instead (e.g. via
    /// [`SyncHandler::with_remote_heads_observer`]).
    ///
    /// The [`PeerCounter`] passed via [`send_counter`](Self::send_counter)
    /// (or a fresh default) is used for `Subduction`. If your custom
    /// handler also stamps outgoing messages with counters (e.g., wraps a
    /// [`SyncHandler`]), you must share the same `PeerCounter` between
    /// the handler and `Subduction` — use [`build`](Self::build) or [`build_composed`](Self::build_composed)
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
        Arc<
            Subduction<'a, Async, Config<Store, Policy, Sign, Timer, Sp, Trees>, Conn, Hdl, Metric>,
        >,
        ListenerFuture<'a, Async>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: SubductionFutureForm<'a, Conn, Hdl::Message> + 'static,
        Async: NodeTasks<
                'a,
                Config<Store, Policy, Sign, Timer, Sp, Trees>,
                Conn,
                Hdl::Message,
                Hdl,
                Metric,
            >,
        Store: Storage<Async>,
        Conn: Connection<Async, Hdl::Message> + PartialEq + Clone + 'a,
        Policy: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        Trees: MinimizedTreeStore<Async> + Send + Sync,
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
        let minimized_tree_store = self.minimized_tree_store;

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let storage = Async::storage_powerbox(
            self.storage.0,
            self.storage.1,
            minimized_tree_store.clone(),
            self.spawner.clone(),
        );
        Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            minimized_tree_store,
            connections,
            subscriptions,
            storage,
            self.send_counter.unwrap_or_default(),
            nonce_cache,
            self.timer,
            self.default_roundtrip_timeout
                .unwrap_or(DEFAULT_ROUNDTRIP_TIMEOUT),
            self.depth_metric,
            self.spawner,
        )
    }
}

impl<Sign, Sp, Store, Policy, Timer, Metric: DepthMetric, HeadsObserver: RemoteHeadsObserver, Trees>
    SubductionBuilder<Sign, Sp, (Store, Arc<Policy>), Timer, Metric, HeadsObserver, Trees>
{
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
        compose: impl FnOnce(
            Arc<
                SyncHandler<
                    Async,
                    Config<Store, Policy, Sign, Timer, Sp, Trees>,
                    Conn,
                    Metric,
                    HeadsObserver,
                >,
            >,
        ) -> (Arc<Hdl>, X),
    ) -> (
        Arc<
            Subduction<'a, Async, Config<Store, Policy, Sign, Timer, Sp, Trees>, Conn, Hdl, Metric>,
        >,
        ListenerFuture<'a, Async>,
        crate::connection::manager::ManagerFuture<Async>,
        X,
    )
    where
        Async: SubductionFutureForm<'a, Conn, Hdl::Message> + 'static,
        Async: NodeTasks<
                'a,
                Config<Store, Policy, Sign, Timer, Sp, Trees>,
                Conn,
                Hdl::Message,
                Hdl,
                Metric,
            >,
        Store: Storage<Async>,
        Conn: Connection<Async, Hdl::Message>
            + Connection<Async, SyncMessage>
            + PartialEq
            + Clone
            + 'a,
        Policy: ConnectionPolicy<Async> + StoragePolicy<Async>,
        Sign: Signer<Async>,
        Timer: Timeout<Async> + Clone + 'a,
        Sp: Spawn<Async> + Clone + Send + Sync + 'static,
        Trees: MinimizedTreeStore<Async> + Send + Sync,
        'a: 'static,
        Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
        Hdl::Message: From<SyncMessage>,
        Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
        Metric: Clone,
        SyncHandler<
            Async,
            Config<Store, Policy, Sign, Timer, Sp, Trees>,
            Conn,
            Metric,
            HeadsObserver,
        >: Handler<Async, Conn, Message = SyncMessage>,
        <SyncHandler<
            Async,
            Config<Store, Policy, Sign, Timer, Sp, Trees>,
            Conn,
            Metric,
            HeadsObserver,
        > as Handler<Async, Conn>>::HandlerError:
            Into<ListenError<Async, Store, Conn, SyncMessage>>,
        ManagedConnection<Conn, Async, Timer>: ManagedCall<
                Async,
                Hdl::Message,
                SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
            >,
    {
        let minimized_tree_store = self.minimized_tree_store;

        let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> =
            Arc::new(Mutex::new(Map::new()));
        let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
            Arc::new(Mutex::new(Map::new()));
        let nonce_cache = self.nonce_cache.unwrap_or_default();

        let storage = Async::storage_powerbox(
            self.storage.0,
            self.storage.1,
            minimized_tree_store.clone(),
            self.spawner.clone(),
        );
        let mut sync_handler = SyncHandler::with_remote_heads_observer(
            minimized_tree_store.clone(),
            connections.clone(),
            subscriptions.clone(),
            storage.clone(),
            self.depth_metric.clone(),
            self.heads_observer,
            self.spawner.clone(),
        );
        if let Some(counter) = self.send_counter {
            sync_handler = sync_handler.with_send_counter(counter);
        }
        let sync_handler = Arc::new(sync_handler);

        let send_counter = sync_handler.send_counter().clone();
        let (handler, extra) = compose(sync_handler);

        let (subduction, listener, manager) = Subduction::new(
            handler,
            self.discovery_id,
            self.signer,
            minimized_tree_store,
            connections,
            subscriptions,
            storage,
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
