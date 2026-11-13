//! The side effects a [`Subduction`] node performs, named by one type.
//!
//! # Why one type, several traits
//!
//! A node has six effects: storage, policy, signing, spawning, timing, and
//! keeping minimized trees. Naming each as its own type parameter makes every
//! node and handler type long, and lets a node and its handler drift apart.
//! Instead, one effects type `E` names them all, so a node and its handler
//! name their components once:
//!
//! ```ignore
//! type Cfg = Config<MemoryStorage, OpenPolicy, MemorySigner, InstantTimeout, TokioSpawn>;
//! type Hdl = SyncHandler<Sendable, Cfg, MyConnection, CountLeadingZeroBytes>;
//! type Node = Subduction<'static, Sendable, Cfg, MyConnection, Hdl>;
//! ```
//!
//! Each effect is its own capability trait, so each consumer bounds `E` only
//! by what it uses. [`Subduction`] needs all of [`Effects`].
//! [`SyncHandler`](crate::handler::sync::SyncHandler) needs
//! [`HasMinimizedTreeStore`], [`HasStoragePolicy`], [`HasSpawner`], and
//! [`HasStorage`], and never sees a connection policy, signer, or timer.
//!
//! Transport ([`Connection`]) and protocol ([`Handler`]) stay separate
//! parameters: they vary per connection and per message type, not per node.
//!
//! # Effects
//!
//! Seven traits cover the six effects; policy has two. Each names its
//! component as an associated type:
//!
//! | Capability                | Associated type      | Bound                   | Purpose                               |
//! |---------------------------|----------------------|-------------------------|---------------------------------------|
//! | [`HasMinimizedTreeStore`] | `MinimizedTreeStore` | [`MinimizedTreeStore`]  | Keep built trees for hot reads        |
//! | [`HasStoragePolicy`]      | `Policy`             | [`StoragePolicy`]       | Decide who may fetch and put          |
//! | [`HasConnectionPolicy`]   | `Policy`             | + [`ConnectionPolicy`]  | Also decide who may connect           |
//! | [`HasSigner`]             | `Signer`             | [`Signer`]              | Sign handshakes with the node's key   |
//! | [`HasSpawner`]            | `Spawner`            | [`Spawn`] + [`Clone`]   | Run background tasks                  |
//! | [`HasStorage`]            | `Storage`            | [`Storage`]             | Persist commits, fragments, and blobs |
//! | [`HasTimer`]              | `Timer`              | [`Timeout`] + [`Clone`] | Bound roundtrip calls                 |
//!
//! The two policy capabilities share one `Policy` type, because a node
//! checks connections and storage access against the same object.
//!
//! [`HasStorage`] and [`HasStoragePolicy`] also surface their components'
//! error types (`StorageError`, `FetchDisallowed`, `PutDisallowed`) as
//! associated types, the way [`IntoIterator`] surfaces `Item`. Callers write
//! `E::StorageError` instead of `<E::Storage as Storage<Async>>::Error`, and
//! the trait checks that each one matches its component.
//!
//! [`Effects`] is all of them together. It has a blanket impl, so never
//! implement it directly.
//!
//! # Implied bounds
//!
//! The bounds in this module are supertraits or associated-type bounds, so
//! consumers that bound `E` never restate them. The one exception is
//! deliberate: each `Has*` trait restates its component's error bounds,
//! because an equality bound does not carry the target's bounds over.
//!
//! Requirements come in three tiers:
//!
//! - [`Effects`] is form-agnostic. It carries the component traits,
//!   `'static` on the timer, `Send + Sync + 'static` on the spawner, and
//!   `Send + Sync` on the minimized tree store. Both are thread-safe even on
//!   [`Local`], because connection management and the storage powerbox take
//!   one spawner bound for both forms, and the powerbox's mutation runner,
//!   which holds the store, is `Send + Sync` on both.
//! - [`SendableEffects`] adds thread-safety: every component `Send + Sync`,
//!   every error `Send`. Bound generic `Sendable` code by this.
//! - [`LocalEffects`] adds only `'static` on `E`. On [`Local`], everything
//!   but the spawner and the store may be `!Send`. Bound generic `Local` code
//!   by this.
//!
//! Both refinements make `E: 'static`, which makes every slot `'static`: a
//! projection of a `'static` type outlives `'static` too.
//!
//! Node types and constructors bound `E: Effects<Async>`; only the impls
//! that spawn node work ask for a refinement. [`SyncHandler`] bounds only
//! the four capabilities it uses.
//!
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//!
//! # Choosing an implementation
//!
//! [`Config`] is the stock implementation. It carries no data; it only
//! names the component types. [`Subduction::new`] and the builder infer it
//! from the components you pass in.
//!
//! To get a node type without nested generics, implement the capabilities
//! on a type of your own. It needs `Clone` and `Debug` only if you want
//! those on the node itself, since `Subduction` derives them. The error
//! types must match the components' own; the compiler rejects a mismatch.
//! Build the node with [`Subduction::with_effects`]; the builder only
//! produces [`Config`] nodes.
//!
//! ```ignore
//! // `RedbStorage` is in `subduction_redb_storage`; `FuturesTimerTimeout`
//! // and `TokioSpawn` are in `subduction_websocket` (`timeout`, `tokio`).
//! #[derive(Clone, Debug)]
//! enum ServerEffects {}
//!
//! impl HasStoragePolicy<Sendable> for ServerEffects {
//!     type Policy = OpenPolicy;
//!     type FetchDisallowed = Infallible;
//!     type PutDisallowed = Infallible;
//! }
//!
//! impl HasStorage<Sendable> for ServerEffects {
//!     type Storage = RedbStorage;
//!     type StorageError = RedbStorageError;
//! }
//!
//! impl HasMinimizedTreeStore<Sendable> for ServerEffects {
//!     type MinimizedTreeStore = MemoryMinimizedTreeStore;
//! }
//!
//! impl HasSigner<Sendable> for ServerEffects { type Signer = MemorySigner; }
//! impl HasSpawner<Sendable> for ServerEffects { type Spawner = TokioSpawn; }
//! impl HasTimer<Sendable> for ServerEffects { type Timer = FuturesTimerTimeout; }
//!
//! type Node = Subduction<'static, Sendable, ServerEffects, MyConnection, MyHandler>;
//! ```
//!
//! [`Connection`]: crate::connection::Connection
//! [`ConnectionPolicy`]: crate::policy::connection::ConnectionPolicy
//! [`Config`]: config::Config
//! [`Handler`]: crate::handler::Handler
//! [`MinimizedTreeStore`]: crate::minimized_tree_store::MinimizedTreeStore
//! [`HasConnectionPolicy`]: policy::HasConnectionPolicy
//! [`HasMinimizedTreeStore`]: minimized_tree_store::HasMinimizedTreeStore
//! [`HasStoragePolicy`]: policy::HasStoragePolicy
//! [`Signer`]: subduction_crypto::signer::Signer
//! [`Spawn`]: crate::spawn::Spawn
//! [`Storage`]: crate::storage::traits::Storage
//! [`StoragePolicy`]: crate::policy::storage::StoragePolicy
//! [`Subduction`]: crate::subduction::Subduction
//! [`Subduction::new`]: crate::subduction::Subduction::new
//! [`Subduction::with_effects`]: crate::subduction::Subduction::with_effects
//! [`Timeout`]: crate::timeout::Timeout

pub mod config;
pub mod minimized_tree_store;
pub mod policy;
pub mod signer;
pub mod spawner;
pub mod storage;
pub mod timer;

use future_form::{FutureForm, Local, Sendable};

use crate::{minimized_tree_store::MinimizedTreeStore, policy::connection::ConnectionPolicy};

use self::{
    minimized_tree_store::HasMinimizedTreeStore, policy::HasConnectionPolicy, signer::HasSigner,
    spawner::HasSpawner, storage::HasStorage, timer::HasTimer,
};

/// Every capability a [`Subduction`](crate::subduction::Subduction) node uses.
///
/// This is a trait alias with a blanket impl. Implement the `Has*` traits
/// instead. See the [module documentation](self) for details.
///
/// The spawner and the minimized tree store are `Send + Sync` even on
/// [`Local`]. [`RunManager::run_manager`] and
/// [`StorageTasks::storage_powerbox`] each take one spawner bound for both
/// forms, and the powerbox's mutation runner, which captures the store, is
/// `Send + Sync` on both.
///
/// [`RunManager::run_manager`]: crate::connection::manager::RunManager::run_manager
/// [`StorageTasks::storage_powerbox`]: crate::subduction::StorageTasks::storage_powerbox
pub trait Effects<Async: FutureForm>:
    HasConnectionPolicy<Async>
    + HasMinimizedTreeStore<Async, MinimizedTreeStore: Send + Sync>
    + HasSigner<Async>
    + HasSpawner<Async, Spawner: Send + Sync + 'static>
    + HasStorage<Async>
    + HasTimer<Async, Timer: 'static>
{
}

impl<
    Async: FutureForm,
    E: HasConnectionPolicy<Async>
        + HasMinimizedTreeStore<Async, MinimizedTreeStore: Send + Sync>
        + HasSigner<Async>
        + HasSpawner<Async, Spawner: Send + Sync + 'static>
        + HasStorage<Async>
        + HasTimer<Async, Timer: 'static>,
> Effects<Async> for E
{
}

/// [`Effects`] for a [`Sendable`] node: every component and error is
/// thread-safe, so node work can move onto a worker pool.
///
/// Every slot is also `'static`, because `E` is: a projection of a
/// `'static` type outlives `'static` too.
///
/// Blanket-implemented. Generic `Sendable` code bounds `E: SendableEffects`
/// instead of restating each slot.
// The blanket impl repeats this bound list; keep the two in sync.
pub trait SendableEffects:
    'static
    + Effects<
        Sendable,
        FetchDisallowed: Send,
        MinimizedTreeStore: MinimizedTreeStore<Sendable, Error: Send, Generation: Send + Sync>,
        Policy: Send + Sync + ConnectionPolicy<Sendable, ConnectionDisallowed: Send>,
        PutDisallowed: Send,
        Signer: Send + Sync,
        Storage: Send + Sync,
        StorageError: Send,
        Timer: Send + Sync,
    >
{
}

impl<
    E: 'static
        + Effects<
            Sendable,
            FetchDisallowed: Send,
            MinimizedTreeStore: MinimizedTreeStore<Sendable, Error: Send, Generation: Send + Sync>,
            Policy: Send + Sync + ConnectionPolicy<Sendable, ConnectionDisallowed: Send>,
            PutDisallowed: Send,
            Signer: Send + Sync,
            Storage: Send + Sync,
            StorageError: Send,
            Timer: Send + Sync,
        >,
> SendableEffects for E
{
}

/// [`Effects`] for a [`Local`] node: every component and error is
/// `'static`, so node work can live in spawned single-threaded tasks.
///
/// Only `E` itself is bounded: a projection of a `'static` type outlives
/// `'static` too. Nothing needs to be `Send` except the spawner and the
/// minimized tree store, which [`Effects`] already requires.
///
/// Blanket-implemented. Generic `Local` code bounds `E: LocalEffects`
/// instead of restating each slot.
pub trait LocalEffects: 'static + Effects<Local> {}

impl<E: 'static + Effects<Local>> LocalEffects for E {}
