//! A wrapper that bounds every call to a store, stops calling one that keeps
//! failing, and never serves a tree that a failed call may have left stale.
//!
//! # Why this exists
//!
//! The node never fails an operation because the store failed: a failed
//! lookup reads storage instead, and a failed `merge` or `remove` is only
//! logged. But a store that _hangs_ stalls whoever is waiting on it, a store
//! that is down still costs a failing remote call on every read, and a
//! failed `merge` or `remove` can leave a tree behind that lacks a write.
//! Wrapping a networked store in [`ResilientMinimizedTreeStore`] handles all
//! three, so an outage degrades to reading storage directly.
//!
//! - **Timeouts.** Every call is bounded by the node's [`Timeout`] effect. A
//!   call that runs out of time fails with [`ResilientError::TimedOut`].
//! - **Circuit breaker.** After a run of consecutive failures (errors or
//!   timeouts), the breaker _opens_: calls fail at once with
//!   [`ResilientError::Open`], without reaching the store. Every
//!   `probe_every`-th call while open is let through as a probe, and the
//!   first probe that succeeds closes the breaker.
//! - **Failed maintenance.** When a `merge` or `remove` fails for any of
//!   those reasons, the wrapper cannot tell whether it reached the store, so
//!   it marks the tree. Until a `remove` of that tree succeeds, `get` retries
//!   that `remove` first and fails if it does not land, `merge` becomes a
//!   `remove`, and `populate` reports [`Populate::Stale`]. This meets the
//!   [error contract](super#errors) for this process.
//!
//! The cooldown counts calls rather than time, so the wrapper needs no clock
//! and behaves the same on every target. Under load it probes often; when
//! idle it probes on the next few calls.
//!
//! On a shared store, other processes do not see the marks: they can keep
//! serving a tree from before the write until this process's retried
//! `remove` lands. See the `SharedMinimizedTreeStore` impl below.
//!
//! In-process stores neither fail nor hang, so they don't need this.

use alloc::sync::Arc;
use core::{
    error::Error,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
    time::Duration,
};

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    collections::Map,
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use spin::Mutex;

use super::{Lookup, MinimizedTreeStore, Populate, SharedMinimizedTreeStore};
use crate::timeout::{TimedOut, Timeout};

/// Consecutive failures that open the breaker, unless configured otherwise.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// While open, every this-many calls one is let through as a probe, unless
/// configured otherwise.
pub const DEFAULT_PROBE_EVERY: u32 = 32;

/// Wraps a store with per-call timeouts, a circuit breaker, and tracking of
/// trees left stale by failed maintenance. Clones share the breaker and the
/// tracking.
///
/// See the [module documentation](self).
#[derive(Debug, Clone)]
pub struct ResilientMinimizedTreeStore<Store, Timer> {
    inner: Store,
    timer: Timer,
    call_timeout: Duration,
    breaker: Arc<Breaker>,
    poisoned: Arc<Poisoned>,
}

impl<Store, Timer> ResilientMinimizedTreeStore<Store, Timer> {
    /// Wrap `inner`, bounding each call to `call_timeout`, with the default
    /// breaker settings.
    #[must_use]
    pub fn new(inner: Store, timer: Timer, call_timeout: Duration) -> Self {
        Self {
            inner,
            timer,
            call_timeout,
            breaker: Arc::new(Breaker::new(DEFAULT_FAILURE_THRESHOLD, DEFAULT_PROBE_EVERY)),
            poisoned: Arc::default(),
        }
    }

    /// Open the breaker after `failure_threshold` consecutive failures, and
    /// while open let one call in every `probe_every` through as a probe.
    ///
    /// # Panics
    ///
    /// Panics if either value is zero.
    #[must_use]
    pub fn with_breaker(mut self, failure_threshold: u32, probe_every: u32) -> Self {
        assert!(
            failure_threshold > 0,
            "the breaker needs at least one failure to open"
        );
        assert!(probe_every > 0, "an open breaker must eventually probe");
        self.breaker = Arc::new(Breaker::new(failure_threshold, probe_every));
        self
    }

    /// Whether the breaker is open, so calls are failing without reaching
    /// the wrapped store.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.breaker.open.load(Ordering::Acquire)
    }

    /// The wrapped store.
    pub const fn inner(&self) -> &Store {
        &self.inner
    }

    /// Whether a failed `merge` or `remove` may have left `id` stale, so the
    /// wrapper will not serve it until a `remove` lands.
    #[must_use]
    pub fn is_poisoned(&self, id: SedimentreeId) -> bool {
        self.poisoned.mark(id).is_some()
    }
}

/// Why a wrapped call failed.
#[derive(Debug, thiserror::Error)]
pub enum ResilientError<E: Error + 'static> {
    /// The wrapped store failed.
    #[error(transparent)]
    Store(E),

    /// The call ran out of time.
    #[error("minimized tree store call timed out")]
    TimedOut,

    /// The breaker is open, so the call did not reach the store.
    #[error("minimized tree store circuit breaker is open")]
    Open,
}

impl<E: Error + 'static> From<TimedOut> for ResilientError<E> {
    fn from(TimedOut: TimedOut) -> Self {
        Self::TimedOut
    }
}

/// Consecutive-failure counting with a call-counted probe while open.
/// Updates race benignly: the breaker only decides how often to try.
#[derive(Debug)]
struct Breaker {
    failure_threshold: u32,
    probe_every: u32,
    failures: AtomicU32,
    open: AtomicBool,
    skipped: AtomicU32,
}

impl Breaker {
    const fn new(failure_threshold: u32, probe_every: u32) -> Self {
        Self {
            failure_threshold,
            probe_every,
            failures: AtomicU32::new(0),
            open: AtomicBool::new(false),
            skipped: AtomicU32::new(0),
        }
    }

    /// Whether this call may reach the store.
    fn admit(&self) -> bool {
        if !self.open.load(Ordering::Acquire) {
            return true;
        }
        let n = self.skipped.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        n.is_multiple_of(self.probe_every)
    }

    fn succeeded(&self) {
        self.failures.store(0, Ordering::Relaxed);
        if self.open.swap(false, Ordering::AcqRel) {
            self.skipped.store(0, Ordering::Relaxed);
        }
    }

    fn failed(&self) {
        let failures = self
            .failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if failures >= self.failure_threshold {
            self.open.store(true, Ordering::Release);
        }
    }

    fn record<T, E: Error + 'static>(
        &self,
        outcome: Result<Result<T, E>, TimedOut>,
    ) -> Result<T, ResilientError<E>> {
        match outcome {
            Ok(Ok(value)) => {
                self.succeeded();
                Ok(value)
            }
            Ok(Err(error)) => {
                self.failed();
                Err(ResilientError::Store(error))
            }
            Err(TimedOut) => {
                self.failed();
                Err(ResilientError::TimedOut)
            }
        }
    }
}

/// Trees whose last `merge` or `remove` failed.
///
/// Each mark records which failure set it, so a `remove` that lands clears
/// only the failures that came before it, never a newer one.
#[derive(Debug, Default)]
struct Poisoned(Mutex<PoisonedInner>);

#[derive(Debug, Default)]
struct PoisonedInner {
    marks: Map<SedimentreeId, u64>,
    next: u64,
}

impl Poisoned {
    fn poison(&self, id: SedimentreeId) {
        let mut inner = self.0.lock();
        let mark = inner.next;
        inner.next = inner.next.wrapping_add(1);
        inner.marks.insert(id, mark);
    }

    fn mark(&self, id: SedimentreeId) -> Option<u64> {
        self.0.lock().marks.get(&id).copied()
    }

    /// Clear `id`'s mark, unless a newer failure replaced `seen`.
    fn heal(&self, id: SedimentreeId, seen: u64) {
        let mut inner = self.0.lock();
        if inner.marks.get(&id) == Some(&seen) {
            inner.marks.remove(&id);
        }
    }
}

#[future_form(
    Sendable where
        Store: MinimizedTreeStore<Sendable> + Send + Sync,
        Store::Generation: Send,
        Timer: Timeout<Sendable> + Send + Sync,
    Local where
        Store: MinimizedTreeStore<Local>,
        Timer: Timeout<Local>
)]
impl<Async: FutureForm, Store, Timer> MinimizedTreeStore<Async>
    for ResilientMinimizedTreeStore<Store, Timer>
{
    type Error = ResilientError<Store::Error>;
    type Generation = Store::Generation;
    type Entry = Store::Entry;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> Async::Future<'_, Result<Lookup<Store::Entry, Store::Generation>, Self::Error>> {
        Async::from_future(async move {
            // Retry the invalidation a failed call may have skipped. Until it
            // lands, fail rather than risk serving the stale tree.
            if let Some(seen) = self.poisoned.mark(id) {
                if !self.breaker.admit() {
                    return Err(ResilientError::Open);
                }
                let outcome = self
                    .timer
                    .timeout(self.call_timeout, self.inner.remove(id))
                    .await;
                self.breaker.record(outcome)?;
                self.poisoned.heal(id, seen);
            }
            if !self.breaker.admit() {
                return Err(ResilientError::Open);
            }
            let outcome = self
                .timer
                .timeout(self.call_timeout, self.inner.get(id))
                .await;
            self.breaker.record(outcome)
        })
    }

    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: Store::Generation,
    ) -> Async::Future<'_, Result<Populate, Self::Error>> {
        Async::from_future(async move {
            // `seen` may predate the failure, which the inner store's
            // generation may not reflect.
            if self.poisoned.mark(id).is_some() {
                return Ok(Populate::Stale);
            }
            if !self.breaker.admit() {
                return Err(ResilientError::Open);
            }
            let outcome = self
                .timer
                .timeout(self.call_timeout, self.inner.populate(id, tree, seen))
                .await;
            self.breaker.record(outcome)
        })
    }

    fn merge(
        &self,
        id: SedimentreeId,
        update: Sedimentree,
    ) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            // A tree that may lack an earlier write must not take this one on
            // top: drop it instead, which also clears the mark if it lands.
            if self.poisoned.mark(id).is_some() {
                return <Self as MinimizedTreeStore<Async>>::remove(self, id).await;
            }
            let result = if self.breaker.admit() {
                let outcome = self
                    .timer
                    .timeout(self.call_timeout, self.inner.merge(id, update))
                    .await;
                self.breaker.record(outcome)
            } else {
                Err(ResilientError::Open)
            };
            if result.is_err() {
                self.poisoned.poison(id);
            }
            result
        })
    }

    fn remove(&self, id: SedimentreeId) -> Async::Future<'_, Result<(), Self::Error>> {
        Async::from_future(async move {
            let seen = self.poisoned.mark(id);
            let result = if self.breaker.admit() {
                let outcome = self
                    .timer
                    .timeout(self.call_timeout, self.inner.remove(id))
                    .await;
                self.breaker.record(outcome)
            } else {
                Err(ResilientError::Open)
            };
            match (&result, seen) {
                (Ok(()), Some(seen)) => self.poisoned.heal(id, seen),
                (Ok(()), None) => {}
                (Err(_), _) => self.poisoned.poison(id),
            }
            result
        })
    }
}

/// Every call that succeeds went through the inner store, so the wrapper keeps
/// the inner store's rules for every call that succeeds.
///
/// A `merge` or `remove` that fails may not have changed the generation:
/// with [`ResilientError::Open`] it never reached the store, and with
/// [`ResilientError::TimedOut`] it may not have. The wrapper marks the tree
/// and keeps it out of use in _this_ process, but other processes sharing
/// the inner store do not see the mark. Until this process's retried
/// `remove` lands, they can keep serving the tree from before the write, or
/// populate one. To bound that, the inner store also needs entries that
/// expire or are validated against storage; see "Failed maintenance on a
/// shared store" in the design notes.
#[future_form(
    Sendable where
        Store: SharedMinimizedTreeStore<Sendable> + Send + Sync,
        Store::Generation: Send,
        Timer: Timeout<Sendable> + Send + Sync,
    Local where
        Store: SharedMinimizedTreeStore<Local>,
        Timer: Timeout<Local>
)]
impl<Async: FutureForm, Store, Timer> SharedMinimizedTreeStore<Async>
    for ResilientMinimizedTreeStore<Store, Timer>
{
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::minimized_tree_store::noop::NoEntry;
    use alloc::{vec, vec::Vec};
    use core::sync::atomic::{AtomicU8, AtomicUsize};
    use futures::{FutureExt, future::BoxFuture};

    const ID: SedimentreeId = SedimentreeId::new([0x7E; 32]);
    const OK: u8 = 0;
    const FAIL: u8 = 1;
    const HANG: u8 = 2;

    #[derive(Debug, thiserror::Error)]
    #[error("injected failure")]
    struct Injected;

    /// Times out any future that isn't ready on its first poll.
    #[derive(Debug, Clone, Copy)]
    struct PollOnce;

    impl Timeout<Sendable> for PollOnce {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, TimedOut>> {
            Box::pin(async move { fut.now_or_never().ok_or(TimedOut) })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct Switched {
        mode: Arc<AtomicU8>,
        calls: Arc<AtomicUsize>,
        /// The operations that reached the store, in order.
        ops: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Switched {
        fn set(&self, mode: u8) {
            self.mode.store(mode, Ordering::SeqCst);
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn ops(&self) -> Vec<&'static str> {
            self.ops.lock().clone()
        }

        fn call<T: Send + 'static>(
            &self,
            op: &'static str,
            value: T,
        ) -> BoxFuture<'_, Result<T, Injected>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.ops.lock().push(op);
            match self.mode.load(Ordering::SeqCst) {
                OK => Box::pin(async move { Ok(value) }),
                FAIL => Box::pin(async { Err(Injected) }),
                _ => Box::pin(futures::future::pending()),
            }
        }
    }

    impl MinimizedTreeStore<Sendable> for Switched {
        type Error = Injected;
        type Generation = ();
        type Entry = NoEntry;

        fn get(&self, _id: SedimentreeId) -> BoxFuture<'_, Result<Lookup<NoEntry, ()>, Injected>> {
            self.call("get", Lookup::Miss(()))
        }

        fn populate(
            &self,
            _id: SedimentreeId,
            _tree: MinimizedSedimentree,
            (): (),
        ) -> BoxFuture<'_, Result<Populate, Injected>> {
            self.call("populate", Populate::Populated)
        }

        fn merge(
            &self,
            _id: SedimentreeId,
            _update: Sedimentree,
        ) -> BoxFuture<'_, Result<(), Injected>> {
            self.call("merge", ())
        }

        fn remove(&self, _id: SedimentreeId) -> BoxFuture<'_, Result<(), Injected>> {
            self.call("remove", ())
        }
    }

    type Wrapped = ResilientMinimizedTreeStore<Switched, PollOnce>;

    fn wrapped() -> (Switched, Wrapped) {
        let inner = Switched::default();
        let store =
            ResilientMinimizedTreeStore::new(inner.clone(), PollOnce, Duration::from_secs(1))
                .with_breaker(3, 4);
        (inner, store)
    }

    async fn get(store: &Wrapped) -> Result<Lookup<NoEntry, ()>, ResilientError<Injected>> {
        MinimizedTreeStore::<Sendable>::get(store, ID).await
    }

    async fn merge(store: &Wrapped) -> Result<(), ResilientError<Injected>> {
        MinimizedTreeStore::<Sendable>::merge(store, ID, Sedimentree::default()).await
    }

    async fn populate(store: &Wrapped) -> Result<Populate, ResilientError<Injected>> {
        MinimizedTreeStore::<Sendable>::populate(store, ID, MinimizedSedimentree::default(), ())
            .await
    }

    #[tokio::test]
    async fn a_healthy_store_passes_through() {
        let (inner, store) = wrapped();
        assert!(matches!(get(&store).await, Ok(Lookup::Miss(()))));
        assert_eq!(inner.calls(), 1);
        assert!(!store.is_open());
    }

    #[tokio::test]
    async fn a_hung_call_times_out() {
        let (inner, store) = wrapped();
        inner.set(HANG);
        assert!(matches!(get(&store).await, Err(ResilientError::TimedOut)));
    }

    #[tokio::test]
    async fn consecutive_failures_open_the_breaker_and_short_circuit() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        for _ in 0..3 {
            assert!(matches!(
                get(&store).await,
                Err(ResilientError::Store(Injected))
            ));
        }
        assert!(store.is_open());
        for _ in 0..3 {
            assert!(matches!(get(&store).await, Err(ResilientError::Open)));
        }
        assert_eq!(inner.calls(), 3, "an open breaker does not call the store");
        assert!(
            matches!(get(&store).await, Err(ResilientError::Store(Injected))),
            "probe"
        );
        assert_eq!(
            inner.calls(),
            4,
            "every fourth call while open probes the store"
        );
        assert!(store.is_open(), "a failed probe keeps the breaker open");
    }

    #[tokio::test]
    async fn timeouts_count_as_failures() {
        let (inner, store) = wrapped();
        inner.set(HANG);
        for _ in 0..3 {
            assert!(matches!(get(&store).await, Err(ResilientError::TimedOut)));
        }
        assert!(store.is_open());
    }

    #[tokio::test]
    async fn a_successful_probe_closes_the_breaker() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        for _ in 0..3 {
            let _ = get(&store).await;
        }
        assert!(store.is_open());
        inner.set(OK);
        for _ in 0..3 {
            assert!(matches!(get(&store).await, Err(ResilientError::Open)));
        }
        assert!(matches!(get(&store).await, Ok(Lookup::Miss(()))), "probe");
        assert!(!store.is_open());
        assert!(matches!(get(&store).await, Ok(Lookup::Miss(()))));
        assert_eq!(inner.calls(), 5);
    }

    #[tokio::test]
    async fn a_success_resets_the_failure_count() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        let _ = get(&store).await;
        let _ = get(&store).await;
        inner.set(OK);
        assert!(get(&store).await.is_ok());
        inner.set(FAIL);
        let _ = get(&store).await;
        let _ = get(&store).await;
        assert!(!store.is_open(), "failures must be consecutive");
    }

    #[tokio::test]
    async fn a_failed_merge_is_not_served_until_a_remove_lands() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        assert!(merge(&store).await.is_err());
        assert!(store.is_poisoned(ID));

        assert!(
            matches!(get(&store).await, Err(ResilientError::Store(Injected))),
            "a lookup fails while the retried remove fails"
        );
        assert_eq!(
            inner.ops(),
            vec!["merge", "remove"],
            "the lookup retried the remove and never reached get"
        );

        inner.set(OK);
        assert!(matches!(get(&store).await, Ok(Lookup::Miss(()))));
        assert!(!store.is_poisoned(ID));
        assert!(matches!(get(&store).await, Ok(Lookup::Miss(()))));
        assert_eq!(
            inner.ops(),
            vec!["merge", "remove", "remove", "get", "get"],
            "a healed tree is looked up directly"
        );
    }

    #[tokio::test]
    async fn a_timed_out_merge_poisons_the_tree() {
        let (inner, store) = wrapped();
        inner.set(HANG);
        assert!(matches!(merge(&store).await, Err(ResilientError::TimedOut)));
        assert!(
            store.is_poisoned(ID),
            "a timed-out merge may or may not have landed"
        );
    }

    #[tokio::test]
    async fn a_merge_refused_by_an_open_breaker_poisons_the_tree() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        for _ in 0..3 {
            let _ = get(&store).await;
        }
        assert!(store.is_open());
        assert!(matches!(merge(&store).await, Err(ResilientError::Open)));
        assert!(store.is_poisoned(ID));
    }

    #[tokio::test]
    async fn a_populate_after_a_failed_merge_is_stale() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        assert!(merge(&store).await.is_err());
        inner.set(OK);
        assert!(matches!(populate(&store).await, Ok(Populate::Stale)));
        assert_eq!(
            inner.ops(),
            vec!["merge"],
            "the populate never reached the store"
        );
    }

    #[tokio::test]
    async fn a_merge_into_a_poisoned_tree_becomes_a_remove() {
        let (inner, store) = wrapped();
        inner.set(FAIL);
        assert!(merge(&store).await.is_err());
        inner.set(OK);
        assert!(merge(&store).await.is_ok());
        assert_eq!(inner.ops(), vec!["merge", "remove"]);
        assert!(!store.is_poisoned(ID), "the remove landed");
    }

    #[tokio::test]
    async fn a_remove_clears_only_failures_before_it() {
        let (_inner, store) = wrapped();
        store.poisoned.poison(ID);
        let seen = store.poisoned.mark(ID).expect("marked");
        store.poisoned.poison(ID);
        store.poisoned.heal(ID, seen);
        assert!(
            store.is_poisoned(ID),
            "a newer failure survives a heal for an older one"
        );
    }
}
