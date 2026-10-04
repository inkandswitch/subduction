//! Exercise the cache/tree critical sections, including synchronous wake-up.
#![allow(clippy::expect_used, clippy::panic)]

use super::*;
use crate::{storage::state::StorageState, stored::StoredEvent};
use alloc::sync::Arc;
use core::{
    cell::Cell,
    sync::atomic::{AtomicBool, Ordering},
    task::Context,
};
use futures::{FutureExt, task::ArcWake};

const ID: SedimentreeId = SedimentreeId::new([71; 32]);

#[tokio::test]
async fn installation_checks_revision_after_acquiring_the_cache_lock() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let cache = Cache::<1>::new();
    let read = tree.prepare_cache_read(&cache).await;
    let install = read.install(Some(MinimizedSedimentree::default()), |_| ());
    futures::pin_mut!(install);

    let mutation = cache
        .with_optional_entry(&ID, |_| {
            // A premature validity check would succeed at this point, before
            // the reader discovers that the cache shard is locked.
            assert!(install.as_mut().now_or_never().is_none());
            tree.track_mutation()
        })
        .await;
    assert_eq!(install.await, None, "the blocked read must revalidate");
    assert!(cache.get_cloned(&ID).await.is_none());
    mutation
        .persist(&cache, async { Ok::<_, ()>(()) }, None, None)
        .await
        .expect("settle");
}

#[tokio::test]
async fn read_handle_installs_only_at_its_own_tree_key() {
    let state = Arc::new(StorageState::default());
    let other = SedimentreeId::new([72; 32]);
    let cache = Cache::<1>::new();
    let first = state.tree(ID).prepare_cache_read(&cache).await;
    let second = state.tree(other).prepare_cache_read(&cache).await;
    assert_eq!(second.install(None, |_| ()).await, Some(None));
    assert_eq!(
        first
            .install(Some(MinimizedSedimentree::default()), |_| ())
            .await,
        Some(Some(()))
    );
    assert!(cache.get_cloned(&ID).await.is_some());
    assert!(cache.get_cloned(&other).await.is_none());
}

/// Unlike a scheduler that merely enqueues work, this waker inspects the locks
/// immediately. Record violations without blocking or panicking in a destructor.
struct CheckUnlocked {
    tree: TreeCoordination,
    cache: Arc<Cache<1>>,
    woken: AtomicBool,
    locked_on_wake: AtomicBool,
}
impl ArcWake for CheckUnlocked {
    fn wake_by_ref(this: &Arc<Self>) {
        let tree_available = this.tree.state.inner.try_lock().is_some();
        let cache_available = this.cache.get_cloned(&ID).now_or_never().is_some();
        if !tree_available || !cache_available {
            this.locked_on_wake.store(true, Ordering::SeqCst);
        }
        this.woken.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn completion_waits_for_cache_and_notifies_only_after_both_locks_are_released() {
    for fail in [false, true] {
        let state = Arc::new(StorageState::default());
        let tree = state.tree(ID);
        let watch = state.watch(ID);
        let cache = Arc::new(Cache::<1>::new());
        cache
            .get_or_insert_with(ID, MinimizedSedimentree::default)
            .await;
        let mutation = tree.track_mutation();
        let read = tree.prepare_read();
        let notified = watch.notified();
        futures::pin_mut!(read, notified);

        let reader_wake = Arc::new(CheckUnlocked {
            tree: tree.clone(),
            cache: cache.clone(),
            woken: AtomicBool::new(false),
            locked_on_wake: AtomicBool::new(false),
        });
        let watch_wake = Arc::new(CheckUnlocked {
            tree: tree.clone(),
            cache: cache.clone(),
            woken: AtomicBool::new(false),
            locked_on_wake: AtomicBool::new(false),
        });
        let reader_waker = futures::task::waker(reader_wake.clone());
        let watch_waker = futures::task::waker(watch_wake.clone());
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(&reader_waker))
                .is_pending()
        );
        assert!(
            notified
                .as_mut()
                .poll(&mut Context::from_waker(&watch_waker))
                .is_pending()
        );

        let persisted = Cell::new(false);
        let writing = mutation.persist(
            &cache,
            async {
                persisted.set(true);
                if fail { Err(()) } else { Ok(()) }
            },
            Some(Sedimentree::default()),
            Some(Vec::new()),
        );
        futures::pin_mut!(writing);
        cache
            .with_optional_entry(&ID, |_| {
                assert!(writing.as_mut().now_or_never().is_none());
                assert!(persisted.get(), "storage runs before acquiring the cache");
                assert_eq!(tree.state.inner.lock().active_mutations, 1);
                assert!(
                    watch.take().is_none(),
                    "publication waits for cache maintenance"
                );
                assert!(!reader_wake.woken.load(Ordering::SeqCst));
                assert!(!watch_wake.woken.load(Ordering::SeqCst));
            })
            .await;

        assert_eq!(writing.await.is_err(), fail);
        assert!(reader_wake.woken.load(Ordering::SeqCst));
        assert!(watch_wake.woken.load(Ordering::SeqCst));
        assert!(!reader_wake.locked_on_wake.load(Ordering::SeqCst));
        assert!(!watch_wake.locked_on_wake.load(Ordering::SeqCst));
        assert!(read.now_or_never().expect("reader wakes").is_current());
        assert!(notified.now_or_never().is_some());
        assert_eq!(cache.get_cloned(&ID).await.is_none(), fail);
        if fail {
            assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
        } else {
            assert!(watch.take().is_none());
        }
        assert!(!state.closed());
    }
}

#[tokio::test]
async fn losing_completion_before_poll_or_while_waiting_for_cache_closes_observation() {
    for poll in [false, true] {
        let state = Arc::new(StorageState::default());
        let tree = state.tree(ID);
        let cache = Cache::<1>::new();
        let watch = state.watch(ID);
        let mut writing = alloc::boxed::Box::pin(tree.track_mutation().persist(
            &cache,
            async { Ok::<_, ()>(()) },
            Some(Sedimentree::default()),
            Some(Vec::new()),
        ));
        if poll {
            cache
                .with_optional_entry(&ID, |_| {
                    assert!(writing.as_mut().now_or_never().is_none());
                })
                .await;
        }
        assert!(!state.closed());
        drop(writing);
        assert!(state.closed());
        assert_eq!(tree.state.inner.lock().active_mutations, 0);
        assert!(cache.get_cloned(&ID).await.is_none());
        assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    }
}
