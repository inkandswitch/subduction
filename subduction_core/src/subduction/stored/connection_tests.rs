#![allow(clippy::expect_used)]
use super::connection_rounds;
use crate::peer::id::PeerId;
use alloc::{boxed::Box, sync::Arc, vec};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use futures::{FutureExt, StreamExt};
use std::{sync::Mutex, time::Duration};

#[tokio::test]
async fn arrival_during_snapshot_cannot_lose_its_wakeup() {
    let changed = Arc::new(event_listener::Event::new());
    let current = Mutex::new(vec![]);
    let first = AtomicBool::new(true);
    let (release, wait) = async_channel::bounded(1);
    let mut rounds = Box::pin(connection_rounds(
        changed.clone(),
        || async {
            let snapshot = current.lock().expect("snapshot").clone();
            if first.swap(false, Ordering::SeqCst) {
                wait.recv().await.expect("release snapshot");
            }
            snapshot
        },
        |peer, _: Arc<()>| async move { Ok(Some((peer, false))) },
    ));
    assert!(rounds.next().now_or_never().is_none());
    assert_eq!(changed.total_listeners(), 1);
    let peer = PeerId::new([1; 32]);
    current.lock().expect("update").push((peer, Arc::new(())));
    changed.notify(usize::MAX);
    release.send(()).await.expect("release");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), rounds.next()).await.expect("wakeup"),
        Some(Ok(Some((found, false)))) if found == peer
    ));
}

#[tokio::test]
async fn completed_generation_is_not_retried_but_replacement_is() {
    let changed = Arc::new(event_listener::Event::new());
    let peer = PeerId::new([2; 32]);
    let current = Mutex::new(vec![(peer, Arc::new(()))]);
    let started = AtomicUsize::new(0);
    let mut rounds = Box::pin(connection_rounds(
        changed.clone(),
        || async { current.lock().expect("snapshot").clone() },
        |peer, _| {
            let started = &started;
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                Ok(Some((peer, false)))
            }
        },
    ));
    assert!(matches!(rounds.next().await, Some(Ok(Some((found, false)))) if found == peer));
    assert!(rounds.next().now_or_never().is_none());
    changed.notify(usize::MAX);
    assert!(rounds.next().now_or_never().is_none());
    assert_eq!(started.load(Ordering::SeqCst), 1);
    // Same peer (and possibly same transport object), different registration.
    *current.lock().expect("replace") = vec![(peer, Arc::new(()))];
    changed.notify(usize::MAX);
    assert!(matches!(rounds.next().await, Some(Ok(Some((found, false)))) if found == peer));
    assert_eq!(started.load(Ordering::SeqCst), 2);
}

struct CountDrop<'a>(&'a AtomicUsize);
impl Drop for CountDrop<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn removal_and_stream_drop_cancel_rounds_and_release_listener() {
    let changed = Arc::new(event_listener::Event::new());
    let peer = PeerId::new([3; 32]);
    let current = Mutex::new(vec![(peer, Arc::new(()))]);
    let started = AtomicUsize::new(0);
    let dropped = AtomicUsize::new(0);
    let mut rounds = Box::pin(connection_rounds(
        changed.clone(),
        || async { current.lock().expect("snapshot").clone() },
        |_, _| async {
            let _guard = CountDrop(&dropped);
            started.fetch_add(1, Ordering::SeqCst);
            futures::future::pending().await
        },
    ));
    assert!(rounds.next().now_or_never().is_none());
    assert_eq!(started.load(Ordering::SeqCst), 1);
    current.lock().expect("remove").clear();
    changed.notify(usize::MAX);
    assert!(rounds.next().now_or_never().is_none());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    current
        .lock()
        .expect("reconnect")
        .push((peer, Arc::new(())));
    changed.notify(usize::MAX);
    assert!(rounds.next().now_or_never().is_none());
    assert_eq!(started.load(Ordering::SeqCst), 2);
    drop(rounds);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert_eq!(changed.total_listeners(), 0);
}
