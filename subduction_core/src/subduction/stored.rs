//! Application-stream coordination above the shared storage validity boundary.
//!
//! This driver scans full verified items through the same local Fetcher used by
//! trusted reads, delivers catch-up/recovery with backpressure, and then consumes
//! bounded notifications. It does not execute mutations or route incoming sync
//! messages. No network listener is required for storage-only watching.
use crate::{
    spawn::yield_once,
    storage::{fetcher::Fetcher, state::Watch, traits::Storage},
    stored::{LoadError, StoredEvent, StoredItem},
};
use alloc::{boxed::Box, string::ToString, sync::Arc, vec::Vec};
use future_form::FutureForm;
use futures::future::{Either, select};

#[cfg(test)]
mod connection_tests;

pub(super) struct Output {
    pub(super) sender: async_channel::Sender<crate::stored::StoredMessage>,
    pub(super) terminal: Arc<spin::Mutex<Option<LoadError>>>,
}
impl Output {
    pub(super) async fn send(&mut self, event: StoredEvent) -> Result<(), LoadError> {
        self.send_marker(event, false).await
    }
    pub(super) async fn send_marker(
        &mut self,
        event: StoredEvent,
        load_ready: bool,
    ) -> Result<(), LoadError> {
        let (accepted, wait) = futures::channel::oneshot::channel();
        self.sender
            .send(crate::stored::StoredMessage {
                event,
                load_ready,
                accepted,
            })
            .await
            .map_err(|_| LoadError::Closed)?;
        // Do not remove another notification while this single delivery slot
        // is occupied. This is delivery backpressure, not a storage barrier.
        wait.await.map_err(|_| LoadError::Closed)
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let mut terminal = self.terminal.lock();
        if terminal.is_none() {
            *terminal = Some(LoadError::Closed);
        }
    }
}
pub(super) async fn snapshot<Async: FutureForm, Store: Storage<Async>>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
    initial: bool,
) -> Result<bool, LoadError> {
    loop {
        yield_once().await;
        let Some(revision) = watch.scan_start() else {
            watch.notified().await;
            continue;
        };
        let mut items: Vec<StoredItem> = fetcher
            .load_loose_commits()
            .await
            .map_err(|e| LoadError::Operation(e.to_string()))?
            .into_iter()
            .map(|item| StoredItem::Commit(Arc::new(item)))
            .collect();
        items.extend(
            fetcher
                .load_fragments()
                .await
                .map_err(|e| LoadError::Operation(e.to_string()))?
                .into_iter()
                .map(|item| StoredItem::Fragment(Arc::new(item))),
        );
        // Validate and clear only notifications covered by this stable read.
        // An overlapping mutation makes the scan retry, never certify a gap.
        if !watch.scan_finish(revision) {
            continue;
        }
        let present = !items.is_empty();
        for item in items {
            yield_once().await;
            output.send(StoredEvent::Item(item)).await?;
        }
        output
            .send_marker(StoredEvent::StorageChecked, initial && present)
            .await?;
        return Ok(present);
    }
}
pub(super) async fn deliver<Async: FutureForm, Store: Storage<Async>>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
    event: StoredEvent,
) -> Result<Option<bool>, LoadError> {
    let rescan = matches!(event, StoredEvent::RescanRequired);
    output.send(event).await?;
    if rescan {
        snapshot(fetcher, watch, output, false).await.map(Some)
    } else {
        Ok(None)
    }
}
pub(super) async fn live<Async: FutureForm, Store: Storage<Async>>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
) -> Result<(), LoadError> {
    loop {
        yield_once().await;
        if let Some(event) = watch.take() {
            deliver(fetcher, watch, output, event).await?;
        } else {
            watch.notified().await;
        }
    }
}

/// Follow connection generations using level-triggered snapshots, not a lossy
/// event queue. Register the wakeup before scanning so arrivals cannot fall into
/// a snapshot/listen gap. Completed attempts remain remembered until disconnect;
/// removals cancel outstanding attempts, and a replacement gets a fresh round.
/// Dropping this stream drops its listener and every in-flight round.
pub(super) fn connection_rounds<Conn, Key, Snapshot, SnapshotFuture, Sync, Round>(
    changed: Arc<event_listener::Event>,
    snapshot: Snapshot,
    sync: Sync,
) -> impl futures::Stream<Item = Result<Option<(crate::peer::id::PeerId, bool)>, LoadError>>
where
    Snapshot: Fn() -> SnapshotFuture,
    SnapshotFuture: core::future::Future<Output = Vec<(Conn, Arc<Key>)>>,
    Sync: Fn(Conn, Arc<Key>) -> Round,
    Round:
        core::future::Future<Output = Result<Option<(crate::peer::id::PeerId, bool)>, LoadError>>,
{
    use futures::{
        StreamExt,
        future::{AbortHandle, Abortable},
        stream::FuturesUnordered,
    };
    let known: Vec<(Arc<Key>, AbortHandle)> = Vec::new();
    let rounds: FuturesUnordered<Abortable<Round>> = FuturesUnordered::new();
    futures::stream::unfold(
        (snapshot, sync, known, rounds),
        move |(snapshot, sync, mut known, mut rounds)| {
            let changed = changed.clone();
            async move {
                loop {
                    let listener = changed.listen();
                    let current = snapshot().await;
                    known.retain(|(key, abort)| {
                        if current
                            .iter()
                            .any(|(_, candidate)| Arc::ptr_eq(key, candidate))
                        {
                            true
                        } else {
                            abort.abort();
                            false
                        }
                    });
                    for (conn, key) in current {
                        if !known.iter().any(|(seen, _)| Arc::ptr_eq(seen, &key)) {
                            let (abort, registration) = AbortHandle::new_pair();
                            rounds.push(Abortable::new(sync(conn, key.clone()), registration));
                            known.push((key, abort));
                        }
                    }
                    if rounds.is_empty() {
                        listener.await;
                        continue;
                    }
                    match select(Box::pin(rounds.next()), Box::pin(listener)).await {
                        Either::Left((Some(Ok(result)), _)) => {
                            return Some((result, (snapshot, sync, known, rounds)));
                        }
                        // A removed connection's round was cancelled, or the
                        // connection snapshot changed. Reconcile before proceeding.
                        Either::Left(_) | Either::Right(_) => {}
                    }
                }
            }
        },
    )
}

/// Drive scoped peer rounds and notifications together. Round construction uses
/// the node's existing sync implementation; this delivery loop needs no network,
/// signer, handler or runtime-form construction bounds of its own.
pub(super) async fn load<Async, Store, Rounds>(
    fetcher: &Fetcher<Async, Store>,
    watch: &Watch,
    output: &mut Output,
    rounds: Rounds,
    mut load_ready: bool,
    stop_after_load: bool,
) -> Result<(), LoadError>
where
    Async: FutureForm,
    Store: Storage<Async>,
    Rounds: futures::Stream<Item = Result<Option<(crate::peer::id::PeerId, bool)>, LoadError>>,
{
    use futures::{FutureExt, StreamExt};
    use sedimentree_core::collections::Set;
    futures::pin_mut!(rounds);
    let mut exhausted = false;
    let mut present = false;
    let mut suppliers = Set::new();
    let mut ready = None;
    loop {
        yield_once().await;
        // Poll rounds even under continuously ready local notification traffic.
        // A single result-handling path covers eager and awaited completions.
        let completed = ready.take().or_else(|| {
            if exhausted {
                None
            } else {
                rounds.next().now_or_never()
            }
        });
        if let Some(result) = completed {
            match result {
                Some(Ok(Some((peer, stored_content)))) => {
                    if stored_content && !load_ready {
                        suppliers.insert(peer);
                    }
                    watch.peer_synced(peer);
                }
                Some(Ok(None)) => {}
                Some(Err(error)) => return Err(error),
                None => exhausted = true,
            }
        }
        if let Some(event) = watch.take() {
            let qualifies = matches!(event, StoredEvent::PeerSynced { peer_id } if suppliers.contains(&peer_id));
            if matches!(event, StoredEvent::Item(_)) {
                present = true;
            }
            if qualifies && present {
                output.send_marker(event, true).await?;
                load_ready = true;
                suppliers.clear();
                if stop_after_load {
                    return Ok(());
                }
                // Watchers keep trying the remaining peers to establish their
                // subscriptions, rather than stopping at the first supplier.
                continue;
            }
            if let Some(snapshot_present) = deliver(fetcher, watch, output, event).await? {
                present = snapshot_present;
            }
            continue;
        }
        if exhausted {
            return if load_ready {
                Ok(())
            } else {
                Err(LoadError::Unavailable)
            };
        }
        // Wait for either source, then loop through the same result path.
        // Keep a completed round so dropping the notification waiter loses none.
        match select(Box::pin(rounds.next()), Box::pin(watch.notified())).await {
            Either::Left((result, _)) => ready = Some(result),
            Either::Right(_) => {}
        }
    }
}
