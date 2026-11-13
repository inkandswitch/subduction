//! Track changes to storage so old reads cannot put stale trees into the store.
//!
//! # Why this is needed
//!
//! Storage is the source of truth; the trees in the
//! [minimized tree store](crate::minimized_tree_store) are derived copies. A
//! write can save some items and then fail, or a deletion can remove some items
//! before returning an error. In either case, we remove the stored tree so the
//! next read loads what is actually in storage.
//!
//! Removing the tree alone is not enough, because another read may already be
//! in progress:
//!
//! 1. A reader loads the old tree from storage, but has not stored it yet.
//! 2. A write saves another commit, then fails. Its error handler removes the tree.
//! 3. The reader finishes and puts its old copy into the store, hiding the commit
//!    that was just saved.
//!
//! This module provides the information needed to detect that overlap and retry
//! the read instead of storing an outdated result.
//!
//! # How it works
//!
//! Each tree has a change counter (its "revision") and a count of writes or
//! deletes still in progress. The revision advances both when an operation starts
//! and when it finishes. An operation is normally finished only after it has
//! merged into or removed the stored tree, not just when its storage call
//! returns. A tree that is not stored is left that way; a later read will load it.
//! Storage handles created by the same powerbox share this information, so a
//! reader and a writer using different handles still track the same tree.
//!
//! Writers call [`TreeCoordination::track_mutation`] before submitting storage
//! work, then consume the tracking handle through [`TrackedMutation::persist`].
//! That operation owns persistence, store maintenance, settlement and notification.
//!
//! A reader filling an empty store calls [`TreeCoordination::prepare_cache_read`].
//! This waits until no writes or deletes are in progress and returns a
//! [`cache::CacheRead`] bound to the tree and destination store. After loading
//! storage, the reader consumes that handle through [`cache::CacheRead::populate`].
//! If a mutation overlapped the read, populating requests a retry. The raw
//! validity check and settlement operations are private: callers cannot separate
//! them from the store critical sections implemented in [`mod@cache`].
//!
//! Each tree has a store lock. The final check and the populate happen under it,
//! and writers finish updating the counters under it when they merge into or
//! remove the stored tree. This prevents a writer from removing the tree between
//! a reader's successful check and its populate. A shared store adds its own
//! generation check, which catches writers on other nodes.
//!
//! Storage reads and writes do not hold the store or counter locks. The locks
//! protecting these counters are also released before waiting for a store lock.
//! Store hits take neither lock.
//!
//! # Watching stored changes
//!
//! Application streams use these same counters when reading their initial
//! contents or recovering after missed changes. A stream registers for updates
//! before reading storage, then checks the revision before accepting that read.
//! A concurrent write therefore cannot fall between the initial read and the
//! stream's later updates without being noticed.
//!
//! Completed writes queue their stored items for watchers whether or not the
//! tree is stored. Failed writes and deletions instead tell watchers to reread
//! storage. The queues are temporary, not a durable event history: each holds
//! at most 256 items or 4 MiB. Overflow also asks the watcher to reread. These
//! limits do not bound the size of a full storage read.
//!
//! Queue updates happen with the revision change under the store lock; readers
//! and watchers are woken after the locks are released. Storage I/O and executor
//! calls never run while the counter locks are held.
//!
//! # Locking
//!
//! The registry mutex only finds or creates a tree and is released before using
//! that tree. Each tree then has one mutex protecting its revision, active count,
//! and all watcher queues. Accepting a read and clearing its covered updates is
//! one critical section; there is no separate queue lock to acquire in order.
//! Watch handles identify entries owned by the tree and remove them on drop.
//!
//! Store maintenance takes the tree's store lock before the tree lock. No code
//! holds the tree lock while waiting for the store, doing I/O, or notifying tasks.
//! Watch consumption also takes the tree lock, so consumers of the same tree
//! serialize their short queue operations with publication and read acceptance.
//!
//! # Limits
//!
//! This does not make storage operations atomic. The node runs admitted writes
//! in its own tasks, so dropping an API waiter does not abandon store maintenance.
//! If the executor drops an unfinished task, its guard marks the shared state
//! closed: backend work may still be running, so readers cannot assume it has
//! finished. The store keeps the tree from before that write. Changes made
//! directly through an unrelated storage handle are not tracked here either.

pub(crate) mod cache;

use crate::{
    peer::id::PeerId,
    stored::{StoredEvent, StoredItem},
};
use alloc::{
    collections::VecDeque,
    sync::{Arc, Weak},
    vec::Vec,
};
use async_lock::Mutex as AsyncMutex;
use sedimentree_core::{
    collections::{Map, Set},
    id::SedimentreeId,
};
use spin::Mutex;

pub(crate) const MAX_PENDING_ITEMS: usize = 256;
pub(crate) const MAX_PENDING_BYTES: usize = 4 * 1024 * 1024;
/// Idle trees are weakly registered; readers, mutations and watches retain them.
#[derive(Debug, Default)]
pub(crate) struct StorageState {
    trees: Mutex<Map<SedimentreeId, Weak<TreeState>>>,
    /// Trees whose store entry may be stale after a failed `merge`/`remove`.
    /// Reads skip the store for these until a `remove` succeeds.
    bypassed: Mutex<Set<SedimentreeId>>,
    closed: core::sync::atomic::AtomicBool,
    changed: event_listener::Event,
}
impl StorageState {
    pub(crate) fn close(&self) {
        self.closed
            .store(true, core::sync::atomic::Ordering::Release);
        self.changed.notify(usize::MAX);
    }
    pub(crate) fn closed(&self) -> bool {
        self.closed.load(core::sync::atomic::Ordering::Acquire)
    }
    pub(crate) async fn wait_closed(&self) {
        loop {
            let listener = self.changed.listen();
            if self.closed() {
                return;
            }
            listener.await;
        }
    }
    pub(crate) fn tree(self: &Arc<Self>, id: SedimentreeId) -> TreeCoordination {
        let mut trees = self.trees.lock();
        if let Some(state) = trees.get(&id).and_then(Weak::upgrade) {
            return TreeCoordination { state };
        }
        trees.retain(|_, tree| tree.strong_count() != 0);
        let state = Arc::new(TreeState {
            id,
            owner: self.clone(),
            inner: Mutex::default(),
            store: AsyncMutex::new(()),
            changed: event_listener::Event::new(),
        });
        trees.insert(id, Arc::downgrade(&state));
        TreeCoordination { state }
    }
    pub(crate) fn watch(self: &Arc<Self>, id: SedimentreeId) -> Watch {
        let tree = self.tree(id).state;
        let (notify, receiver) = async_channel::bounded(1);
        let id = tree.inner.lock().register(notify);
        Watch { tree, id, receiver }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct WatchId(u64);

/// One lock covers both read validity and the updates covered by a valid read.
#[derive(Debug, Default)]
struct TreeInner {
    revision: u64,
    active_mutations: usize,
    next_watch_id: u64,
    watchers: Map<WatchId, WatchState>,
}
impl TreeInner {
    fn register(&mut self, notify: async_channel::Sender<()>) -> WatchId {
        let mut id = WatchId(self.next_watch_id);
        // Even after counter wrap, never replace an existing watch.
        while self.watchers.contains_key(&id) {
            id.0 = id.0.wrapping_add(1);
        }
        self.next_watch_id = id.0.wrapping_add(1);
        self.watchers.insert(
            id,
            WatchState {
                pending: Pending::default(),
                markers: VecDeque::new(),
                notify,
            },
        );
        id
    }
}
/// The same revision validates minimized tree hydration and full-blob catch-up.
#[derive(Debug)]
struct TreeState {
    id: SedimentreeId,
    owner: Arc<StorageState>,
    inner: Mutex<TreeInner>,
    /// Serializes this tree's store maintenance: read validation with
    /// populating, and writes with settlement. Store hits do not take it.
    store: AsyncMutex<()>,
    changed: event_listener::Event,
}

/// Track writes, deletes, and storage reads for one tree.
/// Clones share the same counters; neither the counters nor their ownership
/// mechanism are exposed to callers.
#[derive(Debug, Clone)]
pub(crate) struct TreeCoordination {
    state: Arc<TreeState>,
}
impl TreeCoordination {
    /// Whether the shared storage mutation driver has been closed.
    pub(crate) fn closed(&self) -> bool {
        self.state.owner.closed()
    }
    /// Stop accepting mutations through any capability from this powerbox.
    pub(crate) fn close(&self) {
        self.state.owner.close();
    }
    /// Wait until the shared storage mutation driver closes.
    pub(crate) async fn wait_closed(&self) {
        self.state.owner.wait_closed().await;
    }

    /// Mark a write or delete as pending, before starting its storage work.
    /// This only tracks the operation; it neither authorizes nor performs it.
    pub(crate) fn track_mutation(&self) -> TrackedMutation {
        let mut state = self.state.inner.lock();
        state.active_mutations += 1;
        state.revision = state.revision.wrapping_add(1);
        TrackedMutation {
            coordination: self.clone(),
            completed: false,
            wake: Vec::new(),
        }
    }

    /// Wait for pending mutations to finish, then prepare to read storage.
    ///
    /// The returned attempt does not lock storage or prevent new mutations.
    /// Watchers accept it through `Watch::finish_read`; store readers use
    /// `prepare_cache_read` instead so validation cannot escape populating.
    pub(crate) async fn prepare_read(&self) -> ReadAttempt {
        loop {
            // Listen first so a mutation finishing between the check and the
            // await cannot leave this reader waiting for a missed notification.
            let changed = self.state.changed.listen();
            let revision = {
                let state = self.state.inner.lock();
                (state.active_mutations == 0).then_some(state.revision)
            };
            if let Some(revision) = revision {
                return ReadAttempt {
                    coordination: self.clone(),
                    revision,
                };
            }
            changed.await;
        }
    }
}

/// A storage read's starting revision, tied to the tree it belongs to.
/// This is not a storage snapshot or a lock: overlapping mutations invalidate it.
#[derive(Debug)]
#[must_use = "check the attempt before using the storage read"]
pub(crate) struct ReadAttempt {
    coordination: TreeCoordination,
    revision: u64,
}
impl ReadAttempt {
    /// Whether no mutation has started or finished since this read was prepared.
    /// When storing the result, call this while holding the store lock and keep
    /// that lock until populating finishes; an earlier check is not enough.
    fn is_current(&self) -> bool {
        let state = self.coordination.state.inner.lock();
        state.active_mutations == 0 && state.revision == self.revision
    }
}

/// A pending write or delete, including store updates and watcher publication.
/// Consume with `persist`, which owns store maintenance and notification order.
/// Losing an unfinished driver closes observation: backend work may continue.
#[must_use = "keep the handle until storage and store work are complete"]
pub(crate) struct TrackedMutation {
    coordination: TreeCoordination,
    completed: bool,
    wake: Vec<async_channel::Sender<()>>,
}
impl TrackedMutation {
    pub(crate) fn observed(&self) -> bool {
        !self.coordination.state.inner.lock().watchers.is_empty()
    }
    /// Mark store work complete and queue persisted items (or a rescan request).
    /// Live-node completion is called only by the store integration, under the
    /// store lock. Drop also settles abandoned work, but closes coordination first
    /// rather than claiming its store maintenance succeeded.
    /// Calling this again is harmless; notification happens on drop, outside locks.
    fn complete(&mut self, items: Option<&[StoredItem]>) {
        if self.completed {
            return;
        }
        let mut state = self.coordination.state.inner.lock();
        state.active_mutations -= 1;
        state.revision = state.revision.wrapping_add(1);
        let revision = state.revision;
        for watch in state.watchers.values_mut() {
            watch.pending.publish(revision, items);
            // Retain only the wake-up sender, not queued data or registration.
            self.wake.push(watch.notify.clone());
        }
        self.completed = true;
    }
}
impl Drop for TrackedMutation {
    fn drop(&mut self) {
        if !self.completed {
            self.coordination.close();
        }
        self.complete(None);
        self.coordination.state.changed.notify(usize::MAX);
        for notify in &self.wake {
            let _ = notify.try_send(());
        }
    }
}
#[derive(Debug)]
struct WatchState {
    pending: Pending,
    // Unlike incremental items, peer markers survive recovery. The accepted
    // snapshot is delivered before these markers are taken from the queue.
    markers: VecDeque<(u64, PeerId)>,
    notify: async_channel::Sender<()>,
}
impl WatchState {
    fn take(&mut self) -> Option<StoredEvent> {
        if let Pending::Updates(updates) = &self.pending
            && let Some((revision, _)) = self.markers.front()
            && updates
                .items
                .front()
                .is_none_or(|(item_revision, _)| item_revision > revision)
        {
            return self
                .markers
                .pop_front()
                .map(|(_, peer_id)| StoredEvent::PeerSynced { peer_id });
        }
        self.pending.take()
    }
}

/// A rescan request cannot retain a backlog, and later writes cannot clear it.
#[derive(Debug)]
enum Pending {
    Updates(BoundedUpdates),
    RescanRequired,
}
impl Default for Pending {
    fn default() -> Self {
        Self::Updates(BoundedUpdates::default())
    }
}
impl Pending {
    fn publish(&mut self, revision: u64, items: Option<&[StoredItem]>) {
        let Some(items) = items else {
            *self = Self::RescanRequired;
            return;
        };
        let Self::Updates(updates) = self else {
            return;
        };
        for item in items {
            if !updates.push(revision, item) {
                *self = Self::RescanRequired;
                return;
            }
        }
    }

    fn take(&mut self) -> Option<StoredEvent> {
        match self {
            Self::Updates(updates) => updates.pop().map(StoredEvent::Item),
            Self::RescanRequired => Some(StoredEvent::RescanRequired),
        }
    }

    fn reset_after_read(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug, Default)]
struct BoundedUpdates {
    items: VecDeque<(u64, StoredItem)>,
    bytes: usize,
}
impl BoundedUpdates {
    /// Return false without cloning the item if it would exceed either budget.
    fn push(&mut self, revision: u64, item: &StoredItem) -> bool {
        let bytes = item.byte_len();
        if self.items.len() >= MAX_PENDING_ITEMS
            || self.bytes.saturating_add(bytes) > MAX_PENDING_BYTES
        {
            return false;
        }
        self.bytes += bytes;
        self.items.push_back((revision, item.clone()));
        true
    }

    fn pop(&mut self) -> Option<StoredItem> {
        let (_, item) = self.items.pop_front()?;
        self.bytes -= item.byte_len();
        Some(item)
    }
}
/// One watcher, registered before scanning and unregistered when its driver drops.
pub(crate) struct Watch {
    tree: Arc<TreeState>,
    id: WatchId,
    receiver: async_channel::Receiver<()>,
}
impl Watch {
    pub(crate) async fn notified(&self) {
        let _ = self.receiver.recv().await;
    }
    /// Prepare a storage read after any pending mutations finish.
    pub(crate) async fn prepare_read(&self) -> ReadAttempt {
        TreeCoordination {
            state: self.tree.clone(),
        }
        .prepare_read()
        .await
    }
    /// Accept this watch's read and clear notifications covered by it.
    /// Validation and clearing are one operation under the tree mutex.
    pub(crate) fn finish_read(&self, attempt: &ReadAttempt) -> bool {
        let mut state = self.tree.inner.lock();
        if !Arc::ptr_eq(&self.tree, &attempt.coordination.state)
            || state.active_mutations != 0
            || state.revision != attempt.revision
        {
            return false;
        }
        let Some(watch) = state.watchers.get_mut(&self.id) else {
            return false;
        };
        watch.pending.reset_after_read();
        true
    }
    /// Only load-initiated rounds call this, after accepted writes settle.
    pub(crate) fn peer_synced(&self, peer: PeerId) {
        let mut state = self.tree.inner.lock();
        let revision = state.revision;
        if let Some(watch) = state.watchers.get_mut(&self.id) {
            watch.markers.push_back((revision, peer));
        }
    }
    pub(crate) fn take(&self) -> Option<StoredEvent> {
        self.tree.inner.lock().watchers.get_mut(&self.id)?.take()
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        let removed = self.tree.inner.lock().watchers.remove(&self.id);
        // Release queued data and the sender after releasing the tree lock.
        drop(removed);
    }
}

#[cfg(test)]
mod tests;
