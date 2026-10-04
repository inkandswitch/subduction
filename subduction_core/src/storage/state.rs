//! Shared mutation validity for cache hydration and application stored streams.
//!
//! The powerbox, capabilities, and watchers reuse one weak per-tree registry and
//! revision/active count. Cache installation validates that revision under its
//! existing lock; initial/recovery scans validate it before handing off to live
//! notifications. Neither scan can reinstall or certify pre-mutation contents.
//!
//! Notification queues are ephemeral buffers, not backend storage or historical
//! replay. Persistence and cache completion are owned by [`NodeTasks`](crate::subduction::NodeTasks), not driven
//! here. This module publishes settled results or uncertainty and ends validity
//! after driver loss. Independent backend writers remain outside the guarantee.
//!
//! Bookkeeping locks never cover backend I/O, executor calls, or wake callbacks.
//! Settlement marks cache/revision/publication together, then wakes outside the
//! cache lock. Queues have a fixed count/byte budget; snapshots do not.
use crate::stored::{StoredEvent, StoredItem};
use alloc::{
    collections::VecDeque,
    sync::{Arc, Weak},
    vec::Vec,
};
use sedimentree_core::{collections::Map, id::SedimentreeId};
use spin::Mutex;

pub(crate) const MAX_PENDING_ITEMS: usize = 256;
pub(crate) const MAX_PENDING_BYTES: usize = 4 * 1024 * 1024;
/// Idle trees are weakly registered; readers, mutations and watches retain them.
#[derive(Debug, Default)]
pub(crate) struct StorageState {
    trees: Mutex<Map<SedimentreeId, Weak<TreeState>>>,
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
    pub(crate) fn tree(self: &Arc<Self>, id: SedimentreeId) -> Arc<TreeState> {
        let mut trees = self.trees.lock();
        if let Some(tree) = trees.get(&id).and_then(Weak::upgrade) {
            return tree;
        }
        trees.retain(|_, tree| tree.strong_count() != 0);
        let tree = Arc::new(TreeState {
            owner: self.clone(),
            revision: Mutex::default(),
            changed: event_listener::Event::new(),
        });
        trees.insert(id, Arc::downgrade(&tree));
        tree
    }
    pub(crate) fn watch(self: &Arc<Self>, id: SedimentreeId) -> Watch {
        let tree = self.tree(id);
        let (notify, receiver) = async_channel::bounded(1);
        let queue = Arc::new(Queue {
            pending: Mutex::default(),
            notify,
        });
        tree.revision.lock().watchers.push(Arc::downgrade(&queue));
        Watch {
            tree,
            queue,
            receiver,
        }
    }
}
#[derive(Debug, Default)]
struct Revision {
    value: u64,
    active: usize,
    watchers: Vec<Weak<Queue>>,
}
/// The same revision validates metadata cache hydration and full-blob catch-up.
#[derive(Debug)]
pub(crate) struct TreeState {
    owner: Arc<StorageState>,
    revision: Mutex<Revision>,
    changed: event_listener::Event,
}
impl TreeState {
    pub(crate) fn closed(&self) -> bool {
        self.owner.closed()
    }
    pub(crate) fn close(&self) {
        self.owner.close();
    }
    pub(crate) async fn wait_closed(&self) {
        self.owner.wait_closed().await;
    }
    pub(crate) fn begin(self: &Arc<Self>) -> MutationGuard {
        let mut state = self.revision.lock();
        state.active += 1;
        state.value = state.value.wrapping_add(1);
        MutationGuard {
            tree: self.clone(),
            finished: false,
            wake: Vec::new(),
        }
    }
    pub(crate) fn revision(&self) -> u64 {
        self.revision.lock().value
    }
    pub(crate) fn scan_start(&self) -> Option<u64> {
        let state = self.revision.lock();
        (state.active == 0).then_some(state.value)
    }
    pub(crate) fn scan_current(&self, revision: u64) -> bool {
        let state = self.revision.lock();
        state.active == 0 && state.value == revision
    }
    pub(crate) fn unchanged(&self, revision: u64) -> bool {
        self.revision.lock().value == revision
    }
    /// Register before checking stability, avoiding lost settlement wakeups.
    pub(crate) fn listen(&self) -> event_listener::EventListener {
        self.changed.listen()
    }
}
/// Settlement is marked under the cache completion lock; drop wakes outside it.
pub(crate) struct MutationGuard {
    tree: Arc<TreeState>,
    finished: bool,
    wake: Vec<Arc<Queue>>,
}
impl MutationGuard {
    pub(crate) fn observed(&self) -> bool {
        self.tree
            .revision
            .lock()
            .watchers
            .iter()
            .any(|weak| weak.strong_count() != 0)
    }
    pub(crate) fn finish(&mut self, items: Option<&[StoredItem]>) {
        if self.finished {
            return;
        }
        let mut state = self.tree.revision.lock();
        state.active -= 1;
        state.value = state.value.wrapping_add(1);
        let revision = state.value;
        state.watchers.retain(|weak| {
            let Some(queue) = weak.upgrade() else {
                return false;
            };
            {
                let mut pending = queue.pending.lock();
                if let Some(items) = items {
                    for item in items {
                        if pending.dirty {
                            break;
                        }
                        let bytes = item.byte_len();
                        if pending.items.len() >= MAX_PENDING_ITEMS
                            || pending.bytes.saturating_add(bytes) > MAX_PENDING_BYTES
                        {
                            pending.dirty = true;
                            break;
                        }
                        pending.bytes += bytes;
                        pending.items.push_back((revision, item.clone()));
                    }
                } else {
                    pending.dirty = true;
                }
                if pending.dirty {
                    pending.items.clear();
                    pending.bytes = 0;
                }
            }
            self.wake.push(queue);
            true
        });
        self.finished = true;
    }
}
impl Drop for MutationGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.tree.close();
        }
        self.finish(None);
        self.tree.changed.notify(usize::MAX);
        for queue in &self.wake {
            let _ = queue.notify.try_send(());
        }
    }
}
#[derive(Debug)]
struct Queue {
    pending: Mutex<Pending>,
    notify: async_channel::Sender<()>,
}
#[derive(Debug, Default)]
struct Pending {
    items: VecDeque<(u64, StoredItem)>,
    bytes: usize,
    dirty: bool,
}
/// One watcher, registered before scanning and unregistered when its driver drops.
pub(crate) struct Watch {
    tree: Arc<TreeState>,
    queue: Arc<Queue>,
    receiver: async_channel::Receiver<()>,
}
impl Watch {
    pub(crate) async fn notified(&self) {
        let _ = self.receiver.recv().await;
    }
    pub(crate) fn scan_start(&self) -> Option<u64> {
        self.tree.scan_start()
    }
    pub(crate) fn scan_finish(&self, revision: u64) -> bool {
        let state = self.tree.revision.lock();
        if state.active != 0 || state.value != revision {
            return false;
        }
        let mut pending = self.queue.pending.lock();
        pending.items.clear();
        pending.bytes = 0;
        pending.dirty = false;
        true
    }
    pub(crate) fn take(&self) -> Option<StoredEvent> {
        let mut pending = self.queue.pending.lock();
        if pending.dirty {
            return Some(StoredEvent::RescanRequired);
        }
        pending.items.pop_front().map(|(_, item)| {
            pending.bytes -= item.byte_len();
            StoredEvent::Item(item)
        })
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        self.tree
            .revision
            .lock()
            .watchers
            .retain(|weak| weak.as_ptr() != Arc::as_ptr(&self.queue) && weak.strong_count() != 0);
        let mut pending = self.queue.pending.lock();
        pending.items.clear();
        pending.bytes = 0;
    }
}

#[cfg(test)]
mod tests;
