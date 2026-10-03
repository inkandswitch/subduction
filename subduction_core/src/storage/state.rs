//! Coordinate cache hydration with node-issued storage mutations.
//!
//! Each powerbox shares a registry with the capabilities it mints. A tree's
//! revision changes when a mutation starts and finishes; its active count also
//! lets catch-up readers distinguish a stable scan from unsettled persistence.
//! The cache validates a scan while holding its installation lock, so an old
//! read cannot reinstall data after a failed write or deletion invalidates it.
//! Bookkeeping locks never cover storage I/O or cache-lock acquisition.

use alloc::sync::{Arc, Weak};
use sedimentree_core::{collections::Map, id::SedimentreeId};
use spin::Mutex;

/// Shared, weakly registered tree state. Readers and mutations keep their tree
/// alive; idle entries are reclaimed when another capability is minted.
#[derive(Debug, Default)]
pub(crate) struct StorageState(Mutex<Map<SedimentreeId, Weak<TreeState>>>);
impl StorageState {
    pub(crate) fn tree(&self, id: SedimentreeId) -> Arc<TreeState> {
        let mut trees = self.0.lock();
        if let Some(tree) = trees.get(&id).and_then(Weak::upgrade) {
            return tree;
        }
        trees.retain(|_, tree| tree.strong_count() != 0);
        let tree = Arc::new(TreeState::default());
        trees.insert(id, Arc::downgrade(&tree));
        tree
    }
}

#[derive(Debug, Default)]
struct Revision {
    value: u64,
    active: usize,
}

/// Mutation and scan coordination for a single tree.
#[derive(Debug, Default)]
pub(crate) struct TreeState {
    revision: Mutex<Revision>,
    changed: event_listener::Event,
}
impl TreeState {
    pub(crate) fn begin(self: &Arc<Self>) -> MutationGuard {
        let mut state = self.revision.lock();
        state.active += 1;
        state.value = state.value.wrapping_add(1);
        MutationGuard {
            tree: self.clone(),
            finished: false,
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
    /// Register before checking `scan_start` to avoid losing a settlement wakeup.
    pub(crate) fn listen(&self) -> event_listener::EventListener {
        self.changed.listen()
    }
}

/// Mark settlement under the cache completion lock, then drop outside it to
/// wake readers. Owned execution is added separately; dropping unfinished work
/// currently releases waiters, not arbitrary external backend effects.
pub(crate) struct MutationGuard {
    tree: Arc<TreeState>,
    finished: bool,
}
impl MutationGuard {
    pub(crate) fn finish(&mut self) {
        if !self.finished {
            let mut state = self.tree.revision.lock();
            state.active -= 1;
            state.value = state.value.wrapping_add(1);
            self.finished = true;
        }
    }
}
impl Drop for MutationGuard {
    fn drop(&mut self) {
        self.finish();
        self.tree.changed.notify(usize::MAX);
    }
}
