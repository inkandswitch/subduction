//! Track changes to storage so old reads cannot put stale trees into the cache.
//!
//! # Why this is needed
//!
//! Storage is the source of truth; the in-memory trees are cached copies. A write
//! can save some items and then fail, or a deletion can remove some items before
//! returning an error. In either case, we clear the cached tree so the next read
//! loads what is actually in storage.
//!
//! Clearing the cache alone is not enough, because another read may already be
//! in progress:
//!
//! 1. A reader loads the old tree from storage, but has not cached it yet.
//! 2. A write saves another commit, then fails. Its error handler clears the cache.
//! 3. The reader finishes and puts its old copy into the cache, hiding the commit
//!    that was just saved.
//!
//! This module provides the information needed to detect that overlap and retry
//! the read instead of caching an outdated result.
//!
//! # How it works
//!
//! Each tree has a change counter (its "revision") and a count of writes or
//! deletes still in progress. The revision advances both when an operation starts
//! and when it finishes. An operation is normally finished only after it has
//! updated or cleared any cached entry, not just when its storage call returns.
//! A tree that is not cached is left that way; a later read will load it.
//! Storage handles created by the same powerbox share this information, so a
//! reader and a writer using different handles still track the same tree.
//!
//! Writers call [`TreeCoordination::track_mutation`] before storage work and keep
//! the returned [`TrackedMutation`] until their cache work is complete.
//!
//! A reader filling an empty cache calls [`TreeCoordination::prepare_read`]. This
//! waits until no writes or deletes are in progress and returns a [`ReadAttempt`]
//! remembering the tree and its revision. After loading storage, the reader calls
//! [`ReadAttempt::is_current`] under the cache lock. If a mutation overlapped the
//! read, the check fails and the reader retries. Waiting readers are woken when
//! the mutation's tracking handle is dropped.
//!
//! The final check and cache insertion happen under the same cache lock. Writers
//! also finish updating the counters under that lock when they update or clear
//! the cache. This prevents a writer from clearing the cache between a reader's
//! successful check and its insertion. If another operation has already filled
//! the cache, the reader uses that entry instead of its own loaded copy.
//!
//! Storage reads and writes do not hold the cache or counter locks. The locks
//! protecting these counters are also released before waiting for a cache lock.
//!
//! # Limits
//!
//! This does not make storage operations atomic. The node runs admitted writes
//! in its own tasks, so dropping an API waiter does not abandon cache maintenance.
//! If the executor drops an unfinished task, its guard marks the shared state
//! closed: backend work may still be running, so readers cannot assume it has
//! finished. Changes made directly through an unrelated storage handle are not
//! tracked here either.

use alloc::sync::{Arc, Weak};
use sedimentree_core::{collections::Map, id::SedimentreeId};
use spin::Mutex;

/// Shared, weakly registered tree state. Readers and mutations keep their tree
/// alive; idle entries are reclaimed when another capability is minted.
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
    pub(crate) fn tree(self: &Arc<Self>, id: SedimentreeId) -> TreeCoordination {
        let mut trees = self.trees.lock();
        if let Some(state) = trees.get(&id).and_then(Weak::upgrade) {
            return TreeCoordination { state };
        }
        trees.retain(|_, tree| tree.strong_count() != 0);
        let state = Arc::new(TreeState {
            owner: self.clone(),
            revision: Mutex::default(),
            changed: event_listener::Event::new(),
        });
        trees.insert(id, Arc::downgrade(&state));
        TreeCoordination { state }
    }
}

#[derive(Debug, Default)]
struct Revision {
    value: u64,
    active: usize,
}

#[derive(Debug)]
struct TreeState {
    owner: Arc<StorageState>,
    revision: Mutex<Revision>,
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
    /// Mark a write or delete as pending, before starting its storage work.
    /// This only tracks the operation; it neither authorizes nor performs it.
    pub(crate) fn track_mutation(&self) -> TrackedMutation {
        let mut state = self.state.revision.lock();
        state.active += 1;
        state.value = state.value.wrapping_add(1);
        TrackedMutation {
            coordination: self.clone(),
            completed: false,
        }
    }

    /// Wait for pending mutations to finish, then prepare to read storage.
    ///
    /// The returned attempt does not lock storage or prevent new mutations.
    /// Check it under the cache lock before installing the loaded tree.
    pub(crate) async fn prepare_read(&self) -> ReadAttempt {
        loop {
            // Listen first so a mutation finishing between the check and the
            // await cannot leave this reader waiting for a missed notification.
            let changed = self.state.changed.listen();
            let revision = {
                let state = self.state.revision.lock();
                (state.active == 0).then_some(state.value)
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
    /// When caching the result, call this while holding the cache lock and keep
    /// that lock until installation finishes; an earlier check is not enough.
    pub(crate) fn is_current(&self) -> bool {
        let state = self.coordination.state.revision.lock();
        state.active == 0 && state.value == self.revision
    }
}

/// A pending write or delete, including the work needed to update its cache.
/// Call `complete` under the cache lock, then drop this handle after releasing
/// the lock to wake waiting readers. Losing an unfinished driver closes
/// observation: external backend work might still be running.
#[must_use = "keep the handle until storage and cache work are complete"]
pub(crate) struct TrackedMutation {
    coordination: TreeCoordination,
    completed: bool,
}
impl TrackedMutation {
    /// Mark storage and cache work complete. Calling this again is harmless.
    /// Readers are notified when this handle is dropped, outside the cache lock.
    pub(crate) fn complete(&mut self) {
        if !self.completed {
            let mut state = self.coordination.state.revision.lock();
            state.active -= 1;
            state.value = state.value.wrapping_add(1);
            self.completed = true;
        }
    }
}
impl Drop for TrackedMutation {
    fn drop(&mut self) {
        if !self.completed {
            self.coordination.close();
        }
        self.complete();
        self.coordination.state.changed.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests;
