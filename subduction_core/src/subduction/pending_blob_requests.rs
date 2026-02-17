//! Bounded LRU cache for tracking pending blob requests.
//!
//! Prevents unbounded memory growth from unresponsive peers while avoiding
//! starvation of legitimate requests through sync-completion cleanup.

use alloc::vec::Vec;
use sedimentree_core::{blob::Blob, collections::Set, crypto::digest::Digest, id::SedimentreeId};

/// Default maximum number of pending blob requests.
pub const DEFAULT_MAX_PENDING_BLOB_REQUESTS: usize = 10_000;

type Entry = (SedimentreeId, Digest<Blob>);

/// Bounded LRU cache for pending blob requests.
///
/// Tracks `(SedimentreeId, Digest<Blob>)` pairs that we've requested and are
/// expecting to receive. Uses insertion-order for LRU eviction when capacity
/// is exceeded.
///
/// # Complexity
///
/// Uses a `Vec` for LRU ordering and a `Set` for fast membership checks:
/// - `contains`: O(1) with `std`, O(log n) without
/// - `insert` (new entry): O(1) with `std`, O(log n) without
/// - `insert` (move to back): O(n) — requires finding position in Vec
/// - `remove`: O(n) — requires finding position in Vec
/// - `remove_for_sedimentree`: O(n)
///
/// # Cleanup Strategies
///
/// 1. **On response**: Entries are removed when matching blobs are received
/// 2. **On sync completion**: All entries for a sedimentree are cleared after
///    successful sync (primary cleanup mechanism)
/// 3. **LRU eviction**: Oldest entries are evicted when capacity is exceeded
///    (safety valve for pathological cases)
#[derive(Debug, Clone)]
pub struct PendingBlobRequests {
    /// Insertion-ordered vec for LRU eviction.
    ///
    /// Older entries are at the front, newer at the back.
    entries: Vec<Entry>,

    /// Index for fast membership checks.
    ///
    /// Uses `HashSet` with `std`, `BTreeSet` without.
    index: Set<Entry>,

    /// Maximum capacity before LRU eviction kicks in.
    max_capacity: usize,
}

impl Default for PendingBlobRequests {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PENDING_BLOB_REQUESTS)
    }
}

impl PendingBlobRequests {
    /// Create a new cache with the specified maximum capacity.
    #[must_use]
    pub fn new(max_capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(max_capacity.min(1024)),
            index: Set::default(),
            max_capacity,
        }
    }

    /// Insert a pending blob request.
    ///
    /// If the entry already exists, it is moved to the back (most recent).
    /// If capacity is exceeded after insertion, the oldest entries are evicted
    /// and a warning is logged.
    pub fn insert(&mut self, id: SedimentreeId, digest: Digest<Blob>) {
        let entry = (id, digest);

        // If already present, move to back (most recent)
        if self.index.contains(&entry) {
            if let Some(pos) = self.entries.iter().position(|e| *e == entry) {
                self.entries.remove(pos);
            }
            self.entries.push(entry);
            return;
        }

        // Insert new entry
        self.entries.push(entry);
        self.index.insert(entry);

        // LRU eviction if over capacity
        if self.entries.len() > self.max_capacity {
            let evicted = self.entries.len() - self.max_capacity;
            tracing::warn!(
                evicted,
                capacity = self.max_capacity,
                "pending_blob_requests at capacity, evicting oldest entries"
            );
            // Remove oldest entries from the front and update index
            for entry in self.entries.drain(0..evicted) {
                self.index.remove(&entry);
            }
        }
    }

    /// Remove a pending blob request, returning true if it was present.
    pub fn remove(&mut self, id: SedimentreeId, digest: Digest<Blob>) -> bool {
        let entry = (id, digest);
        if self.index.remove(&entry) {
            if let Some(pos) = self.entries.iter().position(|e| *e == entry) {
                self.entries.remove(pos);
            }
            true
        } else {
            false
        }
    }

    /// Remove all pending blob requests for a sedimentree.
    ///
    /// Called after successful sync completion to clean up entries that are
    /// no longer relevant.
    pub fn remove_for_sedimentree(&mut self, id: SedimentreeId) {
        self.entries.retain(|(sid, _)| *sid != id);
        self.index.retain(|(sid, _)| *sid != id);
    }

    /// Check if a pending blob request exists.
    #[must_use]
    pub fn contains(&self, id: SedimentreeId, digest: Digest<Blob>) -> bool {
        self.index.contains(&(id, digest))
    }

    /// Get the number of pending requests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if there are no pending requests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id(n: u8) -> SedimentreeId {
        SedimentreeId::new([n; 32])
    }

    fn test_digest(n: u8) -> Digest<Blob> {
        Digest::from_bytes([n; 32])
    }

    #[test]
    fn insert_and_remove() {
        let mut pending = PendingBlobRequests::new(100);

        let id = test_id(1);
        let digest = test_digest(1);

        assert!(!pending.contains(id, digest));
        pending.insert(id, digest);
        assert!(pending.contains(id, digest));
        assert!(pending.remove(id, digest));
        assert!(!pending.contains(id, digest));
        assert!(!pending.remove(id, digest)); // Already removed
    }

    #[test]
    fn lru_eviction() {
        let mut pending = PendingBlobRequests::new(3);

        let id = test_id(1);

        // Fill to capacity
        pending.insert(id, test_digest(1));
        pending.insert(id, test_digest(2));
        pending.insert(id, test_digest(3));
        assert_eq!(pending.len(), 3);

        // Insert one more — should evict oldest (digest 1)
        pending.insert(id, test_digest(4));
        assert_eq!(pending.len(), 3);
        assert!(!pending.contains(id, test_digest(1))); // Evicted
        assert!(pending.contains(id, test_digest(2)));
        assert!(pending.contains(id, test_digest(3)));
        assert!(pending.contains(id, test_digest(4)));
    }

    #[test]
    fn reinsert_moves_to_back() {
        let mut pending = PendingBlobRequests::new(3);

        let id = test_id(1);

        pending.insert(id, test_digest(1));
        pending.insert(id, test_digest(2));
        pending.insert(id, test_digest(3));

        // Re-insert digest 1 — moves to back
        pending.insert(id, test_digest(1));
        assert_eq!(pending.len(), 3);

        // Insert new — should evict digest 2 (now oldest)
        pending.insert(id, test_digest(4));
        assert_eq!(pending.len(), 3);
        assert!(pending.contains(id, test_digest(1))); // Moved to back, not evicted
        assert!(!pending.contains(id, test_digest(2))); // Evicted
        assert!(pending.contains(id, test_digest(3)));
        assert!(pending.contains(id, test_digest(4)));
    }

    #[test]
    fn remove_for_sedimentree() {
        let mut pending = PendingBlobRequests::new(100);

        let tree_a = test_id(1);
        let tree_b = test_id(2);

        pending.insert(tree_a, test_digest(1));
        pending.insert(tree_a, test_digest(2));
        pending.insert(tree_b, test_digest(3));
        pending.insert(tree_b, test_digest(4));
        assert_eq!(pending.len(), 4);

        // Remove all for tree A
        pending.remove_for_sedimentree(tree_a);
        assert_eq!(pending.len(), 2);
        assert!(!pending.contains(tree_a, test_digest(1)));
        assert!(!pending.contains(tree_a, test_digest(2)));
        assert!(pending.contains(tree_b, test_digest(3)));
        assert!(pending.contains(tree_b, test_digest(4)));
    }
}
