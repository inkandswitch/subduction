//! The 96-byte composite key `tree_id ++ item_id ++ content_digest` and the
//! key ranges used to scan a whole tree or a single item identity.
//!
//! Keys sort lexicographically, so every item of a tree (or of one
//! commit/fragment identity) is contiguous and a bulk load is a single
//! B+tree range scan. The trailing content digest is what lets multiple
//! payloads share an item id (Byzantine equivocation) without colliding.

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

/// A 96-byte composite key: `tree_id ++ item_id ++ content_digest`.
pub(crate) type Key96 = [u8; 96];

/// Build the composite key for one stored item.
pub(crate) fn key96(tree: SedimentreeId, item: CommitId, digest: &[u8; 32]) -> Key96 {
    let mut key = [0u8; 96];
    key[..32].copy_from_slice(tree.as_bytes());
    key[32..64].copy_from_slice(item.as_bytes());
    key[64..].copy_from_slice(digest);
    key
}

/// Inclusive key range covering every item under a 32-byte tree prefix.
pub(crate) fn tree_range(tree: SedimentreeId) -> (Key96, Key96) {
    let mut lo = [0u8; 96];
    let mut hi = [0xffu8; 96];
    lo[..32].copy_from_slice(tree.as_bytes());
    hi[..32].copy_from_slice(tree.as_bytes());
    (lo, hi)
}

/// Inclusive key range covering every item under a 64-byte (tree, id) prefix.
pub(crate) fn item_range(tree: SedimentreeId, item: CommitId) -> (Key96, Key96) {
    let (mut lo, mut hi) = tree_range(tree);
    lo[32..64].copy_from_slice(item.as_bytes());
    hi[32..64].copy_from_slice(item.as_bytes());
    (lo, hi)
}

/// Extract the item id (bytes 32..64) from a composite key.
pub(crate) const fn item_id_of(key: &Key96) -> CommitId {
    let (_, rest) = key.split_at(32);
    let (item_id, _) = rest.split_at(32);
    let mut id = [0u8; 32];
    id.copy_from_slice(item_id);
    CommitId::new(id)
}
