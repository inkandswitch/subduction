//! More detail about a peer.

use crate::storage::id::StorageId;
use serde::{Deserialize, Serialize};

/// Metadata about a peer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerMetadata {
    /// An optional storage ID for the peer.
    pub storage_id: Option<StorageId>, // FIXME what for?

    /// Whether the peer is ephemeral.
    pub is_ephemeral: Option<bool>,
}
