use crate::storage::id::StorageId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerMetadata {
    pub storage_id: Option<StorageId>,
    pub is_ephemeral: Option<bool>,
}
