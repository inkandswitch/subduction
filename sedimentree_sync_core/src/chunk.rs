use crate::storage::key::StorageKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Chunk {
    pub key: StorageKey,
    pub data: Vec<u8>,
}
