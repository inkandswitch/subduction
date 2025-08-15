use crate::peer::id::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Message {
    pub action: String, // FIXME enum
    pub sender_id: PeerId,
    pub target_id: PeerId,
}
