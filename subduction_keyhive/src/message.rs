//! Message types for the keyhive sync protocol.

use alloc::vec::Vec;

use crate::peer_id::KeyhivePeerId;

/// A hash of a keyhive event/operation.
///
/// Events are identified by their 32-byte BLAKE3 hash.
pub type EventHash = [u8; 32];

/// Serialized event bytes.
pub type EventBytes = Vec<u8>;

/// The keyhive sync protocol messages.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    /// Request to initiate keyhive op synchronization.
    ///
    /// The initiator sends their local operation hashes (ops they have for the
    /// target peer) and any pending operation hashes (ops awaiting dependencies).
    SyncRequest {
        /// The peer ID of the sender (initiator).
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (responder).
        target_id: KeyhivePeerId,

        /// Operation hashes that the initiator has for the target peer.
        ///
        /// These are the intersection of operations accessible to both peers.
        found: Vec<EventHash>,

        /// Operation hashes that are pending (awaiting dependencies).
        ///
        /// These might become relevant once dependencies are resolved.
        pending: Vec<EventHash>,
    },

    /// Response to a sync request.
    ///
    /// The responder calculates which ops to send (ops the initiator is missing)
    /// and which ops to request (ops the responder is missing).
    SyncResponse {
        /// The peer ID of the sender (responder).
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (original initiator).
        target_id: KeyhivePeerId,

        /// Hashes of operations we want to request from the initiator.
        ///
        /// These are operations the initiator has that we don't.
        requested: Vec<EventHash>,

        /// Serialized operations to send to the initiator.
        ///
        /// These are operations we have that the initiator is missing.
        found: Vec<EventBytes>,
    },

    /// Send requested operations.
    ///
    /// The final message in the sync protocol where the initiator sends
    /// the operations that were requested in the sync response.
    SyncOps {
        /// The peer ID of the sender (original initiator).
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        target_id: KeyhivePeerId,

        /// The serialized operations being sent.
        ops: Vec<EventBytes>,
    },

    /// Request a peer's contact card.
    ///
    /// Sent when we need to sync with a peer but don't have their contact card
    /// (and thus can't determine which operations are relevant for them).
    RequestContactCard {
        /// The peer ID of the sender.
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (whose contact card we need).
        target_id: KeyhivePeerId,
    },

    /// Send a contact card that was requested.
    ///
    /// Response to `RequestContactCard`. After receiving this, the peer
    /// will initiate a new sync request.
    MissingContactCard {
        /// The peer ID of the sender.
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        target_id: KeyhivePeerId,
    },
}

impl Message {
    /// Get the sender ID for this message.
    #[must_use]
    pub const fn sender_id(&self) -> &KeyhivePeerId {
        match self {
            SyncMessage::SyncRequest { sender_id, .. }
            | SyncMessage::SyncResponse { sender_id, .. }
            | SyncMessage::SyncOps { sender_id, .. }
            | SyncMessage::RequestContactCard { sender_id, .. }
            | SyncMessage::MissingContactCard { sender_id, .. } => sender_id,
        }
    }

    /// Get the target ID for this message.
    #[must_use]
    pub const fn target_id(&self) -> &KeyhivePeerId {
        match self {
            SyncMessage::SyncRequest { target_id, .. }
            | SyncMessage::SyncResponse { target_id, .. }
            | SyncMessage::SyncOps { target_id, .. }
            | SyncMessage::RequestContactCard { target_id, .. }
            | SyncMessage::MissingContactCard { target_id, .. } => target_id,
        }
    }

    /// Get the variant name of this message for logging purposes.
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            SyncMessage::SyncRequest { .. } => "SyncRequest",
            SyncMessage::SyncResponse { .. } => "SyncResponse",
            SyncMessage::SyncOps { .. } => "SyncOps",
            SyncMessage::RequestContactCard { .. } => "RequestContactCard",
            SyncMessage::MissingContactCard { .. } => "MissingContactCard",
        }
    }
}
