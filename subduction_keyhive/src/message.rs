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
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    /// Request to initiate keyhive op synchronization.
    ///
    /// The initiator sends their local operation hashes (ops they have for the
    /// target peer) and any pending operation hashes (ops awaiting dependencies).
    #[n(0)]
    SyncRequest {
        /// The peer ID of the sender (initiator).
        #[n(0)]
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (responder).
        #[n(1)]
        target_id: KeyhivePeerId,

        /// Operation hashes that the initiator has for the target peer.
        ///
        /// These are the intersection of operations accessible to both peers.
        #[n(2)]
        found: Vec<EventHash>,

        /// Operation hashes that are pending (awaiting dependencies).
        ///
        /// These might become relevant once dependencies are resolved.
        #[n(3)]
        pending: Vec<EventHash>,
    },

    /// Response to a sync request.
    ///
    /// The responder calculates which ops to send (ops the initiator is missing)
    /// and which ops to request (ops the responder is missing).
    #[n(1)]
    SyncResponse {
        /// The peer ID of the sender (responder).
        #[n(0)]
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (original initiator).
        #[n(1)]
        target_id: KeyhivePeerId,

        /// Hashes of operations we want to request from the initiator.
        ///
        /// These are operations the initiator has that we don't.
        #[n(2)]
        requested: Vec<EventHash>,

        /// Serialized operations to send to the initiator.
        ///
        /// These are operations we have that the initiator is missing.
        #[n(3)]
        found: Vec<EventBytes>,
    },

    /// Send requested operations.
    ///
    /// The final message in the sync protocol where the initiator sends
    /// the operations that were requested in the sync response.
    #[n(2)]
    SyncOps {
        /// The peer ID of the sender (original initiator).
        #[n(0)]
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        #[n(1)]
        target_id: KeyhivePeerId,

        /// The serialized operations being sent.
        #[n(2)]
        ops: Vec<EventBytes>,
    },

    /// Request a peer's contact card.
    ///
    /// Sent when we need to sync with a peer but don't have their contact card
    /// (and thus can't determine which operations are relevant for them).
    #[n(3)]
    RequestContactCard {
        /// The peer ID of the sender.
        #[n(0)]
        sender_id: KeyhivePeerId,

        /// The peer ID of the target (whose contact card we need).
        #[n(1)]
        target_id: KeyhivePeerId,
    },

    /// Send a contact card that was requested.
    ///
    /// Response to `RequestContactCard`. After receiving this, the peer
    /// will initiate a new sync request.
    #[n(4)]
    MissingContactCard {
        /// The peer ID of the sender.
        #[n(0)]
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        #[n(1)]
        target_id: KeyhivePeerId,
    },
}

impl Message {
    /// Get the sender ID for this message.
    #[must_use]
    pub const fn sender_id(&self) -> &KeyhivePeerId {
        match self {
            Message::SyncRequest { sender_id, .. }
            | Message::SyncResponse { sender_id, .. }
            | Message::SyncOps { sender_id, .. }
            | Message::RequestContactCard { sender_id, .. }
            | Message::MissingContactCard { sender_id, .. } => sender_id,
        }
    }

    /// Get the target ID for this message.
    #[must_use]
    pub const fn target_id(&self) -> &KeyhivePeerId {
        match self {
            Message::SyncRequest { target_id, .. }
            | Message::SyncResponse { target_id, .. }
            | Message::SyncOps { target_id, .. }
            | Message::RequestContactCard { target_id, .. }
            | Message::MissingContactCard { target_id, .. } => target_id,
        }
    }

    /// Get the variant name of this message for logging purposes.
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Message::SyncRequest { .. } => "SyncRequest",
            Message::SyncResponse { .. } => "SyncResponse",
            Message::SyncOps { .. } => "SyncOps",
            Message::RequestContactCard { .. } => "RequestContactCard",
            Message::MissingContactCard { .. } => "MissingContactCard",
        }
    }
}
