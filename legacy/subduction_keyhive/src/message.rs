//! Message types for the keyhive sync protocol.

use alloc::{collections::BTreeMap, sync::Arc, vec::Vec};

use crate::peer_id::KeyhivePeerId;

/// A hash of a keyhive event/operation.
///
/// Events are identified by their 32-byte BLAKE3 hash.
pub type EventHash = [u8; 32];

/// Bincode-serialized `StaticEvent<T>`.
pub type EventBytes = Vec<u8>;

/// Hash-keyed map of serialized events for a peer or peer pair.
///
/// Values are `Arc`-shared bincode event bytes: the periodic cache stores one
/// copy of each event and hands every peer-pair response an `Arc` clone, so
/// cloning a whole response is a set of refcount bumps, not a byte-for-byte
/// copy of every event.
pub type AgentHashMap = BTreeMap<EventHash, Arc<[u8]>>;

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
        #[cfg_attr(
            feature = "serde",
            serde(with = "crate::serde_compat::vec_byte_array_32")
        )]
        found: Vec<EventHash>,

        /// Operation hashes that are pending (awaiting dependencies).
        ///
        /// These might become relevant once dependencies are resolved.
        #[cfg_attr(
            feature = "serde",
            serde(with = "crate::serde_compat::vec_byte_array_32")
        )]
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
        #[cfg_attr(
            feature = "serde",
            serde(with = "crate::serde_compat::vec_byte_array_32")
        )]
        requested: Vec<EventHash>,

        /// Serialized operations to send to the initiator.
        ///
        /// These are operations we have that the initiator is missing.
        #[cfg_attr(feature = "serde", serde(with = "crate::serde_compat::vec_byte_buf"))]
        found: Vec<Arc<[u8]>>,

        /// Total operation count for the responder (intersection + pending).
        ///
        /// Used by the sync check/confirmation shortcut protocol.
        sync_responder_total: u64,

        /// Total operation count for the requester (found + pending from request).
        ///
        /// Used by the sync check/confirmation shortcut protocol.
        sync_requester_total: u64,
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
        #[cfg_attr(feature = "serde", serde(with = "crate::serde_compat::vec_byte_buf"))]
        ops: Vec<Arc<[u8]>>,

        /// Total operation count for the responder (from the sync response).
        ///
        /// Passed through from the sync response for confirmation.
        sync_responder_total: u64,

        /// Total operation count for the requester (from the sync response).
        ///
        /// Passed through from the sync response for confirmation.
        sync_requester_total: u64,
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

    /// Lightweight sync check.
    ///
    /// Sent instead of a full `SyncRequest` when the protocol has an
    /// established syncpoint for the target. Carries the sender's total
    /// operation count and its syncpoint (the last confirmed total for the
    /// target). If both sides' counts match their respective syncpoints,
    /// no full sync is needed.
    SyncCheck {
        /// The peer ID of the sender.
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        target_id: KeyhivePeerId,

        /// The sender's total operation count for this peer pair.
        sender_total: u64,

        /// The sender's syncpoint for the target (last confirmed target total).
        sender_syncpoint: u64,

        /// Order-independent XOR digest of the sender's per-pair op-hash set.
        #[cfg_attr(feature = "serde", serde(with = "serde_bytes", default))]
        sender_digest: [u8; 32],
    },

    /// Sync confirmation.
    ///
    /// Sent at the end of a sync exchange to establish a syncpoint.
    /// The confirmer reports its own total so the remote peer can store
    /// it as a syncpoint for future sync checks.
    SyncConfirmation {
        /// The peer ID of the sender (confirmer).
        sender_id: KeyhivePeerId,

        /// The peer ID of the target.
        target_id: KeyhivePeerId,

        /// The confirmer's total operation count for this peer pair.
        confirmer_total: u64,
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
            | Message::MissingContactCard { sender_id, .. }
            | Message::SyncCheck { sender_id, .. }
            | Message::SyncConfirmation { sender_id, .. } => sender_id,
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
            | Message::MissingContactCard { target_id, .. }
            | Message::SyncCheck { target_id, .. }
            | Message::SyncConfirmation { target_id, .. } => target_id,
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
            Message::SyncCheck { .. } => "SyncCheck",
            Message::SyncConfirmation { .. } => "SyncConfirmation",
        }
    }
}

#[cfg(all(test, feature = "serde", feature = "std"))]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod wire_format_tests {
    //! Verify the on-wire CBOR shape: every binary field must serialize
    //! as a CBOR byte string (major type 2), never as an array of
    //! integers (major type 4). `keyhive_wasm` expects byte
    //! strings. If these tests fail the cross-stack pipe is broken.

    use super::*;
    use alloc::vec;

    fn cbor(msg: &Message) -> ciborium::Value {
        let mut buf = Vec::new();
        ciborium::into_writer(msg, &mut buf).expect("encode");
        ciborium::de::from_reader(buf.as_slice()).expect("decode as raw value")
    }

    fn variant_payload(msg: &Message, variant: &str) -> ciborium::Value {
        // Serialized enum is a single-key map: { "<Variant>": { ... } }.
        let raw = cbor(msg);
        let map = raw.as_map().expect("enum encoded as map");
        let (_, payload) = map
            .iter()
            .find(|(k, _)| k.as_text() == Some(variant))
            .expect("variant key");
        payload.clone()
    }

    fn field<'a>(payload: &'a ciborium::Value, key: &str) -> &'a ciborium::Value {
        let map = payload.as_map().expect("payload map");
        let (_, v) = map
            .iter()
            .find(|(k, _)| k.as_text() == Some(key))
            .expect("field key");
        v
    }

    const fn peer(byte: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([byte; 32])
    }

    fn assert_peer_id_uses_byte_string(payload: &ciborium::Value, key: &str) {
        let pid = field(payload, key);
        let pid_map = pid.as_map().expect("peer id map");
        let (_, vk) = pid_map
            .iter()
            .find(|(k, _)| k.as_text() == Some("verifying_key"))
            .expect("verifying_key field");
        assert!(
            vk.is_bytes(),
            "{key}.verifying_key must be CBOR bytes, got {vk:?}"
        );
    }

    #[test]
    fn sync_request_event_hashes_are_byte_strings() {
        let msg = Message::SyncRequest {
            sender_id: peer(1),
            target_id: peer(2),
            found: vec![[3u8; 32], [4u8; 32]],
            pending: vec![[5u8; 32]],
        };
        let payload = variant_payload(&msg, "SyncRequest");
        assert_peer_id_uses_byte_string(&payload, "sender_id");
        assert_peer_id_uses_byte_string(&payload, "target_id");
        for (label, key) in [("found", "found"), ("pending", "pending")] {
            let arr = field(&payload, key).as_array().expect("array");
            assert!(
                arr.iter().all(ciborium::Value::is_bytes),
                "{label} elements must be CBOR bytes"
            );
        }
    }

    #[test]
    fn sync_response_event_hashes_and_event_bytes_are_byte_strings() {
        let msg = Message::SyncResponse {
            sender_id: peer(1),
            target_id: peer(2),
            requested: vec![[3u8; 32]],
            found: vec![vec![10, 11, 12].into(), vec![20, 21].into()],
            sync_responder_total: 5,
            sync_requester_total: 4,
        };
        let payload = variant_payload(&msg, "SyncResponse");
        let requested = field(&payload, "requested").as_array().expect("array");
        assert!(requested.iter().all(ciborium::Value::is_bytes));
        let found = field(&payload, "found").as_array().expect("array");
        assert!(
            found.iter().all(ciborium::Value::is_bytes),
            "found event-bytes elements must be CBOR bytes"
        );
    }

    #[test]
    fn sync_ops_event_bytes_are_byte_strings() {
        let msg = Message::SyncOps {
            sender_id: peer(1),
            target_id: peer(2),
            ops: vec![vec![1, 2, 3].into(), vec![4, 5].into()],
            sync_responder_total: 1,
            sync_requester_total: 1,
        };
        let payload = variant_payload(&msg, "SyncOps");
        let ops = field(&payload, "ops").as_array().expect("array");
        assert!(
            ops.iter().all(ciborium::Value::is_bytes),
            "ops elements must be CBOR bytes"
        );
    }

    #[test]
    fn round_trip_through_cbor() {
        let original = Message::SyncRequest {
            sender_id: peer(1),
            target_id: peer(2),
            found: vec![[7u8; 32], [9u8; 32]],
            pending: vec![[11u8; 32]],
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&original, &mut buf).expect("encode");
        let recovered: Message = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(original, recovered);
    }

    /// `SyncResponse.found` byte payloads must survive a CBOR round-trip with
    /// their content intact.
    #[test]
    fn sync_response_round_trips_found_bytes() {
        let original = Message::SyncResponse {
            sender_id: peer(1),
            target_id: peer(2),
            requested: vec![[7u8; 32]],
            found: vec![
                vec![0xDE, 0xAD, 0xBE, 0xEF].into(),
                Vec::new().into(),
                vec![1, 2, 3, 4, 5].into(),
            ],
            sync_responder_total: 9,
            sync_requester_total: 4,
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&original, &mut buf).expect("encode");
        let recovered: Message = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(original, recovered);
    }

    /// `SyncOps.ops` byte payloads must survive a CBOR round-trip intact.
    #[test]
    fn sync_ops_round_trips_op_bytes() {
        let original = Message::SyncOps {
            sender_id: peer(1),
            target_id: peer(2),
            ops: vec![
                vec![1, 2, 3].into(),
                vec![4, 5, 6, 7].into(),
                Vec::new().into(),
            ],
            sync_responder_total: 2,
            sync_requester_total: 3,
        };
        let mut buf = Vec::new();
        ciborium::into_writer(&original, &mut buf).expect("encode");
        let recovered: Message = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(original, recovered);
    }

    /// `SyncCheck.sender_digest` must serialize as a CBOR byte string (not an
    /// array of integers) and survive a round-trip.
    #[test]
    fn sync_check_digest_is_byte_string_and_round_trips() {
        let original = Message::SyncCheck {
            sender_id: peer(1),
            target_id: peer(2),
            sender_total: 35,
            sender_syncpoint: 28,
            sender_digest: [
                0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
                0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                0x19, 0x1A, 0x1B, 0x1C,
            ],
        };
        let payload = variant_payload(&original, "SyncCheck");
        assert!(
            field(&payload, "sender_digest").is_bytes(),
            "sender_digest must be CBOR bytes"
        );
        let mut buf = Vec::new();
        ciborium::into_writer(&original, &mut buf).expect("encode");
        let recovered: Message = ciborium::de::from_reader(buf.as_slice()).expect("decode");
        assert_eq!(original, recovered);
    }
}
