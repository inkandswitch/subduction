//! Message types for the keyhive sync protocol.

use alloc::{collections::BTreeMap, sync::Arc, vec::Vec};

use sedimentree_core::codec::{
    encode::Encode,
    error::{DecodeError, InvalidEnumTag},
};

use crate::{
    codec::{self, Reader},
    error::WireDecodeError,
    peer_id::KeyhivePeerId,
};

/// A hash of a keyhive event/operation.
///
/// Events are identified by their 32-byte BLAKE3 hash.
pub type EventHash = [u8; 32];

/// A keyhive event in keyhive's canonical encoding.
pub type EventBytes = Vec<u8>;

/// Hash-keyed map of serialized events for a peer or peer pair.
///
/// Values are `Arc`-shared encoded event bytes: the periodic cache stores one
/// copy of each event and hands every peer-pair response an `Arc` clone, so
/// cloning a whole response is a set of refcount bumps, not a byte-for-byte
/// copy of every event.
pub type AgentHashMap = BTreeMap<EventHash, Arc<[u8]>>;

/// The keyhive sync protocol messages.
///
/// # Wire Format
///
/// A message is the payload a [`SignedMessage`](crate::SignedMessage)
/// signs. Every variant starts with the same three fields, then its own, in
/// declaration order:
///
/// ```text
/// ╔═════╦═══════════╦═══════════╦═══════════════════════╗
/// ║ Tag ║ Sender ID ║ Target ID ║ Variant-Specific      ║
/// ║ 1B  ║ peer ID   ║ peer ID   ║ Fields                ║
/// ╚═════╩═══════════╩═══════════╩═══════════════════════╝
/// ```
///
/// Totals are big-endian `u64`s and the digest is 32 bytes. A hash list is
/// a big-endian `u32` count and 32 bytes per hash. An event list is a `u32`
/// count and, per event, a bijou64 length and the event's bytes. Lists keep
/// the order they were built in.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

/// The wire tag of each [`Message`] variant.
mod tags {
    pub(super) const SYNC_REQUEST: u8 = 0;
    pub(super) const SYNC_RESPONSE: u8 = 1;
    pub(super) const SYNC_OPS: u8 = 2;
    pub(super) const REQUEST_CONTACT_CARD: u8 = 3;
    pub(super) const MISSING_CONTACT_CARD: u8 = 4;
    pub(super) const SYNC_CHECK: u8 = 5;
    pub(super) const SYNC_CONFIRMATION: u8 = 6;
}

impl Message {
    /// Decode a message from its wire encoding (see [`Message`]).
    ///
    /// # Errors
    ///
    /// Returns [`WireDecodeError`] if `buf` is not exactly one encoded
    /// message.
    pub fn try_decode(buf: &[u8]) -> Result<Self, WireDecodeError> {
        let mut reader = Reader::new(buf);
        let tag = reader.u8()?;
        let sender_id = KeyhivePeerId::decode_from(&mut reader)?;
        let target_id = KeyhivePeerId::decode_from(&mut reader)?;
        let message = match tag {
            tags::SYNC_REQUEST => Self::SyncRequest {
                sender_id,
                target_id,
                found: read_hashes(&mut reader, "found")?,
                pending: read_hashes(&mut reader, "pending")?,
            },
            tags::SYNC_RESPONSE => Self::SyncResponse {
                sender_id,
                target_id,
                requested: read_hashes(&mut reader, "requested")?,
                found: read_events(&mut reader, "found")?,
                sync_responder_total: reader.u64()?,
                sync_requester_total: reader.u64()?,
            },
            tags::SYNC_OPS => Self::SyncOps {
                sender_id,
                target_id,
                ops: read_events(&mut reader, "ops")?,
                sync_responder_total: reader.u64()?,
                sync_requester_total: reader.u64()?,
            },
            tags::REQUEST_CONTACT_CARD => Self::RequestContactCard {
                sender_id,
                target_id,
            },
            tags::MISSING_CONTACT_CARD => Self::MissingContactCard {
                sender_id,
                target_id,
            },
            tags::SYNC_CHECK => Self::SyncCheck {
                sender_id,
                target_id,
                sender_total: reader.u64()?,
                sender_syncpoint: reader.u64()?,
                sender_digest: reader.array::<32>()?,
            },
            tags::SYNC_CONFIRMATION => Self::SyncConfirmation {
                sender_id,
                target_id,
                confirmer_total: reader.u64()?,
            },
            tag => {
                return Err(DecodeError::from(InvalidEnumTag {
                    tag,
                    type_name: "Message",
                })
                .into());
            }
        };
        reader.finish()?;
        Ok(message)
    }

    const fn tag(&self) -> u8 {
        match self {
            Self::SyncRequest { .. } => tags::SYNC_REQUEST,
            Self::SyncResponse { .. } => tags::SYNC_RESPONSE,
            Self::SyncOps { .. } => tags::SYNC_OPS,
            Self::RequestContactCard { .. } => tags::REQUEST_CONTACT_CARD,
            Self::MissingContactCard { .. } => tags::MISSING_CONTACT_CARD,
            Self::SyncCheck { .. } => tags::SYNC_CHECK,
            Self::SyncConfirmation { .. } => tags::SYNC_CONFIRMATION,
        }
    }

    /// The size of the fields after the sender and target IDs.
    fn fields_size(&self) -> usize {
        match self {
            Self::SyncRequest { found, pending, .. } => hashes_size(found) + hashes_size(pending),
            Self::SyncResponse {
                requested, found, ..
            } => hashes_size(requested) + events_size(found) + 8 + 8,
            Self::SyncOps { ops, .. } => events_size(ops) + 8 + 8,
            Self::RequestContactCard { .. } | Self::MissingContactCard { .. } => 0,
            Self::SyncCheck { .. } => 8 + 8 + 32,
            Self::SyncConfirmation { .. } => 8,
        }
    }
}

impl Encode for Message {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.push(self.tag());
        self.sender_id().encode_into(&mut buf);
        self.target_id().encode_into(&mut buf);
        match self {
            Self::SyncRequest { found, pending, .. } => {
                put_hashes(found, &mut buf);
                put_hashes(pending, &mut buf);
            }
            Self::SyncResponse {
                requested,
                found,
                sync_responder_total,
                sync_requester_total,
                ..
            } => {
                put_hashes(requested, &mut buf);
                put_events(found, &mut buf);
                buf.extend_from_slice(&sync_responder_total.to_be_bytes());
                buf.extend_from_slice(&sync_requester_total.to_be_bytes());
            }
            Self::SyncOps {
                ops,
                sync_responder_total,
                sync_requester_total,
                ..
            } => {
                put_events(ops, &mut buf);
                buf.extend_from_slice(&sync_responder_total.to_be_bytes());
                buf.extend_from_slice(&sync_requester_total.to_be_bytes());
            }
            Self::RequestContactCard { .. } | Self::MissingContactCard { .. } => {}
            Self::SyncCheck {
                sender_total,
                sender_syncpoint,
                sender_digest,
                ..
            } => {
                buf.extend_from_slice(&sender_total.to_be_bytes());
                buf.extend_from_slice(&sender_syncpoint.to_be_bytes());
                buf.extend_from_slice(sender_digest);
            }
            Self::SyncConfirmation {
                confirmer_total, ..
            } => buf.extend_from_slice(&confirmer_total.to_be_bytes()),
        }
        buf
    }

    fn encoded_size(&self) -> usize {
        1 + self.sender_id().encoded_size() + self.target_id().encoded_size() + self.fields_size()
    }
}

fn put_hashes(hashes: &[EventHash], buf: &mut Vec<u8>) {
    codec::put_count(hashes.len(), buf);
    for hash in hashes {
        buf.extend_from_slice(hash);
    }
}

const fn hashes_size(hashes: &[EventHash]) -> usize {
    4 + hashes.len() * 32
}

fn read_hashes(
    reader: &mut Reader<'_>,
    field: &'static str,
) -> Result<Vec<EventHash>, DecodeError> {
    let count = reader.count(field, 32)?;
    (0..count).map(|_| reader.array::<32>()).collect()
}

fn put_events(events: &[Arc<[u8]>], buf: &mut Vec<u8>) {
    codec::put_count(events.len(), buf);
    for event in events {
        codec::put_bytes(event, buf);
    }
}

fn events_size(events: &[Arc<[u8]>]) -> usize {
    4 + events
        .iter()
        .map(|event| codec::bytes_size(event))
        .sum::<usize>()
}

fn read_events(
    reader: &mut Reader<'_>,
    field: &'static str,
) -> Result<Vec<Arc<[u8]>>, DecodeError> {
    let count = reader.count(field, 1)?;
    (0..count).map(|_| reader.bytes().map(Arc::from)).collect()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use alloc::{string::ToString, vec};

    use testresult::TestResult;

    use super::*;

    const fn peer(byte: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([byte; 32])
    }

    /// One message of each variant.
    fn every_variant() -> Vec<Message> {
        vec![
            Message::SyncRequest {
                sender_id: peer(1),
                target_id: KeyhivePeerId::from_bytes_with_suffix([2; 32], "relay".to_string()),
                found: vec![[3; 32], [4; 32]],
                pending: vec![[5; 32]],
            },
            Message::SyncResponse {
                sender_id: peer(1),
                target_id: peer(2),
                requested: vec![[3; 32]],
                found: vec![vec![10, 11, 12].into(), Vec::new().into()],
                sync_responder_total: 5,
                sync_requester_total: u64::MAX,
            },
            Message::SyncOps {
                sender_id: peer(1),
                target_id: peer(2),
                ops: vec![vec![1, 2, 3].into()],
                sync_responder_total: 1,
                sync_requester_total: 2,
            },
            Message::RequestContactCard {
                sender_id: peer(1),
                target_id: peer(2),
            },
            Message::MissingContactCard {
                sender_id: peer(1),
                target_id: peer(2),
            },
            Message::SyncCheck {
                sender_id: peer(1),
                target_id: peer(2),
                sender_total: 7,
                sender_syncpoint: 6,
                sender_digest: [9; 32],
            },
            Message::SyncConfirmation {
                sender_id: peer(1),
                target_id: peer(2),
                confirmer_total: 8,
            },
        ]
    }

    #[test]
    fn every_variant_roundtrips_at_its_encoded_size() -> TestResult {
        for message in every_variant() {
            let bytes = message.encode();
            assert_eq!(bytes.len(), message.encoded_size(), "{message:?}");
            assert_eq!(Message::try_decode(&bytes)?, message);
        }
        Ok(())
    }

    #[test]
    fn every_proper_prefix_is_rejected() {
        for message in every_variant() {
            let bytes = message.encode();
            for len in 0..bytes.len() {
                assert!(
                    Message::try_decode(bytes.get(..len).expect("in range")).is_err(),
                    "{} accepted at {len} of {} bytes",
                    message.variant_name(),
                    bytes.len()
                );
            }
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = every_variant().swap_remove(0).encode();
        bytes.push(0);
        assert!(matches!(
            Message::try_decode(&bytes),
            Err(WireDecodeError::Codec(DecodeError::SizeMismatch(_)))
        ));
    }

    #[test]
    fn an_unknown_tag_is_rejected() {
        let mut bytes = every_variant().swap_remove(3).encode();
        bytes[0] = 0xFF;
        assert!(matches!(
            Message::try_decode(&bytes),
            Err(WireDecodeError::Codec(DecodeError::InvalidEnumTag(_)))
        ));
    }

    #[test]
    fn a_count_the_buffer_cannot_hold_is_rejected_before_allocating() {
        let mut bytes = Message::SyncRequest {
            sender_id: peer(1),
            target_id: peer(2),
            found: Vec::new(),
            pending: Vec::new(),
        }
        .encode();
        // Overwrite `found`'s count, which follows the tag and two
        // suffix-less peer IDs.
        let count_at = 1 + 33 + 33;
        bytes[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            Message::try_decode(&bytes),
            Err(WireDecodeError::Codec(DecodeError::ArrayTooLarge(_)))
        ));
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_roundtrip_at_encoded_size() {
        bolero::check!()
            .with_arbitrary::<Message>()
            .for_each(|message| {
                let bytes = message.encode();
                assert_eq!(bytes.len(), message.encoded_size());
                assert_eq!(Message::try_decode(&bytes).as_ref(), Ok(message));
            });
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_decoding_any_bytes_does_not_panic() {
        bolero::check!()
            .with_type::<Vec<u8>>()
            .for_each(|bytes| drop(Message::try_decode(bytes)));
    }
}
