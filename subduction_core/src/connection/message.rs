//! The API contact messages to be sent over a [`Connection`].
//!
//! # Wire Layout
//!
//! ```text
//! ╔════════╦═══════════╦═════╦═════════════════════════════╗
//! ║ Schema ║ TotalSize ║ Tag ║         Payload             ║
//! ║   4B   ║    4B     ║ 1B  ║         variable            ║
//! ╚════════╩═══════════╩═════╩═════════════════════════════╝
//! ```
//!
//! - **Schema**: `SUM\x00` (4 bytes)
//! - **`TotalSize`**: Total message size in bytes (big-endian u32)
//! - **Tag**: Variant discriminant (u8)
//! - **Payload**: Variant-specific data

use alloc::{collections::BTreeSet, vec::Vec};

use sedimentree_core::{
    blob::Blob,
    codec::error::{
        BufferTooShort, DecodeError, InvalidEnumTag, InvalidSchema, ReadingType, SizeMismatch,
    },
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    fragment::{Fragment, id::FragmentId},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::FingerprintSummary,
};
use subduction_crypto::signed::Signed;

use crate::peer::id::PeerId;

/// The API contact messages to be sent over a [`Connection`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Message {
    /// A single loose commit being sent for a particular [`Sedimentree`].
    LooseCommit {
        /// The ID of the [`Sedimentree`] that this commit belongs to.
        id: SedimentreeId,

        /// The signed [`LooseCommit`] being sent.
        commit: Signed<LooseCommit>,

        /// The [`Blob`] containing the commit data.
        blob: Blob,
    },

    /// A single fragment being sent for a particular [`Sedimentree`].
    Fragment {
        /// The ID of the [`Sedimentree`] that this fragment belongs to.
        id: SedimentreeId,

        /// The signed [`Fragment`] being sent.
        fragment: Signed<Fragment>,

        /// The [`Blob`] containing the fragment data.
        blob: Blob,
    },

    /// A request for blobs by their [`Digest`]s within a specific sedimentree.
    BlobsRequest {
        /// The sedimentree to fetch blobs from.
        id: SedimentreeId,
        /// The blob digests being requested.
        digests: Vec<Digest<Blob>>,
    },

    /// A response to a [`BlobsRequest`] with blobs for a specific sedimentree.
    BlobsResponse {
        /// The sedimentree these blobs belong to.
        id: SedimentreeId,
        /// The requested blobs.
        blobs: Vec<Blob>,
    },

    /// A request to "batch sync" an entire [`Sedimentree`].
    BatchSyncRequest(BatchSyncRequest),

    /// A response to a [`BatchSyncRequest`].
    BatchSyncResponse(BatchSyncResponse),

    /// A request to remove subscriptions from specific sedimentrees.
    RemoveSubscriptions(RemoveSubscriptions),

    /// Notification that a data request was rejected due to authorization failure.
    DataRequestRejected(DataRequestRejected),
}

impl Message {
    /// Get the request ID for this message, if any.
    #[must_use]
    pub const fn request_id(&self) -> Option<RequestId> {
        match self {
            Message::BatchSyncRequest(BatchSyncRequest { req_id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { req_id, .. }) => Some(*req_id),
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest { .. }
            | Message::BlobsResponse { .. }
            | Message::RemoveSubscriptions(_)
            | Message::DataRequestRejected(_) => None,
        }
    }

    /// Get the variant name of this message for logging purposes.
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Message::LooseCommit { .. } => "LooseCommit",
            Message::Fragment { .. } => "Fragment",
            Message::BlobsRequest { .. } => "BlobsRequest",
            Message::BlobsResponse { .. } => "BlobsResponse",
            Message::BatchSyncRequest(_) => "BatchSyncRequest",
            Message::BatchSyncResponse(_) => "BatchSyncResponse",
            Message::RemoveSubscriptions(_) => "RemoveSubscriptions",
            Message::DataRequestRejected(_) => "DataRequestRejected",
        }
    }

    /// Get the sedimentree ID associated with this message, if any.
    #[must_use]
    pub const fn sedimentree_id(&self) -> Option<SedimentreeId> {
        match self {
            Message::LooseCommit { id, .. }
            | Message::Fragment { id, .. }
            | Message::BlobsRequest { id, .. }
            | Message::BlobsResponse { id, .. }
            | Message::BatchSyncRequest(BatchSyncRequest { id, .. })
            | Message::BatchSyncResponse(BatchSyncResponse { id, .. })
            | Message::DataRequestRejected(DataRequestRejected { id }) => Some(*id),
            Message::RemoveSubscriptions(_) => None,
        }
    }
}

/// A request to sync a sedimentree in batch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncRequest {
    /// The ID of the sedimentree to sync.
    pub id: SedimentreeId,

    /// The unique ID of the request.
    pub req_id: RequestId,

    /// Compact fingerprint summary of the requester's sedimentree.
    ///
    /// Uses SipHash-2-4 fingerprints with a per-request random seed
    /// instead of full structural data.
    pub fingerprint_summary: FingerprintSummary,

    /// Whether to subscribe to future updates for this sedimentree.
    pub subscribe: bool,
}

impl From<BatchSyncRequest> for Message {
    fn from(req: BatchSyncRequest) -> Self {
        Message::BatchSyncRequest(req)
    }
}

/// A response to a [`BatchSyncRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BatchSyncResponse {
    /// The ID of the request that this is a response to.
    pub req_id: RequestId,

    /// The ID of the sedimentree that was synced.
    pub id: SedimentreeId,

    /// The result of the sync request.
    pub result: SyncResult,
}

/// The result of a batch sync request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncResult {
    /// Sync succeeded.
    Ok(SyncDiff),

    /// Sedimentree not found (peer is authorized but tree doesn't exist).
    NotFound,

    /// Peer is not authorized to access this sedimentree.
    Unauthorized,
}

impl From<BatchSyncResponse> for Message {
    fn from(resp: BatchSyncResponse) -> Self {
        Message::BatchSyncResponse(resp)
    }
}

/// A request to remove subscriptions from specific sedimentrees.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoveSubscriptions {
    /// The IDs of the sedimentrees to unsubscribe from.
    pub ids: Vec<SedimentreeId>,
}

impl From<RemoveSubscriptions> for Message {
    fn from(unsub: RemoveSubscriptions) -> Self {
        Message::RemoveSubscriptions(unsub)
    }
}

/// Sent when a peer's data request cannot be fulfilled due to authorization failure.
///
/// This is informational — the receiver's original sync completed, but their
/// opportunistic `requesting` field could not be fulfilled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DataRequestRejected {
    /// The sedimentree ID that was rejected.
    pub id: SedimentreeId,
}

impl From<DataRequestRejected> for Message {
    fn from(rejection: DataRequestRejected) -> Self {
        Message::DataRequestRejected(rejection)
    }
}

/// A unique identifier for a particular request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestId {
    /// ID for the peer that initiated the request.
    ///
    /// This namespaces nonces so they only need to be unique per-peer rather than globally.
    /// Not redundant with connection-level auth or `Signed<T>` — `RequestId` must be
    /// matchable without accessing the connection, and these messages aren't individually signed.
    pub requestor: PeerId,

    /// A nonce unique to this user and connection.
    pub nonce: u64,
}

/// The calculated difference between two peers.
///
/// Contains both:
/// - Data to send to the requestor (`missing_commits`, `missing_fragments`)
/// - Data the responder is requesting back (`requesting`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncDiff {
    /// Commits the requestor is missing (responder sends these).
    pub missing_commits: Vec<(Signed<LooseCommit>, Blob)>,

    /// Fragments the requestor is missing (responder sends these).
    pub missing_fragments: Vec<(Signed<Fragment>, Blob)>,

    /// Data the responder is requesting from the requestor.
    ///
    /// The requestor should send these commits and fragments back
    /// as individual [`Message::LooseCommit`] and [`Message::Fragment`] messages.
    pub requesting: RequestedData,
}

/// Data that the responder is requesting from the requestor.
///
/// After receiving a [`BatchSyncResponse`], the requestor should send back
/// the commits and fragments identified here (fire-and-forget).
///
/// The fingerprints are echoed back from the requestor's original
/// [`FingerprintSummary`]. The requestor reverse-lookups each fingerprint
/// to find the corresponding local item.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RequestedData {
    /// Fingerprints of commits the responder needs from the requestor.
    pub commit_fingerprints: Vec<Fingerprint<CommitId>>,

    /// Fingerprints of fragments the responder needs from the requestor.
    pub fragment_fingerprints: Vec<Fingerprint<FragmentId>>,
}

impl RequestedData {
    /// Returns `true` if there is no data being requested.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.commit_fingerprints.is_empty() && self.fragment_fingerprints.is_empty()
    }
}

// ============================================================================
// Binary Codec
// ============================================================================

/// Schema header for `Message` envelope.
pub const MESSAGE_SCHEMA: [u8; 4] = *b"SUM\x00";

/// Minimum size of a Message envelope (schema + `total_size` + tag).
const ENVELOPE_HEADER_SIZE: usize = 4 + 4 + 1; // 9 bytes

mod tags {
    pub(super) const LOOSE_COMMIT: u8 = 0x00;
    pub(super) const FRAGMENT: u8 = 0x01;
    pub(super) const BLOBS_REQUEST: u8 = 0x02;
    pub(super) const BLOBS_RESPONSE: u8 = 0x03;
    pub(super) const BATCH_SYNC_REQUEST: u8 = 0x04;
    pub(super) const BATCH_SYNC_RESPONSE: u8 = 0x05;
    pub(super) const REMOVE_SUBSCRIPTIONS: u8 = 0x06;
    pub(super) const DATA_REQUEST_REJECTED: u8 = 0x07;
}

mod min_sizes {
    // sed_id(32) + Signed<LooseCommit>::MIN_SIZE(173) + blob_len_prefix(4)
    pub(super) const LOOSE_COMMIT: usize = 32 + 173 + 4;
    pub(super) const FRAGMENT: usize = 32 + 207 + 4;
    pub(super) const BLOBS_REQUEST: usize = 32 + 2;
    pub(super) const BLOBS_RESPONSE: usize = 32 + 2;
    pub(super) const BATCH_SYNC_REQUEST: usize = 32 + 32 + 8 + 1 + 16 + 2 + 2;
    pub(super) const BATCH_SYNC_RESPONSE: usize = 32 + 8 + 32 + 1;
    pub(super) const REMOVE_SUBSCRIPTIONS: usize = 2;
    pub(super) const DATA_REQUEST_REJECTED: usize = 32;
}

mod result_tags {
    pub(super) const OK: u8 = 0x00;
    pub(super) const NOT_FOUND: u8 = 0x01;
    pub(super) const UNAUTHORIZED: u8 = 0x02;
}

impl Message {
    /// Encode the message to wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload_size = self.payload_size();
        let total_size = ENVELOPE_HEADER_SIZE + payload_size;

        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&MESSAGE_SCHEMA);

        #[allow(clippy::cast_possible_truncation)]
        buf.extend_from_slice(&(total_size as u32).to_be_bytes());

        match self {
            Message::LooseCommit { id, commit, blob } => {
                buf.push(tags::LOOSE_COMMIT);
                encode_loose_commit(&mut buf, id, commit, blob);
            }
            Message::Fragment { id, fragment, blob } => {
                buf.push(tags::FRAGMENT);
                encode_fragment(&mut buf, id, fragment, blob);
            }
            Message::BlobsRequest { id, digests } => {
                buf.push(tags::BLOBS_REQUEST);
                encode_blobs_request(&mut buf, id, digests);
            }
            Message::BlobsResponse { id, blobs } => {
                buf.push(tags::BLOBS_RESPONSE);
                encode_blobs_response(&mut buf, id, blobs);
            }
            Message::BatchSyncRequest(req) => {
                buf.push(tags::BATCH_SYNC_REQUEST);
                encode_batch_sync_request(&mut buf, req);
            }
            Message::BatchSyncResponse(resp) => {
                buf.push(tags::BATCH_SYNC_RESPONSE);
                encode_batch_sync_response(&mut buf, resp);
            }
            Message::RemoveSubscriptions(unsub) => {
                buf.push(tags::REMOVE_SUBSCRIPTIONS);
                encode_remove_subscriptions(&mut buf, unsub);
            }
            Message::DataRequestRejected(rejected) => {
                buf.push(tags::DATA_REQUEST_REJECTED);
                encode_data_request_rejected(&mut buf, rejected);
            }
        }

        buf
    }

    /// Decode a message from wire bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the message is malformed.
    #[allow(clippy::indexing_slicing)] // Length validated before access
    pub fn try_decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < ENVELOPE_HEADER_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "Message envelope",
                need: ENVELOPE_HEADER_SIZE,
                have: bytes.len(),
            });
        }

        let schema: [u8; 4] = bytes.get(0..4).and_then(|s| s.try_into().ok()).ok_or(
            DecodeError::MessageTooShort {
                type_name: "Message schema",
                need: 4,
                have: bytes.len(),
            },
        )?;
        if schema != MESSAGE_SCHEMA {
            return Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into());
        }

        let total_size = u32::from_be_bytes(bytes.get(4..8).and_then(|s| s.try_into().ok()).ok_or(
            DecodeError::MessageTooShort {
                type_name: "Message total_size",
                need: 8,
                have: bytes.len(),
            },
        )?) as usize;
        if bytes.len() != total_size {
            return Err(SizeMismatch {
                declared: total_size,
                actual: bytes.len(),
            }
            .into());
        }

        let tag = *bytes.get(8).ok_or(DecodeError::MessageTooShort {
            type_name: "Message tag",
            need: 9,
            have: bytes.len(),
        })?;
        let payload = bytes
            .get(ENVELOPE_HEADER_SIZE..)
            .ok_or(DecodeError::MessageTooShort {
                type_name: "Message payload",
                need: ENVELOPE_HEADER_SIZE,
                have: bytes.len(),
            })?;

        let (min_payload_size, type_name) = match tag {
            tags::LOOSE_COMMIT => (min_sizes::LOOSE_COMMIT, "LooseCommit"),
            tags::FRAGMENT => (min_sizes::FRAGMENT, "Fragment"),
            tags::BLOBS_REQUEST => (min_sizes::BLOBS_REQUEST, "BlobsRequest"),
            tags::BLOBS_RESPONSE => (min_sizes::BLOBS_RESPONSE, "BlobsResponse"),
            tags::BATCH_SYNC_REQUEST => (min_sizes::BATCH_SYNC_REQUEST, "BatchSyncRequest"),
            tags::BATCH_SYNC_RESPONSE => (min_sizes::BATCH_SYNC_RESPONSE, "BatchSyncResponse"),
            tags::REMOVE_SUBSCRIPTIONS => (min_sizes::REMOVE_SUBSCRIPTIONS, "RemoveSubscriptions"),
            tags::DATA_REQUEST_REJECTED => {
                (min_sizes::DATA_REQUEST_REJECTED, "DataRequestRejected")
            }
            _ => {
                return Err(InvalidEnumTag {
                    tag,
                    type_name: "Message",
                }
                .into());
            }
        };

        if payload.len() < min_payload_size {
            return Err(DecodeError::MessageTooShort {
                type_name,
                need: ENVELOPE_HEADER_SIZE + min_payload_size,
                have: bytes.len(),
            });
        }

        match tag {
            tags::LOOSE_COMMIT => decode_loose_commit(payload),
            tags::FRAGMENT => decode_fragment(payload),
            tags::BLOBS_REQUEST => decode_blobs_request(payload),
            tags::BLOBS_RESPONSE => decode_blobs_response(payload),
            tags::BATCH_SYNC_REQUEST => decode_batch_sync_request(payload),
            tags::BATCH_SYNC_RESPONSE => decode_batch_sync_response(payload),
            tags::REMOVE_SUBSCRIPTIONS => decode_remove_subscriptions(payload),
            tags::DATA_REQUEST_REJECTED => decode_data_request_rejected(payload),
            _ => unreachable!("tag validated above"),
        }
    }

    fn payload_size(&self) -> usize {
        match self {
            Message::LooseCommit { commit, blob, .. } => {
                32 + commit.as_bytes().len() + 4 + blob.as_slice().len()
            }
            Message::Fragment { fragment, blob, .. } => {
                32 + fragment.as_bytes().len() + 4 + blob.as_slice().len()
            }
            Message::BlobsRequest { digests, .. } => 32 + 2 + (digests.len() * 32),
            Message::BlobsResponse { blobs, .. } => {
                32 + 2 + blobs.iter().map(|b| 4 + b.as_slice().len()).sum::<usize>()
            }
            Message::BatchSyncRequest(req) => {
                32 + 32
                    + 8
                    + 1
                    + 16
                    + 2
                    + 2
                    + (req.fingerprint_summary.commit_fingerprints().len() * 8)
                    + (req.fingerprint_summary.fragment_fingerprints().len() * 8)
            }
            Message::BatchSyncResponse(resp) => 32 + 8 + 32 + 1 + sync_result_size(&resp.result),
            Message::RemoveSubscriptions(unsub) => 2 + (unsub.ids.len() * 32),
            Message::DataRequestRejected(_) => 32,
        }
    }
}

fn sync_result_size(result: &SyncResult) -> usize {
    match result {
        SyncResult::Ok(diff) => sync_diff_size(diff),
        SyncResult::NotFound | SyncResult::Unauthorized => 0,
    }
}

fn sync_diff_size(diff: &SyncDiff) -> usize {
    let counts_size = 2 + 2 + 2 + 2;

    let commits_size: usize = diff
        .missing_commits
        .iter()
        .map(|(signed, blob)| signed.as_bytes().len() + 4 + blob.as_slice().len())
        .sum();

    let fragments_size: usize = diff
        .missing_fragments
        .iter()
        .map(|(signed, blob)| signed.as_bytes().len() + 4 + blob.as_slice().len())
        .sum();

    let requested_fps_size = (diff.requesting.commit_fingerprints.len()
        + diff.requesting.fragment_fingerprints.len())
        * 8;

    counts_size + commits_size + fragments_size + requested_fps_size
}

fn encode_loose_commit(
    buf: &mut Vec<u8>,
    id: &SedimentreeId,
    commit: &Signed<LooseCommit>,
    blob: &Blob,
) {
    buf.extend_from_slice(id.as_bytes());
    buf.extend_from_slice(commit.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(blob.as_slice().len() as u32).to_be_bytes());
    buf.extend_from_slice(blob.as_slice());
}

fn encode_fragment(
    buf: &mut Vec<u8>,
    id: &SedimentreeId,
    fragment: &Signed<Fragment>,
    blob: &Blob,
) {
    buf.extend_from_slice(id.as_bytes());
    buf.extend_from_slice(fragment.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(blob.as_slice().len() as u32).to_be_bytes());
    buf.extend_from_slice(blob.as_slice());
}

fn encode_blobs_request(buf: &mut Vec<u8>, id: &SedimentreeId, digests: &[Digest<Blob>]) {
    buf.extend_from_slice(id.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(digests.len() as u16).to_be_bytes());
    for digest in digests {
        buf.extend_from_slice(digest.as_bytes());
    }
}

fn encode_blobs_response(buf: &mut Vec<u8>, id: &SedimentreeId, blobs: &[Blob]) {
    buf.extend_from_slice(id.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(blobs.len() as u16).to_be_bytes());
    for blob in blobs {
        #[allow(clippy::cast_possible_truncation)]
        buf.extend_from_slice(&(blob.as_slice().len() as u32).to_be_bytes());
        buf.extend_from_slice(blob.as_slice());
    }
}

fn encode_batch_sync_request(buf: &mut Vec<u8>, req: &BatchSyncRequest) {
    buf.extend_from_slice(req.id.as_bytes());
    buf.extend_from_slice(req.req_id.requestor.as_bytes());
    buf.extend_from_slice(&req.req_id.nonce.to_be_bytes());
    buf.push(u8::from(req.subscribe));

    let seed = req.fingerprint_summary.seed();
    buf.extend_from_slice(&seed.key0().to_be_bytes());
    buf.extend_from_slice(&seed.key1().to_be_bytes());

    let commit_fps = req.fingerprint_summary.commit_fingerprints();
    let fragment_fps = req.fingerprint_summary.fragment_fingerprints();
    #[allow(clippy::cast_possible_truncation)]
    {
        buf.extend_from_slice(&(commit_fps.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(fragment_fps.len() as u16).to_be_bytes());
    }

    for fp in commit_fps {
        buf.extend_from_slice(&fp.as_u64().to_be_bytes());
    }
    for fp in fragment_fps {
        buf.extend_from_slice(&fp.as_u64().to_be_bytes());
    }
}

fn encode_batch_sync_response(buf: &mut Vec<u8>, resp: &BatchSyncResponse) {
    buf.extend_from_slice(resp.req_id.requestor.as_bytes());
    buf.extend_from_slice(&resp.req_id.nonce.to_be_bytes());
    buf.extend_from_slice(resp.id.as_bytes());

    match &resp.result {
        SyncResult::Ok(diff) => {
            buf.push(result_tags::OK);
            encode_sync_diff(buf, diff);
        }
        SyncResult::NotFound => {
            buf.push(result_tags::NOT_FOUND);
        }
        SyncResult::Unauthorized => {
            buf.push(result_tags::UNAUTHORIZED);
        }
    }
}

fn encode_sync_diff(buf: &mut Vec<u8>, diff: &SyncDiff) {
    #[allow(clippy::cast_possible_truncation)]
    {
        buf.extend_from_slice(&(diff.missing_commits.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(diff.missing_fragments.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(diff.requesting.commit_fingerprints.len() as u16).to_be_bytes());
        buf.extend_from_slice(&(diff.requesting.fragment_fingerprints.len() as u16).to_be_bytes());
    }

    for (signed, blob) in &diff.missing_commits {
        buf.extend_from_slice(signed.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        buf.extend_from_slice(&(blob.as_slice().len() as u32).to_be_bytes());
        buf.extend_from_slice(blob.as_slice());
    }

    for (signed, blob) in &diff.missing_fragments {
        buf.extend_from_slice(signed.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        buf.extend_from_slice(&(blob.as_slice().len() as u32).to_be_bytes());
        buf.extend_from_slice(blob.as_slice());
    }

    for fp in &diff.requesting.commit_fingerprints {
        buf.extend_from_slice(&fp.as_u64().to_be_bytes());
    }
    for fp in &diff.requesting.fragment_fingerprints {
        buf.extend_from_slice(&fp.as_u64().to_be_bytes());
    }
}

fn encode_remove_subscriptions(buf: &mut Vec<u8>, unsub: &RemoveSubscriptions) {
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(unsub.ids.len() as u16).to_be_bytes());
    for id in &unsub.ids {
        buf.extend_from_slice(id.as_bytes());
    }
}

fn encode_data_request_rejected(buf: &mut Vec<u8>, rejected: &DataRequestRejected) {
    buf.extend_from_slice(rejected.id.as_bytes());
}

fn decode_loose_commit(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);

    let commit = Signed::<LooseCommit>::try_decode(
        payload
            .get(offset..)
            .ok_or(BufferTooShort {
                reading: ReadingType::Slice { len: 0 },
                offset,
                need: 1,
                have: 0,
            })?
            .to_vec(),
    )?;
    offset += commit.as_bytes().len();

    let blob_size = read_u32(payload, &mut offset)? as usize;

    let blob = Blob::new(
        payload
            .get(offset..offset + blob_size)
            .ok_or(BufferTooShort {
                reading: ReadingType::Slice { len: blob_size },
                offset,
                need: blob_size,
                have: payload.len().saturating_sub(offset),
            })?
            .to_vec(),
    );

    Ok(Message::LooseCommit { id, commit, blob })
}

fn decode_fragment(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);

    let fragment = Signed::<Fragment>::try_decode(
        payload
            .get(offset..)
            .ok_or(BufferTooShort {
                reading: ReadingType::Slice { len: 0 },
                offset,
                need: 1,
                have: 0,
            })?
            .to_vec(),
    )?;
    offset += fragment.as_bytes().len();

    let blob_size = read_u32(payload, &mut offset)? as usize;

    let blob = Blob::new(
        payload
            .get(offset..offset + blob_size)
            .ok_or(BufferTooShort {
                reading: ReadingType::Slice { len: blob_size },
                offset,
                need: blob_size,
                have: payload.len().saturating_sub(offset),
            })?
            .to_vec(),
    );

    Ok(Message::Fragment { id, fragment, blob })
}

fn decode_blobs_request(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);
    let count = read_u16(payload, &mut offset)? as usize;

    let mut digests = Vec::with_capacity(count);
    for _ in 0..count {
        digests.push(Digest::force_from_bytes(read_array::<32>(
            payload,
            &mut offset,
        )?));
    }

    Ok(Message::BlobsRequest { id, digests })
}

fn decode_blobs_response(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);
    let count = read_u16(payload, &mut offset)? as usize;

    let mut blobs = Vec::with_capacity(count);
    for _ in 0..count {
        let blob_size = read_u32(payload, &mut offset)? as usize;
        blobs.push(Blob::new(
            payload
                .get(offset..offset + blob_size)
                .ok_or(BufferTooShort {
                    reading: ReadingType::Slice { len: blob_size },
                    offset,
                    need: blob_size,
                    have: payload.len().saturating_sub(offset),
                })?
                .to_vec(),
        ));
        offset += blob_size;
    }

    Ok(Message::BlobsResponse { id, blobs })
}

fn decode_batch_sync_request(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);

    let requestor = PeerId::new(read_array::<32>(payload, &mut offset)?);
    let nonce = read_u64(payload, &mut offset)?;
    let req_id = RequestId { requestor, nonce };

    let subscribe_byte = read_u8(payload, &mut offset)?;
    let subscribe = match subscribe_byte {
        0x00 => false,
        0x01 => true,
        _ => {
            return Err(InvalidEnumTag {
                tag: subscribe_byte,
                type_name: "Subscribe",
            }
            .into());
        }
    };

    let key0 = read_u64(payload, &mut offset)?;
    let key1 = read_u64(payload, &mut offset)?;
    let seed = FingerprintSeed::new(key0, key1);

    let commit_count = read_u16(payload, &mut offset)? as usize;
    let fragment_count = read_u16(payload, &mut offset)? as usize;

    let mut commit_fps = BTreeSet::new();
    for _ in 0..commit_count {
        commit_fps.insert(Fingerprint::from_u64(read_u64(payload, &mut offset)?));
    }
    let mut fragment_fps = BTreeSet::new();
    for _ in 0..fragment_count {
        fragment_fps.insert(Fingerprint::from_u64(read_u64(payload, &mut offset)?));
    }

    let fingerprint_summary = FingerprintSummary::new(seed, commit_fps, fragment_fps);

    Ok(Message::BatchSyncRequest(BatchSyncRequest {
        id,
        req_id,
        fingerprint_summary,
        subscribe,
    }))
}

fn decode_batch_sync_response(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let requestor = PeerId::new(read_array::<32>(payload, &mut offset)?);
    let nonce = read_u64(payload, &mut offset)?;
    let req_id = RequestId { requestor, nonce };

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);

    let result_tag = read_u8(payload, &mut offset)?;
    let result = match result_tag {
        result_tags::OK => SyncResult::Ok(decode_sync_diff(payload, &mut offset)?),
        result_tags::NOT_FOUND => SyncResult::NotFound,
        result_tags::UNAUTHORIZED => SyncResult::Unauthorized,
        _ => {
            return Err(InvalidEnumTag {
                tag: result_tag,
                type_name: "SyncResult",
            }
            .into());
        }
    };

    Ok(Message::BatchSyncResponse(BatchSyncResponse {
        req_id,
        id,
        result,
    }))
}

fn decode_sync_diff(payload: &[u8], offset: &mut usize) -> Result<SyncDiff, DecodeError> {
    let commit_count = read_u16(payload, offset)? as usize;
    let fragment_count = read_u16(payload, offset)? as usize;
    let requested_commit_count = read_u16(payload, offset)? as usize;
    let requested_fragment_count = read_u16(payload, offset)? as usize;

    let mut missing_commits = Vec::with_capacity(commit_count);
    for _ in 0..commit_count {
        let commit = Signed::<LooseCommit>::try_decode(
            payload
                .get(*offset..)
                .ok_or(BufferTooShort {
                    reading: ReadingType::Slice { len: 0 },
                    offset: *offset,
                    need: 1,
                    have: 0,
                })?
                .to_vec(),
        )?;
        *offset += commit.as_bytes().len();

        let blob_size = read_u32(payload, offset)? as usize;
        let blob = Blob::new(
            payload
                .get(*offset..*offset + blob_size)
                .ok_or(BufferTooShort {
                    reading: ReadingType::Slice { len: blob_size },
                    offset: *offset,
                    need: blob_size,
                    have: payload.len().saturating_sub(*offset),
                })?
                .to_vec(),
        );
        *offset += blob_size;

        missing_commits.push((commit, blob));
    }

    let mut missing_fragments = Vec::with_capacity(fragment_count);
    for _ in 0..fragment_count {
        let fragment = Signed::<Fragment>::try_decode(
            payload
                .get(*offset..)
                .ok_or(BufferTooShort {
                    reading: ReadingType::Slice { len: 0 },
                    offset: *offset,
                    need: 1,
                    have: 0,
                })?
                .to_vec(),
        )?;
        *offset += fragment.as_bytes().len();

        let blob_size = read_u32(payload, offset)? as usize;
        let blob = Blob::new(
            payload
                .get(*offset..*offset + blob_size)
                .ok_or(BufferTooShort {
                    reading: ReadingType::Slice { len: blob_size },
                    offset: *offset,
                    need: blob_size,
                    have: payload.len().saturating_sub(*offset),
                })?
                .to_vec(),
        );
        *offset += blob_size;

        missing_fragments.push((fragment, blob));
    }

    let mut commit_fingerprints = Vec::with_capacity(requested_commit_count);
    for _ in 0..requested_commit_count {
        commit_fingerprints.push(Fingerprint::from_u64(read_u64(payload, offset)?));
    }
    let mut fragment_fingerprints = Vec::with_capacity(requested_fragment_count);
    for _ in 0..requested_fragment_count {
        fragment_fingerprints.push(Fingerprint::from_u64(read_u64(payload, offset)?));
    }

    Ok(SyncDiff {
        missing_commits,
        missing_fragments,
        requesting: RequestedData {
            commit_fingerprints,
            fragment_fingerprints,
        },
    })
}

fn decode_remove_subscriptions(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let count = read_u16(payload, &mut offset)? as usize;

    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(SedimentreeId::new(read_array::<32>(payload, &mut offset)?));
    }

    Ok(Message::RemoveSubscriptions(RemoveSubscriptions { ids }))
}

fn decode_data_request_rejected(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);

    Ok(Message::DataRequestRejected(DataRequestRejected { id }))
}

fn read_u8(buf: &[u8], offset: &mut usize) -> Result<u8, DecodeError> {
    let val = *buf.get(*offset).ok_or(BufferTooShort {
        reading: ReadingType::U8,
        offset: *offset,
        need: 1,
        have: buf.len().saturating_sub(*offset),
    })?;
    *offset += 1;
    Ok(val)
}

fn read_u16(buf: &[u8], offset: &mut usize) -> Result<u16, DecodeError> {
    let val = u16::from_be_bytes(
        buf.get(*offset..*offset + 2)
            .and_then(|s| s.try_into().ok())
            .ok_or(BufferTooShort {
                reading: ReadingType::U16,
                offset: *offset,
                need: 2,
                have: buf.len().saturating_sub(*offset),
            })?,
    );
    *offset += 2;
    Ok(val)
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32, DecodeError> {
    let val = u32::from_be_bytes(
        buf.get(*offset..*offset + 4)
            .and_then(|s| s.try_into().ok())
            .ok_or(BufferTooShort {
                reading: ReadingType::U32,
                offset: *offset,
                need: 4,
                have: buf.len().saturating_sub(*offset),
            })?,
    );
    *offset += 4;
    Ok(val)
}

fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64, DecodeError> {
    let val = u64::from_be_bytes(
        buf.get(*offset..*offset + 8)
            .and_then(|s| s.try_into().ok())
            .ok_or(BufferTooShort {
                reading: ReadingType::U64,
                offset: *offset,
                need: 8,
                have: buf.len().saturating_sub(*offset),
            })?,
    );
    *offset += 8;
    Ok(val)
}

fn read_array<const N: usize>(buf: &[u8], offset: &mut usize) -> Result<[u8; N], DecodeError> {
    let arr: [u8; N] = buf
        .get(*offset..*offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or(BufferTooShort {
            reading: ReadingType::Array { size: N },
            offset: *offset,
            need: N,
            have: buf.len().saturating_sub(*offset),
        })?;
    *offset += N;
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    mod message_request_id {
        use super::*;
        use future_form::Sendable;
        use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

        fn test_signer() -> MemorySigner {
            MemorySigner::from_bytes(&[42u8; 32])
        }

        #[tokio::test]
        async fn test_loose_commit_has_no_request_id() {
            let signer = test_signer();
            let id = SedimentreeId::new([1u8; 32]);
            let blob = Blob::new(Vec::new());
            let commit = LooseCommit::new(
                id,
                BTreeSet::new(),
                sedimentree_core::blob::BlobMeta::new(&blob),
            );
            let signed_commit = Signed::seal::<Sendable, _>(&signer, commit)
                .await
                .into_signed();
            let msg = Message::LooseCommit {
                id,
                commit: signed_commit,
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[tokio::test]
        async fn test_fragment_has_no_request_id() {
            let signer = test_signer();
            let id = SedimentreeId::new([1u8; 32]);
            let blob = Blob::new(Vec::new());
            let fragment = Fragment::new(
                id,
                Digest::force_from_bytes([2u8; 32]),
                BTreeSet::new(),
                &[],
                sedimentree_core::blob::BlobMeta::new(&blob),
            );
            let signed_fragment = Signed::seal::<Sendable, _>(&signer, fragment)
                .await
                .into_signed();
            let msg = Message::Fragment {
                id,
                fragment: signed_fragment,
                blob: Blob::new(Vec::from([3u8; 16])),
            };
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_blobs_request_has_no_request_id() {
            let msg = Message::BlobsRequest {
                id: SedimentreeId::new([0u8; 32]),
                digests: vec![Digest::force_from_bytes([1u8; 32])],
            };
            assert_eq!(msg.request_id(), None);
        }

        #[test]
        fn test_blobs_response_has_no_request_id() {
            let msg = Message::BlobsResponse {
                id: SedimentreeId::new([0u8; 32]),
                blobs: vec![Blob::new(Vec::from([1u8; 16]))],
            };
            assert_eq!(msg.request_id(), None);
        }
    }

    mod sync_diff {
        use super::*;
        use future_form::Sendable;
        use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};

        fn test_signer() -> MemorySigner {
            MemorySigner::from_bytes(&[42u8; 32])
        }

        #[tokio::test]
        async fn test_sync_diff_with_commits() {
            let signer = test_signer();
            let id = SedimentreeId::new([0u8; 32]);
            let blob = Blob::new(Vec::from([2u8; 16]));
            let commit = LooseCommit::new(
                id,
                BTreeSet::new(),
                sedimentree_core::blob::BlobMeta::new(&blob),
            );
            let signed_commit = Signed::seal::<Sendable, _>(&signer, commit)
                .await
                .into_signed();

            let diff = SyncDiff {
                missing_commits: vec![(signed_commit.clone(), blob.clone())],
                missing_fragments: Vec::new(),
                requesting: RequestedData::default(),
            };

            assert_eq!(diff.missing_commits.len(), 1);

            #[allow(clippy::unwrap_used)]
            {
                assert_eq!(diff.missing_commits.first().unwrap().0, signed_commit);
            }
        }

        #[tokio::test]
        async fn test_sync_diff_with_fragments() {
            let signer = test_signer();
            let id = SedimentreeId::new([0u8; 32]);
            let blob = Blob::new(Vec::from([3u8; 16]));
            let fragment = Fragment::new(
                id,
                Digest::force_from_bytes([2u8; 32]),
                BTreeSet::new(),
                &[],
                sedimentree_core::blob::BlobMeta::new(&blob),
            );
            let signed_fragment = Signed::seal::<Sendable, _>(&signer, fragment)
                .await
                .into_signed();

            let diff = SyncDiff {
                missing_commits: Vec::new(),
                missing_fragments: vec![(signed_fragment, blob)],
                requesting: RequestedData::default(),
            };

            assert_eq!(diff.missing_fragments.len(), 1);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_batch_sync_request_preserves_req_id() {
            bolero::check!()
                .with_arbitrary::<BatchSyncRequest>()
                .for_each(|req| {
                    let msg: Message = req.clone().into();
                    assert_eq!(msg.request_id(), Some(req.req_id));
                });
        }

        #[test]
        fn prop_batch_sync_response_preserves_req_id() {
            bolero::check!()
                .with_arbitrary::<BatchSyncResponse>()
                .for_each(|resp| {
                    let msg: Message = resp.clone().into();
                    assert_eq!(msg.request_id(), Some(resp.req_id));
                });
        }

        #[test]
        #[allow(clippy::expect_used)]
        fn prop_message_codec_roundtrip() {
            bolero::check!()
                .with_arbitrary::<Message>()
                .for_each(|msg| {
                    let encoded = msg.encode();
                    let decoded = Message::try_decode(&encoded).expect("decode should succeed");
                    assert_eq!(msg, &decoded);
                });
        }
    }

    mod codec {
        use super::*;

        type TestResult = Result<(), Box<dyn std::error::Error>>;

        #[test]
        fn invalid_schema_rejected() -> TestResult {
            let mut bad_bytes = vec![0x00; 20];
            bad_bytes
                .get_mut(0..4)
                .ok_or("buffer too short")?
                .copy_from_slice(b"BAD\x00");
            bad_bytes
                .get_mut(4..8)
                .ok_or("buffer too short")?
                .copy_from_slice(&20u32.to_be_bytes());

            let result = Message::try_decode(&bad_bytes);
            assert!(matches!(result, Err(DecodeError::InvalidSchema(_))));
            Ok(())
        }

        #[test]
        fn size_mismatch_rejected() {
            let msg = Message::DataRequestRejected(DataRequestRejected {
                id: SedimentreeId::new([42u8; 32]),
            });
            let mut encoded = msg.encode();
            encoded.truncate(encoded.len() - 5);

            let result = Message::try_decode(&encoded);
            assert!(matches!(result, Err(DecodeError::SizeMismatch(_))));
        }

        #[test]
        fn invalid_tag_rejected() -> TestResult {
            let mut bad_bytes = vec![0x00; 20];
            bad_bytes
                .get_mut(0..4)
                .ok_or("buffer too short")?
                .copy_from_slice(&MESSAGE_SCHEMA);
            bad_bytes
                .get_mut(4..8)
                .ok_or("buffer too short")?
                .copy_from_slice(&20u32.to_be_bytes());
            *bad_bytes.get_mut(8).ok_or("buffer too short")? = 0xFF;

            let result = Message::try_decode(&bad_bytes);
            assert!(matches!(result, Err(DecodeError::InvalidEnumTag(_))));
            Ok(())
        }
    }
}
