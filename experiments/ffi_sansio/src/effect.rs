//! Per-trait effect and response enums for the Storage trait.
//!
//! Each variant corresponds to one Storage trait method.
//! The host language pattern-matches on the effect tag,
//! performs the operation, and provides a response.

use ffi_common::abi;

/// An I/O request from the state machine to the host.
///
/// Each variant carries CBOR-encoded arguments as a byte buffer.
/// The host decodes them, performs the operation, and provides
/// a `StorageResponse` with CBOR-encoded results.
#[derive(Debug)]
pub enum StorageEffect {
    // Sedimentree ID operations
    SaveSedimentreeId { id_cbor: Vec<u8> },
    DeleteSedimentreeId { id_cbor: Vec<u8> },
    LoadAllSedimentreeIds,

    // Loose commit operations
    SaveLooseCommit { args_cbor: Vec<u8> },
    LoadLooseCommit { args_cbor: Vec<u8> },
    ListCommitDigests { id_cbor: Vec<u8> },
    LoadLooseCommits { id_cbor: Vec<u8> },
    DeleteLooseCommit { args_cbor: Vec<u8> },
    DeleteLooseCommits { id_cbor: Vec<u8> },

    // Fragment operations
    SaveFragment { args_cbor: Vec<u8> },
    LoadFragment { args_cbor: Vec<u8> },
    ListFragmentDigests { id_cbor: Vec<u8> },
    LoadFragments { id_cbor: Vec<u8> },
    DeleteFragment { args_cbor: Vec<u8> },
    DeleteFragments { id_cbor: Vec<u8> },

    // Blob operations
    SaveBlob { blob_cbor: Vec<u8> },
    LoadBlob { digest_cbor: Vec<u8> },
    LoadBlobs { digests_cbor: Vec<u8> },
    DeleteBlob { digest_cbor: Vec<u8> },

    // Convenience (multi-step) — these decompose into the above
    SaveCommitWithBlob { args_cbor: Vec<u8> },
    SaveFragmentWithBlob { args_cbor: Vec<u8> },
    SaveBatch { args_cbor: Vec<u8> },
}

impl StorageEffect {
    /// Convert to an FFI-safe effect descriptor.
    pub fn to_ffi(&self) -> abi::FfiEffect {
        use ffi_common::abi::*;
        match self {
            Self::SaveSedimentreeId { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_SEDIMENTREE_ID, id_cbor.clone())
            }
            Self::DeleteSedimentreeId { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_SEDIMENTREE_ID, id_cbor.clone())
            }
            Self::LoadAllSedimentreeIds => {
                FfiEffect::new(EFFECT_TAG_LOAD_ALL_SEDIMENTREE_IDS, Vec::new())
            }
            Self::SaveLooseCommit { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_LOOSE_COMMIT, args_cbor.clone())
            }
            Self::LoadLooseCommit { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_LOOSE_COMMIT, args_cbor.clone())
            }
            Self::ListCommitDigests { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_LIST_COMMIT_DIGESTS, id_cbor.clone())
            }
            Self::LoadLooseCommits { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_LOOSE_COMMITS, id_cbor.clone())
            }
            Self::DeleteLooseCommit { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_LOOSE_COMMIT, args_cbor.clone())
            }
            Self::DeleteLooseCommits { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_LOOSE_COMMITS, id_cbor.clone())
            }
            Self::SaveFragment { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_FRAGMENT, args_cbor.clone())
            }
            Self::LoadFragment { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_FRAGMENT, args_cbor.clone())
            }
            Self::ListFragmentDigests { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_LIST_FRAGMENT_DIGESTS, id_cbor.clone())
            }
            Self::LoadFragments { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_FRAGMENTS, id_cbor.clone())
            }
            Self::DeleteFragment { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_FRAGMENT, args_cbor.clone())
            }
            Self::DeleteFragments { id_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_FRAGMENTS, id_cbor.clone())
            }
            Self::SaveBlob { blob_cbor } => FfiEffect::new(EFFECT_TAG_SAVE_BLOB, blob_cbor.clone()),
            Self::LoadBlob { digest_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_BLOB, digest_cbor.clone())
            }
            Self::LoadBlobs { digests_cbor } => {
                FfiEffect::new(EFFECT_TAG_LOAD_BLOBS, digests_cbor.clone())
            }
            Self::DeleteBlob { digest_cbor } => {
                FfiEffect::new(EFFECT_TAG_DELETE_BLOB, digest_cbor.clone())
            }
            Self::SaveCommitWithBlob { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_COMMIT_WITH_BLOB, args_cbor.clone())
            }
            Self::SaveFragmentWithBlob { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_FRAGMENT_WITH_BLOB, args_cbor.clone())
            }
            Self::SaveBatch { args_cbor } => {
                FfiEffect::new(EFFECT_TAG_SAVE_BATCH, args_cbor.clone())
            }
        }
    }
}

/// A response from the host to a previously emitted `StorageEffect`.
///
/// Contains CBOR-encoded result bytes. The interpretation depends on
/// which effect was emitted.
#[derive(Debug)]
pub struct StorageResponse {
    /// CBOR-encoded response data. Empty for unit-returning operations.
    pub data: Vec<u8>,
}
