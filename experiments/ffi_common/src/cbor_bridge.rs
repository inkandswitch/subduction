//! CBOR serialization bridge for Storage trait types.
//!
//! All complex Subduction types cross the FFI boundary as CBOR-encoded
//! byte buffers. This module provides encode/decode functions for each type
//! used in the `Storage` trait's method signatures.

use crate::error::FfiError;
use sedimentree_core::{
    blob::Blob, collections::Set, digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_core::crypto::signed::Signed;

fn encode<T: minicbor::Encode<()>>(value: &T) -> Result<Vec<u8>, FfiError> {
    minicbor::to_vec(value).map_err(|e| FfiError::Encode(e.to_string()))
}

fn decode<'a, T: minicbor::Decode<'a, ()>>(bytes: &'a [u8]) -> Result<T, FfiError> {
    minicbor::decode(bytes).map_err(FfiError::from)
}

// ======================== SedimentreeId ========================

pub fn encode_sedimentree_id(id: &SedimentreeId) -> Result<Vec<u8>, FfiError> {
    encode(id)
}

pub fn decode_sedimentree_id(bytes: &[u8]) -> Result<SedimentreeId, FfiError> {
    decode(bytes)
}

pub fn encode_sedimentree_id_set(ids: &Set<SedimentreeId>) -> Result<Vec<u8>, FfiError> {
    let vec: Vec<&SedimentreeId> = ids.iter().collect();
    encode(&vec)
}

pub fn decode_sedimentree_id_set(bytes: &[u8]) -> Result<Set<SedimentreeId>, FfiError> {
    let vec: Vec<SedimentreeId> = decode(bytes)?;
    Ok(vec.into_iter().collect())
}

// ======================== Digest<T> ========================

pub fn encode_commit_digest(digest: &Digest<LooseCommit>) -> Result<Vec<u8>, FfiError> {
    encode(digest)
}

pub fn decode_commit_digest(bytes: &[u8]) -> Result<Digest<LooseCommit>, FfiError> {
    decode(bytes)
}

pub fn encode_fragment_digest(digest: &Digest<Fragment>) -> Result<Vec<u8>, FfiError> {
    encode(digest)
}

pub fn decode_fragment_digest(bytes: &[u8]) -> Result<Digest<Fragment>, FfiError> {
    decode(bytes)
}

pub fn encode_blob_digest(digest: &Digest<Blob>) -> Result<Vec<u8>, FfiError> {
    encode(digest)
}

pub fn decode_blob_digest(bytes: &[u8]) -> Result<Digest<Blob>, FfiError> {
    decode(bytes)
}

pub fn encode_commit_digest_set(digests: &Set<Digest<LooseCommit>>) -> Result<Vec<u8>, FfiError> {
    let vec: Vec<&Digest<LooseCommit>> = digests.iter().collect();
    encode(&vec)
}

pub fn decode_commit_digest_set(bytes: &[u8]) -> Result<Set<Digest<LooseCommit>>, FfiError> {
    let vec: Vec<Digest<LooseCommit>> = decode(bytes)?;
    Ok(vec.into_iter().collect())
}

pub fn encode_fragment_digest_set(digests: &Set<Digest<Fragment>>) -> Result<Vec<u8>, FfiError> {
    let vec: Vec<&Digest<Fragment>> = digests.iter().collect();
    encode(&vec)
}

pub fn decode_fragment_digest_set(bytes: &[u8]) -> Result<Set<Digest<Fragment>>, FfiError> {
    let vec: Vec<Digest<Fragment>> = decode(bytes)?;
    Ok(vec.into_iter().collect())
}

// ======================== Blob ========================

pub fn encode_blob(blob: &Blob) -> Result<Vec<u8>, FfiError> {
    encode(blob)
}

pub fn decode_blob(bytes: &[u8]) -> Result<Blob, FfiError> {
    decode(bytes)
}

pub fn encode_optional_blob(blob: &Option<Blob>) -> Result<Vec<u8>, FfiError> {
    encode(blob)
}

pub fn decode_optional_blob(bytes: &[u8]) -> Result<Option<Blob>, FfiError> {
    decode(bytes)
}

pub fn encode_blob_digest_pairs(pairs: &[(Digest<Blob>, Blob)]) -> Result<Vec<u8>, FfiError> {
    encode(&pairs)
}

pub fn decode_blob_digest_pairs(bytes: &[u8]) -> Result<Vec<(Digest<Blob>, Blob)>, FfiError> {
    decode(bytes)
}

pub fn encode_blob_digest_slice(digests: &[Digest<Blob>]) -> Result<Vec<u8>, FfiError> {
    encode(&digests)
}

pub fn decode_blob_digest_slice(bytes: &[u8]) -> Result<Vec<Digest<Blob>>, FfiError> {
    decode(bytes)
}

// ======================== Signed<LooseCommit> ========================

pub fn encode_signed_commit(commit: &Signed<LooseCommit>) -> Result<Vec<u8>, FfiError> {
    encode(commit)
}

pub fn decode_signed_commit(bytes: &[u8]) -> Result<Signed<LooseCommit>, FfiError> {
    decode(bytes)
}

pub fn encode_optional_signed_commit(
    commit: &Option<Signed<LooseCommit>>,
) -> Result<Vec<u8>, FfiError> {
    encode(commit)
}

pub fn decode_optional_signed_commit(
    bytes: &[u8],
) -> Result<Option<Signed<LooseCommit>>, FfiError> {
    decode(bytes)
}

pub fn encode_commit_pairs(
    pairs: &[(Digest<LooseCommit>, Signed<LooseCommit>)],
) -> Result<Vec<u8>, FfiError> {
    encode(&pairs)
}

pub fn decode_commit_pairs(
    bytes: &[u8],
) -> Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, FfiError> {
    decode(bytes)
}

// ======================== Signed<Fragment> ========================

pub fn encode_signed_fragment(fragment: &Signed<Fragment>) -> Result<Vec<u8>, FfiError> {
    encode(fragment)
}

pub fn decode_signed_fragment(bytes: &[u8]) -> Result<Signed<Fragment>, FfiError> {
    decode(bytes)
}

pub fn encode_optional_signed_fragment(
    fragment: &Option<Signed<Fragment>>,
) -> Result<Vec<u8>, FfiError> {
    encode(fragment)
}

pub fn decode_optional_signed_fragment(bytes: &[u8]) -> Result<Option<Signed<Fragment>>, FfiError> {
    decode(bytes)
}

pub fn encode_fragment_pairs(
    pairs: &[(Digest<Fragment>, Signed<Fragment>)],
) -> Result<Vec<u8>, FfiError> {
    encode(&pairs)
}

pub fn decode_fragment_pairs(
    bytes: &[u8],
) -> Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, FfiError> {
    decode(bytes)
}

// ======================== Batch types ========================

/// CBOR-encodable representation of `save_commit_with_blob` args.
#[derive(minicbor::Encode, minicbor::Decode)]
pub struct CommitWithBlobArgs {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub commit: Signed<LooseCommit>,
    #[n(2)]
    pub blob: Blob,
}

/// CBOR-encodable representation of `save_fragment_with_blob` args.
#[derive(minicbor::Encode, minicbor::Decode)]
pub struct FragmentWithBlobArgs {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub fragment: Signed<Fragment>,
    #[n(2)]
    pub blob: Blob,
}

/// CBOR-encodable representation of `save_batch` args.
#[derive(minicbor::Encode, minicbor::Decode)]
pub struct SaveBatchArgs {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub commits: Vec<CommitBlobPair>,
    #[n(2)]
    pub fragments: Vec<FragmentBlobPair>,
}

#[derive(minicbor::Encode, minicbor::Decode)]
pub struct CommitBlobPair {
    #[n(0)]
    pub commit: Signed<LooseCommit>,
    #[n(1)]
    pub blob: Blob,
}

#[derive(minicbor::Encode, minicbor::Decode)]
pub struct FragmentBlobPair {
    #[n(0)]
    pub fragment: Signed<Fragment>,
    #[n(1)]
    pub blob: Blob,
}

/// CBOR-encodable representation of `BatchResult`.
#[derive(minicbor::Encode, minicbor::Decode)]
pub struct BatchResultWire {
    #[n(0)]
    pub commit_digests: Vec<Digest<LooseCommit>>,
    #[n(1)]
    pub fragment_digests: Vec<Digest<Fragment>>,
}

impl From<subduction_core::storage::traits::BatchResult> for BatchResultWire {
    fn from(r: subduction_core::storage::traits::BatchResult) -> Self {
        Self {
            commit_digests: r.commit_digests,
            fragment_digests: r.fragment_digests,
        }
    }
}

pub fn encode_batch_result(
    result: &subduction_core::storage::traits::BatchResult,
) -> Result<Vec<u8>, FfiError> {
    let wire = BatchResultWire::from(result.clone());
    encode(&wire)
}

/// CBOR-encodable pair of (SedimentreeId, Digest) for two-arg methods.
#[derive(minicbor::Encode, minicbor::Decode)]
pub struct IdAndCommitDigest {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub digest: Digest<LooseCommit>,
}

#[derive(minicbor::Encode, minicbor::Decode)]
pub struct IdAndFragmentDigest {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub digest: Digest<Fragment>,
}

#[derive(minicbor::Encode, minicbor::Decode)]
pub struct IdAndSignedCommit {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub commit: Signed<LooseCommit>,
}

#[derive(minicbor::Encode, minicbor::Decode)]
pub struct IdAndSignedFragment {
    #[n(0)]
    pub id: SedimentreeId,
    #[n(1)]
    pub fragment: Signed<Fragment>,
}
