//! Cryptographic primitives for verified data.

pub mod nonce;

/// A signed payload. See [`subduction_crypto::signed::Signed`].
pub type Signed<T> = subduction_crypto::signed::Signed<T>;

/// A verified signature witness. See [`subduction_crypto::verified_signature::VerifiedSignature`].
pub type VerifiedSignature<T> = subduction_crypto::verified_signature::VerifiedSignature<T>;

/// A verified metadata witness. See [`subduction_crypto::verified_meta::VerifiedMeta`].
pub type VerifiedMeta<T> = subduction_crypto::verified_meta::VerifiedMeta<T>;

/// Error when blob doesn't match claimed metadata. See [`subduction_crypto::verified_meta::BlobMismatch`].
pub type BlobMismatch = subduction_crypto::verified_meta::BlobMismatch;
