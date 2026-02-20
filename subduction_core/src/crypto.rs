//! Cryptographic primitives for verified data.

pub mod nonce;
pub mod signer;

// Re-export from subduction_crypto
pub use subduction_crypto::{
    signed, verified_meta, verified_signature, Signed, VerifiedMeta, VerifiedSignature,
};

/// Backwards compatibility alias — use [`VerifiedSignature`] instead.
#[deprecated(since = "0.4.0", note = "Renamed to VerifiedSignature")]
pub type Verified<T> = VerifiedSignature<T>;
