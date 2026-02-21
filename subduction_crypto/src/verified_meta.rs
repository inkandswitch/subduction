//! A payload whose signature is valid AND whose blob matches the claimed metadata.

use future_form::FutureForm;
use sedimentree_core::blob::{Blob, BlobMeta, has_meta::HasBlobMeta, verified::VerifiedBlobMeta};
use thiserror::Error;

use crate::{signed::Signed, signer::Signer, verified_signature::VerifiedSignature};

/// A commit or fragment whose signature is valid AND whose blob matches the claimed metadata.
///
/// This is a stronger witness than [`VerifiedSignature<T>`]:
/// - `VerifiedSignature<T>` proves the signature is valid
/// - `VerifiedMeta<T>` proves both signature validity AND blob integrity
///
/// # Usage
///
/// ```text
/// let verified_sig = signed_commit.try_verify()?;
/// let verified_meta = VerifiedMeta::new(verified_sig, blob)?;
/// putter.save_commit(verified_meta).await?;
/// ```
///
/// The [`Putter`] only accepts `VerifiedMeta<T>`, ensuring blob integrity
/// is checked at compile time before storage.
///
/// [`Putter`]: subduction_core::storage::putter::Putter
#[derive(Clone, Debug)]
pub struct VerifiedMeta<T: for<'a> minicbor::Decode<'a, ()>> {
    verified: VerifiedSignature<T>,
    blob: Blob,
}

/// The blob content doesn't match the claimed metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[error("blob mismatch: claimed {claimed:?}, actual {actual:?}")]
pub struct BlobMismatch {
    /// The metadata claimed by the commit/fragment.
    pub claimed: BlobMeta,

    /// The actual metadata computed from the blob.
    pub actual: BlobMeta,
}

impl<T: HasBlobMeta + minicbor::Encode<()> + for<'a> minicbor::Decode<'a, ()>> VerifiedMeta<T> {
    /// Create a `VerifiedMeta<T>` after verifying the blob matches the claimed metadata.
    ///
    /// # Errors
    ///
    /// Returns [`BlobMismatch`] if the blob's computed metadata doesn't match
    /// the metadata claimed by the payload.
    pub fn new(verified: VerifiedSignature<T>, blob: Blob) -> Result<Self, BlobMismatch> {
        let actual = blob.meta();
        let claimed = verified.payload().blob_meta();
        if actual != claimed {
            return Err(BlobMismatch { claimed, actual });
        }
        Ok(Self { verified, blob })
    }

    /// Returns a reference to the inner `VerifiedSignature<T>`.
    #[must_use]
    pub const fn verified_signature(&self) -> &VerifiedSignature<T> {
        &self.verified
    }

    /// Returns a reference to the verified blob.
    #[must_use]
    pub const fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Returns a reference to the verified payload.
    #[must_use]
    pub const fn payload(&self) -> &T {
        self.verified.payload()
    }

    /// Returns the issuer's verifying key.
    #[must_use]
    pub const fn issuer(&self) -> ed25519_dalek::VerifyingKey {
        self.verified.issuer()
    }

    /// Returns a reference to the original signed value.
    #[must_use]
    pub const fn signed(&self) -> &Signed<T> {
        self.verified.signed()
    }

    /// Consumes the `VerifiedMeta` and returns its parts.
    #[must_use]
    pub fn into_parts(self) -> (VerifiedSignature<T>, Blob) {
        (self.verified, self.blob)
    }

    /// Consumes the `VerifiedMeta` and returns all three parts.
    #[must_use]
    pub fn into_full_parts(self) -> (Signed<T>, T, Blob) {
        let (signed, payload) = self.verified.into_parts();
        (signed, payload, self.blob)
    }

    /// Seal a locally-created payload+blob, producing a verified meta.
    ///
    /// Infallible because [`VerifiedBlobMeta`] guarantees the blob metadata
    /// matches by construction, and `T` is constructed from that metadata.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let verified_blob = VerifiedBlobMeta::new(blob);
    /// let verified_meta = VerifiedMeta::seal::<Sendable, _, LooseCommit>(
    ///     &signer,
    ///     (digest, parents),
    ///     verified_blob,
    /// ).await;
    /// ```
    pub async fn seal<K: FutureForm, S: Signer<K>>(
        signer: &S,
        args: T::Args,
        verified_blob: VerifiedBlobMeta,
    ) -> Self {
        let (blob_meta, blob) = verified_blob.into_parts();
        let meta = T::from_args(args, blob_meta);
        let verified = Signed::seal::<K, _>(signer, meta).await;
        Self { verified, blob }
    }
}
