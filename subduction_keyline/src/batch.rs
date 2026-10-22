//! Keyline certificates as sedimentree commits.
//!
//! Certificates have no causal parents of their own, so each sedimentree
//! commit wraps a _batch_: a canonical, non-empty set of certificates. The
//! commit's parents are every head the writer knew when it issued, which
//! lets fragments form (and compact) across writers.
//!
//! ```text
//! LooseCommit {
//!   sedimentree_id: KEYLINE_TREE_ID
//!   head:           commit_id(parents, blob)
//!   parents:        heads known at issuance
//!   blob:           "KLB" ‖ version ‖ certificate frames, ascending by id
//! }
//! ```
//!
//! The [`CommitId`] covers both the parents and the blob. Two batches with
//! the same certificates but different parents get different IDs, so one ID
//! can never stand for two parent sets.
//!
//! A batch maps to one atomic [`Keyline::insert`]: the certificates from one
//! Keyhive operation land together.
//!
//! [`Keyline::insert`]: keyline::contract::Keyline::insert

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};

use keyhive_codec::traits::{Encode, StreamDecode};
use keyline::certificate::{Certificate, CertificateId, VerifiedCertificate};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};

/// The one sedimentree that holds every certificate.
///
/// The Ed25519 point `y = -1`, which has order 2. It is a canonical point
/// encoding, but never a valid [`Identifier`], so it can never be a document's
/// sedimentree ID. Its upper bytes are not zero, so it is not a legacy
/// zero-padded ID either.
///
/// [`Identifier`]: keyhive_crypto::identifier::Identifier
pub const KEYLINE_TREE_ID: SedimentreeId = SedimentreeId::new([
    0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
]);

/// The first bytes of every batch blob: `"KLB"` and the format version.
pub const BATCH_HEADER: [u8; 4] = *b"KLB\x01";

/// The BLAKE3 key-derivation context for [`commit_id`].
const COMMIT_ID_CONTEXT: &str = "subduction_keyline 2026-10-22 batch commit id v1";

/// A canonical, non-empty set of certificates, as received.
///
/// Signatures are not checked here; [`crate::ingest::admit`] verifies them.
/// The only ways in are [`Batch::new`] and [`Batch::decode`], and both
/// establish the invariant: at least one certificate, keyed by
/// [`CertificateId`].
#[derive(Debug, PartialEq, Eq)]
pub struct Batch<W> {
    certificates: BTreeMap<CertificateId, Certificate<W>>,
}

// Manual: the derive would bound `W: Clone`, which sharing does not need.
impl<W> Clone for Batch<W> {
    fn clone(&self) -> Self {
        Self {
            certificates: self.certificates.clone(),
        }
    }
}

impl<W> Batch<W> {
    /// Collect certificates into a batch. Duplicates collapse.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyBatchError`] if there are no certificates.
    pub fn new<I: IntoIterator<Item = Certificate<W>>>(
        certificates: I,
    ) -> Result<Self, EmptyBatchError> {
        let certificates: BTreeMap<_, _> = certificates
            .into_iter()
            .map(|cert| (cert.id(), cert))
            .collect();

        if certificates.is_empty() {
            Err(EmptyBatchError)
        } else {
            Ok(Self { certificates })
        }
    }

    /// Batch certificates this replica signed or verified.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyBatchError`] if there are no certificates.
    pub fn from_verified<I: IntoIterator<Item = VerifiedCertificate<W>>>(
        certificates: I,
    ) -> Result<Self, EmptyBatchError> {
        Self::new(
            certificates
                .into_iter()
                .map(VerifiedCertificate::into_certificate),
        )
    }

    /// The certificates, ascending by [`CertificateId`].
    pub fn certificates(&self) -> impl Iterator<Item = &Certificate<W>> {
        self.certificates.values()
    }

    /// The certificate IDs, ascending.
    pub fn ids(&self) -> impl Iterator<Item = CertificateId> + '_ {
        self.certificates.keys().copied()
    }

    /// Consume the batch, ascending by [`CertificateId`].
    pub fn into_certificates(self) -> impl Iterator<Item = Certificate<W>> {
        self.certificates.into_values()
    }

    /// The number of certificates. Always at least one.
    #[must_use]
    pub fn len(&self) -> usize {
        self.certificates.len()
    }

    /// Always `false`: a batch has at least one certificate.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// The canonical encoding: [`BATCH_HEADER`], then each certificate's
    /// frame in ascending [`CertificateId`] order.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::from(BATCH_HEADER);
        for cert in self.certificates.values() {
            cert.encode_into(&mut out);
        }
        out
    }

    /// Decode a batch, accepting only the canonical encoding.
    ///
    /// Runs on bytes nobody has authenticated yet. It checks framing only;
    /// signatures are checked on admission.
    ///
    /// # Errors
    ///
    /// Returns [`BatchDecodeError`] for a wrong header, no certificates, a
    /// frame that does not decode, or certificates that are not strictly
    /// ascending by ID (which also rules out duplicates).
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchDecodeError> {
        let mut rest = bytes
            .strip_prefix(BATCH_HEADER.as_slice())
            .ok_or(BatchDecodeError::Header)?;

        let mut certificates = BTreeMap::new();
        while !rest.is_empty() {
            let (cert, tail) = Certificate::<W>::decode_next(rest)?;
            let id = cert.id();
            if certificates
                .last_key_value()
                .is_some_and(|(last, _)| *last >= id)
            {
                return Err(BatchDecodeError::NotAscending);
            }
            certificates.insert(id, cert);
            rest = tail;
        }

        if certificates.is_empty() {
            Err(BatchDecodeError::Empty)
        } else {
            Ok(Self { certificates })
        }
    }

    /// Wrap the batch in an unsigned sedimentree commit on
    /// [`KEYLINE_TREE_ID`], with `parents` as its causal predecessors.
    ///
    /// The caller signs the commit (`Signed::seal`) and sends it with the
    /// blob.
    #[must_use]
    pub fn into_commit(&self, parents: BTreeSet<CommitId>) -> (LooseCommit, Blob) {
        let blob = Blob::new(self.encode());
        let blob_meta = BlobMeta::new(&blob);
        let head = commit_id(&parents, &blob_meta);
        (
            LooseCommit::new(KEYLINE_TREE_ID, head, parents, blob_meta),
            blob,
        )
    }

    /// Parse a received commit and its blob as a batch.
    ///
    /// Checks that the commit is on [`KEYLINE_TREE_ID`], that its blob
    /// matches its metadata, and that its ID is [`commit_id`] of its parents
    /// and blob.
    ///
    /// # Errors
    ///
    /// Returns [`BatchCommitError`] if any check fails or the blob is not a
    /// canonical batch.
    pub fn from_commit(commit: &LooseCommit, blob: &Blob) -> Result<Self, BatchCommitError> {
        if commit.sedimentree_id() != KEYLINE_TREE_ID {
            return Err(BatchCommitError::WrongTree(commit.sedimentree_id()));
        }

        let blob_meta = BlobMeta::new(blob);
        if blob_meta != *commit.blob_meta() {
            return Err(BatchCommitError::BlobMismatch);
        }

        let expected = commit_id(commit.parents(), &blob_meta);
        if commit.head() != expected {
            return Err(BatchCommitError::WrongCommitId {
                expected,
                actual: commit.head(),
            });
        }

        Ok(Self::decode(blob.as_slice())?)
    }
}

/// The [`CommitId`] of a batch commit: BLAKE3, keyed by a fixed context,
/// over its parents (ascending) and its blob digest.
#[must_use]
pub fn commit_id(parents: &BTreeSet<CommitId>, blob_meta: &BlobMeta) -> CommitId {
    let mut hasher = blake3::Hasher::new_derive_key(COMMIT_ID_CONTEXT);
    hasher.update(&(parents.len() as u64).to_le_bytes());
    for parent in parents {
        hasher.update(parent.as_bytes());
    }
    hasher.update(blob_meta.digest().as_bytes());
    CommitId::new(*hasher.finalize().as_bytes())
}

/// A batch needs at least one certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a batch needs at least one certificate")]
pub struct EmptyBatchError;

/// Why bytes are not a canonical batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BatchDecodeError {
    /// The bytes do not start with [`BATCH_HEADER`].
    #[error("missing or unsupported batch header")]
    Header,

    /// The batch has no certificates.
    #[error("batch has no certificates")]
    Empty,

    /// Certificates are not strictly ascending by ID.
    #[error("certificates are not strictly ascending by ID")]
    NotAscending,

    /// A certificate frame did not decode.
    #[error("certificate frame did not decode")]
    Certificate(#[from] keyhive_codec::error::DecodeError),
}

/// Why a commit is not a valid batch commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BatchCommitError {
    /// The commit is on another sedimentree.
    #[error("commit is on sedimentree {0:?}, not the keyline tree")]
    WrongTree(SedimentreeId),

    /// The blob does not match the commit's blob metadata.
    #[error("blob does not match the commit's metadata")]
    BlobMismatch,

    /// The commit ID is not derived from its parents and blob.
    #[error("commit ID {actual:?} is not the batch commit ID {expected:?}")]
    WrongCommitId {
        /// The ID derived from the parents and blob.
        expected: CommitId,

        /// The ID the commit claims.
        actual: CommitId,
    },

    /// The blob is not a canonical batch.
    #[error(transparent)]
    Decode(#[from] BatchDecodeError),
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]
mod tests {
    use super::*;

    use alloc::vec;

    use keyline::{
        power::Power,
        test_utils::{assume_verified, delegation, id},
    };
    use testresult::TestResult;

    type W = ();

    fn cert(issuer: u8, audience: u8, subject: u8, power: Power) -> Certificate<W> {
        assume_verified::<W, _>(delegation(id(issuer), id(audience), id(subject), power))
            .into_certificate()
    }

    fn arbitrary_certs(raw: &[(u8, u8, u8, u8)]) -> Vec<Certificate<W>> {
        raw.iter()
            .map(|&(i, a, s, p)| cert(i % 8, a % 8, s % 8, Power::ALL[usize::from(p % 4)]))
            .collect()
    }

    #[test]
    fn tree_id_is_never_a_document_or_legacy_id() {
        assert!(crate::policy::sedimentree_identifier(KEYLINE_TREE_ID).is_err());
        assert!(KEYLINE_TREE_ID.as_bytes()[16..].iter().any(|&b| b != 0));
    }

    #[test]
    fn empty_batches_are_unrepresentable() {
        assert_eq!(Batch::<W>::new([]), Err(EmptyBatchError));
        assert_eq!(
            Batch::<W>::decode(&BATCH_HEADER),
            Err(BatchDecodeError::Empty)
        );
    }

    #[test]
    fn decode_rejects_bad_framing() -> TestResult {
        let batch = Batch::new([cert(1, 2, 1, Power::Read), cert(1, 3, 1, Power::Edit)])?;
        let bytes = batch.encode();

        assert_eq!(
            Batch::<W>::decode(&bytes[1..]),
            Err(BatchDecodeError::Header)
        );

        let mut wrong_version = bytes.clone();
        wrong_version[3] = 2;
        assert_eq!(
            Batch::<W>::decode(&wrong_version),
            Err(BatchDecodeError::Header)
        );

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            Batch::<W>::decode(&trailing),
            Err(BatchDecodeError::Certificate(_))
        ));

        assert!(matches!(
            Batch::<W>::decode(&bytes[..bytes.len() - 1]),
            Err(BatchDecodeError::Certificate(_))
        ));
        Ok(())
    }

    #[test]
    fn decode_rejects_unsorted_and_duplicate_certificates() -> TestResult {
        let batch = Batch::new([cert(1, 2, 1, Power::Read), cert(1, 3, 1, Power::Edit)])?;
        let mut frames: Vec<Vec<u8>> = batch
            .certificates()
            .map(|c| {
                let mut out = Vec::new();
                c.encode_into(&mut out);
                out
            })
            .collect();

        let reversed: Vec<u8> = BATCH_HEADER
            .iter()
            .copied()
            .chain(frames.iter().rev().flatten().copied())
            .collect();
        assert_eq!(
            Batch::<W>::decode(&reversed),
            Err(BatchDecodeError::NotAscending)
        );

        frames.truncate(1);
        let duplicated: Vec<u8> = BATCH_HEADER
            .iter()
            .copied()
            .chain(frames[0].iter().copied())
            .chain(frames[0].iter().copied())
            .collect();
        assert_eq!(
            Batch::<W>::decode(&duplicated),
            Err(BatchDecodeError::NotAscending)
        );
        Ok(())
    }

    /// Encoding round-trips, and depends only on the set of certificates:
    /// not their order, not repeats.
    #[test]
    fn encoding_is_canonical() {
        bolero::check!()
            .with_type::<(Vec<(u8, u8, u8, u8)>, u8)>()
            .for_each(|(raw, rotate)| {
                let certs = arbitrary_certs(raw);
                let Ok(batch) = Batch::new(certs.clone()) else {
                    assert!(certs.is_empty());
                    return;
                };

                let decoded = Batch::<W>::decode(&batch.encode()).expect("round trip");
                assert_eq!(decoded, batch);

                let mut shuffled = certs.clone();
                let by = usize::from(*rotate) % shuffled.len();
                shuffled.rotate_left(by);
                shuffled.extend(certs.iter().take(2).cloned());
                assert_eq!(
                    Batch::new(shuffled).expect("non-empty").encode(),
                    batch.encode()
                );
            });
    }

    #[test]
    fn commit_round_trips_and_is_checked() -> TestResult {
        let batch = Batch::new([cert(1, 2, 1, Power::Read)])?;
        let parents = BTreeSet::from([CommitId::new([7; 32])]);
        let (commit, blob) = batch.into_commit(parents.clone());

        assert_eq!(Batch::from_commit(&commit, &blob)?, batch);

        let other_tree = LooseCommit::new(
            SedimentreeId::new([1; 32]),
            commit.head(),
            parents.clone(),
            *commit.blob_meta(),
        );
        assert!(matches!(
            Batch::<W>::from_commit(&other_tree, &blob),
            Err(BatchCommitError::WrongTree(_))
        ));

        assert_eq!(
            Batch::<W>::from_commit(&commit, &Blob::new(vec![0])),
            Err(BatchCommitError::BlobMismatch)
        );

        let forged = LooseCommit::new(
            KEYLINE_TREE_ID,
            CommitId::new([9; 32]),
            parents,
            *commit.blob_meta(),
        );
        assert!(matches!(
            Batch::<W>::from_commit(&forged, &blob),
            Err(BatchCommitError::WrongCommitId { .. })
        ));
        Ok(())
    }

    /// The same batch under different parents gets a different commit ID.
    #[test]
    fn commit_id_covers_parents() {
        bolero::check!()
            .with_type::<(Vec<[u8; 2]>, Vec<[u8; 2]>)>()
            .for_each(|(left, right)| {
                let to_parents = |raw: &Vec<[u8; 2]>| -> BTreeSet<CommitId> {
                    raw.iter()
                        .map(|&[a, b]| {
                            let mut bytes = [0; 32];
                            bytes[0] = a;
                            bytes[1] = b;
                            CommitId::new(bytes)
                        })
                        .collect()
                };
                let (left, right) = (to_parents(left), to_parents(right));
                let blob_meta = BlobMeta::new(&Blob::new(vec![1, 2, 3]));

                assert_eq!(
                    commit_id(&left, &blob_meta) == commit_id(&right, &blob_meta),
                    left == right
                );
            });
    }
}
