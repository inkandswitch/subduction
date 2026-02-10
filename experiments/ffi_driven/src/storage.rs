//! `DrivenStorage` — a `Storage` implementation that emits effects
//! instead of performing real I/O.
//!
//! Each method body is sequential async code using `emit()`. The
//! compiler generates the state machine. The external driver steps
//! through it via `DrivenFuture::poll_with_slot`.

use sedimentree_core::{
    blob::Blob, collections::Set, digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_core::{crypto::signed::Signed, storage::traits::BatchResult};

use crate::{driven::DrivenFuture, effect_future::emit};

// ==================== Effect types ====================
// One struct per Storage method's request.

pub struct SaveSedimentreeIdEff(pub SedimentreeId);
pub struct DeleteSedimentreeIdEff(pub SedimentreeId);
pub struct LoadAllSedimentreeIdsEff;

pub struct SaveLooseCommitEff(pub SedimentreeId, pub Signed<LooseCommit>);
pub struct LoadLooseCommitEff(pub SedimentreeId, pub Digest<LooseCommit>);
pub struct ListCommitDigestsEff(pub SedimentreeId);
pub struct LoadLooseCommitsEff(pub SedimentreeId);
pub struct DeleteLooseCommitEff(pub SedimentreeId, pub Digest<LooseCommit>);
pub struct DeleteLooseCommitsEff(pub SedimentreeId);

pub struct SaveFragmentEff(pub SedimentreeId, pub Signed<Fragment>);
pub struct LoadFragmentEff(pub SedimentreeId, pub Digest<Fragment>);
pub struct ListFragmentDigestsEff(pub SedimentreeId);
pub struct LoadFragmentsEff(pub SedimentreeId);
pub struct DeleteFragmentEff(pub SedimentreeId, pub Digest<Fragment>);
pub struct DeleteFragmentsEff(pub SedimentreeId);

pub struct SaveBlobEff(pub Blob);
pub struct LoadBlobEff(pub Digest<Blob>);
pub struct LoadBlobsEff(pub Vec<Digest<Blob>>);
pub struct DeleteBlobEff(pub Digest<Blob>);

// ==================== Response types ====================
// One struct per Storage method's return value.
// The host constructs these with the actual data.

pub struct UnitResp;
pub struct DigestResp<T>(pub Digest<T>);
pub struct OptionalSignedCommitResp(pub Option<Signed<LooseCommit>>);
pub struct OptionalSignedFragmentResp(pub Option<Signed<Fragment>>);
pub struct CommitDigestSetResp(pub Set<Digest<LooseCommit>>);
pub struct FragmentDigestSetResp(pub Set<Digest<Fragment>>);
pub struct SedimentreeIdSetResp(pub Set<SedimentreeId>);
pub struct CommitPairsResp(pub Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>);
pub struct FragmentPairsResp(pub Vec<(Digest<Fragment>, Signed<Fragment>)>);
pub struct OptionalBlobResp(pub Option<Blob>);
pub struct BlobPairsResp(pub Vec<(Digest<Blob>, Blob)>);
pub struct BatchResultResp(pub BatchResult);

// ==================== DrivenStorage ====================

/// A `Storage` implementation that emits effects to an external driver
/// instead of performing real I/O.
pub struct DrivenStorage;

/// Error type for driven storage — the host communicates errors
/// via the response types.
#[derive(Debug, thiserror::Error)]
#[error("driven storage error: {0}")]
pub struct DrivenError(pub String);

// We can't use `#[future_form(Driven)]` because we don't have a real
// FutureForm impl for Driven. Instead, we implement the trait methods
// directly, returning `DrivenFuture` (which is just Pin<Box<dyn Future>>).
//
// This is what the proc macro _would_ generate if Driven were a real
// FutureForm variant. The point of the experiment is to see if this
// pattern works, not to actually modify future_form yet.

// We need a marker type to use as the FutureForm parameter.
// Since we can't add Driven to future_form, we implement Storage
// for a concrete non-generic type and expose it via the FFI layer.
// The trait bounds require K: FutureForm, so we can't impl
// Storage<NotARealFutureForm>. Instead, we skip the trait entirely
// and provide methods with the same signatures.

impl DrivenStorage {
    pub fn save_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp = emit::<SaveSedimentreeIdEff, UnitResp>(SaveSedimentreeIdEff(id)).await;
            Ok(())
        }))
    }

    pub fn delete_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp =
                emit::<DeleteSedimentreeIdEff, UnitResp>(DeleteSedimentreeIdEff(id)).await;
            Ok(())
        }))
    }

    pub fn load_all_sedimentree_ids(
        &self,
    ) -> DrivenFuture<'_, Result<Set<SedimentreeId>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let SedimentreeIdSetResp(ids) =
                emit::<LoadAllSedimentreeIdsEff, SedimentreeIdSetResp>(LoadAllSedimentreeIdsEff)
                    .await;
            Ok(ids)
        }))
    }

    pub fn save_loose_commit(
        &self,
        id: SedimentreeId,
        commit: Signed<LooseCommit>,
    ) -> DrivenFuture<'_, Result<Digest<LooseCommit>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let DigestResp(digest) =
                emit::<SaveLooseCommitEff, DigestResp<LooseCommit>>(SaveLooseCommitEff(id, commit))
                    .await;
            Ok(digest)
        }))
    }

    pub fn load_loose_commit(
        &self,
        id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> DrivenFuture<'_, Result<Option<Signed<LooseCommit>>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let OptionalSignedCommitResp(commit) = emit::<
                LoadLooseCommitEff,
                OptionalSignedCommitResp,
            >(LoadLooseCommitEff(id, digest))
            .await;
            Ok(commit)
        }))
    }

    pub fn list_commit_digests(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<Set<Digest<LooseCommit>>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let CommitDigestSetResp(digests) =
                emit::<ListCommitDigestsEff, CommitDigestSetResp>(ListCommitDigestsEff(id)).await;
            Ok(digests)
        }))
    }

    pub fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, DrivenError>>
    {
        DrivenFuture::new(Box::pin(async move {
            let CommitPairsResp(pairs) =
                emit::<LoadLooseCommitsEff, CommitPairsResp>(LoadLooseCommitsEff(id)).await;
            Ok(pairs)
        }))
    }

    pub fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp =
                emit::<DeleteLooseCommitEff, UnitResp>(DeleteLooseCommitEff(id, digest)).await;
            Ok(())
        }))
    }

    pub fn delete_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp = emit::<DeleteLooseCommitsEff, UnitResp>(DeleteLooseCommitsEff(id)).await;
            Ok(())
        }))
    }

    pub fn save_fragment(
        &self,
        id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> DrivenFuture<'_, Result<Digest<Fragment>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let DigestResp(digest) =
                emit::<SaveFragmentEff, DigestResp<Fragment>>(SaveFragmentEff(id, fragment)).await;
            Ok(digest)
        }))
    }

    pub fn load_fragment(
        &self,
        id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> DrivenFuture<'_, Result<Option<Signed<Fragment>>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let OptionalSignedFragmentResp(fragment) =
                emit::<LoadFragmentEff, OptionalSignedFragmentResp>(LoadFragmentEff(id, digest))
                    .await;
            Ok(fragment)
        }))
    }

    pub fn list_fragment_digests(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<Set<Digest<Fragment>>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let FragmentDigestSetResp(digests) =
                emit::<ListFragmentDigestsEff, FragmentDigestSetResp>(ListFragmentDigestsEff(id))
                    .await;
            Ok(digests)
        }))
    }

    pub fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> DrivenFuture<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let FragmentPairsResp(pairs) =
                emit::<LoadFragmentsEff, FragmentPairsResp>(LoadFragmentsEff(id)).await;
            Ok(pairs)
        }))
    }

    pub fn delete_fragment(
        &self,
        id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp = emit::<DeleteFragmentEff, UnitResp>(DeleteFragmentEff(id, digest)).await;
            Ok(())
        }))
    }

    pub fn delete_fragments(&self, id: SedimentreeId) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp = emit::<DeleteFragmentsEff, UnitResp>(DeleteFragmentsEff(id)).await;
            Ok(())
        }))
    }

    pub fn save_blob(&self, blob: Blob) -> DrivenFuture<'_, Result<Digest<Blob>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let DigestResp(digest) = emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await;
            Ok(digest)
        }))
    }

    pub fn load_blob(
        &self,
        digest: Digest<Blob>,
    ) -> DrivenFuture<'_, Result<Option<Blob>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let OptionalBlobResp(blob) =
                emit::<LoadBlobEff, OptionalBlobResp>(LoadBlobEff(digest)).await;
            Ok(blob)
        }))
    }

    pub fn load_blobs(
        &self,
        digests: Vec<Digest<Blob>>,
    ) -> DrivenFuture<'_, Result<Vec<(Digest<Blob>, Blob)>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let BlobPairsResp(pairs) =
                emit::<LoadBlobsEff, BlobPairsResp>(LoadBlobsEff(digests)).await;
            Ok(pairs)
        }))
    }

    pub fn delete_blob(&self, digest: Digest<Blob>) -> DrivenFuture<'_, Result<(), DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let UnitResp = emit::<DeleteBlobEff, UnitResp>(DeleteBlobEff(digest)).await;
            Ok(())
        }))
    }

    // ==================== Convenience (multi-step) ====================
    // These are where the Driven approach shines: sequential async code
    // that the compiler turns into a multi-step state machine.

    pub fn save_commit_with_blob(
        &self,
        id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> DrivenFuture<'_, Result<Digest<Blob>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            // Step 1: save the commit
            let DigestResp(_commit_digest) =
                emit::<SaveLooseCommitEff, DigestResp<LooseCommit>>(SaveLooseCommitEff(id, commit))
                    .await;

            // Step 2: save the blob
            let DigestResp(blob_digest) =
                emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await;

            Ok(blob_digest)
        }))
    }

    pub fn save_fragment_with_blob(
        &self,
        id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> DrivenFuture<'_, Result<Digest<Blob>, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            // Step 1: save the fragment
            let DigestResp(_fragment_digest) =
                emit::<SaveFragmentEff, DigestResp<Fragment>>(SaveFragmentEff(id, fragment)).await;

            // Step 2: save the blob
            let DigestResp(blob_digest) =
                emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await;

            Ok(blob_digest)
        }))
    }

    pub fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> DrivenFuture<'_, Result<BatchResult, DrivenError>> {
        DrivenFuture::new(Box::pin(async move {
            let mut commit_digests = Vec::with_capacity(commits.len());
            let mut fragment_digests = Vec::with_capacity(fragments.len());

            // Save all commits + their blobs
            for (commit, blob) in commits {
                let DigestResp(cd) = emit::<SaveLooseCommitEff, DigestResp<LooseCommit>>(
                    SaveLooseCommitEff(id, commit),
                )
                .await;
                commit_digests.push(cd);

                let DigestResp(_blob_digest) =
                    emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await;
            }

            // Save all fragments + their blobs
            for (fragment, blob) in fragments {
                let DigestResp(fd) =
                    emit::<SaveFragmentEff, DigestResp<Fragment>>(SaveFragmentEff(id, fragment))
                        .await;
                fragment_digests.push(fd);

                let DigestResp(_blob_digest) =
                    emit::<SaveBlobEff, DigestResp<Blob>>(SaveBlobEff(blob)).await;
            }

            Ok(BatchResult {
                commit_digests,
                fragment_digests,
            })
        }))
    }
}
