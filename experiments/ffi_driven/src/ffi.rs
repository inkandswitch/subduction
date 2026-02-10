//! C ABI for the Driven approach.
//!
//! Same external protocol as sans-I/O. The difference is entirely
//! internal: state machines are compiler-generated from async code.

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::task::Poll;

use ffi_common::{
    abi::{self, FfiEffect, FfiResult},
    cbor_bridge,
};

use crate::{
    driven::{DrivenFuture, EffectSlot},
    effect_future::emit,
    storage::*,
};

/// The driver holds a `DrivenFuture` and its `EffectSlot`.
pub struct DrivenDriver {
    future: Option<DrivenFuture<'static, Result<Vec<u8>, DrivenError>>>,
    slot: EffectSlot,
    result: Option<Result<Vec<u8>, DrivenError>>,
    current_tag: u32,
    current_data: Vec<u8>,
}

type DriverHandle = *mut DrivenDriver;

impl DrivenDriver {
    fn new(future: DrivenFuture<'static, Result<Vec<u8>, DrivenError>>) -> Self {
        Self {
            future: Some(future),
            slot: EffectSlot::new(),
            result: None,
            current_tag: 0,
            current_data: Vec::new(),
        }
    }

    fn poll(&mut self) -> bool {
        let Some(future) = &mut self.future else {
            return true;
        };

        match future.poll_with_slot(&self.slot) {
            Poll::Ready(result) => {
                self.future = None;
                self.result = Some(result);
                true
            }
            Poll::Pending => {
                if let Some(effect) = self.slot.take_effect() {
                    self.extract_typed_effect(effect);
                }
                false
            }
        }
    }

    fn extract_typed_effect(&mut self, effect: Box<dyn Any>) {
        // Dispatch by effect type → CBOR encode → set tag + data
        macro_rules! try_effect {
            ($eff_type:ty, $tag:expr, $encode:expr) => {
                if let Some(eff) = effect.downcast_ref::<$eff_type>() {
                    self.current_tag = $tag;
                    self.current_data = $encode(eff);
                    return;
                }
            };
        }

        try_effect!(
            SaveSedimentreeIdEff,
            abi::EFFECT_TAG_SAVE_SEDIMENTREE_ID,
            |e: &SaveSedimentreeIdEff| cbor_bridge::encode_sedimentree_id(&e.0).unwrap_or_default()
        );
        try_effect!(
            DeleteSedimentreeIdEff,
            abi::EFFECT_TAG_DELETE_SEDIMENTREE_ID,
            |e: &DeleteSedimentreeIdEff| cbor_bridge::encode_sedimentree_id(&e.0)
                .unwrap_or_default()
        );
        try_effect!(
            LoadAllSedimentreeIdsEff,
            abi::EFFECT_TAG_LOAD_ALL_SEDIMENTREE_IDS,
            |_: &LoadAllSedimentreeIdsEff| Vec::new()
        );

        try_effect!(
            SaveLooseCommitEff,
            abi::EFFECT_TAG_SAVE_LOOSE_COMMIT,
            |e: &SaveLooseCommitEff| minicbor::to_vec(&cbor_bridge::IdAndSignedCommit {
                id: e.0,
                commit: e.1.clone()
            })
            .unwrap_or_default()
        );
        try_effect!(
            LoadLooseCommitEff,
            abi::EFFECT_TAG_LOAD_LOOSE_COMMIT,
            |e: &LoadLooseCommitEff| minicbor::to_vec(&cbor_bridge::IdAndCommitDigest {
                id: e.0,
                digest: e.1
            })
            .unwrap_or_default()
        );
        try_effect!(
            ListCommitDigestsEff,
            abi::EFFECT_TAG_LIST_COMMIT_DIGESTS,
            |e: &ListCommitDigestsEff| cbor_bridge::encode_sedimentree_id(&e.0).unwrap_or_default()
        );
        try_effect!(
            LoadLooseCommitsEff,
            abi::EFFECT_TAG_LOAD_LOOSE_COMMITS,
            |e: &LoadLooseCommitsEff| cbor_bridge::encode_sedimentree_id(&e.0).unwrap_or_default()
        );
        try_effect!(
            DeleteLooseCommitEff,
            abi::EFFECT_TAG_DELETE_LOOSE_COMMIT,
            |e: &DeleteLooseCommitEff| minicbor::to_vec(&cbor_bridge::IdAndCommitDigest {
                id: e.0,
                digest: e.1
            })
            .unwrap_or_default()
        );
        try_effect!(
            DeleteLooseCommitsEff,
            abi::EFFECT_TAG_DELETE_LOOSE_COMMITS,
            |e: &DeleteLooseCommitsEff| cbor_bridge::encode_sedimentree_id(&e.0)
                .unwrap_or_default()
        );

        try_effect!(
            SaveFragmentEff,
            abi::EFFECT_TAG_SAVE_FRAGMENT,
            |e: &SaveFragmentEff| minicbor::to_vec(&cbor_bridge::IdAndSignedFragment {
                id: e.0,
                fragment: e.1.clone()
            })
            .unwrap_or_default()
        );
        try_effect!(
            LoadFragmentEff,
            abi::EFFECT_TAG_LOAD_FRAGMENT,
            |e: &LoadFragmentEff| minicbor::to_vec(&cbor_bridge::IdAndFragmentDigest {
                id: e.0,
                digest: e.1
            })
            .unwrap_or_default()
        );
        try_effect!(
            ListFragmentDigestsEff,
            abi::EFFECT_TAG_LIST_FRAGMENT_DIGESTS,
            |e: &ListFragmentDigestsEff| cbor_bridge::encode_sedimentree_id(&e.0)
                .unwrap_or_default()
        );
        try_effect!(
            LoadFragmentsEff,
            abi::EFFECT_TAG_LOAD_FRAGMENTS,
            |e: &LoadFragmentsEff| cbor_bridge::encode_sedimentree_id(&e.0).unwrap_or_default()
        );
        try_effect!(
            DeleteFragmentEff,
            abi::EFFECT_TAG_DELETE_FRAGMENT,
            |e: &DeleteFragmentEff| minicbor::to_vec(&cbor_bridge::IdAndFragmentDigest {
                id: e.0,
                digest: e.1
            })
            .unwrap_or_default()
        );
        try_effect!(
            DeleteFragmentsEff,
            abi::EFFECT_TAG_DELETE_FRAGMENTS,
            |e: &DeleteFragmentsEff| cbor_bridge::encode_sedimentree_id(&e.0).unwrap_or_default()
        );

        try_effect!(SaveBlobEff, abi::EFFECT_TAG_SAVE_BLOB, |e: &SaveBlobEff| {
            cbor_bridge::encode_blob(&e.0).unwrap_or_default()
        });
        try_effect!(LoadBlobEff, abi::EFFECT_TAG_LOAD_BLOB, |e: &LoadBlobEff| {
            cbor_bridge::encode_blob_digest(&e.0).unwrap_or_default()
        });
        try_effect!(
            LoadBlobsEff,
            abi::EFFECT_TAG_LOAD_BLOBS,
            |e: &LoadBlobsEff| cbor_bridge::encode_blob_digest_slice(&e.0).unwrap_or_default()
        );
        try_effect!(
            DeleteBlobEff,
            abi::EFFECT_TAG_DELETE_BLOB,
            |e: &DeleteBlobEff| cbor_bridge::encode_blob_digest(&e.0).unwrap_or_default()
        );

        // Unknown effect type
        self.current_tag = abi::EFFECT_TAG_COMPLETE;
        self.current_data = Vec::new();
    }

    fn provide_response_bytes(&mut self, data: &[u8]) -> bool {
        let response: Box<dyn Any> = match self.current_tag {
            t if matches!(
                t,
                abi::EFFECT_TAG_SAVE_SEDIMENTREE_ID
                    | abi::EFFECT_TAG_DELETE_SEDIMENTREE_ID
                    | abi::EFFECT_TAG_DELETE_LOOSE_COMMIT
                    | abi::EFFECT_TAG_DELETE_LOOSE_COMMITS
                    | abi::EFFECT_TAG_DELETE_FRAGMENT
                    | abi::EFFECT_TAG_DELETE_FRAGMENTS
                    | abi::EFFECT_TAG_DELETE_BLOB
            ) =>
            {
                Box::new(UnitResp)
            }

            abi::EFFECT_TAG_LOAD_ALL_SEDIMENTREE_IDS => {
                let ids = cbor_bridge::decode_sedimentree_id_set(data).unwrap_or_default();
                Box::new(SedimentreeIdSetResp(ids))
            }
            abi::EFFECT_TAG_SAVE_LOOSE_COMMIT => {
                let d = cbor_bridge::decode_commit_digest(data)
                    .unwrap_or_else(|_| sedimentree_core::digest::Digest::from_bytes([0; 32]));
                Box::new(DigestResp(d))
            }
            abi::EFFECT_TAG_LOAD_LOOSE_COMMIT => {
                let c = cbor_bridge::decode_optional_signed_commit(data).unwrap_or(None);
                Box::new(OptionalSignedCommitResp(c))
            }
            abi::EFFECT_TAG_LIST_COMMIT_DIGESTS => {
                let d = cbor_bridge::decode_commit_digest_set(data).unwrap_or_default();
                Box::new(CommitDigestSetResp(d))
            }
            abi::EFFECT_TAG_LOAD_LOOSE_COMMITS => {
                let p = cbor_bridge::decode_commit_pairs(data).unwrap_or_default();
                Box::new(CommitPairsResp(p))
            }
            abi::EFFECT_TAG_SAVE_FRAGMENT => {
                let d = cbor_bridge::decode_fragment_digest(data)
                    .unwrap_or_else(|_| sedimentree_core::digest::Digest::from_bytes([0; 32]));
                Box::new(DigestResp(d))
            }
            abi::EFFECT_TAG_LOAD_FRAGMENT => {
                let f = cbor_bridge::decode_optional_signed_fragment(data).unwrap_or(None);
                Box::new(OptionalSignedFragmentResp(f))
            }
            abi::EFFECT_TAG_LIST_FRAGMENT_DIGESTS => {
                let d = cbor_bridge::decode_fragment_digest_set(data).unwrap_or_default();
                Box::new(FragmentDigestSetResp(d))
            }
            abi::EFFECT_TAG_LOAD_FRAGMENTS => {
                let p = cbor_bridge::decode_fragment_pairs(data).unwrap_or_default();
                Box::new(FragmentPairsResp(p))
            }
            abi::EFFECT_TAG_SAVE_BLOB => {
                let d = cbor_bridge::decode_blob_digest(data)
                    .unwrap_or_else(|_| sedimentree_core::digest::Digest::from_bytes([0; 32]));
                Box::new(DigestResp(d))
            }
            abi::EFFECT_TAG_LOAD_BLOB => {
                let b = cbor_bridge::decode_optional_blob(data).unwrap_or(None);
                Box::new(OptionalBlobResp(b))
            }
            abi::EFFECT_TAG_LOAD_BLOBS => {
                let p = cbor_bridge::decode_blob_digest_pairs(data).unwrap_or_default();
                Box::new(BlobPairsResp(p))
            }
            _ => Box::new(UnitResp),
        };

        self.slot.provide_response(response);
        self.poll()
    }
}

// ==================== C ABI ====================

/// Helper: decode args, build a DrivenFuture that emits effects and
/// encodes the final result as CBOR bytes.
///
/// Wraps the body in `catch_unwind` — returns null on panic (e.g., bad CBOR).
macro_rules! driven_start {
    ($name:ident, |$bytes:ident| $body:expr) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $name(ptr: *const u8, len: usize) -> DriverHandle {
            match panic::catch_unwind(AssertUnwindSafe(|| {
                let $bytes = if ptr.is_null() || len == 0 {
                    &[][..]
                } else {
                    unsafe { std::slice::from_raw_parts(ptr, len) }
                };
                let future: DrivenFuture<'static, Result<Vec<u8>, DrivenError>> = $body;
                let mut driver = DrivenDriver::new(future);
                driver.poll();
                Box::into_raw(Box::new(driver))
            })) {
                Ok(handle) => handle,
                Err(_) => std::ptr::null_mut(),
            }
        }
    };
}

driven_start!(driven_start_save_sedimentree_id, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp = emit::<SaveSedimentreeIdEff, UnitResp>(SaveSedimentreeIdEff(id)).await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_delete_sedimentree_id, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp = emit::<DeleteSedimentreeIdEff, UnitResp>(DeleteSedimentreeIdEff(id)).await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_load_all_sedimentree_ids, |_bytes| {
    DrivenFuture::new(Box::pin(async move {
        let SedimentreeIdSetResp(ids) =
            emit::<LoadAllSedimentreeIdsEff, SedimentreeIdSetResp>(LoadAllSedimentreeIdsEff)
                .await?;
        cbor_bridge::encode_sedimentree_id_set(&ids).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_save_loose_commit, |bytes| {
    let args: cbor_bridge::IdAndSignedCommit = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let DigestResp(d) = emit::<
            SaveLooseCommitEff,
            DigestResp<sedimentree_core::loose_commit::LooseCommit>,
        >(SaveLooseCommitEff(args.id, args.commit))
        .await?;
        cbor_bridge::encode_commit_digest(&d).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_loose_commit, |bytes| {
    let args: cbor_bridge::IdAndCommitDigest = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let OptionalSignedCommitResp(c) = emit::<LoadLooseCommitEff, OptionalSignedCommitResp>(
            LoadLooseCommitEff(args.id, args.digest),
        )
        .await?;
        cbor_bridge::encode_optional_signed_commit(&c).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_list_commit_digests, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let CommitDigestSetResp(d) =
            emit::<ListCommitDigestsEff, CommitDigestSetResp>(ListCommitDigestsEff(id)).await?;
        cbor_bridge::encode_commit_digest_set(&d).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_loose_commits, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let CommitPairsResp(p) =
            emit::<LoadLooseCommitsEff, CommitPairsResp>(LoadLooseCommitsEff(id)).await?;
        cbor_bridge::encode_commit_pairs(&p).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_delete_loose_commit, |bytes| {
    let args: cbor_bridge::IdAndCommitDigest = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp =
            emit::<DeleteLooseCommitEff, UnitResp>(DeleteLooseCommitEff(args.id, args.digest))
                .await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_delete_loose_commits, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp = emit::<DeleteLooseCommitsEff, UnitResp>(DeleteLooseCommitsEff(id)).await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_save_fragment, |bytes| {
    let args: cbor_bridge::IdAndSignedFragment = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let DigestResp(d) =
            emit::<SaveFragmentEff, DigestResp<sedimentree_core::fragment::Fragment>>(
                SaveFragmentEff(args.id, args.fragment),
            )
            .await?;
        cbor_bridge::encode_fragment_digest(&d).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_fragment, |bytes| {
    let args: cbor_bridge::IdAndFragmentDigest = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let OptionalSignedFragmentResp(f) = emit::<LoadFragmentEff, OptionalSignedFragmentResp>(
            LoadFragmentEff(args.id, args.digest),
        )
        .await?;
        cbor_bridge::encode_optional_signed_fragment(&f).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_list_fragment_digests, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let FragmentDigestSetResp(d) =
            emit::<ListFragmentDigestsEff, FragmentDigestSetResp>(ListFragmentDigestsEff(id))
                .await?;
        cbor_bridge::encode_fragment_digest_set(&d).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_fragments, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let FragmentPairsResp(p) =
            emit::<LoadFragmentsEff, FragmentPairsResp>(LoadFragmentsEff(id)).await?;
        cbor_bridge::encode_fragment_pairs(&p).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_delete_fragment, |bytes| {
    let args: cbor_bridge::IdAndFragmentDigest = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp =
            emit::<DeleteFragmentEff, UnitResp>(DeleteFragmentEff(args.id, args.digest)).await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_delete_fragments, |bytes| {
    let id = cbor_bridge::decode_sedimentree_id(bytes).expect("decode id");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp = emit::<DeleteFragmentsEff, UnitResp>(DeleteFragmentsEff(id)).await?;
        Ok(Vec::new())
    }))
});

driven_start!(driven_start_save_blob, |bytes| {
    let blob = cbor_bridge::decode_blob(bytes).expect("decode blob");
    DrivenFuture::new(Box::pin(async move {
        let DigestResp(d) =
            emit::<SaveBlobEff, DigestResp<sedimentree_core::blob::Blob>>(SaveBlobEff(blob))
                .await?;
        cbor_bridge::encode_blob_digest(&d).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_blob, |bytes| {
    let digest = cbor_bridge::decode_blob_digest(bytes).expect("decode digest");
    DrivenFuture::new(Box::pin(async move {
        let OptionalBlobResp(b) =
            emit::<LoadBlobEff, OptionalBlobResp>(LoadBlobEff(digest)).await?;
        cbor_bridge::encode_optional_blob(&b).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_load_blobs, |bytes| {
    let digests = cbor_bridge::decode_blob_digest_slice(bytes).expect("decode digests");
    DrivenFuture::new(Box::pin(async move {
        let BlobPairsResp(p) = emit::<LoadBlobsEff, BlobPairsResp>(LoadBlobsEff(digests)).await?;
        cbor_bridge::encode_blob_digest_pairs(&p).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_delete_blob, |bytes| {
    let digest = cbor_bridge::decode_blob_digest(bytes).expect("decode digest");
    DrivenFuture::new(Box::pin(async move {
        let UnitResp = emit::<DeleteBlobEff, UnitResp>(DeleteBlobEff(digest)).await?;
        Ok(Vec::new())
    }))
});

// Convenience: multi-step (this is where Driven shines)
driven_start!(driven_start_save_commit_with_blob, |bytes| {
    let args: cbor_bridge::CommitWithBlobArgs = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        // Step 1: save commit
        let DigestResp(_cd) = emit::<
            SaveLooseCommitEff,
            DigestResp<sedimentree_core::loose_commit::LooseCommit>,
        >(SaveLooseCommitEff(args.id, args.commit))
        .await?;
        // Step 2: save blob
        let DigestResp(bd) =
            emit::<SaveBlobEff, DigestResp<sedimentree_core::blob::Blob>>(SaveBlobEff(args.blob))
                .await?;
        cbor_bridge::encode_blob_digest(&bd).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_save_fragment_with_blob, |bytes| {
    let args: cbor_bridge::FragmentWithBlobArgs = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let DigestResp(_fd) = emit::<
            SaveFragmentEff,
            DigestResp<sedimentree_core::fragment::Fragment>,
        >(SaveFragmentEff(args.id, args.fragment))
        .await?;
        let DigestResp(bd) =
            emit::<SaveBlobEff, DigestResp<sedimentree_core::blob::Blob>>(SaveBlobEff(args.blob))
                .await?;
        cbor_bridge::encode_blob_digest(&bd).map_err(|e| DrivenError(e.to_string()))
    }))
});

driven_start!(driven_start_save_batch, |bytes| {
    let args: cbor_bridge::SaveBatchArgs = minicbor::decode(bytes).expect("decode args");
    DrivenFuture::new(Box::pin(async move {
        let mut commit_digests = Vec::new();
        let mut fragment_digests = Vec::new();

        for pair in args.commits {
            let DigestResp(cd) = emit::<
                SaveLooseCommitEff,
                DigestResp<sedimentree_core::loose_commit::LooseCommit>,
            >(SaveLooseCommitEff(args.id, pair.commit))
            .await?;
            commit_digests.push(cd);
            let DigestResp(_bd) = emit::<SaveBlobEff, DigestResp<sedimentree_core::blob::Blob>>(
                SaveBlobEff(pair.blob),
            )
            .await?;
        }

        for pair in args.fragments {
            let DigestResp(fd) = emit::<
                SaveFragmentEff,
                DigestResp<sedimentree_core::fragment::Fragment>,
            >(SaveFragmentEff(args.id, pair.fragment))
            .await?;
            fragment_digests.push(fd);
            let DigestResp(_bd) = emit::<SaveBlobEff, DigestResp<sedimentree_core::blob::Blob>>(
                SaveBlobEff(pair.blob),
            )
            .await?;
        }

        let result = cbor_bridge::BatchResultWire {
            commit_digests,
            fragment_digests,
        };
        minicbor::to_vec(&result).map_err(|e| DrivenError(e.to_string()))
    }))
});

// ==================== Driver protocol (same shape as sans-I/O) ====================

#[unsafe(no_mangle)]
pub extern "C" fn driven_next_effect(handle: DriverHandle) -> FfiEffect {
    if handle.is_null() {
        return FfiEffect::complete();
    }
    let driver = unsafe { &*handle };
    if driver.result.is_some() || driver.future.is_none() {
        return FfiEffect::complete();
    }
    FfiEffect::new(driver.current_tag, driver.current_data.clone())
}

#[unsafe(no_mangle)]
pub extern "C" fn driven_provide_response(
    handle: DriverHandle,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    if handle.is_null() {
        return 1;
    }
    // catch_unwind guards against panics from downcast mismatches
    // or protocol violations inside the poll chain.
    match panic::catch_unwind(AssertUnwindSafe(|| {
        let driver = unsafe { &mut *handle };
        let data = if data_ptr.is_null() || data_len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(data_ptr, data_len) }
        };
        let complete = driver.provide_response_bytes(data);
        i32::from(complete)
    })) {
        Ok(v) => v,
        Err(_) => {
            // On panic, mark the driver as complete with an error so
            // driven_finish can report it instead of hanging.
            let driver = unsafe { &mut *handle };
            driver.future = None;
            driver.result = Some(Err(DrivenError("panic during provide_response".into())));
            1 // complete
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn driven_is_complete(handle: DriverHandle) -> i32 {
    if handle.is_null() {
        return 1;
    }
    let driver = unsafe { &*handle };
    i32::from(driver.result.is_some())
}

/// Extract the final result and free the driver.
///
/// Always consumes the driver handle — the handle is invalidated after
/// this call regardless of whether the operation was complete.
///
/// Returns an `FfiResult`:
/// - On success: CBOR-encoded result data.
/// - On driven error: error with status -3.
/// - If not complete: error with status -5.
/// - If handle is null: error with status -4.
///
/// # Safety
///
/// `handle` must have been returned by a `driven_start_*` function
/// and must not have been consumed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn driven_finish(handle: DriverHandle) -> FfiResult {
    if handle.is_null() {
        return FfiResult::err(-4, "null driver handle");
    }
    let driver = unsafe { *Box::from_raw(handle) };
    match driver.result {
        Some(Ok(data)) => FfiResult::ok(data),
        Some(Err(e)) => FfiResult::err(-3, &e.to_string()),
        None => FfiResult::err(-5, "driver not complete"),
    }
}
