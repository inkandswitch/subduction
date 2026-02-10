//! Approach A: Tokio runtime behind synchronous C ABI.
//!
//! Rust owns a multi-threaded tokio runtime. Each FFI call spawns the
//! async work onto the runtime's threadpool and waits on a oneshot
//! channel. This avoids nested-runtime panics and lets concurrent FFI
//! calls from multiple host threads parallelize on tokio's pool.

use std::{panic, sync::Arc};

use ffi_common::{abi::FfiResult, cbor_bridge, error::FfiError};
use future_form::Sendable;
use subduction_core::storage::{memory::MemoryStorage, traits::Storage};

/// Opaque handle wrapping the tokio runtime + storage.
///
/// Storage is `Arc`-wrapped so it can be moved into spawned tasks.
struct TokioStorageInner {
    runtime: tokio::runtime::Runtime,
    storage: Arc<MemoryStorage>,
}

impl TokioStorageInner {
    /// Spawn an async operation onto the runtime and block until it completes.
    ///
    /// Uses spawn + oneshot rather than `block_on` so that:
    /// - Work runs on the tokio threadpool (parallelizes with other calls)
    /// - Safe to call from any thread (no nested-runtime panic)
    /// - Multiple concurrent FFI calls from Go/Java actually parallelize
    fn run<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.runtime.spawn(async move {
            let _ = tx.send(f.await);
        });
        rx.blocking_recv()
            .expect("oneshot sender dropped — runtime shut down?")
    }
}

/// Create a new storage instance backed by a tokio runtime.
#[unsafe(no_mangle)]
pub extern "C" fn storage_new() -> *mut std::ffi::c_void {
    let inner = match tokio::runtime::Runtime::new() {
        Ok(runtime) => Box::new(TokioStorageInner {
            runtime,
            storage: Arc::new(MemoryStorage::new()),
        }),
        Err(_) => return std::ptr::null_mut(),
    };
    Box::into_raw(inner).cast()
}

/// Free a storage instance previously created by `storage_new`.
///
/// # Safety
///
/// `handle` must have been returned by `storage_new` and not freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn storage_free(handle: *mut std::ffi::c_void) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle.cast::<TokioStorageInner>()));
        }
    }
}

/// Reconstruct a byte slice from FFI pointers.
///
/// Returns an empty slice if `ptr` is null or `len` is 0.
///
/// # Safety
///
/// If `ptr` is non-null, it must be valid for `len` bytes for
/// the lifetime `'a`.
unsafe fn slice_from_ffi<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

/// Run a fallible FFI operation with panic catching, returning CBOR data.
///
/// Performs the null check and dereference of the opaque handle internally,
/// so the `&TokioStorageInner` borrow is scoped to the closure — no
/// `'static` lie needed.
fn run_ffi(
    handle: *mut std::ffi::c_void,
    f: impl FnOnce(&TokioStorageInner) -> Result<Vec<u8>, FfiError> + panic::UnwindSafe,
) -> FfiResult {
    match panic::catch_unwind(|| {
        if handle.is_null() {
            return Err(FfiError::NullPointer);
        }
        // Safety: handle was produced by storage_new (Box::into_raw) and
        // the reference does not escape this closure.
        let inner = unsafe { &*handle.cast::<TokioStorageInner>() };
        f(inner)
    }) {
        Ok(Ok(data)) => FfiResult::ok(data),
        Ok(Err(e)) => e.into_ffi_result(),
        Err(_) => FfiResult::err(-99, "rust panic"),
    }
}

/// Run a fallible FFI operation with panic catching, returning no data.
fn run_ffi_unit(
    handle: *mut std::ffi::c_void,
    f: impl FnOnce(&TokioStorageInner) -> Result<(), FfiError> + panic::UnwindSafe,
) -> FfiResult {
    match panic::catch_unwind(|| {
        if handle.is_null() {
            return Err(FfiError::NullPointer);
        }
        let inner = unsafe { &*handle.cast::<TokioStorageInner>() };
        f(inner)
    }) {
        Ok(Ok(())) => FfiResult::ok_empty(),
        Ok(Err(e)) => e.into_ffi_result(),
        Err(_) => FfiResult::err(-99, "rust panic"),
    }
}

// ==================== SedimentreeId operations ====================

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_sedimentree_id(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move { Storage::<Sendable>::save_sedimentree_id(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_sedimentree_id(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move { Storage::<Sendable>::delete_sedimentree_id(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_all_sedimentree_ids(handle: *mut std::ffi::c_void) -> FfiResult {
    run_ffi(handle, |inner| {
        let s = Arc::clone(&inner.storage);
        let ids = inner
            .run(async move { Storage::<Sendable>::load_all_sedimentree_ids(&*s).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_sedimentree_id_set(&ids)
    })
}

// ==================== Loose commit operations ====================

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_loose_commit(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::IdAndSignedCommit =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let digest =
            inner
                .run(async move {
                    Storage::<Sendable>::save_loose_commit(&*s, args.id, args.commit).await
                })
                .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_commit_digest(&digest)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_loose_commit(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::IdAndCommitDigest =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let commit =
            inner
                .run(async move {
                    Storage::<Sendable>::load_loose_commit(&*s, args.id, args.digest).await
                })
                .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_optional_signed_commit(&commit)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_list_commit_digests(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        let digests = inner
            .run(async move { Storage::<Sendable>::list_commit_digests(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_commit_digest_set(&digests)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_loose_commits(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        let pairs = inner
            .run(async move { Storage::<Sendable>::load_loose_commits(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_commit_pairs(&pairs)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_loose_commit(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let args: cbor_bridge::IdAndCommitDigest =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move {
                Storage::<Sendable>::delete_loose_commit(&*s, args.id, args.digest).await
            })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_loose_commits(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move { Storage::<Sendable>::delete_loose_commits(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

// ==================== Fragment operations ====================

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_fragment(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::IdAndSignedFragment =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let digest =
            inner
                .run(async move {
                    Storage::<Sendable>::save_fragment(&*s, args.id, args.fragment).await
                })
                .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_fragment_digest(&digest)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_fragment(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::IdAndFragmentDigest =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let fragment = inner
            .run(async move { Storage::<Sendable>::load_fragment(&*s, args.id, args.digest).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_optional_signed_fragment(&fragment)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_list_fragment_digests(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        let digests = inner
            .run(async move { Storage::<Sendable>::list_fragment_digests(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_fragment_digest_set(&digests)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_fragments(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        let pairs = inner
            .run(async move { Storage::<Sendable>::load_fragments(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_fragment_pairs(&pairs)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_fragment(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let args: cbor_bridge::IdAndFragmentDigest =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move {
                Storage::<Sendable>::delete_fragment(&*s, args.id, args.digest).await
            })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_fragments(
    handle: *mut std::ffi::c_void,
    id_ptr: *const u8,
    id_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let id = cbor_bridge::decode_sedimentree_id(unsafe { slice_from_ffi(id_ptr, id_len) })?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move { Storage::<Sendable>::delete_fragments(&*s, id).await })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

// ==================== Blob operations ====================

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_blob(
    handle: *mut std::ffi::c_void,
    blob_ptr: *const u8,
    blob_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let blob = cbor_bridge::decode_blob(unsafe { slice_from_ffi(blob_ptr, blob_len) })?;
        let s = Arc::clone(&inner.storage);
        let digest = inner
            .run(async move { Storage::<Sendable>::save_blob(&*s, blob).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_blob_digest(&digest)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_blob(
    handle: *mut std::ffi::c_void,
    digest_ptr: *const u8,
    digest_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let digest =
            cbor_bridge::decode_blob_digest(unsafe { slice_from_ffi(digest_ptr, digest_len) })?;
        let s = Arc::clone(&inner.storage);
        let blob = inner
            .run(async move { Storage::<Sendable>::load_blob(&*s, digest).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_optional_blob(&blob)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_load_blobs(
    handle: *mut std::ffi::c_void,
    digests_ptr: *const u8,
    digests_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let digests = cbor_bridge::decode_blob_digest_slice(unsafe {
            slice_from_ffi(digests_ptr, digests_len)
        })?;
        let s = Arc::clone(&inner.storage);
        let pairs = inner
            .run(async move { Storage::<Sendable>::load_blobs(&*s, &digests).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_blob_digest_pairs(&pairs)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_delete_blob(
    handle: *mut std::ffi::c_void,
    digest_ptr: *const u8,
    digest_len: usize,
) -> FfiResult {
    run_ffi_unit(handle, |inner| {
        let digest =
            cbor_bridge::decode_blob_digest(unsafe { slice_from_ffi(digest_ptr, digest_len) })?;
        let s = Arc::clone(&inner.storage);
        inner
            .run(async move { Storage::<Sendable>::delete_blob(&*s, digest).await })
            .map_err(|e| FfiError::Storage(e.to_string()))
    })
}

// ==================== Convenience operations ====================

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_commit_with_blob(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::CommitWithBlobArgs =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let digest = inner
            .run(async move {
                Storage::<Sendable>::save_commit_with_blob(&*s, args.id, args.commit, args.blob)
                    .await
            })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_blob_digest(&digest)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_fragment_with_blob(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::FragmentWithBlobArgs =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;
        let s = Arc::clone(&inner.storage);
        let digest = inner
            .run(async move {
                Storage::<Sendable>::save_fragment_with_blob(&*s, args.id, args.fragment, args.blob)
                    .await
            })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_blob_digest(&digest)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn storage_save_batch(
    handle: *mut std::ffi::c_void,
    args_ptr: *const u8,
    args_len: usize,
) -> FfiResult {
    run_ffi(handle, |inner| {
        let args: cbor_bridge::SaveBatchArgs =
            minicbor::decode(unsafe { slice_from_ffi(args_ptr, args_len) })
                .map_err(FfiError::from)?;

        let commits: Vec<_> = args
            .commits
            .into_iter()
            .map(|p| (p.commit, p.blob))
            .collect();
        let fragments: Vec<_> = args
            .fragments
            .into_iter()
            .map(|p| (p.fragment, p.blob))
            .collect();

        let s = Arc::clone(&inner.storage);
        let id = args.id;
        let result = inner
            .run(async move { Storage::<Sendable>::save_batch(&*s, id, commits, fragments).await })
            .map_err(|e| FfiError::Storage(e.to_string()))?;
        cbor_bridge::encode_batch_result(&result)
    })
}
