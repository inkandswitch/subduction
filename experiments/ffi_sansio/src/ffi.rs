//! C ABI for the sans-I/O driver.
//!
//! The host language drives the state machine through these functions:
//!
//! 1. `sansio_driver_new_*()` — create a driver for a specific operation
//! 2. `sansio_driver_next_effect()` — get the next I/O request
//! 3. `sansio_driver_provide_response()` — provide the I/O result
//! 4. `sansio_driver_is_complete()` — check if done
//! 5. `sansio_driver_finish()` — extract final result and free the driver
//!
//! `finish()` always consumes the handle. There is no separate `free()`.

use std::panic;

use ffi_common::abi::{FfiEffect, FfiResult};

use crate::{driver::StorageDriver, effect::StorageResponse};

/// Opaque driver handle.
type DriverHandle = *mut StorageDriver;

/// Reconstruct a byte slice from FFI pointers, with null guard.
///
/// Returns an empty slice if `ptr` is null or `len` is 0.
fn safe_slice<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

/// Wrap a constructor body in catch_unwind. Returns null on panic.
fn catch_new(f: impl FnOnce() -> DriverHandle + panic::UnwindSafe) -> DriverHandle {
    match panic::catch_unwind(f) {
        Ok(handle) => handle,
        Err(_) => std::ptr::null_mut(),
    }
}

fn box_driver(driver: StorageDriver) -> DriverHandle {
    Box::into_raw(Box::new(driver))
}

unsafe fn ref_driver<'a>(handle: DriverHandle) -> Option<&'a StorageDriver> {
    if handle.is_null() {
        None
    } else {
        Some(unsafe { &*handle })
    }
}

unsafe fn mut_driver<'a>(handle: DriverHandle) -> Option<&'a mut StorageDriver> {
    if handle.is_null() {
        None
    } else {
        Some(unsafe { &mut *handle })
    }
}

// ==================== Driver constructors ====================

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_sedimentree_id(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_save_sedimentree_id(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_sedimentree_id(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_sedimentree_id(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_all_sedimentree_ids() -> DriverHandle {
    catch_new(|| box_driver(StorageDriver::new_load_all_sedimentree_ids()))
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_loose_commit(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_save_loose_commit(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_loose_commit(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_load_loose_commit(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_list_commit_digests(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_list_commit_digests(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_loose_commits(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_load_loose_commits(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_loose_commit(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_loose_commit(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_loose_commits(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_loose_commits(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_fragment(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_save_fragment(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_fragment(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_load_fragment(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_list_fragment_digests(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_list_fragment_digests(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_fragments(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_load_fragments(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_fragment(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_fragment(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_fragments(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_fragments(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_blob(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| box_driver(StorageDriver::new_save_blob(safe_slice(ptr, len).to_vec())))
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_blob(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| box_driver(StorageDriver::new_load_blob(safe_slice(ptr, len).to_vec())))
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_load_blobs(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| box_driver(StorageDriver::new_load_blobs(safe_slice(ptr, len).to_vec())))
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_delete_blob(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_delete_blob(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_commit_with_blob(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_save_commit_with_blob(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_fragment_with_blob(
    ptr: *const u8,
    len: usize,
) -> DriverHandle {
    catch_new(|| {
        box_driver(StorageDriver::new_save_fragment_with_blob(
            safe_slice(ptr, len).to_vec(),
        ))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_new_save_batch(ptr: *const u8, len: usize) -> DriverHandle {
    catch_new(|| box_driver(StorageDriver::new_save_batch(safe_slice(ptr, len).to_vec())))
}

// ==================== Driver protocol ====================

/// Get the next effect to perform.
///
/// Returns an `FfiEffect` with `tag == EFFECT_TAG_COMPLETE` if the
/// operation is done (call `sansio_driver_finish`).
///
/// The caller must free the returned `FfiEffect.data` via `ffi_bytes_free`.
#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_next_effect(handle: DriverHandle) -> FfiEffect {
    let Some(driver) = (unsafe { ref_driver(handle) }) else {
        return FfiEffect::complete();
    };
    match driver.next_effect() {
        Some(effect) => effect.to_ffi(),
        None => FfiEffect::complete(),
    }
}

/// Provide the result of the last effect.
///
/// `data_ptr`/`data_len` contain the CBOR-encoded response. For
/// unit-returning operations, pass NULL/0.
///
/// Returns 1 if the operation is now complete, 0 if there are more effects.
#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_provide_response(
    handle: DriverHandle,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    let Some(driver) = (unsafe { mut_driver(handle) }) else {
        return 1;
    };
    let data = safe_slice(data_ptr, data_len).to_vec();
    let complete = driver.provide_response(StorageResponse { data });
    i32::from(complete)
}

/// Check if the operation is complete.
///
/// Returns 1 if complete, 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn sansio_driver_is_complete(handle: DriverHandle) -> i32 {
    let Some(driver) = (unsafe { ref_driver(handle) }) else {
        return 1;
    };
    i32::from(driver.is_complete())
}

/// Extract the final result and free the driver.
///
/// Always consumes the driver handle — the handle is invalidated after
/// this call regardless of whether the operation was complete.
///
/// Returns an `FfiResult`:
/// - On success: CBOR-encoded result data.
/// - If not complete: error with status -5.
/// - If handle is null: error with status -4.
///
/// # Safety
///
/// `handle` must have been returned by a `sansio_driver_new_*` function
/// and must not have been consumed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sansio_driver_finish(handle: DriverHandle) -> FfiResult {
    if handle.is_null() {
        return FfiResult::err(-4, "null driver handle");
    }
    let driver = unsafe { *Box::from_raw(handle) };
    match driver.finish() {
        Some(data) => FfiResult::ok(data),
        None => FfiResult::err(-5, "driver not complete"),
    }
}
