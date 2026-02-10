//! C ABI types shared across all FFI approaches.
//!
//! All complex Subduction types cross the FFI boundary as CBOR-encoded
//! byte buffers. The host language doesn't need to understand Rust's type
//! layout — it passes opaque CBOR blobs via `FfiBytes`.

use std::ffi::CString;

/// A borrowed byte buffer passed _into_ Rust from the host.
///
/// The host owns the memory; Rust reads it during the call.
/// Never stored past the call boundary.
#[repr(C)]
pub struct FfiBytesRef {
    pub ptr: *const u8,
    pub len: usize,
}

impl FfiBytesRef {
    /// Reconstruct a slice from the C pointers.
    ///
    /// # Safety
    ///
    /// `ptr` must be valid for `len` bytes and must not be mutated
    /// during the lifetime of the returned slice.
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// An owned byte buffer returned _from_ Rust to the host.
///
/// Rust allocates the memory; the host must call `ffi_bytes_free` when done.
#[repr(C)]
pub struct FfiBytes {
    pub ptr: *mut u8,
    pub len: usize,
}

impl FfiBytes {
    pub fn empty() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
        }
    }

    pub fn from_vec(mut v: Vec<u8>) -> Self {
        v.shrink_to_fit();
        let ptr = v.as_mut_ptr();
        let len = v.len();
        std::mem::forget(v);
        Self { ptr, len }
    }
}

/// Free a byte buffer previously returned by Rust.
///
/// # Safety
///
/// `bytes.ptr` must have been allocated by Rust (via `FfiBytes::from_vec`),
/// and must not have been freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ffi_bytes_free(bytes: FfiBytes) {
    if !bytes.ptr.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(bytes.ptr, bytes.len, bytes.len));
        }
    }
}

/// Result of an FFI operation.
///
/// - `status == 0`: success. `data` contains the CBOR-encoded result.
/// - `status != 0`: error. `error_msg` contains a NUL-terminated string.
///
/// The caller must free `data` via `ffi_bytes_free` and `error_msg` via
/// `ffi_error_free`.
#[repr(C)]
pub struct FfiResult {
    pub status: i32,
    pub data: FfiBytes,
    pub error_msg: *mut i8,
}

impl FfiResult {
    pub fn ok(data: Vec<u8>) -> Self {
        Self {
            status: 0,
            data: FfiBytes::from_vec(data),
            error_msg: std::ptr::null_mut(),
        }
    }

    pub fn ok_empty() -> Self {
        Self {
            status: 0,
            data: FfiBytes::empty(),
            error_msg: std::ptr::null_mut(),
        }
    }

    pub fn err(code: i32, msg: &str) -> Self {
        let c_msg = CString::new(msg).unwrap_or_default();
        Self {
            status: code,
            data: FfiBytes::empty(),
            error_msg: c_msg.into_raw(),
        }
    }
}

/// Free an error message previously returned in an `FfiResult`.
///
/// # Safety
///
/// `msg` must have been allocated by Rust (via `CString::into_raw`),
/// and must not have been freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ffi_error_free(msg: *mut i8) {
    if !msg.is_null() {
        unsafe {
            drop(CString::from_raw(msg));
        }
    }
}

/// Effect descriptor for sans-I/O and Driven approaches.
///
/// Represents a request from the Rust state machine to the host.
/// The `tag` identifies which storage operation is needed.
/// The `data` contains CBOR-encoded arguments.
#[repr(C)]
pub struct FfiEffect {
    pub tag: u32,
    pub data: FfiBytes,
}

impl FfiEffect {
    pub fn new(tag: u32, data: Vec<u8>) -> Self {
        Self {
            tag,
            data: FfiBytes::from_vec(data),
        }
    }

    /// Sentinel: no more effects, operation is complete.
    pub fn complete() -> Self {
        Self {
            tag: EFFECT_TAG_COMPLETE,
            data: FfiBytes::empty(),
        }
    }
}

// Effect tag constants — shared between sans-I/O and Driven.
// These must match the Go/Java side.

pub const EFFECT_TAG_COMPLETE: u32 = 0;

// Sedimentree ID operations
pub const EFFECT_TAG_SAVE_SEDIMENTREE_ID: u32 = 1;
pub const EFFECT_TAG_DELETE_SEDIMENTREE_ID: u32 = 2;
pub const EFFECT_TAG_LOAD_ALL_SEDIMENTREE_IDS: u32 = 3;

// Loose commit operations
pub const EFFECT_TAG_SAVE_LOOSE_COMMIT: u32 = 10;
pub const EFFECT_TAG_LOAD_LOOSE_COMMIT: u32 = 11;
pub const EFFECT_TAG_LIST_COMMIT_DIGESTS: u32 = 12;
pub const EFFECT_TAG_LOAD_LOOSE_COMMITS: u32 = 13;
pub const EFFECT_TAG_DELETE_LOOSE_COMMIT: u32 = 14;
pub const EFFECT_TAG_DELETE_LOOSE_COMMITS: u32 = 15;

// Fragment operations
pub const EFFECT_TAG_SAVE_FRAGMENT: u32 = 20;
pub const EFFECT_TAG_LOAD_FRAGMENT: u32 = 21;
pub const EFFECT_TAG_LIST_FRAGMENT_DIGESTS: u32 = 22;
pub const EFFECT_TAG_LOAD_FRAGMENTS: u32 = 23;
pub const EFFECT_TAG_DELETE_FRAGMENT: u32 = 24;
pub const EFFECT_TAG_DELETE_FRAGMENTS: u32 = 25;

// Blob operations
pub const EFFECT_TAG_SAVE_BLOB: u32 = 30;
pub const EFFECT_TAG_LOAD_BLOB: u32 = 31;
pub const EFFECT_TAG_LOAD_BLOBS: u32 = 32;
pub const EFFECT_TAG_DELETE_BLOB: u32 = 33;

// Convenience operations (multi-step)
pub const EFFECT_TAG_SAVE_COMMIT_WITH_BLOB: u32 = 40;
pub const EFFECT_TAG_SAVE_FRAGMENT_WITH_BLOB: u32 = 41;
pub const EFFECT_TAG_SAVE_BATCH: u32 = 42;
