//! A fragment checkpoint: the first 12 bytes of a commit identifier.

use alloc::{string::ToString, vec::Vec};
use sedimentree_core::fragment::checkpoint::Checkpoint;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use crate::commit_id::WasmCommitId;

/// A 12-byte commit identifier prefix, not a full commit identifier or a hash.
///
/// Byte conversions copy their input/output. Each returned wrapper is owned by
/// the caller and should be freed when no longer needed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_copy_implementations)]
#[wasm_bindgen(js_name = Checkpoint)]
pub struct WasmCheckpoint(Checkpoint);

#[wasm_refgen(js_ref = JsCheckpoint)]
#[wasm_bindgen(js_class = Checkpoint)]
impl WasmCheckpoint {
    /// Copy exactly 12 bytes into a checkpoint.
    ///
    /// # Errors
    ///
    /// Throws an `InvalidCheckpoint` error if the input is not exactly 12 bytes.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WasmInvalidCheckpoint> {
        let bytes = bytes
            .try_into()
            .map_err(|_| WasmInvalidCheckpoint::WrongLength(bytes.len()))?;
        Ok(Self(Checkpoint::from_bytes(bytes)))
    }

    /// Copy the first 12 bytes of a full commit identifier without consuming it.
    /// This truncates; it does not hash or change byte order.
    #[must_use]
    #[wasm_bindgen(js_name = fromCommitId)]
    pub fn from_commit_id(id: &WasmCommitId) -> Self {
        Self(Checkpoint::new(id.into()))
    }

    /// Return an owned copy of the checkpoint bytes, not a view into Wasm memory.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}

impl From<Checkpoint> for WasmCheckpoint {
    fn from(checkpoint: Checkpoint) -> Self {
        Self(checkpoint)
    }
}

impl From<WasmCheckpoint> for Checkpoint {
    fn from(checkpoint: WasmCheckpoint) -> Self {
        checkpoint.0
    }
}

impl From<&WasmCheckpoint> for Checkpoint {
    fn from(checkpoint: &WasmCheckpoint) -> Self {
        checkpoint.0
    }
}

/// An error indicating an invalid checkpoint byte length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WasmInvalidCheckpoint {
    /// Wrong byte length (expected 12).
    #[error("expected 12 bytes, got {0}")]
    WrongLength(usize),
}

impl From<WasmInvalidCheckpoint> for JsValue {
    fn from(err: WasmInvalidCheckpoint) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidCheckpoint");
        err.into()
    }
}
