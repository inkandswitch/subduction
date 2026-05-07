//! Correct-by-construction inputs for the bulk-write methods on
//! [`WasmSubduction`](crate::subduction::WasmSubduction).
//!
//! Each entry pairs an unsigned payload ([`WasmLooseCommit`] or
//! [`WasmFragment`]) with its blob bytes. Pairing happens at construction
//! time, so the bulk-write methods never need to compare two parallel
//! arrays for length agreement — the type system enforces the invariant.
//!
//! These types are distinct from
//! [`WasmCommitWithBlob`](sedimentree_wasm::loose_commit::WasmCommitWithBlob)
//! and
//! [`WasmFragmentWithBlob`](sedimentree_wasm::fragment::WasmFragmentWithBlob),
//! which pair the *signed* (post-seal) wire form with a blob and are used by
//! storage adapters when surfacing already-signed data. The types here
//! carry the unsigned form because the local node will sign them itself
//! during the bulk-write call.

use alloc::vec::Vec;

use js_sys::Uint8Array;
use sedimentree_wasm::{fragment::WasmFragment, loose_commit::WasmLooseCommit};
use wasm_bindgen::prelude::*;

/// One unsigned commit + its blob, ready for bulk insertion.
///
/// Construct with `new CommitInput(looseCommit, blob)` from JavaScript;
/// pass arrays of these to
/// [`addBatch`](crate::subduction::WasmSubduction::add_batch) or
/// [`addCommitsBatch`](crate::subduction::WasmSubduction::add_commits_batch).
#[wasm_bindgen(js_name = CommitInput)]
#[derive(Debug, Clone)]
pub struct WasmCommitInput {
    commit: WasmLooseCommit,
    blob: Vec<u8>,
}

#[wasm_bindgen(js_class = CommitInput)]
impl WasmCommitInput {
    /// Pair an unsigned [`LooseCommit`](sedimentree_core::loose_commit::LooseCommit)
    /// with the bytes of its associated blob.
    ///
    /// `blob` accepts any `BufferSource`-like value from JavaScript
    /// (`Uint8Array`, `ArrayBuffer`, plain `number[]`); `wasm_bindgen` copies
    /// the bytes into a `Vec<u8>` owned by this struct.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(commit: WasmLooseCommit, blob: &[u8]) -> Self {
        Self {
            commit,
            blob: blob.to_vec(),
        }
    }

    /// A copy of the underlying unsigned commit.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn commit(&self) -> WasmLooseCommit {
        self.commit.clone()
    }

    /// A copy of the blob bytes.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn blob(&self) -> Uint8Array {
        Uint8Array::from(self.blob.as_slice())
    }
}

impl WasmCommitInput {
    /// Consume the input into the (`LooseCommit`, `Blob`) pair the
    /// `subduction_core` bulk-write paths expect.
    #[must_use]
    pub fn into_core(
        self,
    ) -> (
        sedimentree_core::loose_commit::LooseCommit,
        sedimentree_core::blob::Blob,
    ) {
        (
            sedimentree_core::loose_commit::LooseCommit::from(self.commit),
            sedimentree_core::blob::Blob::from(self.blob),
        )
    }
}

/// One unsigned fragment + its blob, ready for bulk insertion.
///
/// Construct with `new FragmentInput(fragment, blob)` from JavaScript;
/// pass arrays of these to
/// [`addBatch`](crate::subduction::WasmSubduction::add_batch) or
/// [`addFragmentsBatch`](crate::subduction::WasmSubduction::add_fragments_batch).
#[wasm_bindgen(js_name = FragmentInput)]
#[derive(Debug, Clone)]
pub struct WasmFragmentInput {
    fragment: WasmFragment,
    blob: Vec<u8>,
}

#[wasm_bindgen(js_class = FragmentInput)]
impl WasmFragmentInput {
    /// Pair an unsigned [`Fragment`](sedimentree_core::fragment::Fragment)
    /// with the bytes of its associated blob.
    ///
    /// `blob` accepts any `BufferSource`-like value from JavaScript
    /// (`Uint8Array`, `ArrayBuffer`, plain `number[]`); `wasm_bindgen` copies
    /// the bytes into a `Vec<u8>` owned by this struct.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(fragment: WasmFragment, blob: &[u8]) -> Self {
        Self {
            fragment,
            blob: blob.to_vec(),
        }
    }

    /// A copy of the underlying unsigned fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn fragment(&self) -> WasmFragment {
        self.fragment.clone()
    }

    /// A copy of the blob bytes.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn blob(&self) -> Uint8Array {
        Uint8Array::from(self.blob.as_slice())
    }
}

impl WasmFragmentInput {
    /// Consume the input into the (`Fragment`, `Blob`) pair the
    /// `subduction_core` bulk-write paths expect.
    #[must_use]
    pub fn into_core(
        self,
    ) -> (
        sedimentree_core::fragment::Fragment,
        sedimentree_core::blob::Blob,
    ) {
        (
            sedimentree_core::fragment::Fragment::from(self.fragment),
            sedimentree_core::blob::Blob::from(self.blob),
        )
    }
}
