//! Fragment-related wrappers.

use alloc::vec::Vec;
use sedimentree_core::collections::{Map, Set};

use sedimentree_core::{commit::FragmentState, crypto::digest::Digest, loose_commit::LooseCommit};
use sedimentree_wasm::{digest::WasmDigest, fragment::WasmFragment, loose_commit::WasmBlobMeta};
use wasm_bindgen::prelude::*;

/// The state of a fragment while being built.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = FragmentState)]
pub struct WasmFragmentState(pub(crate) FragmentState<Set<Digest<LooseCommit>>>);

#[wasm_bindgen(js_class = FragmentState)]
impl WasmFragmentState {
    /// The "newest" element of the fragment.
    ///
    /// This digest provides a stable point from which
    /// the rest of the fragment is built.
    #[must_use]
    pub fn head_digest(&self) -> WasmDigest {
        self.0.head_digest().into()
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_digest`
    /// and the `boundary` (not including the boundary elements).
    #[must_use]
    pub fn members(&self) -> Vec<WasmDigest> {
        self.0
            .members()
            .iter()
            .copied()
            .map(WasmDigest::from)
            .collect()
    }

    /// The checkpoints of the fragment.
    ///
    /// These are all of the [`Digest`]s that match a valid level
    /// below the target, so that it is possible to know which other fragments
    /// this one covers.
    #[must_use]
    pub fn checkpoints(&self) -> Vec<WasmDigest> {
        self.0
            .checkpoints()
            .iter()
            .copied()
            .map(WasmDigest::from)
            .collect()
    }

    /// The boundary from which the next set of fragments would be built.
    #[must_use]
    pub fn boundary(&self) -> WasmBoundary {
        let boundary = self.0.boundary();

        let mut map = Map::new();
        for (key, value) in boundary {
            let wasm_key: WasmDigest = (*key).into();
            let wasm_value: Set<WasmDigest> = value.iter().copied().map(WasmDigest::from).collect();
            map.insert(wasm_key, wasm_value);
        }
        WasmBoundary(map)
    }

    /// Convert the fragment state into a fragment.
    #[must_use]
    #[wasm_bindgen(js_name = intoFragment)]
    pub fn into_fragment(&self, blob_meta: &WasmBlobMeta) -> WasmFragment {
        self.0.clone().to_fragment(blob_meta.clone().into()).into()
    }
}

impl From<FragmentState<Set<Digest<LooseCommit>>>> for WasmFragmentState {
    fn from(state: FragmentState<Set<Digest<LooseCommit>>>) -> Self {
        WasmFragmentState(state)
    }
}

/// The boundary of a fragment.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Boundary)]
pub struct WasmBoundary(Map<WasmDigest, Set<WasmDigest>>);

#[wasm_bindgen(js_class = Boundary)]
impl WasmBoundary {
    /// Get the set of digests for a given key in the boundary.
    #[must_use]
    pub fn get(&self, key: &WasmDigest) -> Option<Vec<WasmDigest>> {
        self.0.get(key).map(|set| set.iter().cloned().collect())
    }

    /// Get all keys in the boundary.
    #[must_use]
    pub fn keys(&self) -> Vec<WasmDigest> {
        self.0.keys().cloned().collect()
    }
}

impl From<Map<WasmDigest, Set<WasmDigest>>> for WasmBoundary {
    fn from(boundary: Map<WasmDigest, Set<WasmDigest>>) -> Self {
        WasmBoundary(boundary)
    }
}

/// A store for fragment states.
#[derive(Debug, Default, Clone)]
#[wasm_bindgen(js_name = FragmentStateStore)]
pub struct WasmFragmentStateStore(
    pub(crate) Map<Digest<LooseCommit>, FragmentState<Set<Digest<LooseCommit>>>>,
);

#[wasm_bindgen(js_class = FragmentStateStore)]
impl WasmFragmentStateStore {
    /// Create a new empty `WasmFragmentStateStore`.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self(Map::new())
    }

    /// Insert a fragment state into the store.
    pub fn insert(&mut self, digest: &WasmDigest, state: &WasmFragmentState) {
        self.0.insert(digest.clone().into(), state.0.clone());
    }

    /// Get a fragment state from the store.
    pub fn get(&self, digest: &WasmDigest) -> Option<WasmFragmentState> {
        self.0
            .get(&digest.clone().into())
            .cloned()
            .map(WasmFragmentState)
    }
}

#[allow(clippy::implicit_hasher)]
impl From<WasmFragmentStateStore>
    for Map<Digest<LooseCommit>, FragmentState<Set<Digest<LooseCommit>>>>
{
    fn from(store: WasmFragmentStateStore) -> Self {
        store.0
    }
}
