//! Fragment-related wrappers.

use alloc::vec::Vec;
use core::cell::RefCell;
use sedimentree_core::collections::{Map, Set};

use sedimentree_core::{commit::FragmentState, loose_commit::id::CommitId};
use sedimentree_wasm::{
    commit_id::WasmCommitId, fragment::WasmFragment, loose_commit::WasmBlobMeta,
    sedimentree_id::WasmSedimentreeId,
};
use wasm_bindgen::prelude::*;

use crate::WasmParents;

/// The state of a fragment while being built.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = FragmentState)]
pub struct WasmFragmentState(pub(crate) FragmentState<WasmParents>);

#[wasm_bindgen(js_class = FragmentState)]
impl WasmFragmentState {
    /// The "newest" element of the fragment.
    ///
    /// This identifier provides a stable point from which
    /// the rest of the fragment is built.
    #[must_use]
    pub fn head_id(&self) -> WasmCommitId {
        WasmCommitId::from(self.0.head_id())
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_id`
    /// and the `boundary` (not including the boundary elements).
    #[must_use]
    pub fn members(&self) -> Vec<WasmCommitId> {
        self.0
            .members()
            .iter()
            .map(|id| WasmCommitId::from(*id))
            .collect()
    }

    /// The checkpoints of the fragment.
    ///
    /// These are all of the identifiers that match a valid level
    /// below the target, so that it is possible to know which other fragments
    /// this one covers.
    #[must_use]
    pub fn checkpoints(&self) -> Vec<WasmCommitId> {
        self.0
            .checkpoints()
            .iter()
            .map(|id| WasmCommitId::from(*id))
            .collect()
    }

    /// The boundary from which the next set of fragments would be built.
    #[must_use]
    pub fn boundary(&self) -> WasmBoundary {
        let boundary = self.0.boundary();

        let mut map = Map::new();
        for (key, value) in boundary {
            let wasm_key = WasmCommitId::from(*key);
            let wasm_value: Set<WasmCommitId> =
                value.0.iter().map(|id| WasmCommitId::from(*id)).collect();
            map.insert(wasm_key, wasm_value);
        }
        WasmBoundary(map)
    }

    /// Convert the fragment state into a fragment.
    #[must_use]
    #[wasm_bindgen(js_name = intoFragment)]
    pub fn into_fragment(
        &self,
        sedimentree_id: WasmSedimentreeId,
        blob_meta: &WasmBlobMeta,
    ) -> WasmFragment {
        self.0
            .clone()
            .to_fragment(sedimentree_id.into(), blob_meta.clone().into())
            .into()
    }
}

impl From<FragmentState<WasmParents>> for WasmFragmentState {
    fn from(state: FragmentState<WasmParents>) -> Self {
        WasmFragmentState(state)
    }
}

/// The boundary of a fragment.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Boundary)]
pub struct WasmBoundary(Map<WasmCommitId, Set<WasmCommitId>>);

#[wasm_bindgen(js_class = Boundary)]
impl WasmBoundary {
    /// Get the set of commit IDs for a given key in the boundary.
    #[must_use]
    pub fn get(&self, key: &WasmCommitId) -> Option<Vec<WasmCommitId>> {
        self.0.get(key).map(|set| set.iter().cloned().collect())
    }

    /// Get all keys in the boundary.
    #[must_use]
    pub fn keys(&self) -> Vec<WasmCommitId> {
        self.0.keys().cloned().collect()
    }
}

impl From<Map<WasmCommitId, Set<WasmCommitId>>> for WasmBoundary {
    fn from(boundary: Map<WasmCommitId, Set<WasmCommitId>>) -> Self {
        WasmBoundary(boundary)
    }
}

/// A store for fragment states.
///
/// Uses interior mutability (`RefCell`) so that `wasm_bindgen` only takes
/// a shared borrow across the JS boundary, avoiding "recursive use of
/// an object" panics when JS callbacks re-enter during `buildFragmentStore`.
#[derive(Debug, Default, Clone)]
#[wasm_bindgen(js_name = FragmentStateStore)]
pub struct WasmFragmentStateStore(
    #[allow(clippy::type_complexity)] pub(crate) RefCell<Map<CommitId, FragmentState<WasmParents>>>,
);

#[wasm_bindgen(js_class = FragmentStateStore)]
impl WasmFragmentStateStore {
    /// Create a new empty `WasmFragmentStateStore`.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self(RefCell::new(Map::new()))
    }

    /// Insert a fragment state into the store.
    pub fn insert(&self, commit_id: &WasmCommitId, state: &WasmFragmentState) {
        let id = CommitId::from(commit_id);
        self.0.borrow_mut().insert(id, state.0.clone());
    }

    /// Get a fragment state from the store.
    pub fn get(&self, commit_id: &WasmCommitId) -> Option<WasmFragmentState> {
        let id = CommitId::from(commit_id);
        self.0.borrow().get(&id).cloned().map(WasmFragmentState)
    }
}

#[allow(clippy::implicit_hasher)]
impl From<WasmFragmentStateStore> for Map<CommitId, FragmentState<WasmParents>> {
    fn from(store: WasmFragmentStateStore) -> Self {
        store.0.into_inner()
    }
}
