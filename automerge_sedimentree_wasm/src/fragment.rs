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
        sedimentree_id: &WasmSedimentreeId,
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
///
/// All public methods use `try_borrow`/`try_borrow_mut` so a re-entrant
/// JS callback that calls back into the same store gets a typed error
/// (returned as `Err` to JS) rather than panicking the Wasm module.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the store is already locked by an in-flight
    /// `fragment` or `buildFragmentStore` call (e.g. when called from
    /// inside a `getChangeMetaByHash` JS callback). This replaces what
    /// would otherwise be a tab-killing "already borrowed" panic.
    pub fn insert(
        &self,
        commit_id: &WasmCommitId,
        state: &WasmFragmentState,
    ) -> Result<(), JsValue> {
        let id = CommitId::from(commit_id);
        let mut guard = self.0.try_borrow_mut().map_err(|_| {
            let err = js_sys::Error::new(
                "FragmentStateStore.insert: store is already in use by an in-progress \
                 fragment computation; do not call insert from inside a getChangeMetaByHash \
                 callback",
            );
            err.set_name("FragmentError");
            JsValue::from(err)
        })?;
        guard.insert(id, state.0.clone());
        Ok(())
    }

    /// Get a fragment state from the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the store is already locked by an in-flight
    /// mutating operation. (Concurrent reads via `get` from inside a
    /// callback are safe and will succeed.)
    pub fn get(&self, commit_id: &WasmCommitId) -> Result<Option<WasmFragmentState>, JsValue> {
        let id = CommitId::from(commit_id);
        let guard = self.0.try_borrow().map_err(|_| {
            let err = js_sys::Error::new(
                "FragmentStateStore.get: store is currently being mutated by an in-progress \
                 buildFragmentStore call",
            );
            err.set_name("FragmentError");
            JsValue::from(err)
        })?;
        Ok(guard.get(&id).cloned().map(WasmFragmentState))
    }
}

#[allow(clippy::implicit_hasher)]
impl From<WasmFragmentStateStore> for Map<CommitId, FragmentState<WasmParents>> {
    fn from(store: WasmFragmentStateStore) -> Self {
        store.0.into_inner()
    }
}

// ── Tests ──────────────────────────────────────────────────────────────
//
// These exercise the `try_borrow` / `try_borrow_mut` re-entrancy
// fix without requiring a wasm-bindgen-test runtime. The semantic
// bug is purely Rust-level: holding a `RefCell` borrow across an
// operation that triggers a callback into the same store. We reproduce
// the conflict directly by holding a borrow and attempting another.
//
// Note: we cannot exercise `WasmFragmentStateStore::insert` / `get`
// natively because their error paths call `js_sys::Error::new`, which
// panics on non-wasm32 targets. Instead we exercise the underlying
// `RefCell`'s `try_borrow*` semantics, which are exactly what the
// fix relies on. The wasm-bindgen layer is a thin wrapper that
// converts the `try_borrow*` `Err` into a `JsError`.

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use sedimentree_core::{collections::Map as ScMap, loose_commit::id::CommitId};

    fn empty_state(byte: u8) -> FragmentState<WasmParents> {
        FragmentState::new(
            CommitId::new([byte; 32]),
            sedimentree_core::collections::Set::new(),
            sedimentree_core::collections::Set::new(),
            ScMap::new(),
        )
    }

    /// Sanity: the `RefCell` accepts insertion and lookup when free.
    #[test]
    fn insert_and_get_when_free() {
        let store = WasmFragmentStateStore::new();
        let id = CommitId::new([1; 32]);
        let state = empty_state(1);

        // Mirror the body of `insert`:
        store
            .0
            .try_borrow_mut()
            .expect("free store accepts mut borrow")
            .insert(id, state);

        // Mirror the body of `get`:
        let got = store
            .0
            .try_borrow()
            .expect("free store accepts shared borrow")
            .get(&id)
            .cloned();
        assert!(got.is_some());
    }

    /// The bug fix: holding a shared borrow across a re-entrant
    /// mutating operation must return `Err`, not panic. Pre-fix, the
    /// production code path was `borrow_mut()` which would panic with
    /// "already mutably borrowed" — fatal in Wasm. Post-fix, it's
    /// `try_borrow_mut()` which returns `Err`, surfaced to JS as a
    /// `FragmentError`.
    #[test]
    fn try_borrow_mut_fails_during_active_shared_borrow() {
        let store = WasmFragmentStateStore::new();
        let id = CommitId::new([2; 32]);
        let state = empty_state(2);

        // Simulate `js_fragment` holding the shared borrow during the
        // entire `fragment` traversal; the JS callback re-enters and
        // attempts to insert.
        let _guard = store.0.try_borrow().expect("first shared borrow");

        let mut_result = store.0.try_borrow_mut();
        assert!(
            mut_result.is_err(),
            "try_borrow_mut during shared borrow must return Err, not panic"
        );

        // After releasing the guard, mut borrow succeeds normally.
        drop(_guard);
        store
            .0
            .try_borrow_mut()
            .expect("mut borrow after shared released")
            .insert(id, state);
    }

    /// Holding a mutable borrow across a re-entrant shared operation
    /// returns `Err` rather than panicking.
    #[test]
    fn try_borrow_fails_during_active_mut_borrow() {
        let store = WasmFragmentStateStore::new();

        let _guard = store.0.try_borrow_mut().expect("first mut borrow");

        let shared_result = store.0.try_borrow();
        assert!(
            shared_result.is_err(),
            "try_borrow during mut borrow must return Err, not panic"
        );

        drop(_guard);
        store
            .0
            .try_borrow()
            .expect("shared borrow after mut released");
    }

    /// Concurrent shared borrows coexist — only mutable-vs-anything
    /// conflicts trigger the error path. (This is the property that
    /// makes `js_fragment`'s `try_borrow` cheap: read-only re-entry
    /// into the store from a callback is still allowed.)
    #[test]
    fn multiple_shared_borrows_coexist() {
        let store = WasmFragmentStateStore::new();
        let _guard1 = store.0.try_borrow().expect("first shared borrow");
        let _guard2 = store.0.try_borrow().expect("second shared borrow coexists");
        let _guard3 = store.0.try_borrow().expect("third shared borrow coexists");
    }
}
