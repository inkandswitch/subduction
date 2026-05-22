//! Pure-Rust demonstration of the re-entry hazard that affects
//! `WasmFragmentStateStore`. No wasm-bindgen / JS round-tripping;
//! this test runs natively to prove the structural issue exists.
//!
//! The shape mirrors the production code:
//!
//! 1. Outer: hold `cell.borrow_mut()` across a call.
//! 2. The called function invokes a user-supplied callback (the
//!    moral equivalent of `get_change_meta_by_hash` calling into JS).
//! 3. That callback re-enters `cell.borrow*()`.
//!
//! In the production code path, the outer borrow happens in
//! `js_build_fragment_store` at:
//!
//!     self.build_fragment_store(&heads, &mut store.0.borrow_mut(), strategy)
//!
//! The called function is `sedimentree_core::CommitStore::build_fragment_store`,
//! which calls `self.lookup` which calls `get_change_meta_by_hash`
//! (synchronous JS call). The JS callback can call `store.insert` /
//! `store.get`, which lands in `WasmFragmentStateStore::insert`/`get`
//! and does `self.0.borrow_mut()` / `self.0.borrow()` — panicking.

use std::{cell::RefCell, collections::HashMap};

/// Stand-in for `WasmFragmentStateStore.0`.
type Store = RefCell<HashMap<u32, String>>;

/// Stand-in for the user-supplied JS callback (e.g. the body of
/// `getChangeMetaByHash` that re-enters `store.get`).
type Callback<'a> = Box<dyn Fn(u32) -> Option<String> + 'a>;

/// Stand-in for `sedimentree_core::CommitStore::build_fragment_store`.
///
/// Takes `&mut HashMap` (the borrow held by `js_build_fragment_store`),
/// invokes the callback (the moral equivalent of `lookup` →
/// `get_change_meta_by_hash` → user JS).
fn build_with_callback(known: &mut HashMap<u32, String>, cb: &Callback<'_>) {
    // Simulate the traversal calling `lookup` once.
    let _ignored = cb(99);
    // Then write into the known map (analogue of `known.insert(...)`).
    known.insert(1, "first-fragment".to_string());
}

/// The buggy outer wrapper. Mirrors `js_build_fragment_store`'s
/// `borrow_mut()` held across the call.
fn outer_buggy(store: &Store, cb: &Callback<'_>) {
    build_with_callback(&mut store.borrow_mut(), cb);
    //                       ^^^^^^^^^^^^^^^^^^
    // borrow_mut() temporary lives until the `;` at end of statement.
}

/// The clone-and-merge fix. Mirrors the proposed
/// `js_build_fragment_store`: clone, work on the clone, merge back.
fn outer_fixed(store: &Store, cb: &Callback<'_>) {
    let mut working = store.borrow().clone();
    build_with_callback(&mut working, cb);
    // Merge working back into the cell. Re-acquire borrow_mut only
    // for the brief merge, after the cb has finished.
    let mut guard = store.borrow_mut();
    for (k, v) in working {
        guard.insert(k, v);
    }
}

/// Direct demonstration: the buggy outer panics when the callback
/// re-enters.
#[test]
#[should_panic(expected = "already mutably borrowed")]
fn buggy_outer_panics_on_reentrant_get() {
    let store: Store = RefCell::new(HashMap::new());
    let store_ref = &store;
    let cb: Callback<'_> = Box::new(move |_id: u32| -> Option<String> {
        // Re-enter the cell. PANIC: store is already mutably
        // borrowed by outer_buggy.
        store_ref.borrow().get(&0).cloned()
    });
    outer_buggy(&store, &cb);
}

/// Same shape but with the fix in place: no panic.
#[test]
fn fixed_outer_does_not_panic_on_reentrant_get() {
    let store: Store = RefCell::new(HashMap::new());
    let store_ref = &store;
    let cb: Callback<'_> = Box::new(move |_id: u32| -> Option<String> {
        // Re-enter the cell. With the fix, the cell isn't borrowed
        // here — `outer_fixed` operates on an owned clone — so this
        // returns None cleanly.
        store_ref.borrow().get(&0).cloned()
    });
    outer_fixed(&store, &cb);
    // After the call, the merge-back made the new entry visible.
    assert_eq!(
        store.borrow().get(&1).cloned(),
        Some("first-fragment".to_string())
    );
}

/// The pre-fix code also panics if the callback only does a read,
/// because the outer borrow is `_mut` — even a shared borrow conflicts.
#[test]
#[should_panic(expected = "already mutably borrowed")]
fn buggy_outer_panics_on_reentrant_read() {
    let store: Store = RefCell::new(HashMap::new());
    let store_ref = &store;
    let cb: Callback<'_> = Box::new(move |_id: u32| -> Option<String> {
        // Read-only re-entry. Still panics because the outer holds
        // borrow_mut(), and any borrow() conflicts with that.
        store_ref.borrow().get(&123).cloned()
    });
    outer_buggy(&store, &cb);
}
