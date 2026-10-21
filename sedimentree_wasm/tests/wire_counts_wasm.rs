//! POJO admission and metadata parsing share normalized core wire-count limits.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use sedimentree_core::{
    blob::BlobMeta,
    fragment::{Fragment, MAX_BOUNDARIES, MAX_CHECKPOINTS, checkpoint::Checkpoint},
    id::SedimentreeId,
    loose_commit::{LooseCommit, MAX_PARENTS, id::CommitId},
};
use sedimentree_wasm::pojo as p;
use std::collections::BTreeSet;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;
fn id(n: usize) -> CommitId {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
    CommitId::new(bytes)
}
fn ids(n: usize) -> JsValue {
    p::values((0..n).map(|n| p::hex(id(n).as_bytes()).into()))
}
fn input(boundary: JsValue, checkpoints: JsValue) -> JsValue {
    p::object(&[
        ("head", p::hex(id(999).as_bytes()).into()),
        ("parents", boundary.clone()),
        ("boundary", boundary),
        ("checkpoints", checkpoints),
        ("blob", js_sys::Uint8Array::from(&[1, 2, 3][..]).into()),
    ])
}
const fn tree() -> SedimentreeId {
    SedimentreeId::new([1; 32])
}
fn no_checkpoints() -> JsValue {
    js_sys::Array::new().into()
}
#[wasm_bindgen_test]
fn parent_and_boundary_limits_are_consistent() {
    for count in [MAX_PARENTS, MAX_PARENTS + 1] {
        let values = input(ids(count), no_checkpoints());
        let result = p::commit_input(tree(), &values, "commit");
        assert_eq!(result.is_ok(), count == MAX_PARENTS);
        let blob = sedimentree_core::blob::Blob::new(vec![1, 2, 3]);
        assert_eq!(
            result.is_ok(),
            LooseCommit::new(
                tree(),
                id(999),
                (0..count).map(id).collect(),
                BlobMeta::new(&blob)
            )
            .is_ok()
        );
    }
    let repeated =
        p::values((0..MAX_PARENTS * 2).map(|n| p::hex(id(n % MAX_PARENTS).as_bytes()).into()));
    let values = input(repeated, no_checkpoints());
    assert!(p::commit_input(tree(), &values, "commit").is_ok());
    assert!(p::fragment_input(tree(), &values, "fragment").is_ok());
    for count in [MAX_BOUNDARIES, MAX_BOUNDARIES + 1] {
        let result = p::fragment_input(tree(), &input(ids(count), no_checkpoints()), "fragment");
        assert_eq!(result.is_ok(), count == MAX_BOUNDARIES);
    }
}
#[wasm_bindgen_test]
fn checkpoint_limits_apply_after_normalization_in_both_paths() {
    let blob = sedimentree_core::blob::Blob::new(vec![1, 2, 3]);
    for count in [MAX_CHECKPOINTS, MAX_CHECKPOINTS + 1] {
        let checkpoints =
            p::values((0..count).map(|n| p::hex(Checkpoint::new(id(n)).as_bytes()).into()));
        let values = input(ids(0), checkpoints.clone());
        let result = p::fragment_input(tree(), &values, "fragment");
        let core = Fragment::from_parts(
            tree(),
            id(999),
            BTreeSet::new(),
            (0..count).map(|n| Checkpoint::new(id(n))).collect(),
            BlobMeta::new(&blob),
        );
        assert_eq!(result.is_ok(), core.is_ok());
        assert_eq!(result.is_ok(), count == MAX_CHECKPOINTS);
        // Exercise the built-metadata path with identical limits, including rejection.
        let empty = Fragment::from_parts(
            tree(),
            id(999),
            BTreeSet::new(),
            BTreeSet::new(),
            BlobMeta::new(&blob),
        )
        .unwrap();
        let metadata = p::fragment_metadata(&empty);
        js_sys::Reflect::set(&metadata, &"checkpoints".into(), &checkpoints).unwrap();
        let parsed = p::read_fragment(&metadata, "metadata");
        assert_eq!(parsed.is_ok(), core.is_ok());
        if let Ok(core) = core {
            assert_eq!(parsed.unwrap(), core);
            assert_eq!(result.unwrap().0, core);
        }
    }
    // More unique full IDs than the wire allows, but one checkpoint prefix.
    let colliding = (0..=MAX_CHECKPOINTS).map(|n| {
        let mut bytes = [0; 32];
        bytes[24..].copy_from_slice(&(n as u64).to_le_bytes());
        Checkpoint::new(CommitId::new(bytes))
    });
    let values = input(
        ids(0),
        p::values(colliding.map(|cp| p::hex(cp.as_bytes()).into())),
    );
    let (fragment, _) = p::fragment_input(tree(), &values, "fragment").unwrap();
    assert_eq!(fragment.checkpoints().len(), 1);
    assert_eq!(
        p::read_fragment(&p::fragment_metadata(&fragment), "metadata").unwrap(),
        fragment
    );
}
#[wasm_bindgen_test]
fn pojo_admission_returns_named_errors() {
    let values = input(ids(MAX_PARENTS + 1), no_checkpoints());
    for error in [
        p::commit_input(tree(), &values, "commit").unwrap_err(),
        p::fragment_input(tree(), &values, "fragment").unwrap_err(),
    ] {
        assert_eq!(
            p::field(&error, "name").unwrap(),
            JsValue::from_str("TypeError")
        );
        assert!(
            !p::field(&error, "message")
                .unwrap()
                .as_string()
                .unwrap()
                .is_empty()
        );
    }
}
