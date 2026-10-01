//! Native and generated JS constructors share normalized wire-count validation.

#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use sedimentree_core::{
    fragment::{MAX_BOUNDARIES, MAX_CHECKPOINTS, checkpoint::Checkpoint},
    loose_commit::{MAX_PARENTS, id::CommitId},
};
use sedimentree_wasm::{
    checkpoint::WasmCheckpoint,
    commit_id::{JsCommitId, WasmCommitId},
    fragment::{WasmFragment, WasmInvalidFragment},
    loose_commit::{WasmBlobMeta, WasmLooseCommit},
    sedimentree_id::WasmSedimentreeId,
};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

fn id(n: usize) -> CommitId {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&(n as u64).to_le_bytes());
    CommitId::new(bytes)
}

fn ids(n: usize) -> Vec<JsCommitId> {
    (0..n).map(|n| WasmCommitId::from(id(n)).into()).collect()
}

#[wasm_bindgen_test]
fn parent_and_boundary_limits_are_consistent() {
    let tree = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let head = WasmCommitId::from(id(999));
    let meta = WasmBlobMeta::new(&[1, 2, 3]);
    assert!(WasmLooseCommit::new(&tree, &head, ids(MAX_PARENTS), &meta).is_ok());
    assert!(WasmLooseCommit::new(&tree, &head, ids(MAX_PARENTS + 1), &meta).is_err());
    let repeated = || {
        (0..MAX_PARENTS * 2)
            .map(|n| WasmCommitId::from(id(n % MAX_PARENTS)).into())
            .collect()
    };
    assert!(WasmLooseCommit::new(&tree, &head, repeated(), &meta).is_ok());
    assert!(WasmFragment::new(&tree, &head, repeated(), vec![], &meta).is_ok());
    assert!(
        WasmFragment::from_checkpoint_prefixes(&tree, &head, repeated(), vec![], &meta).is_ok()
    );

    assert!(WasmFragment::new(&tree, &head, ids(MAX_BOUNDARIES), vec![], &meta).is_ok());
    assert!(
        WasmFragment::from_checkpoint_prefixes(&tree, &head, ids(MAX_BOUNDARIES), vec![], &meta)
            .is_ok()
    );
    let expected = Err(WasmInvalidFragment::TooManyBoundaries(MAX_BOUNDARIES + 1));
    assert_eq!(
        WasmFragment::new(&tree, &head, ids(MAX_BOUNDARIES + 1), vec![], &meta),
        expected
    );
    assert_eq!(
        WasmFragment::from_checkpoint_prefixes(
            &tree,
            &head,
            ids(MAX_BOUNDARIES + 1),
            vec![],
            &meta
        ),
        expected
    );
}

#[wasm_bindgen_test]
fn checkpoint_limits_apply_after_normalization_in_both_paths() {
    let tree = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let head = WasmCommitId::from(id(999));
    let meta = WasmBlobMeta::new(&[]);
    for count in [MAX_CHECKPOINTS, MAX_CHECKPOINTS + 1] {
        let full = WasmFragment::new(&tree, &head, vec![], ids(count), &meta);
        let prefixes = (0..count)
            .map(|n| WasmCheckpoint::from(Checkpoint::new(id(n))).into())
            .collect();
        let from_prefixes =
            WasmFragment::from_checkpoint_prefixes(&tree, &head, vec![], prefixes, &meta);
        if count == MAX_CHECKPOINTS {
            assert_eq!(full.unwrap(), from_prefixes.unwrap());
        } else {
            assert_eq!(full, Err(WasmInvalidFragment::TooManyCheckpoints(count)));
            assert_eq!(
                from_prefixes,
                Err(WasmInvalidFragment::TooManyCheckpoints(count))
            );
        }
    }

    // More unique full IDs than the wire count allows, but all share one prefix.
    let colliding = (0..=MAX_CHECKPOINTS)
        .map(|n| {
            let mut bytes = [0; 32];
            bytes[24..].copy_from_slice(&(n as u64).to_le_bytes());
            WasmCommitId::from(CommitId::new(bytes)).into()
        })
        .collect();
    let full = WasmFragment::new(&tree, &head, vec![], colliding, &meta).unwrap();
    assert_eq!(full.checkpoints().len(), 1);
    let repeated = (0..=MAX_CHECKPOINTS)
        .map(|_| WasmCheckpoint::from(Checkpoint::new(id(0))).into())
        .collect();
    assert_eq!(
        WasmFragment::from_checkpoint_prefixes(&tree, &head, vec![], repeated, &meta).unwrap(),
        full
    );
}

#[wasm_bindgen_test]
fn exported_js_constructors_throw_named_errors() {
    let tree = WasmSedimentreeId::from_bytes(&[1; 32]).unwrap();
    let head = WasmCommitId::from(id(999));
    let meta = WasmBlobMeta::new(&[]);
    let commit: JsValue = WasmLooseCommit::new(&tree, &head, vec![], &meta)
        .unwrap()
        .into();
    let fragment: JsValue = WasmFragment::new(&tree, &head, vec![], vec![], &meta)
        .unwrap()
        .into();
    let oversized = js_sys::Array::new();
    for n in 0..=MAX_PARENTS {
        oversized.push(&JsValue::from(WasmCommitId::from(id(n))));
    }
    let tree: JsValue = tree.into();
    let head: JsValue = head.into();
    let meta: JsValue = meta.into();
    for (instance, name, is_fragment) in [
        (commit, "InvalidLooseCommit", false),
        (fragment, "InvalidFragment", true),
    ] {
        let constructor: js_sys::Function = js_sys::Reflect::get(&instance, &"constructor".into())
            .unwrap()
            .unchecked_into();
        let args = js_sys::Array::new();
        args.push(&tree);
        args.push(&head);
        args.push(&oversized);
        if is_fragment {
            args.push(&js_sys::Array::new());
        }
        args.push(&meta);
        let error = js_sys::Reflect::construct(&constructor, &args).unwrap_err();
        assert_eq!(
            js_sys::Reflect::get(&error, &"name".into())
                .unwrap()
                .as_string()
                .as_deref(),
            Some(name)
        );
    }
}
