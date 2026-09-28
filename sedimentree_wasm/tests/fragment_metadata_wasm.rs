//! Checkpoint validation and fragment constructor/getter round trips.
//!
//! Run with `wasm-pack test --node sedimentree_wasm --test fragment_metadata_wasm`.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs, clippy::unwrap_used)] // Known-valid test fixtures.

use sedimentree_core::{
    codec::encode::Encode,
    fragment::{Fragment, checkpoint::Checkpoint},
    loose_commit::id::CommitId,
};
use sedimentree_wasm::{
    checkpoint::WasmCheckpoint,
    commit_id::WasmCommitId,
    fragment::{WasmFragment, WasmInvalidFragment},
    loose_commit::WasmBlobMeta,
    sedimentree_id::WasmSedimentreeId,
};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn constructors_and_getters_preserve_core_value_and_encoding() {
    let tree = WasmSedimentreeId::from_bytes(&[0xab; 32]).unwrap();
    let head = WasmCommitId::from_bytes(&[0x80; 32]).unwrap();
    let boundary = [
        WasmCommitId::from_bytes(&[0xff; 32]).unwrap(),
        WasmCommitId::from_bytes(&[0x10; 32]).unwrap(),
    ];
    let mut collision = [0xff; 32];
    collision[31] = 0;
    let ids = [
        CommitId::new([0xff; 32]),
        CommitId::new([0; 32]),
        CommitId::new(collision),
        CommitId::new([0x80; 32]), // Do not filter the head prefix.
        CommitId::new([0x10; 32]), // Or boundary prefixes.
        CommitId::new([0; 32]),
    ];
    let meta = WasmBlobMeta::new(&[1, 255, 0, 128]);
    for ids in [&ids[..], &[][..]] {
        let full = WasmFragment::new(
            &tree,
            &head,
            boundary
                .iter()
                .rev()
                .chain(&boundary)
                .cloned()
                .map(Into::into)
                .collect(),
            ids.iter()
                .copied()
                .map(WasmCommitId::from)
                .map(Into::into)
                .collect(),
            &meta,
        );
        let prefixes = WasmFragment::from_checkpoint_prefixes(
            &tree,
            &head,
            boundary.iter().cloned().map(Into::into).collect(),
            ids.iter()
                .copied()
                .map(Checkpoint::new)
                .map(WasmCheckpoint::from)
                .map(Into::into)
                .collect(),
            &meta,
        )
        .unwrap();
        let reconstructed = WasmFragment::from_checkpoint_prefixes(
            &prefixes.sedimentree_id(),
            &prefixes.head(),
            prefixes.boundary().into_iter().map(Into::into).collect(),
            prefixes.checkpoints().into_iter().map(Into::into).collect(),
            &prefixes.blob_meta(),
        )
        .unwrap();
        let full = Fragment::from(full);
        for fragment in [prefixes, reconstructed] {
            let fragment = Fragment::from(fragment);
            assert_eq!(fragment, full);
            assert_eq!(fragment.encode(), full.encode());
        }
    }
}

#[wasm_bindgen_test]
fn prefix_factory_rejects_unencodable_boundary_counts() {
    let tree = WasmSedimentreeId::from_bytes(&[0; 32]).unwrap();
    let head = WasmCommitId::from_bytes(&[0; 32]).unwrap();
    let meta = WasmBlobMeta::new(&[]);
    let boundary = (0..=u8::MAX)
        .map(|byte| WasmCommitId::from(CommitId::new([byte; 32])).into())
        .collect();
    assert_eq!(
        WasmFragment::from_checkpoint_prefixes(&tree, &head, boundary, vec![], &meta),
        Err(WasmInvalidFragment::TooManyBoundaries(256)),
    );
}

#[wasm_bindgen_test]
fn checkpoint_bytes_are_exact_and_truncation_is_not_hashing() {
    for length in [0, 11, 13, 32] {
        assert!(WasmCheckpoint::from_bytes(&vec![0; length]).is_err());
    }
    let id = WasmCommitId::from_bytes(&[0xa5; 32]).unwrap();
    let prefix = WasmCheckpoint::from_commit_id(&id);
    assert_eq!(prefix.to_bytes(), vec![0xa5; 12]);
    assert_eq!(prefix, WasmCheckpoint::from_bytes(&[0xa5; 12]).unwrap());
    assert_eq!(id.to_bytes(), vec![0xa5; 32]);
}
