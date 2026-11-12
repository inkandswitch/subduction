//! POJO fragment admission/metadata round trips preserve core encoding.
#![cfg(target_arch = "wasm32")]
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    codec::encode::Encode,
    fragment::{Fragment, checkpoint::Checkpoint},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use sedimentree_wasm::pojo as p;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn pojo_input_and_metadata_preserve_core_value_and_encoding() {
    let tree = SedimentreeId::new([0xab; 32]);
    let head = CommitId::new([0x80; 32]);
    let boundary = [CommitId::new([0xff; 32]), CommitId::new([0x10; 32])];
    let mut collision = [0xff; 32];
    collision[31] = 0;
    let ids = [
        CommitId::new([0xff; 32]),
        CommitId::new([0; 32]),
        CommitId::new(collision),
        head,
        boundary[1],
        CommitId::new([0; 32]),
    ];
    let blob = Blob::new(vec![1, 255, 0, 128]);
    for ids in [&ids[..], &[][..]] {
        let expected = Fragment::from_parts(
            tree,
            head,
            boundary.into_iter().collect(),
            ids.iter().copied().map(Checkpoint::new).collect(),
            BlobMeta::new(&blob),
        )
        .unwrap();
        let input = p::object(&[
            ("head", p::hex(head.as_bytes()).into()),
            (
                "boundary",
                p::values(
                    boundary
                        .iter()
                        .rev()
                        .chain(&boundary)
                        .map(|id| p::hex(id.as_bytes()).into()),
                ),
            ),
            (
                "checkpoints",
                p::values(
                    ids.iter()
                        .map(|id| p::hex(Checkpoint::new(*id).as_bytes()).into()),
                ),
            ),
            ("blob", js_sys::Uint8Array::from(blob.as_slice()).into()),
        ]);
        let (admitted, copied_blob) = p::fragment_input(tree, &input, "fragment").unwrap();
        assert_eq!(copied_blob, blob);
        let reconstructed = p::read_fragment(&p::fragment_metadata(&admitted), "metadata").unwrap();
        for fragment in [admitted, reconstructed] {
            assert_eq!(fragment, expected);
            assert_eq!(fragment.encode(), expected.encode());
        }
    }
}
#[wasm_bindgen_test]
fn pojo_rejects_unencodable_boundary_counts() {
    let input = p::object(&[
        ("head", p::hex(&[0; 32]).into()),
        (
            "boundary",
            p::values((0..=u8::MAX).map(|byte| p::hex(&[byte; 32]).into())),
        ),
        ("checkpoints", js_sys::Array::new().into()),
        ("blob", js_sys::Uint8Array::new_with_length(0).into()),
    ]);
    let error = p::fragment_input(SedimentreeId::new([0; 32]), &input, "fragment").unwrap_err();
    assert_eq!(
        p::field(&error, "name").unwrap(),
        JsValue::from_str("TypeError")
    );
}
#[wasm_bindgen_test]
fn checkpoint_hex_is_exact_and_truncation_is_not_hashing() {
    for length in [0, 11, 13, 32] {
        assert!(p::unhex::<12>(&p::hex(&vec![0; length]).into(), "checkpoint").is_err());
    }
    let id = CommitId::new([0xa5; 32]);
    let prefix = Checkpoint::new(id);
    let hex_id = p::hex(id.as_bytes());
    assert_eq!(p::hex(prefix.as_bytes()), hex_id[..24]);
    assert_eq!(
        p::unhex::<12>(&hex_id[..24].into(), "checkpoint").unwrap(),
        [0xa5; 12]
    );
    assert_eq!(id.as_bytes(), &[0xa5; 32]);
}
