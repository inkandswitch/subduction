//! Does `bundle_fragments` reproduce byte-identical blobs across
//! save/load round-trips and repeated calls?
//!
//! The ingest flow signs `BlobMeta` (digest + size) over bundle bytes. Any
//! consumer that later *regenerates* those bytes from the document — instead
//! of storing the originals — implicitly assumes bundling is deterministic.
//! If it is not, regenerated blobs no longer match their signed metadata and
//! every remote rejects them as "blob mismatch".

use automerge::{Automerge, ObjType, ROOT, transaction::Transactable};

fn build_doc() -> Automerge {
    let mut doc = automerge::AutoCommit::new();
    let list = doc.put_object(&ROOT, "items", ObjType::List).expect("list");
    for i in 0..300 {
        doc.insert(&list, i, format!("item-{i}")).expect("insert");
        if i % 25 == 0 {
            doc.commit();
        }
    }
    doc.commit();

    // A second actor with a fork/merge, for non-linear history.
    let mut fork = doc.fork().with_actor("beef".parse().expect("actor"));
    fork.put(&ROOT, "forked", true).expect("put");
    fork.commit();
    doc.merge(&mut fork).expect("merge");

    Automerge::load(&doc.save()).expect("load built doc")
}

fn bundles(doc: &Automerge) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    (doc.bundle_fragments(cached), doc.bundle_fragments(loose))
}

#[test]
fn bundle_fragments_is_deterministic_within_one_doc() {
    let doc = build_doc();
    assert_eq!(bundles(&doc), bundles(&doc), "same doc, two calls");
}

#[test]
fn bundle_fragments_is_stable_across_save_load() {
    let doc = build_doc();
    let reloaded = Automerge::load(&doc.save()).expect("reload");
    assert_eq!(
        bundles(&doc),
        bundles(&reloaded),
        "bundling after save/load must reproduce the original bytes, or \
         signed BlobMeta over bundles cannot be regenerated"
    );
}

#[test]
fn bundle_fragments_is_stable_across_incremental_load() {
    // The shape a storage adapter sees: a doc reassembled from incremental
    // saves rather than one full save.
    let doc = build_doc();

    let changes: Vec<_> = doc.get_changes(&[]);
    let mut rebuilt = Automerge::new();
    for change in changes {
        rebuilt
            .apply_changes([change.clone()])
            .expect("apply change");
    }

    assert_eq!(
        bundles(&doc),
        bundles(&rebuilt),
        "bundling a doc rebuilt from its change log must reproduce the \
         original bytes"
    );
}
