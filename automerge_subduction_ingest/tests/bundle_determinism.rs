//! Does `bundle_fragments` reproduce byte-identical blobs across
//! save/load round-trips and repeated calls?
//!
//! The ingest flow signs `BlobMeta` (digest + size) over bundle bytes. Any
//! consumer that later *regenerates* those bytes from the document — instead
//! of storing the originals — implicitly assumes bundling is deterministic.
//! If it is not, regenerated blobs no longer match their signed metadata and
//! every remote rejects them as "blob mismatch".

use automerge::{Automerge, ObjType, ROOT, transaction::Transactable};
use testresult::TestResult;

fn build_doc() -> TestResult<Automerge> {
    let mut doc = automerge::AutoCommit::new();
    let list = doc.put_object(&ROOT, "items", ObjType::List)?;
    for i in 0..300 {
        doc.insert(&list, i, format!("item-{i}"))?;
        if i % 25 == 0 {
            doc.commit();
        }
    }
    doc.commit();

    // A second actor with a fork/merge, for non-linear history.
    let mut fork = doc.fork().with_actor("beef".parse()?);
    fork.put(&ROOT, "forked", true)?;
    fork.commit();
    doc.merge(&mut fork)?;

    Ok(Automerge::load(&doc.save())?)
}

fn bundles(doc: &Automerge) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);
    (doc.bundle_fragments(cached), doc.bundle_fragments(loose))
}

#[test]
fn bundle_fragments_is_deterministic_within_one_doc() -> TestResult {
    let doc = build_doc()?;
    assert_eq!(bundles(&doc), bundles(&doc), "same doc, two calls");
    Ok(())
}

#[test]
fn bundle_fragments_is_stable_across_save_load() -> TestResult {
    let doc = build_doc()?;
    let reloaded = Automerge::load(&doc.save())?;
    assert_eq!(
        bundles(&doc),
        bundles(&reloaded),
        "bundling after save/load must reproduce the original bytes, or \
         signed BlobMeta over bundles cannot be regenerated"
    );
    Ok(())
}

#[test]
fn bundle_fragments_is_stable_across_incremental_load() -> TestResult {
    // The shape a storage adapter sees: a doc reassembled from incremental
    // saves rather than one full save.
    let doc = build_doc()?;

    let changes: Vec<_> = doc.get_changes(&[]);
    let mut rebuilt = Automerge::new();
    for change in changes {
        rebuilt.apply_changes([change.clone()])?;
    }

    assert_eq!(
        bundles(&doc),
        bundles(&rebuilt),
        "bundling a doc rebuilt from its change log must reproduce the \
         original bytes"
    );
    Ok(())
}
