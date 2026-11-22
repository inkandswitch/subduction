//! Does `Automerge::fragments` reproduce byte-identical blobs across
//! save/load round-trips and repeated calls?
//!
//! The ingest flow signs `BlobMeta` (digest + size) over bundle bytes. Any
//! consumer that later *regenerates* those bytes from the document — instead
//! of storing the originals — implicitly assumes bundling is deterministic.
//! If it is not, regenerated blobs no longer match their signed metadata and
//! every remote rejects them as "blob mismatch".

use std::ops::RangeBounds;

use automerge::{
    ObjType, ROOT,
    next::{AutoCommit, Automerge, transaction::Transactable},
};
use testresult::TestResult;

fn build_doc() -> TestResult<Automerge> {
    let mut doc = AutoCommit::new();
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
    (fragment_bytes(doc, 1..), fragment_bytes(doc, 0..=0))
}

fn fragment_bytes(doc: &Automerge, levels: impl RangeBounds<usize>) -> Vec<Vec<u8>> {
    doc.fragments(levels).into_iter().map(|f| f.bytes).collect()
}

#[test]
fn fragments_are_deterministic_within_one_doc() -> TestResult {
    let doc = build_doc()?;
    assert_eq!(bundles(&doc), bundles(&doc), "same doc, two calls");
    Ok(())
}

#[test]
fn fragments_are_stable_across_save_load() -> TestResult {
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
fn fragments_are_stable_across_incremental_load() -> TestResult {
    // The shape a storage adapter sees: a doc reassembled from its stored
    // fragments rather than one full save.
    let doc = build_doc()?;

    let mut rebuilt = Automerge::new();
    for fragment in doc.fragments_after(&[], false)? {
        rebuilt.load_incremental(&fragment.bytes)?;
    }

    assert_eq!(
        bundles(&doc),
        bundles(&rebuilt),
        "bundling a doc rebuilt from its fragments must reproduce the \
         original bytes"
    );
    Ok(())
}
