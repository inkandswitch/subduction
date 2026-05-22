//! Side-by-side comparison of the `id_ctr` (direct delta-int) and new
//! experimental `id_ctr_inverse` (inverse permutation delta-int) columns
//! in bundles. Verifies they encode the same permutation, then reports
//! raw + DEFLATEd sizes.

#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;

use automerge::{Automerge, ReadDoc};
use flate2::Compression;
use flate2::bufread::DeflateEncoder;

fn deflate_len(bytes: &[u8]) -> usize {
    let mut deflater = DeflateEncoder::new(bytes, Compression::default());
    let mut out = Vec::new();
    deflater.read_to_end(&mut out).unwrap();
    out.len()
}

fn human(n: usize) -> String {
    let mut s = n.to_string();
    let mut out = String::new();
    while s.len() > 3 {
        let tail = s.split_off(s.len() - 3);
        out.insert_str(0, &format!(",{tail}"));
    }
    format!("{s}{out}")
}

// ---- minimal hand-rolled zigzag delta-int decoder ----

fn read_uvarint(bytes: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if *pos >= bytes.len() {
            break;
        }
        let b = bytes[*pos];
        *pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn read_svarint(bytes: &[u8], pos: &mut usize) -> i64 {
    let u = read_uvarint(bytes, pos);
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// Decode an Automerge columnar column of (run_length, value) pairs.
/// The columnar format prefixes each run with a signed length: positive
/// = literal sequence of N values; negative = N repeats of the next
/// value; zero = N nulls (skip).
fn decode_delta_int_column(bytes: &[u8]) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    let mut pos = 0;
    let mut last_value: i64 = 0;
    while pos < bytes.len() {
        let header = read_svarint(bytes, &mut pos);
        if header == 0 {
            // Nulls: next is the count
            let n = read_uvarint(bytes, &mut pos);
            for _ in 0..n {
                out.push(None);
            }
        } else if header > 0 {
            // Literal sequence of `header` delta values
            for _ in 0..header {
                let delta = read_svarint(bytes, &mut pos);
                last_value = last_value.wrapping_add(delta);
                out.push(Some(last_value));
            }
        } else {
            // Repeated delta: -header copies of the next delta
            let count = (-header) as u64;
            let delta = read_svarint(bytes, &mut pos);
            for _ in 0..count {
                last_value = last_value.wrapping_add(delta);
                out.push(Some(last_value));
            }
        }
    }
    out
}

#[test]
fn id_ctr_inverse_matches_id_ctr_for_s1() {
    let bytes = include_bytes!("../test-vectors/S1.am");
    let doc = Automerge::load(bytes).expect("load");
    let h = doc.get_heads()[0];
    let change = doc.get_change_by_hash(&h).expect("change");
    let start_op = change.start_op().get();

    let bundle = doc.bundle([h]).expect("bundle");
    let doc_counters = bundle.op_counters_in_doc_order();
    let n = doc_counters.len();
    eprintln!("\nS1 head[0]: {} ops", human(n));

    let id_ctr_bytes = bundle.id_ctr_column_bytes();
    let id_ctr_inv_bytes = bundle.id_ctr_inverse_column_bytes();

    eprintln!("  id_ctr           uncompressed {:>10}  DEFLATE {:>10}",
        human(id_ctr_bytes.len()), human(deflate_len(&id_ctr_bytes)));
    eprintln!("  id_ctr_inverse   uncompressed {:>10}  DEFLATE {:>10}",
        human(id_ctr_inv_bytes.len()), human(deflate_len(&id_ctr_inv_bytes)));

    // === Verification: decode id_ctr_inverse and reconstruct doc-order counters ===

    // Decode id_ctr_inverse using Automerge's own DeltaCursor.
    let inv_values: Vec<i64> = bundle.id_ctr_inverse_decoded();
    assert_eq!(inv_values.len(), n, "inverse column length must match op count");

    // Build doc-position → counter mapping by inverting the permutation.
    let mut reconstructed = vec![0u64; n];
    for (k, &doc_pos) in inv_values.iter().enumerate() {
        let canonical_counter = start_op + k as u64;
        reconstructed[doc_pos as usize] = canonical_counter;
    }

    // Compare with the ground-truth doc-order counters from iter_ops.
    assert_eq!(
        reconstructed, doc_counters,
        "id_ctr_inverse decoded permutation must match id_ctr"
    );
    eprintln!("  ✓ inverse decode matches id_ctr exactly ({} counters)", human(n));
}

/// Verify the inverse-decoded permutation reconstructs every op's
/// `(actor, counter)` exactly. Returns the canonical (sorted by actor+seq
/// then by counter) list of op ids the inverse column corresponds to.
fn verify_inverse_matches(bundle: &automerge::Bundle) -> usize {
    let doc_ids = bundle.op_ids_in_doc_order();
    let n = doc_ids.len();

    // Reconstruct the canonical-order `(actor, counter)` list by reading
    // the bundle's change metadata sorted the same way `BundleBuilder`
    // sorts it: by (actor, seq). Each change c contributes its ops in
    // counter order: `(c.actor, c.start_op + j)` for j in 0..num_ops(c).
    let mut changes: Vec<_> = bundle.iter_changes().collect();
    changes.sort_by_key(|c| (c.actor, c.seq));
    let mut canonical: Vec<(usize, u64)> = Vec::with_capacity(n);
    for c in &changes {
        let num_ops = (c.max_op - c.start_op + 1) as usize;
        for j in 0..num_ops {
            canonical.push((c.actor, c.start_op + j as u64));
        }
    }
    assert_eq!(canonical.len(), n, "canonical count must equal op count");

    // Decode the inverse column. inv[k] = doc_pos of canonical op #k.
    let inv = bundle.id_ctr_inverse_decoded();
    assert_eq!(inv.len(), n, "inverse column length must equal op count");

    // Reconstruct doc-order `(actor, counter)` from canonical + inverse.
    let mut reconstructed = vec![(usize::MAX, u64::MAX); n];
    for (k, &doc_pos) in inv.iter().enumerate() {
        reconstructed[doc_pos as usize] = canonical[k];
    }
    assert_eq!(
        reconstructed, doc_ids,
        "inverse-encoded permutation must reconstruct every op's (actor, counter) exactly"
    );
    n
}

#[test]
fn id_ctr_inverse_matches_id_ctr_across_vectors() {
    let vectors = [
        ("S1", include_bytes!("../test-vectors/S1.am").as_slice()),
        ("S2", include_bytes!("../test-vectors/S2.am").as_slice()),
        ("S3", include_bytes!("../test-vectors/S3.am").as_slice()),
        ("A1", include_bytes!("../test-vectors/A1.am").as_slice()),
        ("A2", include_bytes!("../test-vectors/A2.am").as_slice()),
        ("C1", include_bytes!("../test-vectors/C1.am").as_slice()),
        ("C2", include_bytes!("../test-vectors/C2.am").as_slice()),
    ];

    eprintln!();
    eprintln!("{:<5}  {:>4}  {:>8}  {:>11} {:>11}  {:>11} {:>11}  {:>5}  {}",
        "doc", "frag", "ops", "id_ctr raw", "id_ctr def",
        "inv raw", "inv def", "ratio", "verified");
    eprintln!("{}", "-".repeat(95));

    let mut total_verified_ops = 0usize;
    let mut bundles_verified = 0usize;

    for (name, bytes) in vectors {
        let doc = Automerge::load(bytes).expect(name);
        let cached = doc.fragments(1..);

        // Take all cached fragments (multi-change bundles) plus one loose change.
        let mut targets: Vec<(String, Vec<automerge::ChangeHash>)> = Vec::new();
        for (i, f) in cached.iter().enumerate().take(3) {
            targets.push((format!("c{i}"), f.members.clone()));
        }
        let loose = doc.fragments(0..=0);
        if let Some(f) = loose.first() {
            targets.push(("loose".to_string(), vec![f.head]));
        }

        for (tag, hashes) in &targets {
            let bundle = doc.bundle(hashes.iter().copied()).expect("bundle");
            let verified_ops = verify_inverse_matches(&bundle);
            total_verified_ops += verified_ops;
            bundles_verified += 1;

            let id_ctr_bytes = bundle.id_ctr_column_bytes();
            let id_ctr_inv_bytes = bundle.id_ctr_inverse_column_bytes();
            let id_ctr_def = deflate_len(&id_ctr_bytes);
            let inv_def = deflate_len(&id_ctr_inv_bytes);
            let ratio = if id_ctr_def > 0 {
                inv_def as f64 / id_ctr_def as f64
            } else {
                0.0
            };
            eprintln!("{name:<5}  {tag:>4}  {:>8}  {:>11} {:>11}  {:>11} {:>11}  {:>4.2}x  ✓",
                human(verified_ops),
                human(id_ctr_bytes.len()),
                human(id_ctr_def),
                human(id_ctr_inv_bytes.len()),
                human(inv_def),
                ratio,
            );
        }
    }

    eprintln!(
        "\n✓ verified {bundles_verified} bundles, {} ops total — every (actor, counter) reconstructed exactly",
        human(total_verified_ops)
    );
}
