//! For each loose (single-change) bundle in the egwalker vectors, compare:
//!   (a) change.bytes()              — Change-format chunk (whole DEFLATE)
//!   (b) bundle([h]).bytes()         — Bundle chunk *as currently written*
//!                                     (carries BOTH id_ctr and id_ctr_inverse)
//!   (c) bundle without id_ctr       — what bundle.bytes() would be if we
//!                                     dropped the id_ctr column and kept
//!                                     only id_ctr_inverse
//!
//! (c) is computed by subtracting the on-disk cost of the id_ctr column
//! from (b). That cost is `deflate(raw)` if the raw column ≥ 256 B
//! (matching `BundleBuilder`'s threshold), otherwise the raw size.

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

/// Mirror of `RawColumn::compress` in storage/columns/raw_column.rs.
fn on_disk(raw: &[u8]) -> usize {
    if raw.len() < 256 {
        raw.len()
    } else {
        deflate_len(raw)
    }
}

struct Row {
    name: &'static str,
    n_changes: usize,
    change_total: usize,
    bundle_with_id_ctr_total: usize,
    bundle_with_inverse_only_total: usize,
    id_ctr_savings_total: usize,
}

fn measure(name: &'static str, bytes: &[u8]) -> Row {
    let doc = Automerge::load(bytes).expect(name);
    let loose = doc.fragments(0..=0);

    let mut change_total = 0usize;
    let mut bundle_with_id_ctr_total = 0usize;
    let mut bundle_with_inverse_only_total = 0usize;
    let mut id_ctr_savings_total = 0usize;
    let n_changes = loose.len();

    for f in &loose {
        assert_eq!(f.members.len(), 1);
        let h = f.head;

        let mut change = doc.get_change_by_hash(&h).expect("change");
        let change_size = change.bytes().len();
        change_total += change_size;

        let bundle = doc.bundle([h]).expect("bundle");
        let bundle_size = bundle.bytes().len();
        bundle_with_id_ctr_total += bundle_size;

        // What would bundle cost if we removed the id_ctr column and
        // kept only id_ctr_inverse?
        let id_ctr_raw = bundle.id_ctr_column_bytes();
        let id_ctr_on_disk = on_disk(&id_ctr_raw);
        // Each column also costs ~3-4 bytes of metadata in the column
        // spec list (spec varint + length varint). Subtract a small fixed
        // overhead; it's a rounding error at these sizes.
        let metadata_savings = 4;
        let total_save = id_ctr_on_disk + metadata_savings;
        bundle_with_inverse_only_total += bundle_size.saturating_sub(total_save);
        id_ctr_savings_total += total_save;
    }

    Row {
        name,
        n_changes,
        change_total,
        bundle_with_id_ctr_total,
        bundle_with_inverse_only_total,
        id_ctr_savings_total,
    }
}

#[test]
fn compare_change_vs_bundle_with_inverse_only() {
    let vectors = [
        ("S1", include_bytes!("../test-vectors/S1.am").as_slice()),
        ("S2", include_bytes!("../test-vectors/S2.am").as_slice()),
        ("S3", include_bytes!("../test-vectors/S3.am").as_slice()),
        ("A1", include_bytes!("../test-vectors/A1.am").as_slice()),
        ("A2", include_bytes!("../test-vectors/A2.am").as_slice()),
        ("C1", include_bytes!("../test-vectors/C1.am").as_slice()),
        ("C2", include_bytes!("../test-vectors/C2.am").as_slice()),
    ];

    let rows: Vec<Row> = vectors.iter().map(|(n, b)| measure(n, b)).collect();

    eprintln!();
    eprintln!(
        "{:<5}  {:>5}  {:>13}  {:>13}  {:>13}  {:>8}  {:>8}",
        "doc",
        "n",
        "Change total",
        "Bundle (today)",
        "Bundle (inv)",
        "B-inv/C",
        "save/ch"
    );
    eprintln!("{}", "-".repeat(82));

    let mut grand_change = 0usize;
    let mut grand_bundle = 0usize;
    let mut grand_bundle_inv = 0usize;
    let mut grand_n = 0usize;

    for r in &rows {
        if r.n_changes == 0 {
            eprintln!("{:<5}  {:>5}  {}", r.name, 0, "(no loose changes)");
            continue;
        }
        let ratio = r.bundle_with_inverse_only_total as f64 / r.change_total as f64;
        let save_per = (r.bundle_with_id_ctr_total - r.bundle_with_inverse_only_total) as f64
            / r.n_changes as f64;
        eprintln!(
            "{:<5}  {:>5}  {:>13}  {:>13}  {:>13}  {:>7.2}x  {:>8.0}",
            r.name,
            r.n_changes,
            human(r.change_total),
            human(r.bundle_with_id_ctr_total),
            human(r.bundle_with_inverse_only_total),
            ratio,
            save_per
        );

        grand_change += r.change_total;
        grand_bundle += r.bundle_with_id_ctr_total;
        grand_bundle_inv += r.bundle_with_inverse_only_total;
        grand_n += r.n_changes;
    }

    eprintln!("{}", "-".repeat(82));
    let grand_ratio = grand_bundle_inv as f64 / grand_change as f64;
    let grand_save_avg = (grand_bundle - grand_bundle_inv) as f64 / grand_n as f64;
    eprintln!(
        "{:<5}  {:>5}  {:>13}  {:>13}  {:>13}  {:>7.2}x  {:>8.0}",
        "ALL",
        grand_n,
        human(grand_change),
        human(grand_bundle),
        human(grand_bundle_inv),
        grand_ratio,
        grand_save_avg
    );

    eprintln!("\nLegend:");
    eprintln!("  n              = number of single-change (loose) bundles");
    eprintln!("  Change total   = Σ change.bytes().len()");
    eprintln!("  Bundle (today) = Σ bundle([h]).bytes().len() with BOTH id_ctr and id_ctr_inverse");
    eprintln!("  Bundle (inv)   = same as today minus the id_ctr column's on-disk cost");
    eprintln!("  B-inv/C        = ratio of inverse-only bundle to Change format");
    eprintln!("  save/ch        = avg bytes saved per change by dropping id_ctr");
}
