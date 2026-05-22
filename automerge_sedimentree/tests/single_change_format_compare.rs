//! Compare blob size for a single change encoded as
//!   (a) `change.bytes()`  — per-change DEFLATE (whole chunk).
//!   (b) `doc.bundle([h]).bytes()` — bundle format with per-column DEFLATE.
//!
//! For each egwalker vector, iterate every loose (level-0) head and report
//! totals + per-change averages. Tells us whether replacing the level-0
//! fast-path in `bundle_fragments` with full bundle encoding would shrink
//! or grow the on-wire size for loose commits.

#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use automerge::{Automerge, ReadDoc};

fn human(n: usize) -> String {
    let mut s = n.to_string();
    let mut out = String::new();
    while s.len() > 3 {
        let tail = s.split_off(s.len() - 3);
        out.insert_str(0, &format!(",{tail}"));
    }
    format!("{s}{out}")
}

#[test]
fn compare_single_change_encodings() {
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
    eprintln!(
        "{:<5}  {:>6}  {:>14}  {:>14}  {:>8}  {:>9}  {:>9}",
        "doc", "n", "change.bytes()", "bundle([h])", "Δ total", "Δ avg/ch", "ratio"
    );
    eprintln!("{}", "-".repeat(80));

    for (name, bytes) in vectors {
        let doc = Automerge::load(bytes).expect(name);
        let loose = doc.fragments(0..=0);

        let mut change_total = 0usize;
        let mut bundle_total = 0usize;
        let mut bigger = 0usize;
        let mut smaller = 0usize;
        let mut same = 0usize;

        for f in &loose {
            assert_eq!(f.members.len(), 1);
            let h = f.head;
            let change_size = doc.get_change_by_hash(&h).unwrap().bytes().len();
            let bundle_size = doc.bundle([h]).unwrap().bytes().len();
            change_total += change_size;
            bundle_total += bundle_size;
            match bundle_size.cmp(&change_size) {
                std::cmp::Ordering::Greater => bigger += 1,
                std::cmp::Ordering::Less => smaller += 1,
                std::cmp::Ordering::Equal => same += 1,
            }
        }

        let delta = bundle_total as i64 - change_total as i64;
        let delta_avg = if loose.is_empty() {
            0.0
        } else {
            delta as f64 / loose.len() as f64
        };
        let ratio = if change_total > 0 {
            bundle_total as f64 / change_total as f64
        } else {
            0.0
        };
        eprintln!(
            "{name:<5}  {:>6}  {:>14}  {:>14}  {:>+8}  {:>+8.1}  {:>9.2}x  [bundle bigger={bigger} smaller={smaller} same={same}]",
            human(loose.len()),
            human(change_total),
            human(bundle_total),
            delta,
            delta_avg,
            ratio,
        );
    }
}
