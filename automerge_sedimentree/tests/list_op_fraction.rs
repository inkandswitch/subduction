//! For each egwalker vector, count how many ops are list-keyed (element id
//! key, i.e., key_actor + key_ctr) vs map-keyed (string key). Doc order for
//! map-keyed ops is derivable from the key column alone; only list-keyed
//! ops would actually need an explicit ordering column.
//!
//! Also: if we only stored ordering for list ops, what's the per-doc
//! savings vs storing it for ALL ops?

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

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v & 0x7f) | 0x80) as u8);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_svarint(out: &mut Vec<u8>, v: i64) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    write_uvarint(out, zigzag);
}

/// inverse_delta encoding: walk creation order, store delta of doc_pos.
fn enc_inverse_delta(perm: &[u32]) -> (usize, usize) {
    let mut inv = vec![0u32; perm.len()];
    for (doc_pos, &creation_pos) in perm.iter().enumerate() {
        inv[creation_pos as usize] = doc_pos as u32;
    }
    let mut raw = Vec::new();
    let mut prev: i64 = 0;
    for &d in &inv {
        write_svarint(&mut raw, d as i64 - prev);
        prev = d as i64;
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated)
}

#[test]
fn list_op_fraction_per_doc() {
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
        "{:<5}  {:>10}  {:>10}  {:>10}  {:>5}",
        "doc", "total ops", "list ops", "map ops", "list%"
    );
    eprintln!("{}", "-".repeat(55));
    for (name, bytes) in vectors {
        let doc = Automerge::load(bytes).expect(name);
        let changes = doc.get_changes(&[]);
        let mut total = 0usize;
        let mut list_ops = 0usize;
        let mut map_ops = 0usize;
        for c in &changes {
            for op in c.decode().operations {
                total += 1;
                // op.key is automerge::legacy::Key; Map(_) for map, Seq(_) for list
                match op.key {
                    automerge::legacy::Key::Map(_) => map_ops += 1,
                    automerge::legacy::Key::Seq(_) => list_ops += 1,
                }
            }
        }
        let pct = if total > 0 {
            100.0 * list_ops as f64 / total as f64
        } else {
            0.0
        };
        eprintln!(
            "{name:<5}  {:>10}  {:>10}  {:>10}  {pct:>4.1}%",
            human(total),
            human(list_ops),
            human(map_ops)
        );
    }
}

#[test]
fn savings_if_only_list_ops_carried_ordering() {
    let bytes = include_bytes!("../test-vectors/S1.am");
    let doc = Automerge::load(bytes).expect("S1");
    let h = doc.get_heads()[0];
    let change = doc.get_change_by_hash(&h).expect("change");
    let start_op = change.start_op().get();
    let bundle = doc.bundle([h]).expect("bundle");
    let counters = bundle.op_counters_in_doc_order();
    let perm_all: Vec<u32> = counters.iter().map(|c| (c - start_op) as u32).collect();

    // To classify each op as list vs map we need to walk in doc order with
    // their key type. We don't easily have that without changing the bundle
    // accessor; for S1 we already know from the column dump that key_str
    // is empty, i.e. 100% list ops, so there's nothing to subtract.

    // Compute the all-ops inverse_delta cost (baseline).
    let (raw_all, def_all) = enc_inverse_delta(&perm_all);
    eprintln!("\nS1 head[0]: {} ops, all list-keyed", human(perm_all.len()));
    eprintln!(
        "  inverse_delta (all ops):       raw {:>10}  on-disk {:>10}",
        human(raw_all),
        human(def_all)
    );
    eprintln!("  (no map ops to skip — full cost paid)");
}
