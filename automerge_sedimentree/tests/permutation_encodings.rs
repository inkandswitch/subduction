//! Experiment: how compactly can we encode the doc-order permutation for
//! a single change's ops?
//!
//! Baseline = what the bundle currently does: `DeltaInt` of counters,
//! then DEFLATE. Alternatives that exploit run/cluster structure may be
//! much smaller for text-editing-style workloads.
//!
//! For S1's first change, get the permutation π[doc_pos] = creation_pos,
//! then try several encodings and report sizes.

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

/// ZigZag-encode signed → unsigned, then varint.
fn write_svarint(out: &mut Vec<u8>, v: i64) {
    let zigzag = ((v << 1) ^ (v >> 63)) as u64;
    write_uvarint(out, zigzag);
}

// =========== Encodings ===========

/// Baseline: DeltaInt + DEFLATE. perm[i+1] - perm[i] as zigzag varint.
/// This is what bundle's `id_ctr` column does (approximately).
fn enc_delta_int(perm: &[u32]) -> (usize, usize) {
    let mut raw = Vec::new();
    let mut prev: i64 = 0;
    for &p in perm {
        let cur = p as i64;
        write_svarint(&mut raw, cur - prev);
        prev = cur;
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated)
}

/// Plain varints of perm[i] (no delta).
fn enc_plain_varint(perm: &[u32]) -> (usize, usize) {
    let mut raw = Vec::new();
    for &p in perm {
        write_uvarint(&mut raw, p as u64);
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated)
}

/// Offset-from-natural: perm[i] - i (zigzag svarint). If doc order ≈
/// creation order, most values are 0.
fn enc_offset_from_natural(perm: &[u32]) -> (usize, usize) {
    let mut raw = Vec::new();
    for (i, &p) in perm.iter().enumerate() {
        write_svarint(&mut raw, p as i64 - i as i64);
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated)
}

/// Monotonic-run RLE: find maximal runs where perm[i+1] = perm[i] + 1
/// (consecutive creation-order ops that ended up consecutively in doc
/// order). Encode each run as (start, length) varint pair.
fn enc_monotonic_runs(perm: &[u32]) -> (usize, usize, usize) {
    let mut raw = Vec::new();
    let mut i = 0;
    let mut run_count = 0;
    while i < perm.len() {
        let start = perm[i];
        let mut len = 1usize;
        while i + len < perm.len() && perm[i + len] == start + len as u32 {
            len += 1;
        }
        write_uvarint(&mut raw, start as u64);
        write_uvarint(&mut raw, len as u64);
        i += len;
        run_count += 1;
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated, run_count)
}

/// Inverse permutation: π⁻¹[creation_pos] = doc_pos. Same information,
/// possibly different DEFLATE efficiency.
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

/// Inverse permutation, offset from natural. If doc order ≈ creation
/// order, inv[c] - c is mostly 0.
fn enc_inverse_offset(perm: &[u32]) -> (usize, usize) {
    let mut inv = vec![0u32; perm.len()];
    for (doc_pos, &creation_pos) in perm.iter().enumerate() {
        inv[creation_pos as usize] = doc_pos as u32;
    }
    let mut raw = Vec::new();
    for (i, &d) in inv.iter().enumerate() {
        write_svarint(&mut raw, d as i64 - i as i64);
    }
    let deflated = deflate_len(&raw);
    (raw.len(), deflated)
}

fn permutation_stats(perm: &[u32]) -> (usize, usize, f64) {
    let mut increasing_runs = 0;
    let mut total_run_len = 0usize;
    let mut i = 0;
    while i < perm.len() {
        let start = perm[i];
        let mut len = 1usize;
        while i + len < perm.len() && perm[i + len] == start + len as u32 {
            len += 1;
        }
        increasing_runs += 1;
        total_run_len += len;
        i += len;
    }
    let avg_run = total_run_len as f64 / increasing_runs as f64;
    (increasing_runs, total_run_len, avg_run)
}

fn report_change(name: &str, perm: &[u32]) {
    eprintln!("\n=== {name} ===");
    eprintln!("  N (ops) = {}", human(perm.len()));
    let (runs, total, avg_run) = permutation_stats(perm);
    eprintln!(
        "  monotonic-increasing runs: {runs}, avg length {avg_run:.1}, total {total}"
    );

    let (raw, def) = enc_delta_int(perm);
    eprintln!("  delta_int + DEFLATE       raw={:>10} on-disk={:>10}", human(raw), human(def));
    let (raw, def) = enc_plain_varint(perm);
    eprintln!("  plain_varint + DEFLATE    raw={:>10} on-disk={:>10}", human(raw), human(def));
    let (raw, def) = enc_offset_from_natural(perm);
    eprintln!("  offset_from_natural + DEFLATE raw={:>10} on-disk={:>10}", human(raw), human(def));
    let (raw, def, runs) = enc_monotonic_runs(perm);
    eprintln!("  monotonic_runs (#runs={runs}) + DEFLATE raw={:>10} on-disk={:>10}", human(raw), human(def));
    let (raw, def) = enc_inverse_delta(perm);
    eprintln!("  inverse_delta + DEFLATE   raw={:>10} on-disk={:>10}", human(raw), human(def));
    let (raw, def) = enc_inverse_offset(perm);
    eprintln!("  inverse_offset + DEFLATE  raw={:>10} on-disk={:>10}", human(raw), human(def));
}

#[test]
fn permutation_encoding_comparison_s1() {
    let bytes = include_bytes!("../test-vectors/S1.am");
    let doc = Automerge::load(bytes).expect("load");
    let heads = doc.get_heads();
    for (i, h) in heads.iter().enumerate() {
        let change = doc.get_change_by_hash(h).expect("change");
        let start_op = change.start_op().get();
        let bundle = doc.bundle([*h]).expect("bundle");
        let counters = bundle.op_counters_in_doc_order();
        let perm: Vec<u32> = counters
            .iter()
            .map(|c| (c - start_op) as u32)
            .collect();
        report_change(&format!("S1 head[{i}]"), &perm);
    }
}

#[test]
fn permutation_encoding_comparison_others() {
    // Sample a few cached fragments from C1 and A1 — multi-change bundles.
    // These show how much the encoding cost amortizes.
    for (name, bytes) in [
        ("A1", include_bytes!("../test-vectors/A1.am").as_slice()),
        ("C1", include_bytes!("../test-vectors/C1.am").as_slice()),
    ] {
        let doc = Automerge::load(bytes).expect(name);
        let cached = doc.fragments(1..);
        // Largest cached fragment
        let f = cached
            .iter()
            .max_by_key(|f| f.members.len())
            .expect("fragments");
        let bundle = doc.bundle(f.members.iter().copied()).expect("bundle");
        let counters = bundle.op_counters_in_doc_order();
        // For multi-change bundles, ops have counters spanning multiple
        // changes. To make the permutation tractable, normalize to indices
        // by sorting unique counters.
        let mut sorted_counters: Vec<u64> = counters.clone();
        sorted_counters.sort();
        sorted_counters.dedup();
        // Map counter → rank
        let rank: std::collections::HashMap<u64, u32> = sorted_counters
            .iter()
            .enumerate()
            .map(|(i, c)| (*c, i as u32))
            .collect();
        let perm: Vec<u32> = counters.iter().map(|c| rank[c]).collect();
        report_change(
            &format!("{name} largest fragment ({} members, {} ops)", f.members.len(), perm.len()),
            &perm,
        );
    }
}
