//! For S1's first change, dissect both encodings — `change.bytes()` (whole-
//! chunk DEFLATE of a Change chunk) vs `bundle([h]).bytes()` (per-column
//! DEFLATE of a Bundle chunk). Both contain the same op data, organized
//! differently. Show where every byte goes.

#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use std::io::Read;

use automerge::{Automerge, ReadDoc};
use flate2::Compression;
use flate2::bufread::{DeflateDecoder, DeflateEncoder};

const HEADER_BYTES: usize = 4 + 4 + 1; // magic + checksum + chunk type byte
                                       // followed by leb128 body length

fn human(n: usize) -> String {
    let mut s = n.to_string();
    let mut out = String::new();
    while s.len() > 3 {
        let tail = s.split_off(s.len() - 3);
        out.insert_str(0, &format!(",{tail}"));
    }
    format!("{s}{out}")
}

fn deflate_len(bytes: &[u8]) -> usize {
    let mut deflater = DeflateEncoder::new(bytes, Compression::default());
    let mut out = Vec::new();
    deflater.read_to_end(&mut out).unwrap();
    out.len()
}

fn inflate(bytes: &[u8]) -> Vec<u8> {
    let mut inflater = DeflateDecoder::new(bytes);
    let mut out = Vec::new();
    inflater.read_to_end(&mut out).unwrap();
    out
}

// ---- minimal hand-rolled varint reader (LEB128 unsigned) ----
fn read_uleb(input: &[u8], pos: &mut usize) -> u64 {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        let b = input[*pos];
        *pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn read_length_prefixed<'a>(input: &'a [u8], pos: &mut usize) -> &'a [u8] {
    let len = read_uleb(input, pos) as usize;
    let bytes = &input[*pos..*pos + len];
    *pos += len;
    bytes
}

/// Returns `(header_len, body_offset)`.
fn parse_chunk_header(bytes: &[u8]) -> (usize, usize) {
    // magic (4) + checksum (4) + type (1) + leb128 length
    let mut pos = 4 + 4 + 1;
    let _body_len = read_uleb(bytes, &mut pos);
    (pos, pos)
}

#[derive(Default, Debug)]
struct RawCol {
    spec: u32,
    len: usize,
}

fn parse_raw_columns(input: &[u8], pos: &mut usize) -> (usize, Vec<RawCol>) {
    let start = *pos;
    let count = read_uleb(input, pos) as usize;
    let mut cols = Vec::with_capacity(count);
    for _ in 0..count {
        let spec = read_uleb(input, pos) as u32;
        let len = read_uleb(input, pos) as usize;
        cols.push(RawCol { spec, len });
    }
    let meta_len = *pos - start;
    (meta_len, cols)
}

fn col_id(spec: u32) -> u32 {
    spec >> 4
}
fn col_type(spec: u32) -> u32 {
    spec & 0x7
}
fn col_deflate(spec: u32) -> bool {
    spec & 0b00001000 != 0
}
fn col_name(spec: u32) -> String {
    // Op-column spec IDs (from storage/bundle/builder.rs / change_op_columns.rs)
    let id = col_id(spec);
    let ty = col_type(spec);
    let t = match ty {
        0 => "Group",
        1 => "Actor",
        2 => "UInt",
        3 => "DeltaInt",
        4 => "Boolean",
        5 => "String",
        6 => "ValueMeta",
        7 => "Value",
        _ => "?",
    };
    format!("col({id},{t})")
}

#[test]
fn dissect_s1_first_change() {
    let bytes = include_bytes!("../test-vectors/S1.am");
    let doc = Automerge::load(bytes).expect("load");

    let heads = doc.get_heads();
    let h = heads[0]; // arbitrary; both S1 changes are similar
    eprintln!("S1 head: {h:?}");

    // === change.bytes() ===
    let mut change = doc.get_change_by_hash(&h).expect("change");
    let change_compressed = change.bytes().to_vec();
    let change_raw_body = change.raw_bytes(); // raw_bytes = uncompressed chunk
    let (change_hdr_len, _) = parse_chunk_header(&change_compressed);
    let change_body_compressed = change_compressed.len() - change_hdr_len;
    let raw_body_size = change_raw_body.len() - change_hdr_len; // raw chunk body

    eprintln!("\n=== change.bytes() (whole-chunk DEFLATE) ===");
    eprintln!("  total              {:>10} B", human(change_compressed.len()));
    eprintln!("  chunk header       {:>10} B", human(change_hdr_len));
    eprintln!(
        "  body (deflated)    {:>10} B   <- single DEFLATE stream over the whole body",
        human(change_body_compressed)
    );
    eprintln!(
        "  body uncompressed  {:>10} B   ({:.2}x compression)",
        human(raw_body_size),
        raw_body_size as f64 / change_body_compressed as f64
    );

    // Parse change body to break it down further.
    // Body layout: deps_count, deps, actor (lp), seq, start_op, timestamp,
    //              message (lp), other_actors (lp list), ops_meta, ops_data, extra
    let body = &change_raw_body[change_hdr_len..];
    let mut pos = 0;
    let deps_count = read_uleb(body, &mut pos) as usize;
    let deps_size = pos + deps_count * 32 - 0;
    pos += deps_count * 32;
    let _actor = read_length_prefixed(body, &mut pos);
    let _seq = read_uleb(body, &mut pos);
    let _start_op = read_uleb(body, &mut pos);
    let _timestamp = read_uleb(body, &mut pos);
    let _message = read_length_prefixed(body, &mut pos);
    let other_actor_count = read_uleb(body, &mut pos) as usize;
    for _ in 0..other_actor_count {
        let _ = read_length_prefixed(body, &mut pos);
    }
    let metadata_size = pos;
    let (op_meta_size, op_cols) = parse_raw_columns(body, &mut pos);
    let op_data_total: usize = op_cols.iter().map(|c| c.len).sum();
    let _op_data_end = pos + op_data_total;

    eprintln!("  ── body breakdown (uncompressed) ──");
    eprintln!(
        "    change metadata  {:>10} B   (deps×{} + actor + seq + start_op + ts + msg + other_actors)",
        human(metadata_size),
        deps_count
    );
    eprintln!("    op columns meta  {:>10} B   ({} columns)", human(op_meta_size), op_cols.len());
    eprintln!("    op columns data  {:>10} B   (all columns concatenated)", human(op_data_total));

    eprintln!("  ── if each op column were DEFLATEd individually (Bundle-style) ──");
    let mut sum_individual = 0usize;
    let mut data_pos = pos;
    for c in &op_cols {
        let col_data = &body[data_pos..data_pos + c.len];
        data_pos += c.len;
        let deflated = if c.len >= 256 { deflate_len(col_data) } else { c.len };
        sum_individual += deflated;
    }
    eprintln!(
        "    sum of per-col DEFLATEd op data: {:>10} B  ({:.2}x of uncompressed)",
        human(sum_individual),
        sum_individual as f64 / op_data_total as f64
    );
    let _ = deps_size;

    // === bundle([h]).bytes() ===
    let bundle = doc.bundle([h]).expect("bundle");
    let bundle_bytes = bundle.bytes().to_vec();
    let (bundle_hdr_len, body_off) = parse_chunk_header(&bundle_bytes);
    eprintln!("\n=== bundle([h]).bytes() (per-column DEFLATE) ===");
    eprintln!("  total              {:>10} B", human(bundle_bytes.len()));

    // Parse bundle body sections.
    let mut pos = body_off;
    let prefix_start = pos;
    let deps_count = read_uleb(&bundle_bytes, &mut pos) as usize;
    pos += deps_count * 32;
    let actors_count = read_uleb(&bundle_bytes, &mut pos) as usize;
    for _ in 0..actors_count {
        let _ = read_length_prefixed(&bundle_bytes, &mut pos);
    }
    let prefix_len = pos - prefix_start;

    let (changes_meta_len, change_cols) = parse_raw_columns(&bundle_bytes, &mut pos);
    let changes_data_total: usize = change_cols.iter().map(|c| c.len).sum();
    let changes_data_start = pos;
    pos += changes_data_total;

    let (ops_meta_len, op_cols_b) = parse_raw_columns(&bundle_bytes, &mut pos);
    let ops_data_total: usize = op_cols_b.iter().map(|c| c.len).sum();
    let ops_data_start = pos;

    eprintln!("  chunk header       {:>10} B", human(bundle_hdr_len));
    eprintln!(
        "  prefix             {:>10} B   (deps×{} + actors×{})",
        human(prefix_len), deps_count, actors_count
    );
    eprintln!("  changes_meta       {:>10} B   ({} change-columns)", human(changes_meta_len), change_cols.len());
    eprintln!("  changes_data       {:>10} B   (per-col, deflate-marked or raw)", human(changes_data_total));
    eprintln!("  ops_meta           {:>10} B   ({} op-columns)", human(ops_meta_len), op_cols_b.len());
    eprintln!("  ops_data           {:>10} B   (per-col, deflate-marked or raw)", human(ops_data_total));
    eprintln!("    ── per op column breakdown ──");

    let mut data_pos = ops_data_start;
    let mut total_raw_op_cols = 0usize;
    for c in &op_cols_b {
        let col_data = &bundle_bytes[data_pos..data_pos + c.len];
        data_pos += c.len;
        let raw_size = if col_deflate(c.spec) {
            inflate(col_data).len()
        } else {
            c.len
        };
        total_raw_op_cols += raw_size;
        let tag = if col_deflate(c.spec) { "DEFLATE" } else { "       " };
        eprintln!(
            "      {:<16}  raw {:>10} B  on-disk {:>10} B  [{}]",
            col_name(c.spec),
            human(raw_size),
            human(c.len),
            tag
        );
    }
    eprintln!("    op cols raw total: {:>10} B", human(total_raw_op_cols));

    eprintln!("\n=== HEAD-TO-HEAD ===");
    eprintln!("  change.bytes() total:   {:>10} B", human(change_compressed.len()));
    eprintln!("  bundle([h]).bytes():    {:>10} B", human(bundle_bytes.len()));
    eprintln!(
        "  bundle / change ratio:  {:.2}x",
        bundle_bytes.len() as f64 / change_compressed.len() as f64
    );
}
