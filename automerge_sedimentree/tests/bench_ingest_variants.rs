//! Comparative timing for the three ingest variants on the largest egwalker
//! vector (C2: 134k changes, 542 fragments). Run with:
//!
//! ```text
//! cargo test --release -p automerge_sedimentree --test bench_ingest_variants \
//!   --features rayon -- --ignored --nocapture
//! ```
//!
//! Reports min / median / max wall-clock over `ITERATIONS` per variant.
//! Variants compared:
//!
//! - `seq` — [`ingest_automerge`] (two `bundle_fragments` calls, no rayon)
//! - `par` — [`ingest_automerge_par`] (one `bundle_fragments(once(f))` per
//!    fragment, parallelized via `par_iter`)
//! - `chk` — [`ingest_automerge_par_chunked`] (fragments split into
//!    `num_threads` chunks, one `bundle_fragments(chunk)` per worker)

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]
#![cfg(feature = "rayon")]

use std::time::{Duration, Instant};

use automerge::Automerge;
use automerge_sedimentree::ingest::{
    ingest_automerge, ingest_automerge_par, ingest_automerge_par_chunked,
};
use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};

const ITERATIONS: usize = 3;

fn sed_id(bytes: &[u8]) -> SedimentreeId {
    let d: Digest<Blob> = Digest::hash(&Blob::new(bytes.to_vec()));
    SedimentreeId::new(*d.as_bytes())
}

fn fmt_ms(d: Duration) -> String {
    format!("{:>8.2}", d.as_secs_f64() * 1000.0)
}

fn time_variant<F>(label: &str, mut f: F) -> (Duration, Duration, Duration)
where
    F: FnMut() -> usize,
{
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let start = Instant::now();
        let fragment_count = f();
        let elapsed = start.elapsed();
        eprintln!(
            "  {label} iter {i}: {} ms ({} fragments)",
            fmt_ms(elapsed),
            fragment_count
        );
        samples.push(elapsed);
    }
    samples.sort();
    let min = samples[0];
    let median = samples[samples.len() / 2];
    let max = samples[samples.len() - 1];
    (min, median, max)
}

#[test]
#[ignore = "perf benchmark; run with --ignored --nocapture in release"]
fn bench_c2_ingest_variants() {
    let bytes = include_bytes!("../test-vectors/C2.am");
    let doc = Automerge::load(bytes).expect("load C2");
    let id = sed_id(bytes);

    eprintln!(
        "C2: changes={}, threads={}",
        doc.get_changes_meta(&[]).len(),
        rayon::current_num_threads()
    );

    let (seq_min, seq_med, seq_max) =
        time_variant("seq", || ingest_automerge(&doc, id).fragment_count);
    let (par_min, par_med, par_max) =
        time_variant("par", || ingest_automerge_par(&doc, id).fragment_count);
    let (chk_min, chk_med, chk_max) = time_variant("chk", || {
        ingest_automerge_par_chunked(&doc, id).fragment_count
    });

    eprintln!();
    eprintln!("=== C2 ingest variants ({ITERATIONS} iterations each) ===");
    eprintln!("variant   min(ms)   med(ms)   max(ms)   speedup_vs_seq");
    for (label, min, med, max) in [
        ("seq", seq_min, seq_med, seq_max),
        ("par", par_min, par_med, par_max),
        ("chk", chk_min, chk_med, chk_max),
    ] {
        let speedup = seq_med.as_secs_f64() / med.as_secs_f64();
        eprintln!(
            "{label:<6}  {}  {}  {}     {speedup:>5.2}x",
            fmt_ms(min),
            fmt_ms(med),
            fmt_ms(max),
        );
    }
}
