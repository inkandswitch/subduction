//! Diagnostic probe: inline value size vs on-disk footprint.
//!
//! redb allocates value storage in power-of-two sized regions (buddy
//! allocator), so an inline value's footprint is its size rounded UP to the
//! next power of two. The worst case is a value just past a boundary —
//! exactly what a power-of-two-sized blob plus our ~245 B of record
//! overhead (tag + `meta_len` + `Signed<LooseCommit>`) produces:
//!
//! ```text
//! blob   61440 B → db  64.25 MiB (amp 1.10x)
//! blob   65024 B → db  64.25 MiB (amp 1.04x)   just under the 64 Ki boundary
//! blob   65536 B → db 128.50 MiB (amp 2.06x)   just over → 128 Ki per value
//! blob   98304 B → db 128.50 MiB (amp 1.37x)
//! blob  131072 B → db 257.00 MiB (amp 2.06x)   doubles again at 128 Ki
//! ```
//!
//! This is *internal* fragmentation in live allocations: `compact()` cannot
//! reclaim it. It is the quantitative argument for the hybrid's external
//! blob files (exact-size + 4 KiB rounding) over inline storage of large
//! blobs, and for the default 16 KiB inline threshold.
//!
//! Run with:
//!
//! ```text
//! cargo test -p subduction_redb_storage --test size_probe -- --ignored --nocapture
//! ```
#![allow(
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};
use subduction_redb_storage::RedbStorage;

/// Build an inline-only store holding `records` commits of `blob_size`
/// bytes each, returning the db file's length.
async fn inline_db_size(records: u32, blob_size: usize) -> testresult::TestResult<u64> {
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let id = SedimentreeId::new([0xAB; 32]);
    let dir = tempfile::tempdir()?;
    let storage = RedbStorage::with_inline_threshold(dir.path(), usize::MAX)?;

    let mut commits = Vec::new();
    for i in 0..records {
        let mut head = [0u8; 32];
        head[..4].copy_from_slice(&i.to_be_bytes());
        let mut blob = vec![0u8; blob_size];
        blob[..4].copy_from_slice(&i.to_be_bytes());
        commits.push(
            VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
                &signer,
                (id, CommitId::new(head), BTreeSet::new()),
                VerifiedBlobMeta::new(Blob::new(blob)),
            )
            .await,
        );
    }
    Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new()).await?;
    drop(storage);

    Ok(std::fs::metadata(dir.path().join(subduction_redb_storage::DB_FILE_NAME))?.len())
}

/// External-assumption tripwire — **not** a behavior test of this crate. It
/// pins a property of *redb's* buddy allocator that `DEFAULT_INLINE_THRESHOLD`
/// relies on: an inline value just *past* a power-of-two boundary allocates
/// double, one just *under* allocates snugly. A failure means a redb upgrade
/// changed its allocation strategy, so the threshold analysis in
/// `.ignore/DECISIONS.md` needs redoing — it does **not** indicate a bug in our
/// code.
///
/// 100 records (~6–13 MiB of I/O) keep it cheap enough for every `cargo test`;
/// the full five-point sweep lives in the `#[ignore]`d probe below.
#[tokio::test]
async fn buddy_allocation_doubles_past_power_of_two_boundary() -> testresult::TestResult {
    const RECORDS: u32 = 100;

    // Blob + ~245 B record overhead lands just under / just over 64 Ki.
    let under = inline_db_size(RECORDS, 65_024).await?;
    let over = inline_db_size(RECORDS, 65_536).await?;

    let amp_under = under as f64 / (u64::from(RECORDS) * 65_024) as f64;
    let amp_over = over as f64 / (u64::from(RECORDS) * 65_536) as f64;
    eprintln!("amp just under 64 Ki: {amp_under:.2}x; just over: {amp_over:.2}x");

    assert!(
        amp_over >= 1.8,
        "expected ~2x amplification just past the 64 Ki boundary, got {amp_over:.2}x \
         — redb's allocation behavior has changed; revisit the inline threshold analysis"
    );
    assert!(
        amp_under <= 1.25,
        "expected near-1x amplification just under the 64 Ki boundary, got {amp_under:.2}x \
         — redb's allocation behavior has changed; revisit the inline threshold analysis"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "diagnostic probe, run manually"]
async fn inline_size_amplification_probe() -> testresult::TestResult {
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let id = SedimentreeId::new([0xAB; 32]);

    let mut amp_at = std::collections::BTreeMap::new();

    // Blob sizes straddling the 64 KiB allocation boundary, accounting for
    // ~245 B of value overhead (tag + meta_len + Signed<LooseCommit>).
    for blob_size in [
        60 * 1024,  // well under 64 Ki
        65_024,     // blob + overhead just UNDER 65,536
        65_536,     // blob + overhead just OVER 65,536 (the bench case)
        96 * 1024,  // 96 Ki + overhead < 128 Ki
        128 * 1024, // 128 Ki + overhead just over 128 Ki
    ] {
        let dir = tempfile::tempdir()?;
        let storage = RedbStorage::with_inline_threshold(dir.path(), usize::MAX)?;

        let mut commits = Vec::new();
        for i in 0..1000u32 {
            let mut head = [0u8; 32];
            head[..4].copy_from_slice(&i.to_be_bytes());
            let mut blob = vec![0u8; blob_size];
            blob[..4].copy_from_slice(&i.to_be_bytes());
            commits.push(
                VerifiedMeta::<LooseCommit>::seal::<Sendable, _>(
                    &signer,
                    (id, CommitId::new(head), BTreeSet::new()),
                    VerifiedBlobMeta::new(Blob::new(blob)),
                )
                .await,
            );
        }
        Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new()).await?;
        drop(storage);

        let db = dir.path().join(subduction_redb_storage::DB_FILE_NAME);
        let file_len = std::fs::metadata(&db)?.len();
        let logical = 1000 * blob_size as u64;
        let amp = file_len as f64 / logical as f64;
        eprintln!(
            "blob {:>7} B  → db file {:>7.2} MiB  (logical {:>6.2} MiB, amp {:.2}x)",
            blob_size,
            file_len as f64 / (1 << 20) as f64,
            logical as f64 / (1 << 20) as f64,
            amp,
        );
        amp_at.insert(blob_size, amp);
    }

    // Pin the buddy-allocator conclusion that justifies
    // `DEFAULT_INLINE_THRESHOLD` and the external-blob design: crossing a
    // power-of-two boundary by a few hundred bytes of record overhead
    // doubles the per-value allocation (~2x amplification), while sitting
    // just under it costs almost nothing. If a redb upgrade changes its
    // allocation strategy, these tripwires say the analysis needs redoing.
    assert!(
        amp_at[&65_536] >= 1.9,
        "expected ~2x amplification just past the 64 Ki boundary, got {:.2}x \
         — redb's allocation behavior has changed; revisit the inline threshold analysis",
        amp_at[&65_536]
    );
    assert!(
        amp_at[&65_024] <= 1.15,
        "expected near-1x amplification just under the 64 Ki boundary, got {:.2}x \
         — redb's allocation behavior has changed; revisit the inline threshold analysis",
        amp_at[&65_024]
    );

    Ok(())
}
