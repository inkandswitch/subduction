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
//! cargo test -p sedimentree_redb_storage --test size_probe -- --ignored --nocapture
//! ```
#![allow(
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing
)]

use std::collections::BTreeSet;

use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_redb_storage::RedbStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signer::memory::MemorySigner, verified_meta::VerifiedMeta};

#[tokio::test]
#[ignore = "diagnostic probe, run manually"]
async fn inline_size_amplification_probe() {
    let signer = MemorySigner::from_bytes(&[42u8; 32]);
    let id = SedimentreeId::new([0xAB; 32]);

    // Blob sizes straddling the 64 KiB allocation boundary, accounting for
    // ~245 B of value overhead (tag + meta_len + Signed<LooseCommit>).
    for blob_size in [
        60 * 1024,  // well under 64 Ki
        65_024,     // blob + overhead just UNDER 65,536
        65_536,     // blob + overhead just OVER 65,536 (the bench case)
        96 * 1024,  // 96 Ki + overhead < 128 Ki
        128 * 1024, // 128 Ki + overhead just over 128 Ki
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = RedbStorage::with_inline_threshold(dir.path(), usize::MAX)
            .expect("create inline storage");

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
        Storage::<Sendable>::save_batch(&storage, id, commits, Vec::new())
            .await
            .expect("save batch");
        drop(storage);

        let db = dir.path().join(sedimentree_redb_storage::DB_FILE_NAME);
        let file_len = std::fs::metadata(&db).expect("metadata").len();
        let logical = 1000 * blob_size as u64;
        eprintln!(
            "blob {:>7} B  → db file {:>7.2} MiB  (logical {:>6.2} MiB, amp {:.2}x)",
            blob_size,
            file_len as f64 / (1 << 20) as f64,
            logical as f64 / (1 << 20) as f64,
            file_len as f64 / logical as f64,
        );
    }
}
