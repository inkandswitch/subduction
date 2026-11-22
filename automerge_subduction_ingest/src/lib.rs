//! Conversion from [`automerge::next`] documents to sedimentree data.

use std::collections::BTreeSet;

use automerge::next::{Automerge, Fragment as AmFragment};
use eyre::{Result, WrapErr};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

/// Automerge history expressed as a [`Sedimentree`] and its blobs.
#[derive(Debug)]
pub struct IngestResult {
    /// The fragments and loose commits, without their blobs.
    pub sedimentree: Sedimentree,
    /// Fragment blobs first, then loose-commit blobs.
    pub blobs: Vec<Blob>,
    /// Changes inside level-1+ fragments.
    pub covered_count: usize,
    /// Changes left as loose commits.
    pub loose_count: usize,
    /// Level-1+ fragments.
    pub fragment_count: usize,
}

/// Ingest a whole Automerge document into a [`Sedimentree`].
///
/// # Errors
///
/// Returns an error if automerge produces a fragment sedimentree rejects.
pub fn ingest_automerge(doc: &Automerge, sedimentree_id: SedimentreeId) -> Result<IngestResult> {
    let mut all = doc.fragments(1..);
    all.extend(doc.fragments(0..=0));
    ingest_fragments(all, sedimentree_id)
}

/// Map level-1+ automerge fragments to sedimentree [`Fragment`]s and level-0
/// fragments to [`LooseCommit`]s.
///
/// # Errors
///
/// Returns an error if a fragment's metadata is rejected by sedimentree.
pub fn ingest_fragments(
    am_fragments: Vec<AmFragment>,
    sedimentree_id: SedimentreeId,
) -> Result<IngestResult> {
    let (cached, loose): (Vec<_>, Vec<_>) =
        am_fragments.into_iter().partition(|f| f.meta.level > 0);

    let mut fragments = Vec::with_capacity(cached.len());
    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let covered_count = cached.iter().map(AmFragment::len).sum();
    for AmFragment {
        meta: f,
        bytes: raw,
    } in cached
    {
        let blob = Blob::new(raw);
        let boundary: BTreeSet<CommitId> = f.boundary.iter().map(|h| CommitId::new(h.0)).collect();
        let checkpoints: Vec<CommitId> = f.checkpoints.iter().map(|h| CommitId::new(h.0)).collect();
        let meta = BlobMeta::new(&blob);
        fragments.push(Fragment::new(
            sedimentree_id,
            CommitId::new(f.head.0),
            boundary,
            &checkpoints,
            meta,
        )?);
        blobs.push(blob);
    }

    let mut loose_commits = Vec::with_capacity(loose.len());
    for AmFragment {
        meta: f,
        bytes: raw,
    } in loose
    {
        let head = CommitId::new(f.head.0);
        let parents: BTreeSet<CommitId> = f.boundary.iter().map(|p| CommitId::new(p.0)).collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        loose_commits.push(LooseCommit::new(sedimentree_id, head, parents, meta)?);
        blobs.push(blob);
    }

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();

    Ok(IngestResult {
        sedimentree: Sedimentree::new(fragments, loose_commits),
        blobs,
        covered_count,
        loose_count,
        fragment_count,
    })
}

/// Decode 64 hex characters into 32 bytes; `name` labels the error.
///
/// # Errors
///
/// Returns an error if `hex` is not exactly 32 bytes of hex.
pub fn parse_32_bytes(hex: &str, name: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(hex)?;
    if bytes.len() != 32 {
        eyre::bail!("{name} must be 32 bytes (64 hex characters)");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

fn sed_id_from_bs58check(encoded: &str) -> Result<SedimentreeId> {
    let decoded = bs58::decode(encoded)
        .with_check(None)
        .into_vec()
        .wrap_err_with(|| format!("invalid base58check document ID: {encoded}"))?;
    if decoded.len() > 32 {
        eyre::bail!("document ID too long: {} bytes (max 32)", decoded.len());
    }
    let mut padded = [0u8; 32];
    #[allow(clippy::indexing_slicing)] // len checked above
    padded[..decoded.len()].copy_from_slice(&decoded);
    Ok(SedimentreeId::new(padded))
}

/// Parse a document ID as the [`SedimentreeId`] automerge-repo uses for it.
///
/// Accepts an `automerge:<base58check>` URL (an optional `#heads` suffix is
/// ignored), a bare base58check ID, or 64 hex characters. A base58check ID is
/// zero-padded to 32 bytes.
///
/// # Errors
///
/// Returns an error if `input` is none of those formats.
pub fn parse_doc_id(input: &str) -> Result<SedimentreeId> {
    let stripped = input.strip_prefix("automerge:").unwrap_or(input);
    let stripped = stripped.split('#').next().unwrap_or(stripped);

    if stripped.len() == 64 && stripped.chars().all(|c| c.is_ascii_hexdigit()) {
        let bytes = parse_32_bytes(stripped, "doc-id")?;
        return Ok(SedimentreeId::new(bytes));
    }

    sed_id_from_bs58check(stripped)
}
