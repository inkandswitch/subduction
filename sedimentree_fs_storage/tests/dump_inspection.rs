//! Investigation harness for a production server dump that exposes a sync
//! convergence bug. The dump lives at `$DUMP_PATH` (default
//! `$HOME/Desktop/dump/dump`) and contains the `commits/` + `fragments/`
//! contents of a single sedimentree — i.e. it is the
//! `root/trees/<sedimentree_id_hex>/` portion of an [`FsStorage`] tree.
//!
//! These tests are `#[ignore]`'d because they require local-only data;
//! run them with:
//!
//! ```sh
//! cargo test -p sedimentree_fs_storage --test dump_inspection -- \
//!     --ignored --nocapture
//! ```
//!
//! # Hypothesis being tested
//!
//! Bad client diverges from server + good client by:
//! - missing 4 commits (`MISSING_FROM_BAD_CLIENT` below) that the good
//!   client has and the server has on disk;
//! - having 1 commit (`BAD_CLIENT_EXTRA`) that the good client doesn't.
//!
//! Symptom: server first logs "4 missing commits" then follow-up syncs
//! log "0 missing commits", yet the bad client never gains those 4. The
//! suspected mechanism is **fragment-coverage masking**: if the 4
//! "missing" commit IDs appear as boundaries or checkpoints inside
//! fragments the bad client already has, then on its next
//! `BatchSyncRequest` it will include their fingerprints (via
//! `Sedimentree::fingerprint_summarize`, which inserts fragment
//! head/boundary fingerprints into the commit set), so the server's
//! `diff_remote_fingerprints` will conclude "client already knows about
//! these CommitIds" and not re-send them — yet the underlying loose
//! commit bytes never arrived at the client.
//!
//! These tests verify that hypothesis directly against the on-disk dump.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob,
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    fragment::{Fragment, checkpoint::Checkpoint},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use tempfile::TempDir;
use testresult::TestResult;

/// Commits the **good** client has that the **bad** client doesn't —
/// i.e. the 4 commits the bad client is missing. All four are expected
/// to be in the server-side dump.
const MISSING_FROM_BAD_CLIENT: [&str; 4] = [
    "10a7276161edc6a718cf7ab7b91bb74c86ca28636ef6abaafac0728629cb6c4a",
    "2d09746e0b060b24fc70639be20acb78a193e6b94cc236ca320e41338c455f19",
    "8de3fcb38cef9aa1b9d487776a0e5072295599733b67504de7083995d3ba1f34",
    "d907e6aab4157b8a8112cd36c320c6d5731f55b9654fd0ee3513229437a69e16",
];

/// Commit the **bad** client has that the **good** client doesn't.
/// Included for symmetry / completeness in the report.
const BAD_CLIENT_EXTRA: &str = "4e818e86f71cbdfccb39034d02f55af6ca2974bc23737e26ff6aa03886ddf0b2";

fn hex_to_commit_id(h: &str) -> CommitId {
    let bytes = hex::decode(h).expect("valid hex");
    let arr: [u8; 32] = bytes.as_slice().try_into().expect("32 bytes");
    CommitId::new(arr)
}

fn dump_path() -> PathBuf {
    std::env::var_os("DUMP_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").expect("HOME set");
            PathBuf::from(home).join("Desktop/dump/dump")
        })
}

/// Build a temp directory whose `trees/<chosen_sed_id>/` is a symlink to
/// the dump dir, so [`FsStorage`] sees a normal tree layout.
fn mount_dump_as_fs_storage() -> TestResult<(TempDir, FsStorage, SedimentreeId)> {
    let dump = dump_path();
    assert!(
        dump.is_dir(),
        "dump path does not exist or is not a directory: {}",
        dump.display()
    );
    assert!(dump.join("commits").is_dir(), "no commits/ in dump");
    assert!(dump.join("fragments").is_dir(), "no fragments/ in dump");

    let root = TempDir::new()?;
    let trees = root.path().join("trees");
    std::fs::create_dir_all(&trees)?;

    // Choose an arbitrary sedimentree ID for the symlink — FsStorage just
    // treats it as an opaque hex name.
    let sed_id_bytes = [0x42u8; 32];
    let sed_id = SedimentreeId::new(sed_id_bytes);
    let target = trees.join(hex::encode(sed_id.as_bytes()));

    #[cfg(unix)]
    std::os::unix::fs::symlink(&dump, &target)?;
    #[cfg(not(unix))]
    panic!("this investigation harness requires a unix-like OS for symlink");

    let storage = FsStorage::new(root.path().to_path_buf())?;
    Ok((root, storage, sed_id))
}

/// Load all loose commits + fragments from the dump via `FsStorage`,
/// returning the verified-trusted payloads.
async fn load_dump() -> TestResult<(
    TempDir,
    SedimentreeId,
    Vec<sedimentree_core::loose_commit::LooseCommit>,
    Vec<sedimentree_core::fragment::Fragment>,
)> {
    let (root, storage, sed_id) = mount_dump_as_fs_storage()?;

    let commits = <FsStorage as Storage<Sendable>>::load_loose_commits(&storage, sed_id).await?;
    let fragments = <FsStorage as Storage<Sendable>>::load_fragments(&storage, sed_id).await?;

    let commits: Vec<_> = commits.into_iter().map(|vm| vm.payload().clone()).collect();
    let fragments: Vec<_> = fragments
        .into_iter()
        .map(|vm| vm.payload().clone())
        .collect();

    Ok((root, sed_id, commits, fragments))
}

/// Diagnostic — print top-level statistics about the dump.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH (default $HOME/Desktop/dump/dump)"]
async fn report_dump_shape() -> TestResult {
    let (_root, sed_id, commits, fragments) = load_dump().await?;

    eprintln!("=== Dump shape (chosen sedimentree_id = {sed_id:?}) ===");
    eprintln!("  loose_commits        : {}", commits.len());
    eprintln!("  fragments            : {}", fragments.len());

    let mut fragments_by_head: BTreeMap<CommitId, usize> = BTreeMap::new();
    for f in &fragments {
        *fragments_by_head.entry(f.head()).or_default() += 1;
    }
    eprintln!("  distinct frag heads  : {}", fragments_by_head.len());
    eprintln!(
        "  max frags per head   : {}",
        fragments_by_head.values().copied().max().unwrap_or(0)
    );

    let mut all_boundaries: BTreeSet<CommitId> = BTreeSet::new();
    let mut all_checkpoints: BTreeSet<Checkpoint> = BTreeSet::new();
    for f in &fragments {
        all_boundaries.extend(f.boundary().iter().copied());
        all_checkpoints.extend(f.checkpoints().iter().copied());
    }
    eprintln!("  total fragment heads : {}", fragments_by_head.len());
    eprintln!("  distinct boundaries  : {}", all_boundaries.len());
    eprintln!("  distinct checkpoints : {}", all_checkpoints.len());

    Ok(())
}

/// **Core hypothesis test.** For each of the 4 commits the bad client is
/// missing, check whether its CommitId appears in any fragment's head,
/// boundary, or checkpoint set. If it does, the bad client — having
/// already ingested those fragments — would include those CommitIds in
/// the fingerprint summary it sends on its next sync, causing the
/// server to report "0 missing commits" even though the underlying
/// loose-commit bytes never arrived.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn missing_commits_appear_as_fragment_coverage() -> TestResult {
    let (_root, _sed_id, commits, fragments) = load_dump().await?;

    let server_commit_ids: BTreeSet<CommitId> = commits.iter().map(|c| c.head()).collect();

    let frag_heads: BTreeSet<CommitId> = fragments.iter().map(|f| f.head()).collect();
    let frag_boundaries: BTreeSet<CommitId> = fragments
        .iter()
        .flat_map(|f| f.boundary().iter().copied())
        .collect();
    let frag_checkpoints: BTreeSet<Checkpoint> = fragments
        .iter()
        .flat_map(|f| f.checkpoints().iter().copied())
        .collect();

    eprintln!("=== Coverage of the 4 missing commits in server-side fragments ===");
    eprintln!(
        "(server stores {} loose commits and {} fragments)",
        commits.len(),
        fragments.len()
    );

    let mut at_least_one_covered = false;
    let mut all_covered = true;
    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        let on_server_as_loose = server_commit_ids.contains(&id);
        let is_frag_head = frag_heads.contains(&id);
        let is_frag_boundary = frag_boundaries.contains(&id);
        let is_frag_checkpoint = frag_checkpoints.contains(&Checkpoint::new(id));
        let covered = is_frag_head || is_frag_boundary || is_frag_checkpoint;
        if covered {
            at_least_one_covered = true;
        } else {
            all_covered = false;
        }
        eprintln!(
            "  {h}: loose_on_server={on_server_as_loose:5} \
             frag_head={is_frag_head:5} frag_boundary={is_frag_boundary:5} \
             frag_checkpoint={is_frag_checkpoint:5} -> covered={covered}",
        );
    }

    // Also check the bad client's extra commit:
    let extra = hex_to_commit_id(BAD_CLIENT_EXTRA);
    let extra_loose = server_commit_ids.contains(&extra);
    let extra_head = frag_heads.contains(&extra);
    let extra_boundary = frag_boundaries.contains(&extra);
    let extra_chk = frag_checkpoints.contains(&Checkpoint::new(extra));
    eprintln!(
        "\n  (bad-client-extra) {BAD_CLIENT_EXTRA}: loose_on_server={extra_loose:5} \
         frag_head={extra_head:5} frag_boundary={extra_boundary:5} \
         frag_checkpoint={extra_chk:5}",
    );

    eprintln!("\n=== Summary ===");
    eprintln!("  any of the 4 covered by fragments? {at_least_one_covered}");
    eprintln!("  ALL of the 4 covered by fragments? {all_covered}");

    if all_covered {
        eprintln!(
            "\nCOVERAGE-MASKING HYPOTHESIS SUPPORTED:\n\
             All 4 missing commit IDs appear in fragment head/boundary/checkpoint sets.\n\
             A bad client that has the fragments would echo those CommitIds in its\n\
             fingerprint summary, causing the server's diff to report '0 missing' even\n\
             though the loose-commit bytes never arrived."
        );
    } else if at_least_one_covered {
        eprintln!(
            "\nCOVERAGE-MASKING HYPOTHESIS PARTIALLY SUPPORTED:\n\
             Some (but not all) of the missing commits are covered by fragments.\n\
             Investigate the uncovered ones separately."
        );
    } else {
        eprintln!(
            "\nCOVERAGE-MASKING HYPOTHESIS REFUTED:\n\
             None of the 4 missing commits appears in any fragment head/boundary/checkpoint.\n\
             Look elsewhere — e.g. cross-platform fingerprint bug (pre-#164), wire\n\
             encoding loss, automerge-repo storage layer ignoring loose commits, or\n\
             a race in the ingest path."
        );
    }

    Ok(())
}

/// Concrete reproduction of the masking effect using `Sedimentree` /
/// `FingerprintSummary`. Construct a "bad client" tree that contains
/// the server's fragments but lacks the 4 loose commits, then simulate
/// the server's `diff_remote_fingerprints` step.
///
/// If the masking hypothesis holds, this prints zero `local_only_commits`
/// for the 4 missing IDs.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn simulate_followup_sync_with_fragments_only() -> TestResult {
    let (_root, _sed_id, commits, fragments) = load_dump().await?;

    // Server's view = everything from the dump.
    let server_tree = Sedimentree::new(fragments.clone(), commits.clone());

    // Bad client's view = the server's fragments, plus all loose commits
    // EXCEPT the 4 we know are missing.
    let missing: BTreeSet<CommitId> = MISSING_FROM_BAD_CLIENT
        .iter()
        .copied()
        .map(hex_to_commit_id)
        .collect();
    let bad_client_loose: Vec<_> = commits
        .iter()
        .filter(|c| !missing.contains(&c.head()))
        .cloned()
        .collect();
    let bad_client_tree = Sedimentree::new(fragments.clone(), bad_client_loose);

    eprintln!(
        "Server tree:     {} commits, {} fragments",
        commits.len(),
        fragments.len()
    );
    eprintln!(
        "Bad client tree: {} commits (4 omitted), {} fragments",
        commits.len() - 4,
        fragments.len()
    );

    // Use a deterministic seed so output is reproducible.
    let seed = FingerprintSeed::new(0xDEAD_BEEF, 0xCAFE_BABE);

    let bad_summary = bad_client_tree.fingerprint_summarize(&seed);
    let diff = server_tree.diff_remote_fingerprints(&bad_summary);

    let local_only_ids: BTreeSet<CommitId> =
        diff.local_only_commits.iter().map(|(id, _)| **id).collect();

    eprintln!(
        "\nServer's diff sees {} local-only commits and {} local-only fragments",
        diff.local_only_commits.len(),
        diff.local_only_fragments.len()
    );

    let mut still_visible = Vec::new();
    let mut masked = Vec::new();
    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        if local_only_ids.contains(&id) {
            still_visible.push(h);
        } else {
            masked.push(h);
        }
    }

    eprintln!("\nFor the 4 commits the bad client is missing:");
    eprintln!(
        "  server will RE-SEND ({} of 4): {still_visible:#?}",
        still_visible.len()
    );
    eprintln!(
        "  server thinks they're known ({} of 4): {masked:#?}",
        masked.len()
    );

    if masked.len() == 4 {
        eprintln!(
            "\nCONFIRMED: All 4 missing commits are masked by fragment coverage.\n\
             This matches the observed '0 missing commits' on follow-up syncs."
        );
    } else if masked.is_empty() {
        eprintln!(
            "\nNOT MASKED: server would still re-send all 4 — fragment coverage\n\
             does not explain the '0 missing commits' follow-up."
        );
    } else {
        eprintln!("\nPARTIAL: some are masked, some aren't. Look at which ones differ.");
    }

    // Also verify: for any commit that IS masked, the fingerprint of its
    // CommitId must appear in `bad_summary.commit_fingerprints()`.
    for h in &masked {
        let id = hex_to_commit_id(h);
        let fp = Fingerprint::<CommitId>::new(&seed, &id);
        assert!(
            bad_summary.commit_fingerprints().contains(&fp),
            "if server thinks {h} is known, its FP must be in client's commit-fp set"
        );
    }

    Ok(())
}

/// Bypass `FsStorage::load_fragments` (which only reads the first `.meta`
/// per head dir) and decode every `.meta` file in `fragments/<head>/`
/// directly. Returns deduplicated fragments by their `(head, boundary,
/// checkpoints)` content.
async fn load_all_fragments_directly() -> TestResult<Vec<Fragment>> {
    let dump = dump_path();
    let fragments_dir = dump.join("fragments");
    let mut out: Vec<Fragment> = Vec::new();

    let mut head_dirs = tokio::fs::read_dir(&fragments_dir).await?;
    while let Some(head_dir) = head_dirs.next_entry().await? {
        if !head_dir.file_type().await?.is_dir() {
            continue;
        }
        let head_path = head_dir.path();
        let mut entries = tokio::fs::read_dir(&head_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().to_string();
            let Some(stem) = name_str.strip_suffix(".meta") else {
                continue;
            };
            let meta_path = head_path.join(&name_str);
            let blob_path = head_path.join(format!("{stem}.blob"));
            let signed_bytes = tokio::fs::read(&meta_path).await?;
            let blob_bytes = tokio::fs::read(&blob_path).await?;
            let signed: Signed<Fragment> = Signed::try_decode(signed_bytes)?;
            let blob = Blob::new(blob_bytes);
            let vm: VerifiedMeta<Fragment> = VerifiedMeta::try_from_trusted(signed, blob)?;
            out.push(vm.payload().clone());
        }
    }
    Ok(out)
}

/// Same idea for loose commits — direct decode of every `.meta` in
/// `commits/<commit_id>/`.
async fn load_all_loose_commits_directly() -> TestResult<Vec<LooseCommit>> {
    let dump = dump_path();
    let commits_dir = dump.join("commits");
    let mut out: Vec<LooseCommit> = Vec::new();

    let mut id_dirs = tokio::fs::read_dir(&commits_dir).await?;
    while let Some(id_dir) = id_dirs.next_entry().await? {
        if !id_dir.file_type().await?.is_dir() {
            continue;
        }
        let id_path = id_dir.path();
        let mut entries = tokio::fs::read_dir(&id_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().to_string();
            let Some(stem) = name_str.strip_suffix(".meta") else {
                continue;
            };
            let meta_path = id_path.join(&name_str);
            let blob_path = id_path.join(format!("{stem}.blob"));
            let signed_bytes = tokio::fs::read(&meta_path).await?;
            let blob_bytes = tokio::fs::read(&blob_path).await?;
            let signed: Signed<LooseCommit> = Signed::try_decode(signed_bytes)?;
            let blob = Blob::new(blob_bytes);
            let vm: VerifiedMeta<LooseCommit> = VerifiedMeta::try_from_trusted(signed, blob)?;
            out.push(vm.payload().clone());
        }
    }
    Ok(out)
}

/// Per-head consistency check. `FsStorage::load_fragments` returns one
/// fragment per head directory (expected: each head dir is one logical
/// fragment, CAS-named files are different on-disk versions of the same
/// payload). For each head dir on disk, decode every `.meta`/`.blob`
/// pair and verify:
///   - all decoded fragments have IDENTICAL content (same head,
///     boundary, checkpoints, blob_meta), AND
///   - all `.blob` files in the dir have the same SHA-shaped digest
///     (file name) and the same byte contents.
///
/// If any head dir has divergent fragment metadata or divergent blob
/// bytes, that's a real bug.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn fragment_versions_within_a_head_dir_are_consistent() -> TestResult {
    use sedimentree_core::crypto::digest::Digest;

    let dump = dump_path();
    let fragments_dir = dump.join("fragments");

    let mut head_dirs = tokio::fs::read_dir(&fragments_dir).await?;
    let mut total_files = 0usize;
    let mut total_dirs = 0usize;
    let mut dirs_with_divergent_metadata = 0usize;
    let mut dirs_with_divergent_blobs = 0usize;
    let mut dirs_with_filename_mismatch = 0usize;

    while let Some(head_dir) = head_dirs.next_entry().await? {
        if !head_dir.file_type().await?.is_dir() {
            continue;
        }
        total_dirs += 1;
        let head_path = head_dir.path();
        let head_name = head_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_string();

        // Collect (filename_digest_hex, signed_bytes, blob_bytes, decoded_fragment).
        let mut entries_vec: Vec<(String, Vec<u8>, Vec<u8>, Fragment)> = Vec::new();
        let mut entries = tokio::fs::read_dir(&head_path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().to_string();
            let Some(stem) = name_str.strip_suffix(".meta") else {
                continue;
            };
            let meta_path = head_path.join(&name_str);
            let blob_path = head_path.join(format!("{stem}.blob"));
            let signed_bytes = tokio::fs::read(&meta_path).await?;
            let blob_bytes = tokio::fs::read(&blob_path).await?;
            let signed: Signed<Fragment> = Signed::try_decode(signed_bytes.clone())?;
            let blob = Blob::new(blob_bytes.clone());
            let vm: VerifiedMeta<Fragment> = VerifiedMeta::try_from_trusted(signed, blob)?;
            entries_vec.push((
                stem.to_string(),
                signed_bytes,
                blob_bytes,
                vm.payload().clone(),
            ));
        }
        total_files += entries_vec.len();

        // (1) All fragments under this head should be equal as content.
        let metadata_unique: BTreeSet<&Fragment> =
            entries_vec.iter().map(|(_, _, _, f)| f).collect();
        let metadata_unique_n = metadata_unique.len();

        // (2) All .blob bytes should be byte-equal.
        let blob_unique: BTreeSet<&[u8]> = entries_vec
            .iter()
            .map(|(_, _, bb, _)| bb.as_slice())
            .collect();
        let blob_unique_n = blob_unique.len();

        // (3) For each file, the filename's hex digest should be
        // Digest::hash(&signed_data) (i.e., the CAS digest of the
        // signed .meta bytes is what the file is named after).
        //
        // Concrete check: `Digest::<Fragment>::hash(&fragment)` should
        // produce the hex name. Note `Digest::hash` works on T:Encode
        // so we hash the decoded fragment.
        let mut filename_mismatches: Vec<(String, String)> = Vec::new();
        for (filename_stem, _signed, _blob, f) in &entries_vec {
            let computed: Digest<Fragment> = Digest::hash(f);
            let computed_hex = hex::encode(computed.as_bytes());
            if &computed_hex != filename_stem {
                filename_mismatches.push((filename_stem.clone(), computed_hex));
            }
        }

        let header = format!(
            "head dir {head_name}: {} pairs, distinct_metadata={metadata_unique_n}, \
             distinct_blob_bytes={blob_unique_n}, filename_mismatches={}",
            entries_vec.len(),
            filename_mismatches.len()
        );

        let dir_has_problems =
            metadata_unique_n > 1 || blob_unique_n > 1 || !filename_mismatches.is_empty();
        if dir_has_problems {
            eprintln!("INCONSISTENT {header}");
            if metadata_unique_n > 1 {
                dirs_with_divergent_metadata += 1;
                eprintln!(
                    "  ! {metadata_unique_n} distinct fragment payloads share head {head_name}"
                );
                for f in &metadata_unique {
                    eprintln!(
                        "    - head={:?} boundary_n={} checkpoint_n={} blob_meta={:?}",
                        f.head(),
                        f.boundary().len(),
                        f.checkpoints().len(),
                        f.summary().blob_meta()
                    );
                }
            }
            if blob_unique_n > 1 {
                dirs_with_divergent_blobs += 1;
                eprintln!("  ! {blob_unique_n} distinct .blob byte sequences in this dir");
                for b in &blob_unique {
                    let bm = sedimentree_core::blob::BlobMeta::new(&Blob::new((*b).to_vec()));
                    eprintln!("    - blob len={} digest={:?}", b.len(), bm.digest());
                }
            }
            if !filename_mismatches.is_empty() {
                dirs_with_filename_mismatch += 1;
                for (filename, computed) in &filename_mismatches {
                    eprintln!("    filename {filename} but Digest::hash(decoded)={computed}");
                }
            }
        } else {
            eprintln!("OK           {header}");
        }
    }

    eprintln!("\n=== Summary across {total_dirs} fragment head dir(s), {total_files} pairs ===");
    eprintln!("  dirs with divergent fragment METADATA: {dirs_with_divergent_metadata}");
    eprintln!("  dirs with divergent BLOB bytes       : {dirs_with_divergent_blobs}");
    eprintln!("  dirs with filename-vs-content mismatch: {dirs_with_filename_mismatch}");

    if dirs_with_divergent_metadata == 0
        && dirs_with_divergent_blobs == 0
        && dirs_with_filename_mismatch == 0
    {
        eprintln!(
            "\nALL HEAD DIRS CONSISTENT: per-head fragments share content + blob.\n\
             FsStorage::load_fragments returning the first .meta is fine here."
        );
    } else {
        eprintln!("\nINCONSISTENCY DETECTED — see above. Investigate.");
    }
    Ok(())
}

/// With **all** fragments (not just the one FsStorage returns), do the
/// 4 missing commits appear as fragment head/boundary/checkpoint?
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn coverage_check_against_all_fragments_on_disk() -> TestResult {
    let fragments = load_all_fragments_directly().await?;
    let commits = load_all_loose_commits_directly().await?;

    let server_commit_ids: BTreeSet<CommitId> = commits.iter().map(|c| c.head()).collect();
    let frag_heads: BTreeSet<CommitId> = fragments.iter().map(|f| f.head()).collect();
    let frag_boundaries: BTreeSet<CommitId> = fragments
        .iter()
        .flat_map(|f| f.boundary().iter().copied())
        .collect();
    let frag_checkpoints: BTreeSet<Checkpoint> = fragments
        .iter()
        .flat_map(|f| f.checkpoints().iter().copied())
        .collect();

    eprintln!(
        "=== Coverage check using ALL {} fragments on disk ===",
        fragments.len()
    );
    eprintln!(
        "  distinct heads={}, distinct boundaries={}, distinct checkpoints={}",
        frag_heads.len(),
        frag_boundaries.len(),
        frag_checkpoints.len()
    );

    let mut covered = 0;
    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        let loose = server_commit_ids.contains(&id);
        let head = frag_heads.contains(&id);
        let boundary = frag_boundaries.contains(&id);
        let chk = frag_checkpoints.contains(&Checkpoint::new(id));
        let any = head || boundary || chk;
        if any {
            covered += 1;
        }
        eprintln!(
            "  {h}: loose={loose:5} frag_head={head:5} \
             frag_boundary={boundary:5} frag_checkpoint={chk:5} -> covered={any}",
        );
    }
    eprintln!("\n  {covered}/4 missing commits covered by SOME fragment on disk.");

    Ok(())
}

/// Detailed report on the 4 missing commits' parent chains. If their
/// parents are missing from the server too, or if they're ancestors of
/// other commits the client has, the DAG-ancestry pruning in
/// `Sedimentree::diff_remote_fingerprints` could be hiding them.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn inspect_parents_of_missing_commits() -> TestResult {
    let commits = load_all_loose_commits_directly().await?;
    let by_head: BTreeMap<CommitId, &LooseCommit> = commits.iter().map(|c| (c.head(), c)).collect();

    let missing: BTreeSet<CommitId> = MISSING_FROM_BAD_CLIENT
        .iter()
        .copied()
        .map(hex_to_commit_id)
        .collect();
    let extra = hex_to_commit_id(BAD_CLIENT_EXTRA);

    eprintln!("=== Parent inspection ===");
    eprintln!("Server has {} loose commits.", commits.len());

    // Build child map (parent -> children).
    let mut children_of: BTreeMap<CommitId, Vec<CommitId>> = BTreeMap::new();
    for c in &commits {
        for p in c.parents() {
            children_of.entry(*p).or_default().push(c.head());
        }
    }

    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        let commit = by_head[&id];
        let parents: Vec<_> = commit.parents().iter().copied().collect();
        let parents_on_server: Vec<_> =
            parents.iter().filter(|p| by_head.contains_key(p)).collect();
        let children: Vec<_> = children_of.get(&id).cloned().unwrap_or_default();
        let children_on_server: Vec<_> = children
            .iter()
            .filter(|c| by_head.contains_key(c))
            .collect();
        eprintln!(
            "  {h}:\n    parents on server: {}/{}\n    children on server: {}/{}",
            parents_on_server.len(),
            parents.len(),
            children_on_server.len(),
            children.len()
        );
        eprintln!("      parents:  {parents:#?}");
        eprintln!("      children: {children:#?}");
    }

    eprintln!("\n  (bad-client-extra) {BAD_CLIENT_EXTRA}:");
    if let Some(extra_commit) = by_head.get(&extra) {
        let parents: Vec<_> = extra_commit.parents().iter().copied().collect();
        eprintln!("    parents on server: {parents:#?}");
        let n = parents.iter().filter(|p| missing.contains(p)).count();
        eprintln!(
            "    of which {n} are in the missing-from-bad-client set ({}/{})",
            n,
            parents.len()
        );
    } else {
        eprintln!("    not present on server");
    }

    // Cross-reference: for each missing commit, is it an ancestor of
    // anything the bad client (which has all-except-these-4-plus-extra)
    // would also have?
    let bad_client_loose: BTreeSet<CommitId> = commits
        .iter()
        .map(|c| c.head())
        .filter(|h| !missing.contains(h))
        .collect();
    eprintln!(
        "\n  Bad client (simulated) holds {} loose commits.",
        bad_client_loose.len()
    );

    // Manual ancestor/descendant walk over the loose-commit DAG.
    let server_tree = Sedimentree::new(Vec::new(), commits.clone());
    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        let descendants = descendants_via_children(&id, &children_of);
        let descendants_in_bad: usize = descendants
            .iter()
            .filter(|d| bad_client_loose.contains(d))
            .count();
        let mut root_set = sedimentree_core::collections::Set::default();
        root_set.insert(id);
        let ancestors = server_tree.ancestors_of(&root_set);
        let ancestors_in_bad: usize = ancestors
            .iter()
            .filter(|a| **a != id && bad_client_loose.contains(*a))
            .count();
        eprintln!(
            "    {h}: descendants_total={} descendants_in_bad_client={} \
             ancestors_in_bad_client={}",
            descendants.len(),
            descendants_in_bad,
            ancestors_in_bad,
        );
    }
    Ok(())
}

fn descendants_via_children(
    root: &CommitId,
    children_of: &BTreeMap<CommitId, Vec<CommitId>>,
) -> BTreeSet<CommitId> {
    let mut out = BTreeSet::new();
    let mut stack = vec![*root];
    while let Some(c) = stack.pop() {
        if let Some(kids) = children_of.get(&c) {
            for k in kids {
                if out.insert(*k) {
                    stack.push(*k);
                }
            }
        }
    }
    out
}

/// Most direct simulation: build the bad-client tree, build the
/// server tree, run `diff_remote_fingerprints`, and print exactly what
/// the server would send back. This is the closest in-process
/// reproduction we can do of "is the server's diff logic itself failing
/// to enumerate the 4 missing commits?"
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn simulate_diff_with_realistic_bad_client_state() -> TestResult {
    let commits = load_all_loose_commits_directly().await?;
    let fragments = load_all_fragments_directly().await?;

    let missing: BTreeSet<CommitId> = MISSING_FROM_BAD_CLIENT
        .iter()
        .copied()
        .map(hex_to_commit_id)
        .collect();
    let extra = hex_to_commit_id(BAD_CLIENT_EXTRA);

    // Bad client: server's loose commits MINUS the 4 missing, PLUS the
    // extra commit (which is also on the server). All fragments.
    let bad_client_loose: Vec<LooseCommit> = commits
        .iter()
        .filter(|c| !missing.contains(&c.head()))
        .cloned()
        .collect();
    eprintln!(
        "Bad client setup: {} loose commits ({} omitted, extra=`{}` is on server too)",
        bad_client_loose.len(),
        missing.len(),
        BAD_CLIENT_EXTRA,
    );
    eprintln!(
        "  (bad-client-extra is on the server? {})",
        commits.iter().any(|c| c.head() == extra)
    );

    let server_tree = Sedimentree::new(fragments.clone(), commits.clone());
    let bad_client_tree = Sedimentree::new(fragments.clone(), bad_client_loose);

    let seed = FingerprintSeed::new(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
    let bad_summary = bad_client_tree.fingerprint_summarize(&seed);
    let diff = server_tree.diff_remote_fingerprints(&bad_summary);

    eprintln!(
        "Server diff: {} local_only_commits, {} local_only_fragments, \
         {} remote_only_commit_fps, {} remote_only_fragment_fps",
        diff.local_only_commits.len(),
        diff.local_only_fragments.len(),
        diff.remote_only_commit_fingerprints.len(),
        diff.remote_only_fragment_fingerprints.len()
    );

    let local_only_ids: BTreeSet<CommitId> =
        diff.local_only_commits.iter().map(|(id, _)| **id).collect();
    let mut would_send = Vec::new();
    let mut would_not_send = Vec::new();
    for h in MISSING_FROM_BAD_CLIENT {
        let id = hex_to_commit_id(h);
        if local_only_ids.contains(&id) {
            would_send.push(h);
        } else {
            would_not_send.push(h);
        }
    }
    eprintln!(
        "  of the 4 missing-from-bad-client commits:\n    would_send: {would_send:#?}\n    would_NOT_send: {would_not_send:#?}",
    );

    if would_not_send.len() == 4 {
        eprintln!(
            "\nKEY FINDING: diff_remote_fingerprints excludes all 4 missing commits.\n\
             Inspect ancestry pruning at sedimentree.rs:740-757."
        );
    }
    if would_send.len() == 4 {
        eprintln!(
            "\nFinding: diff_remote_fingerprints WOULD enumerate all 4 missing commits.\n\
             The bug must be downstream of the server's diff — either wire encoding\n\
             dropped them, the network lost them, or the client's ingest path silently\n\
             rejected them."
        );
    }

    Ok(())
}

/// Drill into "why are the 171 fragments different?". Compares them
/// pairwise on (head, boundary, checkpoint set size, blob_meta.digest,
/// blob_meta.size). If they all share the same head but each carries a
/// *different* blob, the data they describe is genuinely distinct —
/// hydration via `load_fragments` would silently pick one of them and
/// throw away 170 logical fragments' worth of state.
#[tokio::test]
#[ignore = "requires local dump at $DUMP_PATH"]
async fn characterize_171_fragment_divergence() -> TestResult {
    let fragments = load_all_fragments_directly().await?;

    let heads: BTreeSet<CommitId> = fragments.iter().map(|f| f.head()).collect();
    let boundary_signatures: BTreeSet<Vec<CommitId>> = fragments
        .iter()
        .map(|f| f.boundary().iter().copied().collect::<Vec<_>>())
        .collect();
    let checkpoint_set_sizes: BTreeSet<usize> =
        fragments.iter().map(|f| f.checkpoints().len()).collect();
    let blob_digests: BTreeSet<_> = fragments
        .iter()
        .map(|f| f.summary().blob_meta().digest())
        .collect();
    let blob_sizes: BTreeSet<u64> = fragments
        .iter()
        .map(|f| f.summary().blob_meta().size_bytes())
        .collect();
    let sed_ids: BTreeSet<SedimentreeId> = fragments.iter().map(|f| f.sedimentree_id()).collect();

    eprintln!("=== Diversity across the 171 fragment payloads ===");
    eprintln!("  distinct heads               : {}", heads.len());
    eprintln!(
        "  distinct boundary signatures : {}",
        boundary_signatures.len()
    );
    eprintln!(
        "  distinct checkpoint-set sizes: {:?}",
        checkpoint_set_sizes
    );
    eprintln!("  distinct blob digests        : {}", blob_digests.len());
    eprintln!("  distinct blob sizes          : {}", blob_sizes.len());
    eprintln!("  distinct sedimentree_ids     : {}", sed_ids.len());

    // Print a sample of distinct fragments.
    let mut by_blob: BTreeMap<sedimentree_core::crypto::digest::Digest<Blob>, &Fragment> =
        BTreeMap::new();
    for f in &fragments {
        by_blob.entry(f.summary().blob_meta().digest()).or_insert(f);
    }
    eprintln!("\n  Sample of first 5 distinct fragments by blob digest:");
    for f in by_blob.values().take(5) {
        eprintln!(
            "    head={:?} sed_id={:?} boundary_n={} checkpoint_n={} blob_size={} blob_digest={:?}",
            f.head(),
            f.sedimentree_id(),
            f.boundary().len(),
            f.checkpoints().len(),
            f.summary().blob_meta().size_bytes(),
            f.summary().blob_meta().digest()
        );
    }

    // Now check: if we load via `FsStorage`, do we get a single (possibly
    // arbitrary) one of these? Run several times and confirm the choice
    // is dependent on filesystem readdir order (which on Linux ext4 is
    // typically stable but not guaranteed).
    let mut loaded_digests: BTreeSet<_> = BTreeSet::new();
    for _ in 0..5 {
        let (_root, _sed_id, _commits, frags) = load_dump().await?;
        for f in &frags {
            loaded_digests.insert(f.summary().blob_meta().digest());
        }
    }
    eprintln!(
        "\n  Over 5 FsStorage::load_fragments calls, picked {} distinct blob(s).",
        loaded_digests.len()
    );

    if blob_digests.len() > 1 {
        eprintln!(
            "\nBUG: 171 .meta files in one head dir carry {} different fragments \
             (different blob digests). FsStorage::load_fragments returns ONE.\n\
             - If the protocol is supposed to merge multiple fragments sharing a head,\n\
               hydration loses 170 of them.\n\
             - If only one is supposed to exist per head, then save_fragment's\n\
               CAS-by-digest is letting multiple be persisted that shouldn't be —\n\
               (different inputs collide on head, then each gets its own .meta file).\n\
             Either way, the four missing client commits could be members/ancestors\n\
             of fragments that were stored but never loaded.",
            blob_digests.len()
        );
    }

    Ok(())
}

/// Sanity check on #164's fix. If this fails, the fingerprint of a fixed
/// `CommitId` differs from a known good native value, which is the bug.
#[test]
fn fingerprint_baseline_for_known_commit_id() {
    let seed = FingerprintSeed::new(0x1234_5678_9ABC_DEF0, 0xFEDC_BA98_7654_3210);
    let id = CommitId::new([0u8; 32]);
    let fp: Fingerprint<CommitId> = Fingerprint::<CommitId>::new(&seed, &id);

    // Matches `sedimentree_core/tests/fingerprint_stability.rs::EXPECTED_FP_ZEROES`.
    assert_eq!(fp.as_u64(), 6_748_340_123_268_596_282);
}
