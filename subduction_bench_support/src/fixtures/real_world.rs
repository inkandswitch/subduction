//! On-disk real-world CAS fixture.
//!
//! The `automerge_subduction_wasm/data/trees/` directory is a content-addressed sedimentree
//! storage dump generated locally from real user activity. It is **not** checked into git
//! (`.gitignore` excludes the top-level `data` path) and may be absent.
//!
//! Benches depending on this fixture must tolerate absence — typically by skipping with a
//! `tracing::warn!` / `eprintln!` message rather than panicking.

use std::path::{Path, PathBuf};

/// Default path to the real-world CAS fixture, relative to the workspace root.
///
/// Resolved from `CARGO_MANIFEST_DIR` at compile time so it works regardless of the current
/// working directory a bench is launched from.
const DEFAULT_RELATIVE_PATH: &str = "../automerge_subduction_wasm/data/trees";

/// Locate the real-world fixture root.
///
/// Returns `Some(path)` when the directory exists and `None` when it doesn't. Benches should
/// match on the result and skip gracefully in the `None` case.
///
/// Override the default location by setting `SUBDUCTION_REAL_WORLD_FIXTURE` to an absolute path.
#[must_use]
pub fn fixture_path() -> Option<PathBuf> {
    if let Ok(override_path) = std::env::var("SUBDUCTION_REAL_WORLD_FIXTURE") {
        let path = PathBuf::from(override_path);
        return path.is_dir().then_some(path);
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let default = Path::new(manifest_dir).join(DEFAULT_RELATIVE_PATH);

    if default.is_dir() {
        Some(default)
    } else {
        None
    }
}

/// Count the number of sedimentree roots in the fixture (top-level directory entries).
///
/// Returns `0` if the fixture is absent or unreadable.
#[must_use]
pub fn approximate_tree_count() -> usize {
    fixture_path()
        .and_then(|p| std::fs::read_dir(p).ok())
        .map_or(0, |it| it.filter_map(Result::ok).count())
}
