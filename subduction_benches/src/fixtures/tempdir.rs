//! Temporary-directory helpers for filesystem storage benches.
//!
//! Wraps `tempfile::TempDir` with a canonical layout so benches don't each invent their own.

use std::path::PathBuf;

use tempfile::TempDir;

/// A guarded temp directory for filesystem storage benches.
///
/// The directory is deleted when the guard drops. `path()` gives the root that should be passed
/// to `sedimentree_fs_storage::FsStorage::new(...)`.
#[derive(Debug)]
pub struct TempRoot {
    inner: TempDir,
}

impl TempRoot {
    /// Create a new temp root with the default prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the system temp directory is not writable.
    pub fn new() -> std::io::Result<Self> {
        let inner = tempfile::Builder::new()
            .prefix("subduction-bench-")
            .tempdir()?;
        Ok(Self { inner })
    }

    /// The absolute path to the temp root.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }
}
