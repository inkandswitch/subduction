//! Helpers shared by the CLI's end-to-end tests.
//!
//! This is `common/mod.rs` rather than `common.rs` because Cargo builds every
//! top-level file in `tests/` as its own test target.

use std::{collections::HashMap, path::Path, time::Duration};

/// How often to look for a server's ready file.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The `key=value` lines of the ready file a `subduction_cli server` writes
/// at `path` once it is listening, or `None` if none appears within
/// `timeout`.
///
/// The server writes the file in one step (write, then rename), so whatever is
/// read is complete.
pub(crate) async fn wait_for_ready_file(
    path: &Path,
    timeout: Duration,
) -> Option<HashMap<String, String>> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if let Ok(content) = std::fs::read_to_string(path) {
            return Some(
                content
                    .lines()
                    .filter_map(|line| line.split_once('='))
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            );
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
    None
}
