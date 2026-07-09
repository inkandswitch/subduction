//! Embed the git commit hash for the `subduction_build_info` metric.
//!
//! Resolution order: the `SUBDUCTION_GIT_SHA` environment variable (set by
//! the Nix package, whose source tree has no `.git`), then `git rev-parse`,
//! then `"unknown"`.

use std::{env, fs, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=SUBDUCTION_GIT_SHA");

    let sha = env::var("SUBDUCTION_GIT_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=SUBDUCTION_GIT_SHA={sha}");

    watch_git(Path::new("../.git"));

    // `tokio_unstable` is set via `.cargo/config.toml` rustflags, not a
    // feature; declare it so `unexpected_cfgs` accepts the runtime-metrics
    // sampling code.
    println!("cargo:rustc-check-cfg=cfg(tokio_unstable)");
}

fn git_sha() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

/// Re-run when the checked-out commit changes. `HEAD` alone is not enough:
/// on a branch it contains only the ref *name*, which committing does not
/// touch — the resolved ref file (or `packed-refs`) is what moves.
fn watch_git(git_dir: &Path) {
    let head = git_dir.join("HEAD");
    println!("cargo:rerun-if-changed={}", head.display());

    if let Ok(contents) = fs::read_to_string(&head)
        && let Some(ref_path) = contents.strip_prefix("ref: ").map(str::trim)
    {
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join(ref_path).display()
        );
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join("packed-refs").display()
        );
    }
}
