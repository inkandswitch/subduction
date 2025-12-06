//! Build script to embed the current Git commit hash into the compiled binary.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[allow(clippy::unwrap_used)]
fn main() {
    let ws = env::var("CARGO_WORKSPACE_DIR").map_or_else(
        |_| PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()),
        PathBuf::from,
    );

    let repo_root = find_repo_root(&ws).unwrap_or(ws.clone());
    let git_dir = repo_root.join(".git");

    watch_git(&git_dir);

    let git_hash = cmd_out(
        "git",
        &[
            "-C",
            #[allow(clippy::unwrap_used)]
            repo_root.to_str().unwrap(),
            "rev-parse",
            "--short",
            "HEAD",
        ],
    )
    .unwrap_or_else(|| "unknown".to_string());

    let dirty = cmd_out(
        "git",
        &["-C", repo_root.to_str().unwrap(), "status", "--porcelain"],
    )
    .is_some_and(|s| !s.is_empty());

    let git_hash = if dirty {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        format!("{git_hash}-dirty-{secs}")
    } else {
        git_hash
    };

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
}

fn cmd_out(cmd: &str, args: &[&str]) -> Option<String> {
    Command::new(cmd).args(args).output().ok().and_then(|o| {
        if o.status.success() {
            Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
        } else {
            None
        }
    })
}

fn find_repo_root(start: &Path) -> Option<PathBuf> {
    let mut cur = Some(start);
    while let Some(dir) = cur {
        if dir.join(".git").exists() {
            return Some(dir.to_path_buf());
        }
        cur = dir.parent();
    }
    None
}

fn watch_git(git_dir: &Path) {
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());

    if let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) {
        if let Some(rest) = head.strip_prefix("ref: ").map(str::trim) {
            println!("cargo:rerun-if-changed={}", git_dir.join(rest).display());
            println!(
                "cargo:rerun-if-changed={}",
                git_dir.join("packed-refs").display()
            );
        }
    }

    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());

    let fetch_head = git_dir.join("FETCH_HEAD");
    if fetch_head.exists() {
        println!("cargo:rerun-if-changed={}", fetch_head.display());
    }
}
