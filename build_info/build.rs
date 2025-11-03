use std::{
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() {
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Check if there are any uncommitted changes
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|out| !out.stdout.is_empty())
        .unwrap_or(false);

    let git_hash = if dirty {
        let human_time = Command::new("date")
            .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
            .output()
            .ok()
            .and_then(|o| {
                if o.status.success() {
                    Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let epoch_secs = duration.as_secs();
                format!("{}", epoch_secs)
            });

        format!("{git_hash}-dirty-{human_time}")
    } else {
        git_hash
    };

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
}
