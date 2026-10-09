//! The workspace as `cargo metadata` reports it, and the manifest edits a
//! version bump needs.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
};

use semver::{Comparator, Op, Version, VersionReq};
use serde::Deserialize;
use thiserror::Error;
use toml_edit::{DocumentMut, Item, TableLike, Value};

/// A workspace member.
#[derive(Debug)]
pub(crate) struct Crate {
    pub(crate) name: String,
    pub(crate) version: Version,
    publishable: bool,
    manifest: PathBuf,
    dependencies: Vec<Dependency>,
}

#[derive(Debug, Deserialize)]
struct Dependency {
    name: String,
    req: VersionReq,
    kind: Option<DependencyKind>,
    path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum DependencyKind {
    Build,
    Dev,
}

/// The workspace members and root.
#[derive(Debug)]
pub(crate) struct Workspace {
    root: PathBuf,
    crates: Vec<Crate>,
}

impl Workspace {
    /// Read the workspace members from `cargo metadata`.
    pub(crate) fn load() -> Result<Self, WorkspaceError> {
        let output = Command::new(super::env_cargo())
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .output()
            .map_err(WorkspaceError::Spawn)?;
        if !output.status.success() {
            return Err(WorkspaceError::Metadata {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        let metadata: Metadata = serde_json::from_slice(&output.stdout)?;
        Ok(Self {
            root: metadata.workspace_root,
            crates: metadata.packages.into_iter().map(Crate::from).collect(),
        })
    }

    /// Members that publish to crates.io.
    pub(crate) fn publishable(&self) -> impl Iterator<Item = &Crate> {
        self.crates.iter().filter(|krate| krate.publishable)
    }

    /// Fail if `Cargo.lock` does not match the manifests.
    ///
    /// Resolves the full dependency graph (with `--no-deps`, `--locked` checks
    /// nothing), so it may download dependency manifests not yet cached.
    pub(crate) fn check_lockfile(&self) -> Result<(), WorkspaceError> {
        let output = Command::new(super::env_cargo())
            .args(["metadata", "--format-version", "1", "--locked"])
            .current_dir(&self.root)
            .stdout(Stdio::null())
            .output()
            .map_err(WorkspaceError::Spawn)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(WorkspaceError::LockedMetadata(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    /// Require every publishable crate's non-dev dependencies on workspace
    /// members to be publishable and to require exactly `^<current version>`.
    ///
    /// A requirement below the current version would let users who update one
    /// crate keep an older release of a sibling it was not tested with.
    pub(crate) fn check_dependency_floors(&self) -> Result<(), WorkspaceError> {
        let members: BTreeMap<&str, &Crate> = self
            .crates
            .iter()
            .map(|krate| (krate.name.as_str(), krate))
            .collect();

        let problems: Vec<String> = self
            .publishable()
            .flat_map(|krate| {
                krate
                    .dependencies
                    .iter()
                    .filter(|dep| dep.path.is_some() && dep.kind != Some(DependencyKind::Dev))
                    .filter_map(|dep| floor_problem(krate, dep, members.get(dep.name.as_str())))
            })
            .collect();

        if problems.is_empty() {
            Ok(())
        } else {
            Err(WorkspaceError::Floors(problems))
        }
    }

    /// Current bytes of every manifest a bump may touch, plus `Cargo.lock`.
    pub(crate) fn snapshot_manifests(&self) -> Result<Snapshot, WorkspaceError> {
        let paths = self
            .crates
            .iter()
            .map(|krate| krate.manifest.clone())
            .chain([self.root.join("Cargo.toml"), self.root.join("Cargo.lock")]);
        let files = paths
            .map(|path| read(&path).map(|bytes| (path, bytes)))
            .collect::<Result<_, _>>()?;
        Ok(Snapshot { files })
    }

    /// Set each crate's `package.version`, and the `version` of its entry in
    /// the root `[workspace.dependencies]` when it has one.
    ///
    /// Every edited field must already be a version string, so the edit cannot
    /// silently skip a field or replace `version.workspace = true`.
    pub(crate) fn write_versions(&self, bumps: &[(&Crate, Version)]) -> Result<(), WorkspaceError> {
        let root_manifest = self.root.join("Cargo.toml");
        let mut root = parse(&root_manifest)?;

        for (krate, version) in bumps {
            let new = version.to_string();
            let mut manifest = parse(&krate.manifest)?;
            let package = manifest
                .get_mut("package")
                .and_then(Item::as_table_like_mut);
            if !package.is_some_and(|package| set_string(package, "version", &new)) {
                return Err(WorkspaceError::NoVersionString {
                    path: krate.manifest.clone(),
                    field: "package.version".to_owned(),
                });
            }
            write(&krate.manifest, &manifest)?;

            let Some(entry) = root
                .get_mut("workspace")
                .and_then(|workspace| workspace.get_mut("dependencies"))
                .and_then(|dependencies| dependencies.get_mut(&krate.name))
            else {
                continue;
            };
            if !entry
                .as_table_like_mut()
                .is_some_and(|entry| set_string(entry, "version", &new))
            {
                return Err(WorkspaceError::NoVersionString {
                    path: root_manifest,
                    field: format!("workspace.dependencies.{}.version", krate.name),
                });
            }
        }
        write(&root_manifest, &root)?;
        Ok(())
    }

    /// A workspace of `(name, version, publishable)` members with no
    /// dependencies, rooted nowhere. For tests that do not touch files.
    #[cfg(test)]
    pub(crate) fn for_test(members: &[(&str, &str, bool)]) -> Self {
        Self {
            root: PathBuf::new(),
            crates: members
                .iter()
                .map(|&(name, version, publishable)| Crate {
                    name: name.to_owned(),
                    version: Version::parse(version).unwrap_or_else(|_| Version::new(0, 0, 0)),
                    publishable,
                    manifest: PathBuf::new(),
                    dependencies: Vec::new(),
                })
                .collect(),
        }
    }
}

/// `^<version>`, the requirement a sibling should declare.
fn caret(version: &Version) -> VersionReq {
    VersionReq {
        comparators: vec![Comparator {
            op: Op::Caret,
            major: version.major,
            minor: Some(version.minor),
            patch: Some(version.patch),
            pre: version.pre.clone(),
        }],
    }
}

fn floor_problem(krate: &Crate, dep: &Dependency, target: Option<&&Crate>) -> Option<String> {
    let Some(target) = target else {
        return Some(format!(
            "{} depends on {} by path, outside the workspace",
            krate.name, dep.name
        ));
    };
    if !target.publishable {
        return Some(format!(
            "{} depends on unpublished {}",
            krate.name, dep.name
        ));
    }
    let expected = caret(&target.version);
    (dep.req != expected).then(|| {
        format!(
            "{} requires {} {}, expected {expected}",
            krate.name, dep.name, dep.req
        )
    })
}

/// Replace a string value, keeping its surrounding whitespace and comments.
/// Returns `false`, changing nothing, if the key is missing or not a string.
fn set_string(table: &mut dyn TableLike, key: &str, new: &str) -> bool {
    let Some(old) = table
        .get_mut(key)
        .and_then(Item::as_value_mut)
        .filter(|old| old.is_str())
    else {
        return false;
    };
    let decor = old.decor().clone();
    *old = Value::from(new);
    *old.decor_mut() = decor;
    true
}

/// File contents to put back if a bump fails partway.
#[derive(Debug)]
pub(crate) struct Snapshot {
    files: Vec<(PathBuf, Vec<u8>)>,
}

impl Snapshot {
    /// Write every file back, trying all of them even if some fail.
    pub(crate) fn restore(&self) -> Result<(), WorkspaceError> {
        let failures: Vec<(PathBuf, io::Error)> = self
            .files
            .iter()
            .filter_map(|(path, bytes)| fs::write(path, bytes).err().map(|e| (path.clone(), e)))
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(WorkspaceError::Restore(failures))
        }
    }
}

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_root: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    version: Version,
    publish: Option<Vec<String>>,
    manifest_path: PathBuf,
    dependencies: Vec<Dependency>,
}

impl From<Package> for Crate {
    fn from(package: Package) -> Self {
        Self {
            publishable: package
                .publish
                .is_none_or(|registries| registries.iter().any(|r| r == "crates-io")),
            name: package.name,
            version: package.version,
            manifest: package.manifest_path,
            dependencies: package.dependencies,
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>, WorkspaceError> {
    fs::read(path).map_err(|source| WorkspaceError::Read {
        path: path.to_owned(),
        source,
    })
}

fn parse(path: &Path) -> Result<DocumentMut, WorkspaceError> {
    let text = String::from_utf8_lossy(&read(path)?).into_owned();
    text.parse().map_err(|source| WorkspaceError::Toml {
        path: path.to_owned(),
        source,
    })
}

fn write(path: &Path, document: &DocumentMut) -> Result<(), WorkspaceError> {
    fs::write(path, document.to_string()).map_err(|source| WorkspaceError::Write {
        path: path.to_owned(),
        source,
    })
}

fn describe_failures(failures: &[(PathBuf, io::Error)]) -> String {
    failures
        .iter()
        .map(|(path, error)| format!("{}: {error}", path.display()))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Reading or editing the workspace failed.
#[derive(Debug, Error)]
pub(crate) enum WorkspaceError {
    /// `cargo metadata` could not be started.
    #[error("could not run cargo metadata: {0}")]
    Spawn(#[source] io::Error),

    /// `cargo metadata` failed.
    #[error("cargo metadata failed ({status}):\n{stderr}")]
    Metadata {
        /// Cargo's exit status.
        status: ExitStatus,

        /// Cargo's error output.
        stderr: String,
    },

    /// `cargo metadata` printed something unexpected.
    #[error("could not parse cargo metadata: {0}")]
    Parse(#[from] serde_json::Error),

    /// `cargo metadata --locked` failed: usually a stale `Cargo.lock`, but
    /// possibly a network or registry error. Cargo's message says which.
    #[error(
        "`cargo metadata --locked` failed (if Cargo.lock is stale, run `cargo update --workspace` and commit it):\n{0}"
    )]
    LockedMetadata(String),

    /// A field a bump must edit is missing or is not a version string.
    #[error("{}: {field} must be a version string", path.display())]
    NoVersionString {
        /// The manifest.
        path: PathBuf,

        /// The dotted field name.
        field: String,
    },

    /// Workspace dependency requirements do not match the current versions.
    #[error("dependency requirements are out of step:\n  {}", .0.join("\n  "))]
    Floors(Vec<String>),

    /// A manifest is not valid TOML.
    #[error("{}: {source}", path.display())]
    Toml {
        /// The manifest.
        path: PathBuf,

        /// The parse error.
        source: toml_edit::TomlError,
    },

    /// A file could not be read.
    #[error("could not read {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,

        /// The underlying error.
        source: io::Error,
    },

    /// Putting files back after a failed bump failed for these files.
    #[error("could not restore: {}", describe_failures(.0))]
    Restore(Vec<(PathBuf, io::Error)>),

    /// A file could not be written.
    #[error("could not write {}: {source}", path.display())]
    Write {
        /// The file.
        path: PathBuf,

        /// The underlying error.
        source: io::Error,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn version(text: &str) -> Version {
        Version::parse(text).expect("valid version")
    }

    fn member(name: &str, version_text: &str, publishable: bool) -> Crate {
        Crate {
            name: name.to_owned(),
            version: version(version_text),
            publishable,
            manifest: PathBuf::new(),
            dependencies: Vec::new(),
        }
    }

    fn dependency(name: &str, req: &str) -> Dependency {
        Dependency {
            name: name.to_owned(),
            req: VersionReq::parse(req).expect("valid requirement"),
            kind: None,
            path: Some(PathBuf::from(name)),
        }
    }

    #[test]
    fn caret_matches_the_parsed_requirement() {
        bolero::check!()
            .with_type::<(u16, u16, u16)>()
            .for_each(|&(major, minor, patch)| {
                let current = Version::new(major.into(), minor.into(), patch.into());
                let parsed = VersionReq::parse(&format!("^{current}")).expect("caret requirement");
                assert_eq!(caret(&current), parsed);
            });
    }

    #[test]
    fn floors() {
        let user = member("user", "1.0.0", true);
        let core = member("core", "0.19.0", true);
        let wasm = member("wasm", "0.24.0", false);

        assert_eq!(
            floor_problem(&user, &dependency("core", "0.19.0"), Some(&&core)),
            None
        );
        for (dep, target) in [
            (dependency("core", "0.18.2"), Some(&&core)),
            (dependency("core", ">=0.19.0"), Some(&&core)),
            (dependency("wasm", "0.24.0"), Some(&&wasm)),
            (dependency("elsewhere", "1.0.0"), None),
        ] {
            assert!(floor_problem(&user, &dep, target).is_some(), "{}", dep.name);
        }
    }

    #[test]
    fn set_string_keeps_formatting() {
        let mut doc: DocumentMut =
            "[workspace.dependencies]\na = { version = \"0.1.0\", path = \"a\" } # pinned\n"
                .parse()
                .expect("valid TOML");
        let entry = doc
            .get_mut("workspace")
            .and_then(|w| w.get_mut("dependencies"))
            .and_then(|d| d.get_mut("a"))
            .and_then(Item::as_table_like_mut)
            .expect("entry exists");
        assert!(set_string(entry, "version", "0.2.0"));
        assert_eq!(
            doc.to_string(),
            "[workspace.dependencies]\na = { version = \"0.2.0\", path = \"a\" } # pinned\n"
        );
    }

    #[test]
    fn set_string_leaves_non_strings_alone() {
        let mut doc: DocumentMut = "[package]\nversion.workspace = true\n"
            .parse()
            .expect("valid TOML");
        let package = doc
            .get_mut("package")
            .and_then(Item::as_table_like_mut)
            .expect("package table");
        assert!(!set_string(package, "version", "0.2.0"));
        assert_eq!(doc.to_string(), "[package]\nversion.workspace = true\n");
    }

    /// A throwaway workspace on disk: a root manifest and one member.
    fn scratch(root_toml: &str, member_toml: &str) -> (Workspace, PathBuf) {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "xtask-write-versions-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(dir.join("a")).expect("create scratch dir");
        fs::write(dir.join("Cargo.toml"), root_toml).expect("write root");
        fs::write(dir.join("a/Cargo.toml"), member_toml).expect("write member");
        let mut krate = member("a", "0.1.0", true);
        krate.manifest = dir.join("a/Cargo.toml");
        let workspace = Workspace {
            root: dir.clone(),
            crates: vec![krate],
        };
        (workspace, dir)
    }

    #[test]
    fn write_versions_edits_member_and_workspace_entry() {
        let (workspace, dir) = scratch(
            "[workspace.dependencies]\na = { version = \"0.1.0\", path = \"a\" }\n",
            "[package]\nname = \"a\"\nversion = \"0.1.0\"\n",
        );
        let bumps: Vec<_> = workspace
            .publishable()
            .map(|k| (k, version("0.2.0")))
            .collect();
        workspace.write_versions(&bumps).expect("bump");
        let root = fs::read_to_string(dir.join("Cargo.toml")).expect("read root");
        let member = fs::read_to_string(dir.join("a/Cargo.toml")).expect("read member");
        fs::remove_dir_all(&dir).ok();
        assert!(root.contains("a = { version = \"0.2.0\", path = \"a\" }"));
        assert!(member.contains("version = \"0.2.0\""));
    }

    #[test]
    fn write_versions_refuses_entries_it_cannot_edit() {
        for root in [
            "[workspace.dependencies]\na = \"0.1.0\"\n",
            "[workspace.dependencies]\na = { path = \"a\" }\n",
        ] {
            let (workspace, dir) = scratch(root, "[package]\nname = \"a\"\nversion = \"0.1.0\"\n");
            let bumps: Vec<_> = workspace
                .publishable()
                .map(|k| (k, version("0.2.0")))
                .collect();
            let result = workspace.write_versions(&bumps);
            fs::remove_dir_all(&dir).ok();
            assert!(result.is_err(), "{root}");
        }
    }
}
