//! Release the publishable workspace crates to crates.io.
//!
//! Each crate keeps its own version. Merging version bumps to `main` is the
//! release: CI runs [`Command::Validate`], [`Command::Plan`], then (after
//! approval) [`Command::Publish`] and [`Command::Tag`], which records each
//! published crate as a `<crate>-v<version>` git tag.
//!
//! Publishing many crates is not atomic: if one upload fails, earlier crates
//! are already on crates.io. [`Command::Publish`] therefore publishes only the
//! crates whose current version is missing from the registry, so re-running a
//! failed release finishes it.

mod github;
mod index;
mod workspace;

use std::{
    fmt,
    fs::OpenOptions,
    io::{self, Write as _},
    path::PathBuf,
    process::{self, Command as Process},
    str::FromStr,
};

use clap::{Subcommand, ValueEnum};
use semver::Version;
use thiserror::Error;

use self::{
    github::{GitHub, GitHubError, TagOutcome},
    index::{Index, IndexError, Listing},
    workspace::{Crate, Workspace, WorkspaceError},
};

/// `cargo xtask crates` subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Bump every publishable crate and its `[workspace.dependencies]` entry,
    /// refresh `Cargo.lock` offline, then validate the result (which may use
    /// the network to resolve the full graph).
    Prepare {
        /// Which part of each crate's version to bump.
        level: Level,
    },

    /// Check that `Cargo.lock` is current and that every internal dependency
    /// requires its sibling's current version.
    Validate,

    /// List the publishable crates whose current version is not on crates.io.
    ///
    /// Fails if any crate has never been published: its first version must be
    /// published by hand.
    Plan,

    /// Validate, then publish every crate whose current version is not on
    /// crates.io. Publishes nothing if any crate has never been published.
    Publish,

    /// Create a `<crate>-v<version>` tag on GitHub for each given crate version
    /// that crates.io has, at `GITHUB_SHA` (or `HEAD` if unset).
    ///
    /// Pass the commit the versions were published from. With no arguments,
    /// tags every publishable crate at its current version, which is only
    /// right if all of them were published from that commit. An existing tag
    /// at the same commit is left alone; one at another commit is an error.
    /// Needs `GITHUB_TOKEN` and `GITHUB_REPOSITORY`.
    Tag {
        /// Crate versions to tag, as `<crate>@<version>`.
        releases: Vec<Release>,
    },
}

/// The part of a version to increment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Level {
    /// `x.y.z` → `(x+1).0.0`
    Major,

    /// `x.y.z` → `x.(y+1).0`. In 0.x the minor number acts as the major.
    Minor,

    /// `x.y.z` → `x.y.(z+1)`
    Patch,
}

impl Level {
    const fn bump(self, version: &Version) -> Version {
        match self {
            Self::Major => Version::new(version.major + 1, 0, 0),
            Self::Minor => Version::new(version.major, version.minor + 1, 0),
            Self::Patch => Version::new(version.major, version.minor, version.patch + 1),
        }
    }
}

/// One crate at one version. Displays as its git tag, `<crate>-v<version>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Release {
    name: String,
    version: Version,
}

impl Release {
    /// The command-line form, `<crate>@<version>`, which [`FromStr`] parses.
    fn argument(&self) -> String {
        format!("{}@{}", self.name, self.version)
    }
}

/// Cargo's crate-name rules: an ASCII letter, then ASCII letters, digits,
/// `-`, or `_`, at most 64 characters. Such a name is safe in a URL path and a
/// git ref.
fn is_crate_name(name: &str) -> bool {
    name.len() <= 64
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

impl From<&Crate> for Release {
    fn from(krate: &Crate) -> Self {
        Self {
            name: krate.name.clone(),
            version: krate.version.clone(),
        }
    }
}

impl FromStr for Release {
    type Err = CratesError;

    /// Parse `<crate>@<version>`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.split_once('@')
            .filter(|(name, _)| is_crate_name(name))
            .and_then(|(name, version)| {
                Version::parse(version).ok().map(|version| Self {
                    name: name.to_owned(),
                    version,
                })
            })
            .ok_or_else(|| CratesError::Release(text.to_owned()))
    }
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-v{}", self.name, self.version)
    }
}

pub(crate) fn run(command: Command) -> Result<(), CratesError> {
    match command {
        Command::Prepare { level } => prepare(level),
        Command::Validate => {
            validate(&Workspace::load()?)?;
            println!("Cargo.lock is current and internal dependency requirements match.");
            Ok(())
        }
        Command::Plan => {
            let workspace = Workspace::load()?;
            let index = Index::crates_io()?;
            let plan = plan(&workspace, |name| index.listing(name))?;
            report_plan(&plan)?;
            plan.require_no_new()
        }
        Command::Publish => publish(),
        Command::Tag { releases } => tag(releases),
    }
}

fn prepare(level: Level) -> Result<(), CratesError> {
    let workspace = Workspace::load()?;
    if let Some(krate) = workspace
        .publishable()
        .find(|krate| !krate.version.pre.is_empty() || !krate.version.build.is_empty())
    {
        return Err(CratesError::Prerelease(Release::from(krate)));
    }
    let bumps: Vec<(&Crate, Version)> = workspace
        .publishable()
        .map(|krate| (krate, level.bump(&krate.version)))
        .collect();

    let originals = workspace.snapshot_manifests()?;
    let applied = workspace
        .write_versions(&bumps)
        .map_err(CratesError::from)
        .and_then(|()| cargo(&["update", "--workspace", "--offline"]))
        .and_then(|()| validate(&Workspace::load()?));
    if let Err(error) = applied {
        return Err(match originals.restore() {
            Ok(()) => error,
            Err(restore) => CratesError::Rollback {
                error: Box::new(error),
                restore,
            },
        });
    }

    for (krate, version) in &bumps {
        println!("{:<28} {} -> {version}", krate.name, krate.version);
    }
    println!("\nReview and commit the changes, then merge to main. CI publishes the new");
    println!("versions after the crates-io environment is approved, and tags each one.");
    println!("No commit, tag, push, or publish was performed.");
    Ok(())
}

fn validate(workspace: &Workspace) -> Result<(), CratesError> {
    workspace.check_lockfile()?;
    workspace.check_dependency_floors()?;
    Ok(())
}

/// Publishable crates whose current version crates.io does not have yet.
#[derive(Debug, Default)]
struct Plan<'a> {
    /// Already on crates.io under another version; trusted publishing works.
    updates: Vec<&'a Crate>,

    /// Not on crates.io at all. A crate's first version must be published by
    /// hand, because trusted publishing is configured per existing crate.
    new: Vec<&'a Crate>,
}

impl Plan<'_> {
    fn require_no_new(&self) -> Result<(), CratesError> {
        if self.new.is_empty() {
            Ok(())
        } else {
            Err(CratesError::NewCrates(
                self.new.iter().map(|krate| krate.name.clone()).collect(),
            ))
        }
    }
}

fn plan<F: Fn(&str) -> Result<Listing, IndexError>>(
    workspace: &Workspace,
    listing: F,
) -> Result<Plan<'_>, CratesError> {
    let mut plan = Plan::default();
    for krate in workspace.publishable() {
        match listing(&krate.name)? {
            Listing::Missing => plan.new.push(krate),
            Listing::Versions(versions) if !versions.contains(&krate.version) => {
                plan.updates.push(krate);
            }
            Listing::Versions(_) => {}
        }
    }
    Ok(plan)
}

/// Print the plan, and give CI `publish=true|false` and the `planned` releases.
fn report_plan(plan: &Plan<'_>) -> Result<(), CratesError> {
    let list = |crates: &[&Crate]| {
        crates
            .iter()
            .map(|&krate| Release::from(krate).argument())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let updates = list(&plan.updates);
    let new = list(&plan.new);

    println!("To publish: {}", or_none(&updates));
    if !new.is_empty() {
        println!("Never published (publish the first version by hand): {new}");
    }
    append_github_file(
        "GITHUB_OUTPUT",
        &format!("publish={}\nplanned={updates}\n", !plan.updates.is_empty()),
    )?;
    append_github_file(
        "GITHUB_STEP_SUMMARY",
        &format!(
            "### Crates release plan\n\n- To publish: {}\n- Never published: {}\n",
            or_none(&updates),
            or_none(&new),
        ),
    )
}

const fn or_none(text: &str) -> &str {
    if text.is_empty() { "none" } else { text }
}

fn publish() -> Result<(), CratesError> {
    let workspace = Workspace::load()?;
    validate(&workspace)?;
    let index = Index::crates_io()?;
    let plan = plan(&workspace, |name| index.listing(name))?;
    report_plan(&plan)?;
    plan.require_no_new()?;
    if plan.updates.is_empty() {
        return Ok(());
    }

    // Cargo orders the selected packages by their dependencies (Cargo 1.90+)
    // and waits for each to appear on the index before uploading dependents.
    let mut args = vec!["publish", "--locked"];
    for krate in &plan.updates {
        args.extend(["--package", krate.name.as_str()]);
    }
    cargo(&args)
}

fn tag(releases: Vec<Release>) -> Result<(), CratesError> {
    let releases = if releases.is_empty() {
        Workspace::load()?
            .publishable()
            .map(Release::from)
            .collect()
    } else {
        releases
    };
    let index = Index::crates_io()?;
    let github = GitHub::from_env()?;
    let commit = github::head_commit()?;

    for release in &releases {
        let published = matches!(
            index.listing(&release.name)?,
            Listing::Versions(versions) if versions.contains(&release.version)
        );
        if !published {
            println!("{release}: not on crates.io, not tagging");
            continue;
        }
        match github.create_tag(&release.to_string(), &commit)? {
            TagOutcome::Created => println!("{release}: tagged {commit}"),
            TagOutcome::Exists => println!("{release}: already tagged"),
        }
    }
    Ok(())
}

fn cargo(args: &[&str]) -> Result<(), CratesError> {
    let status = Process::new(env_cargo())
        .args(args)
        .status()
        .map_err(|source| CratesError::Spawn {
            program: env_cargo(),
            source,
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(CratesError::Cargo {
            args: args.join(" "),
            status,
        })
    }
}

/// The Cargo running this xtask, so a `cargo +toolchain xtask` stays on it.
fn env_cargo() -> PathBuf {
    std::env::var_os("CARGO").map_or_else(|| PathBuf::from("cargo"), PathBuf::from)
}

/// Append to a GitHub Actions file such as `GITHUB_OUTPUT`, if it is set.
fn append_github_file(variable: &str, text: &str) -> Result<(), CratesError> {
    let Some(path) = std::env::var_os(variable) else {
        return Ok(());
    };
    OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .and_then(|mut file| file.write_all(text.as_bytes()))
        .map_err(|source| CratesError::Write {
            path: PathBuf::from(path),
            source,
        })
}

/// A crates release failure.
#[derive(Debug, Error)]
pub(crate) enum CratesError {
    /// A `tag` argument is not `<crate>@<version>`.
    #[error("expected <crate>@<version>, got {0:?}")]
    Release(String),

    /// `prepare` only bumps stable versions.
    #[error("{0} is a prerelease or has build metadata; set such versions by hand")]
    Prerelease(Release),

    /// Some crates have never been published, so trusted publishing cannot
    /// create them.
    #[error("not on crates.io yet, publish the first version by hand: {}", .0.join(", "))]
    NewCrates(Vec<String>),

    /// A bump failed, and putting the original files back failed too.
    #[error("{error}; restoring the original files also failed: {restore}")]
    Rollback {
        /// Why the bump failed.
        error: Box<CratesError>,

        /// Why the restore failed.
        restore: WorkspaceError,
    },

    /// Reading or editing the workspace failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),

    /// Querying the crates.io index failed.
    #[error(transparent)]
    Index(#[from] IndexError),

    /// Creating a tag on GitHub failed.
    #[error(transparent)]
    GitHub(#[from] GitHubError),

    /// A Cargo command failed.
    #[error("`cargo {args}` failed: {status}")]
    Cargo {
        /// The arguments passed to Cargo.
        args: String,

        /// Cargo's exit status.
        status: process::ExitStatus,
    },

    /// Cargo could not be started.
    #[error("could not run {}: {source}", program.display())]
    Spawn {
        /// The program that failed to start.
        program: PathBuf,

        /// The underlying error.
        source: io::Error,
    },

    /// Writing a CI output file failed.
    #[error("could not write {}: {source}", path.display())]
    Write {
        /// The file being written.
        path: PathBuf,

        /// The underlying error.
        source: io::Error,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn version(text: &str) -> Version {
        Version::parse(text).expect("valid version")
    }

    #[test]
    fn release_round_trips_through_its_argument_form() {
        bolero::check!()
            .with_type::<(String, u16, u16, u16)>()
            .for_each(|(name, major, minor, patch)| {
                let name: String = name
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .take(63)
                    .collect();
                let release = Release {
                    name: format!("c{name}"),
                    version: Version::new((*major).into(), (*minor).into(), (*patch).into()),
                };
                assert_eq!(release.argument().parse::<Release>().ok(), Some(release));
            });
    }

    #[test]
    fn release_displays_as_its_tag() {
        let release: Release = "subduction_core@0.19.0".parse().expect("valid release");
        assert_eq!(release.to_string(), "subduction_core-v0.19.0");
    }

    #[test]
    fn rejects_malformed_releases() {
        for text in [
            "subduction_core",
            "@0.19.0",
            "subduction_core@0.19",
            "x@",
            "a b@1.0.0",
            "a/b@1.0.0",
            "1abc@1.0.0",
        ] {
            assert!(text.parse::<Release>().is_err(), "{text}");
        }
    }

    #[test]
    fn bump_is_the_least_greater_version_at_its_level() {
        bolero::check!()
            .with_type::<(u16, u16, u16)>()
            .for_each(|&(major, minor, patch)| {
                let current = Version::new(major.into(), minor.into(), patch.into());
                let [bumped_major, bumped_minor, bumped_patch] =
                    [Level::Major, Level::Minor, Level::Patch].map(|level| level.bump(&current));

                assert!(current < bumped_patch);
                assert!(bumped_patch < bumped_minor);
                assert!(bumped_minor < bumped_major);
                // Least: each differs from `current` only in the bumped part,
                // by one, with every lower part reset to zero.
                assert_eq!(
                    (bumped_patch.major, bumped_patch.minor, bumped_patch.patch),
                    (current.major, current.minor, current.patch + 1)
                );
                assert_eq!(
                    (bumped_minor.major, bumped_minor.minor, bumped_minor.patch),
                    (current.major, current.minor + 1, 0)
                );
                assert_eq!(
                    (bumped_major.major, bumped_major.minor, bumped_major.patch),
                    (current.major + 1, 0, 0)
                );
            });
    }

    #[test]
    fn plan_sorts_crates_by_what_the_registry_has() {
        let workspace = Workspace::for_test(&[
            ("current", "0.2.0", true),
            ("stale", "0.3.0", true),
            ("yanked", "0.4.0", true),
            ("unlisted", "0.1.0", true),
            ("internal", "0.1.0", false),
        ]);
        let listing = |name: &str| {
            Ok(match name {
                "current" => {
                    Listing::Versions(BTreeSet::from([version("0.1.0"), version("0.2.0")]))
                }
                "stale" => Listing::Versions(BTreeSet::from([version("0.2.0")])),
                // The index lists yanked versions; they cannot be published again.
                "yanked" => Listing::Versions(BTreeSet::from([version("0.4.0")])),
                _ => Listing::Missing,
            })
        };

        let plan = plan(&workspace, listing).expect("plan");
        let names = |crates: &[&Crate]| crates.iter().map(|k| k.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&plan.updates), ["stale"]);
        assert_eq!(names(&plan.new), ["unlisted"]);
        assert!(plan.require_no_new().is_err());
    }
}
