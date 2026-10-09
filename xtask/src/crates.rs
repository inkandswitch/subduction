//! Release the publishable workspace crates to crates.io.
//!
//! Each crate keeps its own version and is released by pushing its tag,
//! `<crate>-v<version>`, at a commit on `main` whose manifest has that version.
//! A release run publishes every crate tagged at its commit that crates.io does
//! not have yet, so crates tagged together at one commit release together, in
//! dependency order, whichever tag's run gets there first.
//!
//! Publishing many crates is not atomic: if one upload fails, earlier crates
//! are already on crates.io. [`Command::Publish`] therefore publishes only the
//! tagged crates whose version is missing from the registry, so re-running a
//! failed release finishes it.

mod index;
mod workspace;

use std::{
    collections::BTreeMap,
    fmt,
    fs::OpenOptions,
    io::{self, Write as _},
    path::PathBuf,
    process::{self, Command as Process},
};

use clap::{Subcommand, ValueEnum};
use semver::Version;
use thiserror::Error;

use self::{
    index::{Index, IndexError, Listing},
    workspace::{Crate, Workspace, WorkspaceError},
};

/// `cargo xtask crates` subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Bump every publishable crate and its `[workspace.dependencies]` entry,
    /// refresh `Cargo.lock` offline, then validate the result (which may use
    /// the network to resolve the full graph). Prints the tags to push.
    Prepare {
        /// Which part of each crate's version to bump.
        level: Level,
    },

    /// Check that `Cargo.lock` is current and that every internal dependency
    /// requires its sibling's current version.
    Validate,

    /// List the crates a release run for `tag` would publish: every crate
    /// tagged `<crate>-v<version>` at `HEAD` that crates.io does not have.
    ///
    /// Fails if `tag` is not such a tag, if a tag's version differs from the
    /// manifest, if a tagged crate has never been published, or if a crate to
    /// publish needs a sibling version that is neither on crates.io nor tagged.
    Plan {
        /// The tag that started the release, e.g. `subduction_core-v0.19.0`.
        tag: String,
    },

    /// Validate and plan as above, then publish the planned crates.
    Publish {
        /// The tag that started the release, e.g. `subduction_core-v0.19.0`.
        tag: String,

        /// Publish only these crates (comma-separated), as an earlier `plan`
        /// listed them. Crates tagged since are left for a later run.
        #[arg(long, value_delimiter = ',')]
        only: Option<Vec<String>>,
    },

    /// Push every `<crate>-v<version>` tag at `HEAD`, one per `git push`.
    ///
    /// GitHub starts no workflow for a single push of more than three tags.
    /// Checks every tag against its manifest before pushing any. Pushing a tag
    /// the remote already has is a no-op.
    PushTags {
        /// The remote to push to.
        #[arg(long, default_value = "origin")]
        remote: String,
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

/// A crate's release tag, `<crate>-v<version>`.
struct Tag<'a>(&'a str, &'a Version);

impl fmt::Display for Tag<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-v{}", self.0, self.1)
    }
}

impl Crate {
    fn tag(&self) -> Tag<'_> {
        Tag(&self.name, &self.version)
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
        Command::Plan { tag } => {
            let workspace = Workspace::load()?;
            let index = Index::crates_io()?;
            let plan = plan(&workspace, &tags_at_head()?, &tag, None, |name| {
                index.listing(name)
            })?;
            report_plan(&plan)
        }
        Command::Publish { tag, only } => publish(&tag, only.as_deref()),
        Command::PushTags { remote } => push_tags(&remote),
    }
}

fn prepare(level: Level) -> Result<(), CratesError> {
    let workspace = Workspace::load()?;
    if let Some(krate) = workspace
        .publishable()
        .find(|krate| !krate.version.pre.is_empty() || !krate.version.build.is_empty())
    {
        return Err(CratesError::Prerelease(krate.tag().to_string()));
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
    let tags: Vec<String> = bumps
        .iter()
        .map(|(krate, version)| Tag(&krate.name, version).to_string())
        .collect();
    println!("\nReview and commit the changes, merge to main, and wait for CI. Then tag");
    println!("the release commit:");
    for tag in &tags {
        println!("  git tag {tag} <release-commit>");
    }
    println!("\nThen, with the release commit checked out, push them one at a time:");
    println!("  cargo xtask crates push-tags");
    println!("\nNo commit, tag, push, or publish was performed.");
    Ok(())
}

fn validate(workspace: &Workspace) -> Result<(), CratesError> {
    workspace.check_lockfile()?;
    workspace.check_dependency_floors()?;
    Ok(())
}

/// The crate a tag names, if it is `<crate>-v<version>` for a publishable
/// member. Prefers the longest matching crate name. Errors if the tag names a
/// member at a version other than the manifest's.
fn tagged_crate<'a>(workspace: &'a Workspace, tag: &str) -> Result<Option<&'a Crate>, CratesError> {
    let Some((krate, version)) = workspace
        .publishable()
        .filter_map(|krate| {
            let rest = tag.strip_prefix(krate.name.as_str())?.strip_prefix("-v")?;
            Version::parse(rest).ok().map(|version| (krate, version))
        })
        .max_by_key(|(krate, _)| krate.name.len())
    else {
        return Ok(None);
    };
    if version == krate.version {
        Ok(Some(krate))
    } else {
        Err(CratesError::TagVersion {
            tag: tag.to_owned(),
            manifest: krate.version.clone(),
        })
    }
}

/// What crates.io has for a crate, relative to its current manifest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Registry {
    /// Never published.
    NeverPublished,

    /// Published, but not this version.
    LacksVersion,

    /// This version is published (possibly yanked).
    HasVersion,
}

/// What a release run would do with the crates tagged at its commit.
#[derive(Debug, Default)]
struct Plan<'a> {
    /// Tagged, and crates.io has an earlier version: to publish.
    publish: Vec<&'a Crate>,

    /// Tagged, and crates.io already has this version: nothing to do.
    published: Vec<&'a Crate>,

    /// Tagged and unpublished, but outside `only`: tagged after the plan this
    /// run was approved with, so left for the run that tag started.
    later: Vec<&'a Crate>,
}

fn plan<'a, F: Fn(&str) -> Result<Listing, IndexError>>(
    workspace: &'a Workspace,
    tags: &[String],
    trigger: &str,
    only: Option<&[String]>,
    listing: F,
) -> Result<Plan<'a>, CratesError> {
    if !tags.iter().any(|tag| tag == trigger) || tagged_crate(workspace, trigger)?.is_none() {
        return Err(CratesError::Trigger(trigger.to_owned()));
    }

    // Each crate's registry status at its current version, looked up once.
    let mut known: BTreeMap<&str, Registry> = BTreeMap::new();
    let mut status = |krate: &'a Crate| -> Result<Registry, CratesError> {
        if let Some(&status) = known.get(krate.name.as_str()) {
            return Ok(status);
        }
        let status = match listing(&krate.name)? {
            Listing::Missing => Registry::NeverPublished,
            Listing::Versions(versions) if versions.contains(&krate.version) => {
                Registry::HasVersion
            }
            Listing::Versions(_) => Registry::LacksVersion,
        };
        known.insert(&krate.name, status);
        Ok(status)
    };

    let mut plan = Plan::default();
    let mut new = Vec::new();
    for tag in tags {
        let Some(krate) = tagged_crate(workspace, tag)? else {
            continue;
        };
        match status(krate)? {
            Registry::HasVersion => plan.published.push(krate),
            Registry::LacksVersion if only.is_some_and(|only| !only.contains(&krate.name)) => {
                plan.later.push(krate);
            }
            Registry::LacksVersion => plan.publish.push(krate),
            Registry::NeverPublished => new.push(krate.name.clone()),
        }
    }
    if !new.is_empty() {
        return Err(CratesError::NewCrates(new));
    }

    // Every sibling a crate to publish needs at its current version must be on
    // crates.io already, or be published in this run.
    let mut missing = Vec::new();
    for krate in &plan.publish {
        for sibling in krate.sibling_dependencies() {
            let in_run = plan.publish.iter().any(|other| other.name == sibling);
            let Some(dep) = workspace.publishable_crate(sibling) else {
                continue; // check_dependency_floors reports unpublishable siblings
            };
            if !in_run && status(dep)? != Registry::HasVersion {
                missing.push(format!("{} needs {}", krate.tag(), dep.tag()));
            }
        }
    }
    if missing.is_empty() {
        Ok(plan)
    } else {
        Err(CratesError::MissingSiblings(missing))
    }
}

/// Print the plan, and give CI `publish=true|false` and `packages` (the crates
/// to publish, comma-separated, for cargo-semver-checks).
fn report_plan(plan: &Plan<'_>) -> Result<(), CratesError> {
    let tags = |crates: &[&Crate]| {
        crates
            .iter()
            .map(|krate| krate.tag().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let publish = tags(&plan.publish);
    let published = tags(&plan.published);
    let later = tags(&plan.later);
    let packages = plan
        .publish
        .iter()
        .map(|krate| krate.name.as_str())
        .collect::<Vec<_>>()
        .join(",");

    println!("To publish: {}", or_none(&publish));
    println!("Already on crates.io: {}", or_none(&published));
    if !later.is_empty() {
        println!("Tagged after this run was planned (left for their own run): {later}");
    }
    append_github_file(
        "GITHUB_OUTPUT",
        &format!(
            "publish={}\npackages={packages}\n",
            !plan.publish.is_empty()
        ),
    )?;
    append_github_file(
        "GITHUB_STEP_SUMMARY",
        &format!(
            "### Crates release plan\n\n- To publish: {}\n- Already on crates.io: {}\n",
            or_none(&publish),
            or_none(&published),
        ),
    )
}

const fn or_none(text: &str) -> &str {
    if text.is_empty() { "none" } else { text }
}

fn publish(trigger: &str, only: Option<&[String]>) -> Result<(), CratesError> {
    let workspace = Workspace::load()?;
    validate(&workspace)?;
    let index = Index::crates_io()?;
    let plan = plan(&workspace, &tags_at_head()?, trigger, only, |name| {
        index.listing(name)
    })?;
    report_plan(&plan)?;
    if plan.publish.is_empty() {
        return Ok(());
    }

    // Cargo orders the selected packages by their dependencies (Cargo 1.90+)
    // and waits for each to appear on the index before uploading dependents.
    let mut args = vec!["publish", "--locked"];
    for krate in &plan.publish {
        args.extend(["--package", krate.name.as_str()]);
    }
    cargo(&args)
}

/// The crate release tags among `tags`, each checked against its manifest.
fn tagged_crates<'a>(
    workspace: &'a Workspace,
    tags: &[String],
) -> Result<Vec<&'a Crate>, CratesError> {
    let mut found = Vec::new();
    for tag in tags {
        if let Some(krate) = tagged_crate(workspace, tag)? {
            found.push(krate);
        }
    }
    Ok(found)
}

/// `crates` reordered so each comes after its siblings in the set. Any prefix
/// of the result is then complete: no crate in it needs a sibling outside it.
fn dependencies_first<'a>(crates: &[&'a Crate]) -> Vec<&'a Crate> {
    fn visit<'a>(krate: &'a Crate, crates: &[&'a Crate], ordered: &mut Vec<&'a Crate>) {
        if ordered.iter().any(|done| done.name == krate.name) {
            return;
        }
        // Cargo forbids cycles among non-dev dependencies, so this terminates.
        for sibling in krate.sibling_dependencies() {
            if let Some(dep) = crates.iter().find(|other| other.name == sibling) {
                visit(dep, crates, ordered);
            }
        }
        ordered.push(krate);
    }

    let mut ordered = Vec::with_capacity(crates.len());
    for krate in crates {
        visit(krate, crates, &mut ordered);
    }
    ordered
}

/// Push the crate tags at `HEAD`, dependencies first, so a release run that
/// starts before the last push still sees a complete set of siblings.
fn push_tags(remote: &str) -> Result<(), CratesError> {
    let workspace = Workspace::load()?;
    let tagged = tagged_crates(&workspace, &tags_at_head()?)?;
    if tagged.is_empty() {
        return Err(CratesError::NoCrateTags);
    }

    // The release workflow rejects commits not on main; catch that before any
    // tag reaches the remote.
    if !git(&["fetch", "--quiet", remote, "main"])?.success() {
        return Err(CratesError::Fetch(remote.to_owned()));
    }
    if !git(&["merge-base", "--is-ancestor", "HEAD", "FETCH_HEAD"])?.success() {
        return Err(CratesError::NotOnMain(remote.to_owned()));
    }

    for krate in dependencies_first(&tagged) {
        let tag = krate.tag().to_string();
        let status = git(&["push", remote, &format!("refs/tags/{tag}")])?;
        if !status.success() {
            return Err(CratesError::Push { tag, status });
        }
    }
    Ok(())
}

fn git(args: &[&str]) -> Result<process::ExitStatus, CratesError> {
    Process::new("git")
        .args(args)
        .status()
        .map_err(|source| CratesError::Spawn {
            program: PathBuf::from("git"),
            source,
        })
}

/// The tags pointing at `HEAD`. CI must fetch tags (`fetch-depth: 0`).
fn tags_at_head() -> Result<Vec<String>, CratesError> {
    let output = Process::new("git")
        .args(["tag", "--points-at", "HEAD"])
        .output()
        .map_err(|source| CratesError::Spawn {
            program: PathBuf::from("git"),
            source,
        })?;
    if !output.status.success() {
        return Err(CratesError::Git(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect())
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
    /// The release tag is not a publishable crate's tag at `HEAD`.
    #[error("{0} is not a `<crate>-v<version>` tag for a publishable crate at HEAD")]
    Trigger(String),

    /// A tag names a crate at a version other than its manifest's.
    #[error("{tag} does not match the manifest version {manifest}")]
    TagVersion {
        /// The tag.
        tag: String,

        /// The crate's version in its manifest at this commit.
        manifest: Version,
    },

    /// `prepare` only bumps stable versions.
    #[error("{0} is a prerelease or has build metadata; set such versions by hand")]
    Prerelease(String),

    /// Some tagged crates have never been published, so trusted publishing
    /// cannot create them.
    #[error("not on crates.io yet, publish the first version by hand: {}", .0.join(", "))]
    NewCrates(Vec<String>),

    /// Crates to publish need sibling versions that are neither on crates.io
    /// nor tagged at this commit.
    #[error("tag these at the same commit too:\n  {}", .0.join("\n  "))]
    MissingSiblings(Vec<String>),

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

    /// `git tag --points-at HEAD` failed.
    #[error("could not list the tags at HEAD:\n{0}")]
    Git(String),

    /// `push-tags` found nothing to push.
    #[error("no `<crate>-v<version>` tags at HEAD; tag the release commit first")]
    NoCrateTags,

    /// `git fetch <remote> main` failed.
    #[error("could not fetch main from {0}")]
    Fetch(String),

    /// `HEAD` is not on the remote's `main`, so the release would be rejected.
    #[error("HEAD is not on {0}'s main; check out the merged release commit")]
    NotOnMain(String),

    /// `git push` of one tag failed. Tags before it were pushed.
    #[error("`git push` of {tag} failed: {status}")]
    Push {
        /// The tag being pushed.
        tag: String,

        /// Git's exit status.
        status: process::ExitStatus,
    },

    /// A Cargo command failed.
    #[error("`cargo {args}` failed: {status}")]
    Cargo {
        /// The arguments passed to Cargo.
        args: String,

        /// Cargo's exit status.
        status: process::ExitStatus,
    },

    /// A program could not be started.
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

    fn tags(names: &[&str]) -> Vec<String> {
        names.iter().map(|&tag| tag.to_owned()).collect()
    }

    fn names(crates: &[&Crate]) -> Vec<String> {
        crates.iter().map(|krate| krate.name.clone()).collect()
    }

    #[test]
    fn a_crates_tag_names_that_crate() {
        bolero::check!()
            .with_type::<(String, u16, u16, u16)>()
            .for_each(|(name, major, minor, patch)| {
                let name: String = name
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .take(63)
                    .collect();
                let name = format!("c{name}");
                let current = Version::new((*major).into(), (*minor).into(), (*patch).into());
                let workspace =
                    Workspace::for_test(&[(name.as_str(), &current.to_string(), true, &[])]);
                let tag = Tag(&name, &current).to_string();
                let found = tagged_crate(&workspace, &tag).expect("matching version");
                assert_eq!(found.map(|krate| krate.name.clone()), Some(name));
            });
    }

    #[test]
    fn tag_parsing() {
        let workspace = Workspace::for_test(&[
            ("subduction", "1.0.0", true, &[]),
            ("subduction-v2", "0.3.0-rc.1", true, &[]),
            ("subduction_core", "0.19.0", true, &[]),
            ("subduction_wasm", "0.24.0", false, &[]),
        ]);
        let found = |tag: &str| {
            tagged_crate(&workspace, tag).map(|krate| krate.map(|krate| krate.name.clone()))
        };

        assert_eq!(
            found("subduction_core-v0.19.0").ok(),
            Some(Some("subduction_core".into()))
        );
        // Longest name wins, and prerelease versions parse.
        assert_eq!(
            found("subduction-v2-v0.3.0-rc.1").ok(),
            Some(Some("subduction-v2".into()))
        );
        // Not crate tags: npm, nightly, unpublished crates, unknown names.
        for tag in [
            "subduction-js-v0.24.0",
            "v0.18.0-nightly.2026-09-30",
            "subduction_wasm-v0.24.0",
            "elsewhere-v1.0.0",
        ] {
            assert_eq!(found(tag).ok(), Some(None), "{tag}");
        }
        assert!(found("subduction_core-v0.18.2").is_err());
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

    /// `core` 0.2.0 is new; `util` 0.1.0 is on crates.io; `app` 0.3.0 needs both.
    fn release_workspace() -> Workspace {
        Workspace::for_test(&[
            ("app", "0.3.0", true, &["core", "util"]),
            ("core", "0.2.0", true, &[]),
            ("util", "0.1.0", true, &[]),
        ])
    }

    fn registry(name: &str) -> Listing {
        match name {
            "app" => Listing::Versions(BTreeSet::from([version("0.2.0")])),
            "core" | "util" => Listing::Versions(BTreeSet::from([version("0.1.0")])),
            _ => Listing::Missing,
        }
    }

    #[test]
    fn plan_publishes_the_tagged_crates_crates_io_lacks() {
        let workspace = release_workspace();
        let tagged = tags(&[
            "app-v0.3.0",
            "core-v0.2.0",
            "util-v0.1.0",
            "subduction-js-v1.0.0",
        ]);
        let plan = plan(&workspace, &tagged, "core-v0.2.0", None, |name| {
            Ok(registry(name))
        })
        .expect("plan");
        assert_eq!(names(&plan.publish), ["app", "core"]);
        assert_eq!(names(&plan.published), ["util"]);
        assert!(plan.later.is_empty());
    }

    #[test]
    fn only_defers_crates_tagged_after_the_approved_plan() {
        let workspace = release_workspace();
        let tagged = tags(&["app-v0.3.0", "core-v0.2.0"]);
        let approved = ["core".to_owned()];
        let plan = plan(
            &workspace,
            &tagged,
            "core-v0.2.0",
            Some(&approved),
            |name| Ok(registry(name)),
        )
        .expect("plan");
        assert_eq!(names(&plan.publish), ["core"]);
        assert_eq!(names(&plan.later), ["app"]);
    }

    #[test]
    fn plan_requires_untagged_siblings_on_crates_io() {
        let workspace = release_workspace();
        // `app` needs `core` 0.2.0, which is neither on crates.io nor tagged.
        let tagged = tags(&["app-v0.3.0"]);
        assert!(matches!(
            plan(&workspace, &tagged, "app-v0.3.0", None, |name| Ok(registry(name))),
            Err(CratesError::MissingSiblings(missing)) if missing == ["app-v0.3.0 needs core-v0.2.0"]
        ));
    }

    #[test]
    fn plan_rejects_a_trigger_not_at_head_or_not_a_crate() {
        let workspace = release_workspace();
        let tagged = tags(&["core-v0.2.0", "subduction-js-v1.0.0"]);
        for trigger in ["app-v0.3.0", "subduction-js-v1.0.0"] {
            assert!(matches!(
                plan(&workspace, &tagged, trigger, None, |name| Ok(registry(
                    name
                ))),
                Err(CratesError::Trigger(_))
            ));
        }
    }

    #[test]
    fn plan_refuses_never_published_crates() {
        let workspace = Workspace::for_test(&[("fresh", "0.1.0", true, &[])]);
        let tagged = tags(&["fresh-v0.1.0"]);
        assert!(matches!(
            plan(&workspace, &tagged, "fresh-v0.1.0", None, |name| Ok(registry(name))),
            Err(CratesError::NewCrates(new)) if new == ["fresh"]
        ));
    }

    #[test]
    fn dependencies_first_puts_every_crate_after_its_siblings() {
        bolero::check!()
            .with_type::<(Vec<Vec<u8>>, u64)>()
            .for_each(|(picks, seed)| {
                // Crate `i` may depend only on crates `< i`: always acyclic.
                let picks: Vec<&Vec<u8>> = picks.iter().take(12).collect();
                let names: Vec<String> = (0..picks.len()).map(|i| format!("c{i}")).collect();
                let deps: Vec<Vec<&str>> = picks
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        p.iter()
                            .filter_map(|b| usize::from(*b).checked_rem(i))
                            .filter_map(|j| names.get(j).map(String::as_str))
                            .collect()
                    })
                    .collect();
                let members: Vec<(&str, &str, bool, &[&str])> = names
                    .iter()
                    .zip(&deps)
                    .map(|(name, deps)| (name.as_str(), "0.1.0", true, deps.as_slice()))
                    .collect();
                let workspace = Workspace::for_test(&members);

                // Present the crates in a scrambled order.
                let mut input: Vec<&Crate> = workspace.publishable().collect();
                let len = input.len().max(1);
                input.rotate_left(usize::try_from(*seed % len as u64).unwrap_or(0));
                input.reverse();

                let ordered = dependencies_first(&input);
                assert_eq!(ordered.len(), input.len());
                for (position, krate) in ordered.iter().enumerate() {
                    for sibling in krate.sibling_dependencies() {
                        let before = ordered.iter().take(position).any(|k| k.name == sibling);
                        assert!(before, "{} pushed before its sibling {sibling}", krate.name);
                    }
                }
            });
    }

    #[test]
    fn dependencies_first_ignores_siblings_outside_the_set() {
        let workspace = release_workspace();
        let app = workspace.publishable_crate("app").expect("app");
        let ordered = dependencies_first(&[app]);
        assert_eq!(names(&ordered), ["app"]);
    }
}
