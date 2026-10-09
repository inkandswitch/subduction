#!/usr/bin/env python3
"""Prepare, check, and publish releases of the npm packages built from this repo.

prepare <package>: prompt for a version, update manifests, and refresh Cargo.lock offline.
validate <tag>: check a release tag against those files and emit CI outputs.
publish <tag> <tarball>: publish a tested tarball unless that version is already on npm.
check-published <tag>: for manual use; query npm and emit published=true/false.

Each package has its own tag prefix, e.g. subduction-js-v0.24.0.

Requires Python 3.11+; prepare needs Cargo, and check-published/publish need npm.
Only publish publishes.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tarfile
import tomllib
from dataclasses import dataclass
from pathlib import Path


REPOSITORY_URL = "https://github.com/inkandswitch/subduction"
WORKSPACE_TOML = "Cargo.toml"
LOCKFILE = "Cargo.lock"


@dataclass(frozen=True)
class Package:
    """An npm package published from one workspace crate."""

    key: str
    npm_name: str
    crate: str

    @property
    def tag_prefix(self):
        return f"{self.key}-js-v"

    @property
    def flake_output(self):
        return f"{self.key}-js"

    @property
    def package_json(self):
        return f"{self.crate}/package.json"

    @property
    def crate_toml(self):
        return f"{self.crate}/Cargo.toml"

    @property
    def version_files(self):
        return (self.package_json, self.crate_toml, WORKSPACE_TOML, LOCKFILE)


PACKAGES = {
    package.key: package
    for package in (
        Package("automerge-subduction", "@automerge/automerge-subduction", "automerge_subduction_wasm"),
        Package("sedimentree", "@automerge/sedimentree", "sedimentree_wasm"),
        Package("subduction", "@automerge/subduction", "subduction_wasm"),
    )
}


# Command handlers


def prepare_release(root, package):
    """Prepare a version bump, restoring all four files if any step fails."""
    current = read_json(root / package.package_json)["version"]
    print(f"Current {package.npm_name} version: {current}")
    version = input("New version (without v): ").strip()

    # Check both the requested version and the starting state before editing.
    validate_version(version)
    validate_manifests(root, package, current)
    if version == current:
        raise ValueError(f"The version is already {current}")

    # Plan the three manifest edits in memory. Cargo will update the lockfile.
    originals = {path: (root / path).read_bytes() for path in package.version_files}
    updates = plan_manifest_updates(originals, package, current, version)

    try:
        write_files(root, updates)
        refresh_lockfile(root, package)
        validate_manifests(root, package, version)
    except (Exception, KeyboardInterrupt):
        write_files(root, originals)
        raise

    print_next_steps(package, version)


def validate_release(root, tag):
    """Check a tagged release without changing files, then report it to CI."""
    package, version = parse_tag(tag)
    validate_manifests(root, package, version)
    write_validation_output(package, version)


def check_published(tag):
    """Report whether a version exists; registry or parsing failures are errors."""
    package, version = parse_tag(tag)
    published = is_published(package, version)
    write_ci_output(f"published={str(published).lower()}\n")


def publish_release(tag, tarball):
    """Publish the tested tarball unchanged. Re-running after success is a no-op."""
    package, version = parse_tag(tag)
    # An absolute path: npm reads a bare `dir/file.tgz` as GitHub `user/repo`
    # shorthand and would fetch that instead of the checked tarball.
    tarball = Path(tarball).resolve(strict=True)
    check_tarball(tarball, package, version)
    if is_published(package, version):
        return

    # Explicit --tag latest bypasses npm's built-in downgrade protection.
    # Only prereleases need an explicit tag; next follows publish order.
    tag_args = ["--tag", "next"] if npm_dist_tag(version) == "next" else []
    subprocess.run(
        ["npm", "publish", str(tarball), "--ignore-scripts", "--access", "public", *tag_args, "--provenance"],
        check=True,
    )


# Version and manifest validation (read-only)


def validate_version(version):
    """Accept stable SemVer or a prerelease, but not build metadata."""
    number = r"(?:0|[1-9][0-9]*)"
    identifier = rf"(?:{number}|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    pattern = rf"{number}\.{number}\.{number}(?:-{identifier}(?:\.{identifier})*)?"
    if not re.fullmatch(pattern, version):
        raise ValueError("Invalid release version")


def parse_tag(tag):
    """Return the package and version named by a release tag."""
    matches = [package for package in PACKAGES.values() if tag.startswith(package.tag_prefix)]
    if len(matches) != 1:
        prefixes = ", ".join(f"{package.tag_prefix}<version>" for package in PACKAGES.values())
        raise ValueError(f"Expected a tag of the form {prefixes}")
    package = matches[0]
    version = tag.removeprefix(package.tag_prefix)
    validate_version(version)
    return package, version


def package_from_key(key):
    if key not in PACKAGES:
        raise ValueError(f"Unknown package {key!r}; expected one of {', '.join(sorted(PACKAGES))}")
    return PACKAGES[key]


def read_json(path):
    return json.loads(path.read_text())


def read_toml(path):
    return tomllib.loads(path.read_text())


def validate_manifests(root, package, version):
    """Require the expected package identity and the same version in all four files."""
    validate_version(version)
    package_json = read_json(root / package.package_json)
    crate = read_toml(root / package.crate_toml)
    workspace = read_toml(root / WORKSPACE_TOML)
    lockfile = read_toml(root / LOCKFILE)

    if package_json["name"] != package.npm_name:
        raise ValueError(f"{package.package_json}: expected npm name {package.npm_name}")
    if package_json["repository"]["url"] != REPOSITORY_URL:
        raise ValueError(f"{package.package_json}: unexpected repository URL")

    locked = [entry for entry in lockfile["package"] if entry["name"] == package.crate]
    if len(locked) != 1:
        raise ValueError(f"Expected exactly one {package.crate} entry in Cargo.lock")

    versions = {
        package.package_json: package_json["version"],
        package.crate_toml: crate["package"]["version"],
        WORKSPACE_TOML: workspace["workspace"]["dependencies"][package.crate]["version"],
        LOCKFILE: locked[0]["version"],
    }
    for path, actual in versions.items():
        if actual != version:
            raise ValueError(f"{path}: expected version {version}, found {actual}")


def check_tarball(tarball, package, version):
    """Refuse to publish a tarball that is not the package and version being released."""
    with tarfile.open(tarball) as archive:
        member = archive.extractfile("package/package.json")
        if member is None:
            raise ValueError(f"{tarball}: missing package/package.json")
        manifest = json.load(member)
    if (manifest.get("name"), manifest.get("version")) != (package.npm_name, version):
        raise ValueError(
            f"{tarball}: contains {manifest.get('name')}@{manifest.get('version')}, "
            f"expected {package.npm_name}@{version}"
        )


# Planning and applying a version bump


def plan_manifest_updates(originals, package, current, version):
    """Return new manifest bytes without writing anything.

    Validation uses JSON/TOML parsers. Editing uses narrow text replacements so
    comments and formatting survive; each pattern must match exactly one field.
    Cargo.lock is deliberately excluded: Cargo is responsible for updating it.
    """
    old = re.escape(current)
    crate = re.escape(package.crate)
    version_fields = {
        package.package_json: rf'(?m)^(\s*"version"\s*:\s*"){old}(")',
        package.crate_toml: rf'(?m)^(version\s*=\s*"){old}(")',
        WORKSPACE_TOML: rf'(?m)^({crate}\s*=\s*\{{[^\n]*?\bversion\s*=\s*"){old}(")',
    }
    updates = {}
    for path, pattern in version_fields.items():
        text = originals[path].decode()
        updated, count = re.subn(pattern, lambda match: match[1] + version + match[2], text)
        if count != 1:
            raise ValueError(f"{path}: expected exactly one version field; no files were changed")
        updates[path] = updated.encode()
    return updates


def write_files(root, contents):
    for path, data in contents.items():
        (root / path).write_bytes(data)


def refresh_lockfile(root, package):
    """Refresh the path package using cached dependencies, without registry queries."""
    subprocess.run(
        ["cargo", "update", "--offline", "--package", package.crate],
        cwd=root,
        check=True,
    )


# Registry queries


def is_published(package, version):
    published = version in published_versions(package)
    if published:
        print(f"{package.npm_name}@{version} is already published.", file=sys.stderr)
    return published


def published_versions(package):
    """The npm package must already exist. Failed lookups must not allow publishing."""
    result = subprocess.run(
        ["npm", "view", package.npm_name, "versions", "--json"],
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    versions = json.loads(result.stdout)
    # npm may return a string when the package has just one version.
    if isinstance(versions, str):
        versions = [versions]
    if not isinstance(versions, list) or not all(isinstance(version, str) for version in versions):
        raise ValueError("Unexpected npm versions response")
    return versions


# User and CI output


def npm_dist_tag(version):
    return "next" if "-" in version else "latest"


def print_next_steps(package, version):
    tag = package.tag_prefix + version
    print(f"\nPrepared {package.npm_name} {version}.")
    print("Review and commit the manifest/Cargo.lock changes, then merge to main and wait for CI.")
    print("Once the release commit is on main and CI is green:")
    print(f"  git tag -a {tag} <release-commit> -m 'Release {package.npm_name} {version}'")
    print(f"  git push origin refs/tags/{tag}")
    print("No commit, tag, push, or publish was performed.")


def write_validation_output(package, version):
    # The workflow reads `package` and `flake_output`; the rest is for the log.
    write_ci_output(
        f"package={package.key}\n"
        f"flake_output={package.flake_output}\n"
        f"npm_name={package.npm_name}\n"
        f"version={version}\n"
        f"npm_tag={npm_dist_tag(version)}\n"
    )
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as handle:
            handle.write(
                f"### {package.npm_name} {version}\n"
                f"Commit: {os.environ.get('GITHUB_SHA', 'unknown')}\n"
                f"npm dist-tag: {npm_dist_tag(version)}\n"
                "If the build passes, the tested tarball is uploaded as the npm-package artifact.\n"
            )


def write_ci_output(output):
    print(output, end="")
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as handle:
            handle.write(output)


# CLI entry point


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="Prompt for a new version and update the manifests and Cargo.lock")
    prepare.add_argument("package", choices=sorted(PACKAGES), help="Package to bump")
    validate = commands.add_parser("validate", help="Check a release tag against the manifests and Cargo.lock")
    validate.add_argument("tag", help="Release tag, e.g. subduction-js-v0.24.0")
    published = commands.add_parser("check-published", help="Check whether a tag's version is already on npm")
    published.add_argument("tag", help="Release tag, e.g. subduction-js-v0.24.0")
    publish = commands.add_parser("publish", help="Publish a tested tarball unless already published")
    publish.add_argument("tag", help="Release tag, e.g. subduction-js-v0.24.0")
    publish.add_argument("tarball", help="Path to the tested npm tarball")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]

    try:
        if args.command == "prepare":
            prepare_release(root, package_from_key(args.package))
        elif args.command == "validate":
            validate_release(root, args.tag)
        elif args.command == "check-published":
            check_published(args.tag)
        else:
            publish_release(args.tag, args.tarball)
    except (KeyboardInterrupt, EOFError):
        sys.exit("Cancelled.")
    except (ValueError, KeyError, OSError, tarfile.TarError, subprocess.CalledProcessError) as error:
        sys.exit(f"Release {args.command} failed: {error}")


if __name__ == "__main__":
    main()
