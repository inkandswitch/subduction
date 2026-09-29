#!/usr/bin/env python3
"""Prepare and check an @automerge/subduction release.

prepare: prompt for a version, update manifests, and refresh Cargo.lock offline.
validate: check a release tag against those files and emit CI outputs.
check-published: query npm for an existing version and emit published=true/false.

Requires Python 3.11+; prepare needs Cargo and check-published needs npm.
None of these commands publishes.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path


TAG_PREFIX = "subduction-js-v"
PACKAGE_JSON = "subduction_wasm/package.json"
CRATE_TOML = "subduction_wasm/Cargo.toml"
WORKSPACE_TOML = "Cargo.toml"
LOCKFILE = "Cargo.lock"
VERSION_FILES = (PACKAGE_JSON, CRATE_TOML, WORKSPACE_TOML, LOCKFILE)


# Command handlers


def prepare_release(root):
    """Prepare a version bump, restoring all four files if any step fails."""
    current = read_json(root / PACKAGE_JSON)["version"]
    print(f"Current @automerge/subduction version: {current}")
    version = input("New version (without v): ").strip()

    # Check both the requested version and the starting state before editing.
    validate_version(version)
    validate_manifests(root, current)
    if version == current:
        raise ValueError(f"The version is already {current}")

    # Plan the three manifest edits in memory. Cargo will update the lockfile.
    originals = {path: (root / path).read_bytes() for path in VERSION_FILES}
    updates = plan_manifest_updates(originals, current, version)

    try:
        write_files(root, updates)
        refresh_lockfile(root)
        validate_manifests(root, version)
    except (Exception, KeyboardInterrupt):
        write_files(root, originals)
        raise

    print_next_steps(version)


def validate_release(root, tag):
    """Check a tagged release without changing files, then report it to CI."""
    version = version_from_tag(tag)
    validate_manifests(root, version)
    write_validation_output(version)


def check_published(version):
    """Report whether a version exists; registry or parsing failures are errors."""
    validate_version(version)
    published = version in published_versions()
    write_ci_output(f"published={str(published).lower()}\n")
    if published:
        print(f"@automerge/subduction@{version} is already published.", file=sys.stderr)


# Version and manifest validation (read-only)


def validate_version(version):
    """Accept stable SemVer or a prerelease, but not build metadata."""
    number = r"(?:0|[1-9][0-9]*)"
    identifier = rf"(?:{number}|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    pattern = rf"{number}\.{number}\.{number}(?:-{identifier}(?:\.{identifier})*)?"
    if not re.fullmatch(pattern, version):
        raise ValueError("Invalid release version")


def version_from_tag(tag):
    if not tag.startswith(TAG_PREFIX):
        raise ValueError(f"Expected a {TAG_PREFIX}<version> tag")
    version = tag.removeprefix(TAG_PREFIX)
    validate_version(version)
    return version


def read_json(path):
    return json.loads(path.read_text())


def read_toml(path):
    return tomllib.loads(path.read_text())


def validate_manifests(root, version):
    """Require the expected package identity and the same version in all four files."""
    validate_version(version)
    package = read_json(root / PACKAGE_JSON)
    crate = read_toml(root / CRATE_TOML)
    workspace = read_toml(root / WORKSPACE_TOML)
    lockfile = read_toml(root / LOCKFILE)

    if package["name"] != "@automerge/subduction":
        raise ValueError("Unexpected npm package name")
    if package["repository"]["url"] != "https://github.com/inkandswitch/subduction":
        raise ValueError("Unexpected npm repository URL")

    locked = [entry for entry in lockfile["package"] if entry["name"] == "subduction_wasm"]
    if len(locked) != 1:
        raise ValueError("Expected exactly one subduction_wasm entry in Cargo.lock")

    versions = {
        PACKAGE_JSON: package["version"],
        CRATE_TOML: crate["package"]["version"],
        WORKSPACE_TOML: workspace["workspace"]["dependencies"]["subduction_wasm"]["version"],
        LOCKFILE: locked[0]["version"],
    }
    for path, actual in versions.items():
        if actual != version:
            raise ValueError(f"{path}: expected version {version}, found {actual}")


# Planning and applying a version bump


def plan_manifest_updates(originals, current, version):
    """Return new manifest bytes without writing anything.

    Validation uses JSON/TOML parsers. Editing uses narrow text replacements so
    comments and formatting survive; each pattern must match exactly one field.
    Cargo.lock is deliberately excluded: Cargo is responsible for updating it.
    """
    old = re.escape(current)
    version_fields = {
        PACKAGE_JSON: rf'(?m)^(\s*"version"\s*:\s*"){old}(")',
        CRATE_TOML: rf'(?m)^(version\s*=\s*"){old}(")',
        WORKSPACE_TOML: rf'(?m)^(subduction_wasm\s*=\s*\{{[^\n]*?\bversion\s*=\s*"){old}(")',
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


def refresh_lockfile(root):
    """Refresh the path package using cached dependencies, without registry queries."""
    subprocess.run(
        ["cargo", "update", "--offline", "--package", "subduction_wasm"],
        cwd=root,
        check=True,
    )


# Registry queries (only used by check-published)


def published_versions():
    """The npm package must already exist. Failed lookups must not allow publishing."""
    result = subprocess.run(
        ["npm", "view", "@automerge/subduction", "versions", "--json"],
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


def print_next_steps(version):
    tag = TAG_PREFIX + version
    print(f"\nPrepared @automerge/subduction {version}.")
    print("Review and commit the manifest/Cargo.lock changes, then merge to main and wait for CI.")
    print("Once the release commit is on main and CI is green:")
    print(f"  git tag -a {tag} <release-commit> -m 'Release @automerge/subduction {version}'")
    print(f"  git push origin refs/tags/{tag}")
    print("No commit, tag, push, or publish was performed.")


def write_validation_output(version):
    npm_tag = "next" if "-" in version else "latest"
    write_ci_output(f"version={version}\nnpm_tag={npm_tag}\n")


def write_ci_output(output):
    print(output, end="")
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as handle:
            handle.write(output)


# CLI entry point


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare", help="Prompt for a new version and update the manifests and Cargo.lock")
    validate = commands.add_parser("validate", help="Check a release tag against the manifests and Cargo.lock")
    validate.add_argument("tag", help="Release tag, e.g. subduction-js-v0.22.1")
    published = commands.add_parser("check-published", help="Check whether a version is already on npm")
    published.add_argument("version", help="Package version, e.g. 0.22.1")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]

    try:
        if args.command == "prepare":
            prepare_release(root)
        elif args.command == "validate":
            validate_release(root, args.tag)
        else:
            check_published(args.version)
    except (KeyboardInterrupt, EOFError):
        sys.exit("Cancelled.")
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        sys.exit(f"Release {args.command} failed: {error}")


if __name__ == "__main__":
    main()
