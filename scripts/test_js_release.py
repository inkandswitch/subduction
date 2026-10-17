"""Tests for js-release.py. CI runs them in test-js-package.yml.

Run locally with: python3 scripts/test_js_release.py
"""

import importlib.util
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("js-release.py")
spec = importlib.util.spec_from_file_location("js_release", SCRIPT)
js_release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(js_release)


def tarball(directory, manifest):
    path = Path(directory) / "package.tgz"
    data = json.dumps(manifest).encode()
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo("package/package.json")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return path


class ParseTag(unittest.TestCase):
    def test_every_package_round_trips(self):
        for package in js_release.PACKAGES.values():
            for version in ("0.1.0", "1.20.3", "0.25.0-rc.1"):
                with self.subTest(package=package.key, version=version):
                    self.assertEqual(
                        js_release.parse_tag(package.tag_prefix + version),
                        (package, version),
                    )

    def test_rejects_other_tags(self):
        for tag in (
            "subduction-js-v0.24",
            "subduction-js-v0.24.0+build",
            "subduction-js-v01.0.0",
            "subduction_core-v0.19.0",
            "v0.24.0",
        ):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                js_release.parse_tag(tag)


class PlanManifestUpdates(unittest.TestCase):
    package = js_release.PACKAGES["subduction"]

    def originals(self, workspace_line):
        return {
            self.package.package_json: b'{\n  "name": "@automerge/subduction",\n  "version": "0.24.0"\n}\n',
            self.package.crate_toml: b'[package]\nname = "subduction_wasm"\nversion = "0.24.0"\n',
            js_release.WORKSPACE_TOML: workspace_line.encode(),
        }

    def test_edits_only_the_version_fields(self):
        updates = js_release.plan_manifest_updates(
            self.originals(
                'subduction_wasm = { version = "0.24.0", path = "subduction_wasm" }\n'
                'subduction_wasm_bootstrap = { version = "0.24.0", path = "x" }\n'
            ),
            self.package,
            "0.24.0",
            "0.25.0",
        )
        self.assertIn(b'"version": "0.25.0"', updates[self.package.package_json])
        self.assertIn(b'version = "0.25.0"', updates[self.package.crate_toml])
        self.assertEqual(
            updates[js_release.WORKSPACE_TOML].decode(),
            'subduction_wasm = { version = "0.25.0", path = "subduction_wasm" }\n'
            'subduction_wasm_bootstrap = { version = "0.24.0", path = "x" }\n',
        )

    def test_refuses_when_a_field_is_missing(self):
        with self.assertRaises(ValueError):
            js_release.plan_manifest_updates(
                self.originals("other = { version = \"0.24.0\" }\n"),
                self.package,
                "0.24.0",
                "0.25.0",
            )


class CheckTarball(unittest.TestCase):
    package = js_release.PACKAGES["sedimentree"]

    def test_accepts_the_tagged_package(self):
        with tempfile.TemporaryDirectory() as directory:
            path = tarball(directory, {"name": "@automerge/sedimentree", "version": "0.18.0"})
            js_release.check_tarball(path, self.package, "0.18.0")

    def test_rejects_another_package_or_version(self):
        manifests = (
            {"name": "@automerge/subduction", "version": "0.18.0"},
            {"name": "@automerge/sedimentree", "version": "0.18.1"},
        )
        for manifest in manifests:
            with self.subTest(manifest=manifest), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    js_release.check_tarball(tarball(directory, manifest), self.package, "0.18.0")


class RepoManifests(unittest.TestCase):
    """The package table matches the repo, so a typo fails on the PR, not at release."""

    def test_every_package_is_consistent(self):
        root = SCRIPT.resolve().parents[1]
        for package in js_release.PACKAGES.values():
            with self.subTest(package=package.key):
                version = js_release.read_json(root / package.package_json)["version"]
                js_release.validate_manifests(root, package, version)


class Publish(unittest.TestCase):
    def publish(self, tag, published):
        """Publish from a relative `npm-artifacts/package.tgz`, as CI does."""
        version = tag.rsplit("-js-v", 1)[1]
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            js_release, "published_versions", return_value=published
        ), mock.patch.object(js_release.subprocess, "run") as run:
            artifacts = Path(directory) / "npm-artifacts"
            artifacts.mkdir()
            tarball(artifacts, {"name": "@automerge/sedimentree", "version": version})
            previous = Path.cwd()
            os.chdir(directory)
            try:
                js_release.publish_release(tag, "npm-artifacts/package.tgz")
            finally:
                os.chdir(previous)
        return run

    def test_passes_npm_an_absolute_path_and_the_prerelease_tag(self):
        run = self.publish("sedimentree-js-v0.18.0-rc.1", published=[])
        argv = run.call_args.args[0]
        # A relative `dir/file.tgz` would be read as GitHub `user/repo` shorthand.
        self.assertTrue(Path(argv[2]).is_absolute())
        self.assertEqual(argv[argv.index("--tag") + 1], "next")

    def test_skips_a_published_version(self):
        run = self.publish("sedimentree-js-v0.18.0", published=["0.18.0"])
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
