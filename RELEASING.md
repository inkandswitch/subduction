# Releasing the JavaScript package

The `@automerge/subduction` NPM package is published from `subduction_wasm/` by
[Publish JS package](.github/workflows/publish-js.yml). This workflow is
triggered by pushes of tags matching `subduction-js-v*` to the `main` branch.

Releasing is thus a two step process:

- Prepare a release commit on `main` with the new version in all manifests
- Tag the commit and push the tag to trigger the release

## Preparing a release

Run with Python 3.11+ and Cargo available:

```sh
./scripts/js-release.py prepare
```

Enter the new version when prompted. The script updates `subduction_wasm/package.json`,
`subduction_wasm/Cargo.toml`, the root workspace dependency, and `Cargo.lock`.
It uses Cargo offline, does not look up available versions, and restores the
original files if preparation fails.

Review and commit the changes, merge to `main`, and wait for CI to pass before
tagging the release. The script prints the tag/push commands but does not run
them or create a commit.

## Triggering a release

Push a package-specific tag pointing to the CI-green release commit on `main`:

```sh
# Replace the example version and commit with the release you prepared.
git tag -a subduction-js-v0.22.1 <release-commit> -m 'Release @automerge/subduction 0.22.1'
git push origin refs/tags/subduction-js-v0.22.1
```

The tag version must match all the manifests and `Cargo.lock`. Stable versions
publish to `latest`; prereleases such as `subduction-js-v0.23.0-rc.1` publish to
`next`. The `subduction-js-v` prefix avoids collisions with CLI/nightly release
tags. 

Review the tagged commit and build artifact, then approve the `npm` environment
deployment. The publish job will then publish the package to NPM.
