# Releasing

Every package is released by pushing a tag to a commit on `main`. A GitHub
Actions workflow then publishes it with trusted publishing (OIDC, so no
long-lived registry token is stored), after an environment approval. Workflow
steps are single commands; the logic lives in `scripts/js-release.py` (npm) and
`cargo xtask crates` (crates).

| What | Tag | Workflow | Approval |
|---|---|---|---|
| `@automerge/subduction` (`subduction_wasm/`) | `subduction-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| `@automerge/sedimentree` (`sedimentree_wasm/`) | `sedimentree-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| `@automerge/automerge-subduction` (`automerge_subduction_wasm/`) | `automerge-subduction-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| Each publishable crate, e.g. `subduction_core` | `<crate>-v<version>`, e.g. `subduction_core-v0.19.0` | [Publish crates](.github/workflows/publish-crates.yml) | `crates-io` |

The tag's version must match the package's manifest at the tagged commit.

## npm packages

### Preparing

```sh
./scripts/js-release.py prepare subduction   # or sedimentree, automerge-subduction
```

Enter the new version when prompted. This updates `package.json`, the crate's
`Cargo.toml`, the workspace dependency, and `Cargo.lock`. Commit, merge to
`main`, and wait for CI.

### Publishing

Push the package's tag pointing to the CI-green release commit on `main`:

```sh
git tag -a subduction-js-v0.24.0 <release-commit> -m 'Release @automerge/subduction 0.24.0'
git push origin refs/tags/subduction-js-v0.24.0
```

Stable versions publish to `latest`; prereleases such as
`subduction-js-v0.25.0-rc.1` publish to `next`.

Review the tagged commit and build artifact, then approve the `npm` environment
deployment. The publish step checks that the tarball is the tagged package and
version, and skips the upload if npm already has that version.

Tests for the script: `python3 scripts/test_js_release.py`.

## Crates

### Preparing

```sh
cargo xtask crates prepare minor   # or major, patch
```

This bumps every crate without `publish = false` at the given level, updates
its `[workspace.dependencies]` entry, and refreshes `Cargo.lock` offline. It
then checks the result with `cargo metadata --locked`, which may download
dependency manifests that are not cached. It restores the original files if any
step fails. If you interrupt it (Ctrl-C), it does not; run
`git restore Cargo.toml Cargo.lock '*/Cargo.toml'` to undo a partial bump. It
only bumps stable versions; set prerelease versions by hand. It prints the tags
to push.

In 0.x, use `minor` for new features or breaking changes and `patch` for fixes.
The `semver` workflow on pull requests flags breaking changes, which need
`minor`. It does not flag new features; choose `minor` for those yourself.

Merge the release commit to `main` and wait for CI.

Tests for the crates tool: `cargo test -p xtask`.

### Publishing

Tag each crate to release at the CI-green release commit, then push the tags
from that commit:

```sh
git tag subduction_core-v0.19.0 <release-commit>
git tag subduction_crypto-v0.10.0 <release-commit>
git switch --detach <release-commit>
cargo xtask crates push-tags
```

`push-tags` checks every `<crate>-v<version>` tag at `HEAD` against its manifest
and checks that `HEAD` is on the remote's `main`, before pushing anything. It
then pushes the tags one per `git push` (GitHub starts no workflow for a single
push of more than three tags), dependencies first, so a run that starts before
the last push still sees every sibling it needs. Re-running it is a no-op for
tags already pushed.

A release run publishes every crate tagged at its commit that crates.io does not
have yet, so crates tagged together release together whichever tag's run starts
first. The run:

1. Checks that the commit is on `main`, that `Cargo.lock` is current, and that
   every publishable crate's non-dev dependency on a sibling is publishable and
   requires `^<sibling's current version>`.
2. Plans the release from the crate tags at the commit. It fails if a tag's
   version differs from the manifest, if a tagged crate has never been
   published, or if a crate to publish needs a sibling version that is neither
   on crates.io nor tagged at the commit; the error names the tag to add.
3. Runs `cargo-semver-checks` on the planned crates against crates.io.
4. Waits for approval of the `crates-io` environment. The plan is in the run's
   summary.
5. Publishes the planned crates in dependency order.

Runs for tags pushed later find their crates already published and stop at the
plan. If a run starts before all the tags arrive, it publishes the crates tagged
so far, and a later run asks for a second approval for the rest. A tag pushed
while a run waits for approval is published by its own run, not by the waiting
one.

The `crates-io` environment's required reviewers are the approval gate: anyone
who can push a tag can start a release, but only a reviewer can let it publish.

Publishing many crates is not atomic. If a run fails partway, re-run it: crates
already on crates.io are skipped. If a re-run fails because a version already
exists, wait a few minutes for the crates.io index to catch up and run it again.

## Adding a package

Trusted publishing cannot create a package. Publish the first version of a new
crate or npm package by hand, then add a trusted publisher for it on the
registry: repository `inkandswitch/subduction`, workflow `publish-crates.yml`
with environment `crates-io` for a crate, or workflow `publish-js.yml` with
environment `npm` for an npm package.
