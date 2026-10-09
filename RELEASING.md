# Releasing

Every release except a package's first goes through a GitHub Actions workflow,
authenticates with trusted publishing (OIDC, so no long-lived registry token is
stored), and waits for an environment approval. Workflow steps are single
commands; the logic lives in `scripts/js-release.py` (npm) and
`cargo xtask crates` (crates).

| What | Trigger | Workflow | Approval |
|---|---|---|---|
| `@automerge/subduction` (`subduction_wasm/`) | Push tag `subduction-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| `@automerge/sedimentree` (`sedimentree_wasm/`) | Push tag `sedimentree-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| `@automerge/automerge-subduction` (`automerge_subduction_wasm/`) | Push tag `automerge-subduction-js-v<version>` | [Publish JS package](.github/workflows/publish-js.yml) | `npm` |
| Every publishable crate whose version is not on crates.io | Merge version bumps to `main` | [Publish crates](.github/workflows/publish-crates.yml) | `crates-io` |

Crates have independent version numbers. After publishing, the crates workflow
tags each published crate as `<crate>-v<version>`, for example
`subduction_core-v0.19.0`. Those tags record what shipped; they trigger nothing.

## npm packages

### Preparing

Run with Python 3.11+ and Cargo available, naming the package
(`subduction`, `sedimentree`, or `automerge-subduction`):

```sh
./scripts/js-release.py prepare subduction
```

Enter the new version when prompted. The script updates the crate's
`package.json` and `Cargo.toml`, the root workspace dependency, and `Cargo.lock`.
It uses Cargo offline, does not check npm for the version, and restores the
original files if preparation fails. It prints the tag commands but does not
commit, tag, or push.

Merge the release commit to `main` and wait for CI.

To build and smoke-test a package locally:

```sh
nix build .#subduction-js   # or .#sedimentree-js, .#automerge-subduction-js
```

This writes `result/<package>.tgz`, e.g. `result/subduction.tgz`.

### Publishing

Push the package's tag pointing to the CI-green release commit on `main`:

```sh
git tag -a subduction-js-v0.24.0 <release-commit> -m 'Release @automerge/subduction 0.24.0'
git push origin refs/tags/subduction-js-v0.24.0
```

The tag version must match the manifests and `Cargo.lock`. Stable versions
publish to `latest`; prereleases such as `subduction-js-v0.25.0-rc.1` publish to
`next`.

Review the tagged commit and build artifact, then approve the `npm` environment
deployment. The publish step checks that the tarball is the tagged package and
version, and skips the upload if npm already has that version.

Do not push `*-js-v*` tags to record versions published before this workflow
existed: a tag push runs the workflow file from the tagged commit, which may be
the old single-package workflow or none.

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
only bumps stable versions; set prerelease versions by hand.

In 0.x, use `minor` for new features or breaking changes and `patch` for fixes.
The `semver` workflow on pull requests flags breaking changes, which need
`minor`. It does not flag new features; choose `minor` for those yourself.

To see what a release would publish:

```sh
cargo xtask crates plan
```

Tests for the crates tool: `cargo test -p xtask`.

### Publishing

Merge the version bumps to `main`. On every push to `main`, the crates workflow:

1. Checks that `Cargo.lock` is current, and that every publishable crate's
   non-dev dependency on a sibling is publishable and requires
   `^<sibling's current version>`.
2. Plans the release: the crates whose current version is not on crates.io.
   If there are none, the workflow stops here. If any publishable crate has
   never been published, it fails here.
3. Runs `cargo-semver-checks` against crates.io.
4. Waits for approval of the `crates-io` environment. The plan is in the run's
   summary.
5. Publishes the planned crates in dependency order.
6. Tags each published crate as `<crate>-v<version>` at the released commit.

The semver check covers every publishable crate, not only the planned ones. A
crate with breaking changes since its last release blocks the release until its
version is bumped too.

Rejecting the deployment, or failing the semver check, does not undo the bump:
every later push to `main` plans the same release again. To retry without a new
commit, run "Publish crates" on `main` from the Actions tab.

Publishing many crates is not atomic. If it fails partway, re-run the failed
jobs before merging anything else to `main`: crates already on crates.io are
skipped, and the tag job then tags the run's planned crates at that run's
commit. If a later push publishes the rest first, the crates the failed run
published stay untagged; tag them by hand (see "Tagging by hand"). If a re-run
fails because a version already exists, wait a few minutes for the crates.io
index to catch up and run it again. The tag step is also safe to re-run.

### Tagging by hand

To tag crate versions published outside the workflow, name each one and the
commit it was published from:

```sh
GITHUB_REPOSITORY=inkandswitch/subduction GITHUB_TOKEN="$(gh auth token)" \
  GITHUB_SHA=<published-commit> \
  cargo xtask crates tag subduction_core@0.19.0 subduction_crypto@0.10.0
```

It skips versions crates.io does not have, leaves an existing tag at the same
commit alone, and refuses to move one at another commit. Always pass the crate
versions explicitly: with no arguments it tags every publishable crate at the
given commit, which is only right if all of them were published from it.

## Switching over

Before merging the crates workflow (or any change to it), run
`cargo xtask crates plan` on the commit being merged:

- `To publish: none`: the first push to `main` does nothing.
- It lists crates: the first push to `main` starts that release.
- It names never-published crates: the workflow fails on every push to `main`
  until their first versions are published by hand.

Crates published by hand before the workflow existed (the 0.19 release) have no
tags. Tag them as in "Tagging by hand", with `GITHUB_SHA` set to the commit they
were published from.

## Adding a package

Trusted publishing cannot create a package. Publish the first version of a new
crate or npm package by hand, then add its trusted publisher (below). A
publishable crate that is not on crates.io makes the crates workflow fail on
every push to `main`, so publish it before merging, or merge it with
`publish = false` first. `js-release.py publish` fails if npm has no such
package.

## One-time setup

Repository settings:

- Create the `npm` and `crates-io` environments with required reviewers. The
  approval gate depends on them.
- Restrict which refs may deploy to each environment: `main` only for
  `crates-io`, and the three `*-js-v*` tag patterns for `npm`. Trusted
  publishing checks the repository, workflow and environment, not the branch,
  so this is what stops a run on another branch from publishing.
- Add a tag ruleset limiting who can create `*-js-v*` tags. Leave crate tags
  (`*_*-v*`) creatable by GitHub Actions.

Trusted publishing must be configured per package by an owner:

- npm: for each package, add a trusted publisher for repository
  `inkandswitch/subduction`, workflow `publish-js.yml`, environment `npm`.
- crates.io: for each workspace crate without `publish = false` (all except the
  three `*_wasm` crates, `subduction_wasm_bootstrap`, and `xtask`), add a
  trusted publisher for repository `inkandswitch/subduction`, workflow
  `publish-crates.yml`, environment `crates-io`.
