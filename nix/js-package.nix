{
  lib,
  rustPlatform,
  wasm-bodge,
  wasm-bindgen-cli,
  binaryen,
  esbuild,
  nodejs,
  cargoLock,
  # Workspace crate holding package.json, e.g. "subduction_wasm".
  crate,
  # Package key from scripts/js-release.py, e.g. "subduction". Names the
  # flake output (`<key>-js`) and the tarball (`<key>.tgz`).
  key,
}:
let
  packageJson = builtins.fromJSON (builtins.readFile ../${crate}/package.json);
  tarball = "${key}.tgz";
in
rustPlatform.buildRustPackage {
  pname = "${key}-js";
  inherit (packageJson) version;
  src = lib.cleanSource ../.;

  # Nix fetches and verifies these dependencies before entering the sandbox.
  inherit cargoLock;
  auditable = false; # The output is Wasm processed by wasm-bindgen/wasm-opt, not a native executable.

  nativeBuildInputs = [
    wasm-bodge
    wasm-bindgen-cli
    binaryen
    esbuild
    nodejs
  ];

  CARGO_NET_OFFLINE = "true";

  buildPhase = ''
    runHook preBuild

    export HOME="$TMPDIR/home"
    export CARGO_BUILD_JOBS="$NIX_BUILD_CORES"
    mkdir -p "$HOME" "$TMPDIR/npm-package"
    cp Cargo.lock "$TMPDIR/original-Cargo.lock"

    wasm-bodge build \
      --crate-path "$PWD/${crate}" \
      --package-json "$PWD/${crate}/package.json" \
      --out-dir "$PWD/${crate}/dist" \
      --debug-profile wasm-debug
    # wasm-bodge invokes Cargo itself; don't permit silent lockfile changes.
    cmp Cargo.lock "$TMPDIR/original-Cargo.lock"

    (cd ${crate} && npm pack --offline --ignore-scripts --pack-destination "$TMPDIR/npm-package")
    mv "$TMPDIR/npm-package/"*.tgz "$TMPDIR/${tarball}"

    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck

    # Rust/Wasm and browser suites run in test-wasm.yml. Only smoke-test the
    # exact tarball to be installed here, not the build tree's dist/.
    tar -xzf "$TMPDIR/${tarball}" -C "$TMPDIR/npm-package"
    node scripts/check-js-package.mjs "$TMPDIR/npm-package/package" "${packageJson.name}"

    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    cp "$TMPDIR/${tarball}" "$out/${tarball}"
    runHook postInstall
  '';

  # Only a ready-to-publish tarball is installed; no native binaries to fix up.
  dontFixup = true;

  meta = {
    description = "Tested npm tarball for ${packageJson.name} (release and debug Wasm)";
    homepage = "https://github.com/inkandswitch/subduction";
    license = [ lib.licenses.mit lib.licenses.asl20 ];
    platforms = lib.platforms.unix;
  };
}
