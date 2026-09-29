{
  lib,
  rustPlatform,
  wasm-bodge,
  wasm-bindgen-cli,
  binaryen,
  esbuild,
  nodejs,
  cargoLock,
}:
rustPlatform.buildRustPackage {
  pname = "subduction-js";
  version = (builtins.fromJSON (builtins.readFile ../subduction_wasm/package.json)).version;
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
      --crate-path "$PWD/subduction_wasm" \
      --package-json "$PWD/subduction_wasm/package.json" \
      --out-dir "$PWD/subduction_wasm/dist" \
      --debug-profile wasm-debug
    # wasm-bodge invokes Cargo itself; don't permit silent lockfile changes.
    cmp Cargo.lock "$TMPDIR/original-Cargo.lock"

    (cd subduction_wasm && npm pack --offline --ignore-scripts --pack-destination "$TMPDIR/npm-package")
    mv "$TMPDIR/npm-package/"*.tgz "$TMPDIR/subduction.tgz"

    runHook postBuild
  '';

  doCheck = true;
  checkPhase = ''
    runHook preCheck

    # Rust/Wasm and browser suites run in test-wasm.yml. Only smoke-test the
    # exact tarball to be installed here, not the build tree's dist/.
    tar -xzf "$TMPDIR/subduction.tgz" -C "$TMPDIR/npm-package"
    node scripts/check-js-package.mjs "$TMPDIR/npm-package/package"

    runHook postCheck
  '';

  installPhase = ''
    runHook preInstall
    mkdir -p "$out"
    cp "$TMPDIR/subduction.tgz" "$out/subduction.tgz"
    runHook postInstall
  '';

  # Only a ready-to-publish tarball is installed; no native binaries to fix up.
  dontFixup = true;

  meta = {
    description = "Tested npm tarball for @automerge/subduction (release and debug Wasm)";
    homepage = "https://github.com/inkandswitch/subduction";
    license = [ lib.licenses.mit lib.licenses.asl20 ];
    platforms = lib.platforms.unix;
  };
}
