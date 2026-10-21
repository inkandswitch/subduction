# Subduction JavaScript API tests

The browser suites exercise the public plain-object API, not private Wasm value
wrappers. They cover memory/IndexedDB persistence, snapshots and streams,
MessageChannel authentication, WebSocket/HTTP long-poll interoperability with a
real Rust server, and ephemeral gossip.

## Build and run

From the repository root, with a Wasm-enabled Rust toolchain:

```sh
cargo build -p subduction_cli
cd subduction_wasm
pnpm install
pnpm build
pnpm exec playwright install
pnpm exec playwright test --project chromium
```

The tests load the wasm-bodge public wrapper from `dist/wrapper/esm/web.js`,
including the JS storage backends. Rebuild after Rust or JS changes (`pnpm build`);
raw wasm-bindgen output no longer includes those factories. Building requires
wasm-bodge and its prerequisites: a matching wasm-bindgen CLI, esbuild, TypeScript,
and wasm-opt. For a development build without wasm-opt, run
`wasm-bodge build --debug-profile wasm-debug --no-wasm-opt`.
The Playwright fixture serves this package directory using Python 3 on port 9891.

Network tests use `SUBDUCTION_CLI` if provided, otherwise the release binary when
present, falling back to `target/debug/subduction_cli`. Ensure the selected binary
was built from the current checkout. Each worker gets an isolated ephemeral server
port. Fixtures wait for readiness and terminate owned server processes afterward.
Network tests are not skipped in CI.

```sh
pnpm exec playwright test subduction.spec.ts messagechannel-connection.spec.ts
pnpm exec playwright test peer-connection.spec.ts longpoll-connection.spec.ts
pnpm exec playwright test ephemeral.spec.ts
pnpm exec playwright test --project firefox
pnpm exec playwright test --project webkit
pnpm exec playwright test --trace on
pnpm exec playwright show-report
```

## Node and declaration tests

```sh
pnpm run test:pojo
```

This rebuilds the wasm-bodge public package in `dist/`, compiles
`tests/pojo-consumer.ts` against its emitted declarations, and runs
`tests/pojo-api.test.cjs` against both release and debug wrappers. It deliberately
builds first so stale generated JS or declarations cannot hide a regression.
The tests assert that only `Subduction` is exported as a
library class and that the declared methods exist at runtime. They also cover
cross-realm inputs, Buffer slices, invalid/shared/detached buffers, conversion
before asynchronous work, callback errors, validation before side effects, signed
record round trips, and lifecycle behavior.

`wasm-bindgen` must match the version in `Cargo.lock`. Alternate installations can
be selected with `CARGO_TOOLCHAIN=stable` and `WASM_BINDGEN=/path/to/wasm-bindgen`.
To test an already-built package without rebuilding:

```sh
POJO_WASM_PATH=/absolute/path/to/dist/wrapper/cjs/node.cjs node --test tests/pojo-api.test.cjs
```

The Rust Wasm tests remain separate and retain detailed lifecycle, reentry,
partial-write recovery, reconnect, and iterator cancellation regressions.
