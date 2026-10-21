# Subduction Wasm bindings

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Plain-object API

`Subduction` is the only public library class. IDs, digests, and topics are
64-character hex strings; checkpoint prefixes are 24-character hex strings.
Input accepts either case, without `0x`; output is always lowercase. Blobs and
signed envelopes remain `Uint8Array`. Metadata, sync results, and stream events
are recursively plain objects and need no `.free()` calls.

```javascript
import {
  Subduction, createMemorySigner, createMemoryStorage,
} from "@automerge/subduction";

const node = new Subduction({
  signer: createMemorySigner(),
  storage: createMemoryStorage(),
});
try {
  await node.storeBatch(treeId, {
    commits: [{ head: commitHash, parents: parentHashes, blob: changeBytes }],
  });
  const results = await node.syncWithAllPeers(treeId, { subscribe: true });
  for (const [peerId, result] of Object.entries(results)) {
    console.log(peerId, result.success, result.stats);
  }
} finally {
  try { await node.close(); } finally { node.free(); }
}
```

Data arguments are synchronously decoded/copied into owned Rust values before
asynchronous work is scheduled. Callers may reuse or mutate their objects and
ordinary buffers once a method returns. Inputs are neither consumed nor detached;
freezing is optional. Shared-memory payloads are rejected. Outputs own independent
JS memory and remain usable after shutdown. Services (storage, signers, policies,
transports, callbacks, and cancellation signals) remain live interfaces.

### Migration from wrapper classes

This is a breaking API change; there is no implicit legacy-wrapper conversion.

| Previously | Now |
| --- | --- |
| `PeerId`, `CommitId`, `SedimentreeId`, `Digest`, `Topic` constructors | Hex strings |
| `Checkpoint.fromBytes(bytes)` | 24-character hex prefix (`commitId.slice(0, 24)` for a valid hex ID) |
| `new MemoryStorage()` | `createMemoryStorage()` |
| `IndexedDbStorage.setup(indexedDB, name)` | `createIndexedDbStorage(indexedDB, name)` |
| `MemorySigner.generate()` / `.fromBytes(seed)` | `createMemorySigner()` / `createMemorySigner(seed)` |
| `WebCryptoSigner.setup()` | `createWebCryptoSigner()` |
| `new CommitInput(new LooseCommit(...), blob)` | `{ head, parents, blob }` |
| `new FragmentInput(new Fragment(...), blob)` | `{ head, boundary, checkpoints, blob }` |
| `storeCommit(id, head, parents, blob)` | `storeCommit(id, { head, parents, blob })` |
| `storeBuiltBatch(id, commits, fragments)` for ordinary publication | `storeBatch(id, { commits, fragments })` |
| Precomputed metadata publication | `storeBuiltBatch(id, { commits: [{ metadata, blob }], fragments: [...] })` |
| Positional sync boolean/timeout | `{ subscribe, timeoutMilliseconds }` options |
| `PeerResultMap.getResult(peer)` / `.entries()` | `results[peer]` / `Object.entries(results)` |
| `Depth.value` | Numeric depth |
| Metadata wrappers | Records from `getCommits`, `getFragments`, or stored-update streams |
| `AuthenticatedTransport.setup(...)` then `addConnection(...)` | `node.connectTransport(transport, { expectedPeerId })` |
| Discovery transport setup | `connectTransport` / `acceptTransport` with `{ serviceName }` |

Custom storage implementations receive hex IDs and signed bytes; loads return
`{ signed: Uint8Array, blob: Uint8Array }` records, not classes. The built-in
IndexedDB backend retains the existing version-1 stores, indexes, keys, and record
layout. Its returned structural object has `close()`; storage is caller-owned and
is not closed by the node. Storage is trusted to preserve locally signed or
previously verified envelopes. On reload Subduction checks encoding and blob
agreement, but does not re-verify signatures. Storage callbacks are not an
admission path for unverified network records.

Custom policies, remote-head observers, ephemeral callbacks, and frame handlers
also receive hex identifiers. `hashMetricOverride` now takes a commit ID string
and returns a numeric depth. Existing infallible Rust signer/metric trait contracts
still require a valid signature/public key and a nonthrowing valid depth metric;
this representation change does not make those traits fallible.

The detailed boundary design is in [POJO_API.md](../POJO_API.md). Synchronization,
authorization, cancellation, and partial-write guarantees are unchanged.

## Build package

From the repository root:

```sh
nix build .#subduction-js
```

This builds and tests the npm package, including the `/debug` variant, and writes
`result/subduction.tgz`. See [Releasing](../RELEASING.md#npm-packages)
for the tag-driven publishing workflow.

For an in-place development build of `subduction_wasm/dist/`, use
`nix develop` followed by `bodge subduction_wasm` instead.

## Run tests

Install dependencies:
```
pnpm install
```

Install Playwright's browser binaries:
```
pnpm run test:install
```

Run tests:
```
pnpm test
```

Rebuild and test the public package and its TypeScript declarations:

```sh
pnpm run test:pojo
```

This requires a Wasm-enabled Rust toolchain, wasm-bodge, esbuild, TypeScript, and a
`wasm-bindgen` CLI matching `Cargo.lock`. Override the Rust toolchain and bindgen
location using `CARGO_TOOLCHAIN` and `WASM_BINDGEN` if needed. The script rebuilds
`dist/`, typechecks a consumer against the emitted package declarations, and runs
Node tests against both release and debug wrappers. Browser tests use that same
built public wrapper and require a current `subduction_cli` (or `SUBDUCTION_CLI`).

View Playwright report:
```
pnpm run test:report
```

## Naming Conventions

`wasm-bindgen`-generated types ownership rules can be confusing and cumbersome, with different behavior depending on which side of the Wasm boundary your type was created from. To help keep this straight, we adopt the naming convention:

> [!NOTE]
> Internal Rust wrapper types are prefixed with `Wasm`.
> Types imported from JS are prefixed with `Js`.

These are implementation conventions, not public JavaScript construction
requirements. Private drivers may still use Wasm wrappers internally.

## Debugging with Chrome DevTools

Subduction's Wasm packages can ship a parallel `./debug` subpath export that preserves DWARF debug info, enabling source-level step-through in Chrome DevTools. This section applies to `@automerge/subduction` but is also valid for `@automerge/sedimentree` and `@automerge/automerge-subduction`.

### One-time Chrome setup

1. Install the _C/C++ DevTools Support (DWARF)_ extension from the Chrome Web Store. It handles reading DWARF sections from Wasm modules; wasm-bindgen output uses the same DWARF format the extension was built for.
2. In Chrome, visit `chrome://flags/#enable-webassembly-debugging` and enable the _WebAssembly Debugging: Enable DWARF support_ flag. Restart the browser.
3. Open DevTools on the page that loads your Wasm bundle, go to the gear icon → _Experiments_, and confirm _WebAssembly Debugging: Enable DWARF support_ is checked there too.

### Building the debug variant

From the workspace root:

```
bodge subduction_wasm          # one crate
bodge:all                      # all four _wasm crates
```

Both `bodge` and `bodge:all` pass `--debug-profile wasm-debug` to `wasm-bodge`, which runs a second `cargo build` under the `[profile.wasm-debug]` profile declared in the workspace `Cargo.toml`. The generated `dist/` tree contains both the release artifacts (under the usual paths) _and_ debug artifacts (prefixed with `debug-` under `esm/` / `cjs/`, plus `*-debug.wasm` standalone, plus `iife/debug.js`).

### Consuming the debug bundle

Your downstream app imports from the `./debug` subpath:

```javascript
import { init, /* ... */ } from "@automerge/subduction/debug";
```

For Chrome DevTools to locate DWARF symbols, it must fetch the Wasm binary at a URL it can read. This rules out the inline-base64 path. Use one of:

- **Bundler path (recommended)**: import `@automerge/subduction/debug` under a bundler (Vite/Webpack/Rollup). The `browser` condition resolves to `./dist/esm/debug-bundler.js`, which delegates Wasm loading to the bundler's native `.wasm` import — leaving the binary as a fetchable asset that DevTools can read DWARF from.
- **Manual fetch**: serve `./dist/subduction-debug.wasm` directly and call `initSync(bytes)` from `@automerge/subduction/slim` (bypassing auto-init).

### Source path resolution

DWARF embeds absolute paths from the compile host (e.g. `/home/expede/Documents/Code/subduction/subduction_core/src/…`). For step-through to work, DevTools needs to map those paths to files on disk. Two ways:

- _Preferred_: open DevTools on a page served from inside the Subduction checkout so the absolute DWARF paths resolve as-is.
- _Or_: in the C/C++ DevTools Support extension's options page, add a path substitution mapping the DWARF paths to your local checkout.

If DevTools shows "`<file>: file not found`" in the Sources panel, you need to set up the mapping.

### Tradeoffs baked into the profile

`[profile.wasm-debug]` (see the workspace `Cargo.toml` for the full annotated definition) inherits from `release` and tunes these knobs:

- `opt-level = 1` — most locals remain visible, line-stepping feels natural; optimized enough for interactive debugging.
- `lto = "thin"` — avoids the heavy cross-crate inlining of `lto = "fat"` that would collapse stack frames.
- `debug = "limited"` — keeps line tables, type info, and most locals, at ~½ the DWARF size of `debug = "full"`. Empirically subduction_wasm lands at ~29 MiB raw / ~6 MiB gzipped (versus ~3 MiB / 723 KiB for release).
- `debug-assertions = true` and `overflow-checks = true` — catch bugs the release build wouldn't trip on. Disable these on a branch if you're reproducing a release-only codepath.
- `strip = "none"` on the four `_wasm` crates specifically, overriding the release profile's `strip = "debuginfo"` (which would otherwise wipe the DWARF).
- Automerge and `sedimentree_core` are pinned to `opt-level = 2` to keep their hashing/iteration-heavy internals from crawling in the browser. If you need to step through _those_ crates' internals, comment out the corresponding `[profile.wasm-debug.package.*]` overrides in the workspace `Cargo.toml`.

### Verifying DWARF is present

```
wasm-tools objdump ./subduction_wasm/dist/subduction-debug.wasm | grep debug_
```

Should list `.debug_info`, `.debug_line`, `.debug_str`, `.debug_abbrev`, and friends. If those are missing, something stripped DWARF — most likely an accidental `wasm-opt` run, or a `strip = "debuginfo"` leaking through from the release profile overrides.
