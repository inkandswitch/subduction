# Subduction Wasm bindings

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Build package

```
wasm-pack build --target web --out-dir pkg
```

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

View Playwright report:
```
pnpm run test:report
```

## Naming Conventions

`wasm-bindgen`-generated types ownership rules can be confusing and cumbersome, with different behavior depending on which side of the Wasm boundary your type was created from. To help keep this straight, we adopt the naming convention:

> [!NOTE]
> Types exported from Rust are prefixed with `Wasm`.
> Types imported from JS are prefixed with `Js`.

## Debugging with Chrome DevTools

Subduction's Wasm packages can ship a parallel `./debug` subpath export that preserves DWARF debug info, enabling source-level step-through in Chrome DevTools. This section applies to `@automerge/subduction` but is also valid for `@automerge/sedimentree`, `@automerge/automerge-sedimentree`, and `@automerge/automerge-subduction`.

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
