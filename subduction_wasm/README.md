# Subduction Wasm bindings

> [!WARNING]
> This is an early release preview. It has a very unstable API. No guarantees are given. DO NOT use for production use cases at this time. USE AT YOUR OWN RISK.

## Build package

From the repository root:

```sh
nix build .#subduction-js
```

This builds and tests the npm package, including the `/debug` variant, and writes
`result/subduction.tgz`. See [Releasing the JavaScript package](../RELEASING.md)
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

View Playwright report:
```
pnpm run test:report
```

## Stored updates

```javascript
const controller = new AbortController();
const updates = node.loadAndWatch(treeId, { signal: controller.signal });
// Call controller.abort() to cancel, or use AbortSignal.timeout(ms) for a timeout.
try {
  for await (const event of updates) {
    // commit / fragment: metadata and Uint8Array blob
    // storageChecked / peerSynced / rescanRequired: ordered markers
    applyStoredEvent(event);
  }
} finally {
  updates.close();
}
```

`watchStored(treeId)` starts no network work. `loadAndWatch` emits local contents
first, then syncs and subscribes on all current and future connections, including
reconnects, even when local contents exist. It remains open when storage is empty
or current peers have no data, waiting for future updates and connections.
Construction admits the stream; it does not promise readiness. Markers never end
iteration. Connections are contacted concurrently without a concurrency cap or
loading/peer deadline. Stalled connections do not block new ones to the same peer.
`peerSynced` may occur again for a peer after reconnect or on another connection.
The optional `{ signal }` replaces the former numeric timeout argument; omit it
to control cancellation using `close()` or `for await` break.

`return()` (including `for await` break) and `close()` cancel observation and settle
pending reads. Concurrent reads are serialized; close may safely reenter during
conversion. Node close rejects pending reads with `SubductionClosedError` and new
stream construction throws the same error. Storage/local-processing failures use
`StoredDataError`. Unlike one-shot `fetchBlobs`, exhausting current peers does not
produce a `DocumentUnavailableError`.

Aborting the signal cancels observation and outstanding sync attempts. Pending
and subsequent reads reject with `signal.reason` (normally an `AbortError`, or
`TimeoutError` with `AbortSignal.timeout(ms)`). A signal already aborted at
construction throws that reason immediately. Closing/breaking the iterator
removes its abort listener, as does abort or a terminal read. Timing out a
`Promise.race` alone does not cancel the iterator.

Cancellation neither rolls back writes nor unsubscribes. See
[SYNC_COMPLETION.md](../SYNC_COMPLETION.md) for shared validity, recovery,
identity, and peer-marker guarantees. `fetchBlobs` collects that same loader
without subscriptions, returning from cached contents without contacting peers.

## Closing a node

```javascript
try {
  // Connect, store, and sync. Observe errors from these operations as usual.
} finally {
  try {
    await node.close();
  } finally {
    node.free(); // Release the Wasm wrapper after shutdown.
  }
}
```

`close(): Promise<void>` starts shutdown immediately, before its promise is awaited:

- New asynchronous operations reject with `SubductionClosedError`. Active API calls
  are cancelled and reject with the same error; callers should observe their promises.
- The node stops accepting inbound work, disconnects its transports (including
  handshakes in progress), and awaits its owned manager, listener, dispatch, fan-out,
  ephemeral-delivery, and long-poll driver tasks.
- Graceful draining has one shared **five-second deadline**. At expiry, remaining
  Rust tasks are aborted and joined. Close rejects with `SubductionCloseError` if
  draining timed out or transport cleanup failed; every transport is attempted even
  when another fails. The event loop must keep running: synchronous blocking code
  and Wasm traps cannot be forcibly interrupted.
- Repeated/concurrent calls observe the same completion or error. Abandoning the
  close promise does not stop shutdown. `free()` alone initiates best-effort shutdown
  but cannot await it; prefer explicit close. Do not free a wrapper still borrowed by
  outstanding API calls.

**Cancellation is not rollback.** Already-started JavaScript promises—especially
storage writes—can still complete after close. No cancelled Rust operation resumes
its continuation, but Subduction cannot cancel arbitrary work inside a supplied JS
storage, signer, policy, or transport. Built-in WebSocket dialing and HTTP requests
are cancelled and their handlers/timers released. No remote flush, remote storage
acknowledgement, or permanent durability is implied by close.

The node takes ownership of transports admitted through its connection APIs and
cleans them up on handshake/registration failure as well as shutdown. A call rejected
because the node is already closing does not take ownership of its argument. Do not
share one transport between nodes. For supplied transports, `disconnect()` must stop
I/O, release transport-owned resources, and wake blocked reads. Tasks started outside
the node remain the supplier's responsibility. Cleanup callbacks must not await the
same node's close (that would wait on themselves).

The supplied storage and signer are **not** closed or freed; they may be shared by
other nodes. The synchronous `storage` and `commitDepth` accessors remain available
until `free()`. Existing sync results are unchanged. JS promise result types are
unchanged; the Rust Wasm-binding methods now return `Result<_, JsValue>` to represent
lifecycle rejection alongside their previous named errors.

`disconnectAll()` only disconnects peers; it leaves the node running and able to
accept new work. It is not a substitute for `close()`. Per-operation `AbortSignal`
support and `Symbol.asyncDispose` are not included in this change.

## Naming Conventions

`wasm-bindgen`-generated types ownership rules can be confusing and cumbersome, with different behavior depending on which side of the Wasm boundary your type was created from. To help keep this straight, we adopt the naming convention:

> [!NOTE]
> Types exported from Rust are prefixed with `Wasm`.
> Types imported from JS are prefixed with `Js`.

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
