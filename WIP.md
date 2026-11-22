# WIP: Subduction on `automerge::next` (hashless)

Work in progress, so we can pick this up on another machine. Delete this file
before merging.

## Repos and branches

All three checkouts sit side by side (`~/subduction`, `~/automerge`, `~/automerge-repo`).
The relative paths below depend on that layout.

| Repo | Branch | State |
|---|---|---|
| `subduction` | `hashless` | this commit |
| `automerge` (`github.com/automerge/automerge`) | `hashless` | origin at `835942e7d WIP`. **At handoff the local checkout also had ~36 uncommitted files, so push those too if they matter.** |
| `automerge-repo` | `subductionjs` (tracks `origin/subductionjs`, at `ce1f63de`) | **Uncommitted local changes. Recreate them from the "automerge-repo side" section below.** |

## Goal

Use only `automerge::next` (no audit mode) in Subduction, and test real
end-to-end sync between automerge-repo (JS, `subductionjs` branch) and a Rust
peer on `automerge::next`.

Rules agreed with Orion:
- Use `automerge::next` types only. Never use the crate-root `automerge::Automerge` / `AutoCommit` (the audit types).
- Don't call `get_changes()`; use `fragments_after()`. If change-level access is unavoidable, call `into_audit().get_changes()` explicitly.
- Get the change count from `next::Automerge::num_changes()` and fragment sizes from `next::Fragment::len()`.

## What's done in this repo

- `Cargo.toml`: `automerge = { version = "0.12.0", path = "../automerge/rust/automerge" }`.
  `Cargo.lock` therefore resolves automerge 0.12.0 and hexane 1.0.0-alpha.6 from
  disk. **This only builds with automerge checked out beside this repo; swap
  it for a released or git version before merging.**
- `automerge_subduction_ingest/src/lib.rs` (new): `IngestResult`,
  `ingest_automerge`, `ingest_fragments` (splits by `meta.level`: level 0 becomes
  `LooseCommit`, level 1+ becomes `Fragment`), and `parse_doc_id`.
- `automerge_subduction_ingest/src/main.rs`: uses the lib and `next::Automerge`.
  `change_count` (used only in printed stats) is `num_changes()`.
  `covered_count` is the sum of `Fragment::len()` over level-1+ fragments, which
  assumes those fragments never overlap.
- `automerge_subduction_ingest/tests/bundle_determinism.rs`: on `next`.
  The incremental-load test rebuilds the document from
  `fragments_after(&[], false)` + `load_incremental`. It used to replay
  `get_changes(&[])`, which fails intermittently on `next` with "needs change
  hashes that are only kept in audit mode", because which hashes get freed
  depends on the random actor ID.
- `automerge_subduction_ingest/examples/next_peer.rs` (new): a Rust Subduction
  peer. It connects over WebSocket, runs `sync_with_peer`, then `get_blobs`. It
  concatenates the blobs and loads them with a single `AutoCommit::load`, checks
  `--expect key=value`, writes `--put key=value` as **text objects**, and pushes
  `fragments_after(&before, false)` via `ingest_fragments` + `add_sedimentree`.
- Upstream API change already absorbed: `fragments_after(heads, partial)` gained
  the `partial` arg. We pass `false`, which re-sends a partly-covered fragment
  whole and is fast. `true` sends each change exactly once but may rebuild freed
  hashes, which is slow.
- At handoff `cargo fmt --check`, `cargo clippy --workspace --all-targets` and
  `cargo test --workspace` were clean, and `automerge_subduction_wasm` checked
  for `wasm32-unknown-unknown`.

## automerge-repo side (recreate on the new machine)

```sh
cd ~/automerge-repo && git switch subductionjs   # tracks origin/subductionjs
```

1. Add to `overrides:` in `pnpm-workspace.yaml`:
   ```yaml
   "@automerge/automerge-subduction": link:../subduction/automerge_subduction_wasm
   "@automerge/automerge": link:../automerge/javascript
   ```
2. Build the wasm package (needs the Nix flake for `wasm-bodge`):
   `cd ~/subduction && nix develop -c bodge automerge_subduction_wasm`.
   The JS automerge at `~/automerge/javascript` needs a built `dist/` (`npm run build` there).
3. Install with pnpm 11. The flake only has 10.x, so use `npx -y pnpm@11.5.0 install`.
4. Create `packages/automerge-repo/test/subduction/RustNextPeer.test.ts` (full
   contents at the end of this file) and run:
   `npx -y pnpm@11.5.0 exec vitest run packages/automerge-repo/test/subduction/RustNextPeer.test.ts`
   It ran `cargo run --example next_peer` in `../subduction` (override with
   `SUBDUCTION_DIR`) and passed 5 of 5 runs.
5. Full suite: `npx -y pnpm@11.5.0 exec vitest run packages/automerge-repo/test/subduction`.

## Findings

1. **npm `@automerge/automerge` 3.3.2 can't read `next` bundles.** `fragments_after`
   writes chunk type 4 (the new bundle format). 3.3.2's `loadIncremental` quietly
   ignores it: no error, no data. That is why the JS automerge override to
   `../automerge/javascript` is needed.
2. **Out-of-order loads in `next` mode.** `load_incremental` into an *empty*
   document takes chunks in any order. Into a non-empty one, a change whose
   dependencies are missing is dropped (Rust) or throws
   `change's deps should already be in the document` (JS, `auditMode: false`,
   which is the default). Audit mode queues it instead. The Rust peer avoids
   this by loading all pulled blobs in one call. Standalone JS repro (in the
   session scratchpad at handoff):
   ```js
   import * as A from "~/automerge/javascript/dist/mjs/entrypoints/fullfat_node.js"
   let src = A.init({ auditMode: false }), chunks = []
   for (let i = 0; i < 3; i++) { src = A.change(src, d => { d[`k${i}`] = i }); chunks.push(A.saveIncremental(src)) }
   let dst = A.load(chunks[0], { auditMode: false })
   A.loadIncremental(dst, chunks[2])   // throws in next mode; works with auditMode: true
   ```
3. **Resulting regression in automerge-repo with the local JS automerge:**
   "rapid sequential edits all arrive" (`Topology.test.ts`) fails every time,
   and 2–3 others fail some of the time ("resyncSubduction re-arms…", "emits
   subduction-remote-heads…", AdapterTopology "rapid sequential edits"). With
   npm 3.3.2 all 102 passed. Cause: `#flushInbound` in
   `packages/automerge-repo/src/subduction/source.ts` calls
   `Automerge.loadIncremental(doc, merged)`. When a live-synced commit arrives
   before its dependency, that throws, and the catch logs at debug level and
   **drops the batch**.
4. JS Automerge 3 writes plain strings as `Text` objects. A Rust scalar string
   would show up in JS as `ImmutableString`, which is why `next_peer` uses text
   objects.
5. The doc ID mapping matches: a base58check doc ID zero-padded to a 32-byte
   `SedimentreeId` on both sides.

## Open questions / next steps

- Decide where out-of-order changes get handled: should `next` mode queue
  changes with missing dependencies like audit mode, or should automerge-repo
  buffer inbound blobs until their dependencies arrive? Then fix the
  regressions above.
- Replace the automerge path dependency before merging.
- Check whether `covered_count` can double-count (overlapping level-1+ fragments).
- Optionally upstream `RustNextPeer.test.ts` to automerge-repo (it needs cargo
  and the side-by-side layout, so probably as an opt-in test).

## `RustNextPeer.test.ts` (full contents)

```ts
import { describe, it, expect, afterEach } from "vitest"
import { once } from "events"
import { execFile } from "child_process"
import { promisify } from "util"
import path from "path"
import { WebSocketServer } from "ws"

import {
  Subduction,
  MemorySigner,
  MemoryStorage,
} from "@automerge/automerge-subduction"
import { Repo } from "../../src/Repo.js"
import { DummyStorageAdapter } from "../../src/helpers/DummyStorageAdapter.js"
import { type PeerId } from "../../src/types.js"
import { toSedimentreeId } from "../../src/subduction/helpers.js"
import { WebSocketTransport } from "../../src/subduction/websocket-transport.js"
import { awaitDoc } from "../helpers/awaitDoc.js"
import { waitFor } from "../helpers/waitFor.js"

// A Rust peer (subduction's `next_peer` example, built on automerge::next)
// pulls a document a JS Repo created, checks it, edits it, and pushes back.
const SUBDUCTION_DIR =
  process.env.SUBDUCTION_DIR ??
  path.resolve(__dirname, "../../../../../subduction")

const run = promisify(execFile)

async function runRustPeer(args: string[]) {
  // Async on purpose: the server lives in this process's event loop.
  return run(
    "cargo",
    [
      "run",
      "-q",
      "-p",
      "automerge_subduction_ingest",
      "--example",
      "next_peer",
      "--",
      ...args,
    ],
    { cwd: SUBDUCTION_DIR, timeout: 120_000 }
  )
}

async function startSubductionServer() {
  const subduction = new Subduction({
    signer: new MemorySigner(),
    storage: new MemoryStorage(),
  })
  const wss = new WebSocketServer({ port: 0 })
  await once(wss, "listening")
  const address = wss.address()
  if (typeof address === "string") throw new Error("unexpected address type")
  const serviceName = `localhost:${address.port}`

  wss.on("connection", ws => {
    subduction
      .acceptTransport(new WebSocketTransport(ws as any), serviceName)
      .catch(e => console.error("acceptTransport failed:", e))
  })

  return {
    url: `ws://localhost:${address.port}`,
    serviceName,
    subduction,
    async close() {
      await subduction.disconnectAll()
      await new Promise<void>((resolve, reject) =>
        wss.close(err => (err ? reject(err) : resolve()))
      )
    },
  }
}

describe("JS Repo ↔ Rust automerge::next peer via Subduction", () => {
  const cleanups: (() => Promise<void> | void)[] = []
  afterEach(async () => {
    for (const c of cleanups.splice(0).reverse()) await c()
  })

  it("Rust pulls the JS edit, and JS receives the Rust edit", async () => {
    const server = await startSubductionServer()
    cleanups.push(() => server.close())

    const repo = new Repo({
      peerId: "js-client" as PeerId,
      storage: new DummyStorageAdapter(),
      subductionWebsocketEndpoints: [server.url],
    })
    cleanups.push(() => repo.shutdown())

    const handle = repo.create<{ fromJs?: string; fromRust?: string }>()
    handle.change(d => {
      d.fromJs = "hello from js"
    })

    const sid = toSedimentreeId(handle.documentId)
    await waitFor(async () => {
      const blobs = await server.subduction.getBlobs(sid)
      expect(blobs?.length ?? 0).toBeGreaterThan(0)
    }, 10_000)

    const { stderr } = await runRustPeer([
      "--server",
      server.url,
      "--service-name",
      server.serviceName,
      "--doc-id",
      handle.url,
      "--expect",
      "fromJs=hello from js",
      "--put",
      "fromRust=hello from rust",
    ])
    expect(stderr).toContain("saw fromJs=hello from js")

    await awaitDoc(handle, h => h.doc()?.fromRust === "hello from rust", {
      timeout: 10_000,
    })
    expect(handle.doc()).toEqual({
      fromJs: "hello from js",
      fromRust: "hello from rust",
    })
  }, 180_000)
})
```
