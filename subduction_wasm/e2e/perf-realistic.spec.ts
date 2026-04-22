import { test, expect } from "@playwright/test";
import { URL } from "./config";

/**
 * Realistic-scale browser workload benchmarks.
 *
 * Where `perf.spec.ts` measures micro-operations (single commit, hash, handshake),
 * this spec measures at sizes that match actual-user workloads:
 *
 *   - **1 000-commit document ingest** — typical long-running collaborative doc
 *   - **10 000-commit ingest** — power-user stress test
 *   - **IndexedDB cold hydrate** — what a user pays on browser tab restore
 *
 * None of these tests depend on the `data/trees` on-disk fixture (which is
 * gitignored and may be absent). Instead they synthesize matching-size data
 * in-browser so CI can run the same workload anywhere.
 *
 * # Why a separate spec file?
 *
 * Playwright's default timeout is 30 s. These tests need longer — they set
 * `test.setTimeout(...)` per test. Keeping them separate from `perf.spec.ts`
 * lets the micro suite stay fast.
 *
 * # Running
 *
 * ```sh
 * cd subduction_wasm && pnpm exec playwright test perf-realistic.spec.ts --project=chromium
 * ```
 *
 * # Output
 *
 * Each test attaches a JSON summary to the Playwright report. Format matches
 * the wasm-pack-test harness (`subduction_bench_support::harness::wasm::Summary`).
 */

declare global {
  interface Window {
    subduction: any;
    subductionReady: boolean;
  }
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe("Realistic-scale browser workloads", () => {
  // 1 000 commits is about an hour of dense collaborative editing.
  test("ingest 1000 commits with in-memory storage", async ({ page }, testInfo) => {
    test.setTimeout(120_000);
    await page.addInitScript(() => { /* no-op, reserved for instrumentation */ });

    const summary = await page.evaluate(async (count) => {
      const {
        Subduction, MemoryStorage, MemorySigner, SedimentreeId, CommitId,
      } = window.subduction;

      const signer = MemorySigner.generate();
      const storage = new MemoryStorage();
      const syncer = new Subduction(signer, storage);

      const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0xa1));
      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);

      // Pre-materialise commit ids outside the measurement window so the
      // number reflects `addCommit` cost only (not RNG).
      const ids = new Array(count);
      for (let i = 0; i < count; i++) {
        const bytes = new Uint8Array(32);
        new DataView(bytes.buffer).setBigUint64(0, BigInt(i), true);
        ids[i] = new CommitId(bytes);
      }

      const start = performance.now();
      let prev: any = null;
      for (let i = 0; i < count; i++) {
        const parents = prev ? [prev] : [];
        await syncer.addCommit(sedId, ids[i], parents, blob);
        prev = ids[i];
      }
      const end = performance.now();
      const total = end - start;

      return {
        name: `browser/realistic/ingest_${count}_commits_memory`,
        unit: "ms",
        iters: 1,
        min: total,
        median: total,
        mean: total,
        p95: total,
        max: total,
        commits: count,
        ms_per_commit: total / count,
      };
    }, 1_000);

    console.log(JSON.stringify(summary));
    expect(summary.median).toBeGreaterThan(0);
    expect(summary.ms_per_commit).toBeLessThan(50); // sanity — 50ms per commit is the cliff
    await testInfo.attach("perf-summary-realistic-1000", {
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json",
    });
  });

  // 10 000 commits — power-user stress. Only run on chromium by default;
  // firefox/webkit can be added later via `--project` flags.
  test.skip(({ browserName }) => browserName !== "chromium",
    "10k-commit ingest runs on chromium only by default");
  test("ingest 10000 commits with in-memory storage (chromium)", async ({ page }, testInfo) => {
    test.setTimeout(600_000);

    const summary = await page.evaluate(async (count) => {
      const {
        Subduction, MemoryStorage, MemorySigner, SedimentreeId, CommitId,
      } = window.subduction;

      const signer = MemorySigner.generate();
      const storage = new MemoryStorage();
      const syncer = new Subduction(signer, storage);

      const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0xa2));
      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);

      let prev: any = null;
      const start = performance.now();
      for (let i = 0; i < count; i++) {
        const bytes = new Uint8Array(32);
        new DataView(bytes.buffer).setBigUint64(0, BigInt(i), true);
        const id = new CommitId(bytes);
        const parents = prev ? [prev] : [];
        await syncer.addCommit(sedId, id, parents, blob);
        prev = id;
      }
      const total = performance.now() - start;

      return {
        name: `browser/realistic/ingest_${count}_commits_memory`,
        unit: "ms",
        iters: 1,
        min: total,
        median: total,
        mean: total,
        p95: total,
        max: total,
        commits: count,
        ms_per_commit: total / count,
      };
    }, 10_000);

    console.log(JSON.stringify(summary));
    expect(summary.median).toBeGreaterThan(0);
    await testInfo.attach("perf-summary-realistic-10000", {
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json",
    });
  });

  // IndexedDB cold hydrate: populate IDB with N commits, drop the Subduction
  // instance, then measure `Subduction.hydrate()` wall-time to reconstruct
  // the in-memory sedimentree state from the IDB-backed store.
  test("IndexedDB cold hydrate with 500 commits", async ({ page }, testInfo) => {
    test.setTimeout(120_000);

    const summary = await page.evaluate(async (count) => {
      const {
        Subduction, IndexedDbStorage, MemorySigner, SedimentreeId, CommitId,
      } = window.subduction;

      // Populate once.
      const signer = MemorySigner.generate();
      const storage1 = await IndexedDbStorage.setup((window as any).indexedDB, undefined);
      const syncer1 = new Subduction(signer, storage1);

      const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0xa3));
      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);

      let prev: any = null;
      for (let i = 0; i < count; i++) {
        const bytes = new Uint8Array(32);
        new DataView(bytes.buffer).setBigUint64(0, BigInt(i), true);
        const id = new CommitId(bytes);
        const parents = prev ? [prev] : [];
        await syncer1.addCommit(sedId, id, parents, blob);
        prev = id;
      }

      // Now measure the cold hydrate path. Open a fresh IndexedDbStorage
      // handle backed by the same (persistent) DB and run Subduction.hydrate.
      const storage2 = await IndexedDbStorage.setup((window as any).indexedDB, undefined);

      const start = performance.now();
      const syncer2 = await Subduction.hydrate(signer, storage2);
      const total = performance.now() - start;

      const ids = await syncer2.sedimentreeIds();

      return {
        name: `browser/realistic/hydrate_${count}_commits_idb`,
        unit: "ms",
        iters: 1,
        min: total,
        median: total,
        mean: total,
        p95: total,
        max: total,
        commits: count,
        hydrated_sedimentree_count: ids.length,
      };
    }, 500);

    console.log(JSON.stringify(summary));
    expect(summary.median).toBeGreaterThan(0);
    expect(summary.hydrated_sedimentree_count).toBeGreaterThanOrEqual(1);
    await testInfo.attach("perf-summary-realistic-hydrate", {
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json",
    });
  });
});
