import { test, expect } from "@playwright/test";
import { URL } from "./config";

/**
 * Browser IndexedDB write-cost characterization.
 *
 * Node has no IndexedDB; `wasm-pack test --node` cannot exercise the
 * `IndexedDbStorage` backend at all. The existing `perf.spec.ts` covers a
 * single-commit warm-state write, and `perf-realistic.spec.ts` covers a
 * one-shot cold hydrate. Neither answers the questions we actually have
 * about the IDB write path:
 *
 *   1. How does per-commit write cost scale as the store grows?
 *      (Steady-state plateau vs degradation curve.)
 *   2. How does write cost scale with blob size? IDB serializes through
 *      structured-clone; large `Uint8Array`s may be the dominant cost.
 *   3. Can we saturate the IDB transaction with concurrent `addCommit`
 *      calls, or does each write force its own readwrite txn?
 *   4. What does a cold (just-opened) IDB write cost vs a warmed-up one
 *      after N commits already exist?
 *
 * Each test uses a distinct database name so cross-test residue can't
 * pollute the timing curve. Browsers persist IDB across pages on the
 * same origin within a Playwright run; deleteDatabase calls clean up.
 *
 * # Output
 *
 * Same JSON-summary shape as `perf.spec.ts` so any downstream parser
 * (`scripts/parse-wasm-bench-output.sh`) can ingest both. Each test
 * attaches to the Playwright report via `testInfo.attach`.
 *
 * # Running
 *
 * ```sh
 * cd subduction_wasm && pnpm exec playwright test perf-idb.spec.ts
 *
 * # Single browser, with stdout visible:
 * cd subduction_wasm && pnpm exec playwright test perf-idb.spec.ts \
 *   --project=chromium --reporter=list
 * ```
 */

declare global {
  interface Window {
    subduction: any;
    subductionReady: boolean;
  }
}

// ---------------------------------------------------------------------------
// Per-bench setup
// ---------------------------------------------------------------------------

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30_000 : 10_000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

// ---------------------------------------------------------------------------
// Browser-side helpers, injected as a script tag so they share a JS realm
// with the wasm bindings exposed on `window.subduction`.
// ---------------------------------------------------------------------------

const HELPERS_SRC = `
  // Sample mean / median / p95 from an array of ms timings.
  function summarize(name, samples) {
    const sorted = samples.slice().sort((a, b) => a - b);
    const n = sorted.length;
    const min = sorted[0];
    const max = sorted[n - 1];
    const p95Idx = Math.max(0, Math.ceil(0.95 * n) - 1);
    const p95 = sorted[p95Idx];
    const median = n % 2 === 0
      ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2
      : sorted[(n - 1) / 2];
    const mean = sorted.reduce((a, b) => a + b, 0) / n;
    return { name, unit: "ms", iters: n, min, median, mean, p95, max };
  }

  // Build a CommitId from a 64-bit little-endian counter padded to 32 bytes.
  // Distinct counters give distinct commit ids without paying RNG cost
  // inside the measured loop.
  function mkCommitId(seq) {
    const { CommitId } = window.subduction;
    const bytes = new Uint8Array(32);
    new DataView(bytes.buffer).setBigUint64(0, BigInt(seq), true);
    return new CommitId(bytes);
  }

  // Delete an IDB database by name. Resolves once the deletion is done
  // (or blocked-but-fired). Used between tests so each scenario gets a
  // truly empty store.
  function deleteIdb(name) {
    return new Promise((resolve) => {
      const req = window.indexedDB.deleteDatabase(name);
      req.onsuccess = () => resolve();
      req.onerror = () => resolve();
      req.onblocked = () => resolve();
    });
  }

  window.__perfHelpers = { summarize, mkCommitId, deleteIdb };
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test.describe("Browser IndexedDB write characterization", () => {
  /**
   * Per-commit write cost, sampled at increasing store sizes within the
   * same Subduction instance.
   *
   * Each "checkpoint" measures the cost of the next 50 sequential commits
   * given that N commits already exist in the store. The summaries let us
   * see whether IDB write cost is flat (good — the index isn't the
   * bottleneck) or rising (bad — would point at autoIncrement, index
   * maintenance, or structured-clone tax growing with store size).
   */
  test("addCommit per-commit cost vs store size", async ({ page }, testInfo) => {
    test.setTimeout(180_000);
    await page.addScriptTag({ content: HELPERS_SRC });

    const summaries = await page.evaluate(async () => {
      const { Subduction, IndexedDbStorage, MemorySigner, SedimentreeId } = window.subduction;
      const { summarize, mkCommitId, deleteIdb } = (window as any).__perfHelpers;

      const dbName = "perf-idb-scaling";
      await deleteIdb(dbName);

      const signer = MemorySigner.generate();
      const storage = await IndexedDbStorage.setup((window as any).indexedDB, dbName);
      const syncer = new Subduction(signer, storage);
      const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0x42));

      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);

      // Checkpoints define the prefix size *before* the measurement window.
      // Sample window = 50 commits at each checkpoint; store grows
      // monotonically across checkpoints (no reset).
      const checkpoints = [0, 100, 500, 1_000, 2_500];
      const windowSize = 50;

      const results: any[] = [];
      let seq = 0;

      for (const target of checkpoints) {
        // Fast-forward the store to `target` commits without measuring.
        while (seq < target) {
          await syncer.addCommit(sedId, mkCommitId(seq), [], blob);
          seq += 1;
        }

        // Measure the next `windowSize` commits one-by-one.
        const samples = new Array(windowSize);
        for (let i = 0; i < windowSize; i++) {
          const id = mkCommitId(seq);
          const start = performance.now();
          await syncer.addCommit(sedId, id, [], blob);
          samples[i] = performance.now() - start;
          seq += 1;
        }
        results.push({
          ...summarize(`browser/idb/addCommit_at_${target}_existing`, samples),
          existing_commits: target,
        });
      }

      await deleteIdb(dbName);
      return results;
    });

    for (const s of summaries) {
      console.log(JSON.stringify(s));
      expect(s.iters).toBeGreaterThan(0);
      expect(s.median).toBeGreaterThan(0);
    }
    await testInfo.attach("perf-summary-idb-scaling", {
      body: JSON.stringify(summaries, null, 2),
      contentType: "application/json",
    });
  });

  /**
   * Write cost vs blob size. IDB serializes via structured-clone; for
   * large payloads the clone + write should dominate the measurement
   * and tell us whether browser-side compression would pay off.
   */
  test("addCommit cost vs blob size", async ({ page }, testInfo) => {
    test.setTimeout(120_000);
    await page.addScriptTag({ content: HELPERS_SRC });

    const summaries = await page.evaluate(async () => {
      const { Subduction, IndexedDbStorage, MemorySigner, SedimentreeId } = window.subduction;
      const { summarize, mkCommitId, deleteIdb } = (window as any).__perfHelpers;

      const dbName = "perf-idb-blobsize";
      await deleteIdb(dbName);

      const sizes: [string, number][] = [
        ["64b", 64],
        ["1kb", 1024],
        ["16kb", 16 * 1024],
        ["256kb", 256 * 1024],
        ["1mb", 1024 * 1024],
      ];

      const results: any[] = [];
      let seq = 0;

      for (const [label, size] of sizes) {
        const signer = MemorySigner.generate();
        const storage = await IndexedDbStorage.setup((window as any).indexedDB, dbName);
        const syncer = new Subduction(signer, storage);
        const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0x55));

        const blob = new Uint8Array(size);
        crypto.getRandomValues(blob);

        // Heavier payloads use fewer iterations; total bytes written is
        // bounded so the suite stays under the 120s test timeout.
        const iters = size >= 256 * 1024 ? 8 : size >= 16 * 1024 ? 20 : 30;

        // Warm up so JIT and the IDB connection are settled.
        for (let i = 0; i < 3; i++) {
          await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
        }

        const samples = new Array(iters);
        for (let i = 0; i < iters; i++) {
          const start = performance.now();
          await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
          samples[i] = performance.now() - start;
        }

        results.push({
          ...summarize(`browser/idb/addCommit_blob_${label}`, samples),
          blob_bytes: size,
          mb_per_sec_median: (size / 1_000_000) / (samples.slice().sort((a: number, b: number) => a - b)[Math.floor(samples.length / 2)] / 1000),
        });

        await deleteIdb(dbName);
      }
      return results;
    });

    for (const s of summaries) {
      console.log(JSON.stringify(s));
      expect(s.iters).toBeGreaterThan(0);
    }
    await testInfo.attach("perf-summary-idb-blobsize", {
      body: JSON.stringify(summaries, null, 2),
      contentType: "application/json",
    });
  });

  /**
   * Concurrency: do `Promise.all([addCommit, ...])` calls overlap their
   * IDB transactions, or does each call serialize?
   *
   * Compares total wall time of N sequential addCommit calls vs N
   * concurrent ones. If the ratio is ~1, IDB serializes us anyway and
   * any "batching" speedup must come from amortizing wasm boundary cost.
   */
  test("addCommit sequential vs concurrent", async ({ page }, testInfo) => {
    test.setTimeout(120_000);
    await page.addScriptTag({ content: HELPERS_SRC });

    const result = await page.evaluate(async () => {
      const { Subduction, IndexedDbStorage, MemorySigner, SedimentreeId } = window.subduction;
      const { mkCommitId, deleteIdb } = (window as any).__perfHelpers;

      const dbName = "perf-idb-concurrent";
      await deleteIdb(dbName);

      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);
      const batches = [10, 50, 200];
      const trials = 5;

      const out: any[] = [];
      let seq = 0;

      for (const batchSize of batches) {
        const signer = MemorySigner.generate();
        const storage = await IndexedDbStorage.setup((window as any).indexedDB, dbName);
        const syncer = new Subduction(signer, storage);
        const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0x77));

        // Sequential trials.
        const seqSamples: number[] = [];
        for (let t = 0; t < trials; t++) {
          const start = performance.now();
          for (let i = 0; i < batchSize; i++) {
            await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
          }
          seqSamples.push(performance.now() - start);
        }

        // Concurrent trials.
        const concSamples: number[] = [];
        for (let t = 0; t < trials; t++) {
          const start = performance.now();
          const promises = [];
          for (let i = 0; i < batchSize; i++) {
            promises.push(syncer.addCommit(sedId, mkCommitId(seq++), [], blob));
          }
          await Promise.all(promises);
          concSamples.push(performance.now() - start);
        }

        const med = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];
        const seqMedian = med(seqSamples);
        const concMedian = med(concSamples);
        out.push({
          name: `browser/idb/addCommit_batch_${batchSize}_seq_vs_concurrent`,
          unit: "ms",
          batch_size: batchSize,
          trials,
          sequential_median_total_ms: seqMedian,
          concurrent_median_total_ms: concMedian,
          sequential_per_commit_ms: seqMedian / batchSize,
          concurrent_per_commit_ms: concMedian / batchSize,
          // <1 means concurrency helps; ~1 means IDB serialized us;
          // >1 means concurrency hurt (contention).
          ratio_concurrent_over_sequential: concMedian / seqMedian,
        });

        await deleteIdb(dbName);
      }
      return out;
    });

    for (const r of result) {
      console.log(JSON.stringify(r));
      expect(r.sequential_median_total_ms).toBeGreaterThan(0);
      expect(r.concurrent_median_total_ms).toBeGreaterThan(0);
    }
    await testInfo.attach("perf-summary-idb-concurrent", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
  });

  /**
   * Cold-vs-warm comparison. The first IDB write after `setup()` may be
   * slower because the connection, transaction queue and onupgradeneeded
   * tail are still settling. Measures the first single addCommit on a
   * just-opened database vs steady-state at N=200 commits.
   */
  test("addCommit cold first-write vs warm steady-state", async ({ page }, testInfo) => {
    test.setTimeout(60_000);
    await page.addScriptTag({ content: HELPERS_SRC });

    const result = await page.evaluate(async () => {
      const { Subduction, IndexedDbStorage, MemorySigner, SedimentreeId } = window.subduction;
      const { mkCommitId, deleteIdb } = (window as any).__perfHelpers;

      const dbName = "perf-idb-cold";
      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);
      const trials = 10;

      const coldSamples: number[] = [];
      const warmSamples: number[] = [];

      for (let t = 0; t < trials; t++) {
        await deleteIdb(dbName);

        // Cold: first addCommit after a fresh setup().
        const signer = MemorySigner.generate();
        const storage = await IndexedDbStorage.setup((window as any).indexedDB, dbName);
        const syncer = new Subduction(signer, storage);
        const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0xc0));

        let seq = 0;
        const cs = performance.now();
        await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
        coldSamples.push(performance.now() - cs);

        // Warm: pre-populate to 200 commits, then measure the next one.
        for (let i = 0; i < 199; i++) {
          await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
        }
        const ws = performance.now();
        await syncer.addCommit(sedId, mkCommitId(seq++), [], blob);
        warmSamples.push(performance.now() - ws);
      }

      await deleteIdb(dbName);

      const med = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];
      return {
        name: "browser/idb/cold_vs_warm_first_write",
        unit: "ms",
        trials,
        cold_median_ms: med(coldSamples),
        warm_median_ms: med(warmSamples),
        cold_max_ms: Math.max(...coldSamples),
        warm_max_ms: Math.max(...warmSamples),
        // >1 means the cold path is slower; values close to 1 mean the
        // first-write penalty is negligible.
        cold_over_warm_ratio: med(coldSamples) / med(warmSamples),
      };
    });

    console.log(JSON.stringify(result));
    expect(result.cold_median_ms).toBeGreaterThan(0);
    expect(result.warm_median_ms).toBeGreaterThan(0);
    await testInfo.attach("perf-summary-idb-cold-vs-warm", {
      body: JSON.stringify(result, null, 2),
      contentType: "application/json",
    });
  });
});
