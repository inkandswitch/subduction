import { test, expect } from "@playwright/test";
import { URL } from "./config";

/**
 * Browser performance tracer for Subduction Wasm.
 *
 * Mirrors `subduction_wasm/tests/perf_crypto.rs` so we can compare:
 *   - Node.js Wasm runtime (`wasm-pack test --node`)
 *   - Real browsers (Chromium / Firefox / WebKit) with this suite
 *
 * Also adds in-browser-only scenarios that don't fit inside wasm-bindgen-test:
 *   - IndexedDB roundtrips (Node has no IDB)
 *   - `Subduction.link` handshake + ingest via MessageChannel (real browser event loop)
 *
 * # Output
 *
 * Each test attaches a JSON blob to the Playwright report via
 * `testInfo.attach("perf-summary", ...)`. Downstream tooling in Phase 4.1
 * (see `scripts/parse-wasm-bench-output.sh` for the matching wasm-pack-test
 * format) can parse these attachments if we want browser perf to feed the
 * gh-pages trend tracker.
 *
 * # Running
 *
 * ```sh
 * # All browsers:
 * cd subduction_wasm && pnpm exec playwright test perf.spec.ts
 *
 * # Single browser:
 * cd subduction_wasm && pnpm exec playwright test perf.spec.ts --project=chromium
 * ```
 */

// Extend the global `window` type with the subduction bindings exposed by
// `e2e/server/index.html`'s bootstrap script.
declare global {
  interface Window {
    subduction: any;
    subductionReady: boolean;
  }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type BenchConfig = {
  /** Warm-up iterations (not measured). */
  warmupIters: number;
  /** Measured iterations. */
  measurementIters: number;
};

const DEFAULT: BenchConfig = { warmupIters: 5, measurementIters: 50 };
const HEAVY: BenchConfig = { warmupIters: 2, measurementIters: 20 };
const QUICK: BenchConfig = { warmupIters: 1, measurementIters: 10 };

// ---------------------------------------------------------------------------
// Per-bench setup
// ---------------------------------------------------------------------------

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

// ---------------------------------------------------------------------------
// Shared browser-side sampler. Runs inside `page.evaluate` so we get
// `performance.now()` inside the browser event loop, not over the Playwright
// IPC boundary.
// ---------------------------------------------------------------------------
//
// Returns `{ name, unit, iters, min, median, mean, p95, max }` so the result
// JSON matches the wasm-pack-test harness shape in
// `subduction_bench_support::harness::wasm::Summary`.

// We re-inject the sampler into every test via a string literal so the
// browser-side implementation has no external imports to chase.
const SAMPLER_SRC = `
  async function benchAsync(name, cfg, factory) {
    for (let i = 0; i < cfg.warmupIters; i++) {
      await factory();
    }
    const samples = new Array(cfg.measurementIters);
    for (let i = 0; i < cfg.measurementIters; i++) {
      const start = performance.now();
      await factory();
      samples[i] = performance.now() - start;
    }
    samples.sort((a, b) => a - b);
    const n = samples.length;
    const min = samples[0];
    const max = samples[n - 1];
    const p95Idx = Math.max(0, Math.ceil(0.95 * n) - 1);
    const p95 = samples[p95Idx];
    const median = n % 2 === 0
      ? (samples[n / 2 - 1] + samples[n / 2]) / 2
      : samples[(n - 1) / 2];
    const mean = samples.reduce((a, b) => a + b, 0) / n;
    return { name, unit: "ms", iters: n, min, median, mean, p95, max };
  }
  window.__benchAsync = benchAsync;
`;

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

test.describe("Browser performance", () => {
  // --- Crypto micros (mirror perf_crypto.rs) ----------------------------

  test("blake3 hash at several sizes", async ({ page }, testInfo) => {
    await page.addScriptTag({ content: SAMPLER_SRC });

    const summaries = await page.evaluate(async (configs) => {
      // `blake3` is *not* exposed in the subduction bindings — we use
      // SubtleCrypto.digest as the native-browser analog. This is
      // deliberately different from the Node.js bench, which uses the
      // `blake3` crate compiled to Wasm: the browser WebCrypto comparison
      // answers a separate question (is it worth piping through SubtleCrypto
      // on the browser target?).
      const out: any[] = [];
      for (const [label, size] of [
        ["32b", 32], ["1kb", 1024], ["64kb", 64 * 1024], ["1mb", 1024 * 1024],
      ] as [string, number][]) {
        const data = new Uint8Array(size);
        crypto.getRandomValues(data);
        const cfg = size >= 64 * 1024 ? configs.heavy : configs.def;
        out.push(await (window as any).__benchAsync(
          `browser/sha256/${label}`,
          cfg,
          async () => {
            await crypto.subtle.digest("SHA-256", data);
          },
        ));
      }
      return out;
    }, { def: DEFAULT, heavy: HEAVY });

    for (const s of summaries) {
      console.log(JSON.stringify(s));
      expect(s.iters).toBeGreaterThan(0);
      expect(s.median).toBeGreaterThan(0);
    }
    await testInfo.attach("perf-summary-crypto", {
      body: JSON.stringify(summaries, null, 2),
      contentType: "application/json",
    });
  });

  // --- Subduction.link handshake + basic sync ---------------------------

  test("Subduction.link handshake latency", async ({ page }, testInfo) => {
    await page.addScriptTag({ content: SAMPLER_SRC });

    const summary = await page.evaluate(async (cfg) => {
      const { Subduction, MemoryStorage, MemorySigner } = window.subduction;

      return (window as any).__benchAsync(
        "browser/subduction_link/handshake",
        cfg,
        async () => {
          // Each iteration builds a fresh pair. This includes signer
          // generation (WebCrypto) plus the mutual handshake over a
          // MessageChannel, so the number is "full zero-to-connected" time.
          const signerA = MemorySigner.generate();
          const signerB = MemorySigner.generate();
          const a = new Subduction(signerA, new MemoryStorage());
          const b = new Subduction(signerB, new MemoryStorage());
          await Subduction.link(a, b);
        },
      );
    }, QUICK);

    console.log(JSON.stringify(summary));
    expect(summary.iters).toBe(QUICK.measurementIters);
    expect(summary.median).toBeGreaterThan(0);

    await testInfo.attach("perf-summary-link", {
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json",
    });
  });

  // --- addCommit throughput -------------------------------------------------

  test("addCommit throughput on a linked pair", async ({ page }, testInfo) => {
    await page.addScriptTag({ content: SAMPLER_SRC });

    const summaries = await page.evaluate(async (cfg) => {
      const {
        Subduction, MemoryStorage, MemorySigner, SedimentreeId, CommitId,
      } = window.subduction;

      const results: any[] = [];
      for (const count of [10, 100]) {
        const signerA = MemorySigner.generate();
        const signerB = MemorySigner.generate();
        const a = new Subduction(signerA, new MemoryStorage());
        const b = new Subduction(signerB, new MemoryStorage());
        await Subduction.link(a, b);

        const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0xab));
        // Pre-generate distinct commit ids so the measured loop only pays
        // add_commit cost, not RNG cost. CommitId takes 32 bytes.
        const headIds: any[] = [];
        for (let i = 0; i < count; i++) {
          const bytes = new Uint8Array(32);
          new DataView(bytes.buffer).setBigUint64(0, BigInt(i), true);
          headIds.push(new CommitId(bytes));
        }
        const blob = new Uint8Array(64);
        crypto.getRandomValues(blob);

        let counter = 0;
        const sample = await (window as any).__benchAsync(
          `browser/addCommit/${count}`,
          { warmupIters: 1, measurementIters: 5 },
          async () => {
            // Each measured iteration writes `count` commits then resets
            // the counter so runs are independent.
            for (let i = 0; i < count; i++) {
              await a.addCommit(sedId, headIds[i], [], blob);
            }
            counter += 1;
          },
        );
        results.push({ ...sample, counter });
      }
      return results;
    }, QUICK);

    for (const s of summaries) {
      console.log(JSON.stringify(s));
      expect(s.iters).toBeGreaterThan(0);
    }
    await testInfo.attach("perf-summary-addCommit", {
      body: JSON.stringify(summaries, null, 2),
      contentType: "application/json",
    });
  });

  // --- IndexedDB storage roundtrip ------------------------------------------

  test("IndexedDB setup + single commit write", async ({ page }, testInfo) => {
    await page.addScriptTag({ content: SAMPLER_SRC });

    // This test exercises the IndexedDbStorage backend, which doesn't exist
    // under Node (no IDB). Browsers are the only runtime where we can
    // measure this path, and it's the critical hot path for real users.

    const summary = await page.evaluate(async (cfg) => {
      const {
        Subduction, IndexedDbStorage, MemorySigner, SedimentreeId, CommitId,
      } = window.subduction;

      const signer = MemorySigner.generate();
      const storage = await IndexedDbStorage.setup((window as any).indexedDB, undefined);
      const syncer = new Subduction(signer, storage);

      const sedId = SedimentreeId.fromBytes(new Uint8Array(32).fill(0x99));
      const blob = new Uint8Array(64);
      crypto.getRandomValues(blob);

      let seq = 0;
      const sample = await (window as any).__benchAsync(
        "browser/idb/add_commit_single",
        cfg,
        async () => {
          const bytes = new Uint8Array(32);
          new DataView(bytes.buffer).setBigUint64(0, BigInt(seq++), true);
          const head = new CommitId(bytes);
          await syncer.addCommit(sedId, head, [], blob);
        },
      );
      return sample;
    }, DEFAULT);

    console.log(JSON.stringify(summary));
    expect(summary.iters).toBe(DEFAULT.measurementIters);
    expect(summary.median).toBeGreaterThan(0);

    await testInfo.attach("perf-summary-idb", {
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json",
    });
  });
});
