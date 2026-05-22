// Probe what JS-side surfaces actually see when Rust code panics in
// the production-shape Wasm build. This is the empirical answer to
// "with the panic hook installed, can JS root-cause a production
// panic, or do we need panic=unwind?"
//
// **Empirically measured (2026-05-22, chromium-1194, wasm-bindgen
// 0.2.121, `panic = "abort"`):**
//
//   ┌─────────────────────────┬──────────────────────────────────┐
//   │ Surface                 │ What it sees                     │
//   ├─────────────────────────┼──────────────────────────────────┤
//   │ try/catch e.message     │ "unreachable"  (NOT panic text)  │
//   │ try/catch String(e)     │ "RuntimeError: unreachable"      │
//   │ console.error           │ "panicked at ...:LINE: <msg>" ✓  │
//   │ pageerror               │ (no event)                       │
//   │ window.onerror          │ (no event)                       │
//   │ unhandledrejection      │ (no event)                       │
//   └─────────────────────────┴──────────────────────────────────┘
//
// **Operational implication:** error-reporting tools that read
// `error.message` from `try/catch` payloads (e.g. naive Sentry
// integrations) get NO useful information. Tools that capture
// `console.error` (most observability platforms, properly
// configured) get the full panic text.
//
// To use this spec:
//   1. Build with `wasm-pack build subduction_wasm --features e2e_panic_probe ...`.
//      Default release builds gate `__testPanicForE2E` behind the
//      `e2e_panic_probe` feature so it doesn't ship in production.
//   2. Run via Playwright.
//
// This test is informational, not a pass/fail gate. We assert only
// that the panic DID fire (so a regression that silently swallows
// panics is caught), and print a structured report. The report is
// the actual artifact.

import { test, expect } from "@playwright/test";
import { URL } from "./config";

interface PanicReport {
  marker: string;
  caughtException: {
    fired: boolean;
    name: string | null;
    messageText: string | null;
    messageContainsMarker: boolean;
    stackFirstLine: string | null;
  };
  consoleError: {
    captured: string[];
    anyContainsMarker: boolean;
  };
  windowOnError: {
    captured: Array<{
      message: string;
      filename: string;
      lineno: number | null;
    }>;
    anyContainsMarker: boolean;
  };
  unhandledRejection: {
    captured: string[];
    anyContainsMarker: boolean;
  };
}

test.beforeEach(async ({ page }) => {
  await page.goto(URL);
  const wasmTimeout = process.env.CI ? 30000 : 10000;
  await page.waitForFunction(() => window.subductionReady === true, { timeout: wasmTimeout });
});

test.describe("Panic visibility from JS perspective", () => {
  test("synchronous panic via __testPanicForE2E surfaces a message", async ({ page }) => {
    const marker = "panic-marker-sync-" + Math.random().toString(36).slice(2, 10);

    const report: PanicReport | null = await page.evaluate(async (marker) => {
      // ──── Set up capture hooks ────────────────────────────────────
      const consoleErrors: string[] = [];
      const origConsoleError = console.error;
      console.error = (...args) => {
        consoleErrors.push(args.map((a) => String(a)).join(" "));
        origConsoleError.apply(console, args);
      };

      const windowErrors: Array<{ message: string; filename: string; lineno: number | null }> = [];
      const onErrorHandler = (event: ErrorEvent) => {
        windowErrors.push({
          message: String(event.message ?? ""),
          filename: String(event.filename ?? ""),
          lineno: event.lineno ?? null,
        });
      };
      window.addEventListener("error", onErrorHandler);

      const unhandledRejections: string[] = [];
      const onRejectionHandler = (event: PromiseRejectionEvent) => {
        unhandledRejections.push(String(event.reason));
      };
      window.addEventListener("unhandledrejection", onRejectionHandler);

      // ──── Trigger the panic ───────────────────────────────────────
      const wasm: any = (window as any).subduction;
      const triggerFn = wasm.__testPanicForE2E ?? wasm.testPanicForE2E ?? wasm.__test_panic_for_e2e;
      if (!triggerFn) {
        // Built without the `e2e_panic_probe` feature; spec skips.
        return null;
      }

      let exception: unknown = undefined;
      let fired = false;
      try {
        triggerFn(marker);
      } catch (e) {
        exception = e;
        fired = true;
      }

      // Yield a microtask so window error events that were dispatched
      // synchronously but processed via the event loop have a chance
      // to land in our capture array. (Synchronous Wasm traps usually
      // also fire a window error event in browsers.)
      await new Promise((resolve) => setTimeout(resolve, 50));

      // ──── Clean up hooks ──────────────────────────────────────────
      console.error = origConsoleError;
      window.removeEventListener("error", onErrorHandler);
      window.removeEventListener("unhandledrejection", onRejectionHandler);

      // ──── Build report ────────────────────────────────────────────
      const ex: any = exception;
      return {
        marker,
        caughtException: {
          fired,
          name: ex?.name ?? null,
          messageText: ex?.message ?? null,
          messageContainsMarker:
            typeof ex?.message === "string" && ex.message.includes(marker),
          stackFirstLine: typeof ex?.stack === "string" ? ex.stack.split("\n")[0] : null,
        },
        consoleError: {
          captured: consoleErrors,
          anyContainsMarker: consoleErrors.some((s) => s.includes(marker)),
        },
        windowOnError: {
          captured: windowErrors,
          anyContainsMarker: windowErrors.some((e) => e.message.includes(marker)),
        },
        unhandledRejection: {
          captured: unhandledRejections,
          anyContainsMarker: unhandledRejections.some((s) => s.includes(marker)),
        },
      };
    }, marker);

    if (report === null) {
      test.skip(true, "Wasm built without `e2e_panic_probe` feature; panic trigger not available");
      return;
    }

    // ─── Structured human-readable report ───────────────────────────
    // eslint-disable-next-line no-console
    console.log(
      "\n──── Panic visibility report (sync panic) ────────────────────\n" +
        JSON.stringify(report, null, 2) +
        "\n",
    );

    // ─── Floor: the panic must actually fire ────────────────────────
    expect(report.caughtException.fired).toBe(true);

    // Diagnostic summary table — does NOT fail the test; just records
    // which surfaces give back the panic message.
    const surfaces: Record<string, boolean> = {
      "try/catch e.message contains marker": report.caughtException.messageContainsMarker,
      "console.error contains marker": report.consoleError.anyContainsMarker,
      "window.onerror contains marker": report.windowOnError.anyContainsMarker,
      "unhandledrejection contains marker": report.unhandledRejection.anyContainsMarker,
    };
    // eslint-disable-next-line no-console
    console.log("Surfaces that recovered the panic message:");
    for (const [name, ok] of Object.entries(surfaces)) {
      // eslint-disable-next-line no-console
      console.log(`  ${ok ? "✓" : "✗"}  ${name}`);
    }

    // At least ONE surface must have recovered the marker, otherwise
    // we have no diagnosability at all and the panic hook is broken
    // / not installed.
    const anySurfaceRecoveredMarker = Object.values(surfaces).some(Boolean);
    expect(anySurfaceRecoveredMarker).toBe(true);
  });
});
