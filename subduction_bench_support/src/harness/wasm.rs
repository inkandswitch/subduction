//! Wasm-target bench harness driven by `performance.now()`.
//!
//! Criterion is unavailable on `wasm32-*` targets (it relies on OS timers, threads, and file
//! I/O). This module provides a minimal sampling-loop + summary-statistics harness that runs
//! cleanly under `wasm-pack test --node` and `wasm-pack test --headless --chrome/firefox/...`.
//!
//! ## Design
//!
//! Each `bench(...)` call:
//! 1. Runs `warmup_iters` iterations to let the JIT optimise.
//! 2. Runs `measurement_iters` iterations, recording the wall-clock duration of each.
//! 3. Returns a [`Summary`] with min / median / mean / p95 / max.
//!
//! The harness measures **per-iteration** wall-clock in milliseconds (the resolution that
//! browsers give via `performance.now()` — typically quantised to ~5 µs in Chrome and 1 ms in
//! Firefox for security reasons). For sub-microsecond operations, wrap multiple logical calls
//! per iteration and divide out afterwards.
//!
//! Both synchronous ([`bench`]) and asynchronous ([`bench_async`]) harness entrypoints are
//! provided. Async benches use `wasm_bindgen_futures` under the hood.
//!
//! ## Output
//!
//! Summaries are printed to the console via `web_sys::console::log_1` when `report()` is
//! called. The format is machine-parseable JSON:
//!
//! ```text
//! {"bench":"handshake","unit":"ms","iters":50,"min":1.23,"median":1.45,"mean":1.52,"p95":1.80,"max":2.01}
//! ```
//!
//! Downstream tooling (github-action-benchmark in Phase 4.1, a Playwright reporter for the
//! browser-driven suite) can parse these lines out of the test log.
//!
//! ## Cross-runtime timing
//!
//! `performance.now()` lives on different globals depending on where the Wasm module runs:
//!
//! | Runtime         | Global                           |
//! |-----------------|----------------------------------|
//! | Browser (main)  | `window.performance.now()`       |
//! | Worker / SW     | `self.performance.now()`         |
//! | Node.js (≥16)   | `globalThis.performance.now()`   |
//!
//! [`now_ms`] resolves all three via `js_sys::Reflect` so the same bench source runs in any of
//! them without conditional compilation.

// Bench harness: `expect` on `now_ms()` is intentional — if `performance.now` is missing the
// environment is too old to benchmark meaningfully. Panic is the right behaviour.
#![allow(clippy::expect_used)]

use alloc::{format, string::String, vec::Vec};

use js_sys::{Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};

// ============================================================================
// Timer
// ============================================================================

/// Read the high-resolution wall clock in milliseconds.
///
/// Returns `None` if no `performance.now` callable can be found on the current global. This
/// should only happen on very old runtimes — Node ≥16, Chrome ≥60, Firefox ≥60, and Safari
/// ≥11 all expose it.
#[must_use]
pub fn now_ms() -> Option<f64> {
    // `js_sys::global()` returns `Object` for the correct global regardless of whether we are
    // in a browser main thread, a worker, or Node.js.
    let global = js_sys::global();

    let perf = Reflect::get(&global, &JsValue::from_str("performance")).ok()?;
    if perf.is_undefined() || perf.is_null() {
        return None;
    }

    let now = Reflect::get(&perf, &JsValue::from_str("now")).ok()?;
    let now_fn: &Function = now.dyn_ref::<Function>()?;

    let result = now_fn.call0(&perf).ok()?;
    result.as_f64()
}

/// Spin-read [`now_ms`] but panic if the runtime doesn't expose it.
///
/// Benches invoke this on every sample and gain nothing from degrading gracefully — if
/// `performance.now` is missing the environment is too old to measure meaningfully.
///
/// # Panics
///
/// Panics if no `performance.now` callable exists on the current global.
#[must_use]
pub fn now_ms_or_panic() -> f64 {
    now_ms().expect("performance.now() not available on the current global")
}

// ============================================================================
// Summary statistics
// ============================================================================

/// Per-bench summary of `n` measured iterations.
#[derive(Debug, Clone)]
pub struct Summary {
    /// Bench name, as passed to `bench` / `bench_async`.
    pub name: String,
    /// Number of measured iterations (excludes warm-up).
    pub iters: usize,
    /// Shortest iteration (ms).
    pub min: f64,
    /// Median iteration (ms) — linear interpolation, no ranking bias.
    pub median: f64,
    /// Arithmetic mean of all iterations (ms).
    pub mean: f64,
    /// 95th-percentile iteration (ms) — nearest-rank variant.
    pub p95: f64,
    /// Longest iteration (ms).
    pub max: f64,
}

impl Summary {
    /// Compute a `Summary` from a list of per-iteration durations in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if `samples` is empty — an empty bench is a programmer error, not a runtime
    /// condition.
    #[must_use]
    pub fn from_samples(name: &str, samples: &[f64]) -> Self {
        assert!(!samples.is_empty(), "Summary::from_samples: empty samples");

        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(core::cmp::Ordering::Equal));

        let min = *sorted.first().unwrap_or(&0.0);
        let max = *sorted.last().unwrap_or(&0.0);

        // Nearest-rank percentile: idx = ceil(p/100 * n) - 1, clamped.
        let p95_idx = percentile_index(sorted.len(), 95);
        let p95 = *sorted.get(p95_idx).unwrap_or(&max);

        // Median: interpolate between the two middle samples on even counts.
        let median = {
            let n = sorted.len();
            if n.is_multiple_of(2) {
                let a = *sorted.get(n / 2 - 1).unwrap_or(&0.0);
                let b = *sorted.get(n / 2).unwrap_or(&0.0);
                f64::midpoint(a, b)
            } else {
                *sorted.get(n / 2).unwrap_or(&0.0)
            }
        };

        let sum: f64 = sorted.iter().sum();
        // `len()` is non-zero per the assert above, so the cast and division are both safe.
        #[allow(clippy::cast_precision_loss)]
        let mean = sum / sorted.len() as f64;

        Self {
            name: String::from(name),
            iters: samples.len(),
            min,
            median,
            mean,
            p95,
            max,
        }
    }

    /// Format the summary as a single-line JSON string.
    ///
    /// Downstream tooling (github-action-benchmark, custom Playwright reporters) parses these
    /// lines out of the wasm-pack test log.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"bench\":\"{}\",\"unit\":\"ms\",\"iters\":{},\"min\":{:.6},\"median\":{:.6},\"mean\":{:.6},\"p95\":{:.6},\"max\":{:.6}}}",
            escape_for_json(&self.name),
            self.iters,
            self.min,
            self.median,
            self.mean,
            self.p95,
            self.max,
        )
    }

    /// Emit the summary to the JS console (and Node stdout, as `console.log` is the canonical
    /// bridge under `wasm-pack test --node`).
    pub fn report(&self) {
        let line = self.to_json();
        web_sys::console::log_1(&JsValue::from_str(&line));
    }
}

/// Nearest-rank percentile index for a sorted slice of `len` elements.
fn percentile_index(len: usize, percentile: u32) -> usize {
    // len > 0 enforced upstream; percentile ∈ 0..=100.
    let p = f64::from(percentile).clamp(0.0, 100.0);
    #[allow(clippy::cast_precision_loss)]
    let rank = (p / 100.0 * len as f64).ceil();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = rank as usize;
    idx.saturating_sub(1).min(len - 1)
}

/// Minimal JSON string escaping — only `"` and `\` need handling for our bench names.
fn escape_for_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out
}

// ============================================================================
// Bench loops
// ============================================================================

/// Configuration for a single bench invocation.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Warm-up iterations (results discarded). Defaults to 5.
    pub warmup_iters: usize,
    /// Measurement iterations. Defaults to 50.
    pub measurement_iters: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            warmup_iters: 5,
            measurement_iters: 50,
        }
    }
}

impl Config {
    /// Quick config: 1 warm-up + 10 measured iterations. Use for smoke-testing.
    #[must_use]
    pub const fn quick() -> Self {
        Self {
            warmup_iters: 1,
            measurement_iters: 10,
        }
    }

    /// Long config: 10 warm-up + 200 measured iterations. Use for publication-quality numbers
    /// at the cost of wall-clock time.
    #[must_use]
    pub const fn long() -> Self {
        Self {
            warmup_iters: 10,
            measurement_iters: 200,
        }
    }
}

/// Run a synchronous bench loop and return a [`Summary`].
///
/// `body` is invoked once per iteration. The closure takes no arguments — if setup state is
/// needed per iteration, build it inside the closure (accepting the setup cost as part of the
/// measured window) or reuse a single `RefCell`-shared value across iterations.
///
/// # Example
///
/// ```ignore
/// use subduction_bench_support::harness::wasm::{bench, Config};
///
/// let summary = bench("blake3_hash_1kb", Config::default(), || {
///     let _ = blake3::hash(b"...1024 bytes of data...");
/// });
/// summary.report();
/// ```
#[must_use]
pub fn bench<F: FnMut()>(name: &str, config: Config, mut body: F) -> Summary {
    for _ in 0..config.warmup_iters {
        body();
    }

    let mut samples = Vec::with_capacity(config.measurement_iters);
    for _ in 0..config.measurement_iters {
        let start = now_ms_or_panic();
        body();
        let end = now_ms_or_panic();
        samples.push(end - start);
    }

    Summary::from_samples(name, &samples)
}

/// Run an asynchronous bench loop and return a [`Summary`].
///
/// `body` is a factory that returns a fresh future on each call. The factory pattern is
/// required because futures are single-use. Example:
///
/// ```ignore
/// use subduction_bench_support::harness::wasm::{bench_async, Config};
///
/// let summary = bench_async("sync_roundtrip", Config::default(), || {
///     let peer_a = peer_a.clone();
///     let peer_b = peer_b.clone();
///     async move { peer_a.sync_with_peer(&peer_b.id()).await }
/// }).await;
/// summary.report();
/// ```
///
/// **Caveat**: `performance.now()` readings bracket the whole future — including any awaits
/// the runtime resolves concurrently. If the future yields to a long-running background task,
/// the bench will include that wall time. For isolated measurement of a single future's
/// wake-to-completion path, drive it synchronously with `futures::executor::block_on`
/// (native) or ensure the test has no other pending work when the bench runs.
pub async fn bench_async<F, Fut>(name: &str, config: Config, mut factory: F) -> Summary
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    for _ in 0..config.warmup_iters {
        factory().await;
    }

    let mut samples = Vec::with_capacity(config.measurement_iters);
    for _ in 0..config.measurement_iters {
        let start = now_ms_or_panic();
        factory().await;
        let end = now_ms_or_panic();
        samples.push(end - start);
    }

    Summary::from_samples(name, &samples)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_index_edge_cases() {
        // Single-element slice: everything points at index 0.
        assert_eq!(percentile_index(1, 0), 0);
        assert_eq!(percentile_index(1, 50), 0);
        assert_eq!(percentile_index(1, 100), 0);

        // 100-element slice: p95 is index 94 (nearest-rank).
        assert_eq!(percentile_index(100, 95), 94);
        assert_eq!(percentile_index(100, 100), 99);
    }

    #[test]
    fn summary_from_samples_basic() {
        // 10 samples: 1.0 .. 10.0. Median is between 5 and 6 → 5.5. p95 is 10.0 (index 9).
        let samples: Vec<f64> = (1..=10).map(f64::from).collect();
        let summary = Summary::from_samples("test", &samples);

        assert_eq!(summary.iters, 10);
        assert!((summary.min - 1.0).abs() < f64::EPSILON);
        assert!((summary.max - 10.0).abs() < f64::EPSILON);
        assert!((summary.median - 5.5).abs() < f64::EPSILON);
        assert!((summary.mean - 5.5).abs() < f64::EPSILON);
    }

    #[test]
    fn summary_to_json_roundtrips_name_with_quotes() {
        let summary = Summary::from_samples("quote\"in-name", &[1.0]);
        let json = summary.to_json();
        assert!(json.contains(r#"\"in-name"#));
    }
}
